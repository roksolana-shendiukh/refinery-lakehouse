"""The agent: a short loop in which the model searches the EIA articles and then answers.

Each question is answered on its own (no chat history). The model may call search_articles; each call is run
and its result goes back to the model, until it answers in text. The loop makes at most max_iterations model
calls; the last call is not allowed to use tools, so the model has to answer with what it has found.
"""

import datetime
import logging
import time
from dataclasses import dataclass

from openai import OpenAI

from assistant.prompts import REFUSAL, answer_sources, system_prompt
from assistant.retrieval import ArticleSearch
from assistant.tools import SEARCH_TOOL_NAME, TOOLS, run_tool

logger = logging.getLogger(__name__)

# Shown to the user when the model or the search fails; the error itself goes to the request log
ERROR_ANSWER = "The assistant could not answer right now. Please try again in a moment."


@dataclass(frozen=True)
class ToolCall:
    name: str
    arguments: str  # JSON, as the model sent it
    chunk_ids: tuple = ()  # chunks the search returned


@dataclass(frozen=True)
class AgentAnswer:
    question: str
    answer: str
    model: str
    sources: tuple = ()  # prompts.Source, cited pages first
    chunks: tuple = ()  # retrieval.Chunk, every chunk the searches returned, without repeats
    tool_calls: tuple = ()
    iterations: int = 0  # model calls
    prompt_tokens: int = 0
    completion_tokens: int = 0
    latency_ms: int = 0
    stopped_at_limit: bool = False  # the model still wanted to search after max_iterations calls
    error: str | None = None


def assistant_message(message):
    """The model's reply as a chat message, to send back with the tool results."""
    return {
        "role": "assistant",
        "content": message.content or "",
        "tool_calls": [
            {
                "id": call.id,
                "type": "function",
                "function": {"name": call.function.name, "arguments": call.function.arguments},
            }
            for call in message.tool_calls
        ],
    }


def final_answer(content, searched):
    """The model's text, or the refusal when the answer rests on no search."""
    answer = (content or "").strip()
    if not answer:
        return REFUSAL
    if not searched and answer != REFUSAL:
        logger.warning("Answer without a search replaced by the refusal: %.80s", answer)
        return REFUSAL
    return answer


class Agent:
    def __init__(self, llm, model, search, max_iterations=3, today=None, clock=time.monotonic):
        self.llm = llm  # OpenAI-compatible client (WorkspaceModels in the app)
        self.model = model  # serving endpoint name
        self.search = search  # retrieval.ArticleSearch, or anything with the same call
        self.max_iterations = max_iterations
        self.today = today or (lambda: datetime.datetime.now(datetime.UTC).date())
        self.clock = clock

    def ask(self, question):
        started = self.clock()
        state = {"chunks": {}, "tool_calls": [], "iterations": 0, "prompt_tokens": 0, "completion_tokens": 0}
        try:
            answer, stopped = self._loop(question, state)
            error = None
        except Exception as exc:  # the user gets a short message; the log gets the error
            logger.exception("Question failed: %s", question)
            answer, stopped, error = ERROR_ANSWER, False, f"{type(exc).__name__}: {exc}"

        chunks = tuple(state["chunks"].values())
        return AgentAnswer(
            question=question,
            answer=answer,
            model=self.model,
            sources=tuple(answer_sources(chunks, answer)),
            chunks=chunks,
            tool_calls=tuple(state["tool_calls"]),
            iterations=state["iterations"],
            prompt_tokens=state["prompt_tokens"],
            completion_tokens=state["completion_tokens"],
            latency_ms=round((self.clock() - started) * 1000),
            stopped_at_limit=stopped,
            error=error,
        )

    def _loop(self, question, state):
        messages = [
            {"role": "system", "content": system_prompt(self.today())},
            {"role": "user", "content": question},
        ]
        for iteration in range(1, self.max_iterations + 1):
            last = iteration == self.max_iterations
            response = self.llm.chat.completions.create(
                model=self.model,
                messages=messages,
                tools=TOOLS,
                tool_choice="none" if last else "auto",  # the last call must answer
                temperature=0,
            )
            state["iterations"] = iteration
            if response.usage:
                state["prompt_tokens"] += response.usage.prompt_tokens or 0
                state["completion_tokens"] += response.usage.completion_tokens or 0

            message = response.choices[0].message
            if not message.tool_calls:
                searched = any(call.name == SEARCH_TOOL_NAME for call in state["tool_calls"])
                return final_answer(message.content, searched), False
            if last:
                return REFUSAL, True

            messages.append(assistant_message(message))
            for call in message.tool_calls:
                text, chunks = run_tool(call.function.name, call.function.arguments, self.search)
                logger.info("Tool %s(%s): %d chunks", call.function.name, call.function.arguments, len(chunks))
                state["tool_calls"].append(
                    ToolCall(call.function.name, call.function.arguments, tuple(c.chunk_id for c in chunks))
                )
                for chunk in chunks:
                    state["chunks"].setdefault(chunk.chunk_id, chunk)
                messages.append({"role": "tool", "tool_call_id": call.id, "content": text})
        return REFUSAL, True  # max_iterations < 1


class WorkspaceModels:
    """OpenAI-compatible client for the workspace's model serving endpoints, with a fresh token for every request.

    The app's service principal gets OAuth tokens that expire; config.authenticate() renews them when needed.
    max_retries: retries of 429 and 5xx with exponential backoff (the OpenAI client's default is 2).
    """

    def __init__(self, workspace_client, max_retries=2):
        self.workspace_client = workspace_client
        self.max_retries = max_retries

    @property
    def chat(self):
        config = self.workspace_client.config
        token = config.authenticate()["Authorization"].removeprefix("Bearer ")
        base_url = f"{config.host.rstrip('/')}/serving-endpoints"
        return OpenAI(base_url=base_url, api_key=token, max_retries=self.max_retries).chat


def build_agent(settings, workspace_client):
    """The agent of the app: Databricks-hosted model and the article search, both with the app's credentials."""
    return Agent(
        llm=WorkspaceModels(workspace_client),
        model=settings.llm_endpoint,
        search=ArticleSearch(workspace_client, settings.index_name, settings.search_results),
        max_iterations=settings.max_iterations,
    )
