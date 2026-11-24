"""Lakeflow job task: evaluate the assistant on the control questions and log the results to MLflow.

Two runs in the experiment: the agent with the article search, and the same model without it (baseline).
The vector search index must exist: in PROD run assistant-start first, in DEV refinery-rag-search-start.
Answers are not written to the request log, so evaluation does not mix with real users' questions.
"""

import argparse
import datetime
import logging
import os
import sys

import mlflow
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

logger = logging.getLogger("refinery.assistant.evaluate")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    # Serverless runs this script with exec(), without __file__, so the job passes the folder explicitly
    parser.add_argument("--app-dir", required=True, help="Folder of the assistant app (holds the assistant package)")
    parser.add_argument("--questions", required=True, help="JSON file with the control questions")
    parser.add_argument("--index-name", required=True)
    parser.add_argument("--llm-endpoint", required=True)
    parser.add_argument("--experiment", required=True, help="MLflow experiment path")
    # Never empty: the Terraform provider turns an empty job parameter into null and fails to create the job
    parser.add_argument(
        "--judge-model", default="built-in", help="Judge endpoint, e.g. databricks:/...; built-in = Databricks-hosted"
    )
    parser.add_argument("--search-results", type=int, default=5)
    parser.add_argument("--max-iterations", type=int, default=3)
    # The pay-per-token model has a per-workspace QPS limit; MLflow answers 10 questions at once by default
    parser.add_argument("--max-workers", type=int, default=2, help="Questions answered at the same time")
    return parser.parse_args()


def require_index(client, index_name):
    try:
        client.vector_search_indexes.get_index(index_name)
    except NotFound:
        raise SystemExit(
            f"Vector search index {index_name} does not exist: run assistant-start (PROD) "
            "or refinery-rag-search-start (DEV) first"
        ) from None


def evaluate(args):
    # Read by mlflow.genai.evaluate when it starts answering the questions
    os.environ["MLFLOW_GENAI_EVAL_MAX_WORKERS"] = str(args.max_workers)
    judge_model = None if args.judge_model == "built-in" else args.judge_model
    # The assistant package comes from the app folder, not the project wheel: import it once that folder is known
    sys.path.insert(0, args.app_dir)
    from assistant.agent import Agent, WorkspaceModels
    from assistant.evaluation import (
        NoSearchAssistant,
        TracedSearch,
        answer_fn,
        build_scorers,
        eval_records,
        load_questions,
    )
    from assistant.retrieval import ArticleSearch

    client = WorkspaceClient()
    require_index(client, args.index_name)

    questions = load_questions(args.questions)
    data = eval_records(questions)
    mlflow.set_experiment(args.experiment)
    mlflow.openai.autolog()

    llm = WorkspaceModels(client, max_retries=6)
    search = TracedSearch(ArticleSearch(client, args.index_name, args.search_results), mlflow.start_span)
    assistants = {
        "with_search": Agent(llm, args.llm_endpoint, search, max_iterations=args.max_iterations),
        "without_search": NoSearchAssistant(llm, args.llm_endpoint),
    }

    today = datetime.date.today().isoformat()
    for mode, assistant in assistants.items():
        # One trace per question with the model calls and searches inside it: the judges read the
        # retrieved passages from this trace, so they must not land in separate traces
        predict = mlflow.trace(answer_fn(assistant), name=f"assistant_{mode}", span_type="AGENT")
        with mlflow.start_run(run_name=f"{mode} {today}") as run:
            mlflow.log_params(
                {
                    "mode": mode,
                    "llm_endpoint": args.llm_endpoint,
                    "index_name": args.index_name,
                    "search_results": args.search_results,
                    "max_iterations": args.max_iterations,
                    "max_workers": args.max_workers,
                    "judge_model": args.judge_model,
                    "questions": len(questions),
                }
            )
            result = mlflow.genai.evaluate(
                data=data,
                predict_fn=predict,
                scorers=build_scorers(with_search=mode == "with_search", judge_model=judge_model),
            )
            logger.info("%s (run %s): %s", mode, run.info.run_id, result.metrics)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", force=True)
    evaluate(parse_args())


if __name__ == "__main__":
    main()
