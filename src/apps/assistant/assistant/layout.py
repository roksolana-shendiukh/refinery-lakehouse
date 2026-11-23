"""The chat page: header, question box with example questions, and the places for the answer, its sources,
the retrieved context and the run details. Built once; callbacks only fill the parts that carry an id below.
"""

from dash import dcc, html

# Component ids used by the callbacks
QUESTION = "question"
ASK = "ask"
EXAMPLE = "example"  # pattern id: {"type": EXAMPLE, "index": n}
STATUS = "status"
ANSWER = "answer"
SOURCES = "sources"
CONTEXT = "context"
DETAILS = "details"

MAX_QUESTION_LENGTH = 500
EXAMPLE_QUESTIONS = (
    "What is EIA's forecast for the Brent crude oil price in 2026?",
    "How did attacks in the Red Sea affect oil shipping?",
    "Why did U.S. retail gasoline prices fall below $3 per gallon?",
)
SUBTITLE = "Answers from EIA Today in Energy articles (since 2024) and the current Short-Term Energy Outlook"


def header():
    return html.Header(
        className="app-header",
        children=[html.H1("Refinery assistant"), html.Span(SUBTITLE, className="subtitle")],
    )


def question_box():
    return html.Section(
        className="ask-panel",
        children=[
            dcc.Textarea(
                id=QUESTION,
                className="question",
                placeholder="Ask about crude oil, gasoline, diesel, refining, prices or trade...",
                maxLength=MAX_QUESTION_LENGTH,
            ),
            html.Div(
                className="ask-row",
                children=[
                    html.Div(
                        className="examples",
                        children=[
                            html.Button(text, id={"type": EXAMPLE, "index": n}, className="example", n_clicks=0)
                            for n, text in enumerate(EXAMPLE_QUESTIONS)
                        ],
                    ),
                    html.Button("Ask", id=ASK, className="ask", n_clicks=0),
                ],
            ),
        ],
    )


def answer_area():
    """Empty until the first question; the spinner shows while the agent works (5-10 seconds)."""
    return dcc.Loading(
        type="dot",
        color="#7CC4FF",
        children=html.Section(
            className="answer-panel",
            children=[
                html.Div(id=STATUS),
                html.Div(id=ANSWER, className="answer"),
                html.Div(id=SOURCES, className="sources"),
                html.Div(id=CONTEXT, className="context"),
                html.Div(id=DETAILS, className="details"),
            ],
        ),
    )


def page():
    return html.Div(
        className="app",
        children=[
            header(),
            html.Main(className="app-main", children=[question_box(), answer_area()]),
            html.Footer(
                "Each question is answered on its own, from the passages the search finds. "
                "Check figures in the linked EIA pages.",
                className="app-footer",
            ),
        ],
    )


def not_configured(error):
    return html.Div(
        className="app",
        children=[
            header(),
            html.Main(className="app-main app-error", children=[html.P(f"The assistant is not configured: {error}")]),
        ],
    )
