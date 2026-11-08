"""The Terminals tab: terminal zones ranked by tankers waiting. Clicking a row opens it on the map."""

import pandas as pd
from dash import html
from styling import LEVEL_LABELS, LEVEL_TEXT_COLOR
from views import ALL_COASTS

ROW_TYPE = "terminal-row"  # pattern-matching id type of a clickable row

HEADERS = ("Terminal", "Coast", "Waiting", "At berth", "Inbound", "Congestion")
NUMERIC = {"Waiting", "At berth", "Inbound"}
MIN_BAR_PERCENT = 4  # an empty bar still shows a sliver, so rows line up visually


def table_frame(terminals: pd.DataFrame, coast: str | None) -> pd.DataFrame:
    """Rows for the table, filtered by coast and ranked by waiting, then at berth, then name."""
    if terminals.empty:
        return terminals
    rows = terminals
    if coast and coast != ALL_COASTS:
        rows = rows[rows["coast"] == coast]
    return rows.sort_values(["waiting", "at_berth", "port_name"], ascending=[False, False, True]).reset_index(drop=True)


def bar_percent(waiting: int, top_waiting: int) -> int:
    if top_waiting <= 0:
        return MIN_BAR_PERCENT
    return max(MIN_BAR_PERCENT, round(waiting / top_waiting * 100))


def _row(row, top_waiting: int) -> html.Tr:
    level = row.congestion_level or "normal"
    color = LEVEL_TEXT_COLOR.get(level, LEVEL_TEXT_COLOR["normal"])
    return html.Tr(
        id={"type": ROW_TYPE, "port": row.port_bk},
        n_clicks=0,
        className="terminal-row",
        title="Open on the map",
        children=[
            html.Td(row.port_name, className="name"),
            html.Td(f"{row.coast} · {row.padd}", className="muted"),
            html.Td(int(row.waiting), className="num", style={"color": color}),
            html.Td(int(row.at_berth), className="num"),
            html.Td(int(row.inbound), className="num inbound"),
            html.Td(
                html.Span(
                    className="level",
                    children=[
                        html.Span(
                            className="bar",
                            children=html.Span(
                                className="bar-fill",
                                style={"width": f"{bar_percent(int(row.waiting), top_waiting)}%", "background": color},
                            ),
                        ),
                        html.Span(LEVEL_LABELS.get(level, level), style={"color": color}),
                    ],
                )
            ),
        ],
    )


def table(frame: pd.DataFrame) -> html.Table:
    top_waiting = int(frame["waiting"].max()) if not frame.empty else 0
    head = html.Thead(html.Tr([html.Th(name, className="num" if name in NUMERIC else None) for name in HEADERS]))
    if frame.empty:
        body = html.Tbody(html.Tr(html.Td("No terminal data yet", colSpan=len(HEADERS), className="muted")))
    else:
        body = html.Tbody([_row(row, top_waiting) for row in frame.itertuples()])
    return html.Table([head, body], className="terminals")
