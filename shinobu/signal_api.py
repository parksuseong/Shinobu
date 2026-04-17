from __future__ import annotations

import json
import sqlite3
from typing import Any

from fastapi import FastAPI, Query
from pydantic import BaseModel, Field

from shinobu.cache_db import DB_PATH


APP_TITLE = "Shinobu Signal API"
APP_VERSION = "1.0.0"
DEFAULT_TIMEFRAME = "5분봉"
DEFAULT_STRATEGY_NAME = "src_v2_adx"
DEFAULT_ADJUSTMENT_KEY = "s0_c0_r0"
PRIMARY_SIGNAL_SYMBOL = "122630.KS"
PAIR_SIGNAL_SYMBOL = "252670.KS"


class SignalItem(BaseModel):
    symbol: str
    timeframe: str
    strategy_name: str
    adjustment_key: str
    ts: str
    buy_open: bool = Field(default=False)
    buy_close: bool = Field(default=False)
    scr_line: float | None = None
    close: float | None = None
    signal_detail: str | None = None
    raw: dict[str, Any] = Field(default_factory=dict)


class SignalListResponse(BaseModel):
    count: int
    items: list[SignalItem]


class ExecutionItem(BaseModel):
    symbol: str
    name: str | None = None
    side: str | None = None
    quantity: float | None = None
    price: float | None = None
    amount: float | None = None
    timestamp: str
    order_no: str | None = None
    order_branch: str | None = None


class ExecutionListResponse(BaseModel):
    count: int
    updated_at: str | None = None
    items: list[ExecutionItem]


class SignalEventItem(BaseModel):
    symbol: str
    instrument: str
    ts: str
    signal: str


class SignalEventListResponse(BaseModel):
    count: int
    items: list[SignalEventItem]


app = FastAPI(
    title=APP_TITLE,
    version=APP_VERSION,
    description=(
        "Read strategy signal rows from sqlite indicator_data.\n\n"
        "- Swagger UI: /docs\n"
        "- ReDoc: /redoc"
    ),
)


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)


def _decode_row_json(raw: str) -> dict[str, Any]:
    try:
        payload = json.loads(raw)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _to_signal_item(
    *,
    symbol: str,
    timeframe: str,
    strategy_name: str,
    adjustment_key: str,
    ts: str,
    row_json: str,
) -> SignalItem:
    payload = _decode_row_json(row_json)
    return SignalItem(
        symbol=symbol,
        timeframe=timeframe,
        strategy_name=strategy_name,
        adjustment_key=adjustment_key,
        ts=ts,
        buy_open=bool(payload.get("buy_open", False)),
        buy_close=bool(payload.get("buy_close", False)),
        scr_line=(float(payload["scr_line"]) if payload.get("scr_line") is not None else None),
        close=(float(payload["Close"]) if payload.get("Close") is not None else None),
        signal_detail=(str(payload.get("signal_detail")) if payload.get("signal_detail") is not None else None),
        raw=payload,
    )


def _is_triggered_signal(payload: dict[str, Any]) -> bool:
    return bool(payload.get("buy_open", False) or payload.get("buy_close", False))


def _normalize_sort(sort: str) -> str:
    return "asc" if str(sort).lower() == "asc" else "desc"


def _normalize_signal_filter(signal: str | None) -> str | None:
    if signal is None:
        return None
    token = str(signal).strip().lower()
    if token in {"open", "buy_open", "buy open"}:
        return "open"
    if token in {"close", "buy_close", "buy close"}:
        return "close"
    return None


def _instrument_name(symbol: str) -> str:
    if symbol == "122630.KS":
        return "레버리지"
    if symbol == "252670.KS":
        return "곱버스"
    return symbol


def _expand_signal_events(item: SignalItem) -> list[SignalEventItem]:
    events: list[SignalEventItem] = []
    if item.buy_open:
        events.append(
            SignalEventItem(
                symbol=item.symbol,
                instrument=_instrument_name(item.symbol),
                ts=item.ts,
                signal="open",
            )
        )
    if item.buy_close:
        events.append(
            SignalEventItem(
                symbol=item.symbol,
                instrument=_instrument_name(item.symbol),
                ts=item.ts,
                signal="close",
            )
        )
    return events


def _build_position_signal_events(items: list[SignalItem]) -> list[SignalEventItem]:
    scoped = [item for item in items if item.symbol in {PRIMARY_SIGNAL_SYMBOL, PAIR_SIGNAL_SYMBOL}]
    if not scoped:
        return []

    grouped: dict[str, dict[str, SignalItem]] = {}
    for item in scoped:
        grouped.setdefault(item.ts, {})[item.symbol] = item

    current_position: str | None = None
    events: list[SignalEventItem] = []
    for ts in sorted(grouped.keys()):
        bucket = grouped[ts]
        primary = bucket.get(PRIMARY_SIGNAL_SYMBOL)
        pair = bucket.get(PAIR_SIGNAL_SYMBOL)
        primary_open = bool(primary.buy_open) if primary is not None else False
        primary_close = bool(primary.buy_close) if primary is not None else False
        pair_open = bool(pair.buy_open) if pair is not None else False
        pair_close = bool(pair.buy_close) if pair is not None else False

        if current_position is None:
            if primary_open and pair_open:
                primary_scr = float(primary.scr_line or 0.0) if primary is not None else 0.0
                pair_scr = float(pair.scr_line or 0.0) if pair is not None else 0.0
                current_position = PRIMARY_SIGNAL_SYMBOL if primary_scr >= pair_scr else PAIR_SIGNAL_SYMBOL
            elif primary_open:
                current_position = PRIMARY_SIGNAL_SYMBOL
            elif pair_open:
                current_position = PAIR_SIGNAL_SYMBOL

            if current_position is not None:
                events.append(
                    SignalEventItem(
                        symbol=current_position,
                        instrument=_instrument_name(current_position),
                        ts=ts,
                        signal="open",
                    )
                )
            continue

        if current_position == PRIMARY_SIGNAL_SYMBOL:
            if pair_open:
                events.append(
                    SignalEventItem(
                        symbol=PRIMARY_SIGNAL_SYMBOL,
                        instrument=_instrument_name(PRIMARY_SIGNAL_SYMBOL),
                        ts=ts,
                        signal="close",
                    )
                )
                events.append(
                    SignalEventItem(
                        symbol=PAIR_SIGNAL_SYMBOL,
                        instrument=_instrument_name(PAIR_SIGNAL_SYMBOL),
                        ts=ts,
                        signal="open",
                    )
                )
                current_position = PAIR_SIGNAL_SYMBOL
            elif primary_close:
                events.append(
                    SignalEventItem(
                        symbol=PRIMARY_SIGNAL_SYMBOL,
                        instrument=_instrument_name(PRIMARY_SIGNAL_SYMBOL),
                        ts=ts,
                        signal="close",
                    )
                )
                current_position = None
            continue

        if current_position == PAIR_SIGNAL_SYMBOL:
            if primary_open:
                events.append(
                    SignalEventItem(
                        symbol=PAIR_SIGNAL_SYMBOL,
                        instrument=_instrument_name(PAIR_SIGNAL_SYMBOL),
                        ts=ts,
                        signal="close",
                    )
                )
                events.append(
                    SignalEventItem(
                        symbol=PRIMARY_SIGNAL_SYMBOL,
                        instrument=_instrument_name(PRIMARY_SIGNAL_SYMBOL),
                        ts=ts,
                        signal="open",
                    )
                )
                current_position = PRIMARY_SIGNAL_SYMBOL
            elif pair_close:
                events.append(
                    SignalEventItem(
                        symbol=PAIR_SIGNAL_SYMBOL,
                        instrument=_instrument_name(PAIR_SIGNAL_SYMBOL),
                        ts=ts,
                        signal="close",
                    )
                )
                current_position = None
    return events


def _load_triggered_signals(
    *,
    timeframe: str,
    strategy_name: str | None,
    symbol: str | None,
    from_ts: str | None,
    to_ts: str | None,
    sort: str,
) -> list[SignalItem]:
    adjustment_key = DEFAULT_ADJUSTMENT_KEY
    conditions = ["timeframe = ?", "adjustment_key = ?"]
    params: list[Any] = [timeframe, adjustment_key]
    if strategy_name:
        conditions.append("strategy_name = ?")
        params.append(strategy_name)
    if symbol:
        conditions.append("symbol = ?")
        params.append(symbol)
    if from_ts:
        conditions.append("ts >= ?")
        params.append(from_ts)
    if to_ts:
        conditions.append("ts <= ?")
        params.append(to_ts)

    order_by = "ASC" if _normalize_sort(sort) == "asc" else "DESC"
    sql = f"""
        SELECT symbol, timeframe, strategy_name, adjustment_key, ts, row_json
        FROM indicator_data
        WHERE {' AND '.join(conditions)}
        ORDER BY ts {order_by}
    """
    with _connect() as connection:
        rows = connection.execute(sql, params).fetchall()

    items: list[SignalItem] = []
    for row in rows:
        item = _to_signal_item(
            symbol=str(row[0]),
            timeframe=str(row[1]),
            strategy_name=str(row[2]),
            adjustment_key=str(row[3]),
            ts=str(row[4]),
            row_json=str(row[5]),
        )
        if _is_triggered_signal(item.raw):
            items.append(item)
    return items


def _load_latest_execution_rows() -> tuple[str | None, list[dict[str, Any]]]:
    with _connect() as connection:
        row = connection.execute(
            """
            SELECT frame_json, updated_at
            FROM execution_cache
            ORDER BY updated_at DESC
            LIMIT 1
            """
        ).fetchone()
    if row is None:
        return None, []

    updated_at = str(row[1]) if row[1] is not None else None
    try:
        payload = json.loads(str(row[0]))
    except Exception:
        return updated_at, []

    columns = payload.get("columns", [])
    data_rows = payload.get("data", [])
    if not isinstance(columns, list) or not isinstance(data_rows, list):
        return updated_at, []

    decoded: list[dict[str, Any]] = []
    for values in data_rows:
        if not isinstance(values, list):
            continue
        decoded.append(dict(zip(columns, values)))
    return updated_at, decoded


def _to_execution_item(row: dict[str, Any]) -> ExecutionItem:
    return ExecutionItem(
        symbol=str(row.get("symbol", "")),
        name=(str(row.get("name")) if row.get("name") is not None else None),
        side=(str(row.get("side")) if row.get("side") is not None else None),
        quantity=(float(row["quantity"]) if row.get("quantity") is not None else None),
        price=(float(row["price"]) if row.get("price") is not None else None),
        amount=(float(row["amount"]) if row.get("amount") is not None else None),
        timestamp=str(row.get("timestamp", "")),
        order_no=(str(row.get("order_no")) if row.get("order_no") is not None else None),
        order_branch=(str(row.get("order_branch")) if row.get("order_branch") is not None else None),
    )


@app.get("/health")
def health() -> dict[str, Any]:
    return {"ok": True, "db_path": str(DB_PATH)}


@app.get("/v1/signals", response_model=SignalEventListResponse, tags=["signals"])
def query_signals(
    from_ts: str | None = Query(None, description="inclusive. e.g. 2026-04-16T09:00:00"),
    to_ts: str | None = Query(None, description="inclusive. e.g. 2026-04-16T15:30:00"),
    sort: str = Query("desc", description="asc or desc"),
    symbol: str | None = Query(None, description="e.g. 122630.KS or 252670.KS. omit for all"),
    signal: str | None = Query(None, description="open or close. omit for all"),
    timeframe: str = Query(DEFAULT_TIMEFRAME),
    limit: int = Query(2000, ge=1, le=10000),
) -> SignalEventListResponse:
    items = _load_triggered_signals(
        timeframe=timeframe,
        strategy_name=DEFAULT_STRATEGY_NAME,
        symbol=None,
        from_ts=from_ts,
        to_ts=to_ts,
        sort="asc",
    )

    filter_signal = _normalize_signal_filter(signal)
    events = _build_position_signal_events(items)
    if symbol:
        events = [event for event in events if event.symbol == symbol]
    if filter_signal:
        events = [event for event in events if event.signal == filter_signal]
    if _normalize_sort(sort) == "desc":
        events = list(reversed(events))

    events = events[: int(limit)]
    return SignalEventListResponse(count=len(events), items=events)


@app.get("/v1/executions/recent", response_model=ExecutionListResponse, tags=["executions"])
def get_recent_executions(
    symbol: str | None = Query(None, description="e.g. 069500.KS"),
    side: str | None = Query(None, description="buy or sell"),
    limit: int = Query(100, ge=1, le=2000),
) -> ExecutionListResponse:
    updated_at, rows = _load_latest_execution_rows()

    normalized_side = side.lower() if side else None
    if symbol:
        rows = [row for row in rows if str(row.get("symbol", "")) == symbol]
    if normalized_side:
        rows = [row for row in rows if str(row.get("side", "")).lower() == normalized_side]

    rows = sorted(rows, key=lambda row: str(row.get("timestamp", "")), reverse=True)[: int(limit)]
    items = [_to_execution_item(row) for row in rows]
    return ExecutionListResponse(count=len(items), updated_at=updated_at, items=items)
