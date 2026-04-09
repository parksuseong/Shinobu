from __future__ import annotations

import base64
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components
from PIL import Image

from config import has_kis_account
from shinobu import data as market_data
from shinobu.chart import build_candlestick_chart, update_candlestick_chart
from shinobu.chart_server import ensure_chart_server
from shinobu.live_chart_component import build_live_chart_html
from shinobu.kis import KisApiError, fetch_domestic_balance, fetch_domestic_daily_ccld
from shinobu.live_trading import (
    get_asset_history,
    get_live_logs,
    get_live_orders,
    get_live_runtime_state,
    get_live_started_at,
    init_live_state,
    is_live_enabled,
    process_live_trading_cycle,
    record_asset_snapshot,
    set_live_enabled,
)
from shinobu.strategy import StrategyAdjustments, calculate_scr_strategy
LIVE_TIMEFRAME = "5분봉"


LIVE_TIMEFRAME = "5분봉"
PRIMARY_SYMBOL = "122630.KS"
LIVE_CHART_STATE_KEY = "live_chart_state"
LIVE_FIGURE_STATE_KEY = "live_figure_state"
MAX_LIVE_CHART_CANDLES = 50
ASSET_DIR = Path(__file__).resolve().parent / "assets"
POSITIVE_IMAGE_PATH = ASSET_DIR / "shinobu_positive.png"
NEGATIVE_IMAGE_PATH = ASSET_DIR / "shinobu_negative.png"
POSITIVE_FALLBACK_PATH = ASSET_DIR / "shinobu_positive.svg"
NEGATIVE_FALLBACK_PATH = ASSET_DIR / "shinobu_negative.svg"


st.set_page_config(page_title="Shinobu Project", page_icon="??", layout="wide")


display_name = market_data.display_name
get_pair_symbol = market_data.get_pair_symbol
load_ui_chart_data = getattr(market_data, "load_ui_chart_data", market_data.load_live_chart_data)


def render_header() -> None:
    st.title("Shinobu Project")
    st.caption("실전 5분봉 자동매매")


def init_live_chart_state() -> None:
    if LIVE_CHART_STATE_KEY not in st.session_state:
        st.session_state[LIVE_CHART_STATE_KEY] = {"started_at": "", "frames": {}}
    if LIVE_FIGURE_STATE_KEY not in st.session_state:
        st.session_state[LIVE_FIGURE_STATE_KEY] = {}


@st.cache_data(ttl=5, show_spinner=False)
def get_cached_raw_frame(symbol: str, timeframe_label: str) -> pd.DataFrame:
    return load_ui_chart_data(symbol, timeframe_label)


@st.cache_data(ttl=5, show_spinner=False)
def get_cached_strategy_frame(symbol: str, timeframe_label: str, stoch_pct: int, cci_pct: int, rsi_pct: int) -> pd.DataFrame:
    adjustments = StrategyAdjustments(stoch_pct=stoch_pct, cci_pct=cci_pct, rsi_pct=rsi_pct)
    raw = get_cached_raw_frame(symbol, timeframe_label)
    return calculate_scr_strategy(raw, adjustments, timeframe_label)


def filter_frame_from_live_start(frame: pd.DataFrame) -> pd.DataFrame:
    started_at = get_live_started_at()
    if started_at is None:
        return frame.iloc[0:0].copy()

    before = frame.loc[frame.index < started_at].tail(MAX_LIVE_CHART_CANDLES)
    after = frame.loc[frame.index >= started_at]
    combined = pd.concat([before, after]).sort_index()
    if combined.empty and not frame.empty:
        return frame.tail(MAX_LIVE_CHART_CANDLES).copy()
    return combined.tail(MAX_LIVE_CHART_CANDLES).copy()


def _empty_live_frame(template: pd.DataFrame | None = None) -> pd.DataFrame:
    if template is None:
        return pd.DataFrame()
    return template.iloc[0:0].copy()


def _merge_live_frame(cache_frame: pd.DataFrame, latest_frame: pd.DataFrame) -> pd.DataFrame:
    if latest_frame.empty:
        return cache_frame
    if cache_frame.empty:
        return latest_frame.tail(MAX_LIVE_CHART_CANDLES).copy()

    last_index = cache_frame.index.max()
    appended = latest_frame.loc[latest_frame.index > last_index]
    if appended.empty:
        return cache_frame.tail(MAX_LIVE_CHART_CANDLES).copy()

    merged = pd.concat([cache_frame, appended]).sort_index()
    merged = merged[~merged.index.duplicated(keep="last")]
    return merged.tail(MAX_LIVE_CHART_CANDLES).copy()


def get_live_chart_frame(symbol: str, adjustments: StrategyAdjustments) -> pd.DataFrame:
    init_live_chart_state()
    started_at = get_live_started_at()
    state = st.session_state[LIVE_CHART_STATE_KEY]
    started_key = started_at.isoformat() if started_at is not None else ""

    if state.get("started_at") != started_key:
        state["started_at"] = started_key
        state["frames"] = {}

    latest_frame = get_cached_strategy_frame(
        symbol,
        LIVE_TIMEFRAME,
        adjustments.stoch_pct,
        adjustments.cci_pct,
        adjustments.rsi_pct,
    )
    latest_frame = filter_frame_from_live_start(latest_frame)

    if started_at is None:
        return _empty_live_frame(latest_frame)

    frames = state["frames"]
    cache_frame = frames.get(symbol)
    if cache_frame is None:
        frames[symbol] = latest_frame.tail(MAX_LIVE_CHART_CANDLES).copy()
    else:
        frames[symbol] = _merge_live_frame(cache_frame, latest_frame)
    return frames[symbol]


def get_preview_chart_frame(symbol: str, adjustments: StrategyAdjustments) -> pd.DataFrame:
    frame = get_cached_strategy_frame(
        symbol,
        LIVE_TIMEFRAME,
        adjustments.stoch_pct,
        adjustments.cci_pct,
        adjustments.rsi_pct,
    )
    return frame.tail(MAX_LIVE_CHART_CANDLES).copy()


def get_preview_raw_chart_frame(symbol: str) -> pd.DataFrame:
    frame = get_cached_raw_frame(symbol, LIVE_TIMEFRAME)
    return frame.tail(MAX_LIVE_CHART_CANDLES).copy()


def get_live_raw_chart_frame(symbol: str) -> pd.DataFrame:
    frame = get_cached_raw_frame(symbol, LIVE_TIMEFRAME)
    return filter_frame_from_live_start(frame)


def _get_chart_figure(
    chart_kind: str,
    frame: pd.DataFrame,
    symbol: str,
    pair_symbol: str | None,
    pair_frame: pd.DataFrame | None,
) -> go.Figure:
    init_live_chart_state()
    state = st.session_state[LIVE_FIGURE_STATE_KEY]
    include_scr_panel = "scr_line" in frame.columns
    figure_key = f"{chart_kind}:{symbol}:{pair_symbol or ''}:{include_scr_panel}"
    symbol_name = display_name(symbol)
    pair_name = display_name(pair_symbol) if pair_symbol else None

    cached_figure = state.get(figure_key)
    if cached_figure is None:
        cached_figure = build_candlestick_chart(
            frame,
            LIVE_TIMEFRAME,
            symbol_name,
            symbol,
            pair_frame=pair_frame,
            pair_name=pair_name,
            pair_symbol_code=pair_symbol,
        )
        state[figure_key] = cached_figure
        return cached_figure

    return update_candlestick_chart(
        cached_figure,
        frame,
        LIVE_TIMEFRAME,
        symbol_name,
        symbol,
        pair_frame=pair_frame,
        pair_name=pair_name,
        pair_symbol_code=pair_symbol,
    )


def _add_live_order_markers(figure: go.Figure, order_frame: pd.DataFrame, price_frame: pd.DataFrame) -> None:
    if order_frame.empty or price_frame.empty:
        return

    aligned = price_frame.reindex(order_frame["candle_time"]).ffill()
    if aligned.empty:
        return

    working = order_frame.copy()
    x_positions = pd.Series(range(len(price_frame)), index=price_frame.index)
    y_values = []
    for (_, order), (_, candle) in zip(working.iterrows(), aligned.iterrows(), strict=False):
        y_values.append(float(candle["Low"]) * 0.985 if order["side"] == "buy" else float(candle["High"]) * 1.015)

    working["x"] = x_positions.reindex(working["candle_time"]).tolist()
    working["y"] = y_values
    working = working[working["x"].notna()].copy()
    if working.empty:
        return

    color_map = {"buy": "#3b82f6", "sell": "#ef4444"}
    label_map = {"buy": "실제 매수", "sell": "실제 매도"}
    for (side, order_symbol), group in working.groupby(["side", "symbol"]):
        color = color_map.get(side, "#9aa4b2")
        label = label_map.get(side, side)
        figure.add_trace(
            go.Scatter(
                x=group["x"],
                y=group["y"],
                mode="markers+text",
                marker={"symbol": "heart", "size": 15, "color": color, "line": {"width": 1, "color": "#ffffff"}},
                text=[f"{label} · {display_name(order_symbol)}" for _ in range(len(group))],
                textposition="top center",
                textfont={"size": 10, "color": color},
                hovertemplate="%{text}<extra></extra>",
                name=f"{label} {display_name(order_symbol)}",
            ),
            row=1,
            col=1,
        )


def render_live_trade_header(symbol: str, pair_symbol: str | None) -> None:
    if pair_symbol is None:
        st.subheader(f"실전 매매 차트 · {display_name(symbol)}")
        return
    st.subheader(f"실전 매매 차트 · {display_name(symbol)} / {display_name(pair_symbol)}")


def mask_account_number(account_number: str) -> str:
    if not account_number:
        return ""
    return f"{account_number[:2]}{'*' * max(len(account_number) - 2, 0)}"


def _format_positions_frame(positions: pd.DataFrame) -> pd.DataFrame:
    if positions.empty:
        return positions

    display_columns = ["code", "name", "quantity", "avg_price", "current_price", "eval_amount", "profit_amount", "profit_rate"]
    view = positions.loc[:, [column for column in display_columns if column in positions.columns]].copy()
    view = view.rename(
        columns={
            "code": "종목코드",
            "name": "종목명",
            "quantity": "보유수량",
            "avg_price": "평균단가",
            "current_price": "현재가",
            "eval_amount": "평가금액",
            "profit_amount": "평가손익",
            "profit_rate": "수익률(%)",
        }
    )
    for column in ["보유수량", "평균단가", "현재가", "평가금액", "평가손익"]:
        if column in view.columns:
            view[column] = view[column].map(lambda value: f"{float(value):,.0f}")
    if "수익률(%)" in view.columns:
        view["수익률(%)"] = view["수익률(%)"].map(lambda value: f"{float(value):+.2f}")
    return view


def _format_five_min_bucket_label(timestamp: pd.Timestamp) -> str:
    start = pd.Timestamp(timestamp).floor("5min")
    end = start + pd.Timedelta(minutes=5)
    return f"{start.strftime('%m-%d %H:%M')}~{end.strftime('%H:%M')}"


def _group_execution_ledger_by_5m(executions: pd.DataFrame) -> pd.DataFrame:
    if executions.empty:
        return executions

    ledger = executions.copy()
    ledger["bucket_start"] = pd.to_datetime(ledger["timestamp"]).dt.floor("5min")
    grouped = (
        ledger.groupby(["bucket_start", "symbol", "name", "side"], as_index=False)
        .agg(
            quantity=("quantity", "sum"),
            amount=("amount", "sum"),
            order_count=("order_no", "nunique"),
        )
    )
    grouped["price"] = grouped["amount"] / grouped["quantity"]
    grouped["time_range"] = grouped["bucket_start"].map(_format_five_min_bucket_label)
    return grouped.sort_values("bucket_start", ascending=False).reset_index(drop=True)


def _account_return_rate(summary: dict) -> float:
    purchase_amount = float(summary.get("매입금액", 0) or 0)
    profit = float(summary.get("평가손익", 0) or 0)
    if purchase_amount <= 0:
        return 0.0
    return (profit / purchase_amount) * 100


def _emotion_image_path(image_path: Path, fallback_path: Path) -> Path | None:
    if image_path.exists():
        return image_path
    if fallback_path.exists():
        return fallback_path
    return None


@st.cache_data(ttl=3600, show_spinner=False)
def _get_thumbnail_base64(image_path: str, max_width: int = 280, max_height: int = 320) -> str:
    path = Path(image_path)
    with Image.open(path) as image:
        converted = image.convert("RGB")
        converted.thumbnail((max_width, max_height))
        from io import BytesIO

        buffer = BytesIO()
        converted.save(buffer, format="JPEG", quality=72, optimize=True)
        return base64.b64encode(buffer.getvalue()).decode("ascii")


def _render_emotion_card(title: str, caption: str, image_path: Path, fallback_path: Path, highlighted: bool, tone: str) -> None:
    border = "#2a2e39"
    background = "#131722"
    accent = "#e5e7eb"
    if tone == "negative":
        accent = "#e5e7eb"
    if highlighted:
        accent = "#3b82f6" if tone == "positive" else "#ef4444"

    caption_size = "30px" if highlighted else "22px"
    caption_weight = "900" if highlighted else "700"
    st.markdown(
        f"""
        <div style="border:2px solid {border};background:{background};border-radius:14px;padding:10px 10px 6px 10px;">
            <div style="font-size:13px;color:#e5e7eb;font-weight:700;margin-bottom:4px;">{title}</div>
            <div style="font-size:{caption_size};font-weight:{caption_weight};color:{accent};margin-bottom:10px;text-align:center;">{caption}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    card_id = f"emotion-{tone}"
    st.markdown(
        f"""
        <style>
        #{card_id} {{
            width: 100%;
            aspect-ratio: 4 / 4.6;
            max-height: 260px;
            border-radius: 12px;
            overflow: hidden;
            margin-top: 8px;
            background: #0f1420;
            display: flex;
            align-items: center;
            justify-content: center;
        }}
        #{card_id} img, #{card_id} svg {{
            width: 100%;
            height: 100%;
            object-fit: contain;
            display: block;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )

    resolved = _emotion_image_path(image_path, fallback_path)
    if resolved:
        if resolved.suffix.lower() == ".svg":
            svg_text = resolved.read_text(encoding="utf-8")
            st.markdown(f'<div id="{card_id}">{svg_text}</div>', unsafe_allow_html=True)
        else:
            image_base64 = _get_thumbnail_base64(str(resolved))
            st.markdown(
                f'<div id="{card_id}"><img src="data:image/jpeg;base64,{image_base64}"></div>',
                unsafe_allow_html=True,
            )
        return

    fallback_symbol = ":-)" if tone == "positive" else ">:("
    st.markdown(
        f"""
        <div id="{card_id}" style="border:1px dashed {border};color:#d1d4dc;font-size:42px;">
            {fallback_symbol}
        </div>
        """,
        unsafe_allow_html=True,
    )


def _emotion_by_position(positions: pd.DataFrame) -> str:
    if positions.empty:
        return "neutral"

    code_column = "code" if "code" in positions.columns else None
    if code_column is None:
        return "neutral"

    codes = positions[code_column].astype(str).tolist()
    if "122630" in codes or "069500" in codes:
        return "positive"
    if "252670" in codes or "114800" in codes:
        return "negative"
    return "neutral"
def _extract_total_assets(summary: dict) -> float:
    for key in ["total_assets", "총자산"]:
        if key in summary:
            try:
                return float(summary.get(key) or 0)
            except (TypeError, ValueError):
                return 0.0
    return 0.0
def _build_asset_history_figure() -> go.Figure | None:
    history = get_asset_history()
    if not history:
        return None

    frame = pd.DataFrame(history)
    if frame.empty or "timestamp" not in frame.columns or "total_assets" not in frame.columns:
        return None

    frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="coerce")
    frame["total_assets"] = pd.to_numeric(frame["total_assets"], errors="coerce")
    frame = frame.dropna().tail(80)
    if frame.empty:
        return None

    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            x=frame["timestamp"],
            y=frame["total_assets"],
            mode="lines",
            line={"color": "#f59e0b", "width": 2},
            fill="tozeroy",
            fillcolor="rgba(245,158,11,0.12)",
            hovertemplate="%{x|%m-%d %H:%M}<br>총자산 %{y:,.0f}원<extra></extra>",
            name="자산 추이",
        )
    )
    figure.update_layout(
        height=318,
        margin={"l": 26, "r": 26, "t": 52, "b": 26},
        paper_bgcolor="#131722",
        plot_bgcolor="#131722",
        font={"color": "#d1d4dc", "family": "Malgun Gothic"},
        showlegend=False,
        title={"text": "자산 상승 그래프", "x": 0.02, "font": {"size": 13}},
    )
    figure.update_xaxes(showgrid=False, tickfont={"size": 10, "color": "#9aa4b2"}, automargin=True)
    figure.update_yaxes(
        side="right",
        showgrid=True,
        gridcolor="rgba(42,46,57,0.35)",
        tickformat=",.0f",
        tickfont={"size": 10, "color": "#9aa4b2"},
        automargin=True,
    )
    return figure


@st.cache_data(ttl=60, show_spinner=False)
def get_live_trade_history(lookback_days: int = 7) -> pd.DataFrame:
    history_window_start = pd.Timestamp.now(tz=None).normalize() - pd.Timedelta(days=max(int(lookback_days), 1) - 1)
    fetch_start = history_window_start
    start_date = fetch_start.strftime("%Y%m%d")
    end_date = pd.Timestamp.now(tz=None).strftime("%Y%m%d")
    frames = []
    for symbol in ["069500.KS", "114800.KS"]:
        frame = fetch_domestic_daily_ccld(start_date, end_date, symbol=symbol, max_pages=2)
        if not frame.empty:
            frames.append(frame)
    executions = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if executions.empty:
        return executions

    executions = executions.copy()
    executions["timestamp"] = pd.to_datetime(executions["timestamp"], errors="coerce")
    executions = executions.dropna(subset=["timestamp"])
    dedupe_keys = [column for column in ["symbol", "side", "quantity", "price", "timestamp", "order_no"] if column in executions.columns]
    if dedupe_keys:
        executions = executions.drop_duplicates(subset=dedupe_keys, keep="first")
    executions = executions.loc[executions["symbol"].isin(["069500.KS", "114800.KS"])].sort_values("timestamp")
    if executions.empty:
        return pd.DataFrame()

    trades: list[dict[str, object]] = []
    open_lots: dict[str, dict[str, object]] = {}

    for execution in executions.itertuples(index=False):
        symbol = str(execution.symbol)
        side = str(execution.side)
        quantity = float(execution.quantity)
        price = float(execution.price)
        name = str(execution.name or display_name(symbol))
        timestamp = pd.Timestamp(execution.timestamp)

        if side == "buy":
            lot = open_lots.get(symbol)
            if lot is None:
                open_lots[symbol] = {
                    "symbol": symbol,
                    "name": name,
                    "entry_time": timestamp,
                    "entry_qty": quantity,
                    "entry_amount": quantity * price,
                }
            else:
                lot["entry_qty"] = float(lot["entry_qty"]) + quantity
                lot["entry_amount"] = float(lot["entry_amount"]) + (quantity * price)
            continue

        lot = open_lots.get(symbol)
        if lot is None:
            continue

        entry_qty = float(lot["entry_qty"])
        entry_amount = float(lot["entry_amount"])
        matched_qty = min(entry_qty, quantity)
        if matched_qty <= 0:
            continue

        entry_avg = entry_amount / entry_qty if entry_qty > 0 else 0.0
        exit_amount = matched_qty * price
        pnl_amount = exit_amount - (matched_qty * entry_avg)
        pnl_rate = (pnl_amount / (matched_qty * entry_avg) * 100.0) if entry_avg > 0 else 0.0
        trades.append(
            {
                "symbol": symbol,
                "name": name,
                "entry_time": pd.Timestamp(lot["entry_time"]),
                "exit_time": timestamp,
                "quantity": matched_qty,
                "entry_price": entry_avg,
                "exit_price": price,
                "pnl_amount": pnl_amount,
                "pnl_rate": pnl_rate,
                "result": "승" if pnl_amount > 0 else "패" if pnl_amount < 0 else "보합",
            }
        )

        remaining_qty = entry_qty - matched_qty
        if remaining_qty > 0:
            open_lots[symbol] = {
                "symbol": symbol,
                "name": name,
                "entry_time": lot["entry_time"],
                "entry_qty": remaining_qty,
                "entry_amount": remaining_qty * entry_avg,
            }
        else:
            del open_lots[symbol]

    history = pd.DataFrame(trades)
    if history.empty:
        return history
    history["exit_time"] = pd.to_datetime(history["exit_time"], errors="coerce")
    history = history.loc[history["exit_time"] >= history_window_start]
    return history.reset_index(drop=True)


@st.cache_data(ttl=60, show_spinner=False)
def get_recent_execution_ledger(lookback_days: int = 7) -> pd.DataFrame:
    start = (pd.Timestamp.now(tz=None).normalize() - pd.Timedelta(days=max(int(lookback_days), 1) - 1)).strftime("%Y%m%d")
    end = pd.Timestamp.now(tz=None).strftime("%Y%m%d")
    frames = []
    for symbol in ["069500.KS", "114800.KS"]:
        frame = fetch_domestic_daily_ccld(start, end, symbol=symbol, max_pages=2)
        if not frame.empty:
            frames.append(frame)
    executions = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if executions.empty:
        return executions
    executions = executions.copy()
    executions = executions.loc[executions["symbol"].isin(["069500.KS", "114800.KS"])]
    if executions.empty:
        return executions
    executions["timestamp"] = pd.to_datetime(executions["timestamp"], errors="coerce")
    dedupe_keys = [column for column in ["symbol", "side", "quantity", "price", "timestamp", "order_no"] if column in executions.columns]
    if dedupe_keys:
        executions = executions.drop_duplicates(subset=dedupe_keys, keep="first")
    executions = executions.dropna(subset=["timestamp"]).sort_values("timestamp", ascending=False)
    return executions.reset_index(drop=True)


def _render_open_live_positions() -> None:
    try:
        current_positions, _ = fetch_domestic_balance()
    except Exception:
        current_positions = pd.DataFrame()

    trade_codes = {"069500", "114800"}
    if not current_positions.empty and "code" in current_positions.columns:
        open_view = current_positions[current_positions["code"].astype(str).isin(trade_codes)].copy()
    else:
        open_view = pd.DataFrame()

    st.markdown("##### 미청산 주문 / 보유중")
    if open_view.empty:
        st.caption("현재 보유 중인 실전 포지션이 없습니다.")
    else:
        st.dataframe(_format_positions_frame(open_view), use_container_width=True, hide_index=True)


def _render_closed_live_trades() -> None:
    st.markdown("##### 청산 완료 거래")
    try:
        history = get_live_trade_history(7)
        executions = get_recent_execution_ledger(7)
    except KisApiError as exc:
        st.caption(f"거래내역 조회 오류: {exc}")
        return
    except Exception as exc:
        st.caption(f"거래내역 집계 오류: {exc}")
        return
    if history.empty:
        st.caption("최근 7일 기준으로 집계된 청산 완료 거래가 없습니다.")
    else:
        wins = int((history["pnl_amount"] > 0).sum())
        draws = int((history["pnl_amount"] == 0).sum())
        losses = int((history["pnl_amount"] < 0).sum())
        total = len(history)
        win_rate = (wins / total * 100.0) if total else 0.0
        realized = float(history["pnl_amount"].sum())

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("거래수", f"{total}")
        col2.metric("승/무/패", f"{wins}/{draws}/{losses}")
        col3.metric("승률", f"{win_rate:.1f}%")
        col4.metric("실현손익", f"{realized:,.0f}원")

        view = history.sort_values("exit_time", ascending=False).copy()
        view["진입구간"] = pd.to_datetime(view["entry_time"]).map(_format_five_min_bucket_label)
        view["청산구간"] = pd.to_datetime(view["exit_time"]).map(_format_five_min_bucket_label)
        view["entry_time"] = pd.to_datetime(view["entry_time"]).dt.strftime("%m-%d %H:%M")
        view["exit_time"] = pd.to_datetime(view["exit_time"]).dt.strftime("%m-%d %H:%M")
        view["quantity"] = view["quantity"].map(lambda value: f"{float(value):,.0f}")
        view["entry_price"] = view["entry_price"].map(lambda value: f"{float(value):,.0f}")
        view["exit_price"] = view["exit_price"].map(lambda value: f"{float(value):,.0f}")
        view["pnl_amount"] = view["pnl_amount"].map(lambda value: f"{float(value):+,.0f}")
        view["pnl_rate"] = view["pnl_rate"].map(lambda value: f"{float(value):+.2f}%")
        view = view.rename(
            columns={
                "name": "종목",
                "entry_time": "진입",
                "exit_time": "청산",
                "quantity": "수량",
                "entry_price": "진입가",
                "exit_price": "청산가",
                "pnl_amount": "손익",
                "pnl_rate": "수익률",
                "result": "결과",
            }
        )
        st.dataframe(
            view[["종목", "진입구간", "청산구간", "수량", "진입가", "청산가", "손익", "수익률", "결과"]].head(20),
            use_container_width=True,
            hide_index=True,
        )

    if executions.empty:
        st.caption("최근 7일 체결 원장도 비어 있습니다.")
        return

    st.markdown("###### 최근 7일 체결 원장")
    ledger = _group_execution_ledger_by_5m(executions)
    ledger = ledger.loc[:, [column for column in ["time_range", "name", "side", "quantity", "price", "amount", "order_count"] if column in ledger.columns]].copy()
    if "side" in ledger.columns:
        ledger["side"] = ledger["side"].map({"buy": "매수", "sell": "매도"}).fillna(ledger["side"])
    for column in ["quantity", "price", "amount", "order_count"]:
        if column in ledger.columns:
            ledger[column] = ledger[column].map(lambda value: f"{float(value):,.0f}")
    ledger = ledger.rename(
        columns={
            "time_range": "구간",
            "name": "종목",
            "side": "구분",
            "quantity": "수량",
            "price": "가격",
            "amount": "금액",
            "order_count": "주문건수",
        }
    )
    st.dataframe(
        ledger.head(20),
        use_container_width=True,
        hide_index=True,
    )


def render_emotion_panel(positions: pd.DataFrame, summary: dict) -> None:
    total_assets = _extract_total_assets(summary)
    if total_assets > 0:
        record_asset_snapshot(total_assets)

    emotion_state = _emotion_by_position(positions)
    positive = emotion_state == "positive"
    negative = emotion_state == "negative"

    left, right = st.columns([1.35, 1], vertical_alignment="top")
    with left:
        emotion_left, emotion_right = st.columns(2)
        with emotion_left:
            _render_emotion_card(
                "",
                "롱이다!!!!!!!!!!",
                POSITIVE_IMAGE_PATH,
                POSITIVE_FALLBACK_PATH,
                positive,
                "positive",
            )
        with emotion_right:
            _render_emotion_card(
                "",
                "숏이다!!!!!!!!!!",
                NEGATIVE_IMAGE_PATH,
                NEGATIVE_FALLBACK_PATH,
                negative,
                "negative",
            )
    with right:
        figure = _build_asset_history_figure()
        if figure is None:
            st.info("아직 자산 이력이 충분하지 않습니다.")
        else:
            st.plotly_chart(figure, use_container_width=True, theme=None, config={"displaylogo": False})
@st.fragment(run_every="10s")
def render_live_account_panel() -> None:
    panel = st.empty()
    with panel.container():
        st.markdown("#### 실계좌")
        if not has_kis_account():
            st.info("한투 계좌 정보가 없어 계좌 화면을 표시할 수 없습니다.")
            return

        try:
            with st.spinner("계좌 정보를 불러오는 중..."):
                positions, summary = fetch_domestic_balance()
        except KisApiError as exc:
            st.warning(f"계좌 조회 오류: {exc}")
            return
        except Exception as exc:
            st.warning(f"계좌 조회 중 알 수 없는 오류가 발생했습니다: {exc}")
            return

        purchase_amount = float(summary.get("purchase_amount", 0) or 0)
        profit_amount = float(summary.get("profit_amount", 0) or 0)
        profit_rate = (profit_amount / purchase_amount * 100.0) if purchase_amount > 0 else 0.0

        col1, col2 = st.columns(2)
        col1.metric("총자산", f"{summary.get('total_assets', 0):,.0f}원")
        col2.metric("잔고", f"{summary.get('orderable_cash', 0):,.0f}원")
        col3, col4 = st.columns(2)
        col3.metric("평가손익", f"{profit_amount:,.0f}원")
        col4.metric("수익률", f"{profit_rate:+.2f}%")
        st.caption(f"계좌 {mask_account_number(summary.get('account_number', ''))}")

        st.markdown("##### 보유종목")
        if positions.empty:
            st.info("현재 보유 포지션이 없습니다.")
        else:
            st.dataframe(_format_positions_frame(positions), use_container_width=True, hide_index=True)


@st.fragment(run_every="10s")
def render_live_trade_history_panel() -> None:
    panel = st.empty()
    with panel.container():
        st.markdown("#### 실전 거래 내역")
        _render_open_live_positions()


@st.fragment(run_every="60s")
def render_closed_live_trade_history_panel() -> None:
    panel = st.empty()
    with panel.container():
        _render_closed_live_trades()


@st.fragment(run_every="10s")
def render_emotion_section() -> None:
    try:
        positions, summary = fetch_domestic_balance()
    except Exception:
        return
    render_emotion_panel(positions, summary)
@st.fragment(run_every="5s")
def render_live_trade_chart(symbol: str, pair_symbol: str | None, adjustments: StrategyAdjustments) -> None:
    components.html(
        build_live_chart_html(
            server_url=ensure_chart_server(),
            symbol=symbol,
            pair_symbol=pair_symbol,
            stoch_pct=adjustments.stoch_pct,
            cci_pct=adjustments.cci_pct,
            rsi_pct=adjustments.rsi_pct,
        ),
        height=640,
    )
    return

    server_url = ensure_chart_server()
    pair_query = pair_symbol or ""
    component_key = f"live-chart-{symbol}-{pair_query}-{adjustments.stoch_pct}-{adjustments.cci_pct}-{adjustments.rsi_pct}"
    html = f"""
    <div id="chart-root" style="width:100%;height:560px;background:#131722;border:1px solid #2a2e39;border-radius:12px;"></div>
    <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
    <script>
    const root = document.getElementById("chart-root");
    const endpoint = "{server_url}/chart?kind=overlay&symbol={symbol}&pair_symbol={pair_query}&stoch_pct={adjustments.stoch_pct}&cci_pct={adjustments.cci_pct}&rsi_pct={adjustments.rsi_pct}";
    let initialized = false;

    function markerTrace(markers, color, symbol, axisSuffix = "") {{
      const indicatorMode = axisSuffix === "2";
      return {{
        type: "scatter",
        mode: indicatorMode ? "markers" : "markers+text",
        x: markers.map((item) => item.x),
        y: markers.map((item) => item.y),
        text: indicatorMode ? [] : markers.map((item) => item.label),
        textposition: "top center",
        textfont: {{ size: 10, color }},
        marker: {{ color, size: indicatorMode ? 7 : 10, symbol, opacity: indicatorMode ? 0.42 : 1, line: {{ color: "#ffffff", width: 1 }} }},
        hoverinfo: "text",
        hovertext: markers.map((item) => item.label),
        xaxis: axisSuffix ? `x${{axisSuffix}}` : "x",
        yaxis: axisSuffix ? `y${{axisSuffix}}` : "y",
        showlegend: false
      }};
    }}

    function buildFigure(data) {{
      const x = data.candles.map((_, index) => index);
      const step = Math.max(1, Math.ceil(x.length / 8));
      const tickvals = x.filter((_, index) => index % step === 0 || index === x.length - 1);
      const ticktext = data.tickText.filter((_, index) => index % step === 0 || index === x.length - 1);

      return {{
        data: [
          {{
            type: "candlestick",
            x,
            open: data.candles.map((item) => item.o),
            high: data.candles.map((item) => item.h),
            low: data.candles.map((item) => item.l),
            close: data.candles.map((item) => item.c),
            increasing: {{ line: {{ color: "#089981" }}, fillcolor: "#089981" }},
            decreasing: {{ line: {{ color: "#f23645" }}, fillcolor: "#f23645" }},
            xaxis: "x",
            yaxis: "y",
            hovertemplate: "시가 %{{open:,.0f}}<br>고가 %{{high:,.0f}}<br>저가 %{{low:,.0f}}<br>종가 %{{close:,.0f}}<extra></extra>",
            showlegend: false
          }},
          markerTrace(data.signals.primaryOpenMain || [], "#3b82f6", "circle"),
          markerTrace(data.signals.primaryCloseMain || [], "#ef4444", "circle"),
          markerTrace(data.signals.pairOpenMain || [], "#3b82f6", "star"),
          markerTrace(data.signals.pairCloseMain || [], "#ef4444", "star"),
          markerTrace(data.orders || [], "#f59e0b", "diamond"),
          {{
            type: "scatter", mode: "lines", x, y: data.scr || [], xaxis: "x2", yaxis: "y2",
            line: {{ color: "#ffffff", width: 3.1, dash: "solid" }}, showlegend: false,
            hovertemplate: `${{data.symbolName}} SCR %{{y:.2f}}<extra></extra>`
          }},
          {{
            type: "scatter", mode: "lines", x, y: data.pairScr || [], xaxis: "x2", yaxis: "y2",
            line: {{ color: "#f59e0b", width: 2.5, dash: "dot" }}, showlegend: false,
            hovertemplate: `${{data.pairName || "곱버스"}} SCR %{{y:.2f}}<extra></extra>`
          }},
          markerTrace(data.signals.primaryOpenIndicator || [], "#3b82f6", "circle", "2"),
          markerTrace(data.signals.primaryCloseIndicator || [], "#ef4444", "circle", "2"),
          markerTrace(data.signals.pairOpenIndicator || [], "#3b82f6", "star", "2"),
          markerTrace(data.signals.pairCloseIndicator || [], "#ef4444", "star", "2")
        ],
        layout: {{
          paper_bgcolor: "#131722",
          plot_bgcolor: "#131722",
          font: {{ color: "#d1d4dc", family: "Malgun Gothic" }},
          margin: {{ l: 8, r: 56, t: 42, b: 18 }},
          height: 560,
          dragmode: false,
          hovermode: "x unified",
          hoverdistance: 30,
          spikedistance: 30,
          hoverlabel: {{ bgcolor: "#1e222d", font: {{ color: "#d1d4dc" }} }},
          showlegend: false,
          uirevision: "shinobu-live-chart",
          xaxis: {{ domain: [0, 1], anchor: "y", showgrid: false, showticklabels: false, range: [-0.45, Math.max(x.length - 0.55, 1)], fixedrange: true, showspikes: true, spikemode: "across", spikecolor: "#4b5563", spikethickness: 1 }},
          yaxis: {{ domain: [0.31, 1], side: "right", showgrid: true, gridcolor: "rgba(42,46,57,0.65)", fixedrange: true }},
          xaxis2: {{ domain: [0, 1], anchor: "y2", tickmode: "array", tickvals, ticktext, showgrid: false, range: [-0.45, Math.max(x.length - 0.55, 1)], fixedrange: true, showspikes: true, spikemode: "across", spikecolor: "#4b5563", spikethickness: 1 }},
          yaxis2: {{ domain: [0, 0.24], side: "right", range: [-1.6, 1.6], tickmode: "array", tickvals: [-1, 0, 1], ticktext: ["하단", "0", "상단"], showgrid: true, gridcolor: "rgba(42,46,57,0.35)" }},
          annotations: [
            {{ x: 0.01, y: 1.04, xref: "paper", yref: "paper", showarrow: false, text: `${{data.symbolName}} · 5분봉 · 실전`, font: {{ size: 14, color: "#e5e7eb", family: "Malgun Gothic" }} }},
            {{ x: 0.01, y: 0.27, xref: "paper", yref: "paper", showarrow: false, text: "보조지표 (흰색 점선: 레버리지 / 주황 점선: 곱버스)", font: {{ size: 12, color: "#9aa4b2", family: "Malgun Gothic" }} }}
          ]
        }}
      }};
    }}

    async function refreshChart() {{
      const response = await fetch(endpoint, {{ cache: "no-store" }});
      if (!response.ok) throw new Error(`HTTP ${{response.status}}`);
      const payload = await response.json();
      const figure = buildFigure(payload);
      const config = {{ responsive: true, displaylogo: false, displayModeBar: false, scrollZoom: false, modeBarButtonsToRemove: ["zoom2d", "pan2d", "lasso2d", "select2d", "zoomIn2d", "zoomOut2d", "autoScale2d", "resetScale2d"] }};
      if (!initialized) {{
        await Plotly.newPlot(root, figure.data, figure.layout, config);
        initialized = true;
      }} else {{
        await Plotly.react(root, figure.data, figure.layout, config);
      }}
    }}

    refreshChart();
    setInterval(refreshChart, 5000);
    </script>
    """
    components.html(html, height=580)
@st.fragment(run_every="5s")
def run_live_engine(loaded_symbol: str, pair_symbol: str | None, adjustments: StrategyAdjustments) -> None:
    if not is_live_enabled() or pair_symbol is None:
        return

    try:
        process_live_trading_cycle(loaded_symbol, pair_symbol, adjustments)
    except KisApiError:
        return
    except Exception:
        return


def _handle_live_start() -> None:
    set_live_enabled(True)


def _handle_live_stop() -> None:
    set_live_enabled(False)


def render_live_trading_panel(loaded_symbol: str, pair_symbol: str | None, adjustments: StrategyAdjustments) -> None:
    st.markdown("#### 실전 투자")
    left_button, right_button = st.columns(2)
    with left_button:
        st.button(
            "실행",
            use_container_width=True,
            key="live_start_button",
            disabled=pair_symbol is None,
            on_click=_handle_live_start,
        )
    with right_button:
        st.button(
            "중지",
            use_container_width=True,
            key="live_stop_button",
            on_click=_handle_live_stop,
        )

    if pair_symbol is None:
        st.warning("실전 투자는 레버리지/인버스 페어 종목에서만 실행됩니다.")

    enabled = is_live_enabled()
    status_text = "실행 중" if enabled else "중지됨"
    status_color = "#3b82f6" if enabled else "#9aa4b2"
    st.markdown(
        f"""
        <div style="margin-bottom:10px;">
            <span style="display:inline-block;padding:5px 10px;border-radius:999px;background:{status_color}22;color:{status_color};font-size:12px;">
                상태: {status_text}
            </span>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.caption("실전 주문은 5분봉 기준으로만 처리하고, 5초마다 최신 완료 봉을 확인합니다. 매수 시 주문가능현금을 최대한 사용합니다.")

    runtime = get_live_runtime_state()
    status_name = {
        "running": "실행 중",
        "stopped": "중지됨",
        "checking": "봉 확인 중",
        "waiting_data": "데이터 대기",
        "idle": "신호 대기",
        "holding": "보유 유지",
        "ordered": "주문 완료",
        "waiting_cash": "주문 가능 금액 대기",
        "error": "오류",
    }.get(runtime["last_status"], runtime["last_status"] or "-")
    st.markdown("##### 엔진 상태")
    info_left, info_right = st.columns(2)
    info_left.caption(f"마지막 확인: {runtime['last_cycle_at'] or '-'}")
    info_right.caption(f"마지막 주문: {runtime['last_order_at'] or '-'}")
    st.caption(f"마지막 완료 봉: {runtime['last_checked_candle'] or '-'}")
    st.caption(f"엔진 상태: {status_name}")
    if runtime["last_error"]:
        st.warning(runtime["last_error"])

    if enabled and pair_symbol is None:
        st.warning("현재 종목은 실전 페어 전략 대상이 아닙니다.")

    logs = get_live_logs()
    log_html = "".join(
        f'<div style="padding:10px 0;border-top:1px solid #1e222d;color:#d1d4dc;font-size:14px;">{message}</div>'
        for message in logs
    )
    if not log_html:
        log_html = '<div style="padding:10px 0;color:#9aa4b2;font-size:14px;">실전 매매 로그가 아직 없습니다.</div>'

    st.markdown(
        f"""
        <div style="background:#131722;border:1px solid #2a2e39;border-radius:12px;padding:14px;height:280px;overflow-y:auto;">
            <div style="font-size:13px;color:#9aa4b2;margin-bottom:12px;">실전 매매 로그</div>
            {log_html}
        </div>
        """,
        unsafe_allow_html=True,
    )
def main() -> None:
    init_live_state()
    init_live_chart_state()
    loaded_symbol = PRIMARY_SYMBOL
    adjustments = StrategyAdjustments(stoch_pct=0, cci_pct=0, rsi_pct=0)

    render_header()
    pair_symbol = get_pair_symbol(loaded_symbol)

    left, right = st.columns([2.2, 1], vertical_alignment="top")
    with right:
        render_live_account_panel()
        run_live_engine(loaded_symbol, pair_symbol, adjustments)
        render_live_trading_panel(loaded_symbol, pair_symbol, adjustments)
    with left:
        render_live_trade_header(loaded_symbol, pair_symbol)
        chart_slot = st.empty()
        emotion_slot = st.empty()
        history_slot = st.empty()
        with emotion_slot.container():
            render_emotion_section()
        with chart_slot.container():
            render_live_trade_chart(loaded_symbol, pair_symbol, adjustments)
        with history_slot.container():
            st.markdown("---")
            render_live_trade_history_panel()
            render_closed_live_trade_history_panel()


if __name__ == "__main__":
    main()
