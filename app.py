from __future__ import annotations

import base64
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from PIL import Image

from config import has_kis_account
from shinobu import data as market_data
from shinobu.chart import build_candlestick_chart
from shinobu.kis import KisApiError, fetch_domestic_balance
from shinobu.live_trading import (
    LIVE_ALLOCATION_KRW,
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
PRIMARY_SYMBOL = "122630.KS"
LIVE_CHART_STATE_KEY = "live_chart_state"
MAX_LIVE_CHART_CANDLES = 50
ASSET_DIR = Path(__file__).resolve().parent / "assets"
POSITIVE_IMAGE_PATH = ASSET_DIR / "shinobu_positive.png"
NEGATIVE_IMAGE_PATH = ASSET_DIR / "shinobu_negative.png"
POSITIVE_FALLBACK_PATH = ASSET_DIR / "shinobu_positive.svg"
NEGATIVE_FALLBACK_PATH = ASSET_DIR / "shinobu_negative.svg"


st.set_page_config(page_title="해동밀교 군자금 확보", page_icon="차트", layout="wide")


display_name = market_data.display_name
get_pair_symbol = market_data.get_pair_symbol
load_ui_chart_data = getattr(market_data, "load_ui_chart_data", market_data.load_live_chart_data)


def render_header() -> None:
    st.title("해동밀교 군자금 확보")
    st.caption("실전 5분봉 자동매매")


def init_live_chart_state() -> None:
    if LIVE_CHART_STATE_KEY not in st.session_state:
        st.session_state[LIVE_CHART_STATE_KEY] = {"started_at": "", "frames": {}}


@st.cache_data(ttl=20, show_spinner=False)
def get_cached_raw_frame(symbol: str, timeframe_label: str) -> pd.DataFrame:
    return load_ui_chart_data(symbol, timeframe_label)


@st.cache_data(ttl=20, show_spinner=False)
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
    border = "#3b82f6" if highlighted else "#2a2e39"
    background = "rgba(59,130,246,0.12)" if highlighted else "#131722"
    if tone == "negative":
        border = "#ef4444" if highlighted else "#2a2e39"
        background = "rgba(239,68,68,0.12)" if highlighted else "#131722"

    st.markdown(
        f"""
        <div style="border:2px solid {border};background:{background};border-radius:14px;padding:10px 10px 6px 10px;">
            <div style="font-size:13px;color:#e5e7eb;font-weight:700;margin-bottom:4px;">{title}</div>
            <div style="font-size:12px;color:#9aa4b2;margin-bottom:10px;">{caption}</div>
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
    if "122630" in codes:
        return "positive"
    if "252670" in codes:
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

        col1, col2 = st.columns(2)
        col1.metric("총자산", f"{summary.get('total_assets', 0):,.0f}원")
        col2.metric("예수금", f"{summary.get('cash', 0):,.0f}원")
        col3, col4 = st.columns(2)
        col3.metric("평가금액", f"{summary.get('eval_amount', 0):,.0f}원")
        col4.metric("평가손익", f"{summary.get('profit_amount', 0):,.0f}원")
        st.caption(
            f"계좌 {mask_account_number(summary.get('account_number', ''))} / 주문가능현금 {summary.get('orderable_cash', 0):,.0f}원"
        )

        if positions.empty:
            st.info("현재 보유 포지션이 없습니다.")
            return

        display_columns = ["code", "name", "quantity", "avg_price", "current_price", "eval_amount", "profit_amount", "profit_rate"]
        styled = positions.loc[:, [column for column in display_columns if column in positions.columns]].copy()
        rename_map = {
            "code": "종목코드",
            "name": "종목명",
            "quantity": "보유수량",
            "avg_price": "평균단가",
            "current_price": "현재가",
            "eval_amount": "평가금액",
            "profit_amount": "평가손익",
            "profit_rate": "수익률(%)",
        }
        styled = styled.rename(columns=rename_map)
        for column in ["보유수량", "평균단가", "현재가", "평가금액", "평가손익"]:
            if column in styled.columns:
                styled[column] = styled[column].map(lambda value: f"{float(value):,.0f}")
        if "수익률(%)" in styled.columns:
            styled["수익률(%)"] = styled["수익률(%)"].map(lambda value: f"{float(value):+.2f}")
        st.dataframe(styled, use_container_width=True, hide_index=True)
@st.fragment(run_every="10s")
def render_emotion_section() -> None:
    try:
        positions, summary = fetch_domestic_balance()
    except Exception:
        return
    render_emotion_panel(positions, summary)
@st.fragment(run_every="300s")
def render_live_trade_chart(symbol: str, pair_symbol: str | None, adjustments: StrategyAdjustments) -> None:
    chart_placeholder = st.empty()
    try:
        with st.spinner("차트를 불러오는 중..."):
            started_at = get_live_started_at()
            if started_at is None:
                frame = get_preview_raw_chart_frame(symbol)
                pair_frame = get_preview_raw_chart_frame(pair_symbol) if pair_symbol is not None else None
                st.info(f"실행 전 최근 완료 {LIVE_TIMEFRAME} {MAX_LIVE_CHART_CANDLES}개를 미리 보여줍니다.")
            else:
                frame = get_live_raw_chart_frame(symbol)
                pair_frame = get_live_raw_chart_frame(pair_symbol) if pair_symbol is not None else None

            if frame.empty:
                st.info("실행 이후 아직 표시할 5분봉 데이터가 없습니다.")
                return

            figure = build_candlestick_chart(
                frame,
                LIVE_TIMEFRAME,
                display_name(symbol),
                symbol,
                pair_frame=pair_frame,
                pair_name=display_name(pair_symbol) if pair_symbol else None,
                pair_symbol_code=pair_symbol,
            )

            orders = get_live_orders()
            if orders:
                order_frame = pd.DataFrame(orders)
                order_frame["candle_time"] = pd.to_datetime(order_frame["candle_time"])
                allowed_symbols = [value for value in [symbol, pair_symbol] if value is not None]
                order_frame = order_frame[order_frame["symbol"].isin(allowed_symbols)]
                _add_live_order_markers(figure, order_frame, frame)
    except Exception as exc:
        st.warning(f"차트 로딩 실패: {exc}")
        return

    chart_placeholder.plotly_chart(
        figure,
        use_container_width=True,
        theme=None,
        config={
            "scrollZoom": True,
            "displaylogo": False,
            "showAxisDragHandles": True,
            "showAxisRangeEntryBoxes": True,
            "doubleClick": "reset+autosize",
            "modeBarButtonsToRemove": ["lasso2d", "select2d", "zoomIn2d", "zoomOut2d"],
        },
    )

    try:
        if started_at is None:
            overlay_frame = get_preview_chart_frame(symbol, adjustments)
            overlay_pair_frame = get_preview_chart_frame(pair_symbol, adjustments) if pair_symbol is not None else None
        else:
            overlay_frame = get_live_chart_frame(symbol, adjustments)
            overlay_pair_frame = get_live_chart_frame(pair_symbol, adjustments) if pair_symbol is not None else None

        if overlay_frame.empty:
            return

        overlay_figure = build_candlestick_chart(
            overlay_frame,
            LIVE_TIMEFRAME,
            display_name(symbol),
            symbol,
            pair_frame=overlay_pair_frame,
            pair_name=display_name(pair_symbol) if pair_symbol else None,
            pair_symbol_code=pair_symbol,
        )

        orders = get_live_orders()
        if orders:
            order_frame = pd.DataFrame(orders)
            order_frame["candle_time"] = pd.to_datetime(order_frame["candle_time"])
            allowed_symbols = [value for value in [symbol, pair_symbol] if value is not None]
            order_frame = order_frame[order_frame["symbol"].isin(allowed_symbols)]
            _add_live_order_markers(overlay_figure, order_frame, overlay_frame)

        chart_placeholder.plotly_chart(
            overlay_figure,
            use_container_width=True,
            theme=None,
            config={
                "scrollZoom": True,
                "displaylogo": False,
                "showAxisDragHandles": True,
                "showAxisRangeEntryBoxes": True,
                "doubleClick": "reset+autosize",
                "modeBarButtonsToRemove": ["lasso2d", "select2d", "zoomIn2d", "zoomOut2d"],
            },
        )
    except Exception:
        pass
@st.fragment(run_every="30s")
def run_live_engine(loaded_symbol: str, pair_symbol: str | None, adjustments: StrategyAdjustments) -> None:
    if not is_live_enabled() or pair_symbol is None:
        return

    try:
        process_live_trading_cycle(loaded_symbol, pair_symbol, adjustments)
    except KisApiError:
        return
    except Exception:
        return


@st.fragment(run_every="3s")
def render_live_trading_panel(loaded_symbol: str, pair_symbol: str | None, adjustments: StrategyAdjustments) -> None:
    st.markdown("#### 실전 투자")
    left_button, right_button = st.columns(2)
    with left_button:
        if st.button("실행", use_container_width=True, key="live_start_button"):
            if pair_symbol is None:
                st.warning("실전 투자는 레버리지/인버스 페어 종목에서만 실행됩니다.")
            else:
                set_live_enabled(True)
    with right_button:
        if st.button("중지", use_container_width=True, key="live_stop_button"):
            set_live_enabled(False)

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
    st.caption(
        f"실전 주문은 5분봉 기준으로만 처리하고, 30초마다 최신 완료 봉을 확인합니다. 최대 투입금은 {LIVE_ALLOCATION_KRW:,.0f}원입니다."
    )

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
        with emotion_slot.container():
            render_emotion_section()
        with chart_slot.container():
            render_live_trade_chart(loaded_symbol, pair_symbol, adjustments)


if __name__ == "__main__":
    main()
