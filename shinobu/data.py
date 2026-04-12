from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from zoneinfo import ZoneInfo
import urllib.request

import pandas as pd
import streamlit as st
import yfinance as yf

from config import has_kis_credentials
from shinobu.kis import KisApiError, fetch_domestic_daily, fetch_domestic_intraday_history
from shinobu.live_data import load_intraday_recent, load_intraday_seed, merge_intraday_frames
from shinobu.strategy import get_strategy_history_business_days


KST = ZoneInfo("Asia/Seoul")
DEFAULT_SYMBOL = "122630.KS"
DEFAULT_SYMBOL_NAME = "KODEX 레버리지"
KRX_CORP_LIST_URL = "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13"

CRYPTO_SYMBOL_ALIASES = {
    "BTC": "BTC-USD",
    "BTC-USD": "BTC-USD",
    "BITCOIN": "BTC-USD",
    "비트코인": "BTC-USD",
}

SYMBOL_NAME_MAP = {
    "005930.KS": "삼성전자",
    "069500.KS": "KODEX 200",
    "122630.KS": "KODEX 레버리지",
    "252670.KS": "KODEX 200선물인버스2X",
    "BTC-USD": "비트코인",
}

PAIR_SYMBOL_MAP = {
    "122630.KS": "252670.KS",
    "252670.KS": "122630.KS",
}

LIVE_INTRADAY_LOOKBACK_DAYS = 5
LIVE_RECENT_WINDOW_MINUTES = 720
RAW_CACHE_DIR = Path(__file__).resolve().parent.parent / ".streamlit" / "raw_cache"

INTRADAY_RESAMPLE_MINUTES = {
    "5분봉": 5,
    "15분봉": 15,
    "30분봉": 30,
    "1시간봉": 60,
    "4시간봉": 240,
}


@dataclass(frozen=True)
class TimeframeConfig:
    label: str
    interval: str
    period: str
    resample_kind: str | None = None


TIMEFRAME_OPTIONS = {
    "5분봉": TimeframeConfig("5분봉", "5m", "60d"),
    "15분봉": TimeframeConfig("15분봉", "15m", "60d"),
    "30분봉": TimeframeConfig("30분봉", "30m", "60d"),
    "1시간봉": TimeframeConfig("1시간봉", "60m", "730d"),
    "4시간봉": TimeframeConfig("4시간봉", "60m", "730d", "4h"),
    "일봉": TimeframeConfig("일봉", "1d", "10y"),
    "주봉": TimeframeConfig("주봉", "1wk", "10y"),
    "월봉": TimeframeConfig("월봉", "1mo", "10y"),
}


def is_crypto_symbol(symbol: str) -> bool:
    normalized = symbol.upper()
    return normalized.endswith("-USD") or normalized in {"BTC", "BTC-USD"}


def is_domestic_stock_symbol(symbol: str) -> bool:
    normalized = symbol.upper()
    return normalized.endswith(".KS") or normalized.endswith(".KQ")


def _fetch_krx_symbol_table() -> pd.DataFrame:
    with urllib.request.urlopen(KRX_CORP_LIST_URL, timeout=20) as response:
        html = response.read().decode("euc-kr", errors="ignore")
    frame = pd.read_html(StringIO(html), flavor="lxml")[0]
    frame["종목코드"] = frame["종목코드"].astype(str).str.zfill(6)
    frame["symbol"] = frame["종목코드"] + ".KS"
    frame["회사명정규화"] = frame["회사명"].astype(str).str.strip().str.upper()
    return frame[["회사명", "종목코드", "symbol", "회사명정규화"]]


@st.cache_data(ttl=86400, show_spinner=False)
def get_krx_symbol_table() -> pd.DataFrame:
    return _fetch_krx_symbol_table()


def resolve_symbol(user_input: str) -> tuple[str, str]:
    cleaned = user_input.strip()
    if not cleaned:
        return DEFAULT_SYMBOL, DEFAULT_SYMBOL_NAME

    upper_cleaned = cleaned.upper()
    if cleaned in CRYPTO_SYMBOL_ALIASES:
        symbol = CRYPTO_SYMBOL_ALIASES[cleaned]
        return symbol, display_name(symbol)
    if upper_cleaned in CRYPTO_SYMBOL_ALIASES:
        symbol = CRYPTO_SYMBOL_ALIASES[upper_cleaned]
        return symbol, display_name(symbol)
    if "." not in upper_cleaned and upper_cleaned.isdigit() and len(upper_cleaned) == 6:
        symbol = f"{upper_cleaned}.KS"
        return symbol, display_name(symbol)
    if upper_cleaned.endswith(".KS") or upper_cleaned.endswith(".KQ"):
        return upper_cleaned, display_name(upper_cleaned)

    table = get_krx_symbol_table()
    exact_matches = table[table["회사명정규화"] == upper_cleaned]
    if not exact_matches.empty:
        row = exact_matches.iloc[0]
        return row["symbol"], str(row["회사명"])

    prefix_matches = table[table["회사명정규화"].str.startswith(upper_cleaned, na=False)]
    if not prefix_matches.empty:
        row = prefix_matches.iloc[0]
        return row["symbol"], str(row["회사명"])

    contains_matches = table[table["회사명정규화"].str.contains(upper_cleaned, na=False, regex=False)]
    if not contains_matches.empty:
        row = contains_matches.iloc[0]
        return row["symbol"], str(row["회사명"])

    try:
        search = yf.Search(cleaned, max_results=8)
        exact_quote = None
        fallback_quote = None
        for quote in search.quotes:
            symbol = str(quote.get("symbol", "")).upper()
            if not symbol or not (symbol.endswith(".KS") or symbol.endswith(".KQ") or symbol.endswith("-USD")):
                continue

            short_name = str(quote.get("shortname") or quote.get("longname") or symbol)
            long_name = str(quote.get("longname") or short_name)
            normalized_short = short_name.strip().upper()
            normalized_long = long_name.strip().upper()

            if normalized_short == upper_cleaned or normalized_long == upper_cleaned:
                exact_quote = (symbol, short_name)
                break
            if fallback_quote is None:
                fallback_quote = (symbol, short_name)

        if exact_quote is not None:
            SYMBOL_NAME_MAP[exact_quote[0]] = exact_quote[1]
            return exact_quote
        if fallback_quote is not None:
            SYMBOL_NAME_MAP[fallback_quote[0]] = fallback_quote[1]
            return fallback_quote
    except Exception:
        pass

    return upper_cleaned, upper_cleaned


def display_symbol(symbol: str) -> str:
    if symbol.upper() == "BTC-USD":
        return "BTC"
    return symbol.replace(".KS", "").replace(".KQ", "")


def display_name(symbol: str) -> str:
    if symbol in SYMBOL_NAME_MAP:
        return SYMBOL_NAME_MAP[symbol]

    if is_domestic_stock_symbol(symbol):
        short_code = display_symbol(symbol)
        table = get_krx_symbol_table()
        match = table[table["종목코드"] == short_code]
        if not match.empty:
            return str(match.iloc[0]["회사명"])

    return display_symbol(symbol)


def get_pair_symbol(symbol: str) -> str | None:
    return PAIR_SYMBOL_MAP.get(symbol)


def _normalize_columns(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    if isinstance(normalized.columns, pd.MultiIndex):
        flattened = []
        for left, right in normalized.columns:
            flattened.append(left or right)
        normalized.columns = flattened
    return normalized


def _to_kst_index(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = _normalize_columns(frame).reset_index()

    time_column = None
    for candidate in ["Datetime", "Date", "index"]:
        if candidate in normalized.columns:
            time_column = candidate
            break

    if time_column is None:
        raise ValueError("시간 열을 찾지 못했습니다.")

    timestamps = pd.to_datetime(normalized[time_column])
    if getattr(timestamps.dt, "tz", None) is None:
        timestamps = timestamps.dt.tz_localize(KST)
    else:
        timestamps = timestamps.dt.tz_convert(KST)

    normalized["시간"] = timestamps.dt.tz_localize(None)
    normalized = normalized.set_index("시간")
    return normalized[["Open", "High", "Low", "Close", "Volume"]].dropna().sort_index()


def _resample_four_hour_crypto(frame: pd.DataFrame) -> pd.DataFrame:
    aggregated = frame.resample("4h").agg(
        {"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}
    )
    return aggregated.dropna().sort_index()


def _resample_domestic_intraday(frame: pd.DataFrame, interval_minutes: int) -> pd.DataFrame:
    minutes = frame.index.hour * 60 + frame.index.minute
    session_open = 9 * 60
    bucket = pd.Series((minutes - session_open) // interval_minutes, index=frame.index)
    bucket = bucket[(minutes >= session_open) & (minutes <= 15 * 60 + 30)]
    filtered = frame.loc[bucket.index]
    filtered = filtered.assign(_bucket=bucket.values, _date=filtered.index.normalize())
    filtered = filtered[filtered["_bucket"] >= 0]

    aggregated = filtered.groupby(["_date", "_bucket"], sort=True).agg(
        {"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}
    )
    aggregated = aggregated.dropna()
    rebuilt_index = []
    for trade_date, bucket_index in aggregated.index:
        start_time = pd.Timestamp(trade_date) + pd.Timedelta(hours=9) + pd.Timedelta(minutes=interval_minutes * int(bucket_index))
        rebuilt_index.append(start_time)
    aggregated.index = pd.DatetimeIndex(rebuilt_index, name="시간")
    return aggregated.sort_index()


def _load_yfinance_data(symbol: str, timeframe_label: str) -> pd.DataFrame:
    config = TIMEFRAME_OPTIONS[timeframe_label]
    raw = yf.download(
        symbol,
        period=config.period,
        interval=config.interval,
        auto_adjust=False,
        progress=False,
    )
    data = _to_kst_index(raw)

    if config.resample_kind == "4h":
        if is_crypto_symbol(symbol):
            data = _resample_four_hour_crypto(data)
        else:
            data = _resample_domestic_intraday(data, 240)
    return data


def _load_chart_data_impl(symbol: str, timeframe_label: str) -> pd.DataFrame:
    if is_domestic_stock_symbol(symbol) and has_kis_credentials():
        try:
            short_code = display_symbol(symbol)
            if timeframe_label in {"일봉", "주봉", "월봉"}:
                period_code = {"일봉": "D", "주봉": "W", "월봉": "M"}[timeframe_label]
                return fetch_domestic_daily(short_code, period_code)
            if timeframe_label in INTRADAY_RESAMPLE_MINUTES:
                minute_frame = fetch_domestic_intraday_history(short_code, lookback_days=LIVE_INTRADAY_LOOKBACK_DAYS)
                return _resample_domestic_intraday(minute_frame, INTRADAY_RESAMPLE_MINUTES[timeframe_label])
        except KisApiError:
            pass
        except Exception:
            pass

    return _load_yfinance_data(symbol, timeframe_label)


def _business_days_to_lookback_days(business_days: int) -> int:
    return max(int(business_days * 2), LIVE_INTRADAY_LOOKBACK_DAYS)


def _raw_cache_path(symbol: str, timeframe_label: str, lookback_days: int) -> Path:
    safe_symbol = symbol.replace(".", "_")
    safe_timeframe = timeframe_label.replace("/", "_")
    RAW_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return RAW_CACHE_DIR / f"{safe_symbol}_{safe_timeframe}_{int(lookback_days)}d.pkl"


def _load_cached_minute_frame(symbol: str, timeframe_label: str, lookback_days: int) -> pd.DataFrame:
    cache_path = _raw_cache_path(symbol, timeframe_label, lookback_days)
    if not cache_path.exists():
        return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])

    try:
        cached = pd.read_pickle(cache_path)
    except Exception:
        return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])

    if not isinstance(cached, pd.DataFrame) or cached.empty:
        return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])
    return cached.sort_index()


def _write_cached_minute_frame(symbol: str, timeframe_label: str, lookback_days: int, frame: pd.DataFrame) -> None:
    cache_path = _raw_cache_path(symbol, timeframe_label, lookback_days)
    trimmed = frame.sort_index()
    pd.to_pickle(trimmed, cache_path)


def _load_persisted_intraday_frame(symbol: str, timeframe_label: str, lookback_days: int) -> pd.DataFrame:
    cutoff = pd.Timestamp.now().floor("min") - pd.Timedelta(days=lookback_days)
    cached = _load_cached_minute_frame(symbol, timeframe_label, lookback_days)
    has_enough_history = not cached.empty and pd.Timestamp(cached.index.min()) <= cutoff

    if has_enough_history:
        seed_frame = cached.loc[cached.index >= cutoff].copy()
    else:
        seed_frame = load_intraday_seed(symbol, lookback_days=lookback_days)

    recent_frame = load_intraday_recent(symbol, lookback_minutes=LIVE_RECENT_WINDOW_MINUTES)
    merged = merge_intraday_frames(seed_frame, recent_frame)
    merged = merged.loc[merged.index >= cutoff].copy()
    if not merged.empty:
        _write_cached_minute_frame(symbol, timeframe_label, lookback_days, merged)
    return merged


def _load_live_chart_data_impl(
    symbol: str,
    timeframe_label: str,
    lookback_days: int = LIVE_INTRADAY_LOOKBACK_DAYS,
) -> pd.DataFrame:
    if is_domestic_stock_symbol(symbol):
        if not has_kis_credentials():
            raise KisApiError("live domestic chart requires KIS credentials")

        short_code = display_symbol(symbol)
        if timeframe_label in INTRADAY_RESAMPLE_MINUTES:
            minute_frame = _load_persisted_intraday_frame(short_code, timeframe_label, lookback_days)
            return _resample_domestic_intraday(minute_frame, INTRADAY_RESAMPLE_MINUTES[timeframe_label])
        if timeframe_label in {"ì¼ë´", "ì£¼ë´", "ìë´"}:
            period_code = {"ì¼ë´": "D", "ì£¼ë´": "W", "ìë´": "M"}[timeframe_label]
            return fetch_domestic_daily(short_code, period_code)
        raise KisApiError("unsupported live timeframe")

    return _load_yfinance_data(symbol, timeframe_label)


def _load_ui_chart_data_impl(
    symbol: str,
    timeframe_label: str,
    timeout_seconds: float = 10.0,
    lookback_days: int = LIVE_INTRADAY_LOOKBACK_DAYS,
) -> pd.DataFrame:
    if not is_domestic_stock_symbol(symbol) or not has_kis_credentials():
        return _load_yfinance_data(symbol, timeframe_label)

    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(_load_live_chart_data_impl, symbol, timeframe_label, lookback_days)
    try:
        return future.result(timeout=timeout_seconds)
    except (FuturesTimeoutError, KisApiError, Exception):
        return _load_yfinance_data(symbol, timeframe_label)
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


@st.cache_data(ttl=300, show_spinner=False)
def load_chart_data(symbol: str, timeframe_label: str) -> pd.DataFrame:
    return _load_chart_data_impl(symbol, timeframe_label)


@st.cache_data(ttl=5, show_spinner=False)
def load_live_chart_data(symbol: str, timeframe_label: str) -> pd.DataFrame:
    return _load_live_chart_data_impl(symbol, timeframe_label)


@st.cache_data(ttl=5, show_spinner=False)
def load_ui_chart_data(symbol: str, timeframe_label: str) -> pd.DataFrame:
    return _load_ui_chart_data_impl(symbol, timeframe_label)


@st.cache_data(ttl=5, show_spinner=False)
def load_live_chart_data_for_strategy(symbol: str, timeframe_label: str, strategy_name: str) -> pd.DataFrame:
    business_days = get_strategy_history_business_days(strategy_name)
    lookback_days = _business_days_to_lookback_days(business_days)
    return _load_live_chart_data_impl(symbol, timeframe_label, lookback_days=lookback_days)


@st.cache_data(ttl=5, show_spinner=False)
def load_ui_chart_data_for_strategy(symbol: str, timeframe_label: str, strategy_name: str) -> pd.DataFrame:
    business_days = get_strategy_history_business_days(strategy_name)
    lookback_days = _business_days_to_lookback_days(business_days)
    return _load_ui_chart_data_impl(symbol, timeframe_label, lookback_days=lookback_days)


def get_notice(timeframe_label: str, symbol: str | None = None) -> str:
    if symbol and is_crypto_symbol(symbol):
        if timeframe_label in {"일봉", "주봉", "월봉"}:
            return "비트코인은 24시간 거래되며, 시간 표시는 한국시간 기준입니다."
        if timeframe_label in {"1시간봉", "4시간봉"}:
            return "비트코인은 24시간 거래되며, 시간 표시는 한국시간 기준입니다. 데이터 한계로 최근 약 730일까지만 제공합니다."
        return "비트코인은 24시간 거래되며, 시간 표시는 한국시간 기준입니다. 데이터 한계로 최근 약 60일까지만 제공합니다."

    if symbol and is_domestic_stock_symbol(symbol) and has_kis_credentials():
        if timeframe_label in {"일봉", "주봉", "월봉"}:
            return "국내주식 일봉·주봉·월봉은 한국투자 Open API를 우선 사용하며, 시간 표시는 한국시간 기준입니다."
        return "국내주식 분봉은 한국투자 Open API 1분 데이터를 이어붙여 재구성하며, 최대 약 1년까지 조회합니다."

    if timeframe_label in {"일봉", "주봉", "월봉"}:
        return "기본 조회 기간은 최근 10년이며, 시간 표시는 한국시간 기준입니다."
    if timeframe_label in {"1시간봉", "4시간봉"}:
        return "시간 표시는 한국시간 기준이며, 데이터 한계로 최근 약 730일까지만 제공합니다."
    return "시간 표시는 한국시간 기준이며, 데이터 한계로 최근 약 60일까지만 제공합니다."
