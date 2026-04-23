from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from zoneinfo import ZoneInfo
from typing import Any
import re

import pandas as pd
import requests
import yfinance as yf

from shinobu import data as market_data
from shinobu.cache_db import list_payload_cache_by_prefix, load_payload_cache, save_payload_cache


KST = ZoneInfo("Asia/Seoul")
RECOMMENDATION_CACHE_PREFIX = "stock_reco:"
MAX_UNIVERSE_SIZE = 1400
DOWNLOAD_BATCH_SIZE = 40
NAVER_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://finance.naver.com/",
}


@dataclass
class RecommendationMetrics:
    symbol: str
    name: str
    weekly_cloud_top: float
    weekly_ma5: float
    weekly_ma20: float
    weekly_ma60: float
    weekly_ma120: float
    daily_ma5: float
    daily_ma20: float
    daily_ma60: float
    daily_ma120: float
    daily_close: float
    daily_traded_value_20d: float
    daily_alignment: str
    correction_ratio: float
    fib_rebound_low: float
    fib_rebound_high: float
    wave_target_1: float
    wave_target_2: float
    invalidation: float
    score: float
    reason: str


def _cache_key(run_date: date) -> str:
    return f"{RECOMMENDATION_CACHE_PREFIX}{run_date.isoformat()}"


def _today_kst() -> date:
    return pd.Timestamp.now(tz=KST).date()


def _is_market_day(day: date) -> bool:
    return pd.Timestamp(day).dayofweek < 5


def _normalize_ohlcv_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])
    normalized = frame.copy()
    if isinstance(normalized.columns, pd.MultiIndex):
        normalized.columns = normalized.columns.get_level_values(-1)
    rename_map = {str(col): str(col).title() for col in normalized.columns}
    normalized = normalized.rename(columns=rename_map)
    required = ["Open", "High", "Low", "Close", "Volume"]
    if any(col not in normalized.columns for col in required):
        return pd.DataFrame(columns=required)
    out = normalized.loc[:, required].copy()
    out.index = pd.to_datetime(out.index, errors="coerce")
    out = out[~out.index.isna()].sort_index()
    return out.dropna(subset=["Open", "High", "Low", "Close"])


def _extract_symbol_frame(raw: pd.DataFrame, symbol: str, batch: list[str]) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])
    if len(batch) == 1 and not isinstance(raw.columns, pd.MultiIndex):
        return _normalize_ohlcv_frame(raw)
    if isinstance(raw.columns, pd.MultiIndex):
        # yfinance usually returns MultiIndex with levels (field, ticker)
        try:
            frame = raw.xs(symbol, axis=1, level=1, drop_level=True)
            return _normalize_ohlcv_frame(frame)
        except Exception:
            pass
        try:
            frame = raw[symbol]
            return _normalize_ohlcv_frame(frame)
        except Exception:
            pass
    return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])


def _download_ohlcv_map(symbols: list[str], *, interval: str, period: str) -> dict[str, pd.DataFrame]:
    results: dict[str, pd.DataFrame] = {}
    for start in range(0, len(symbols), DOWNLOAD_BATCH_SIZE):
        batch = symbols[start : start + DOWNLOAD_BATCH_SIZE]
        if not batch:
            continue
        try:
            raw = yf.download(
                tickers=batch,
                interval=interval,
                period=period,
                auto_adjust=False,
                progress=False,
                prepost=False,
                threads=True,
                group_by="column",
            )
        except Exception:
            continue
        for symbol in batch:
            frame = _extract_symbol_frame(raw, symbol, batch)
            if not frame.empty:
                results[symbol] = frame
    return results


def _fetch_html_text(url: str) -> str:
    response = requests.get(url, headers=NAVER_HEADERS, timeout=12)
    response.raise_for_status()
    return response.text


def _extract_six_digit_codes_from_naver_html(html: str) -> list[str]:
    codes = re.findall(r"/item/main\.naver\?code=(\d{6})", str(html))
    deduped: list[str] = []
    seen: set[str] = set()
    for code in codes:
        if code not in seen:
            seen.add(code)
            deduped.append(code)
    return deduped


def _fetch_naver_kospi200_codes(max_pages: int = 25) -> list[str]:
    collected: list[str] = []
    seen: set[str] = set()
    stagnant_pages = 0
    for page in range(1, max_pages + 1):
        html = _fetch_html_text(f"https://finance.naver.com/sise/entryJongmok.naver?type=KPI200&page={page}")
        before = len(seen)
        for code in _extract_six_digit_codes_from_naver_html(html):
            if code not in seen:
                seen.add(code)
                collected.append(code)
        if len(seen) == before:
            stagnant_pages += 1
        else:
            stagnant_pages = 0
        if stagnant_pages >= 2:
            break
    return collected


def _fetch_naver_kosdaq150_codes(top_n: int = 150, max_pages: int = 30) -> list[str]:
    # Public source fallback: KOSDAQ market-cap ranking top 150.
    collected: list[str] = []
    seen: set[str] = set()
    for page in range(1, max_pages + 1):
        html = _fetch_html_text(f"https://finance.naver.com/sise/sise_market_sum.naver?sosok=1&page={page}")
        page_codes = _extract_six_digit_codes_from_naver_html(html)
        if not page_codes:
            break
        for code in page_codes:
            if code not in seen:
                seen.add(code)
                collected.append(code)
                if len(collected) >= int(top_n):
                    return collected
    return collected[: int(top_n)]


def _weekly_condition(frame_w: pd.DataFrame) -> tuple[bool, dict[str, float]]:
    close = frame_w["Close"].astype(float)
    high = frame_w["High"].astype(float)
    low = frame_w["Low"].astype(float)
    ma5 = close.rolling(5).mean().iloc[-1]
    ma20 = close.rolling(20).mean().iloc[-1]
    ma60 = close.rolling(60).mean().iloc[-1]
    ma120 = close.rolling(120).mean().iloc[-1]
    tenkan = (high.rolling(9).max() + low.rolling(9).min()) / 2.0
    kijun = (high.rolling(26).max() + low.rolling(26).min()) / 2.0
    # Match chart-style Ichimoku cloud at current candle position:
    # cloud plotted forward by 26 is aligned back to "now" via shift(26).
    senkou_a = (tenkan + kijun) / 2.0
    senkou_b = (high.rolling(52).max() + low.rolling(52).min()) / 2.0
    senkou_a_now = senkou_a.shift(26)
    senkou_b_now = senkou_b.shift(26)
    cloud_series = pd.concat([senkou_a_now, senkou_b_now], axis=1).max(axis=1)
    cloud_top = cloud_series.iloc[-1]
    if pd.isna(cloud_top):
        cloud_top = pd.concat([senkou_a, senkou_b], axis=1).max(axis=1).iloc[-1]
    bull_cloud = bool(
        pd.notna(senkou_a_now.iloc[-1]) and pd.notna(senkou_b_now.iloc[-1]) and (senkou_a_now.iloc[-1] > senkou_b_now.iloc[-1])
    )
    values = {
        "ma5": float(ma5 or 0.0),
        "ma20": float(ma20 or 0.0),
        "ma60": float(ma60 or 0.0),
        "ma120": float(ma120 or 0.0),
        "cloud_top": float(cloud_top or 0.0),
        "bull_cloud": 1.0 if bull_cloud else 0.0,
    }
    cond = (
        pd.notna(ma5)
        and pd.notna(ma20)
        and pd.notna(cloud_top)
        and bull_cloud
        and ma5 > cloud_top
        and ma20 > cloud_top
    )
    return bool(cond), values


def _daily_alignment(close: pd.Series) -> tuple[str, float, float, float, float]:
    ma5 = close.rolling(5).mean().iloc[-1]
    ma20 = close.rolling(20).mean().iloc[-1]
    ma60 = close.rolling(60).mean().iloc[-1]
    ma120 = close.rolling(120).mean().iloc[-1]
    aligned = bool(ma5 > ma20 > ma60 > ma120)
    forming = bool(
        ma5 > ma20 > ma60
        and ma60 > ma120 * 0.98
        and close.rolling(120).mean().diff().iloc[-1] > 0
    )
    if aligned:
        status = "정배열"
    elif forming:
        status = "정배열 진행"
    else:
        status = "중립"
    return status, float(ma5), float(ma20), float(ma60), float(ma120)


def _elliott_early_stage_metrics(frame_d: pd.DataFrame) -> dict[str, float | bool]:
    close = frame_d["Close"].astype(float)
    window = close.tail(220)
    if len(window) < 140:
        return {"ok": False}

    pivot_low_idx = window.iloc[:-40].idxmin()
    segment_after_low = window.loc[pivot_low_idx:]
    if len(segment_after_low) < 60:
        return {"ok": False}

    pivot_high_idx = segment_after_low.iloc[:-20].idxmax() if len(segment_after_low) > 20 else segment_after_low.idxmax()
    if pd.Timestamp(pivot_high_idx) <= pd.Timestamp(pivot_low_idx):
        return {"ok": False}

    impulse_low = float(window.loc[pivot_low_idx])
    impulse_high = float(window.loc[pivot_high_idx])
    if impulse_high <= impulse_low:
        return {"ok": False}

    correction_leg = window.loc[pivot_high_idx:]
    correction_low = float(correction_leg.min())
    current = float(window.iloc[-1])
    if correction_low <= 0 or current <= 0:
        return {"ok": False}

    amplitude = impulse_high - impulse_low
    correction_ratio = (impulse_high - correction_low) / amplitude
    fib_382 = impulse_high - amplitude * 0.382
    fib_50 = impulse_high - amplitude * 0.5
    fib_618 = impulse_high - amplitude * 0.618

    rebound_span = impulse_high - correction_low
    rebound_min = correction_low + rebound_span * 0.382
    rebound_max = correction_low + rebound_span * 0.5

    target_1 = impulse_high + amplitude * 0.382
    target_2 = impulse_high + amplitude * 0.618
    invalidation = correction_low
    drawdown_from_high = (impulse_high - current) / impulse_high if impulse_high > 0 else 0.0
    drawdown_from_high = max(0.0, float(drawdown_from_high))
    recent_20_return = 0.0
    if len(window) >= 20 and float(window.iloc[-20]) > 0:
        recent_20_return = (current / float(window.iloc[-20])) - 1.0

    early_stage = (
        0.20 <= correction_ratio <= 0.62
        and current >= rebound_min
        and drawdown_from_high <= 0.18
        and recent_20_return >= -0.03
        and current <= target_2 * 1.18
    )
    return {
        "ok": True,
        "early_stage": bool(early_stage),
        "correction_ratio": float(correction_ratio),
        "fib_382": float(fib_382),
        "fib_50": float(fib_50),
        "fib_618": float(fib_618),
        "rebound_min": float(rebound_min),
        "rebound_max": float(rebound_max),
        "target_1": float(target_1),
        "target_2": float(target_2),
        "invalidation": float(invalidation),
        "drawdown_from_high": float(drawdown_from_high),
        "recent_20_return": float(recent_20_return),
        "impulse_low": float(impulse_low),
        "impulse_high": float(impulse_high),
        "current": float(current),
    }


def _build_reason(name: str, symbol: str, daily_alignment: str, elliott: dict[str, float | bool]) -> str:
    if not bool(elliott.get("ok", False)):
        return "엘리어트 파동 구조를 안정적으로 산출하지 못해 해석을 생략했습니다."
    stage_text = "상승 3~5파 진행/초입 가능 구간"
    if bool(elliott.get("early_stage", False)):
        stage_text = "조정파 마무리 이후 상승파 재개 가능 구간"
    return (
        f"엘리어트 관점: {stage_text}. "
        f"직전 임펄스 {elliott['impulse_low']:.0f}→{elliott['impulse_high']:.0f} 이후 "
        f"조정 비율 {float(elliott['correction_ratio']) * 100:.1f}%이며, "
        f"최소 반등 구간 {elliott['rebound_min']:.0f}~{elliott['rebound_max']:.0f}, "
        f"목표가 {elliott['target_1']:.0f}/{elliott['target_2']:.0f}, "
        f"무효화 {elliott['invalidation']:.0f}, "
        f"고점 대비 되돌림 {float(elliott.get('drawdown_from_high', 0.0)) * 100:.1f}%."
    )


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _as_dict(metric: RecommendationMetrics) -> dict[str, Any]:
    return {
        "symbol": metric.symbol,
        "name": metric.name,
        "weekly_cloud_top": metric.weekly_cloud_top,
        "weekly_ma5": metric.weekly_ma5,
        "weekly_ma20": metric.weekly_ma20,
        "weekly_ma60": metric.weekly_ma60,
        "weekly_ma120": metric.weekly_ma120,
        "daily_ma5": metric.daily_ma5,
        "daily_ma20": metric.daily_ma20,
        "daily_ma60": metric.daily_ma60,
        "daily_ma120": metric.daily_ma120,
        "daily_close": metric.daily_close,
        "daily_traded_value_20d": metric.daily_traded_value_20d,
        "daily_alignment": metric.daily_alignment,
        "correction_ratio": metric.correction_ratio,
        "fib_rebound_low": metric.fib_rebound_low,
        "fib_rebound_high": metric.fib_rebound_high,
        "wave_target_1": metric.wave_target_1,
        "wave_target_2": metric.wave_target_2,
        "invalidation": metric.invalidation,
        "score": metric.score,
        "reason": metric.reason,
    }


def generate_stock_recommendations(max_count: int | None = None) -> dict[str, Any]:
    table = market_data.get_krx_symbol_table()
    if table.empty:
        raise ValueError("KRX 종목 테이블을 불러오지 못했습니다.")

    working = table.copy()
    code_col = "종목코드" if "종목코드" in working.columns else None
    if code_col is None:
        raise ValueError("KRX symbol table is missing 종목코드 column.")

    name_col = "회사명" if "회사명" in working.columns else None
    working[code_col] = working[code_col].astype(str).str.zfill(6)
    working = working[working[code_col].str.match(r"^\d{6}$", na=False)].copy()
    code_to_name = {
        code: (str(row[name_col]) if name_col and pd.notna(row[name_col]) else code)
        for code, row in working.set_index(code_col).iterrows()
    }

    kospi200_codes: list[str] = []
    kosdaq150_codes: list[str] = []
    source_note = "코스피200 + 코스닥150 후보군"
    try:
        kospi200_codes = _fetch_naver_kospi200_codes()
    except Exception:
        kospi200_codes = []
    try:
        kosdaq150_codes = _fetch_naver_kosdaq150_codes(top_n=150)
    except Exception:
        kosdaq150_codes = []

    code_pool = sorted(set(kospi200_codes) | set(kosdaq150_codes))
    valid_codes = set(working[code_col].tolist())
    code_pool = [code for code in code_pool if code in valid_codes]

    if not code_pool:
        source_note = "후보군 소스 실패로 KRX 전체 후보로 임시 대체"
        code_pool = working[code_col].head(MAX_UNIVERSE_SIZE).tolist()

    symbols_ks = [f"{code}.KS" for code in code_pool]
    weekly_map = _download_ohlcv_map(symbols_ks, interval="1wk", period="3y")
    missing_codes = [code for code in code_pool if f"{code}.KS" not in weekly_map]
    if missing_codes:
        symbols_kq = [f"{code}.KQ" for code in missing_codes]
        weekly_map.update(_download_ohlcv_map(symbols_kq, interval="1wk", period="3y"))

    symbols = list(weekly_map.keys())
    weekly_pass_symbols: list[str] = []
    weekly_stats: dict[str, dict[str, float]] = {}
    liquidity_rank: list[tuple[str, float]] = []
    for symbol, frame_w in weekly_map.items():
        if len(frame_w) < 130:
            continue
        passed, stats = _weekly_condition(frame_w)
        if not passed:
            continue
        weekly_pass_symbols.append(symbol)
        weekly_stats[symbol] = stats
        traded_value = _safe_float((frame_w["Close"].astype(float) * frame_w["Volume"].astype(float)).tail(12).mean())
        liquidity_rank.append((symbol, traded_value))

    if not weekly_pass_symbols:
        return {
            "run_date": _today_kst().isoformat(),
            "generated_at": pd.Timestamp.now(tz=KST).isoformat(),
            "items": [],
            "meta": {
                "universe_scope": source_note,
                "kospi200_count": len(kospi200_codes),
                "kosdaq150_count": len(kosdaq150_codes),
                "candidate_pool_count": len(code_pool),
                "universe_scanned": len(symbols),
                "weekly_pass": 0,
            },
        }

    liquidity_rank.sort(key=lambda item: item[1], reverse=True)
    daily_candidates = [symbol for symbol, _ in liquidity_rank]
    daily_map = _download_ohlcv_map(daily_candidates, interval="1d", period="3y")

    recommendations: list[RecommendationMetrics] = []
    for symbol in daily_candidates:
        frame_d = daily_map.get(symbol)
        if frame_d is None or frame_d.empty or len(frame_d) < 170:
            continue
        daily_close = frame_d["Close"].astype(float)
        alignment, ma5, ma20, ma60, ma120 = _daily_alignment(daily_close)
        if alignment not in {"정배열", "정배열 진행"}:
            continue
        current_price = _safe_float(daily_close.iloc[-1])
        if ma120 <= 0 or current_price > (ma120 * 1.5):
            continue
        elliott = _elliott_early_stage_metrics(frame_d)
        has_elliott = bool(elliott.get("ok", False))
        if not has_elliott or not bool(elliott.get("early_stage", False)):
            continue

        volume_20 = _safe_float(frame_d["Volume"].astype(float).tail(20).mean())
        traded_value_20 = _safe_float((frame_d["Close"].astype(float) * frame_d["Volume"].astype(float)).tail(20).mean())
        weekly = weekly_stats.get(symbol, {})
        cloud_margin = 0.0
        if weekly:
            cloud = _safe_float(weekly.get("cloud_top", 0.0))
            if cloud > 0:
                cloud_margin = (_safe_float(weekly.get("ma5", 0.0)) / cloud) - 1.0

        score = (
            cloud_margin * 100.0
            + (20.0 if alignment == "정배열" else 10.0)
            + (10.0 if _safe_float(elliott.get("correction_ratio", 0.0)) <= 0.62 else 4.0)
            + 12.0
            + min(15.0, traded_value_20 / 5_000_000_000.0)
            + min(8.0, volume_20 / 1_000_000.0)
        )

        code = symbol.replace(".KS", "").replace(".KQ", "")
        name = code_to_name.get(code, market_data.display_name(symbol))
        reason = _build_reason(name, symbol, alignment, elliott)
        recommendations.append(
            RecommendationMetrics(
                symbol=symbol,
                name=name,
                weekly_cloud_top=_safe_float(weekly.get("cloud_top")),
                weekly_ma5=_safe_float(weekly.get("ma5")),
                weekly_ma20=_safe_float(weekly.get("ma20")),
                weekly_ma60=_safe_float(weekly.get("ma60")),
                weekly_ma120=_safe_float(weekly.get("ma120")),
                daily_ma5=ma5,
                daily_ma20=ma20,
                daily_ma60=ma60,
                daily_ma120=ma120,
                daily_close=_safe_float(daily_close.iloc[-1]),
                daily_traded_value_20d=traded_value_20,
                daily_alignment=alignment,
                correction_ratio=_safe_float(elliott.get("correction_ratio")),
                fib_rebound_low=_safe_float(elliott.get("rebound_min")),
                fib_rebound_high=_safe_float(elliott.get("rebound_max")),
                wave_target_1=_safe_float(elliott.get("target_1")),
                wave_target_2=_safe_float(elliott.get("target_2")),
                invalidation=_safe_float(elliott.get("invalidation")),
                score=float(score),
                reason=reason,
            )
        )

    recommendations.sort(key=lambda item: item.score, reverse=True)
    if max_count is None or int(max_count) <= 0:
        selected = recommendations
    else:
        selected = recommendations[: max(int(max_count), 1)]
    run_date = _today_kst()
    return {
        "run_date": run_date.isoformat(),
        "generated_at": pd.Timestamp.now(tz=KST).isoformat(),
        "items": [_as_dict(item) for item in selected],
        "meta": {
            "universe_scope": source_note,
            "kospi200_count": len(kospi200_codes),
            "kosdaq150_count": len(kosdaq150_codes),
            "candidate_pool_count": len(code_pool),
            "universe_scanned": len(symbols),
            "weekly_pass": len(weekly_pass_symbols),
            "daily_checked": len(daily_candidates),
            "selected": len(selected),
        },
    }


def load_recommendations_for(day: date) -> dict[str, Any] | None:
    payload = load_payload_cache(_cache_key(day))
    return payload if isinstance(payload, dict) else None


def load_recommendation_history(days: int = 90) -> dict[str, Any]:
    safe_days = max(1, int(days))
    records = list_payload_cache_by_prefix(RECOMMENDATION_CACHE_PREFIX, limit=safe_days + 30)

    daily_rows: list[dict[str, Any]] = []
    by_symbol: dict[str, dict[str, Any]] = {}
    previous_symbols: set[str] = set()

    # cache_key DESC -> ascending date order for change analysis
    records_sorted = sorted(records, key=lambda row: str(row.get("cache_key", "")))
    for row in records_sorted:
        cache_key = str(row.get("cache_key", ""))
        day_text = cache_key.replace(RECOMMENDATION_CACHE_PREFIX, "", 1).strip()
        payload = row.get("payload")
        if not isinstance(payload, dict):
            continue

        items = payload.get("items", [])
        if not isinstance(items, list):
            items = []

        symbol_map: dict[str, str] = {}
        for item in items:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol", "")).strip()
            if not symbol:
                continue
            name = str(item.get("name", "")).strip()
            symbol_map[symbol] = name or symbol

        current_symbols = set(symbol_map.keys())
        added = sorted(current_symbols - previous_symbols)
        removed = sorted(previous_symbols - current_symbols)
        stayed = sorted(current_symbols & previous_symbols)
        previous_symbols = current_symbols

        daily_rows.append(
            {
                "date": day_text,
                "count": len(current_symbols),
                "added_count": len(added),
                "removed_count": len(removed),
                "added_symbols": ", ".join(added[:12]) if added else "-",
                "removed_symbols": ", ".join(removed[:12]) if removed else "-",
            }
        )

        for symbol in current_symbols:
            entry = by_symbol.setdefault(
                symbol,
                {
                    "symbol": symbol,
                    "name": symbol_map.get(symbol, symbol),
                    "count": 0,
                    "first_date": day_text,
                    "last_date": day_text,
                    "active_today": False,
                    "last_removed_date": "",
                },
            )
            entry["name"] = symbol_map.get(symbol, entry.get("name", symbol))
            entry["count"] = int(entry.get("count", 0)) + 1
            entry["last_date"] = day_text

        for symbol in removed:
            entry = by_symbol.setdefault(
                symbol,
                {
                    "symbol": symbol,
                    "name": symbol,
                    "count": 0,
                    "first_date": day_text,
                    "last_date": day_text,
                    "active_today": False,
                    "last_removed_date": "",
                },
            )
            entry["last_removed_date"] = day_text

    if safe_days and len(daily_rows) > safe_days:
        daily_rows = daily_rows[-safe_days:]

    active_today = set()
    if daily_rows:
        latest_date = daily_rows[-1]["date"]
        for symbol, entry in by_symbol.items():
            is_active = str(entry.get("last_date", "")) == str(latest_date)
            entry["active_today"] = is_active
            if is_active:
                active_today.add(symbol)

    symbol_rows = sorted(
        by_symbol.values(),
        key=lambda row: (-int(row.get("count", 0)), str(row.get("symbol", ""))),
    )
    totals = {
        "days_loaded": len(daily_rows),
        "unique_symbols": len(symbol_rows),
        "active_today": len(active_today),
    }
    return {"daily": daily_rows, "symbols": symbol_rows, "totals": totals}


def ensure_recommendations_for_today(max_count: int | None = None) -> dict[str, Any]:
    now = pd.Timestamp.now(tz=KST)
    today = now.date()
    existing = load_recommendations_for(today)
    if isinstance(existing, dict):
        return existing

    if not _is_market_day(today):
        return {
            "run_date": today.isoformat(),
            "generated_at": now.isoformat(),
            "items": [],
            "meta": {"market_day": False, "message": "휴장일입니다. 종목추천 계산을 건너뜁니다."},
        }
    if int(now.hour) < 6:
        return {
            "run_date": today.isoformat(),
            "generated_at": now.isoformat(),
            "items": [],
            "meta": {"market_day": True, "message": "06:00 이전입니다. 당일 계산 전입니다."},
        }

    payload = generate_stock_recommendations(max_count=max_count)
    save_payload_cache(_cache_key(today), payload)
    return payload
