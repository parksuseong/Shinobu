from __future__ import annotations

import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

from config import get_secret


KIS_BASE_URL = "https://openapi.koreainvestment.com:9443"
KIS_MIN_REQUEST_INTERVAL = 0.35
KIS_RETRY_DELAY_SECONDS = 0.7
KIS_MAX_RETRIES = 3
_KIS_REQUEST_LOCK = threading.RLock()
_KIS_LAST_REQUEST_AT = 0.0
KIS_TOKEN_FILE = Path(__file__).resolve().parent.parent / ".streamlit" / "kis_token.json"


class KisApiError(RuntimeError):
    pass


def _read_cached_token() -> str | None:
    try:
        payload = json.loads(KIS_TOKEN_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None

    token = str(payload.get("access_token") or "").strip()
    expires_at = str(payload.get("expires_at") or "").strip()
    if not token or not expires_at:
        return None

    try:
        expires_ts = datetime.fromisoformat(expires_at)
    except ValueError:
        return None

    if expires_ts <= datetime.now() + timedelta(minutes=5):
        return None
    return token


def _write_cached_token(token: str, expires_in_seconds: int) -> None:
    KIS_TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    expires_at = datetime.now() + timedelta(seconds=max(expires_in_seconds - 300, 0))
    payload = {
        "access_token": token,
        "expires_at": expires_at.isoformat(timespec="seconds"),
    }
    KIS_TOKEN_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _respect_rate_limit() -> None:
    global _KIS_LAST_REQUEST_AT
    with _KIS_REQUEST_LOCK:
        now = time.monotonic()
        wait_seconds = KIS_MIN_REQUEST_INTERVAL - (now - _KIS_LAST_REQUEST_AT)
        if wait_seconds > 0:
            time.sleep(wait_seconds)
        _KIS_LAST_REQUEST_AT = time.monotonic()


def _is_rate_limit_error(detail: str) -> bool:
    return "EGW00201" in detail or "초당 거래건수" in detail


def _request_json(method: str, url: str, headers: dict[str, str] | None = None, body: dict | None = None) -> dict:
    payload = None
    request_headers = headers.copy() if headers else {}
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
        request_headers["content-type"] = "application/json"

    last_error: KisApiError | None = None
    for attempt in range(KIS_MAX_RETRIES + 1):
        _respect_rate_limit()
        request = urllib.request.Request(url=url, method=method, headers=request_headers, data=payload)
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                content = response.read().decode("utf-8")
                return json.loads(content)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")
            if _is_rate_limit_error(detail) and attempt < KIS_MAX_RETRIES:
                time.sleep(KIS_RETRY_DELAY_SECONDS * (attempt + 1))
                continue
            last_error = KisApiError(f"한투 요청 실패 ({exc.code}): {detail}")
            break
        except urllib.error.URLError as exc:
            if attempt < KIS_MAX_RETRIES:
                time.sleep(KIS_RETRY_DELAY_SECONDS * (attempt + 1))
                continue
            last_error = KisApiError(f"한투 요청 실패: {exc}")
            break

    if last_error is not None:
        raise last_error
    raise KisApiError("한투 요청 실패")


def issue_access_token() -> str:
    cached_token = _read_cached_token()
    if cached_token:
        return cached_token

    app_key = get_secret("KIS_APP_KEY")
    app_secret = get_secret("KIS_APP_SECRET")
    if not app_key or not app_secret:
        raise KisApiError("íí¬ API í¤ê° ììµëë¤.")

    response = _request_json(
        "POST",
        f"{KIS_BASE_URL}/oauth2/tokenP",
        body={
            "grant_type": "client_credentials",
            "appkey": app_key,
            "appsecret": app_secret,
        },
    )
    token = response.get("access_token")
    if not token:
        raise KisApiError(f"íí¬ í í° ë°ê¸ ì¤í¨: {response}")

    expires_in = int(response.get("expires_in") or 86400)
    _write_cached_token(str(token), expires_in)
    return str(token)


def _build_headers(tr_id: str) -> dict[str, str]:
    return {
        "authorization": f"Bearer {issue_access_token()}",
        "appkey": get_secret("KIS_APP_KEY"),
        "appsecret": get_secret("KIS_APP_SECRET"),
        "tr_id": tr_id,
        "custtype": "P",
    }


def _is_real_account() -> bool:
    return get_secret("KIS_IS_REAL", "true").strip().lower() not in {"false", "0", "n", "no", "demo", "mock"}


def _account_params() -> tuple[str, str]:
    cano = get_secret("KIS_CANO")
    acnt_prdt_cd = get_secret("KIS_ACNT_PRDT_CD", "01")
    if not cano or not acnt_prdt_cd:
        raise KisApiError("한투 계좌 정보가 없습니다.")
    return cano, acnt_prdt_cd


def _issue_hashkey(body: dict) -> str:
    response = _request_json(
        "POST",
        f"{KIS_BASE_URL}/uapi/hashkey",
        headers={
            "appkey": get_secret("KIS_APP_KEY"),
            "appsecret": get_secret("KIS_APP_SECRET"),
        },
        body=body,
    )
    hashkey = response.get("HASH")
    if not hashkey:
        raise KisApiError(f"한투 hashkey 발급 실패: {response}")
    return str(hashkey)


def _parse_kis_date(date_text: str, time_text: str = "") -> pd.Timestamp:
    if time_text:
        return pd.Timestamp(datetime.strptime(f"{date_text}{time_text}", "%Y%m%d%H%M%S"))
    return pd.Timestamp(datetime.strptime(date_text, "%Y%m%d"))


@st.cache_data(ttl=10, show_spinner=False)
def fetch_domestic_balance() -> tuple[pd.DataFrame, dict]:
    cano, acnt_prdt_cd = _account_params()
    tr_id = "TTTC8434R" if _is_real_account() else "VTTC8434R"
    params = {
        "CANO": cano,
        "ACNT_PRDT_CD": acnt_prdt_cd,
        "AFHR_FLPR_YN": "N",
        "OFL_YN": "",
        "INQR_DVSN": "02",
        "UNPR_DVSN": "01",
        "FUND_STTL_ICLD_YN": "N",
        "FNCG_AMT_AUTO_RDPT_YN": "N",
        "PRCS_DVSN": "00",
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": "",
    }
    url = f"{KIS_BASE_URL}/uapi/domestic-stock/v1/trading/inquire-balance?{urllib.parse.urlencode(params)}"
    response = _request_json("GET", url, headers=_build_headers(tr_id))

    output1 = response.get("output1", [])
    output2 = response.get("output2", [])
    summary_raw = output2[0] if output2 else {}

    positions = []
    for item in output1:
        quantity = float(item.get("hldg_qty") or 0)
        if quantity <= 0:
            continue
        positions.append(
            {
                "code": str(item.get("pdno", "")),
                "name": str(item.get("prdt_name", "")),
                "quantity": quantity,
                "avg_price": float(item.get("pchs_avg_pric") or 0),
                "current_price": float(item.get("prpr") or 0),
                "eval_amount": float(item.get("evlu_amt") or 0),
                "profit_amount": float(item.get("evlu_pfls_amt") or 0),
                "profit_rate": float(item.get("evlu_pfls_rt") or 0),
            }
        )

    positions_frame = pd.DataFrame(positions)
    if not positions_frame.empty and "code" in positions_frame.columns:
        numeric_columns = [
            column
            for column in ["quantity", "avg_price", "current_price", "eval_amount", "profit_amount", "profit_rate"]
            if column in positions_frame.columns
        ]
        positions_frame = (
            positions_frame.groupby(["code", "name"], as_index=False)
            .agg(
                {
                    **{column: "sum" for column in ["quantity", "eval_amount", "profit_amount"] if column in numeric_columns},
                    **{column: "last" for column in ["avg_price", "current_price", "profit_rate"] if column in numeric_columns},
                }
            )
            .sort_values(["eval_amount", "quantity"], ascending=False)
            .reset_index(drop=True)
        )

    summary = {
        "cash": float(summary_raw.get("dnca_tot_amt") or 0),
        "orderable_cash": float(summary_raw.get("nxdy_excc_amt") or 0),
        "purchase_amount": float(summary_raw.get("pchs_amt_smtl_amt") or 0),
        "eval_amount": float(summary_raw.get("evlu_amt_smtl_amt") or 0),
        "profit_amount": float(summary_raw.get("evlu_pfls_smtl_amt") or 0),
        "total_assets": float(summary_raw.get("tot_evlu_amt") or 0),
        "account_number": f"{cano}-{acnt_prdt_cd}",
    }
    return positions_frame, summary


def place_domestic_order(symbol: str, side: str, quantity: int, order_type: str = "01", price: str = "0") -> dict:
    if side not in {"buy", "sell"}:
        raise KisApiError("주문 방향이 잘못되었습니다.")
    if quantity <= 0:
        raise KisApiError("주문 수량은 1주 이상이어야 합니다.")

    cano, acnt_prdt_cd = _account_params()
    tr_id_map = {
        ("buy", True): "TTTC0802U",
        ("sell", True): "TTTC0801U",
        ("buy", False): "VTTC0802U",
        ("sell", False): "VTTC0801U",
    }
    body = {
        "CANO": cano,
        "ACNT_PRDT_CD": acnt_prdt_cd,
        "PDNO": symbol,
        "ORD_DVSN": order_type,
        "ORD_QTY": str(int(quantity)),
        "ORD_UNPR": str(price),
        "CTAC_TLNO": "",
        "SLL_TYPE": "01" if side == "sell" else "",
        "ALGO_NO": "",
    }
    headers = _build_headers(tr_id_map[(side, _is_real_account())])
    headers["hashkey"] = _issue_hashkey(body)

    response = _request_json(
        "POST",
        f"{KIS_BASE_URL}/uapi/domestic-stock/v1/trading/order-cash",
        headers=headers,
        body=body,
    )
    output = response.get("output", {})
    if not output:
        raise KisApiError(f"한투 주문 실패: {response}")
    fetch_domestic_balance.clear()
    fetch_domestic_daily_ccld.clear()
    return output


@st.cache_data(ttl=15, show_spinner=False)
def fetch_domestic_daily_ccld(start_date: str, end_date: str, symbol: str = "", max_pages: int = 3) -> pd.DataFrame:
    cano, acnt_prdt_cd = _account_params()
    tr_id = "TTTC0081R" if _is_real_account() else "VTTC0081R"
    params = {
        "CANO": cano,
        "ACNT_PRDT_CD": acnt_prdt_cd,
        "INQR_STRT_DT": start_date,
        "INQR_END_DT": end_date,
        "SLL_BUY_DVSN_CD": "00",
        "INQR_DVSN": "00",
        "PDNO": symbol.replace(".KS", "").strip(),
        "CCLD_DVSN": "01",
        "ORD_GNO_BRNO": "",
        "ODNO": "",
        "INQR_DVSN_3": "00",
        "INQR_DVSN_1": "",
        "EXCG_ID_DVSN_CD": "ALL",
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": "",
    }

    rows: list[dict[str, object]] = []
    page_count = 0
    while True:
        page_count += 1
        url = f"{KIS_BASE_URL}/uapi/domestic-stock/v1/trading/inquire-daily-ccld?{urllib.parse.urlencode(params)}"
        response = _request_json("GET", url, headers=_build_headers(tr_id))
        items = response.get("output1", [])
        if not isinstance(items, list):
            items = []

        for item in items:
            side_code = str(item.get("sll_buy_dvsn_cd") or "")
            side = "buy" if side_code == "02" else "sell" if side_code == "01" else ""
            qty = float(item.get("tot_ccld_qty") or item.get("ccld_qty_sum") or item.get("ord_qty") or 0)
            price = float(item.get("tot_ccld_unpr") or item.get("avg_prvs") or item.get("avg_prvs_unpr") or item.get("ord_unpr") or 0)
            if qty <= 0 or price <= 0 or not side:
                continue

            order_date = str(item.get("ord_dt") or item.get("trad_dt") or item.get("dt") or "").strip()
            order_time = str(item.get("ord_tmd") or item.get("ord_tm") or item.get("ccld_dtime") or "000000").strip()
            timestamp = None
            if order_date:
                try:
                    timestamp = _parse_kis_date(order_date, order_time[:6].ljust(6, "0"))
                except ValueError:
                    timestamp = _parse_kis_date(order_date)

            rows.append(
                {
                    "symbol": f"{str(item.get('pdno') or '').strip()}.KS",
                    "name": str(item.get("prdt_name") or item.get("prdt_abrv_name") or "").strip(),
                    "side": side,
                    "quantity": qty,
                    "price": price,
                    "amount": qty * price,
                    "timestamp": timestamp,
                    "order_no": str(item.get("odno") or "").strip(),
                    "order_branch": str(item.get("ord_gno_brno") or "").strip(),
                }
            )

        fk = str(response.get("ctx_area_fk100") or "").strip()
        nk = str(response.get("ctx_area_nk100") or "").strip()
        if (not fk and not nk) or page_count >= max(int(max_pages), 1):
            break
        params["CTX_AREA_FK100"] = fk
        params["CTX_AREA_NK100"] = nk

    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    if "timestamp" in frame.columns:
        frame = frame.dropna(subset=["timestamp"]).sort_values("timestamp")
    return frame.reset_index(drop=True)


@st.cache_data(ttl=300, show_spinner=False)
def fetch_domestic_daily(symbol: str, period_code: str) -> pd.DataFrame:
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=3650)).strftime("%Y%m%d")
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": symbol,
        "FID_INPUT_DATE_1": start_date,
        "FID_INPUT_DATE_2": end_date,
        "FID_PERIOD_DIV_CODE": period_code,
        "FID_ORG_ADJ_PRC": "0",
    }
    url = f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice?{urllib.parse.urlencode(params)}"
    response = _request_json("GET", url, headers=_build_headers("FHKST03010100"))
    items = response.get("output2", [])
    if not items:
        raise KisApiError(f"한투 일봉 데이터 응답이 비었습니다: {response}")

    rows = []
    for item in reversed(items):
        rows.append(
            {
                "시간": _parse_kis_date(item["stck_bsop_date"]),
                "Open": float(item["stck_oprc"]),
                "High": float(item["stck_hgpr"]),
                "Low": float(item["stck_lwpr"]),
                "Close": float(item["stck_clpr"]),
                "Volume": float(item["acml_vol"]),
            }
        )

    return pd.DataFrame(rows).set_index("시간").sort_index()


def _fetch_domestic_intraday_batch(symbol: str, cursor: pd.Timestamp) -> pd.DataFrame:
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": symbol,
        "FID_INPUT_DATE_1": cursor.strftime("%Y%m%d"),
        "FID_INPUT_HOUR_1": cursor.strftime("%H%M%S"),
        "FID_PW_DATA_INCU_YN": "Y",
        "FID_FAKE_TICK_INCU_YN": "",
    }
    url = f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice?{urllib.parse.urlencode(params)}"
    response = _request_json("GET", url, headers=_build_headers("FHKST03010230"))
    items = response.get("output2", [])
    if not items:
        return pd.DataFrame(columns=["시간", "Open", "High", "Low", "Close", "Volume"])

    rows = []
    for item in items:
        rows.append(
            {
                "시간": _parse_kis_date(item["stck_bsop_date"], item["stck_cntg_hour"]),
                "Open": float(item["stck_oprc"]),
                "High": float(item["stck_hgpr"]),
                "Low": float(item["stck_lwpr"]),
                "Close": float(item["stck_prpr"]),
                "Volume": float(item["cntg_vol"]),
            }
        )
    return pd.DataFrame(rows)


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_domestic_intraday_history(
    symbol: str,
    lookback_days: int = 365,
    max_requests: int = 900,
) -> pd.DataFrame:
    end_dt = pd.Timestamp(datetime.now().replace(second=0, microsecond=0))
    start_dt = end_dt - pd.Timedelta(days=lookback_days)
    cursor = end_dt.replace(hour=15, minute=30)
    if end_dt.hour < 15 or (end_dt.hour == 15 and end_dt.minute < 30):
        cursor = end_dt

    frames: list[pd.DataFrame] = []
    request_count = 0
    last_oldest: pd.Timestamp | None = None

    while cursor >= start_dt and request_count < max_requests:
        batch = _fetch_domestic_intraday_batch(symbol, cursor)
        request_count += 1

        if batch.empty:
            cursor = (cursor.normalize() - pd.Timedelta(days=1)).replace(hour=15, minute=30)
            continue

        batch = batch.drop_duplicates(subset=["시간"]).sort_values("시간", ascending=False)
        batch = batch[batch["시간"] <= cursor]
        if batch.empty:
            break

        frames.append(batch)
        oldest = pd.Timestamp(batch["시간"].min())
        if last_oldest is not None and oldest >= last_oldest:
            break
        last_oldest = oldest
        cursor = oldest - pd.Timedelta(minutes=1)

    if not frames:
        raise KisApiError("한투 분봉 데이터를 가져오지 못했습니다.")

    frame = pd.concat(frames, ignore_index=True)
    frame = frame.drop_duplicates(subset=["시간"]).sort_values("시간")
    frame = frame[frame["시간"] >= start_dt]
    return frame.set_index("시간")[["Open", "High", "Low", "Close", "Volume"]]
