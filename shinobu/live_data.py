from __future__ import annotations

from datetime import datetime

import pandas as pd
import streamlit as st

from shinobu.kis import _fetch_domestic_intraday_batch, fetch_domestic_intraday_history


LIVE_RECENT_LOOKBACK_MINUTES = 720


@st.cache_data(ttl=1800, show_spinner=False)
def load_intraday_seed(symbol: str, lookback_days: int = 5) -> pd.DataFrame:
    return fetch_domestic_intraday_history(symbol, lookback_days=lookback_days)


@st.cache_data(ttl=5, show_spinner=False)
def load_intraday_recent(symbol: str, lookback_minutes: int = LIVE_RECENT_LOOKBACK_MINUTES) -> pd.DataFrame:
    end_dt = pd.Timestamp(datetime.now().replace(second=0, microsecond=0))
    batch = _fetch_domestic_intraday_batch(symbol, end_dt)
    if batch.empty:
        return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])

    cutoff = end_dt - pd.Timedelta(minutes=max(int(lookback_minutes), 30))
    batch = batch.drop_duplicates(subset=["시간"]).sort_values("시간")
    batch = batch[batch["시간"] >= cutoff]
    if batch.empty:
        return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])
    return batch.set_index("시간")[["Open", "High", "Low", "Close", "Volume"]]


def merge_intraday_frames(seed_frame: pd.DataFrame, recent_frame: pd.DataFrame) -> pd.DataFrame:
    if seed_frame.empty:
        return recent_frame.copy()
    if recent_frame.empty:
        return seed_frame.copy()

    cutoff = recent_frame.index.min()
    combined = pd.concat([seed_frame[seed_frame.index < cutoff], recent_frame]).sort_index()
    combined = combined[~combined.index.duplicated(keep="last")]
    return combined
