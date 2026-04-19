import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

from shinobu.strategy_src import _calculate_cci, _calculate_rsi, _calculate_stochastic_fast


DB_PATH = Path(".streamlit/shinobu_cache.db")


def load_symbol(con: sqlite3.Connection, symbol: str) -> pd.DataFrame:
    df = pd.read_sql_query(
        "select ts,open,high,low,close,volume from raw_market_data where symbol=? order by ts",
        con,
        params=[symbol],
    )
    df["ts"] = pd.to_datetime(df["ts"])
    df = df.set_index("ts").sort_index()
    df.columns = ["Open", "High", "Low", "Close", "Volume"]
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["Open", "High", "Low", "Close"])


def adx_components(frame: pd.DataFrame, n: int = 14):
    high, low, close = frame["High"], frame["Low"], frame["Close"]
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)
    tr = pd.concat(
        [high - low, (high - close.shift(1)).abs(), (low - close.shift(1)).abs()],
        axis=1,
    ).max(axis=1)
    atr = tr.ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    plus_di = 100 * (plus_dm.ewm(alpha=1 / n, adjust=False, min_periods=n).mean() / atr.replace(0, np.nan))
    minus_di = 100 * (minus_dm.ewm(alpha=1 / n, adjust=False, min_periods=n).mean() / atr.replace(0, np.nan))
    dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)) * 100
    adx = dx.ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    return adx, plus_di.fillna(0), minus_di.fillna(0)


def enrich(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["stoch"] = _calculate_stochastic_fast(out, 5).rolling(3, min_periods=1).mean()
    out["cci9"] = _calculate_cci(out, 9)
    out["cci20"] = _calculate_cci(out, 20)
    out["rsi"] = _calculate_rsi(out["Close"], 14)
    out["stoch_slope"] = out["stoch"].diff()
    out["cci20_slope"] = out["cci20"].diff()
    out["rsi_slope"] = out["rsi"].diff()

    adx5, pdi5, mdi5 = adx_components(out)
    out["adx5"] = adx5
    out["pdi5"] = pdi5
    out["mdi5"] = mdi5
    out["adx5_slope"] = out["adx5"].diff()
    out["pdi5_slope"] = out["pdi5"].diff()

    ema_fast = out["Close"].ewm(span=12, adjust=False).mean()
    ema_slow = out["Close"].ewm(span=26, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    macd_signal = macd_line.ewm(span=9, adjust=False).mean()
    out["macd_hist"] = macd_line - macd_signal
    out["macd_hist_delta"] = out["macd_hist"].diff()

    delta = out["Close"].diff().fillna(0.0)
    signed_vol = out["Volume"].where(delta >= 0, -out["Volume"])
    out["obv"] = signed_vol.cumsum().fillna(0.0)
    out["obv_ema"] = out["obv"].ewm(span=9, adjust=False).mean()
    out["obv_slope"] = out["obv"].diff(3)

    vol_ma = out["Volume"].rolling(20, min_periods=5).mean()
    out["vol_ratio"] = (out["Volume"] / vol_ma.replace(0, np.nan)).fillna(0.0)

    for label, rule in [("h1", "60min"), ("h4", "240min")]:
        tf = df.resample(rule, origin="start_day", offset="9h", label="right", closed="right").agg(
            {"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}
        ).dropna()
        tf["ema20"] = tf["Close"].ewm(span=20, adjust=False).mean()
        adx, pdi, mdi = adx_components(tf)
        tf[f"{label}_adx"] = adx
        tf[f"{label}_trend"] = (tf["Close"] >= tf["ema20"]) & (pdi >= mdi)
        out = pd.merge_asof(
            out.sort_index().reset_index().rename(columns={"index": "ts"}),
            tf[[f"{label}_adx", f"{label}_trend"]].sort_index().reset_index().rename(columns={"index": "ts"}),
            on="ts",
            direction="backward",
        ).set_index("ts")
        out[f"{label}_adx"] = out[f"{label}_adx"].ffill().fillna(0.0)
        out[f"{label}_trend"] = out[f"{label}_trend"].ffill().fillna(False)
    return out


def signal(df: pd.DataFrame, core: dict, post: dict):
    prev = df.shift(1)
    oversold_prev = (
        (prev["stoch"] <= core["st_os"]).astype(int)
        + (prev["cci20"] <= core["cci_os"]).astype(int)
        + (prev["rsi"] <= core["rsi_os"]).astype(int)
    )
    cross_up = (
        ((prev["stoch"] <= core["st_os"]) & (df["stoch"] > core["st_os"])).astype(int)
        + ((prev["cci20"] <= core["cci_os"]) & (df["cci20"] > core["cci_os"])).astype(int)
        + ((prev["rsi"] <= core["rsi_os"]) & (df["rsi"] > core["rsi_os"])).astype(int)
    )
    cci9_recover = ((prev["cci9"] <= -100) & (df["cci9"] > -100)).rolling(core["cci9_window"], min_periods=1).max().astype(bool)
    up_slope = (
        (df["stoch_slope"] > 0).astype(int)
        + (df["cci20_slope"] > 0).astype(int)
        + (df["rsi_slope"] > 0).astype(int)
    )
    regime = (
        (df["h1_adx"] >= core["h1_min"]) & df["h1_trend"]
        & (df["h4_adx"] >= core["h4_min"]) & df["h4_trend"]
    )
    open_sig = (
        (oversold_prev >= core["open_prev_need"])
        & (cross_up >= core["open_cross_need"])
        & (df["cci20"] > -100)
        & cci9_recover
        & (up_slope >= core["up_slope_need"])
        & regime
    )

    cond1 = df["vol_ratio"] >= post["vol"]
    cond2 = (df["pdi5"] > df["mdi5"]) & (df["adx5_slope"] >= post["adx_s"]) & (df["pdi5_slope"] >= post["pdi_s"])
    cond3 = (df["macd_hist"] > post["macd0"]) & (df["macd_hist_delta"] >= post["macd_d"])
    cond4 = (df["obv"] > df["obv_ema"]) & (df["obv_slope"] >= post["obv_s"])
    open_sig = open_sig & ((cond1.astype(int) + cond2.astype(int) + cond3.astype(int) + cond4.astype(int)) >= post["need"])

    close_cross = (
        ((prev["stoch"] >= core["st_ob"]) & (df["stoch"] < core["st_ob"])).astype(int)
        + ((prev["cci20"] >= core["cci_ob"]) & (df["cci20"] < core["cci_ob"])).astype(int)
        + ((prev["rsi"] >= core["rsi_ob"]) & (df["rsi"] < core["rsi_ob"])).astype(int)
    )
    down_slope = (
        (df["stoch_slope"] < 0).astype(int)
        + (df["cci20_slope"] < 0).astype(int)
        + (df["rsi_slope"] < 0).astype(int)
    )
    close_sig = (close_cross >= core["close_cross_need"]) | (
        (down_slope >= core["down_slope_need"])
        & (
            (df["stoch"] >= core["st_ob"] - 5)
            | (df["rsi"] >= core["rsi_ob"] - 5)
            | (df["cci20"] >= core["cci_ob"] - 15)
        )
    )
    return open_sig.fillna(False), close_sig.fillna(False)


def backtest(long_df: pd.DataFrame, short_df: pd.DataFrame, index: pd.DatetimeIndex, core: dict, post: dict):
    l_open, l_close = signal(long_df, core, post)
    s_open, s_close = signal(short_df, core, post)
    position = None
    entry = None
    high_since = None
    equity = 1.0
    peak = 1.0
    mdd = 0.0
    trades = []
    for ts in index:
        lp = float(long_df.at[ts, "Close"])
        sp = float(short_df.at[ts, "Close"])
        if position is None:
            if l_open.at[ts] and not s_open.at[ts]:
                position = "L"
                entry = lp
                high_since = lp
            elif s_open.at[ts] and not l_open.at[ts]:
                position = "S"
                entry = sp
                high_since = sp
            continue

        if position == "L":
            high_since = max(high_since, lp)
            switch = bool(s_open.at[ts])
            close_now = bool(l_close.at[ts]) or lp <= entry * (1 - core["stop"]) or lp <= high_since * (1 - core["trail"]) or switch
            if close_now:
                ret = (lp / entry) - 1
                equity *= (1 + ret)
                peak = max(peak, equity)
                mdd = max(mdd, 1 - equity / peak)
                trades.append(ret)
                if switch:
                    position = "S"
                    entry = sp
                    high_since = sp
                else:
                    position = None
        else:
            high_since = max(high_since, sp)
            switch = bool(l_open.at[ts])
            close_now = bool(s_close.at[ts]) or sp <= entry * (1 - core["stop"]) or sp <= high_since * (1 - core["trail"]) or switch
            if close_now:
                ret = (sp / entry) - 1
                equity *= (1 + ret)
                peak = max(peak, equity)
                mdd = max(mdd, 1 - equity / peak)
                trades.append(ret)
                if switch:
                    position = "L"
                    entry = lp
                    high_since = lp
                else:
                    position = None

    tseries = pd.Series(trades)
    return {
        "cum": (equity - 1) * 100,
        "mdd": mdd * 100,
        "win": float((tseries > 0).mean() * 100) if len(tseries) else 0.0,
        "trades": int(len(tseries)),
    }


def main():
    con = sqlite3.connect(DB_PATH)
    long_df = load_symbol(con, "122630")
    short_df = load_symbol(con, "252670")
    idx = long_df.index.intersection(short_df.index)
    long_df = enrich(long_df.loc[idx])
    short_df = enrich(short_df.loc[idx])

    rng = np.random.default_rng(42)
    core_space = {
        "open_prev_need": [1, 2],
        "open_cross_need": [1, 2],
        "up_slope_need": [1, 2],
        "close_cross_need": [1, 2],
        "down_slope_need": [1, 2],
        "h1_min": [15, 18],
        "h4_min": [15, 18],
    }
    post_space = {
        "vol": [0.85, 0.9, 1.0],
        "need": [1, 2],
        "adx_s": [0.0, 0.05],
        "pdi_s": [0.0, 0.3],
        "macd0": [-0.03, -0.02, -0.01],
        "macd_d": [-0.03, -0.02, -0.01],
        "obv_s": [-10000, 0],
    }
    static_core = {
        "st_os": 20, "cci_os": -100, "rsi_os": 30,
        "cci9_window": 2, "st_ob": 80, "cci_ob": 100, "rsi_ob": 70,
        "stop": 0.03, "trail": 0.05,
    }

    samples = []
    seen = set()
    n = 420
    while len(samples) < n:
        core = {k: rng.choice(v).item() if hasattr(rng.choice(v), "item") else rng.choice(v) for k, v in core_space.items()}
        post = {k: rng.choice(v).item() if hasattr(rng.choice(v), "item") else rng.choice(v) for k, v in post_space.items()}
        key = tuple(core[k] for k in sorted(core)) + tuple(post[k] for k in sorted(post))
        if key in seen:
            continue
        seen.add(key)
        full_core = {**static_core, **core}
        result = backtest(long_df, short_df, idx, full_core, post)
        score = result["cum"] + 0.03 * result["trades"] - 0.4 * result["mdd"]
        samples.append((score, full_core, post, result))

    samples.sort(key=lambda x: x[0], reverse=True)
    aggressive = [x for x in samples if x[3]["trades"] >= 90]
    neutral = [x for x in samples if 70 <= x[3]["trades"] < 90]

    print("AGGRESSIVE_TOP3")
    for i, (score, core, post, result) in enumerate(aggressive[:3], 1):
        print(
            f"#{i} score={score:.2f} cum={result['cum']:.2f}% mdd={result['mdd']:.2f}% "
            f"win={result['win']:.1f}% trades={result['trades']} core={core} post={post}"
        )

    print("\nNEUTRAL_TOP3")
    for i, (score, core, post, result) in enumerate(neutral[:3], 1):
        print(
            f"#{i} score={score:.2f} cum={result['cum']:.2f}% mdd={result['mdd']:.2f}% "
            f"win={result['win']:.1f}% trades={result['trades']} core={core} post={post}"
        )

    print(f"\nmax_trades_seen={max(x[3]['trades'] for x in samples)}")


if __name__ == "__main__":
    main()
