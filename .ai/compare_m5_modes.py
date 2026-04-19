import sqlite3
from pathlib import Path
import pandas as pd
import numpy as np
from shinobu.strategy_src import _calculate_cci, _calculate_rsi, _calculate_stochastic_fast

P = {
    'st_os': 20,
    'cci_os': -100,
    'rsi_os': 30,
    'open_prev_need': 2,
    'open_cross_need': 2,
    'cci9_window': 2,
    'up_slope_need': 2,
    'st_ob': 80,
    'cci_ob': 100,
    'rsi_ob': 70,
    'close_cross_need': 2,
    'down_slope_need': 2,
    'stop': 0.03,
    'trail': 0.05,
    'm30_min': 16,
    'h1_min': 18,
    'h4_min': 18,
}

con = sqlite3.connect(Path('.streamlit/shinobu_cache.db'))


def load(sym: str) -> pd.DataFrame:
    d = pd.read_sql_query('select ts,open,high,low,close,volume from raw_market_data where symbol=? order by ts', con, params=[sym])
    d['ts'] = pd.to_datetime(d['ts'])
    d = d.set_index('ts').sort_index()
    d.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    for c in d.columns:
        d[c] = pd.to_numeric(d[c], errors='coerce')
    return d.dropna(subset=['Open', 'High', 'Low', 'Close'])


def adx(frame: pd.DataFrame, period: int = 14):
    h, l, c = frame['High'], frame['Low'], frame['Close']
    up = h.diff(); down = -l.diff()
    pdm = up.where((up > down) & (up > 0), 0.0)
    mdm = down.where((down > up) & (down > 0), 0.0)
    tr = pd.concat([h - l, (h - c.shift(1)).abs(), (l - c.shift(1)).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    pdi = 100 * (pdm.ewm(alpha=1 / period, adjust=False, min_periods=period).mean() / atr.replace(0, np.nan))
    mdi = 100 * (mdm.ewm(alpha=1 / period, adjust=False, min_periods=period).mean() / atr.replace(0, np.nan))
    dx = ((pdi - mdi).abs() / (pdi + mdi).replace(0, np.nan)) * 100
    adxv = dx.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    return adxv, pdi.fillna(0), mdi.fillna(0)


def enrich(df: pd.DataFrame) -> pd.DataFrame:
    o = df.copy()
    o['stoch'] = _calculate_stochastic_fast(o, 5).rolling(3, min_periods=1).mean()
    o['cci9'] = _calculate_cci(o, 9)
    o['cci20'] = _calculate_cci(o, 20)
    o['rsi'] = _calculate_rsi(o['Close'], 14)
    o['stoch_slope'] = o['stoch'].diff()
    o['cci20_slope'] = o['cci20'].diff()
    o['rsi_slope'] = o['rsi'].diff()

    for label, rule in [('m30', '30min'), ('h1', '60min'), ('h4', '240min')]:
        tf = df.resample(rule, origin='start_day', offset='9h', label='right', closed='right').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}).dropna()
        tf['ema20'] = tf['Close'].ewm(span=20, adjust=False).mean()
        a, pdi, mdi = adx(tf)
        tf[f'{label}_adx'] = a
        tf[f'{label}_trend'] = (tf['Close'] >= tf['ema20']) & (pdi >= mdi)

        o = pd.merge_asof(
            o.sort_index().reset_index().rename(columns={'index': 'ts'}),
            tf[[f'{label}_adx', f'{label}_trend']].sort_index().reset_index().rename(columns={'index': 'ts'}),
            on='ts', direction='backward'
        ).set_index('ts')
        o[f'{label}_adx'] = o[f'{label}_adx'].ffill().fillna(0.0)
        o[f'{label}_trend'] = o[f'{label}_trend'].ffill().fillna(False)
    return o


def make_signal(df: pd.DataFrame, mode: str):
    pr = df.shift(1)
    os = ((pr['stoch'] <= P['st_os']).astype(int) + (pr['cci20'] <= P['cci_os']).astype(int) + (pr['rsi'] <= P['rsi_os']).astype(int))
    cu = (((pr['stoch'] <= P['st_os']) & (df['stoch'] > P['st_os'])).astype(int) + ((pr['cci20'] <= P['cci_os']) & (df['cci20'] > P['cci_os'])).astype(int) + ((pr['rsi'] <= P['rsi_os']) & (df['rsi'] > P['rsi_os'])).astype(int))
    c9 = ((pr['cci9'] <= -100) & (df['cci9'] > -100)).rolling(P['cci9_window'], min_periods=1).max().astype(bool)
    up = ((df['stoch_slope'] > 0).astype(int) + (df['cci20_slope'] > 0).astype(int) + (df['rsi_slope'] > 0).astype(int))

    if mode == 'm5_only':
        regime = pd.Series(True, index=df.index)
    elif mode == 'm5_m30':
        regime = (df['m30_adx'] >= P['m30_min']) & df['m30_trend']
    elif mode == 'm5_m30_h1':
        regime = (df['m30_adx'] >= P['m30_min']) & df['m30_trend'] & (df['h1_adx'] >= P['h1_min']) & df['h1_trend']
    else:
        raise ValueError(mode)

    open_sig = (os >= P['open_prev_need']) & (cu >= P['open_cross_need']) & (df['cci20'] > -100) & c9 & (up >= P['up_slope_need']) & regime

    cd = (((pr['stoch'] >= P['st_ob']) & (df['stoch'] < P['st_ob'])).astype(int) + ((pr['cci20'] >= P['cci_ob']) & (df['cci20'] < P['cci_ob'])).astype(int) + ((pr['rsi'] >= P['rsi_ob']) & (df['rsi'] < P['rsi_ob'])).astype(int))
    dn = ((df['stoch_slope'] < 0).astype(int) + (df['cci20_slope'] < 0).astype(int) + (df['rsi_slope'] < 0).astype(int))
    crash = (dn >= P['down_slope_need']) & ((df['stoch'] >= P['st_ob'] - 5) | (df['rsi'] >= P['rsi_ob'] - 5) | (df['cci20'] >= P['cci_ob'] - 15))
    close_sig = (cd >= P['close_cross_need']) | crash
    return open_sig.fillna(False), close_sig.fillna(False)


def backtest(L: pd.DataFrame, S: pd.DataFrame, idx: pd.DatetimeIndex, mode: str):
    lo, lc = make_signal(L, mode)
    so, sc = make_signal(S, mode)

    pos = None; entry = None; high = None; et = None
    eq = 1.0; peak = 1.0; mdd = 0.0
    rows = []

    for ts in idx:
        lp = float(L.at[ts, 'Close'])
        sp = float(S.at[ts, 'Close'])

        if pos is None:
            if lo.at[ts] and not so.at[ts]:
                pos = 'L'; entry = lp; high = lp; et = ts
            elif so.at[ts] and not lo.at[ts]:
                pos = 'S'; entry = sp; high = sp; et = ts
            continue

        if pos == 'L':
            high = max(high, lp)
            sw = bool(so.at[ts])
            close = bool(lc.at[ts]) or lp <= entry * (1 - P['stop']) or lp <= high * (1 - P['trail']) or sw
            if close:
                r = (lp / entry) - 1
                eq *= (1 + r); peak = max(peak, eq); mdd = max(mdd, 1 - eq / peak)
                rows.append((et, ts, r))
                if sw:
                    pos = 'S'; entry = sp; high = sp; et = ts
                else:
                    pos = None
        else:
            high = max(high, sp)
            sw = bool(lo.at[ts])
            close = bool(sc.at[ts]) or sp <= entry * (1 - P['stop']) or sp <= high * (1 - P['trail']) or sw
            if close:
                r = (sp / entry) - 1
                eq *= (1 + r); peak = max(peak, eq); mdd = max(mdd, 1 - eq / peak)
                rows.append((et, ts, r))
                if sw:
                    pos = 'L'; entry = lp; high = lp; et = ts
                else:
                    pos = None

    tr = pd.DataFrame(rows, columns=['entry', 'exit', 'ret'])
    win = float((tr['ret'] > 0).mean() * 100) if len(tr) else 0.0
    return {
        'cum': (eq - 1) * 100,
        'mdd': mdd * 100,
        'win': win,
        'trades': int(len(tr)),
    }


L = load('122630')
S = load('252670')
idx = L.index.intersection(S.index)
L = enrich(L.loc[idx])
S = enrich(S.loc[idx])

for mode in ['m5_only', 'm5_m30', 'm5_m30_h1']:
    r = backtest(L, S, idx, mode)
    print(f"{mode}: cum={r['cum']:.2f}% mdd={r['mdd']:.2f}% win={r['win']:.1f}% trades={r['trades']}")
