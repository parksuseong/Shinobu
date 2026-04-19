import sqlite3
from pathlib import Path
import pandas as pd
import numpy as np
from shinobu.strategy_src import _calculate_cci, _calculate_rsi, _calculate_stochastic_fast

P = {
    'st_os': 20, 'cci_os': -100, 'rsi_os': 30,
    'open_prev_need': 2, 'open_cross_need': 2,
    'cci9_window': 2, 'up_slope_need': 2,
    'st_ob': 80, 'cci_ob': 100, 'rsi_ob': 70,
    'close_cross_need': 2, 'down_slope_need': 2,
    'stop': 0.03, 'trail': 0.05,
    'm30_min': 16, 'h1_min': 18, 'h4_min': 18,
}

con = sqlite3.connect(Path('.streamlit/shinobu_cache.db'))

def load(sym):
    d = pd.read_sql_query('select ts,open,high,low,close,volume from raw_market_data where symbol=? order by ts', con, params=[sym])
    d['ts'] = pd.to_datetime(d['ts'])
    d = d.set_index('ts').sort_index()
    d.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    for c in d.columns:
        d[c] = pd.to_numeric(d[c], errors='coerce')
    return d.dropna(subset=['Open', 'High', 'Low', 'Close'])

def adx(frame, period=14):
    h, l, c = frame['High'], frame['Low'], frame['Close']
    up = h.diff(); dn = -l.diff()
    pdm = up.where((up > dn) & (up > 0), 0.0)
    mdm = dn.where((dn > up) & (dn > 0), 0.0)
    tr = pd.concat([h - l, (h - c.shift(1)).abs(), (l - c.shift(1)).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    pdi = 100 * (pdm.ewm(alpha=1/period, adjust=False, min_periods=period).mean() / atr.replace(0, np.nan))
    mdi = 100 * (mdm.ewm(alpha=1/period, adjust=False, min_periods=period).mean() / atr.replace(0, np.nan))
    dx = ((pdi - mdi).abs() / (pdi + mdi).replace(0, np.nan)) * 100
    adxv = dx.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    return adxv, pdi.fillna(0), mdi.fillna(0)

def enrich(df):
    o = df.copy()
    o['stoch'] = _calculate_stochastic_fast(o, 5).rolling(3, min_periods=1).mean()
    o['cci9'] = _calculate_cci(o, 9)
    o['cci20'] = _calculate_cci(o, 20)
    o['rsi'] = _calculate_rsi(o['Close'], 14)
    o['stoch_slope'] = o['stoch'].diff()
    o['cci20_slope'] = o['cci20'].diff()
    o['rsi_slope'] = o['rsi'].diff()

    for label, rule in [('m30', '30min'), ('h1', '60min'), ('h4', '240min')]:
        tf = df.resample(rule, origin='start_day', offset='9h', label='right', closed='right').agg({'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'}).dropna()
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

def signal(df):
    pr = df.shift(1)
    os = ((pr['stoch'] <= P['st_os']).astype(int) + (pr['cci20'] <= P['cci_os']).astype(int) + (pr['rsi'] <= P['rsi_os']).astype(int))
    cu = (((pr['stoch'] <= P['st_os']) & (df['stoch'] > P['st_os'])).astype(int) + ((pr['cci20'] <= P['cci_os']) & (df['cci20'] > P['cci_os'])).astype(int) + ((pr['rsi'] <= P['rsi_os']) & (df['rsi'] > P['rsi_os'])).astype(int))
    c9 = ((pr['cci9'] <= -100) & (df['cci9'] > -100)).rolling(P['cci9_window'], min_periods=1).max().astype(bool)
    up = ((df['stoch_slope'] > 0).astype(int) + (df['cci20_slope'] > 0).astype(int) + (df['rsi_slope'] > 0).astype(int))

    regime = (
        (df['m30_adx'] >= P['m30_min']) & df['m30_trend']
        & (df['h1_adx'] >= P['h1_min']) & df['h1_trend']
        & (df['h4_adx'] >= P['h4_min']) & df['h4_trend']
    )

    o = (os >= P['open_prev_need']) & (cu >= P['open_cross_need']) & (df['cci20'] > -100) & c9 & (up >= P['up_slope_need']) & regime

    cd = (((pr['stoch'] >= P['st_ob']) & (df['stoch'] < P['st_ob'])).astype(int) + ((pr['cci20'] >= P['cci_ob']) & (df['cci20'] < P['cci_ob'])).astype(int) + ((pr['rsi'] >= P['rsi_ob']) & (df['rsi'] < P['rsi_ob'])).astype(int))
    dn = ((df['stoch_slope'] < 0).astype(int) + (df['cci20_slope'] < 0).astype(int) + (df['rsi_slope'] < 0).astype(int))
    crash = (dn >= P['down_slope_need']) & ((df['stoch'] >= P['st_ob'] - 5) | (df['rsi'] >= P['rsi_ob'] - 5) | (df['cci20'] >= P['cci_ob'] - 15))
    c = (cd >= P['close_cross_need']) | crash

    return o.fillna(False), c.fillna(False)

L = load('122630')
S = load('252670')
idx = L.index.intersection(S.index)
L = enrich(L.loc[idx])
S = enrich(S.loc[idx])

lo, lc = signal(L)
so, sc = signal(S)

pos = None
entry = None
high = None
entry_ts = None
trades = []

for ts in idx:
    lp = float(L.at[ts, 'Close'])
    sp = float(S.at[ts, 'Close'])

    if pos is None:
        if lo.at[ts] and not so.at[ts]:
            pos = 'L'; entry = lp; high = lp; entry_ts = ts
        elif so.at[ts] and not lo.at[ts]:
            pos = 'S'; entry = sp; high = sp; entry_ts = ts
        continue

    if pos == 'L':
        high = max(high, lp)
        sw = bool(so.at[ts])
        close = bool(lc.at[ts]) or lp <= entry * (1 - P['stop']) or lp <= high * (1 - P['trail']) or sw
        if close:
            trades.append((entry_ts, ts, 'long', (lp / entry) - 1))
            if sw:
                pos = 'S'; entry = sp; high = sp; entry_ts = ts
            else:
                pos = None
    else:
        high = max(high, sp)
        sw = bool(lo.at[ts])
        close = bool(sc.at[ts]) or sp <= entry * (1 - P['stop']) or sp <= high * (1 - P['trail']) or sw
        if close:
            trades.append((entry_ts, ts, 'short', (sp / entry) - 1))
            if sw:
                pos = 'L'; entry = lp; high = lp; entry_ts = ts
            else:
                pos = None

tr = pd.DataFrame(trades, columns=['entry', 'exit', 'side', 'ret'])
tr['exit'] = pd.to_datetime(tr['exit'])
tr['win'] = tr['ret'] > 0
tr['week'] = tr['exit'].dt.to_period('W-MON').astype(str)

weekly = tr.groupby('week').agg(
    trades=('ret', 'size'),
    win_rate=('win', 'mean'),
    ret=('ret', lambda s: (1 + s).prod() - 1),
)
weekly['win_rate'] *= 100
weekly['ret'] *= 100

print(weekly.to_string(float_format=lambda x: f'{x:.2f}'))
