import sqlite3
from pathlib import Path
import pandas as pd
import numpy as np
from shinobu.strategy_src import _calculate_cci, _calculate_rsi, _calculate_stochastic_fast

BASE = {
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


def adx_components(frame: pd.DataFrame, period: int = 14):
    h, l, c = frame['High'], frame['Low'], frame['Close']
    up = h.diff(); dn = -l.diff()
    pdm = up.where((up > dn) & (up > 0), 0.0)
    mdm = dn.where((dn > up) & (dn > 0), 0.0)
    tr = pd.concat([h - l, (h - c.shift(1)).abs(), (l - c.shift(1)).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    pdi = 100 * (pdm.ewm(alpha=1 / period, adjust=False, min_periods=period).mean() / atr.replace(0, np.nan))
    mdi = 100 * (mdm.ewm(alpha=1 / period, adjust=False, min_periods=period).mean() / atr.replace(0, np.nan))
    dx = ((pdi - mdi).abs() / (pdi + mdi).replace(0, np.nan)) * 100
    adx = dx.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    return adx, pdi.fillna(0.0), mdi.fillna(0.0)


def macd(close: pd.Series, fast=12, slow=26, signal=9):
    ema_f = close.ewm(span=fast, adjust=False).mean()
    ema_s = close.ewm(span=slow, adjust=False).mean()
    line = ema_f - ema_s
    sig = line.ewm(span=signal, adjust=False).mean()
    hist = line - sig
    return line, sig, hist


def obv(close: pd.Series, volume: pd.Series):
    direction = close.diff().fillna(0.0)
    signed = volume.where(direction >= 0, -volume)
    return signed.cumsum().fillna(0.0)


def enrich(df: pd.DataFrame) -> pd.DataFrame:
    o = df.copy()
    o['stoch'] = _calculate_stochastic_fast(o, 5).rolling(3, min_periods=1).mean()
    o['cci9'] = _calculate_cci(o, 9)
    o['cci20'] = _calculate_cci(o, 20)
    o['rsi'] = _calculate_rsi(o['Close'], 14)
    o['stoch_slope'] = o['stoch'].diff()
    o['cci20_slope'] = o['cci20'].diff()
    o['rsi_slope'] = o['rsi'].diff()

    # 5m DMI/ADX + MACD + OBV + volume expansion
    o['adx5'], o['pdi5'], o['mdi5'] = adx_components(o)
    o['adx5_slope'] = o['adx5'].diff()
    o['pdi5_slope'] = o['pdi5'].diff()

    _, _, hist = macd(o['Close'])
    o['macd_hist'] = hist
    o['macd_hist_delta'] = o['macd_hist'].diff()

    o['obv'] = obv(o['Close'], o['Volume'])
    o['obv_ema'] = o['obv'].ewm(span=9, adjust=False).mean()
    o['obv_slope'] = o['obv'].diff(3)

    vol_ma = o['Volume'].rolling(20, min_periods=5).mean()
    o['vol_ratio'] = (o['Volume'] / vol_ma.replace(0, np.nan)).fillna(0.0)

    # h1/h4 trend filter
    for label, rule in [('h1', '60min'), ('h4', '240min')]:
        tf = df.resample(rule, origin='start_day', offset='9h', label='right', closed='right').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}).dropna()
        tf['ema20'] = tf['Close'].ewm(span=20, adjust=False).mean()
        adx, pdi, mdi = adx_components(tf)
        tf[f'{label}_adx'] = adx
        tf[f'{label}_trend'] = (tf['Close'] >= tf['ema20']) & (pdi >= mdi)
        o = pd.merge_asof(
            o.sort_index().reset_index().rename(columns={'index': 'ts'}),
            tf[[f'{label}_adx', f'{label}_trend']].sort_index().reset_index().rename(columns={'index': 'ts'}),
            on='ts', direction='backward'
        ).set_index('ts')
        o[f'{label}_adx'] = o[f'{label}_adx'].ffill().fillna(0.0)
        o[f'{label}_trend'] = o[f'{label}_trend'].ffill().fillna(False)
    return o


def build_signal(df: pd.DataFrame, use_post_filter: bool):
    pr = df.shift(1)
    os = ((pr['stoch'] <= BASE['st_os']).astype(int) + (pr['cci20'] <= BASE['cci_os']).astype(int) + (pr['rsi'] <= BASE['rsi_os']).astype(int))
    cu = (((pr['stoch'] <= BASE['st_os']) & (df['stoch'] > BASE['st_os'])).astype(int) + ((pr['cci20'] <= BASE['cci_os']) & (df['cci20'] > BASE['cci_os'])).astype(int) + ((pr['rsi'] <= BASE['rsi_os']) & (df['rsi'] > BASE['rsi_os'])).astype(int))
    c9 = ((pr['cci9'] <= -100) & (df['cci9'] > -100)).rolling(BASE['cci9_window'], min_periods=1).max().astype(bool)
    up = ((df['stoch_slope'] > 0).astype(int) + (df['cci20_slope'] > 0).astype(int) + (df['rsi_slope'] > 0).astype(int))

    regime = (df['h1_adx'] >= BASE['h1_min']) & df['h1_trend'] & (df['h4_adx'] >= BASE['h4_min']) & df['h4_trend']

    open_sig = (os >= BASE['open_prev_need']) & (cu >= BASE['open_cross_need']) & (df['cci20'] > -100) & c9 & (up >= BASE['up_slope_need']) & regime

    if use_post_filter:
        # post 224229015709 inspired: volume-backed impulse + DMI vertical + MACD/OBV expansion
        dmi_impulse = (df['pdi5'] > df['mdi5']) & (df['adx5_slope'] > 0.4) & (df['pdi5_slope'] > 1.0)
        macd_impulse = (df['macd_hist'] > 0) & (df['macd_hist_delta'] > 0)
        obv_impulse = (df['obv'] > df['obv_ema']) & (df['obv_slope'] > 0)
        vol_impulse = df['vol_ratio'] >= 1.20
        open_sig = open_sig & dmi_impulse & macd_impulse & obv_impulse & vol_impulse

    cd = (((pr['stoch'] >= BASE['st_ob']) & (df['stoch'] < BASE['st_ob'])).astype(int) + ((pr['cci20'] >= BASE['cci_ob']) & (df['cci20'] < BASE['cci_ob'])).astype(int) + ((pr['rsi'] >= BASE['rsi_ob']) & (df['rsi'] < BASE['rsi_ob'])).astype(int))
    dn = ((df['stoch_slope'] < 0).astype(int) + (df['cci20_slope'] < 0).astype(int) + (df['rsi_slope'] < 0).astype(int))
    crash = (dn >= BASE['down_slope_need']) & ((df['stoch'] >= BASE['st_ob'] - 5) | (df['rsi'] >= BASE['rsi_ob'] - 5) | (df['cci20'] >= BASE['cci_ob'] - 15))
    close_sig = (cd >= BASE['close_cross_need']) | crash

    if use_post_filter:
        # post-style quick de-risk when expansion fades after overbought
        over2 = ((df['stoch'] >= BASE['st_ob']).astype(int) + (df['cci20'] >= BASE['cci_ob']).astype(int) + (df['rsi'] >= BASE['rsi_ob']).astype(int)) >= 2
        fade = (df['macd_hist_delta'] < 0) | (df['obv'] < df['obv_ema'])
        close_sig = close_sig | (over2 & (dn >= 2) & fade)

    return open_sig.fillna(False), close_sig.fillna(False)


def backtest(L: pd.DataFrame, S: pd.DataFrame, idx: pd.DatetimeIndex, use_post_filter: bool):
    lo, lc = build_signal(L, use_post_filter)
    so, sc = build_signal(S, use_post_filter)

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
            close = bool(lc.at[ts]) or lp <= entry * (1 - BASE['stop']) or lp <= high * (1 - BASE['trail']) or sw
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
            close = bool(sc.at[ts]) or sp <= entry * (1 - BASE['stop']) or sp <= high * (1 - BASE['trail']) or sw
            if close:
                r = (sp / entry) - 1
                eq *= (1 + r); peak = max(peak, eq); mdd = max(mdd, 1 - eq / peak)
                rows.append((et, ts, r))
                if sw:
                    pos = 'L'; entry = lp; high = lp; et = ts
                else:
                    pos = None

    tr = pd.DataFrame(rows, columns=['entry', 'exit', 'ret'])
    wr = float((tr['ret'] > 0).mean() * 100) if len(tr) else 0.0
    return {
        'cum': (eq - 1) * 100,
        'mdd': mdd * 100,
        'win': wr,
        'trades': int(len(tr)),
        'tr': tr,
    }


L = load('122630')
S = load('252670')
idx = L.index.intersection(S.index)
L = enrich(L.loc[idx])
S = enrich(S.loc[idx])

base = backtest(L, S, idx, use_post_filter=False)
post = backtest(L, S, idx, use_post_filter=True)

print('BASE(h1+h4):', {k: round(v, 2) if isinstance(v, float) else v for k, v in base.items() if k != 'tr'})
print('POST_FILTER:', {k: round(v, 2) if isinstance(v, float) else v for k, v in post.items() if k != 'tr'})

if len(post['tr']) > 0:
    t = post['tr'].copy()
    t['exit'] = pd.to_datetime(t['exit'])
    t['win'] = t['ret'] > 0
    t['month'] = t['exit'].dt.to_period('M').astype(str)
    t['week'] = t['exit'].dt.to_period('W-MON').astype(str)
    mon = t.groupby('month').agg(trades=('ret', 'size'), win_rate=('win', 'mean'), ret=('ret', lambda s: (1 + s).prod() - 1))
    mon['win_rate'] *= 100
    mon['ret'] *= 100
    wk = t.groupby('week').agg(trades=('ret', 'size'), win_rate=('win', 'mean'), ret=('ret', lambda s: (1 + s).prod() - 1))
    wk['win_rate'] *= 100
    wk['ret'] *= 100

    print('\nPOST_FILTER MONTHLY')
    print(mon.to_string(float_format=lambda x: f'{x:.2f}'))
    print('\nPOST_FILTER WEEKLY_LAST10')
    print(wk.tail(10).to_string(float_format=lambda x: f'{x:.2f}'))
