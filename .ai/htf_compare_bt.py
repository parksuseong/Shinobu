import sqlite3
from pathlib import Path
import pandas as pd
import numpy as np
from shinobu.strategy_src import _calculate_cci, _calculate_rsi, _calculate_stochastic_fast

DB = Path('.streamlit/shinobu_cache.db')
con = sqlite3.connect(DB)


def load_symbol(sym: str) -> pd.DataFrame:
    df = pd.read_sql_query(
        'select ts,open,high,low,close,volume from raw_market_data where symbol=? order by ts',
        con,
        params=[sym],
    )
    df['ts'] = pd.to_datetime(df['ts'])
    df = df.set_index('ts').sort_index()
    df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    for c in df.columns:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    return df.dropna(subset=['Open', 'High', 'Low', 'Close'])


def adx_components(frame: pd.DataFrame, period: int = 14) -> tuple[pd.Series, pd.Series, pd.Series]:
    high, low, close = frame['High'], frame['Low'], frame['Close']
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

    tr = pd.concat([
        high - low,
        (high - close.shift(1)).abs(),
        (low - close.shift(1)).abs(),
    ], axis=1).max(axis=1)

    atr = tr.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    plus_di = 100 * (plus_dm.ewm(alpha=1 / period, adjust=False, min_periods=period).mean() / atr.replace(0, np.nan))
    minus_di = 100 * (minus_dm.ewm(alpha=1 / period, adjust=False, min_periods=period).mean() / atr.replace(0, np.nan))
    dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)) * 100
    adx = dx.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    return adx, plus_di.fillna(0.0), minus_di.fillna(0.0)


def resample_1h(df: pd.DataFrame) -> pd.DataFrame:
    out = df.resample('60min', origin='start_day', offset='9h', label='right', closed='right').agg(
        {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}
    )
    return out.dropna()


def resample_4h(df: pd.DataFrame) -> pd.DataFrame:
    out = df.resample('240min', origin='start_day', offset='9h', label='right', closed='right').agg(
        {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}
    )
    return out.dropna()


def enrich_5m(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out['stoch'] = _calculate_stochastic_fast(out, 5).rolling(3, min_periods=1).mean()
    out['cci9'] = _calculate_cci(out, 9)
    out['cci20'] = _calculate_cci(out, 20)
    out['rsi'] = _calculate_rsi(out['Close'], 14)
    out['stoch_slope'] = out['stoch'].diff()
    out['cci20_slope'] = out['cci20'].diff()
    out['rsi_slope'] = out['rsi'].diff()
    return out


def attach_htf_filters(df5: pd.DataFrame) -> pd.DataFrame:
    out = df5.copy()

    h1 = resample_1h(df5)
    h1['ema20'] = h1['Close'].ewm(span=20, adjust=False).mean()
    h1_adx, h1_pdi, h1_mdi = adx_components(h1)
    h1['adx'] = h1_adx
    h1['trend'] = (h1['Close'] >= h1['ema20']) & (h1_pdi >= h1_mdi)

    h4 = resample_4h(df5)
    h4['ema20'] = h4['Close'].ewm(span=20, adjust=False).mean()
    h4_adx, h4_pdi, h4_mdi = adx_components(h4)
    h4['adx'] = h4_adx
    h4['trend'] = (h4['Close'] >= h4['ema20']) & (h4_pdi >= h4_mdi)

    out = pd.merge_asof(
        out.sort_index().reset_index().rename(columns={'index': 'ts'}),
        h1[['adx', 'trend']].sort_index().reset_index().rename(columns={'index': 'ts', 'adx': 'h1_adx', 'trend': 'h1_trend'}),
        on='ts',
        direction='backward',
    ).set_index('ts')

    out = pd.merge_asof(
        out.sort_index().reset_index(),
        h4[['adx', 'trend']].sort_index().reset_index().rename(columns={'index': 'ts', 'adx': 'h4_adx', 'trend': 'h4_trend'}),
        on='ts',
        direction='backward',
    ).set_index('ts')

    out[['h1_adx', 'h4_adx']] = out[['h1_adx', 'h4_adx']].ffill().fillna(0.0)
    out[['h1_trend', 'h4_trend']] = out[['h1_trend', 'h4_trend']].ffill().fillna(False)
    return out


def build_signal(df: pd.DataFrame, p: dict, mode: str) -> tuple[pd.Series, pd.Series]:
    prev = df.shift(1)

    oversold_prev = (
        (prev['stoch'] <= p['st_os']).astype(int)
        + (prev['cci20'] <= p['cci_os']).astype(int)
        + (prev['rsi'] <= p['rsi_os']).astype(int)
    )
    cross_up = (
        ((prev['stoch'] <= p['st_os']) & (df['stoch'] > p['st_os'])).astype(int)
        + ((prev['cci20'] <= p['cci_os']) & (df['cci20'] > p['cci_os'])).astype(int)
        + ((prev['rsi'] <= p['rsi_os']) & (df['rsi'] > p['rsi_os'])).astype(int)
    )

    cci9_recent = ((prev['cci9'] <= -100) & (df['cci9'] > -100)).rolling(p['cci9_window'], min_periods=1).max().astype(bool)
    up_slope = (
        (df['stoch_slope'] > 0).astype(int)
        + (df['cci20_slope'] > 0).astype(int)
        + (df['rsi_slope'] > 0).astype(int)
    )

    if mode == 'h1':
        regime = (df['h1_adx'] >= p['h1_adx_min']) & df['h1_trend']
    elif mode == 'h4':
        regime = (df['h4_adx'] >= p['h4_adx_min']) & df['h4_trend']
    else:
        regime = (
            (df['h1_adx'] >= p['h1_adx_min']) & df['h1_trend']
            & (df['h4_adx'] >= p['h4_adx_min']) & df['h4_trend']
        )

    open_sig = (
        (oversold_prev >= p['open_prev_need'])
        & (cross_up >= p['open_cross_need'])
        & (df['cci20'] > -100)
        & cci9_recent
        & (up_slope >= p['up_slope_need'])
        & regime
    )

    cross_down = (
        ((prev['stoch'] >= p['st_ob']) & (df['stoch'] < p['st_ob'])).astype(int)
        + ((prev['cci20'] >= p['cci_ob']) & (df['cci20'] < p['cci_ob'])).astype(int)
        + ((prev['rsi'] >= p['rsi_ob']) & (df['rsi'] < p['rsi_ob'])).astype(int)
    )
    down_slope = (
        (df['stoch_slope'] < 0).astype(int)
        + (df['cci20_slope'] < 0).astype(int)
        + (df['rsi_slope'] < 0).astype(int)
    )
    crash = (
        (down_slope >= p['down_slope_need'])
        & ((df['stoch'] >= p['st_ob'] - 5) | (df['rsi'] >= p['rsi_ob'] - 5) | (df['cci20'] >= p['cci_ob'] - 15))
    )
    close_sig = (cross_down >= p['close_cross_need']) | crash

    return open_sig.fillna(False), close_sig.fillna(False)


def run_bt(L: pd.DataFrame, S: pd.DataFrame, idx: pd.DatetimeIndex, p: dict, mode: str) -> dict | None:
    lo, lc = build_signal(L, p, mode)
    so, sc = build_signal(S, p, mode)

    position = None
    entry_price = None
    high_price = None
    entry_ts = None

    equity = 1.0
    peak = 1.0
    mdd = 0.0
    rows = []

    for ts in idx:
        lp = float(L.at[ts, 'Close'])
        sp = float(S.at[ts, 'Close'])

        if position is None:
            if lo.at[ts] and not so.at[ts]:
                position = 'long'; entry_price = lp; high_price = lp; entry_ts = ts
            elif so.at[ts] and not lo.at[ts]:
                position = 'short'; entry_price = sp; high_price = sp; entry_ts = ts
            continue

        if position == 'long':
            high_price = max(high_price, lp)
            switch = bool(so.at[ts])
            close = bool(lc.at[ts]) or lp <= entry_price * (1 - p['stop']) or lp <= high_price * (1 - p['trail']) or switch
            if close:
                ret = (lp / entry_price) - 1
                equity *= 1 + ret
                peak = max(peak, equity)
                mdd = max(mdd, 1 - equity / peak)
                rows.append((entry_ts, ts, ret))
                if switch:
                    position = 'short'; entry_price = sp; high_price = sp; entry_ts = ts
                else:
                    position = None
        else:
            high_price = max(high_price, sp)
            switch = bool(lo.at[ts])
            close = bool(sc.at[ts]) or sp <= entry_price * (1 - p['stop']) or sp <= high_price * (1 - p['trail']) or switch
            if close:
                ret = (sp / entry_price) - 1
                equity *= 1 + ret
                peak = max(peak, equity)
                mdd = max(mdd, 1 - equity / peak)
                rows.append((entry_ts, ts, ret))
                if switch:
                    position = 'long'; entry_price = lp; high_price = lp; entry_ts = ts
                else:
                    position = None

    if not rows:
        return None

    tr = pd.DataFrame(rows, columns=['entry', 'exit', 'ret'])
    return {
        'cum': (equity - 1) * 100,
        'trades': len(tr),
        'win': float((tr['ret'] > 0).mean() * 100),
        'mdd': float(mdd * 100),
        'tr': tr,
    }


L = load_symbol('122630')
S = load_symbol('252670')
idx = L.index.intersection(S.index)
L = L.loc[idx].copy()
S = S.loc[idx].copy()


long5 = attach_htf_filters(enrich_5m(L))
short5 = attach_htf_filters(enrich_5m(S))

candidates = [
    {'st_os': 20, 'cci_os': -100, 'rsi_os': 30, 'open_prev_need': 2, 'open_cross_need': 2, 'cci9_window': 2, 'up_slope_need': 2, 'st_ob': 80, 'cci_ob': 100, 'rsi_ob': 70, 'close_cross_need': 2, 'down_slope_need': 2, 'stop': 0.03, 'trail': 0.05, 'h1_adx_min': 16, 'h4_adx_min': 16},
    {'st_os': 20, 'cci_os': -100, 'rsi_os': 30, 'open_prev_need': 3, 'open_cross_need': 2, 'cci9_window': 2, 'up_slope_need': 2, 'st_ob': 80, 'cci_ob': 100, 'rsi_ob': 70, 'close_cross_need': 2, 'down_slope_need': 2, 'stop': 0.03, 'trail': 0.05, 'h1_adx_min': 18, 'h4_adx_min': 16},
    {'st_os': 20, 'cci_os': -100, 'rsi_os': 30, 'open_prev_need': 3, 'open_cross_need': 2, 'cci9_window': 3, 'up_slope_need': 2, 'st_ob': 80, 'cci_ob': 100, 'rsi_ob': 70, 'close_cross_need': 2, 'down_slope_need': 2, 'stop': 0.03, 'trail': 0.05, 'h1_adx_min': 18, 'h4_adx_min': 18},
    {'st_os': 20, 'cci_os': -100, 'rsi_os': 30, 'open_prev_need': 3, 'open_cross_need': 2, 'cci9_window': 3, 'up_slope_need': 2, 'st_ob': 80, 'cci_ob': 110, 'rsi_ob': 70, 'close_cross_need': 2, 'down_slope_need': 2, 'stop': 0.03, 'trail': 0.05, 'h1_adx_min': 20, 'h4_adx_min': 18},
    {'st_os': 20, 'cci_os': -110, 'rsi_os': 30, 'open_prev_need': 3, 'open_cross_need': 2, 'cci9_window': 3, 'up_slope_need': 2, 'st_ob': 80, 'cci_ob': 100, 'rsi_ob': 70, 'close_cross_need': 2, 'down_slope_need': 2, 'stop': 0.03, 'trail': 0.05, 'h1_adx_min': 18, 'h4_adx_min': 20},
    {'st_os': 20, 'cci_os': -100, 'rsi_os': 30, 'open_prev_need': 3, 'open_cross_need': 2, 'cci9_window': 3, 'up_slope_need': 2, 'st_ob': 80, 'cci_ob': 100, 'rsi_ob': 70, 'close_cross_need': 3, 'down_slope_need': 2, 'stop': 0.03, 'trail': 0.05, 'h1_adx_min': 18, 'h4_adx_min': 18},
]

modes = ['h1', 'h4', 'both']
all_rows = []

for mode in modes:
    for p in candidates:
        out = run_bt(long5, short5, idx, p, mode)
        if out is None:
            continue
        score = out['cum'] - 0.35 * out['mdd'] + 0.12 * (out['win'] - 50) + 0.03 * min(out['trades'], 120)
        all_rows.append((mode, score, p, out))

if not all_rows:
    print('no results')
    raise SystemExit(0)

print(f'tested: {len(all_rows)} results ({len(candidates)} candidates x {len(modes)} modes)')

for mode in modes:
    subset = [r for r in all_rows if r[0] == mode]
    subset.sort(key=lambda x: x[1], reverse=True)
    mode_best = subset[0]
    _, score, p, o = mode_best
    print(f"MODE={mode} BEST score={score:.2f} cum={o['cum']:.2f}% win={o['win']:.1f}% trades={o['trades']} mdd={o['mdd']:.2f}%")
    print(' params:', p)

best = sorted(all_rows, key=lambda x: x[1], reverse=True)[0]
mode, score, p, o = best
tr = o['tr'].copy()
tr['exit'] = pd.to_datetime(tr['exit'])
tr['win'] = tr['ret'] > 0
tr['month'] = tr['exit'].dt.to_period('M').astype(str)
tr['week'] = tr['exit'].dt.to_period('W-MON').astype(str)
monthly = tr.groupby('month').agg(trades=('ret', 'size'), win_rate=('win', 'mean'), ret=('ret', lambda s: (1 + s).prod() - 1))
monthly['win_rate'] *= 100
monthly['ret'] *= 100
weekly = tr.groupby('week').agg(trades=('ret', 'size'), win_rate=('win', 'mean'), ret=('ret', lambda s: (1 + s).prod() - 1))
weekly['win_rate'] *= 100
weekly['ret'] *= 100

print('\nGLOBAL_BEST')
print('mode:', mode)
print('period:', idx.min(), '->', idx.max(), 'bars:', len(idx))
print('cum:', round(o['cum'], 2), 'win:', round(o['win'], 1), 'trades:', o['trades'], 'mdd:', round(o['mdd'], 2))
print('params:', p)
print('\nMONTHLY')
print(monthly.to_string(float_format=lambda x: f'{x:.2f}'))
print('\nWEEKLY_ALL')
print(weekly.to_string(float_format=lambda x: f'{x:.2f}'))
