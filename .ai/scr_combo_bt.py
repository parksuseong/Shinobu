import sqlite3
from pathlib import Path
import pandas as pd
import numpy as np
from shinobu.strategy_src import _calculate_cci, _calculate_rsi, _calculate_stochastic_fast

con = sqlite3.connect(Path('.streamlit/shinobu_cache.db'))


def load(sym: str) -> pd.DataFrame:
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


L = load('122630')
S = load('252670')
idx = L.index.intersection(S.index)
L = L.loc[idx].copy()
S = S.loc[idx].copy()


def enrich(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out['stoch'] = _calculate_stochastic_fast(out, 5).rolling(3, min_periods=1).mean()
    out['cci9'] = _calculate_cci(out, 9)
    out['cci20'] = _calculate_cci(out, 20)
    out['rsi'] = _calculate_rsi(out['Close'], 14)

    daily = out.resample('1D').agg(
        {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}
    ).dropna()
    daily['ma50'] = daily['Close'].rolling(50, min_periods=20).mean()

    h, l, c = daily['High'], daily['Low'], daily['Close']
    period = 14
    up = h.diff()
    down = -l.diff()
    plus_dm = up.where((up > down) & (up > 0), 0.0)
    minus_dm = down.where((down > up) & (down > 0), 0.0)
    tr = pd.concat([h - l, (h - c.shift(1)).abs(), (l - c.shift(1)).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    plus_di = 100 * (plus_dm.ewm(alpha=1 / period, adjust=False, min_periods=period).mean() / atr.replace(0, np.nan))
    minus_di = 100 * (minus_dm.ewm(alpha=1 / period, adjust=False, min_periods=period).mean() / atr.replace(0, np.nan))
    dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)) * 100
    daily['adx'] = dx.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()

    out['day'] = out.index.normalize()
    out = out.join(daily[['ma50', 'adx']], on='day')
    out[['ma50', 'adx']] = out[['ma50', 'adx']].ffill().bfill()
    out = out.drop(columns='day')
    out['stoch_slope'] = out['stoch'].diff()
    out['cci20_slope'] = out['cci20'].diff()
    out['rsi_slope'] = out['rsi'].diff()
    return out


L = enrich(L)
S = enrich(S)


def build_signal(df: pd.DataFrame, p: dict) -> tuple[pd.Series, pd.Series]:
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

    open_sig = (
        (oversold_prev >= p['open_prev_need'])
        & (cross_up >= p['open_cross_need'])
        & (df['cci20'] > -100)
        & cci9_recent
        & (df['Close'] >= df['ma50'])
        & (df['adx'] >= p['adx_min'])
        & (up_slope >= p['up_slope_need'])
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


def run_backtest(p: dict) -> dict | None:
    long_open, long_close = build_signal(L, p)
    short_open, short_close = build_signal(S, p)

    position = None
    entry = None
    high = None
    entry_ts = None

    equity = 1.0
    peak = 1.0
    mdd = 0.0
    rows = []

    for ts in idx:
        lp = float(L.at[ts, 'Close'])
        sp = float(S.at[ts, 'Close'])

        if position is None:
            if long_open.at[ts] and not short_open.at[ts]:
                position = 'long'; entry = lp; high = lp; entry_ts = ts
            elif short_open.at[ts] and not long_open.at[ts]:
                position = 'short'; entry = sp; high = sp; entry_ts = ts
            continue

        if position == 'long':
            high = max(high, lp)
            switch = bool(short_open.at[ts])
            close = bool(long_close.at[ts]) or lp <= entry * (1 - p['stop']) or lp <= high * (1 - p['trail']) or switch
            if close:
                ret = (lp / entry) - 1
                equity *= (1 + ret)
                peak = max(peak, equity)
                mdd = max(mdd, 1 - equity / peak)
                rows.append((entry_ts, ts, ret))
                if switch:
                    position = 'short'; entry = sp; high = sp; entry_ts = ts
                else:
                    position = None
        else:
            high = max(high, sp)
            switch = bool(long_open.at[ts])
            close = bool(short_close.at[ts]) or sp <= entry * (1 - p['stop']) or sp <= high * (1 - p['trail']) or switch
            if close:
                ret = (sp / entry) - 1
                equity *= (1 + ret)
                peak = max(peak, equity)
                mdd = max(mdd, 1 - equity / peak)
                rows.append((entry_ts, ts, ret))
                if switch:
                    position = 'long'; entry = lp; high = lp; entry_ts = ts
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


candidates = [
    {'st_os': 20, 'cci_os': -100, 'rsi_os': 30, 'open_prev_need': 2, 'open_cross_need': 2, 'cci9_window': 2, 'adx_min': 16, 'up_slope_need': 2, 'st_ob': 80, 'cci_ob': 100, 'rsi_ob': 70, 'close_cross_need': 2, 'down_slope_need': 2, 'stop': 0.03, 'trail': 0.045},
    {'st_os': 20, 'cci_os': -100, 'rsi_os': 30, 'open_prev_need': 3, 'open_cross_need': 2, 'cci9_window': 3, 'adx_min': 18, 'up_slope_need': 2, 'st_ob': 80, 'cci_ob': 100, 'rsi_ob': 70, 'close_cross_need': 2, 'down_slope_need': 2, 'stop': 0.03, 'trail': 0.05},
    {'st_os': 22, 'cci_os': -110, 'rsi_os': 32, 'open_prev_need': 2, 'open_cross_need': 2, 'cci9_window': 3, 'adx_min': 18, 'up_slope_need': 2, 'st_ob': 80, 'cci_ob': 110, 'rsi_ob': 70, 'close_cross_need': 2, 'down_slope_need': 2, 'stop': 0.035, 'trail': 0.05},
    {'st_os': 20, 'cci_os': -110, 'rsi_os': 30, 'open_prev_need': 2, 'open_cross_need': 3, 'cci9_window': 2, 'adx_min': 20, 'up_slope_need': 3, 'st_ob': 80, 'cci_ob': 100, 'rsi_ob': 70, 'close_cross_need': 3, 'down_slope_need': 3, 'stop': 0.03, 'trail': 0.045},
]

results = []
for p in candidates:
    out = run_backtest(p)
    if out is None:
        continue
    score = out['cum'] - (0.35 * out['mdd']) + (0.12 * (out['win'] - 50)) + (0.03 * min(out['trades'], 120))
    results.append((score, p, out))

results.sort(key=lambda x: x[0], reverse=True)
print('tested', len(results))
for i, (score, p, out) in enumerate(results, 1):
    print(f"#{i} score={score:.2f} cum={out['cum']:.2f}% win={out['win']:.1f}% trades={out['trades']} mdd={out['mdd']:.2f}%")

best = results[0]
bp = best[1]
bo = best[2]
tr = bo['tr'].copy()
tr['exit'] = pd.to_datetime(tr['exit'])
tr['win'] = tr['ret'] > 0
tr['month'] = tr['exit'].dt.to_period('M').astype(str)
monthly = tr.groupby('month').agg(trades=('ret', 'size'), win_rate=('win', 'mean'), ret=('ret', lambda s: (1 + s).prod() - 1))
monthly['win_rate'] *= 100
monthly['ret'] *= 100

print('\nBEST')
print('period', idx.min(), '->', idx.max(), 'bars', len(idx))
print('cum', round(bo['cum'], 2), 'win', round(bo['win'], 1), 'trades', bo['trades'], 'mdd', round(bo['mdd'], 2))
print('params', bp)
print('\nMONTHLY')
print(monthly.to_string(float_format=lambda x: f'{x:.2f}'))
