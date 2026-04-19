import sqlite3
from pathlib import Path
from itertools import product
import pandas as pd
import numpy as np
from shinobu.strategy_src import _calculate_cci, _calculate_rsi, _calculate_stochastic_fast

BASE = {
    'st_os': 20, 'cci_os': -100, 'rsi_os': 30,
    'open_prev_need': 2, 'open_cross_need': 2,
    'cci9_window': 2, 'up_slope_need': 2,
    'st_ob': 80, 'cci_ob': 100, 'rsi_ob': 70,
    'close_cross_need': 2, 'down_slope_need': 2,
    'stop': 0.03, 'trail': 0.05,
    'h1_adx_min': 16, 'h4_adx_min': 16,
}

con = sqlite3.connect(Path('.streamlit/shinobu_cache.db'))

def load(sym):
    d = pd.read_sql_query('select ts,open,high,low,close,volume from raw_market_data where symbol=? order by ts', con, params=[sym])
    d['ts'] = pd.to_datetime(d['ts'])
    d = d.set_index('ts').sort_index()
    d.columns = ['Open','High','Low','Close','Volume']
    for c in d.columns:
        d[c] = pd.to_numeric(d[c], errors='coerce')
    return d.dropna(subset=['Open','High','Low','Close'])

def adx_components(frame, period=14):
    h,l,c = frame['High'], frame['Low'], frame['Close']
    up = h.diff(); dn = -l.diff()
    pdm = up.where((up > dn) & (up > 0), 0.0)
    mdm = dn.where((dn > up) & (dn > 0), 0.0)
    tr = pd.concat([h-l, (h-c.shift(1)).abs(), (l-c.shift(1)).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    pdi = 100*(pdm.ewm(alpha=1/period, adjust=False, min_periods=period).mean()/atr.replace(0,np.nan))
    mdi = 100*(mdm.ewm(alpha=1/period, adjust=False, min_periods=period).mean()/atr.replace(0,np.nan))
    dx = ((pdi-mdi).abs()/(pdi+mdi).replace(0,np.nan))*100
    adx = dx.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    return adx, pdi.fillna(0.0), mdi.fillna(0.0)

def enrich(df):
    o = df.copy()
    o['stoch'] = _calculate_stochastic_fast(o,5).rolling(3,min_periods=1).mean()
    o['cci9'] = _calculate_cci(o,9)
    o['cci20'] = _calculate_cci(o,20)
    o['rsi'] = _calculate_rsi(o['Close'],14)
    o['stoch_slope'] = o['stoch'].diff()
    o['cci20_slope'] = o['cci20'].diff()
    o['rsi_slope'] = o['rsi'].diff()

    h1 = df.resample('60min', origin='start_day', offset='9h', label='right', closed='right').agg({'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'}).dropna()
    h1['ema20'] = h1['Close'].ewm(span=20, adjust=False).mean()
    a1,p1,m1 = adx_components(h1)
    h1['h1_adx'] = a1
    h1['h1_trend'] = (h1['Close'] >= h1['ema20']) & (p1 >= m1)

    h4 = df.resample('240min', origin='start_day', offset='9h', label='right', closed='right').agg({'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'}).dropna()
    h4['ema20'] = h4['Close'].ewm(span=20, adjust=False).mean()
    a4,p4,m4 = adx_components(h4)
    h4['h4_adx'] = a4
    h4['h4_trend'] = (h4['Close'] >= h4['ema20']) & (p4 >= m4)

    o = pd.merge_asof(o.sort_index().reset_index().rename(columns={'index':'ts'}), h1[['h1_adx','h1_trend']].sort_index().reset_index().rename(columns={'index':'ts'}), on='ts', direction='backward').set_index('ts')
    o = pd.merge_asof(o.sort_index().reset_index(), h4[['h4_adx','h4_trend']].sort_index().reset_index().rename(columns={'index':'ts'}), on='ts', direction='backward').set_index('ts')
    o[['h1_adx','h4_adx']] = o[['h1_adx','h4_adx']].ffill().fillna(0.0)
    o[['h1_trend','h4_trend']] = o[['h1_trend','h4_trend']].ffill().fillna(False)
    return o

def make_signal(df, p):
    prev = df.shift(1)
    os = ((prev['stoch']<=p['st_os']).astype(int) + (prev['cci20']<=p['cci_os']).astype(int) + (prev['rsi']<=p['rsi_os']).astype(int))
    cu = (((prev['stoch']<=p['st_os'])&(df['stoch']>p['st_os'])).astype(int) + ((prev['cci20']<=p['cci_os'])&(df['cci20']>p['cci_os'])).astype(int) + ((prev['rsi']<=p['rsi_os'])&(df['rsi']>p['rsi_os'])).astype(int))
    c9 = ((prev['cci9']<=-100)&(df['cci9']>-100)).rolling(p['cci9_window'], min_periods=1).max().astype(bool)
    up = ((df['stoch_slope']>0).astype(int) + (df['cci20_slope']>0).astype(int) + (df['rsi_slope']>0).astype(int))
    regime = (df['h1_adx']>=p['h1_adx_min']) & df['h1_trend'] & (df['h4_adx']>=p['h4_adx_min']) & df['h4_trend']
    o = (os>=p['open_prev_need']) & (cu>=p['open_cross_need']) & (df['cci20']>-100) & c9 & (up>=p['up_slope_need']) & regime

    cd = (((prev['stoch']>=p['st_ob'])&(df['stoch']<p['st_ob'])).astype(int) + ((prev['cci20']>=p['cci_ob'])&(df['cci20']<p['cci_ob'])).astype(int) + ((prev['rsi']>=p['rsi_ob'])&(df['rsi']<p['rsi_ob'])).astype(int))
    dn = ((df['stoch_slope']<0).astype(int) + (df['cci20_slope']<0).astype(int) + (df['rsi_slope']<0).astype(int))
    crash = (dn>=p['down_slope_need']) & ((df['stoch']>=p['st_ob']-5)|(df['rsi']>=p['rsi_ob']-5)|(df['cci20']>=p['cci_ob']-15))
    c = (cd>=p['close_cross_need']) | crash
    return o.fillna(False), c.fillna(False)

def backtest(L,S,idx,p,switch_confirm=0):
    lo,lc = make_signal(L,p)
    so,sc = make_signal(S,p)
    pos=None; entry=None; high=None; entry_ts=None; eq=1.0; peak=1.0; mdd=0.0; rows=[]
    pending_side=None; pending_count=0

    for ts in idx:
        lp=float(L.at[ts,'Close']); sp=float(S.at[ts,'Close'])
        lo_sig, so_sig = bool(lo.at[ts]), bool(so.at[ts])
        lc_sig, sc_sig = bool(lc.at[ts]), bool(sc.at[ts])

        if pos is None:
            if lo_sig and not so_sig:
                pos='long'; entry=lp; high=lp; entry_ts=ts
            elif so_sig and not lo_sig:
                pos='short'; entry=sp; high=sp; entry_ts=ts
            continue

        if pos=='long':
            high=max(high,lp)
            raw_switch = so_sig
            switch=False
            if raw_switch and switch_confirm>0:
                if pending_side=='short':
                    pending_count += 1
                else:
                    pending_side='short'; pending_count=1
                switch = pending_count > switch_confirm
            elif raw_switch:
                switch=True
            else:
                if pending_side=='short':
                    pending_side=None; pending_count=0

            close = lc_sig or lp<=entry*(1-p['stop']) or lp<=high*(1-p['trail']) or switch
            if close:
                r=(lp/entry)-1; eq*=1+r; peak=max(peak,eq); mdd=max(mdd,1-eq/peak); rows.append((entry_ts,ts,r))
                pending_side=None; pending_count=0
                if switch:
                    pos='short'; entry=sp; high=sp; entry_ts=ts
                else:
                    pos=None

        else:
            high=max(high,sp)
            raw_switch = lo_sig
            switch=False
            if raw_switch and switch_confirm>0:
                if pending_side=='long':
                    pending_count += 1
                else:
                    pending_side='long'; pending_count=1
                switch = pending_count > switch_confirm
            elif raw_switch:
                switch=True
            else:
                if pending_side=='long':
                    pending_side=None; pending_count=0

            close = sc_sig or sp<=entry*(1-p['stop']) or sp<=high*(1-p['trail']) or switch
            if close:
                r=(sp/entry)-1; eq*=1+r; peak=max(peak,eq); mdd=max(mdd,1-eq/peak); rows.append((entry_ts,ts,r))
                pending_side=None; pending_count=0
                if switch:
                    pos='long'; entry=lp; high=lp; entry_ts=ts
                else:
                    pos=None

    if not rows:
        return None
    tr=pd.DataFrame(rows,columns=['entry','exit','ret'])
    return {'cum':(eq-1)*100,'trades':len(tr),'win':float((tr['ret']>0).mean()*100),'mdd':float(mdd*100),'tr':tr}

L = load('122630')
S = load('252670')
idx = L.index.intersection(S.index)
L = enrich(L.loc[idx])
S = enrich(S.loc[idx])

grid = {
    'h1_adx_min': [16, 18, 20],
    'h4_adx_min': [16, 18, 20],
    'trail': [0.05, 0.04, 0.035],
    'switch_confirm': [0, 1],
}

rows=[]
for h1,h4,trail,sc in product(grid['h1_adx_min'],grid['h4_adx_min'],grid['trail'],grid['switch_confirm']):
    p = dict(BASE)
    p['h1_adx_min']=h1
    p['h4_adx_min']=h4
    p['trail']=trail
    out = backtest(L,S,idx,p,switch_confirm=sc)
    if out is None:
        continue
    score = out['cum'] - 0.4*out['mdd'] + 0.12*(out['win']-50) + 0.02*min(out['trades'],120)
    rows.append((score,h1,h4,trail,sc,out))

rows.sort(key=lambda x:x[0], reverse=True)
print('tested', len(rows))
for i,(score,h1,h4,trail,sc,o) in enumerate(rows[:8],1):
    print(f"#{i} score={score:.2f} cum={o['cum']:.2f}% mdd={o['mdd']:.2f}% win={o['win']:.1f}% trades={o['trades']} h1={h1} h4={h4} trail={trail} switch_confirm={sc}")

best=rows[0]
_,h1,h4,trail,sc,o=best
tr=o['tr'].copy(); tr['exit']=pd.to_datetime(tr['exit']); tr['win']=tr['ret']>0
tr['week']=tr['exit'].dt.to_period('W-MON').astype(str)
tr['month']=tr['exit'].dt.to_period('M').astype(str)
weekly=tr.groupby('week').agg(trades=('ret','size'),win_rate=('win','mean'),ret=('ret',lambda s:(1+s).prod()-1)); weekly['win_rate']*=100; weekly['ret']*=100
monthly=tr.groupby('month').agg(trades=('ret','size'),win_rate=('win','mean'),ret=('ret',lambda s:(1+s).prod()-1)); monthly['win_rate']*=100; monthly['ret']*=100
print('\nBEST')
print('cum',round(o['cum'],2),'mdd',round(o['mdd'],2),'win',round(o['win'],1),'trades',o['trades'])
print('params',{'h1_adx_min':h1,'h4_adx_min':h4,'trail':trail,'switch_confirm':sc,'stop':BASE['stop']})
print('\nMONTHLY')
print(monthly.to_string(float_format=lambda x:f'{x:.2f}'))
print('\nWEEKLY_LAST10')
print(weekly.tail(10).to_string(float_format=lambda x:f'{x:.2f}'))
