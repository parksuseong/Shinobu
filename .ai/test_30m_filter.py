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

def adx_comp(frame, period=14):
    h,l,c = frame['High'], frame['Low'], frame['Close']
    up = h.diff(); dn = -l.diff()
    pdm = up.where((up > dn) & (up > 0), 0.0)
    mdm = dn.where((dn > up) & (dn > 0), 0.0)
    tr = pd.concat([h-l, (h-c.shift(1)).abs(), (l-c.shift(1)).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    pdi = 100 * (pdm.ewm(alpha=1/period, adjust=False, min_periods=period).mean() / atr.replace(0,np.nan))
    mdi = 100 * (mdm.ewm(alpha=1/period, adjust=False, min_periods=period).mean() / atr.replace(0,np.nan))
    dx = ((pdi-mdi).abs() / (pdi+mdi).replace(0,np.nan)) * 100
    adx = dx.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    return adx, pdi.fillna(0), mdi.fillna(0)

def enrich(df):
    o = df.copy()
    o['stoch'] = _calculate_stochastic_fast(o,5).rolling(3,min_periods=1).mean()
    o['cci9'] = _calculate_cci(o,9)
    o['cci20'] = _calculate_cci(o,20)
    o['rsi'] = _calculate_rsi(o['Close'],14)
    o['stoch_slope'] = o['stoch'].diff()
    o['cci20_slope'] = o['cci20'].diff()
    o['rsi_slope'] = o['rsi'].diff()

    for label, rule in [('m30','30min'),('h1','60min'),('h4','240min')]:
        tf = df.resample(rule, origin='start_day', offset='9h', label='right', closed='right').agg({'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'}).dropna()
        tf['ema20'] = tf['Close'].ewm(span=20, adjust=False).mean()
        a,p,m = adx_comp(tf)
        tf[f'{label}_adx'] = a
        tf[f'{label}_trend'] = (tf['Close'] >= tf['ema20']) & (p >= m)
        o = pd.merge_asof(
            o.sort_index().reset_index().rename(columns={'index':'ts'}),
            tf[[f'{label}_adx',f'{label}_trend']].sort_index().reset_index().rename(columns={'index':'ts'}),
            on='ts', direction='backward'
        ).set_index('ts')
        o[f'{label}_adx'] = o[f'{label}_adx'].ffill().fillna(0.0)
        o[f'{label}_trend'] = o[f'{label}_trend'].ffill().fillna(False)
    return o

def signal(df, p, mode):
    prev = df.shift(1)
    os = ((prev['stoch']<=p['st_os']).astype(int)+(prev['cci20']<=p['cci_os']).astype(int)+(prev['rsi']<=p['rsi_os']).astype(int))
    cu = (((prev['stoch']<=p['st_os'])&(df['stoch']>p['st_os'])).astype(int)+((prev['cci20']<=p['cci_os'])&(df['cci20']>p['cci_os'])).astype(int)+((prev['rsi']<=p['rsi_os'])&(df['rsi']>p['rsi_os'])).astype(int))
    c9 = ((prev['cci9']<=-100)&(df['cci9']>-100)).rolling(p['cci9_window'],min_periods=1).max().astype(bool)
    up = ((df['stoch_slope']>0).astype(int)+(df['cci20_slope']>0).astype(int)+(df['rsi_slope']>0).astype(int))

    if mode == 'm30':
        regime = (df['m30_adx']>=p['m30_min']) & df['m30_trend']
    elif mode == 'm30_h1':
        regime = (df['m30_adx']>=p['m30_min']) & df['m30_trend'] & (df['h1_adx']>=p['h1_min']) & df['h1_trend']
    elif mode == 'm30_h4':
        regime = (df['m30_adx']>=p['m30_min']) & df['m30_trend'] & (df['h4_adx']>=p['h4_min']) & df['h4_trend']
    else:
        regime = (df['m30_adx']>=p['m30_min']) & df['m30_trend'] & (df['h1_adx']>=p['h1_min']) & df['h1_trend'] & (df['h4_adx']>=p['h4_min']) & df['h4_trend']

    o = (os>=p['open_prev_need']) & (cu>=p['open_cross_need']) & (df['cci20']>-100) & c9 & (up>=p['up_slope_need']) & regime

    cd = (((prev['stoch']>=p['st_ob'])&(df['stoch']<p['st_ob'])).astype(int)+((prev['cci20']>=p['cci_ob'])&(df['cci20']<p['cci_ob'])).astype(int)+((prev['rsi']>=p['rsi_ob'])&(df['rsi']<p['rsi_ob'])).astype(int))
    dn = ((df['stoch_slope']<0).astype(int)+(df['cci20_slope']<0).astype(int)+(df['rsi_slope']<0).astype(int))
    crash = (dn>=p['down_slope_need']) & ((df['stoch']>=p['st_ob']-5)|(df['rsi']>=p['rsi_ob']-5)|(df['cci20']>=p['cci_ob']-15))
    c = (cd>=p['close_cross_need']) | crash
    return o.fillna(False), c.fillna(False)

def bt(L,S,idx,p,mode):
    lo,lc = signal(L,p,mode)
    so,sc = signal(S,p,mode)
    pos=None; ep=None; hp=None; eq=1.0; peak=1.0; mdd=0.0; et=None; rows=[]
    for ts in idx:
        lp=float(L.at[ts,'Close']); sp=float(S.at[ts,'Close'])
        if pos is None:
            if lo.at[ts] and not so.at[ts]: pos='L'; ep=lp; hp=lp; et=ts
            elif so.at[ts] and not lo.at[ts]: pos='S'; ep=sp; hp=sp; et=ts
            continue
        if pos=='L':
            hp=max(hp,lp); sw=bool(so.at[ts]); cl=bool(lc.at[ts]) or lp<=ep*(1-p['stop']) or lp<=hp*(1-p['trail']) or sw
            if cl:
                r=(lp/ep)-1; eq*=1+r; peak=max(peak,eq); mdd=max(mdd,1-eq/peak); rows.append((et,ts,r))
                if sw: pos='S'; ep=sp; hp=sp; et=ts
                else: pos=None
        else:
            hp=max(hp,sp); sw=bool(lo.at[ts]); cl=bool(sc.at[ts]) or sp<=ep*(1-p['stop']) or sp<=hp*(1-p['trail']) or sw
            if cl:
                r=(sp/ep)-1; eq*=1+r; peak=max(peak,eq); mdd=max(mdd,1-eq/peak); rows.append((et,ts,r))
                if sw: pos='L'; ep=lp; hp=lp; et=ts
                else: pos=None
    if not rows:
        return None
    tr=pd.DataFrame(rows,columns=['entry','exit','ret'])
    return {'cum':(eq-1)*100,'trades':len(tr),'win':float((tr['ret']>0).mean()*100),'mdd':float(mdd*100)}

L=load('122630'); S=load('252670'); idx=L.index.intersection(S.index)
L=enrich(L.loc[idx]); S=enrich(S.loc[idx])

modes=['m30','m30_h1','m30_h4','m30_h1_h4']
rows=[]
for mode in modes:
    for m30,h1,h4 in product([16,18],[16,18],[16,18]):
        p=dict(BASE); p['m30_min']=m30; p['h1_min']=h1; p['h4_min']=h4
        o=bt(L,S,idx,p,mode)
        if not o: continue
        score=o['cum']-0.4*o['mdd']+0.12*(o['win']-50)+0.02*min(o['trades'],120)
        rows.append((mode,score,m30,h1,h4,o))

rows.sort(key=lambda x:x[1], reverse=True)
for mode in modes:
    sub=[r for r in rows if r[0]==mode]
    b=sub[0]
    print(f"{mode}: cum={b[5]['cum']:.2f}% mdd={b[5]['mdd']:.2f}% win={b[5]['win']:.1f}% trades={b[5]['trades']} (m30/h1/h4={b[2]}/{b[3]}/{b[4]})")

best=rows[0]
print(f"\nBEST overall -> mode={best[0]} cum={best[5]['cum']:.2f}% mdd={best[5]['mdd']:.2f}% win={best[5]['win']:.1f}% trades={best[5]['trades']} (m30/h1/h4={best[2]}/{best[3]}/{best[4]})")
