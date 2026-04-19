import sqlite3
from pathlib import Path
from itertools import product
import pandas as pd
import numpy as np
from shinobu.strategy_src import _calculate_cci, _calculate_rsi, _calculate_stochastic_fast

BASE={'st_os':20,'cci_os':-100,'rsi_os':30,'open_prev_need':2,'open_cross_need':2,'cci9_window':2,'up_slope_need':2,'st_ob':80,'cci_ob':100,'rsi_ob':70,'close_cross_need':2,'down_slope_need':2,'stop':0.03,'trail':0.05,'h1_min':18,'h4_min':18}
con=sqlite3.connect(Path('.streamlit/shinobu_cache.db'))

def load(sym):
 d=pd.read_sql_query('select ts,open,high,low,close,volume from raw_market_data where symbol=? order by ts',con,params=[sym]); d['ts']=pd.to_datetime(d['ts']); d=d.set_index('ts').sort_index(); d.columns=['Open','High','Low','Close','Volume'];
 for c in d.columns: d[c]=pd.to_numeric(d[c],errors='coerce')
 return d.dropna(subset=['Open','High','Low','Close'])

def adx_comp(frame,n=14):
 h,l,c=frame['High'],frame['Low'],frame['Close']; up=h.diff(); dn=-l.diff(); pdm=up.where((up>dn)&(up>0),0.0); mdm=dn.where((dn>up)&(dn>0),0.0)
 tr=pd.concat([h-l,(h-c.shift(1)).abs(),(l-c.shift(1)).abs()],axis=1).max(axis=1); atr=tr.ewm(alpha=1/n,adjust=False,min_periods=n).mean(); pdi=100*(pdm.ewm(alpha=1/n,adjust=False,min_periods=n).mean()/atr.replace(0,np.nan)); mdi=100*(mdm.ewm(alpha=1/n,adjust=False,min_periods=n).mean()/atr.replace(0,np.nan)); dx=((pdi-mdi).abs()/(pdi+mdi).replace(0,np.nan))*100; adx=dx.ewm(alpha=1/n,adjust=False,min_periods=n).mean(); return adx,pdi.fillna(0),mdi.fillna(0)

def enrich(df):
 o=df.copy(); o['stoch']=_calculate_stochastic_fast(o,5).rolling(3,min_periods=1).mean(); o['cci9']=_calculate_cci(o,9); o['cci20']=_calculate_cci(o,20); o['rsi']=_calculate_rsi(o['Close'],14); o['stoch_slope']=o['stoch'].diff(); o['cci20_slope']=o['cci20'].diff(); o['rsi_slope']=o['rsi'].diff()
 a,p,m=adx_comp(o); o['adx5']=a; o['pdi5']=p; o['mdi5']=m; o['adx5_slope']=o['adx5'].diff(); o['pdi5_slope']=o['pdi5'].diff()
 ef=o['Close'].ewm(span=12,adjust=False).mean(); es=o['Close'].ewm(span=26,adjust=False).mean(); line=ef-es; sig=line.ewm(span=9,adjust=False).mean(); o['macd_hist']=line-sig; o['macd_hist_delta']=o['macd_hist'].diff()
 d=o['Close'].diff().fillna(0.0); signed=o['Volume'].where(d>=0,-o['Volume']); o['obv']=signed.cumsum().fillna(0.0); o['obv_ema']=o['obv'].ewm(span=9,adjust=False).mean(); o['obv_slope']=o['obv'].diff(3)
 vma=o['Volume'].rolling(20,min_periods=5).mean(); o['vol_ratio']=(o['Volume']/vma.replace(0,np.nan)).fillna(0.0)
 for label,rule in [('h1','60min'),('h4','240min')]:
  tf=df.resample(rule,origin='start_day',offset='9h',label='right',closed='right').agg({'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'}).dropna(); tf['ema20']=tf['Close'].ewm(span=20,adjust=False).mean(); aa,pp,mm=adx_comp(tf); tf[f'{label}_adx']=aa; tf[f'{label}_trend']=(tf['Close']>=tf['ema20'])&(pp>=mm)
  o=pd.merge_asof(o.sort_index().reset_index().rename(columns={'index':'ts'}),tf[[f'{label}_adx',f'{label}_trend']].sort_index().reset_index().rename(columns={'index':'ts'}),on='ts',direction='backward').set_index('ts'); o[f'{label}_adx']=o[f'{label}_adx'].ffill().fillna(0.0); o[f'{label}_trend']=o[f'{label}_trend'].ffill().fillna(False)
 return o

def signal(df,post):
 pr=df.shift(1)
 os=((pr['stoch']<=BASE['st_os']).astype(int)+(pr['cci20']<=BASE['cci_os']).astype(int)+(pr['rsi']<=BASE['rsi_os']).astype(int))
 cu=((pr['stoch']<=BASE['st_os'])&(df['stoch']>BASE['st_os'])).astype(int)+((pr['cci20']<=BASE['cci_os'])&(df['cci20']>BASE['cci_os'])).astype(int)+((pr['rsi']<=BASE['rsi_os'])&(df['rsi']>BASE['rsi_os'])).astype(int)
 c9=((pr['cci9']<=-100)&(df['cci9']>-100)).rolling(BASE['cci9_window'],min_periods=1).max().astype(bool)
 up=((df['stoch_slope']>0).astype(int)+(df['cci20_slope']>0).astype(int)+(df['rsi_slope']>0).astype(int))
 reg=(df['h1_adx']>=BASE['h1_min'])&df['h1_trend']&(df['h4_adx']>=BASE['h4_min'])&df['h4_trend']
 o=(os>=BASE['open_prev_need'])&(cu>=BASE['open_cross_need'])&(df['cci20']>-100)&c9&(up>=BASE['up_slope_need'])&reg
 cond1=df['vol_ratio']>=post['vol']; cond2=(df['pdi5']>df['mdi5'])&(df['adx5_slope']>=post['adx_s'])&(df['pdi5_slope']>=post['pdi_s']); cond3=(df['macd_hist']>post['macd0'])&(df['macd_hist_delta']>=post['macd_d']); cond4=(df['obv']>df['obv_ema'])&(df['obv_slope']>=post['obv_s'])
 score=cond1.astype(int)+cond2.astype(int)+cond3.astype(int)+cond4.astype(int)
 o=o & (score>=post['need'])
 cd=((pr['stoch']>=BASE['st_ob'])&(df['stoch']<BASE['st_ob'])).astype(int)+((pr['cci20']>=BASE['cci_ob'])&(df['cci20']<BASE['cci_ob'])).astype(int)+((pr['rsi']>=BASE['rsi_ob'])&(df['rsi']<BASE['rsi_ob'])).astype(int)
 dn=((df['stoch_slope']<0).astype(int)+(df['cci20_slope']<0).astype(int)+(df['rsi_slope']<0).astype(int))
 c=(cd>=BASE['close_cross_need'])|((dn>=BASE['down_slope_need'])&((df['stoch']>=BASE['st_ob']-5)|(df['rsi']>=BASE['rsi_ob']-5)|(df['cci20']>=BASE['cci_ob']-15)))
 return o.fillna(False),c.fillna(False)

def bt(L,S,idx,post):
 lo,lc=signal(L,post); so,sc=signal(S,post); pos=None; ep=None; hp=None; et=None; eq=1.0; peak=1.0; mdd=0.0; rows=[]
 for ts in idx:
  lp=float(L.at[ts,'Close']); sp=float(S.at[ts,'Close'])
  if pos is None:
   if lo.at[ts] and not so.at[ts]: pos='L'; ep=lp; hp=lp; et=ts
   elif so.at[ts] and not lo.at[ts]: pos='S'; ep=sp; hp=sp; et=ts
   continue
  if pos=='L':
   hp=max(hp,lp); sw=bool(so.at[ts]); close=bool(lc.at[ts]) or lp<=ep*(1-BASE['stop']) or lp<=hp*(1-BASE['trail']) or sw
   if close:
    r=(lp/ep)-1; eq*=1+r; peak=max(peak,eq); mdd=max(mdd,1-eq/peak); rows.append(r)
    if sw: pos='S'; ep=sp; hp=sp; et=ts
    else: pos=None
  else:
   hp=max(hp,sp); sw=bool(lo.at[ts]); close=bool(sc.at[ts]) or sp<=ep*(1-BASE['stop']) or sp<=hp*(1-BASE['trail']) or sw
   if close:
    r=(sp/ep)-1; eq*=1+r; peak=max(peak,eq); mdd=max(mdd,1-eq/peak); rows.append(r)
    if sw: pos='L'; ep=lp; hp=lp; et=ts
    else: pos=None
 tr=pd.Series(rows)
 win=float((tr>0).mean()*100) if len(tr) else 0.0
 return {'cum':(eq-1)*100,'mdd':mdd*100,'win':win,'trades':len(tr)}

L=load('122630'); S=load('252670'); idx=L.index.intersection(S.index); L=enrich(L.loc[idx]); S=enrich(S.loc[idx])
results=[]
for vol,need,adxs,pdis,macd0,macdd,obvs in product([0.9,1.0],[1,2],[0.0,0.1],[0.0,0.5],[-0.02,-0.01],[-0.02,-0.01],[-10000,0]):
 post={'vol':vol,'need':need,'adx_s':adxs,'pdi_s':pdis,'macd0':macd0,'macd_d':macdd,'obv_s':obvs}
 o=bt(L,S,idx,post)
 if o['trades']<60: continue
 score=o['cum']*1.0 + o['trades']*0.04 - o['mdd']*0.45
 results.append((score,post,o))
results.sort(key=lambda x:x[0],reverse=True)
print('TOP high-frequency candidates')
for i,(s,p,o) in enumerate(results[:8],1):
 print(f"#{i} score={s:.2f} cum={o['cum']:.2f}% mdd={o['mdd']:.2f}% win={o['win']:.1f}% trades={o['trades']} post={p}")
