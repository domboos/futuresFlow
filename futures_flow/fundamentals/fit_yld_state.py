import numpy as np
import pandas as pd
import cfunctions as cf
import sqlalchemy as sq
import av
import statsmodels.api as sm
import matplotlib.pyplot as plt
import mwh_functions
from pandas.plotting import register_matplotlib_converters
ed = '2019-12-31'
cp = 'Corn'
state = 'Ohio'


engine = mwh_functions.mwh_engine()

w_yld = av.getw(engine, wtype='Yield per Harvested Acre', crop=cp, region=state, start_dt='1999-12-31')
print(w_yld)
w_yld.index.names = ['date']
w_yld['log_yld'] = np.log(w_yld.yield_per_harvested_acre)
w_hvst = av.getw(engine, wtype='Area Harvested', crop=cp, region=state, start_dt='1999-12-31', header='area')
w_pltd = av.getw(engine, wtype='Area Planted', crop=cp, region=state, start_dt='1999-12-31', header='area')
nhvst = 1 - w_hvst / w_pltd
plt.plot(nhvst)


c1 = av.getc(engine, crop=cp, region=state, category='Condition', ctype='Excellent', start_dt='1999-12-31', end_dt=ed)
c2 = av.getc(engine, crop=cp, region=state, category='Condition', ctype='Good', start_dt='1999-12-31', end_dt=ed)
c3 = av.getc(engine, crop=cp, region=state, category='Condition', ctype='Fair', start_dt='1999-12-31', end_dt=ed)
c4 = av.getc(engine, crop=cp, region=state, category='Condition', ctype='Poor', start_dt='1999-12-31', end_dt=ed)
c5 = av.getc(engine, crop=cp, region=state, category='Condition', ctype='Very Poor', start_dt='1999-12-31', end_dt=ed)
c = pd.concat([c1, c2, c3, c4, c5], axis=1).fillna(value=0)
c.index.names = ['date']
c.index = pd.to_datetime(c.index)
comb = pd.concat([w_yld, c], axis=1).fillna(method='bfill').loc[c.index]
#comb['xyld'] = comb.log_yld-comb.lt_log_yld
comb['year'] = comb.index.year
print(comb.to_string())

# results = sm.ols(formula='xyld ~ good + poor', data=comb).fit()
gamma = cf.getGamma(5, gammatype='flat')
#gamma = np.eye(5)

x0 = comb.loc[:, ['excellent', 'good', 'fair', 'poor', 'very_poor']].values
y0 = comb.loc[:, ['xyld']]
alpha = cf.getAlpha(alpha_type='loocv', y=y0, x=x0, gma=gamma, start=10)
print(alpha * gamma)
x = np.concatenate((comb.loc[:, ['excellent', 'good', 'fair', 'poor', 'very_poor']], alpha * gamma), axis=0)
y = np.concatenate((comb.loc[:, ['xyld']], np.zeros((gamma.shape[0], 1))))
model_fit = sm.OLS(y, x).fit()

print(model_fit.summary())

print(c.cov())
print(c.corr())
w, v = np.linalg.eig(np.array(c.corr()))
print(w)
print(v)