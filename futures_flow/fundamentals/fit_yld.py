import numpy as np
import pandas as pd
import sqlalchemy as sq
import futures_flow.fetch.av as av
import futures_flow.ridge.ridge_functions as ridge
from futures_flow.private.engines import *
import statsmodels.api as sm
import matplotlib.pyplot as plt
from pandas.plotting import register_matplotlib_converters
ed = '2018-12-31'
cp = 'Corn'

engine = dbo_engine()


lt_yld = av.getlt(engine, crop=cp, type='Yield per Harvested Acre', start_yr=1990, header='yld')
df = pd.DataFrame({'year': lt_yld.index.tolist(),
                   'month': 2,
                   'day': 28})
date = pd.to_datetime(df)
date.index = lt_yld.index
print(lt_yld)
lt_yld = pd.concat([lt_yld[['yld']], date], axis=1).set_index(0)
lt_yld.index.names = ['date']
lt_yld['lt_log_yld'] = np.log(lt_yld.yld)

w_yld = av.getw(engine, wtype='Yield per Harvested Acre', crop=cp, region='United States', start_dt='1999-12-31')
w_yld.index.names = ['date']
w_yld['log_yld'] = np.log(w_yld.yield_per_harvested_acre)
w_hvst = av.getw(engine, wtype='Area Harvested', crop=cp, region='United States', start_dt='1999-12-31', header='area')
w_pltd = av.getw(engine, wtype='Area Planted', crop=cp, region='United States', start_dt='1999-12-31', header='area')
nhvst = 1 - w_hvst / w_pltd
plt.plot(nhvst)


c1 = av.getc(engine, crop=cp, category='Condition', ctype='Excellent', start_dt='1999-12-31', end_dt=ed)
c2 = av.getc(engine, crop=cp, category='Condition', ctype='Good', start_dt='1999-12-31', end_dt=ed)
c3 = av.getc(engine, crop=cp, category='Condition', ctype='Fair', start_dt='1999-12-31', end_dt=ed)
c4 = av.getc(engine, crop=cp, category='Condition', ctype='Poor', start_dt='1999-12-31', end_dt=ed)
c5 = av.getc(engine, crop=cp, category='Condition', ctype='Very Poor', start_dt='1999-12-31', end_dt=ed)
c = pd.concat([c1, c2, c3, c4, c5], axis=1).fillna(value=0)
c.index.names = ['date']
c.index = pd.to_datetime(c.index)
comb = pd.concat([w_yld, lt_yld, c], axis=1).fillna(method='bfill').loc[c.index]
comb['xyld'] = comb.log_yld-comb.lt_log_yld
comb['year'] = comb.index.year
print(comb.to_string())

# results = sm.ols(formula='xyld ~ good + poor', data=comb).fit()
gamma = ridge.getGamma(5, gammatype='flat')

#gamma = np.eye(5)

x0 = comb.loc[:, ['excellent', 'good', 'fair', 'poor', 'very_poor']].values
y0 = comb.loc[:, ['xyld']]
alpha = ridge.get_alpha(criteria='loocv', y=y0, x=x0, gma=gamma, alpha_start=100, scale=10000000)
alpha = 500
print(alpha)
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