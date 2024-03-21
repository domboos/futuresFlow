import av
import sqlalchemy as sq
import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import mwh_functions
import statsmodels.formula.api as sm

engine = mwh_functions.mwh_engine()

h = av.getw(engine, 'Yield per Harvested Acre', start_dt = '1995-12-31', end_dt='2017-12-31', crop='Corn',
            region='United States', header='corn_yield_wd')
df = h[h.index.month == 5]
df.index = df.index.year

norm_yld = av.getlt(engine, crop='Corn', type='Yield per Harvested Acre', include_marketing_yr=False, end_yr=2017,
                    header='corn_yield_lt').merge(df, how='outer', left_index=True, right_index=True)
norm_yld.corn_yield_lt.fillna(norm_yld.corn_yield_wd, inplace=True)
norm_yld.drop(columns='corn_yield_wd', inplace=True)
print(norm_yld)

cpa = av.getpc(engine, type='Total, Costs Listed', crop='Corn', header='cost')
print(cpa)
cpa.columns = ['cost']
print(norm_yld)
norm_yld.columns = ['cost']
cost = cpa.div(norm_yld)
plt.plot(cost)
df = pd.DataFrame({'year': cost.index.tolist(),
                   'month': 6,
                   'day': 30})
date = pd.to_datetime(df)
print(type(date))
cost.index = date.values
print(cost)

sd = '1996-05-01'
cp = 'Corn'
series = [2]
w = [1]
# stock to use
EndingStocks = av.getw(engine, wtype='Ending Stocks', crop=cp, region='United States', unit='Million Bushels',
                       start_dt=sd)
TotalUse = av.getw(engine, wtype='Use, Total', crop=cp, region='United States', unit='Million Bushels', start_dt=sd)

C2 = pd.concat([EndingStocks, TotalUse], axis=1)
C2['stock2use'] = np.log(C2['ending_stocks']) - np.log(C2['use_total'])

C1 = np.log(av.getf(engine, cp, 'Stocks to Use Ratio', 365, header='stock2use0'))

val = av.gets(engine, series_id=series, tab='dbo.vw_fut_avg_log_px_all', type='px_log_avg', start_dt=sd)
roll = av.gets(engine, series_id=series, tab='dbo.vw_fut_slope_log_px_all', type='px_log_slope', start_dt=sd)

comb = pd.concat([C1, C2, val, roll, cost], axis=1).fillna(method='pad').loc[C2.index].fillna(method='bfill')
comb['margin'] = comb.px_log_avg - np.log(comb.cost*100)

print(list(comb))

# ---
fig, axs = plt.subplots(1, 2, figsize=(20, 10), sharey=True)
fig.suptitle('Stock to Use Ratio and Margin')
axs[0].scatter(comb.stock2use, comb.margin)
# calc the trendline
z2 = np.polyfit(comb.stock2use, comb.margin, 1)
p2 = np.poly1d(z2)
results = sm.ols(formula='margin ~ stock2use', data=comb).fit()
print(results.summary())
axs[0].plot(comb.stock2use, p2(comb.stock2use), "r--")

axs[1].scatter(comb.stock2use0, comb.margin)
# cal
z2 = np.polyfit(comb.stock2use0, comb.margin, 1)
p2 = np.poly1d(z2)
results = sm.ols(formula='margin ~ stock2use0', data=comb).fit()
print(results.summary())
axs[1].plot(comb.stock2use0, p2(comb.stock2use0), "r--")

# ---
plt.figure(4)
plt.scatter(comb.px_log_slope, comb.margin)
# calc the trendline
z3 = np.polyfit(comb.px_log_slope, comb.margin, 1)
p3 = np.poly1d(z3)
plt.plot(comb.px_log_slope, p3(comb.px_log_slope), "r--")
plt.title('Roll and Margin')

plt.show()