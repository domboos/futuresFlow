# -*- coding: utf-8 -*-
"""
Created on Tue Aug 25 19:10:00 2020

@author: grbi, dominik
"""

import numpy as np
import sqlalchemy as sq
import statsmodels.api as sm
from datetime import datetime
import scipy.optimize as op
from cfunctions import *

# crate engine
from cengine import cftc_engine
engine1 = cftc_engine()

# speed up db
from pandas.io.sql import SQLTable

def _execute_insert(self, conn, keys, data_iter):
    print("Using monkey-patched _execute_insert")
    data = [dict(zip(keys, row)) for row in data_iter]
    conn.execute(self.table.insert().values(data))

SQLTable._execute_insert = _execute_insert

import pandas as pd

# MAIN

# refreshing model view and fetching
alpha = 100
s0 = np.array([100, 100])
conn = engine1.connect()
conn.execute('REFRESH MATERIALIZED VIEW cftc.vw_model_desc')
model_list = pd.read_sql_query("SELECT * FROM cftc.vw_model_desc WHERE "
                               "max_date IS NULL and model_id > 4779 ORDER BY bb_tkr, bb_ykey",
                               engine1)
conn.close()

print(model_list.to_string())

for idx, model in model_list.iterrows():
    # feching and structure returns
    print(datetime.now().strftime("%H:%M:%S"))
    if idx == 0 or (bb_tkr != model.bb_tkr or bb_ykey != model.bb_ykey):
        bb_tkr = model.bb_tkr
        bb_ykey = model.bb_ykey
        fut = gets(engine1, type='px_last', desc_tab='fut_desc', data_tab='data', bb_tkr=bb_tkr, adjustment='by_ratio')
        # calc rets:
        ret_series = pd.DataFrame(index=fut.index)
        ret_series.loc[:, 'ret'] = np.log(fut / fut.shift(1))
        ret_series = ret_series.dropna()  # deletes first value
        ret_series.columns = pd.MultiIndex(levels=[['ret'], [str(0).zfill(3)]], codes=[[0], [0]])

    # decay
    window = model.est_window
    lags = np.arange(1, model.lookback+1)
    beta = pd.DataFrame(columns={'model_id', 'px_date', 'return_lag', 'qty'})
    beta['return_lag'] = lags
    beta['model_id'] = model.model_id
    fcast = pd.DataFrame(data=[model.model_id], columns={'model_id'})
    alpha_df = pd.DataFrame(data=[model.model_id], columns={'model_id'})
    if model.decay is not None:
        retFac = np.fromfunction(lambda i, j: model.decay ** i, [window, model.lookback])[::-1]

    # gamma
    gamma = getGamma(maxlag=model.lookback, regularization=model.regularization, gammatype=model.gamma_type,
                     gammapara=model.gamma_para, naildownvalue0=model.naildown_value0,
                     naildownvalue1=model.naildown_value1)

    # fecthing cot, crate lagged returns and merge
    pos = getexposure(engine1, type_of_trader=model.cot_type, norm=model.cot_norm, bb_tkr=bb_tkr, bb_ykey=bb_ykey)
    ret = getRetMat(ret_series, model.lookback) # this is too long
    cr = merge_pos_ret(pos, ret, model.diff)

    for idx2, day in enumerate(cr.index[0:-(window + model.fit_lag)]):

        # rolling window parameters:
        w_start = cr.index[idx2]
        w_end = cr.index[idx2 + window]
        # welcher wert????
        forecast_period = cr.index[idx2 + window + model.fit_lag]  # includes the day x in [:x]

        if model.decay is not None:
            x0 = cr['ret'].loc[w_start:w_end, :].values * retFac
            y0 = cr['cftc'].loc[w_start:w_end, :] * retFac[:, 1] # not tested
            print('applying decay')
        else:
            x0 = cr['ret'].loc[w_start:w_end, :].values
            y0 = cr['cftc'].loc[w_start:w_end, :]

        #try:
        if model.gamma_type == 'new3':
            gamma1 = getGamma(maxlag=model.lookback, regularization=model.regularization, gammatype='linear',
                            gammapara=model.gamma_para, naildownvalue0=model.naildown_value0,
                            naildownvalue1=model.naildown_value1)
            gamma2 = getGamma(maxlag=model.lookback, regularization=model.regularization, gammatype='flat',
                            gammapara=model.gamma_para, naildownvalue0=model.naildown_value0,
                            naildownvalue1=model.naildown_value1)
            gamma_tmp, s0 = getGammaOpt(y=y0, x=x0, gma1=gamma1, gma2=gamma2, start=s0)
        else:
            alpha = getAlpha(alpha_type=model.alpha_type, y=y0, x=x0, gma=gamma, start=alpha) * model.alpha
            gamma_tmp = gamma * alpha
        #except:
        #   print(y0)
        #   print(x0.shape)
        #   print(gamma.shape)
        #   print(model.lookback)
        #   alpha = getAlpha(alpha_type=model.alpha_type, y=y0, x=x0, gma=gamma, start=alpha) * model.alpha

        y = np.concatenate((y0, np.zeros((gamma.shape[0], 1))))
        x = np.concatenate((x0, gamma_tmp), axis=0)

        ##  fit the models
        model_fit = sm.OLS(y, x).fit()

        beta.qty = model_fit.params
        beta.px_date = forecast_period
        fcast['qty'] = model_fit.predict(cr['ret'].loc[forecast_period, :].values)
        fcast['px_date'] = forecast_period
        alpha_df['qty'] = alpha
        alpha_df['px_date'] = forecast_period
        if idx2 == 0:
            beta_all = beta.copy()
            fcast_all = fcast.copy()
            alpha_all = alpha_df.copy()
        else:
            beta_all = beta_all.append(beta, ignore_index=True)
            fcast_all = fcast_all.append(fcast, ignore_index=True)
            alpha_all = alpha_all.append(alpha_df, ignore_index=True)
        del x0, y0, x, y

    beta_all.to_sql('beta', engine1, schema='cftc', if_exists='append', index=False)
    fcast_all.to_sql('forecast', engine1, schema='cftc', if_exists='append', index=False)
    alpha_all.to_sql('alpha', engine1, schema='cftc', if_exists='append', index=False)
    del gamma, cr, ret, pos
    print('---')

    # hi