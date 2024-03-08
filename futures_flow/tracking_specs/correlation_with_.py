# -*- coding: utf-8 -*-
"""
Created on Tue April 20, 2022

@author: grbi, dominik
"""

import numpy as np
import sqlalchemy as sq
import statsmodels.api as sm
from datetime import datetime
import scipy.optimize as op
from cfunctions import *

#


# crate engine
from cengine import cftc_engine
engine1 = cftc_engine()
print(engine1)

# speed up db
from pandas.io.sql import SQLTable

def _execute_insert(self, conn, keys, data_iter):
    print("Using monkey-patched _execute_insert")
    data = [dict(zip(keys, row)) for row in data_iter]
    conn.execute(self.table.insert().values(data))

SQLTable._execute_insert = _execute_insert

import pandas as pd

#h = gets(engine1, type, data_tab='forecast', desc_tab='cot_desc', series_id=None, bb_tkr=None, bb_ykey='COMDTY',
#         start_dt='1900-01-01', end_dt='2100-01-01', constr=None, adjustment=None)
bb = pd.read_sql_query("select FM.bb_tkr, FM.multiplier from cftc.order_of_things OT inner join cftc.fut_mult FM on "
                       "FM.bb_tkr = OT.bb_tkr order by ranking", engine1)
print(bb)
bb2 = bb.copy()
model_id1 = 82
model_id2 = 149
trader = 'long_pump'

for idx, tkr in bb.iterrows():
    print(tkr.bb_tkr)
    forecast1 = pd.read_sql_query("select FC.px_date, FC.qty as mom_pos_change from cftc.forecast FC inner join cftc.model_desc " +
                                  " MD ON FC.model_id = MD.model_id where MD.model_type_id = " + str(model_id1) + " and MD.bb_tkr = '"
                                  + str(tkr.bb_tkr) + "' order by FC.px_date ",
                                  engine1, index_col='px_date')
    nc = getexposure(engine1, 'net_non_commercials', norm='number', bb_tkr=tkr.bb_tkr)
    nc.columns = nc.columns.droplevel(0)
    price = gets(engine1, type='px_last', desc_tab='fut_desc', data_tab='data', bb_tkr=tkr.bb_tkr, adjustment='none')
    price.columns = ['prz']
    ret_series = gets(engine1, type='px_last', desc_tab='fut_desc', data_tab='data', bb_tkr=tkr.bb_tkr, adjustment='by_ratio')
    ret_series.loc[:, 'ret'] = ret_series['qty'] / ret_series['qty'].shift(1)-1

    hh = pd.merge(nc, price, on='px_date')
    hh.loc[:, 'exposure'] = hh['net_non_commercials'] * hh['prz'] * tkr.multiplier
    hh.loc[:, 'exposure_l'] = hh['exposure'].shift(1)
    h2 = pd.merge(hh, ret_series.loc[:, 'ret'], on='px_date')
    h2['exp_change'] = h2['exposure_l'] * h2['ret']
    h3 = pd.merge(forecast1, h2['exp_change'], on='px_date')
    h3['delta'] = h3['mom_pos_change'] - h3['exp_change']

    forecast2 = pd.read_sql_query("select FC.px_date, FC.qty as oth_pos_change from cftc.forecast FC inner join cftc.model_desc " +
                                  " MD ON FC.model_id = MD.model_id where MD.model_type_id = " + str(model_id2) + " and MD.bb_tkr = '"
                                  + str(tkr.bb_tkr) + "' order by FC.px_date ",
                                  engine1, index_col='px_date')
    pm = getexposure(engine1, trader, norm='number', bb_tkr=tkr.bb_tkr)
    pm.columns = pm.columns.droplevel(0)
    gg = pd.merge(pm, price, on='px_date')
    gg.loc[:, 'exposure'] = gg[trader] * gg['prz'] * tkr.multiplier
    gg.loc[:, 'exposure_l'] = gg['exposure'].shift(1)
    g2 = pd.merge(gg, ret_series.loc[:, 'ret'], on='px_date')
    g2['exp_change'] = g2['exposure_l'] * g2['ret']
    g3 = pd.merge(forecast2, g2['exp_change'], on='px_date')
    g3['delta'] = g3['oth_pos_change'] - g3['exp_change']

    hh = pd.merge(forecast1, forecast2, on='px_date')

    bb2.loc[idx, 'corr_'] = hh['oth_pos_change'].corr(hh['mom_pos_change'])
    bb2.loc[idx, 'std1_'] = hh['oth_pos_change'].std()
    bb2.loc[idx, 'std2_'] = hh['mom_pos_change'].std()
    bb2.loc[idx, 'ratio_'] = hh['mom_pos_change'].std()/hh['oth_pos_change'].std()

    bb2.loc[idx, 'corr'] = h3['delta'].corr(g3['delta'])
    bb2.loc[idx, 'std1'] = h3['delta'].std()
    bb2.loc[idx, 'std2'] = g3['delta'].std()
    bb2.loc[idx, 'ratio'] = h3['delta'].std()/g3['delta'].std()

print(bb2.to_string())
# px = gets(engine1, type='px_last', desc_tab='fut_desc', data_tab='data', bb_tkr='C', adjustment='none')



