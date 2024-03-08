"""
Created on Tue Aug 25 19:10:00 2020

@author: Linus Grob, Dominik Boos
"""
import os
import numpy as np
import pandas as pd
import sqlalchemy as sq
import statsmodels.api as sm
from datetime import datetime
import datetime as dt
import scipy.optimize as op
from dotenv import load_dotenv

# ----------------------------------------------------------------------------------------------------------------------
# speed up db
from pandas.io.sql import SQLTable


def _execute_insert(self, conn, keys, data_iter):
    print("Using monkey-patched _execute_insert")
    data = [dict(zip(keys, row)) for row in data_iter]
    conn.execute(self.table.insert().values(data))


SQLTable._execute_insert = _execute_insert


# functions
# -------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
def gets(engine1, type, data_tab='data', desc_tab='cot_desc', series_id=None, bb_tkr=None, bb_ykey='COMDTY',
         start_dt='1900-01-01', end_dt='2100-01-01', constr=None, adjustment=None):
    if constr is None:
        constr = ''
    else:
        constr = ' AND ' + constr

    if series_id is None:
        if desc_tab == 'cot_desc':
            series_id = pd.read_sql_query("SELECT cot_id FROM cftc.cot_desc WHERE bb_tkr = '" + bb_tkr +
                                          "' AND bb_ykey = '" + bb_ykey + "' AND cot_type = '" + type + "'", engine1)
        else:
            series_id = pd.read_sql_query("SELECT px_id FROM cftc.fut_desc WHERE bb_tkr = '" + bb_tkr +
                                          "' AND adjustment= '" + adjustment + "' AND bb_ykey = '" + bb_ykey +
                                          "' AND data_type = '" + type + "'", engine1)
        print(desc_tab)
        print(type)
        series_id = str(series_id.values[0][0])
    else:
        series_id = str(series_id)

    h_1 = " WHERE px_date >= '" + str(start_dt) + "' AND px_date <= '" + str(end_dt) + "' AND px_id = "
    h_2 = series_id + constr + " order by px_date"
    fut = pd.read_sql_query('SELECT px_date, qty FROM cftc.' + data_tab + h_1 + h_2, engine1, index_col='px_date')
    return fut


# ----------------------------------------------------------------------------------------------------------------------
def getexposure(engine, type_of_trader, norm, bb_tkr, start_dt='1900-01-01', end_dt='2100-01-01', bb_ykey='COMDTY'):
    """
    Parameters
    ----------
    type_of_exposure : str()
        one of: 'net_managed_money','net_non_commercials','ratio_mm','ratio_nonc'
    bb_tkr : TYPE
        Ticker from the commofity; example 'KC'
    start_dt : str(), optional
        The default is '1900-01-01'.
    end_dt :  str(), optional
        The default is '2100-01-01'.
    bb_ykey :  str(), optional
        The default is 'COMDTY'.

    Returns
    -------
    exposure : pd.DataFrame() with Multiindex (cftc,net_specs)
        Returns the exposure of the underlying position in USD (net_pos * fut_price * (Multiplier(?)) )
    """
    # Note:
    # - Exposure = mult * fut_adj_none * net_pos
    # - contract_size =  mult * fut_adj_none

    pos = gets(engine, type=type_of_trader, data_tab='vw_data', bb_tkr=bb_tkr, bb_ykey=bb_ykey,
               start_dt=start_dt, end_dt=end_dt, adjustment=None)  # constr=constr,

    if norm == 'percent_oi':
        oi = gets(engine, type='agg_open_interest', data_tab='vw_data', desc_tab='cot_desc', bb_tkr=bb_tkr,
                  bb_ykey=bb_ykey, start_dt=start_dt, end_dt=end_dt)
        pos_temp = pd.merge(left=pos, right=oi, how='left', left_index=True, right_index=True,
                            suffixes=('_pos', '_oi'))
        exposure = pd.DataFrame(index=pos_temp.index, data=(pos_temp.qty_pos / pos_temp.qty_oi), columns=['qty'])

    elif norm == 'exposure':
        price_non_adj = gets(engine, type='contract_size', desc_tab='fut_desc', data_tab='vw_data', bb_tkr=bb_tkr,
                             bb_ykey=bb_ykey, start_dt=start_dt, end_dt=end_dt, adjustment='none')
        df_merge = pd.merge(left=pos, right=price_non_adj, left_index=True, right_index=True, how='left')

        exposure = pd.DataFrame(index=df_merge.index)
        exposure['qty'] = (df_merge.qty_y * df_merge.qty_x).values

    elif norm == 'number':
        exposure = pos.copy()
        print('--- NUMBER ---')

    midx = pd.MultiIndex(levels=[['cftc'], [type_of_trader]], codes=[[0], [0]])
    exposure.columns = midx

    exposure.dropna(inplace=True)
    # print(exposure.to_string())

    return exposure


# ----------------------------------------------------------------------------------------------------------------------
def getRetMat(_ret, maxlag, prefix='ret', decay=1, dropna=True):
    """
    Parameters
    ----------
    ret_ : pd.DataFrame()
        log return series
    maxlag : int
        DESCRIPTION.

    """

    # loop creates lagged returns in ret
    for i in range(0, maxlag):
        _ret[prefix, str(i + 1).zfill(3)] = _ret[prefix, '000'].shift(i + 1)

    if dropna:
        _ret = _ret.iloc[maxlag:, :maxlag + 1]  # delete the rows with nan due to its shift.
    return _ret


def merge_pos_ret(pos, ret, diff):
    if diff:
        cr = pd.merge(pos, ret.iloc[:, :-1], how='inner', left_index=True, right_index=True).diff().dropna()
    else:  # level
        print('---------------')
        cr = pd.merge(pos, ret.iloc[:, :-1], how='inner', left_index=True, right_index=True).dropna()
    return cr

# ----------------------------------------------------------------------------------------------------------------------
def getBeta(engine, model_id=None, model_type_id=None, bb_tkr=None, bb_ykey='COMDTY',
            start_dt='1900-01-01', end_dt='2100-01-01', constr=None, expand=False, drop_beta_date=True):
    if constr is None:
        constr = ''
    else:
        constr = ' AND ' + constr

    if model_id is None:
        model_id = pd.read_sql_query("SELECT model_id FROM cftc.model_desc WHERE bb_tkr = '" + bb_tkr +
                                     "' AND bb_ykey = '" + bb_ykey + "' AND model_type_id = '" + str(model_type_id) +
                                     "'", engine1)
        model_id = str(model_id.values[0][0])
    else:
        model_id = str(model_id)

    h_1 = " WHERE px_date >= '" + str(start_dt) + "' AND px_date <= '" + str(end_dt) + "'"
    h_1b = " WHERE beta_date >= '" + str(start_dt) + "' AND beta_date <= '" + str(end_dt) + "'"
    h_2 = " AND model_id = " + model_id + constr + " order by px_date"
    beta0 = pd.read_sql_query('SELECT px_date, return_lag, qty FROM cftc.beta' + h_1 + h_2, engine)
    beta = beta0.pivot(index='px_date', columns='return_lag', values='qty')

    if expand:
        beta.index.rename(name='beta_date', inplace=True)
        dates = pd.read_sql_query('SELECT * FROM cftc.beta_date' + h_1b, engine)
        beta = pd.merge(left=dates, right=beta, on='beta_date', how='left').set_index(['px_date', 'beta_date']).dropna()
        if drop_beta_date:
            beta.index = beta.index.droplevel('beta_date')
    return beta, model_id