import numpy as np
import pandas as pd
from futures_flow.ridge.ridge_functions import getRetMat

# done in Excel
def get_day_of_year(X):
    # define the day of year function
    # year has 366 days
    f = lambda x: x.timetuple().tm_yday
    f_vec = np.vectorize(f)
    return f_vec(X.index.values[:])[:, np.newaxis]

# https://stats.stackexchange.com/questions/17463/signal-extraction-problem-conditional-expectation-of-one-item-in-sum-of-indepen


def garch_prdict(y_ins, y_oos, W, omega=0, name='daily_return'):
    lags = np.size(W)
    y = pd.concat([y_ins, y_oos], sort=True)
    y.columns = pd.MultiIndex(levels=[['ret'], [str(0).zfill(3)]], codes=[[0], [0]])
    lag_y = getRetMat(y, lags)
    _W = np.concatenate(([[0], W]))
    _predict = lag_y @ _W + omega
    return _predict[_predict.index.isin(y_oos.index)].to_frame(name)


def getR2(df_sample, df_fcast):
    df_sample.columns = ['stupid_pandas']
    df_fcast.columns = ['stupid_pandas']
    e_diff = df_sample - df_fcast
    mspe_diff = (e_diff ** 2).mean(axis=0)
    var_diff = ((df_sample - df_sample.mean(axis=0)) ** 2).mean(axis=0)
    oosR2_diff = 1 - mspe_diff / var_diff
    return oosR2_diff[0]


def getMSE(df_sample, df_fcast):
    df_sample.columns = ['stupid_pandas']
    df_fcast.columns = ['stupid_pandas']
    e_diff = df_sample - df_fcast
    mse = (e_diff ** 2).mean(axis=0)
    return mse[0]


def getMAE(df_sample, df_fcast):
    df_sample.columns = ['stupid_pandas']
    df_fcast.columns = ['stupid_pandas']
    e_diff = df_sample - df_fcast
    mae = e_diff.abs().mean(axis=0)
    return mae[0]


def getLL(df_sample, df_fcast):
    df_sample.columns = ['stupid_pandas']
    df_fcast.columns = ['stupid_pandas']
    e_diff = np.log(df_sample) - np.log(df_fcast)
    LL = (e_diff ** 2).mean(axis=0)
    return LL[0]


def getHMSE(df_sample, df_fcast):
    df_sample.columns = ['stupid_pandas']
    df_fcast.columns = ['stupid_pandas']
    e_diff = df_sample / df_fcast -1
    HMSE = (e_diff ** 2).mean(axis=0)
    return HMSE[0]


def getGMLE(df_sample, df_fcast):
    df_sample.columns = ['stupid_pandas']
    df_fcast.columns = ['stupid_pandas']
    e_diff = np.log(df_fcast) + (df_sample / df_fcast)
    GMLE = e_diff.mean(axis=0)
    return GMLE[0]