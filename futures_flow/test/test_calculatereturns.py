import pickle

import numpy as np
import pandas as pd

from futures_flow.core.calculatereturns import create_ret_mat, calculate_returns_single_asset, \
    calculate_avg_returns
from futures_flow.core.util import get_root_directory


def getTestData() -> pd.DataFrame:
    root_dir = get_root_directory()
    df_prices = pd.read_csv(f"{root_dir}/optimized_momentum/test/test_data/PL_prices.csv", sep=';')
    df_prices = df_prices.set_index('date')
    df_prices.index = pd.to_datetime(df_prices.index)
    return df_prices



def get_ordinary_return_series() -> pd.DataFrame:
    root_dir = get_root_directory()

    with open(root_dir + '/data/clean/ordinaryReturnSeries.pickle', 'rb') as file:
        result_dict = pickle.load(file)

    result_df = pd.concat(result_dict, axis=0)
    return result_df




def test_create_ret_mat_logic():
    # GIVEN
    maxlag = 3
    values = np.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

    # WHEN
    result = create_ret_mat(rets=values, maxlag=maxlag)

    # THEN -- check shape:
    assert result.shape[0] == 8 and result.shape[1] == 4
    # check values:
    assert result[0][0] == 3 and result[0][1] == 2 and result[0][2] == 1 and result[0][3] == 0
    assert result[1][0] == 4 and result[1][1] == 3 and result[1][2] == 2 and result[1][3] == 1
    assert result[2][0] == 5 and result[2][1] == 4 and result[2][2] == 3 and result[2][3] == 2
    assert result[3][0] == 6 and result[3][1] == 5 and result[3][2] == 4 and result[3][3] == 3
    assert result[7][0] == 10 and result[7][1] == 9 and result[7][2] == 8 and result[7][3] == 7


def test_create_ret_mat():
    # GIVEN
    df_prices = getTestData()
    root_dir = get_root_directory()
    df_prices = pd.read_csv(f"{root_dir}/optimized_momentum/test/test_data/PL_prices.csv", sep=';')
    df_prices = df_prices.set_index('date')

    ret_series = pd.DataFrame(index=df_prices.index)
    ret_series.loc[:, 'ret'] = np.log(df_prices / df_prices.shift(1))
    ret_series = ret_series.dropna()  # deletes first value
    rets = ret_series.values

    maxlag = 260

    # WHEN
    result = create_ret_mat(rets=rets, maxlag=maxlag)

    # THEN
    # first col is equal to the return series
    for el in range(260, len(rets)):
        assert result[el - 260][0] == rets[el][0]
    assert result.shape[0] == 539 and result.shape[1] == 261


def test_calculate_returns_checkShape():
    # GIVEN
    df_prices = getTestData()

    # WHEN
    rets = calculate_returns_single_asset(df_prices)
    rets = rets.dropna()

    # THEN
    shape_p = df_prices.shape
    shape_r = rets.shape
    assert shape_r[0] == shape_p[0] - 1 and shape_r[1] == shape_p[1]


def test_calculate_demeaned_returnMatrix():
    #GIVEN
    rets = get_ordinary_return_series()

    #WHEN
    avg_rets = calculate_avg_returns()

    #THEN:
    assert avg_rets.shape[0] == 5535
    assert avg_rets.shape[1] == 1



