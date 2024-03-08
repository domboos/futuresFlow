"""All funcs for Return Calculation"""
import numpy as np
import pandas as pd


def create_ret_mat(rets: np.array, maxlag: int) -> np.ndarray:
    """
    Info: returns need to be in ascending order.
    """
    innermaxlag = maxlag + 1
    mat = []

    for ind in range(innermaxlag, len(rets) + 1):
        mat.append(np.flip(rets[ind - innermaxlag:ind]))

    _retmat = np.squeeze(np.array(mat))
    return _retmat


def calculate_returns_single_asset(prices: pd.DataFrame) -> pd.DataFrame:
    """ calculates returns for a single asset"""
    if not isinstance(prices.index, pd.DatetimeIndex):
        raise TypeError('Index of variable prices must be DateTimeIndex!')
    if not prices.shape[1] == 1:
        raise ValueError(f'bad input shape {prices.shape} must be ({prices.shape[0]},1)')

    ret_df = pd.DataFrame(prices.index, columns=['ret'])
    ret_df.loc[:, 'ret'] = np.log(prices / prices.shift(1))
    return ret_df
