"""insample Statistics for Regressions with Thikonov Regularization"""
# see TST Appendix C and referencest threrein

import numpy as np
import pandas as pd


def hatMatrix(_x, g):
    return _x @ np.linalg.inv(np.transpose(_x) @ _x + np.transpose(g) @ g) @ np.transpose(_x)


def degFree(_x, g, n):
    H = hatMatrix(_x, g)
    return n - np.trace(2 * H - H @ np.transpose(H))


def std_err(_x, _y, g, b):
    _inv = np.linalg.inv(np.transpose(_x) @ _x + np.transpose(g) @ g)
    v0 = np.diag(_inv @ np.transpose(_x) @ _x @ _inv)
    nu = degFree(_x, g, _y.shape[0])
    resid = _y - _x @ b
    # sigma_sqr = np.transpose(resid) @ resid / nu
    sigma_sqr = np.sum(resid * resid) / nu
    return np.sqrt(v0 * sigma_sqr)


def rsqrt(_x, _y, g, b):
    if isinstance(_x, pd.DataFrame):
        _x = _x.values
    if isinstance(_y, pd.DataFrame):
        _y = _y.values
    nu = degFree(_x, g, _y.shape[0])
    resid = _y - _x @ b
    vaResid = np.sum(resid * resid) / nu
    dm_y = _y - np.mean(_y)
    varY = np.sum(dm_y * _y) / (dm_y.shape[0] - 1)
    return (varY - vaResid) / varY


def tstat(_x, _y, g, b):
    if isinstance(_x, pd.DataFrame):
        _x = _x.values
    if isinstance(_y, pd.DataFrame):
        _y = _y.values
    print(_x.shape)
    print(_y.shape)
    print(b.shape)
    return b / np.reshape(std_err(_x, _y, g, b), (-1, 1))
