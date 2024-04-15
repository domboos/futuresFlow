"""Functions for Leave-one-out cross validation for generalized ridge"""
import numpy as np
import pandas as pd
import scipy.optimize as op


def get_alpha(criteria, y, x, gma=np.ndarray, alpha_start: float = 100, scale: float = 1) -> float:
    """Leave-one-out cross validation with PRESS or GCV

    Parameters
    ----------
    criteria: press/loocv or gcv (see TST or risky times for details)
    y : ndarray, shape (n,) or pandas df
        dependent variable
    x : ndarray, shape (n,m) or pandas df
        independent observations
    gma : ndarray, shape (m,m)
        generalized ridge / Tikhonov matrix (e.g. fused ridge)
    alpha_start : float
        initial value for alpha
    scale: scaling factor of the target function (to get better numerical properties)

    Returns
    -------
    alpha: float
        value which is multiplied by gamma to get the best estimations for the betas
    """
    if isinstance(y, np.ndarray):
        y_tmp = y
    else:
        y_tmp = y.values
    if isinstance(x, np.ndarray):
        x_tmp = x
    else:
        x_tmp = x.values

    if criteria in ('loocv', 'press'):
        _fun = lambda z, z1=y_tmp, z2=x_tmp, z3=gma: press(z, z1, z2, z3) * scale
    elif criteria == 'gcv':
        _fun = lambda z, z1=y_tmp, z2=x_tmp, z3=gma: gcv(z, z1, z2, z3) * scale

    print('press done -> now optimizing')
    # percent: 10000000000000000
    res = op.minimize(_fun,
                      x0=alpha_start,
                      method='BFGS',
                      tol=0.0001,
                      options={'disp': True,
                               'maxiter': 20,
                               'gtol': 0.00001,
                               'eps': 0.0001}
                      )

    if res.success:
        # print(res.x)
        print(f"optimization successful: alpha has the value of: {res.x}")
        return res.x
    else:
        print(f"optimization was not successful: alpha has the value of: 1")
        return alpha_start


def press(alpha, _y: np.ndarray, _x: np.ndarray, gamma_mat):
    """PRESS criteria function to be minimized for loocv - see TST Appendix B.1

    Parameters
    ----------
    alpha : scaling factor
    _y: independent variable
    _x: dependent variable
    gamma_mat: generalized ridge / Tikhonov matrix (e.g. fused ridge)

    Returns
    -------
    PRESS value

    """
    n = np.shape(_y)[0]
    # H = _x @ np.linalg.inv(np.transpose(_x) @ _x + a * a * np.transpose(g) @ g) @ np.transpose(_x)
    _H = np.transpose(_x) @ _x + alpha * alpha * np.transpose(gamma_mat) @ gamma_mat
    H = _x @ np.linalg.solve(_H, np.transpose(_x))
    # iH = np.identity(n) - H # B = np.diag(1 / np.diag(iH)) # BiHy = B @ iH @ _y
    BiHy = (_y - H @ _y) / (1 - np.diag(H))
    k = np.transpose(BiHy) @ BiHy / n
    if np.isscalar(k):
        return k
    else:
        return k[0][0]


def gcv(alpha, _y: np.ndarray, _x: np.ndarray, gamma_mat):
    """criteria function to be minimized for gcv (generalized cross validation)

    Parameters
    ----------
    alpha : scaling factor
    _y: independent variable
    _x: dependent variable
    gamma_mat: generalized ridge / Tikhonov matrix (e.g. fused ridge)

    Returns
    -------
    PRESS value

    """
    n = np.shape(_y)[0]

    _H = np.transpose(_x) @ _x + alpha * alpha * np.transpose(gamma_mat) @ gamma_mat
    # iH = np.identity(n) - _x @ np.linalg.inv(np.transpose(_x) @ _x + a * a * np.transpose(g) @ g) @ np.transpose(_x)
    iH = np.identity(n) - _x @ np.linalg.solve(_H, np.transpose(_x))
    iHy = iH @ _y
    tiH = np.trace(iH)
    k = np.transpose(iHy) @ iHy / (tiH * tiH)
    if np.isscalar(k):
        return k
    else:
        return k[0][0]


# ----------------------------------------------------------------------------------------------------------------------
# Creating Tikhonov Matrices & lag structure
# ----------------------------------------------------------------------------------------------------------------------


def get_kth_diag_indices(mat, k):
    """helper function for the Gamma Function"""
    rows, cols = np.diag_indices_from(mat)
    if k < 0:
        return rows[-k:], cols[:k]
    if k > 0:
        return rows[:-k], cols[k:]

    return rows, cols


def getGamma(maxlag, regularization='d1', gammatype='sqrt', gammapara=1, weights=None, normalize=True):
    """
    Parameters
    ----------
    ...

    """

    def kth_diag_indices(a, k):
        rows, cols = np.diag_indices_from(a)
        if k < 0:
            return rows[-k:], cols[:k]
        elif k > 0:
            return rows[:-k], cols[k:]
        else:
            return rows, cols

    gamma = np.zeros((maxlag, maxlag))

    t = maxlag

    if gammatype == 'loo':
        gammatype_tmp = 'loo'
        gammatype = 'flat'
    elif gammatype == 'circ':
        gammatype_tmp = 'circ'
        gammatype = 'flat'
    else:
        gammatype_tmp = 'xxx'

    # loop creates gamma
    if weights is None:
        for i in range(0, t):

            if gammatype == 'dom':
                gamma[i, i] = 1 - 1 / (i + 1) ** gammapara

            elif gammatype == 'flat':
                gamma[i, i] = 1

            elif gammatype == 'flat1':
                if i > 0:
                    gamma[i, i] = 1

            elif gammatype == 'linear1':
                if i > 0:
                    gamma[i, i] = (i + 1) / (maxlag + 1)

            elif gammatype == 'linear':
                gamma[i, i] = (i + 1) / (maxlag + 1)

            elif gammatype == 'arctan':
                gamma[i, i] = np.arctan(gammapara * i)

            elif gammatype == 'log':
                gamma[i, i] = np.log(1 + gammapara * i / maxlag)

            elif gammatype == 'sqrt':
                gamma[i, i] = np.sqrt(i + gammapara)

            elif gammatype == 'decay':
                gamma[i, i] = gammapara ** i
    else:
        gamma = np.diag(weights)

    # standardize sum of diagonal values to 1
    if normalize:
        gsum = gamma.diagonal(0).sum()
        gamma[np.diag_indices_from(gamma)] /= gsum

    # default case
    rows, cols = kth_diag_indices(gamma, 1)
    gamma[rows, cols] = -gamma.diagonal()[:-1]
    # naildown

    if regularization == 'd2':
        if gammatype_tmp != 'circ':
            gamma[np.diag_indices_from(gamma)] /= 2

            rowsm1, colsm1 = kth_diag_indices(gamma, 2)
            gamma[rowsm1, colsm1] = gamma.diagonal()[:-2]

        if gammatype_tmp == 'circ':
            gamma[np.diag_indices_from(gamma)] /= 2

            rowsm1, colsm1 = kth_diag_indices(gamma, 2)
            gamma[rowsm1, colsm1] = gamma.diagonal()[:-2]

            # fade out:
            gamma[maxlag - 2, 0] = gamma[maxlag - 2, maxlag - 2]
            gamma[maxlag - 1, 1] = gamma[maxlag - 1, maxlag - 1]
            gamma[maxlag - 1, 0] = - 2 * gamma[maxlag - 1, maxlag - 1]
    else:
        if gammatype_tmp == 'circ':
            gamma[maxlag - 1, 0] = - gamma[maxlag - 2, maxlag - 2]
            gamma[maxlag - 1, maxlag - 1] = gamma[maxlag - 2, maxlag - 2]

        if gammatype_tmp == 'loo':
            gamma = np.delete(gamma, gammapara - 1, 0)

    gamma = np.delete(gamma, np.where(~gamma.any(axis=1))[0], axis=0)
    return gamma


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


# ----------------------------------------------------------------------------------------------------------------------
# Variants (currently not in use)
# ----------------------------------------------------------------------------------------------------------------------

def press_2(a1, _y, _x, g1, g2):
    """
    Insert press function description later
    """
    # A is an array
    # G are Gamma matrices
    a1 = np.squeeze(a1)
    print(a1.shape)

    g = a1[0] * g1 + a1[1] * g2
    print(a1)
    n = np.shape(_y)[0]
    iH = np.identity(n) - _x @ \
         np.linalg.inv(np.transpose(_x) @ _x + np.transpose(g) @ g) @ \
         np.transpose(_x)
    B = np.diag(1 / np.diag(iH))
    BiHy = B @ iH @ _y
    return np.transpose(BiHy) @ BiHy / n


def getGammaOpt(y, x=None, gma1=None, gma2=None, start=None):
    """
    Parameters
    ----------
    y:  np vector - independent variable
    x:
    gma1:
    gma2:
    start

    Returns
    -------
     alpha : value
           scaling factor for gamma matrix
    """
    # bounds=bnds,
    # press1 = lambda a1, z1=y.values, z2=x, z3=gma1, z4=gma2: press_2(a1, z1, z2, z3, z4)
    res = op.minimize(press_2, args=(y.values, x, gma1, gma2), x0=start, method='powell')

    alpha = np.squeeze(res.x)

    print(f'alpha: {alpha}')

    return alpha[0] * gma1 + alpha[1] * gma2, alpha