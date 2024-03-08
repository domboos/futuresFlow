import pandas as pd
import statsmodels.api as sm


def calc_mincer_zarnowitz_statistics(df: pd.DataFrame) -> dict:
    """
    Parameters
    ----------
    df: pd.DataFrame with 'comp_id' and 'date' in MultiIndex, fcast and empirical values as columns
        column names are 'fcast' for the forecast and 'emp_vals' for the empirical values

    -------
    """

    if 'fcast' and 'emp_vals' in df.columns:
        residuals = df['fcast'].values - df['emp_vals'].values

    x = sm.add_constant(df['fcast'])

    model_mz = sm.OLS(residuals, x).fit()
    model_base = sm.OLS(df['emp_vals'].values, x).fit()
    print(model_base.summary())

    return {'rsquared': model_base.rsquared,
            'intercept': model_base.params[0],
            'tstat_intercept': model_base.bse[0] / model_base.HC3_se[0] * model_base.tvalues[0],
            'pval_intercept': model_base.pvalues[0],
            'beta': model_base.params[1],
            'tstat_beta': model_mz.bse[1] / model_mz.HC3_se[1] * model_mz.tvalues[1],
            'pval_beta': model_mz.pvalues[1],
            'nobs': model_mz.nobs,
            'mz_model': model_mz,
            'base_model': model_base
            }
