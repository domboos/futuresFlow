"""Estimates the pooled smoothed OLS"""
from __future__ import annotations

import numpy as np
import pandas as pd
import statsmodels.api as sm

from futures_flow.ridge.ridge_functions import get_gamma  # ,get_alpha_loocv
from futures_flow.models.model import Model


def calculate_pooled_ridge_ols(model: Model, aggregated_return_matrix: pd.DataFrame,
                               alpha: float):
    """estimate regularized Pooled OLS with parameters as defined in the Model Class

    Parameters
    ----------
    alpha: float value for scaling Gamma
    aggregated_return_matrix
    model: Model
        Complete Model which contains all the information which is necessary to estimate regularized OLS

    Returns
    -------
    df_fcast_all: pd.DataFrame()
        Dataframe with MultiIndex(CompanyIdentifier,Date) and the associated forecast

    beta_all: pd.DataFrame()
        DataFrame with MultiIndex(Date,lag) containing the regularized estimation of the betas'
    alpha_all: list
        contains all alphas which improve OLS estimation
    """

    # Initialize:
    df_fcast_all = pd.DataFrame()
    # beta_all = pd.DataFrame()
    # alpha_all = list()

    all_dates = list(aggregated_return_matrix.index.get_level_values(1).unique())
    insample_est_per = len(
        all_dates) - model.number_of_days_not_used_for_insample_estimation  # todo insample estimation Period take from data class Model: Replace insample_est_per with model.insample_estimation_period
    cols = list(aggregated_return_matrix.columns)
    x_cols: list = cols[1:model.maxlag + 1]
    y_cols: list = list([cols[0]])
    for idx2, day in enumerate(all_dates[: -(insample_est_per + model.fit_lag)]):
        # rolling window parameters:
        w_start = day
        w_end = all_dates[idx2 + insample_est_per]
        forecast_period = all_dates[idx2 +
                                    insample_est_per +
                                    model.fit_lag]
        print(f"start date: {w_start}")
        print(f"end_date: {w_end}")
        print(f"forecast_period: {forecast_period}")

        idx = pd.IndexSlice
        in_sample_df = aggregated_return_matrix.loc[idx[:, w_start:w_end], :]

        df_x = in_sample_df[x_cols]
        x_obs = sm.add_constant(df_x.values)
        y_obs = in_sample_df[y_cols].values

        gamma = get_gamma(model.maxlag + 1, model.gamma_type, model.gamma_para)
        gamma_tmp = gamma * alpha

        y_obs = np.concatenate((y_obs, np.zeros((gamma.shape[0], 1))))
        x_obs = np.concatenate((x_obs, gamma_tmp), axis=0)

        ##  fit the models
        model_fit1 = sm.OLS(y_obs, x_obs).fit()

        print(f"regularized in-sample r-squared: {model_fit1.rsquared} + Alpha: {alpha}")

        df_beta = pd.DataFrame(index=list(zip(*[np.repeat(forecast_period, 260), np.arange(1, model.maxlag + 1)])),
                               data=model_fit1.params[1:], columns=['betas'])

        #  Forecast per Company:
        idx = pd.IndexSlice
        temp = df_x.loc[idx[:, w_end], :]

        # index 0 is the actual return from t-1,t
        emp_vals = aggregated_return_matrix.loc[idx[:, forecast_period:forecast_period], 0]
        emp_vals = emp_vals.rename('emp_vals')
        fcast = temp.apply(lambda row: np.dot(row, df_beta.values)[0], axis=1)
        idex = pd.MultiIndex.from_tuples(
            tuples=list(zip(*[
                fcast.index.get_level_values(0),
                np.repeat(forecast_period, len(fcast.index.values))
            ])), names=["comp_id", "date"])


        fcast = pd.DataFrame(data=fcast.values,
                             index=idex, columns=['fcast'])
        fcast = fcast.join(emp_vals)
        print(fcast.head())
        print(fcast.shape)

        if df_fcast_all.empty:
            df_fcast_all = fcast
        else:
            df_fcast_all = pd.concat([df_fcast_all,fcast],axis=0)

        return df_beta, df_fcast_all
