"""Data Models for estimation of Ridge Regression"""
from __future__ import annotations

import datetime
from dataclasses import dataclass


@dataclass
class Model:
    """Parameters which are used to estimate the pooled regularized OLS"""
    def __init__(self, maxlag: int,
                 gamma_type: str,
                 regularization: str,
                 gamma_para: str,
                 number_of_days_not_used_for_insample_estimation: int,
                 fit_lag: int,
                 start_date: datetime.datetime,
                 end_date: datetime.datetime):
        self.maxlag = maxlag
        self.gamma_type = gamma_type
        self.regularization = regularization
        self.gamma_para = gamma_para
        self.number_of_days_not_used_for_insample_estimation = number_of_days_not_used_for_insample_estimation
        self.fit_lag = fit_lag
        self.start_date = start_date
        self.end_date = end_date

    def get_Model_as_Dict(self) -> dict:
        return {
            'maxlag': self.maxlag,
            'gamma_type': self.gamma_type,
            'regularization': self.regularization,
            'gamma_para': self.gamma_para,
            'number_of_days_not_used_for_insample_estimation': self.number_of_days_not_used_for_insample_estimation,
            'fit_lag': self.fit_lag,
            'start_date': self.start_date,
            'end_date': self.end_date,
        }
