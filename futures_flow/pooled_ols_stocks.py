"""Module calculating the pooled OLS of all Stocks which have a complete TimeSeries"""

from __future__ import annotations

import pandas as pd
import statsmodels.api as sm

from futures_flow.core.util import get_root_directory
from futures_flow.dataProcessing.data_util import get_specific_return_matrix
from futures_flow.visualization.linechart import linechart

if __name__ == '__main__':
    # INPUT
    START_DATE = '2002-12-31'
    END_DATE = '2003-01-02'
    # Info: Adjust run config, so that the working directory is: ~/futures_flow

    aggregatedRetMat: pd.DataFrame = get_specific_return_matrix(start_date=START_DATE,
                                                                end_date=END_DATE)
    cols = list(aggregatedRetMat.columns)
    # Estimate Regression
    x = sm.add_constant(aggregatedRetMat[cols[1:]])
    y = aggregatedRetMat[[cols[0]]].values
    model_fit = sm.OLS(y, x).fit()
    print(model_fit.summary())

    # Plot Betas:
    coef_df = pd.DataFrame(data=model_fit.params[1:], columns=['beta'])
    root_dir = get_root_directory()
    linechart(xaxis_values=coef_df.index,
              yaxis_values=coef_df.beta,
              title=f'Betas for Period: {START_DATE} - {END_DATE}', fontsize=16,
              save_figure_bool=True,
              save_figre_path=f'{root_dir}/Graphs/regularized_pooled_ols/'
                              f'Betas_Pooled_OLS_{START_DATE}_{END_DATE}')
