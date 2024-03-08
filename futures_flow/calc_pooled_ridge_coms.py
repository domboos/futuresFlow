import numpy as np
import pandas as pd

from core.pooled_ridge_ols import calculate_pooled_ridge_ols
from futures_flow.analytics.mz_regression import calc_mincer_zarnowitz_statistics
from futures_flow.dataProcessing.data_util import read_serialized_dict
from futures_flow.models.model import Model
from futures_flow.visualization.linechart import linechart


def get_transformed(df, startdate, enddate, comKey):
    df = df[df.index < enddate]
    df = df[df.index > startdate]

    idex = pd.MultiIndex.from_tuples(
        tuples=list(zip(*[
            np.repeat(comKey, len(df.index)),
            df.index
        ])), names=["comp_id", "date"])

    return pd.DataFrame(index=idex, data=df.values, columns=df.columns)


def main(return_type: str):
    """
    Findings so far; Optimization does not work if too many observations;
    if the shape of the Matrix is > 15000 x 261 Optimization takes way too long,
    best works if shape of ret_mat is ~ 5000 x 261.
    Parameters
    ----------
    return_type: str = 'ordinary' | 'demeaned'

    Returns
    -------

    """
    alpha_grid = [50000]

    retMats = read_serialized_dict('returnMatrixCommodityOrdinaryReturns.pickle')

    all_dates = retMats['BO'].index

    time_step = int((len(all_dates) - 1) / 3)
    print(time_step)
    dates_to_iterate = {'set1': {'start_date': all_dates[0],
                                 'end_date': all_dates[time_step]},
                        'set2': {'start_date': all_dates[time_step + 1],
                                 'end_date': all_dates[2 * time_step]},
                        'set3': {'start_date': all_dates[2 * time_step],
                                 'end_date': all_dates[-1]},
                        }

    for key, value in dates_to_iterate.items():
        print(f'{key}: {dates_to_iterate[key]}')

    for date_key, v in dates_to_iterate.items():
        agg_ret_mat = pd.DataFrame()
        for comKey, value in retMats.items():

            model_x = Model(
                maxlag=260,
                gamma_type='sqrt',
                regularization='d1',
                gamma_para=1.0,
                number_of_days_not_used_for_insample_estimation=2,
                fit_lag=1,
                start_date=dates_to_iterate[date_key]['start_date'],
                end_date=dates_to_iterate[date_key]['end_date'])

            if agg_ret_mat.empty:
                agg_ret_mat = get_transformed(retMats[comKey],
                                              startdate=dates_to_iterate[date_key]['start_date'],
                                              enddate=dates_to_iterate[date_key]['end_date'],
                                              comKey=comKey)
            else:
                df_temp = get_transformed(retMats[comKey],
                                          startdate=dates_to_iterate[date_key]['start_date'],
                                          enddate=dates_to_iterate[date_key]['end_date'],
                                          comKey=comKey)
                agg_ret_mat = pd.concat([agg_ret_mat, df_temp], axis=0)

            if model_x.maxlag != 260:
                agg_ret_mat = agg_ret_mat.loc[:, :model_x.maxlag + 1]

        print(f'shape of agg_ret_mat: {agg_ret_mat.shape}')

        for alpha in alpha_grid:
            betas, fcast = calculate_pooled_ridge_ols(model=model_x, aggregated_return_matrix=agg_ret_mat, alpha=alpha)

            title = f'Coms-all-{model_x.start_date}-{model_x.end_date}-{return_type}-Aplha:{alpha}'
            linechart(range(0, len(betas.index), 1), betas.betas, title=title, fontsize=10, save_figure_bool=True,
                      save_figre_path=f'{title}.png')

            print(calc_mincer_zarnowitz_statistics(fcast))
            # return betas, fcast


if __name__ == '__main__':
    # main(return_type='ordinary')
    betas, fcast = main(return_type='ordinary')
    fcast.to_csv('fcast_comm_all_first_period.csv')
    print('Done like Donzo')
