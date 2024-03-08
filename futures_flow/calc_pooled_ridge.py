from core.pooled_ridge_ols import calculate_pooled_ridge_ols
from futures_flow.analytics.mz_regression import calc_mincer_zarnowitz_statistics
from futures_flow.dataProcessing.data_util import get_specific_return_matrix
from futures_flow.models.model import Model
from futures_flow.visualization.linechart import linechart


# FIXME companies_to_iterate and dates_to_iterate

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
    alpha_grid = [15000,20000,25000,30000]

    all_dates = list(
        get_specific_return_matrix(start_date=None, end_date=None,
                                   return_type=return_type).index.get_level_values(1).unique())

    time_step = int((len(all_dates) - 1) / 2)
    print(time_step)
    dates_to_iterate = {'set1': {'start_date': all_dates[0].strftime("%Y-%m-%d"),
                                 'end_date': all_dates[time_step].strftime("%Y-%m-%d")},
                        'set2': {'start_date': all_dates[time_step + 1].strftime("%Y-%m-%d"),
                                 'end_date': all_dates[-1].strftime("%Y-%m-%d"), }
                        }

    for key, value in dates_to_iterate.items():
        print(f'{key}: {dates_to_iterate[key]}')

    companies = list(
        get_specific_return_matrix(start_date=None, end_date=None,
                                   return_type=return_type).index.get_level_values(0).unique())
    print(f'number of companies with complete history: {len(companies)}')

    step = int((len(companies) - 1) / 2)
    print(f'Number of Companies in subsample: {step}')
    companies_to_iterate = {'all': companies,
                            'set1': companies[step:],
                            'set2': companies[:step]
                            }


    del companies, all_dates

    for _set, l in companies_to_iterate.items():
        for date_key, v in dates_to_iterate.items():

            if _set == 'set2':
                print('Done like Donzo!!')
                exit()
            model_x = Model(
                maxlag=260,
                gamma_type='sqrt',
                regularization='d1',
                gamma_para=1.0,
                number_of_days_not_used_for_insample_estimation=2,
                fit_lag=1,
                start_date=dates_to_iterate[date_key]['start_date'],
                end_date=dates_to_iterate[date_key]['end_date'])

            agg_ret_mat = get_specific_return_matrix(
                return_type=return_type,
                start_date=model_x.start_date,
                end_date=model_x.end_date,
                company_identifiers=companies_to_iterate[_set],

            )
            print(f'shape of agg_ret_mat: {agg_ret_mat.shape}')

            if model_x.maxlag != 260:
                agg_ret_mat = agg_ret_mat.loc[:, :model_x.maxlag + 1]

            for alpha in alpha_grid:
                betas,fcast = calculate_pooled_ridge_ols(model=model_x, aggregated_return_matrix=agg_ret_mat, alpha=alpha)

                title = f'{model_x.start_date}-{model_x.end_date}-{_set}-{return_type}-Aplha:{alpha}'
                linechart(range(0, len(betas.index), 1), betas.betas, title=title, fontsize=10, save_figure_bool=True,
                          save_figre_path=f'{title}.png')

                print(calc_mincer_zarnowitz_statistics(fcast))
                return betas,fcast



if __name__ == '__main__':
    # main(return_type='ordinary')
    betas,fcast = main(return_type='demeaned')
    fcast.to_csv('fcast_all_first_period.csv')
    print('Done like Donzo')
