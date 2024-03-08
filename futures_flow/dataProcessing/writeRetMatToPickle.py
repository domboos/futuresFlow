import pandas as pd

from futures_flow.core.calculatereturns import calculate_returns_single_asset, create_ret_mat
from futures_flow.dataProcessing.data_util import get_complete_stock_prices, serialize_dict, read_serialized_dict


def serialize_return_matrices(return_type: str):
    """
    calculates return_type specific returns  N x 260 return Matrix for each company
    and writes results to serialized dictionaries

    Parameters:
    -----------
    return_type either [ordinary, demeaned]

    """
    global avg_rets

    maxlag = 260
    ordinary_rets_all: dict = read_serialized_dict(file_name='ordinaryReturnSeries.pickle')

    result = {}
    if return_type == 'demeaned':
        df_rets = pd.concat(ordinary_rets_all, axis=0)

        df_rets.index.names = ['comp_id', 'date']
        avg_rets: pd.DataFrame = df_rets.groupby('date').mean()

    for key, value in ordinary_rets_all.items():

        ret_series: pd.DataFrame = ordinary_rets_all[key].dropna()

        if return_type == 'demeaned' and avg_rets.shape == ret_series.shape:
            print('calculating demeaned Returns')
            ret_series = ret_series - avg_rets

        ret_mat = create_ret_mat(ret_series.values, maxlag=260)
        ret_mat_df = pd.DataFrame(index=ret_series.index[maxlag:], data=ret_mat)
        result[key] = ret_mat_df
        print('working')

    if return_type == 'ordinary':
        serialize_dict(result_dict=result, file_name='returnMatrixOrdinaryReturns.pickle')

    elif return_type == 'demeaned':
        serialize_dict(result_dict=result, file_name='demeanedReturnMatrixOrdinaryReturns.pickle')
    else:
        raise KeyError(f'return_type: {return_type} is not specified')


def serialize_return_series():

    df_prices = get_complete_stock_prices()

    companies = df_prices.columns.to_list()
    result = {}

    for counter, company in enumerate(companies):
        rets = calculate_returns_single_asset(df_prices[[company]]).dropna()
        result[company] = rets
        print('working')
    print('writing')

    serialize_dict(result_dict=result, file_name='ordinaryReturnSeries.pickle')


if __name__ == '__main__':
    serialize_return_matrices(return_type='demeaned')
    # serialize_return_series()

# todo:
# writefromPickle , complete and with time Stamps and return ndArray.
