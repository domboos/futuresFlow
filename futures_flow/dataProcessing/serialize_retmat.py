import pandas as pd

from futures_flow.core.calculatereturns import calculate_returns_single_asset, create_ret_mat
from futures_flow.core.util import get_data_path
from futures_flow.dataProcessing.data_util import get_complete_stock_prices, serialize_dict, read_serialized_dict


def serialize_return_matrices_from_data_frame(df:pd.DataFrame):
    """

    Parameters
    ----------
    df: pd.DataFrame, Index = descending, columns = Instruments, values = rets

    Returns
    -------
    pickle file with Return Matrix for each column
    """
    maxlag = 260
    res = {}
    for instrument in df.columns:
        ret_mat = create_ret_mat(df[[instrument]].values, maxlag=260)
        ret_mat_df = pd.DataFrame(index=df.index[maxlag:], data=ret_mat)
        res[instrument] = ret_mat_df

    serialize_dict(result_dict=res, file_name='returnMatrixCommodityOrdinaryReturns')




def serialize_return_matrices(return_type: str):
    """
    calculates return_type specific returns  N x 260 return Matrix for each company
    and writes results to serialized dictionaries

    Parameters:
    -----------
    return_type either [ordinary, demeaned]

    """


    maxlag = 260
    ordinary_rets_all: dict = read_serialized_dict(file_name='ordinaryReturnSeries.pickle')

    result = {}
    if return_type == 'demeaned':
        df_rets = pd.concat(ordinary_rets_all, axis=0)

        df_rets.index.names = ['comp_id', 'date']
        avg_rets: pd.DataFrame = df_rets.groupby('date').mean()

    for key, value in ordinary_rets_all.items():

        ret_series: pd.DataFrame = ordinary_rets_all[key].dropna()

        if return_type == 'demeaned' and 'avg_rets' in locals():
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
    #serialize_return_matrices(return_type='demeaned')
    # serialize_return_series()
    data_dir = get_data_path()
    com_rets = pd.read_csv(f'{data_dir}/clean/commodity_rets.csv',index_col=0)
    serialize_return_matrices_from_data_frame(com_rets)