from __future__ import annotations

import pickle
from datetime import datetime

import pandas as pd

from futures_flow.core.util import get_root_directory


def intersection(lst1, lst2) -> list:
    return [value for value in lst1 if value in lst2]


def get_complete_stock_prices(numb_obs: int | None = None,
                              file_name: str = 'stock_prices.csv') -> pd.DataFrame:
    """Returns a dataframe with all companies as columns with the prices and the prices
        in the sample are only the companies with a complete Series History.

    Parameters
    ----------
    numb_obs: int | None
    if None then we only take the companies into account which have a complete Series.
    file_name: by Default stock_prices.csv

    Returns
    -------
    prices: pd.DataFrame
    complete list with all companies and the corresponding Prices
    """

    root_dir = get_root_directory()
    data_dir = root_dir + '/data/refinitiv/'
    prices_raw = pd.read_csv(data_dir + file_name, sep=',')
    prices_raw = prices_raw.rename({'Unnamed: 0': 'date'}, axis=1)
    prices_raw.date = pd.to_datetime(prices_raw.date)

    summary_df = prices_raw.groupby('date').count().sum().to_frame('numb_obs')
    if numb_obs is not None:
        relevant_companies = summary_df[summary_df.numb_obs > numb_obs]
    else:
        relevant_companies = summary_df[summary_df.numb_obs == max(summary_df.numb_obs)]

    _intersection = intersection(prices_raw.columns.to_list(), list(relevant_companies.index.values))
    _intersection.append('date')
    prices = prices_raw[_intersection]
    prices = prices.set_index('date')
    prices = prices.dropna(how='all')
    return prices


def get_specific_return_matrix(return_type: str,
                               start_date: None | str,
                               end_date: None | str,
                               company_identifiers: list | None = None,

                               ) -> pd.DataFrame:
    """
    Parameters
    ----------
    return_type: either 'ordinary' or 'demeaned'
    start_date: str
    end_date
    company_identifiers: list of Refinitiv Tickers

    Returns
    -------

    """
    global result_dict

    if return_type == 'ordinary':
        result_dict = read_serialized_dict('returnMatrixOrdinaryReturns.pickle')
    if return_type == 'demeaned':
        result_dict = read_serialized_dict('demeanedReturnMatrixOrdinaryReturns.pickle')

    if not result_dict or return_type not in ['ordinary', 'demeaned']:
        raise KeyError(f'return_type: {return_type} does not exist or dict is empty')

    result_df = pd.concat(result_dict, axis=0)
    result_df.index.names = ['comp_id', 'date']

    if (start_date is not None) and (end_date is not None):
        start_date_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')

        idx = pd.IndexSlice
        result_df = result_df.loc[idx[:, start_date_dt:end_date_dt], :]

    if company_identifiers is not None:
        result_df = result_df.loc[company_identifiers]

    return result_df


def serialize_dict(file_name: str, result_dict: dict):
    """Serializes Results and writes them into the data/clean Directory"""
    root_dir = get_root_directory()
    with open(f'{root_dir}/data/clean/{file_name}.pickle', 'wb') as handle:
        pickle.dump(result_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)
    handle.close()
    print('serialized Dictionary to:')
    print(f'path: {root_dir}/data/clean/')
    print(f'filename: {file_name}')


def read_serialized_dict(file_name) -> dict:
    root_dir = get_root_directory()
    with open(f'{root_dir}/data/clean/{file_name}', 'rb') as handle:
        result = pickle.load(handle)
    return result
