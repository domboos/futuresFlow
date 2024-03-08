import pandas as pd

from futures_flow.dataProcessing.data_util import get_specific_return_matrix


def test_function_getSpecificRetMatrixFromPickle_without_start_end_date():
    dir_to_file = '/data/ignore_large_files/returnMatrixOrdinaryReturns.pickle'
    res = get_specific_return_matrix(start_date=None, end_date=None)
    assert (isinstance(res, pd.DataFrame))
    assert (isinstance(res.index, pd.MultiIndex))
    assert (isinstance(res.index.get_level_values(0), pd.Index))
    assert (isinstance(res.index.get_level_values(1), pd.DatetimeIndex))


def test_function_getSpecificRetMatrixFromPickle_with_start_end_date():
    dir_to_file = '/data/ignore_large_files/returnMatrixOrdinaryReturns.pickle'
    start_date = '2003-01-01'
    end_date = '2003-01-02'
    res = get_specific_return_matrix(start_date=start_date, end_date=end_date)
    assert (isinstance(res, pd.DataFrame))
    assert (isinstance(res.index, pd.MultiIndex))
    assert (isinstance(res.index.get_level_values(0), pd.Index))
    assert (isinstance(res.index.get_level_values(1), pd.DatetimeIndex))


def test_get_date_specific_ordinary_return_matrix_input_company_identifiers():
    res = get_specific_return_matrix(start_date=None,
                                     end_date=None,
                                     company_identifiers=['ADSK.OQ', 'TJX.N', 'AZO.N'])

    assert len(res.index.get_level_values(0).unique()) == 3
