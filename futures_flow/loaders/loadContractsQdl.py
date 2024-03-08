# load futures contract data from quandl

# import other libraries
import datetime
import quandl
import os
import mwh_functions as mwh

# speed up to_sql inserts heavily with low latency db connection (https://github.com/pandas-dev/pandas/issues/8953)
from pandas.io.sql import SQLTable

# speed up db
from pandas.io.sql import SQLTable

def _execute_insert(self, conn, keys, data_iter):
#    print("Using monkey-patched _execute_insert")
    data = [dict(zip(keys, row)) for row in data_iter]
    conn.execute(self.table.insert().values(data))

SQLTable._execute_insert = _execute_insert


import pandas as pd

# print('Quandl loader started...')

## parameters
hist_flag = True   ## if true also load expired contracts that have no data loaded so far from Quandl

# get a timestamp
now = datetime.datetime.now()

# create db engine
engine = mwh.mwh_engine()

# set quandl API key
quandl.ApiConfig.api_key = 'pkFmvgPV6zPmGxSEwpLz'


# read fut_desc table
my_path = os.path.abspath(os.path.dirname(__file__))
if hist_flag:
    sql_file_path = os.path.join(my_path, os.pardir, 'sql_repo', 'queries', 'qry_quandl_loader_hist.sql')
else:
    sql_file_path = os.path.join(my_path, os.pardir, 'sql_repo', 'queries', 'qry_quandl_loader.sql')
sql_file = open(sql_file_path, 'r').read()
fut_contract = pd.read_sql_query(sql_file, engine, index_col='contract_id')
# print(fut_contract)

no_data_contracts = []
fut_data_all = pd.DataFrame()

n_load = 0
n_data = 0

# call to quandl database and write to macrowarehouse
for c, row in fut_contract.iterrows():

    # initialize fut_data
    fut_data = None

    # load full contract dataset
    try:
        if pd.isnull(fut_contract['px_date'][c]):
            try:
                fut_data = quandl.get(fut_contract['quandl_tkr'][c])
            except:
                # print('Quandl error: No data available for quandl ticker: ' + fut_contract['quandl_tkr'][c])
                no_data_contracts.append(fut_contract['quandl_tkr'][c])
        else:
            start_load = str(fut_contract['px_date'][c])
            try:
                fut_data = quandl.get(fut_contract['quandl_tkr'][c], start_date=start_load)
                fut_data = fut_data.iloc[1:]
            except:
                # print('Quandl error: No more data loaded for quandl ticker: ' + fut_contract['quandl_tkr'][c])
                no_data_contracts.append(fut_contract['quandl_tkr'][c])

        if fut_data is not None:
            # rename index and columns
            fut_data.index.name = 'px_date'
            columns_list = []
            if 'Open Interest' in fut_data.columns:
                fut_data = fut_data.rename(columns={'Open Interest': 'open_int'})
                columns_list.append('open_int')
            elif 'Previous Day Open Interest' in fut_data.columns:
                fut_data = fut_data.rename(columns={'Previous Day Open Interest': 'prev_day_open_in'})
                columns_list.append('prev_day_open_in')

            if 'Volume' in fut_data.columns:
                fut_data = fut_data.rename(columns={'Volume': 'vol'})
                columns_list.append('vol')

            if 'Low' in fut_data.columns:
                fut_data = fut_data.rename(columns={'Low': 'px_low'})
                columns_list.append('px_low')

            if 'High' in fut_data.columns:
                fut_data = fut_data.rename(columns={'High': 'px_high'})
                columns_list.append('px_high')

            if 'Open' in fut_data.columns:
                fut_data = fut_data.rename(columns={'Open': 'px_open'})
                columns_list.append('px_open')

            if 'Settle' in fut_data.columns:
                fut_data = fut_data.rename(columns={'Settle': 'px_close'})
                columns_list.append('px_close')

            if len(columns_list) > 0:
                # only select needed columns
                fut_data = fut_data[columns_list]
                fut_data = fut_data.reset_index()

                # add contract_id from fut_contract
                fut_data['contract_id'] = c
                fut_data['source'] = 20

                # write data to fut_data table
                if fut_data.empty:
                    n_data = n_data + 1
                    # print('No more data to load for quandl ticker: ' + fut_contract['quandl_tkr'][c])
                else:
                    n_load = n_load + 1
                    fut_data_all = fut_data_all.append(fut_data, sort=False)
                    # print('Loaded data for quandl ticker: ' + fut_contract['quandl_tkr'][c])

    except:
        print('Error: Wrong data format for quandl ticker: ' + fut_contract['quandl_tkr'][c])

fut_data_all.to_sql('fut_data', engine, schema='dbo', if_exists='append', index=False)

conn = engine.connect()
conn.execute('REFRESH MATERIALIZED VIEW dbo.vw_fut_data')
conn.execute('REFRESH MATERIALIZED VIEW dbo.vw_fut_data_all')
conn.execute('REFRESH MATERIALIZED VIEW dbo.vw_recon2mul')
conn.execute("UPDATE dbo.dates SET dt = (SELECT MAX(px_date) FROM dbo.wkdays WHERE px_date < CURRENT_DATE) " +
             "WHERE dt_desc = 'quandl_loaded_upto'")
conn.execute('REFRESH MATERIALIZED VIEW dbo.vw_fut_data_last')

print('Quandl loader: Successfully loaded {0} tickers. No more data for {1} tickers. N/A: {2} tickers.'.format(n_load, n_data, len(no_data_contracts)))
