import mwh_functions as mwh
import quandl
import os

from pandas.io.sql import SQLTable

def _execute_insert(self, conn, keys, data_iter):
    print("Using monkey-patched _execute_insert")
    data = [dict(zip(keys, row)) for row in data_iter]
    conn.execute(self.table.insert().values(data))

SQLTable._execute_insert = _execute_insert

import pandas as pd

# set quandl API key & reate db engine and connection
quandl.ApiConfig.api_key = 'pkFmvgPV6zPmGxSEwpLz'
engine = mwh.mwh_engine()

# get actual data from db
my_path = os.path.abspath(os.path.dirname(__file__))
sql_file_path = os.path.join(my_path, os.pardir, 'sql_repo', 'queries', 'qry_qdl_cftc_start_dt.sql')
quandl_roots_sql = open(sql_file_path, 'r').read()
quandl_roots = pd.read_sql_query(quandl_roots_sql, engine)
name_match = pd.read_sql_query("SELECT category, quandl_name FROM dbo.cot_category where quandl_name is not null", engine)
desc = pd.read_sql_query("SELECT * FROM dbo.cot_desc", engine)

for idx, code in quandl_roots.iterrows():
    fo_data = quandl.get('CFTC/' + code.qdl_cftc_code + '_FO_ALL', start_date=code.start_dt).stack().reset_index()\
        .replace(name_match.quandl_name.tolist(),name_match.category.tolist())
    print(fo_data)
    f_data = quandl.get('CFTC/' + code.qdl_cftc_code + '_F_ALL', start_date=code.start_dt).stack().reset_index()\
        .replace(name_match.quandl_name.tolist(),name_match.category.tolist())
    print(f_data)
    fo_data.columns = ['px_date', 'category', 'quantity']
    f_data.columns = ['px_date', 'category', 'quantity']
    fo_data['series_id'] = code.series_id
    f_data['series_id'] = code.series_id
    fo_data['futonly'] = False
    f_data['futonly'] = True
    print(pd.merge(right=desc , left=fo_data, on=['series_id', 'category', 'futonly'], how='left')\
                .loc[:, ['category', 'cot_tkr', 'px_date', 'quantity']].to_string())
    fo_data = pd.merge(right=desc , left=fo_data, on=['series_id','category', 'futonly'], how='left')\
                .loc[:, ['cot_tkr', 'px_date', 'quantity']]\
                .to_sql('cot_data', engine, schema='dbo', if_exists='append', index=False)
    f_data = pd.merge(right=desc, left=f_data, on=['series_id', 'category', 'futonly'], how='left') \
                .loc[:, ['cot_tkr', 'px_date', 'quantity']] \
                .to_sql('cot_data', engine, schema='dbo', if_exists='append', index=False)
    print('----- ' + str(code.series_name) + ' loaded -----')