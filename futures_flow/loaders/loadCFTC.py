from sodapy import Socrata
import numpy as np
import datetime
import os
from futures_flow.loaders.loaderFunctions import *
# import private stuff
from futures_flow.private.engines import *

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
engine = dbo_engine()
conn = engine.connect()

cit_drop = ['change_open_interest_all', 'change_noncomm_long_all_nocit', 'Change_NonComm_Short_All_NoCIT',
            'Change_NonComm_Spead_All_NoCIT', 'change_comm_long_all_nocit', 'change_comm_short_all_nocit',
            'change_tot_rept_long_all', 'change_tot_rept_short_all', 'change_nonrept_long_all',
            'change_nonrept_short_all', 'change_cit_long_all', 'change_cit_short_all', 'pct_open_interest_all',
            'pct_oi_noncomm_long_all_nocit', 'Pct_OI_NonComm_Short_All_NoCIT', 'Pct_OI_NonComm_Spread_All_NoCIT',
            'pct_oi_comm_long_all_nocit', 'pct_oi_comm_short_all_nocit', 'Pct_OI_Tot_Rept_Long_All_NoCIT',
            'Pct_OI_Tot_Rept_Short_All_NoCIT', 'pct_oi_nonrept_long_all_nocit', 'Pct_OI_NonRept_Short_All_NoCIT',
            'pct_oi_cit_long_all', 'pct_oi_cit_short_all']

cit_rename = {'id': 'cit_id', 'report_date_as_yyyy_mm_dd':'report_date'}


client = Socrata("publicreporting.cftc.gov",None)
#cit_data = client.get("yjak-hhbj", select='traders_tot_all', where="id like '08%%%%%%%%%%' ")
cit_data = client.get("jun7-fc8e", select='report_date_as_yyyy_mm_dd, open_interest_all',
                      where="cftc_contract_market_code='001601' AND report_date_as_yyyy_mm_dd<'2005-12-31'",
                      order="report_date_as_yyyy_mm_dd")

##
cit_data_df = pd.DataFrame.from_records(cit_data)
print(cit_data_df)

sdfas




cit_data_df = pd.DataFrame.from_records(cit_data).rename(columns=cit_rename).drop(columns=cit_drop)
cit_data_df.columns = map(str.lower, cit_data_df.columns)
cit_data_df['cit_id'] = cit_data_df['cit_id'].astype(np.int64)
print(cit_data_df)
loadedData = pd.read_sql("SELECT cit_id FROM dbo.cftc_cit", con=engine)
cit_data_df = pd.merge(right=loadedData, left=cit_data_df, on=['cit_id'], how='left', indicator=True)
cit_data_df = cit_data_df.loc[cit_data_df._merge.isin(['left_only']), :]
cit_data_df2 = cit_data_df.drop(columns='_merge')
cit_data_df2.to_sql('cftc_cit', engine, schema='dbo', if_exists='append', index=False)

## disagg
results = client.get("72hh-3qpy", limit=2000)
