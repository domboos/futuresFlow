import pandas as pd
from futures_flow.private.engines import *
from futures_flow.loaders.loaderFunctions import *

# Futures Flow
# Dominik Boos, 2024-03-03
# generates new contracts in fut_contract

year = 2025
bb_year = str(year)[2:]
# create db engine
engine = dbo_engine()

contracts = pd.read_sql_query("SELECT series_id, expiry_months, bb_root, bb_yellow_key FROM dbo.fut_desc", engine)

for idx, tab in contracts.iterrows():
    for month in tab.expiry_months:
        try:
            if len(tab.bb_root) == 1:
                bb_root = str(tab.bb_root) + " "
            else:
                bb_root = tab.bb_root

            d = {'expiry_year': year, 'expiry_month': [month], 'series_id': [tab.series_id],
                 'bb_permanent_tkr': bb_root + month + str(year)[2:] + " " + tab.bb_yellow_key,
                 'bb_actual_tkr': bb_root + month + str(year)[3:] + " " + tab.bb_yellow_key}
            df = pd.DataFrame(data=d)
            df.to_sql('fut_contract', engine, schema='dbo', if_exists='append', index=False)
            print(df)
        except:
            print(tab.series_id)
            print('wait...')