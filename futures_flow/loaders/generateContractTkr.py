import pandas as pd
from futures_flow.private.engines import *

# Futures Flow
# Dominik Boos, 2024-03-03
# generates missing Bloomberg tickers in fut_contract
# is usually integrated into generate contracts

start_year = 2015
end_year = 2100

engine = dbo_engine()
conn = engine.connect()

contracts = pd.read_sql_query(
    """SELECT con.contract_id, con.expiry_month, con.expiry_year, bb_root, bb_yellow_key FROM dbo.fut_contract con
    INNER JOIN dbo.fut_desc des ON des.series_id = con.series_id
    WHERE expiry_year>=""" + str(start_year) + " AND bb_permanent_tkr IS null AND bb_root IS NOT null"
    , engine)

print(contracts)

for idx, tab in contracts.iterrows():
    print(tab.bb_root)
    if len(tab.bb_root) == 1:
        bb_root = str(tab.bb_root) + " "
    else:
        bb_root = tab.bb_root

    bb_permanent_tkr = bb_root + tab.expiry_month + str(tab.expiry_year)[2:] + " " + tab.bb_yellow_key
    bb_actual_tkr = bb_root + tab.expiry_month + str(tab.expiry_year)[3:] + " " + tab.bb_yellow_key

    load_str = " UPDATE dbo.fut_contract SET bb_permanent_tkr = '" + bb_permanent_tkr + \
               "' , bb_actual_tkr = '" + bb_actual_tkr + "' WHERE contract_id = " + str(tab.contract_id)

    conn.execute(load_str)

    print(load_str)

    print(bb_permanent_tkr)

conn.close
