
import numpy as np
import pandas as pd

from futures_flow.core.util import get_data_path
from futures_flow.db.zhawdb import ZhawDb

data_dir = get_data_path()
db_conn = ZhawDb()

fd = open(f'{data_dir}/sql_queries/commodity_prices.sql', 'r')
sql_query = fd.read()
fd.close()

df = pd.read_sql(sql_query,db_conn.engine)

prices = pd.pivot_table(df,index='px_date',columns='bb_tkr',values="qty")
prices = prices.sort_index(ascending=True)
prices = prices.dropna()
#%%

rets = np.log(prices/prices.shift(1))
rets = rets.dropna()



rets.to_csv(f'{data_dir}/clean/commodity_rets.csv')
