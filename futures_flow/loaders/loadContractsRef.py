import refinitiv.data as rd
import numpy as np
from pandas.io.sql import SQLTable

def _execute_insert(self, conn, keys, data_iter):
    print("Using monkey-patched _execute_insert")
    data = [dict(zip(keys, row)) for row in data_iter]
    conn.execute(self.table.insert().values(data))

SQLTable._execute_insert = _execute_insert

import pandas as pd
import sqlalchemy as sq

dic = {'Date': 'px_date',
       'TR.CLOSEPRICE': 'px_close',
       'TR.OPENPRICE': 'px_open',
       'TR.HIGHPRICE': 'px_high',
       'TR.LOWPRICE': 'px_low',
       'TR.ACCUMULATEDVOLUME': 'vol',
       'TR.OPENINTEREST': 'open_int'
       }

rd.open_session()

engine = sq.create_engine(
        "postgresql+psycopg2://bood:1l0v3futur3z@iwa.postgres.database.azure.com:5432/reprisk")

df = rd.get_history(universe="WK22", fields=["TR.CLOSEPRICE", "TR.OPENPRICE", "TR.HIGHPRICE", "TR.LOWPRICE",
                                             "TR.ACCUMULATEDVOLUME","TR.OPENINTEREST"],
                    interval="1D", start="2024-02-24", end="2024-02-29", use_field_names_in_headers=True)\
    .replace('',np.NaN).reset_index().rename(columns = dic) #.stack()

print(df.to_string)

tkr_list = pd.read_sql_query("""
                                                        """
                             , engine1
                             )

print(tkr_list)

for idx, isin in tkr_list.iterrows():

    print(isin[0])

    df = rd.get_history(universe=isin, fields=["TR.CO2EmissionTotal", "TR.CO2DirectScope1", "TR.CO2IndirectScope2",
                                                "TR.CO2IndirectScope3", "TR.TRESGEmissionsScore", "TR.EnvironmentPillarScore"],
                    interval="1Y", start="2000-01-01", end="2023-10-01", use_field_names_in_headers=True).replace('', np.NaN).stack().reset_index()

    df['isin'] = isin[0]
    df.set_axis(['sin_date', 'sin_tkr', 'sin_value', 'isin'], axis='columns', inplace=True)
    # df.rename(columns={'test': 'TEST'}, inplace=True)

    df.drop_duplicates(inplace=True)
    print(df.to_string())
    df.to_sql('sin_data', engine1, schema='capaloc', if_exists='append', index=False)

rd.close_session()
#print(df.to_string())