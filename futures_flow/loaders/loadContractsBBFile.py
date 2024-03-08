import pandas as pd
#import av
import numpy as np
import sqlalchemy as sq
import datetime
import glob
import os
from pandas.io.sql import SQLTable
from futures_flow.loaders.loaderFunctions import *
# import private stuff
from futures_flow.private.engines import *
from futures_flow.private.folders import data_folder

def _execute_insert(self, conn, keys, data_iter):
    print("Using monkey-patched _execute_insert")
    data = [dict(zip(keys, row)) for row in data_iter]
    conn.execute(self.table.insert().values(data))

SQLTable._execute_insert = _execute_insert

# engine and connection
engine = dbo_engine()
conn = engine.connect()

headers_contract = ['px_date', 'px_open', 'px_high', 'px_low', 'px_close', 'vol', 'open_int']
headers_desc = ['bb_permanent_tkr', 'first_trade', 'last_trade', 'last_delivery', 'first_delivery', 'first_notice']

archiveFolder = data_folder + '\\uploaded'
print(archiveFolder)
os.chdir(data_folder)

for file in glob.glob("*.XLSX"):
    print(file + " loading...")
    contract_desc = pd.read_excel(file, sheet_name='desc', usecols="C:H", skiprows=2, header=None,
                                  names=headers_desc, parse_dates=[1, 2, 3, 4, 5])
    contract_desc['bb_permanent_tkr'] = contract_desc['bb_permanent_tkr'].str.upper()
    print(contract_desc)
    loadFutDesc(contract_desc, 'bb_permanent_tkr', conn)

    sheets = pd.ExcelFile(file).sheet_names
    for sheet in sheets:
        if sheet != 'desc':
            contract_code = read_value_from_excel(file, sheet_name=sheet, column="B", row=4).upper()
            contract_id = pd.read_sql("SELECT contract_id FROM dbo.fut_contract WHERE bb_permanent_tkr = '"
                                + str(contract_code) + "'", engine).iloc[0, 0]
            print("loading " + contract_code + " ... .. .")
            fut_data = pd.read_excel(file, sheet_name=sheet, skiprows=6, names=headers_contract, header=None)
            loadedData = pd.read_sql("SELECT px_date FROM dbo.fut_data WHERE source=10 AND contract_id = '"
                                     + str(contract_id) + "'", con=engine, parse_dates=['px_date'])
            loadFutData(fut_data, loadedData, engine, contract_id=contract_id, source=10)

    os.rename(data_folder + '\\' + file, archiveFolder + "\\" + file)

print('all contract loaded')