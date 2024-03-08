import pandas as pd
import sqlalchemy as sq


def loadFutData(futData, loadedData, engine, source=None, contract_id=None):

    if source is not None:
        futData['source'] = source

    if contract_id is not None:
        futData['contract_id'] = contract_id

    if not loadedData.empty:
        futData['px_date'] = pd.to_datetime(futData['px_date'])
        loadedData['px_date'] = pd.to_datetime(loadedData['px_date'])
        futData = pd.merge(right=loadedData, left=futData, on=['px_date'], how='left', indicator=True)
        futData = futData.loc[futData._merge.isin(['left_only']), ['px_date', 'px_open',  'px_high',  'px_low',
                                'px_close', 'vol',  'open_int',  'source',  'contract_id']]

    futData.to_sql('fut_data', engine, schema='dbo', if_exists='append', index=False)


def loadFutDesc(contracts, identifier, conn):

    for index, contract in contracts.iterrows():
        qry = sq.text("UPDATE dbo.fut_contract SET last_trade = '" + str(contract.last_trade.date()) +
                      "', first_delivery = '" + str(contract.first_delivery.date()) +
                      "', last_delivery = '" + str(contract.last_delivery.date()) +
                      "', first_trade = '" + str(contract.first_trade.date()) +
                      "', first_notice = '" + str(contract.first_notice.date()) +
                      "' WHERE " + identifier + "='" + str(contract[identifier]) + "'")
        print(qry)
        conn.execute(qry)


def read_value_from_excel(filename, sheet_name, column="C", row=3):
    """Read a single cell value from an Excel file"""
    return pd.read_excel(filename, sheet_name=sheet_name, skiprows=row - 1, usecols=column, nrows=1, header=None,
                         names=["Value"]).iloc[0]["Value"]

