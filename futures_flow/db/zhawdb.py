"""Connection to the ZHAW Internal Postgres DB"""
import sqlalchemy as sq
from dotenv import dotenv_values
from pandas.io.sql import SQLTable


class ZhawDb:
    """ connection to DB"""
    def __init__(self):
        env_var = dotenv_values()
        if not isinstance(env_var['DATABASE_URL'], str):
            raise KeyError('Connection to db could not be established,'
                           ' because DATABASE_URL is not defined')

        self.engine = self.create_engine(env_var['DATABASE_URL'])

    @staticmethod
    def create_engine(db_string: str):
        """ Create Engine with DB Connection String"""
        try:
            return sq.create_engine(db_string)
        except ConnectionError as error:
            print(f'Connection to db could not be established,'
                  f' because DATABASE_URL is not defined: {format(error)}')


# speed-up insert:
def _execute_insert(self, conn, keys, data_iter):
    print("Using monkey-patched _execute_insert")
    data = [dict(zip(keys, row)) for row in data_iter]
    conn.execute(self.table.insert().values(data))



SQLTable._execute_insert = _execute_insert
