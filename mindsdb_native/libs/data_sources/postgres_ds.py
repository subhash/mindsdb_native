import os

import pandas as pd
import pg8000

from mindsdb_native.libs.data_types.data_source import DataSource


class PostgresDS(DataSource):

    def _setup(self, table=None, query=None, where=None, limit=None,
                database='postgres', host='localhost',
                port=5432, user='postgres', password=''):

        self._database_name = database
        self._table_name = table
        self._host_name = host
        self._port = port
        self._user_name = user
        self._password = password

        if query is None:
            query = f'SELECT * FROM {table}'

        if where is not None:
            def make_cond(w):
                (op1, op, op2) = w
                if isinstance(op2, str): op2 = f'\'{op2}\'' 
                return f' {op1} {op} {op2}'
            clauses = map(make_cond, where)
            query = f'{query} WHERE {" AND ".join(clauses)}'

        if limit is not None:
            query = f'{query} LIMIT {limit}'

        con = pg8000.connect(database=database, user=user, password=password,
                             host=host, port=port)
        df = pd.read_sql(query, con=con)
        con.close()

        df.columns = [x.decode('utf-8') for x in df.columns]
        for col_name in df.columns:
            try:
                df[col_name] = df[col_name].apply(lambda x: x.decode("utf-8"))
            except:
                pass

        col_map = {}
        for col in df.columns:
            col_map[col] = col

        return df, col_map

    def filter(self, where, limit=None):
        return PostgresDS(where=where, limit=limit,
            table=self._table_name, database=self._database_name, 
            host=self._host_name, port=self._port, user=self._user_name, password=self._password)


    def name(self):
        return '{}: {}/{}'.format(
            self.__class__.__name__,
            self._database_name,
            self._table_name
        )