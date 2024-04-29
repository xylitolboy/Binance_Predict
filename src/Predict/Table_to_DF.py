import sqlite3
from sqlite3 import Error
import pandas as pd 

class preprocess_table:     # CCXT binance 거래소: 선물을 기본으로 함
    def __init__(self):
        self.db_path = "./binance_trading/src/Database/spot.db"
        self.db = sqlite3.connect(self.db_path)
        self.con = self.connection()
        self.table_names = []
        self.df_list = []
        self.database = {}
    def connection(self):
        try:
            con = sqlite3.connect(self.db_path)
            return con
        except Error:
            print(Error)

    def fetch_table_lists(self):
        cursor_db = self.con.cursor()
        cursor_db.execute('SELECT name FROM sqlite_master WHERE type="table"')
        table_lst = cursor_db.fetchall()
        table_names = [table[0] for table in table_lst]
        self.table_names = table_names

    def table_to_df(self):
        df_list = []
        cursor_db = self.con.cursor()
        for table_name in self.table_names:
            try:
                cursor_db.execute(f'SELECT * FROM {table_name}')
                data = cursor_db.fetchall()
                columns = [col[0] for col in cursor_db.description]
                df = pd.DataFrame(data, columns=columns)
                df_list.append(df)
            except:
                None
        self.df_list = df_list

    def execute(self):
        L = []
        temp = {}
        self.fetch_table_lists()
        self.table_to_df()
        for i in range(len(self.table_names)):
            self.database = temp[self.table_names[i]] = self.df_list[i]
tmp = preprocess_table()
tmp.execute()
print(tmp.database)
