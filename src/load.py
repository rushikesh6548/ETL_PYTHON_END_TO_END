from datetime import datetime, timedelta
import pandas as pd
from src.con_details import conn_str
from src.db_connector import DbConnect


class Load:
    def __init__(self, file_path, file_name, con_str):
        self.file_path = file_path
        self.file_name = file_name
        self.con_str = con_str
        self.comp_file_path = f'{self.file_path}\{self.file_name}.csv'

        try:
            self.just_read = pd.read_csv(self.comp_file_path)
            self.just_read['Load_Date'] = datetime.now().date()
        except FileNotFoundError as e:
            raise FileNotFoundError(e)

    def load_to_db(self, table_name, if_exists='append'):
        obj = DbConnect(con_str=self.con_str)
        obj.dbwrite(table_name=table_name, df=self.just_read, if_exists=if_exists)


## running it 
# L = Load(
#     file_path='data\\processed',
#     file_name='Sales_Dimension_2026-03-23',
#     con_str=conn_str        # ← don't forget to pass this!
# )
# L.load_to_db(table_name='Sales_Dimension')
