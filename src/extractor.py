from src.db_connector import DbConnect
import os 
import pyodbc
<<<<<<< Updated upstream
from src.con_details import conn_str

=======

import pandas as pd
>>>>>>> Stashed changes
from dotenv import load_dotenv

from datetime import datetime 
# Load variables from .env
load_dotenv()

# Fetch variables
server = os.getenv('DB_SERVER')
database = os.getenv('DB_NAME')
driver = os.getenv('DB_DRIVER')

# Construct connection string
conn_str = (
    f"Driver={driver};"
    f"Server={server};"
    f"Database={database};"
    "Trusted_Connection=yes;"
    "Encrypt=no;"
)



class Extractor:
    def __init__(self,conn_str):
        try:
            self.connection_obj : DbConnect = DbConnect(con_str=conn_str)
        except:
            pass 
            
        


    def execute_query(self,query):
        try:
            connection_obj :DbConnect = DbConnect(con_str=self.conn_str)
            cols, result_from_query = connection_obj.execute(query = query)
            return cols, result_from_query
        except pyodbc.Error as e:
            raise ConnectionError(f'Error occured during connection as {e}') 

<<<<<<< Updated upstream
t = Extractor(conn_str=conn_str)
print(t.execute_query(query = 'SELECT 1 as t '))
        
=======

    def extract_tables(self,schema):
        ## Using the connection object:
        ## getting the tables inside the schema :
        table_names : tuple = self.connection_obj.execute(f"SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME like  '%{schema}%' ")
        all_table_names = table_names[1]

        ## all table_names contains tables like : [('Table1',), ('Table2',), ('Table3',)]
        ## Hence now we extract data for all the tables:

        for table_tuple in all_table_names:
            one_table = table_tuple[0]  ## as table_tuple is like this : ('Table1',) hence, table_tuple[0] will give us : table1
            ## Using the connection object now to extract that tables data:

            table_data   = self.connection_obj.execute(query = f"SELECT * FROM {schema}.{one_table}  ")
            all_rows = table_data[1]
            col_names= table_data[0]
            
            
            df = pd.DataFrame(all_rows, columns=col_names)
            file_path = f'E:\TUTS\PYTHON_THINGS\END_TO_END_PROJECTS\ETL_PYTHON\data\staging\{one_table}_{datetime.now().date()}.csv'
            df.to_csv(file_path) 

t = Extractor(conn_str=conn_str)
print(t.extract_tables(schema='SALES'))



>>>>>>> Stashed changes
