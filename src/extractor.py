
import os 
import pyodbc

import pandas as pd
from dotenv import load_dotenv

from datetime import datetime 
# Load variables from .env
load_dotenv()

import sys
from pathlib import Path

# Add parent directory to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logger import get_logger
from src.db_connector import DbConnect

logger = get_logger()
PROJECT_ROOT = Path(__file__).resolve().parent.parent
STAGING_DIR = PROJECT_ROOT / "data" / "staging"

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
            logger.info('Trying to establish DB Connection!')
        except pyodbc.Error as e :
            logger.critical(f"Cant Connect to db due to {e}") 
            
        


    def execute_query(self,query):
        try:
            connection_obj :DbConnect = DbConnect(con_str=self.conn_str)
            cols, result_from_query = connection_obj.execute(query = query)
            return cols, result_from_query
        except pyodbc.Error as e:
            raise ConnectionError(f'Error occured during connection as {e}') 


    def extract_tables(self,schema):
        ## Using the connection object:
        ## getting the tables inside the schema :
        STAGING_DIR.mkdir(parents=True, exist_ok=True)
        table_names : tuple = self.connection_obj.execute(f"""SELECT DISTINCT 
                                                          TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME like  '%{schema}%' 
                                                          and table_name not like '%dimension%' 
                                                          """)
        all_table_names = table_names[1]

        logger.info(f'Fetched {len(all_table_names)} Unique tables to be extracted!')

        ## all table_names contains tables like : [('Table1',), ('Table2',), ('Table3',)]
        ## Hence now we extract data for all the tables:

        for table_tuple in all_table_names:
            one_table = table_tuple[0]  ## as table_tuple is like this : ('Table1',) hence, table_tuple[0] will give us : table1
            ## Using the connection object now to extract that tables data:

            table_data   = self.connection_obj.execute(query = f"SELECT * FROM {schema}.{one_table}  ")
            all_rows = table_data[1]
            col_names= table_data[0]
            
            
            df = pd.DataFrame(all_rows, columns=col_names)
            file_path = STAGING_DIR / f"{one_table}_{datetime.now().date()}.csv"
            df.to_csv(file_path) 
            logger.info(f'Currently Saving Table {one_table} to CSV!')




