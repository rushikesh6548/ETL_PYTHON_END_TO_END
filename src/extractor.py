from .db_connector import DbConnect
import os 
import pyodbc
from src.con_details import conn_str

from dotenv import load_dotenv


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
        self.conn_str = conn_str
        


    def execute_query(self,query):
        try:
            connection_obj :DbConnect = DbConnect(con_str=self.conn_str)
            cols, result_from_query = connection_obj.execute(query = query)
            return cols, result_from_query
        except pyodbc.Error as e:
            raise ConnectionError(f'Error occured during connection as {e}') 

t = Extractor(conn_str=conn_str)
print(t.execute_query(query = 'SELECT 1 as t '))
        