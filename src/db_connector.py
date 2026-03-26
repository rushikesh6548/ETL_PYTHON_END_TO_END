import os
import pyodbc

from sqlalchemy import create_engine, event
import urllib
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


class DbConnect:
    def __init__(self,con_str):
        self.con_str = con_str


    def execute(self,query):
        try:
            conn = pyodbc.connect(self.con_str)
            cursor = conn.cursor()
        except pyodbc.Error as e:
            raise ConnectionError(e)
        else:
            res = cursor.execute(query).fetchall()
            columns = [column[0] for column in cursor.description]
            rows = [tuple(row) for row in res]
            conn.close()
            return columns,rows 
        
    def dbwrite(self,df,table_name,if_exists = 'append'):
            try:

                params = urllib.parse.quote_plus(self.con_str)
                engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

                # 2. Performance Hack: Enable fast_executemany
                @event.listens_for(engine, "before_cursor_execute")
                def receive_before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
                    if executemany:
                        cursor.fast_executemany = True

                # 3. Write the DataFrame
                df.to_sql(
                    name=table_name, 
                    con=engine, 
                    index=False, 
                    if_exists=if_exists
                )
                return (f"Successfully wrote to {table_name}")
            
            
            
            except Exception as e:
        # Here you can log the error 'e' if needed
                print(f"Database Write Error: {e}")
                return 0 # Or raise e if you want the calling code to handle it
        

# t = DbConnect(con_str=conn_str)
# print(t.execute("SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS where table_name like '%SALES%'"))