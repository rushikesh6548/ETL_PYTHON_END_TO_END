import os
import pyodbc


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
        conn = pyodbc.connect(self.con_str)
        cursor = conn.cursor()
        res = cursor.execute(query).fetchall()
        columns = [column[0] for column in cursor.description]
<<<<<<< Updated upstream
        conn.close()
        return columns,res 
=======
        rows = [tuple(row) for row in res]
        conn.close()
        return columns,rows 
>>>>>>> Stashed changes
    
# t = DbConnect(con_str=conn_str)
# print(t.execute("SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS where table_name like '%SALES%'"))