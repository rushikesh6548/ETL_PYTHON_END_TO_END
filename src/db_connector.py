import os
import pyodbc

print(pyodbc.drivers())
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
        return res 
    
t : DbConnect= DbConnect(con_str = conn_str)
print(t.execute(query= 'SELECT top 2 * from person.password'))

