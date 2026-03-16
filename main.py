from src.extractor import Extractor
from src.con_details import conn_str
## For now we will be bringin in 4-5 tables into staging area using the extractor ! 
from datetime import datetime , timedelta
import pandas as pd 
## bringing in some table names 


t = Extractor(conn_str=conn_str)

table_list = t.execute_query(query = "select distinct table_name from information_schema.tables where table_name like \n'%Sales%'")


## Now , extracting all tables and pushing them into the staging area: 
## Creating a function to bring in tables from different scheams 

def extract_schema_tables(schema_name):
    table_list_schema = t.execute_query(query = f"SELECT DISTINCT TOP 1 TABLE_NAME FROM INFORMATION_SCHEMA.tables where table_name like '%{schema_name}%'")[1]  ## 1 implies we
                                                                                                                                    # are taking the data part and not colname
    ## above will return a tuple of all tables inside that schema ! 
    print(f'table_list_schema : {table_list_schema}')
    for item in table_list_schema:
        table_name = item[0]
        query = f"SELECT top 2 * FROM {schema_name}.{table_name}"
        ## extraction of that table now : 
        cols,result = t.execute_query(query= query)
        clean_data = [list(row) for row in result]
        
        df = pd.DataFrame(clean_data,columns= cols)
        
    ## Now, saving these in a csv file in the staging area 
        file_path = f'E:/TUTS/PYTHON_THINGS/END_TO_END_PROJECTS/ETL_PYTHON/data/staging/{table_name}_{datetime.now().date()}.csv'
        df.to_csv(file_path)


        

extract_schema_tables(schema_name='Sales')
