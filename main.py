from src.extractor import Extractor
from src.con_details import conn_str
from src.transformer import BaseTransformer,SalesTransformer
## For now we will be bringin in 4-5 tables into staging area using the extractor ! 
from src.load import Load

## bringing in some table names 


extractor = Extractor(conn_str=conn_str)

## Extracting the data by using extractor class 


t = Extractor(conn_str=conn_str)
t.extract_tables('Sales')

transfom_sales :SalesTransformer= SalesTransformer()
transfom_sales.extract_schema_tables()


# sales = SalesTransformer()
# print(sales.extract_schema_tables())

## Loading to db:

l = Load(file_path='E:\TUTS\PYTHON_THINGS\END_TO_END_PROJECTS\ETL_PYTHON\data\processed',file_name=
         'Sales_Dimension_2026-03-26',con_str=conn_str)
l.load_to_db(table_name='Sales_Dimension')