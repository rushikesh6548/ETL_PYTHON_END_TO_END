from src.extractor import Extractor
from src.con_details import conn_str
from src.transformer import BaseTransformer,SalesTransformer
## For now we will be bringin in 4-5 tables into staging area using the extractor ! 


## bringing in some table names 


extractor = Extractor(conn_str=conn_str)

## Extracting the data by using extractor class 


t = Extractor(conn_str=conn_str)




sales = SalesTransformer()
print(sales.extract_schema_tables())