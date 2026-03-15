from src.extractor import Extractor
from src.con_details import conn_str
## For now we will be bringin in 4-5 tables into staging area using the extractor ! 


## bringing in some table names 


t = Extractor(conn_str=conn_str)

print(conn_str)