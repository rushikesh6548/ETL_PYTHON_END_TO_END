from datetime import datetime
import pandas as pd 

class BaseTransformer:
    def __init__(self,file_path,file_name):
        
        self.file_path =  file_path 
        self.file_name = file_name 
        self.comp_file_path  = f'{self.file_path}\{self.file_name}.csv'    

    def drop_duplicates(self):
        ## reading the file:
        ## using pandas to read the file : 
        file = pd.read_csv(self.comp_file_path)
        result = file.drop_duplicates()
        result.to_csv(f'E:\TUTS\PYTHON_THINGS\END_TO_END_PROJECTS\ETL_PYTHON\data\processed\{self.file_name}.csv')

    def drop_na(self):
        file = pd.read_csv(self.comp_file_path)
        result = file.dropna(how='any')
        result.to_csv(f'E:\TUTS\PYTHON_THINGS\END_TO_END_PROJECTS\ETL_PYTHON\data\processed\{self.file_name}.csv')

# t = BaseTransformer(file_path='E:\TUTS\PYTHON_THINGS\END_TO_END_PROJECTS\ETL_PYTHON\data\staging'
#                     ,file_name='SalesOrderDetail_2026-03-16')


class SalesTransformer(BaseTransformer):
    def __init__(self):
        pass 

    def extract_schema_tables(self):
        ## Bringing in all the important Sales Data for the day :
        SalesOrderHeader = pd.read_csv(f'E:\TUTS\PYTHON_THINGS\END_TO_END_PROJECTS\ETL_PYTHON\data\staging\SalesOrderHeader_{datetime.now().date()}.csv')
        SalesOrderDetail = pd.read_csv(f'E:\TUTS\PYTHON_THINGS\END_TO_END_PROJECTS\ETL_PYTHON\data\staging\SalesOrderDetail_{datetime.now().date()}.csv')

        res = pd.merge(SalesOrderHeader,SalesOrderDetail, left_on = ['SalesOrderID'],right_on=['SalesOrderID'] , how = 'inner' )
        res = res.loc[:,['SalesOrderID','OrderDate','CustomerID','ProductID','OrderQty','LineTotal','TotalDue']]
        res.to_csv(f'E:\TUTS\PYTHON_THINGS\END_TO_END_PROJECTS\ETL_PYTHON\data\processed\Sales_Dimension_{datetime.now().date()}.csv')

    
