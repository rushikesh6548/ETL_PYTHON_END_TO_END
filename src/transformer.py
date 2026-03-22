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
    def __init__(self,file_path,file_name):
        super().__init__(file_path,file_name)

    def extract_schema_tables():
        pass 

