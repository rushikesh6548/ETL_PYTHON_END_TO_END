from datetime import datetime
import pandas as pd 
import sys
from pathlib import Path
import logging 

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logger import get_logger
from src.db_connector import DbConnect
logger = get_logger()
PROJECT_ROOT = Path(__file__).resolve().parent.parent
STAGING_DIR = PROJECT_ROOT / "data" / "staging"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

class BaseTransformer:
    def __init__(self,file_path,file_name):
        
        self.file_path =  file_path 
        self.file_name = file_name 
        self.comp_file_path  = Path(self.file_path) / f"{self.file_name}.csv"

    def drop_duplicates(self):
        ## reading the file:
        ## using pandas to read the file : 
        try:
            file = pd.read_csv(self.comp_file_path)
            result = file.drop_duplicates()
            PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
            result.to_csv(PROCESSED_DIR / f"{self.file_name}.csv")
            logger.info(f'REMOVED DUPLICATES AND FILE SAVED !! ')
        except FileNotFoundError as e :
            logging.error(f'File Not Found at {self.comp_file_path}')
            raise e 


    def drop_na(self):
        try:
            file = pd.read_csv(self.comp_file_path)
            result = file.dropna(how='any')
            PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
            result.to_csv(PROCESSED_DIR / f"{self.file_name}.csv")
            logger.info(f'DROPPED NA AND FILE SAVED !! ')
        except FileNotFoundError as e:
            logger.error(f'File Not Found at {self.comp_file_path}')
            raise e 
# t = BaseTransformer(file_path=STAGING_DIR
#                     ,file_name='SalesOrderDetail_2026-03-16')


class SalesTransformer(BaseTransformer):
    def __init__(self):
        pass 

    def extract_schema_tables(self):
        ## Bringing in all the important Sales Data for the day :
        try:
            PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

            SalesOrderHeader = pd.read_csv(STAGING_DIR / f"SalesOrderHeader_{datetime.now().date()}.csv")
            SalesOrderDetail = pd.read_csv(STAGING_DIR / f"SalesOrderDetail_{datetime.now().date()}.csv")

            res = pd.merge(SalesOrderHeader,SalesOrderDetail, left_on = ['SalesOrderID'],right_on=['SalesOrderID'] , how = 'inner' )
            res = res.loc[:,['SalesOrderID','OrderDate','CustomerID','ProductID','OrderQty','LineTotal','TotalDue']]
            res.to_csv(PROCESSED_DIR / f"Sales_Dimension_{datetime.now().date()}.csv")
            logger.info(f'Saved Dimension table for Sales ! ')
        except FileNotFoundError as e:
            logger.error(f'File not found ')
            raise e
    
