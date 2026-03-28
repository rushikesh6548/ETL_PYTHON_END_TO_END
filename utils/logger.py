import logging 


def get_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    logging.basicConfig(
    filename=rf'E:\TUTS\PYTHON_THINGS\END_TO_END_PROJECTS\ETL_PYTHON\logs\app.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s')
    
    return logger