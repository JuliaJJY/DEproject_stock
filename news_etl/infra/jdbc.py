"""
JDBC Connect Info
"""

from enum import Enum

from infra.spark_session import get_spark_session


#ctrl+. enter => import 자동
class DataWarehouse(Enum):
    """
    JDBC Connect Enum
    """
    URL = 'jdbc:oracle:thin:@stockpr_high?TNS_ADMIN=/home/big/project/db/Wallet_stock_pr'
    PROPS={
        'user':'dw_finance_test'
       ,'password':'123qwe!@#QWE'
    }

class DataMart(Enum):
    """
    JDBC Connect Enum
    """
    URL = 'jdbc:oracle:thin:@stockpr_high?TNS_ADMIN=/home/big/project/db/Wallet_stock_pr'
    PROPS={
        'user':'dw_finance_test'
       ,'password':'123qwe!@#QWE'
    }
def save_data(config, dataframe, table_name):
    dataframe.write.jdbc(url=config.URL.value
                        , table=table_name
                        , mode='append'
                        , properties=config.PROPS.value)

def overwrite_data(config, dataframe, table_name):
    dataframe.write.jdbc(url=config.URL.value
                        , table=table_name
                        , mode='overwrite'
                        , properties=config.PROPS.value)                        
def find_data(config, table_name):
    return get_spark_session().read.jdbc(url=config.URL.value
                            , table=table_name
                            ,properties=config.PROPS.value)

                            
