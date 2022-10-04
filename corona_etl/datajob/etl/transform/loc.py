import findspark
from pyspark.sql import SparkSession
from infra.jdbc import DataWarehouse, save_data
from pyspark.sql import Row
from infra.spark_session import get_spark_session
from infra.util import cal_std_day
from pyspark.sql.functions import col,count


class LocTransformer:
    AREA = get_spark_session().read.csv('/corona_data/loc/sido_area.csv', encoding='CP949', header=True)
    POPU = get_spark_session().read.csv('/corona_data/loc/sido_population.csv', encoding='CP949', header=True)
    FACILITY=get_spark_session().read.csv('/corona_data/loc/전국다중이용시설.csv', encoding='CP949', header=True)


    @classmethod
    def transform(cls):
        area_pop = cls.AREA.join(cls.POPU, on='loc')
        area_pop = area_pop.select(col('loc').alias('LOC')
                                        , col('area').alias('AREA')
                                        ,col('total').alias('POPULATION')
                                )

        fac_cnt=cls.FACILITY.groupby(col('광역').alias('LOC')).agg(count('*').alias('FACILITY_CNT'))
        
        area_pop_fac=area_pop.join(fac_cnt, on ='LOC')
        save_data(DataWarehouse, area_pop_fac,'LOC')
        
        #LOC는 테이블 이름