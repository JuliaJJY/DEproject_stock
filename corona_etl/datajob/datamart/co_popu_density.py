from pyspark.sql.functions import col, ceil
from infra.jdbc import DataWarehouse, find_data, save_data
from pyspark.sql import SparkSession

class CoPopuDensity:
    
    @classmethod
    def save(cls):
        popu = find_data(DataWarehouse,'loc')
        # popu=spark.read.jdbc(url=JDBC_DW['url'], table='loc',properties=JDBC_DW['props'])
        patients=find_data(DataWarehouse,'corona_patients')
        # patients=spark.read.jdbc(url=JDBC_DW['url'], table='corona_patients',properties=JDBC_DW['props'])
        pop_patients = cls.__generate_data(popu, patients)
        pop_patients.show()
        save_data(DataMart,pop_patients,'CO_POPU_DENSITY')

    @classmethod
    def __generate_data(cls,popu, patients):
        pop_patients = popu.join(patients, on='loc') \
                            .select('loc'
                                   ,ceil(col('population')/col('area')).alias('POPU_DENSITY')
                                        ,'qur_rate'
                                        ,'std_day') \
                                    .orderBy(col('std_day'))
                                            
        return pop_patients
