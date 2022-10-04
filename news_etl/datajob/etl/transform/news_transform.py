from infra.jdbc import DataWarehouse, save_data
from pyspark.sql import Row
from infra.spark_session import get_spark_session
from infra.util import cal_std_day

#리팩토링 전
# class NewsTransformer:
#     @classmethod
#     def transform(cls):
#         file_name = '/news_data/article/realtimenews_' + cal_std_day(0) + '.json'
#         tmp = spark.read.json(file_name, encoding='UTF-8')
#         # tmp.show(10)

#         data=[]
#         for r1 in tmp.select(tmp.data,tmp.meta.std_day).toLocalIterator():
#             if not r1.data:
#                 continue
#             for r2 in r1.data:
#                 data.append(r2)
        

#         data2=spark.createDataFrame(data)
    

#         news_day =data2.withColumn('STD_DAY',current_date())
#         news_day = news_day[[col('STD_DAY'),col('TITLE')]]
    

#         realtime_news = news_day.select(
#             news_day.STD_DAY.alias('STD_DAY')
#             ,news_day.TITLE.alias('TITLE')
#         )

#         pd_news=realtime_news.to_pandas_on_spark()

#         realtime_news=pd_news.to_spark()


#         realtime_news.write.jdbc(url=JDBC['url'], table='REALTIME_NEWS', mode='append', properties=JDBC['props'])



class NewsTransformer:
    file_name = '/news_data/article/realtimenews_'+ cal_std_day(0)+'.json'


    #완료
    @classmethod
    def transform(cls):

        print("a")

        news= get_spark_session().read.json(cls.file_name, multiLine=True)
        data = cls.generate_rows(news)  
                
                
        news_data = get_spark_session().createDataFrame(data)
        news_data = cls.__stack_dataframe(news_data)
        save_data(DataWarehouse, news_data,'NEWS_DATA')

    @classmethod
    def __stack_dataframe(cls, news_data):
        pd_news = news_data.to_pandas_on_spark() #판다스로 변환


        realtime_news=pd_news.to_spark()
        return realtime_news

    @classmethod
    def generate_rows(cls, news):
        data = []

        for r1 in news.select(news.data, news.meta.std_day).toLocalIterator():
            for r2 in r1.data:
                # row 객체를 만들기 위해 dict 생성
                # ** 파이썬 압축해제 키워드 
                # **kwargs   => 여러 쌍의 키워드 args 가 넘어오면 받아서 dictionary로 반환
                # fnc(**dict) => dict에 있는 key-value 값들이 여러쌍의 kwargs  형태로 함수에 전달
                temp = r2.asDict()
                temp['std_day'] = r1['meta.std_day']
                data.append(Row(**temp))
        return data
        