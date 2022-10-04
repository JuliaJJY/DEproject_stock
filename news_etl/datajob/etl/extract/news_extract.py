# !pip install BeautifulSoup4
import json
import time
from bs4 import BeautifulSoup
from infra.util import cal_std_day, execute_rest_api
from infra.hdfs_client import get_client
import requests
import random
from bs4 import BeautifulSoup


class NewsExtractor:
    file_dir = '/news_data/article/'
    file_name = 'realtimenews_' + cal_std_day(0) + '.json'

    
    @classmethod
    def extract_data(cls):
        cols=['TITLE']
        data=[]
        
        for page in range (1,20):
            time.sleep(random.uniform(3, 4))
            url='https://finance.naver.com/news/news_list.naver?mode=LSS2D&section_id=101&section_id2=258&page='+str(page)
                # url = 'https://finance.naver.com/news/news_list.naver?mode=LSS2D&section_id=101&section_id2=258&date=20220927&page='+str(page)
            # headers = {"user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36"}
            response = execute_rest_api('get',url,{},{})
            # response_txt = response.text
            soup = BeautifulSoup(response, 'html.parser')
            
            
            article_list=soup.select('#contentarea_left > ul > li > dl')
            
            for articles in article_list:
                for idx, article in enumerate(articles.select('a')):
                    rows=[]

                    news_title = article.text
                
                    if len(news_title) > 0:
                        rows.append(news_title)
                        tmp=dict(zip(cols,rows))
                
                        data.append(tmp)
                                            

        res = {
            'meta':{
                'desc':'네이버 실시간 속보 크롤링',
                'cols':{
                    'TITLE':'뉴스제목'
                },
                'std_day':cal_std_day(0)
            },
        'data':data
        }

        get_client().write(cls.file_dir+cls.file_name, json.dumps(res, ensure_ascii=False), encoding='utf-8')



# class newsExtractor:
#     FILE_DIR = '/news_data/article/'
#     FILE_NAME = 'realtimenews_' + cal_std_day(0) + '.json'
#     URL = 'https://finance.naver.com/news/news_list.naver?mode=LSS2D&section_id=101&section_id2=258&page='+str(page)
    
#     @classmethod
#     def extract_data(cls):
#         response_txt = execute_rest_api('get',cls.URL,{},{})
#         # trs = cls.__parse_news_htmltag(response_txt)
#         #dls 전역변수로 설정하고 하기
#         data = cls.__create_news_json(dls,articles)

#         cls.__dump_to_hdfs(data)

#     @classmethod
#     def __dump_to_hdfs(cls, data):
#         res = cls.__create_json_data(data)

#         get_client().write(cls.FILE_DIR+cls.FILE_NAME, json.dumps(res, ensure_ascii=False),overwrite=True, encoding='utf-8')
    

        
#     @classmethod
#     def __create_json_data(cls, data):
#         res = {
#             'meta':{
#                 'desc':'네이버 실시간 속보 크롤링',
#                 'cols':{
#                     'TITLE':'뉴스제목'
#                 },
#                 'std_day':cal_std_day(0)
#             },
#         'data':data
#         }
#         return res

#     #완료    
#     # @classmethod
#     # def __parse_news_htmltag(cls, response_txt):
#     #     soup = BeautifulSoup(response_txt, 'html.parser')
#     #     dls = soup.select('#contentarea_left > ul > li > dl')
#     #     return dls

#     #완료  
#     @classmethod
#     def __create_news_json(cls, dls,articles):
#         cols=['TITLE']
#         data=[]

#         for page in range (1,20):
#             # time.sleep(random.uniform(3, 4))
#             url='https://finance.naver.com/news/news_list.naver?mode=LSS2D&section_id=101&section_id2=258&page='+str(page)
#                 # url = 'https://finance.naver.com/news/news_list.naver?mode=LSS2D&section_id=101&section_id2=258&date=20220927&page='+str(page)
#             headers = {"user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36"}
#             response = requests.get(url, headers=headers)
#             #html = execute_rest_api('get',url,{},{})

            
#             response_txt = response.text
#             soup = BeautifulSoup(response_txt, 'html.parser')
            
#             dls=soup.select('#contentarea_left > ul > li > dl')
            
#             for articles in dls:
#                 for idx, article in enumerate(articles.select('a')):
#                     rows=[]

#                     news_title = article.text
                
#                     if len(news_title) > 0:
#                         rows.append(news_title)
#                         tmp=dict(zip(cols,rows))
                
#                         data.append(tmp)
#             return data, dls
                