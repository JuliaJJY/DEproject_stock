#!/usr/bin/env python3
"""
Module Docstring
"""

__author__ = "Your Name"
__version__ = "0.1.0"
__license__ = "MIT"


import sys
# from datajob.datamart.co_popu_density import CoPopuDensity

from datajob.etl.extract.news_extract import newsExtractor

# from datajob.etl.transform.corona_patient import CoronaPatientTransformer



works ={
    'extract':{
        'news_extract':newsExtractor.extract_data
    }
}

def main():
    """ Main entry point of the app """
    print("hello world")


if __name__ == "__main__":
    args=sys.argv
    print(args)
    #main.py 작업(extract, transform, datamart) 저장할 위치(테이블, 작업)
    #매개변수 2개
    if len(args) !=3 :
        raise Exception('2개의 전달인자가 필요합니다.')

    if args[1] not in works.keys():
        raise Exception('첫번째 전달인자가 이상함 >> ' + str(works.keys()))
    if args[2] not in works[args[1]].keys():
        raise Exception('첫번째 전달인자가 이상함 >> ' + str(works[args[1]].keys()))
    
    work=works[args[1]][args[2]]
    work()
    
    # try:
        # work=works[args[1]][args[2]]
    # except Exception as ex:
    #     print(works.keys()) #사용자가 사용할 예약어 알려줌
    #     print('이상한 작업을 요청하신게 아닐까요?')
    # print(work)
    # 

