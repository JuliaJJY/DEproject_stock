from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import os
import sys
import time
import random


def crawler(company_code, maxpage):

    page = 1
    total_source_result = list()
    total_date_result = list()
    total_title_result = list()
    total_link_result = list()
    total_content_result = list()

    while page <= int(maxpage):

        url = 'https://finance.naver.com/item/news_news.nhn?code=' + str(company_code) + '&page=' + str(page)
        source_code = requests.get(url).text
        html = BeautifulSoup(source_code, "lxml")
        time.sleep(random.uniform(3, 4))

        # 뉴스 제목
        titles = html.select('.title')
        title_result = []
        for title in titles:
            title = title.get_text()
            title = re.sub('\n', '', title)
            title_result.append(title)

        # 뉴스 링크
        links = html.select('.title')

        link_result = []
        content_list = []
        for link in links:
            add = 'https://finance.naver.com' + link.find('a')['href']
            link_result.append(add)
            res = requests.get(add)
            soup = BeautifulSoup(res.text, 'lxml')
            content = soup.select_one('#news_read')
            content = content.text.strip()
            content_list.append(content)

        # 뉴스 날짜
        dates = html.select('.date')
        date_result = [date.get_text() for date in dates]

        # 뉴스 매체
        sources = html.select('.info')
        source_result = [source.get_text() for source in sources]
        total_source_result.extend(source_result)
        total_date_result.extend(date_result)
        total_title_result.extend(title_result)
        total_link_result.extend(link_result)
        total_content_result.extend(content_list)

        page += 1

    # 변수들 합쳐서 해당 디렉토리에 csv파일로 저장하기
    print(page)
    result = {"날짜": total_date_result, "언론사": total_source_result,
              "기사제목": total_title_result, "내용": total_content_result, "링크": total_link_result}
    df_result = pd.DataFrame(result)

    print("다운 받고 있습니다------")
    df_result.to_csv('page' + str(company_code) + '.csv',
                     mode='w', encoding='utf-8-sig')


# 종목 리스트 파일 열기
# 회사명을 종목코드로 변환
def convert_to_code(company, maxpage):
    data = pd.read_csv(
        'C:/CODE/Project_S/company_list.txt', dtype=str, sep='\t')   # 종목코드 추출
    company_name = data['회사명']
    keys = [i for i in company_name]  # 데이터프레임에서 리스트로 바꾸기

    company_code = data['종목코드']
    values = [j for j in company_code]

    dict_result = dict(zip(keys, values))  # 딕셔너리 형태로 회사이름과 종목코드 묶기

    pattern = '[a-zA-Z가-힣]+'

    if bool(re.match(pattern, company)) == True:         # Input에 이름으로 넣었을 때
        company_code = dict_result.get(str(company))
        crawler(company_code, maxpage)

    else:                                                # Input에 종목코드로 넣었을 때
        company_code = str(company)
        crawler(company_code, maxpage)

def main():
    info_main = input("="*50+"\n"+"실시간 뉴스기사 다운받기."+"\n"+" 시작하시려면 Enter를 눌러주세요."+"\n"+"="*50)
    
    company = input("종목 이름이나 코드 입력: ") 
    
    maxpage = input("최대 뉴스 페이지 수 입력: ")  
 
    convert_to_code(company, maxpage) 
