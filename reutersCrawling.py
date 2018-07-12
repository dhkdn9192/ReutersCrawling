from bs4 import BeautifulSoup
from tqdm import tqdm
import requests
import numpy as np
import time
import json
import os
from urllib.request import urlopen
from dateutil.parser import parse as date_parse


# url 링크의 html 코드를 가져오는 함수
def get_html(url):
    response = requests.get(url)
    html = response.text

    return html


# 로이터통신 홈페이지의 카테고리 헤드라인 페이지에서 html 코드를 가져오는 함수
def get_reuters_html(category, page_index):
    url_form = 'https://www.reuters.com/news/archive/{}News?view=page&page={}&pageSize=10'
    url = url_form.format(category, page_index)
    html = get_html(url)

    return html


# 헤드라인 페이지에서 각 뉴스에 대한 링크 url을 추출하는 함수
def get_news_link_from_html(html):
    base_url = 'https://www.reuters.com'
    soup = BeautifulSoup(html, 'html.parser')
    articles = soup.select('div.news-headline-list > article.story')

    links = []
    for article in articles:
        story_div = article.select_one('div.story-content')
        link = story_div.find('a').attrs['href']
        links.append(base_url + link)

    return links


# 뉴스 url로부터 뉴스의 BODY, TITLE, DATE, TIME을 추출하여 json으로 반환하는 함수
def parse_news_text_from_html(url, news_source):
    news_dict = {}

    html = get_html(url)
    soup = BeautifulSoup(html, 'html.parser')

    # 뉴스 기사에 대한 메타데이터를 json 포맷으로 저장한 태그를 추출한다
    meta_list = soup.select('script[type="application/ld+json"]')
    if len(meta_list) < 1:
        # 메타데이터를 못 찾은 경우 예외처리
        print('[INFO] news meta data not found... return emtpy news body..')
        return ''

    # 메타데이터를 딕셔너리 형태로 변환
    meta_str = meta_list[0].get_text()
    meta_dict = json.loads(meta_str)

    # DATE, TIME - 뉴스 작성일자, 시간
    meta_date = meta_dict['dateCreated']  # 2018-07-08T02:52:44+0000 포맷
    parsed_date = date_parse(meta_date)
    news_date = parsed_date.strftime('%Y%m%d')
    news_time = parsed_date.strftime('%H:%M:%S')
    news_dict['DATE'] = news_date
    news_dict['TIME'] = news_time

    # TITLE - 뉴스 제목
    news_title = meta_dict['headline']
    news_dict['TITL'] = news_title

    # SOUR - 뉴스 출처
    news_dict['SOUR'] = news_source

    # BODY - 본문
    body_div = soup.select_one('div.body_1gnLA')
    news_text = []
    for child_div in body_div.children:
        if child_div.name == 'p':
            news_text.append(child_div.text)
    news_body = ' '.join(news_text)
    news_dict['BODY'] = news_body

    return news_dict


# 지정된 json 파일명으로 데이터 저장하는 함수
def append_data_to_jsonfile(data, filepath):
    with open(filepath, 'a', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)
        f.write('\n')       # 각 json 데이터 사이 띄워서 저장하기
        f.close()


# 메인 실행 함수
def execute_news_crawling(category, page_start, page_end, save_dir):
    """
    params
        - category : str. 'business'를 넣으면 된다
        - pages : int. 헤드라인 페이지를 얼마나 크롤링할지 정하는 값. 한 페이지에 10개 기사가 있다.
    output
        - json 포맷으로 저장된 뉴스 데이터 파일
    """

    # 예외처리-category
    if category not in ['business', 'world', 'politics', 'technology']:
        print('[ERROR] invalid category...')
        return

    # 예외처리-page
    if page_start <= 0 or page_end < page_start or page_end - page_start > 3270:
        print('[ERROR] invalid page params...')
        return

    # 로그를 저장할 디렉토리가 없다면 생성한다
    log_dir = './logs'
    if os.access(log_dir, os.F_OK) == False:
        os.mkdir(log_dir)

    # 각 뉴스 별 인덱스를 입력하기 위한 카운터
    news_cnt = 0

    # 헤드라인 페이지 각각에 대해 크롤링 수행
    for page_index in range(page_start, page_end):

        # 헤드라인 한 페이지에서 각 뉴스들의 url 리스트룰 추출
        reuters_html = get_reuters_html(category=category, page_index=page_index)
        url_list = get_news_link_from_html(reuters_html)

        for url in url_list:
            news_cnt += 1

            log_str = 'news_cnt : {} / news_page : {} / url : {}'.format(news_cnt, page_index, url)
            append_data_to_jsonfile(log_str, log_dir + '/log_page{}to{}'.format(page_start, page_end))

            # 뉴스 출처 작성
            news_source = 'Reuters_' + category

            # dict형태의 뉴스 데이터를 생성
            news_data = parse_news_text_from_html(url, news_source)

            # 뉴스를 저장할 파일명 생성
            if save_dir[-1] != '/':
                save_dir += '/'

            path_y = news_data['DATE'][:4] + '/'
            path_m = news_data['DATE'][4:6] + '/'
            filedir = save_dir + path_y + path_m
            filepath = filedir + 'news_' + news_data['DATE'] + '.json'

            # 뉴스를 저장할 디렉토리가 없다면 생성한다
            if os.access(filedir, os.F_OK) == False:
                os.makedirs(filedir)

            # json 파일에 저장하기
            append_data_to_jsonfile(news_data, filepath)

            if news_cnt % 10 == 0:
                print('now crawling ... {}'.format(news_cnt))

    finish_log = 'crawling finished ... {}'.format(news_cnt)
    append_data_to_jsonfile(finish_log, log_dir + '/log_page{}to{}'.format(page_start, page_end))


# 메인
if __name__ == '__main__':

    # run
    # execute_news_crawling(category='business', page_start=1, page_end=300, save_dir='/data/data/news_foreign')
    execute_news_crawling(category='business', page_start=300, page_end=600, save_dir='/data/data/news_foreign')
    # execute_news_crawling(category='business', page_start=600, page_end=900, save_dir='/data/data/news_foreign')
    # execute_news_crawling(category='business', page_start=900, page_end=1200, save_dir='/data/data/news_foreign')
    # execute_news_crawling(category='business', page_start=1200, page_end=1500, save_dir='/data/data/news_foreign')
    # execute_news_crawling(category='business', page_start=1500, page_end=1800, save_dir='/data/data/news_foreign')
    # execute_news_crawling(category='business', page_start=1800, page_end=2100, save_dir='/data/data/news_foreign')
    # execute_news_crawling(category='business', page_start=2100, page_end=2400, save_dir='/data/data/news_foreign')
    # execute_news_crawling(category='business', page_start=2400, page_end=2700, save_dir='/data/data/news_foreign')
    # execute_news_crawling(category='business', page_start=2700, page_end=3000, save_dir='/data/data/news_foreign')
