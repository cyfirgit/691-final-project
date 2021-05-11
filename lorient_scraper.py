import concurrent.futures as cf
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from random import randint

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from tqdm import tqdm  # the One True Import of import
from urllib3.util.retry import Retry

lorientlog = 'lorient-scraper-' + datetime.now().strftime('%Y-%m-%d-%H:%M:%S') + '.log'

logging.basicConfig(
    filename=lorientlog, 
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


# Unless their site structure changes this is the lowest value for an article id.
LORIENT_MIN_ARTICLE = 218146


"""
Session builder timeout-tolerant requests.
"""
def requests_session(
    retries = 3,
    backoff_factor = 0.3,
    status_forcelist=(500, 502, 504),
    session=None,
    ):

    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


"""
Get the html for an article.
"""
def get_html(url):
    domain = re.compile(r'lorientlejour.com/article')
    try:
        page = requests_session().get(url, timeout=4)
        if page.status_code == 404:
            logging.warning(f'404 Error: {url}')
            return
        elif re.search(domain, page.url) == None:
            logging.warning(f'Non-article redirect: {url} --> {page.url}')
            return
        else:
            return page
    except Exception as e:
        logging.exception(e)
        logging.error(f'Failure on {url}')
        return



"""
Extract the article text and metadata from article Soup.
"""
def parse_article(soup):
    article = {}
    article['language'] = article_langauge(soup)
    article['title'] = parse_meta(soup, 'content', **{'property':'og:title'})
    datetime_str = parse_meta(soup, 'content', **{'property':'article:published_time'})
    try:
        article['datetime'] = datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M%z')
    except Exception as e:
        logging.exception(e)
        logging.error(f'Article {article["title"]} parsed datetime_str to {datetime_str}')
    divs = soup.find_all('div', class_='article_full_text')
    article['text'] = ""
    for div in divs:
        paras = div.find_all('p')
        article['text'] += (" ".join([x.text for x in paras]))
    return article


"""
Get the value of a specific meta tag attribute without throwing an error if
the attribute is not present.
"""
def parse_meta(soup, attr, **kwargs):
    try:
        result = soup.find('meta', attrs=kwargs)[attr]
    except:
        result = None
    return result


"""
Identify the language of the article.  This is predicated on the assumptions:
 - canonical links are tagged as 'www' for fr articles
 - canonical links are tagged as 'today' for en articles
 - those are the only two languages articles use.
"""
def article_langauge(soup):
    fr_article = re.compile(r'www.lorientlejour.com')
    en_article = re.compile(r'today.lorientlejour.com')
    info = soup.find('link',  attrs={'rel':'canonical'})
    if re.search(fr_article, str(info)) != None:
        return 'fr'
    elif re.search(en_article, str(info)) != None:
        return 'en'
    else:
        logging.warning(f"Could not detect article language: {info['href']}")
        return None


"""
Write batch of articles to .json file
"""
def articles_to_json(parsed_articles:list, filename:str, convert_datetimes=True):
    if convert_datetimes:
        print('Converting datetimes to string...')
        for article in parsed_articles:
            article['datetime'] = datetime.strftime(article['datetime'], '%Y-%m-%dT%H:%M%z')
    print('Writing to file...')
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(parsed_articles, f, ensure_ascii=False, indent=4)
    print('Write successful!')
        

"""
Status code tester for find_latest()
"""
def test_id(article_id):
    try:
        url = 'https://www.lorientlejour.com/article/' + str(article_id)
        r = requests_session().head(url, timeout=3)
        code = r.status_code
        if code == 404 or code == 200 or code == 301:
            return code
        else:
            raise ConnectionError
    except Exception as e:
        raise e


"""
Find the most recently published article.  This is not a perfect guarantee of
the latest article: it may land in the space right before a as-yet-unpublished
article.  It operates on the assumption that all ids correspond, eventually,
to articles.
"""
def find_latest(start_id):
    #This is just a binary search with an unboounded upper limit.
    searcher = 1
    under = True
    top = 0
    bottom = 0
    while under:
        try:
            code = test_id(start_id + searcher)
        except Exception as e:
            print(e)
            return None

        #Look forward 10 article_ids to make sure you didn't land on a dead id.
        if code == 404:
            for i in range(1,11):
                check_code = test_id(start_id + searcher + i)
                if check_code != 404:
                    code = check_code
                    break
        
        if code == 404:
            under = False
            bottom = int(searcher / 2) + start_id
            top = searcher + start_id
        else:
            searcher *= 2

        if searcher == 2 ** 21:
            print(f'End of articles not found in {2 ** 20} articles forward from start_id {start_id}.')
            return start_id + 2 ** 20
    
    while not (top - 1 == bottom):
        searcher = bottom + int((top - bottom) / 2)
        try:
            code = test_id(searcher)
        except Exception as e:
            print(e)
            return None
        if code == 404:
            top = searcher
        else:
            bottom = searcher
    
    return bottom


"""
Date tester for find_backwards()
"""
def test_date(article_id):
    url = 'https://www.lorientlejour.com/article/' + str(article_id)
    page = requests_session(retries=8).get(url, timeout=3)
    if page.status_code == 404:
        return test_date(article_id + 1)
    soup = BeautifulSoup(page.text, 'html.parser')
    datetime_str = parse_meta(soup, 'content', **{'property':'article:published_time'})
    result = datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M%z')
    return result
    

"""
Binary search backwards to find the first article published after target_date.
"""
def find_backwards(top, top_date, target_date):
    width = 16
    try:
        while test_date(top - width) > target_date:
            width *= 2
    except Exception as e:
        logging.exception(e)
        raise e
    
    bottom = top - width
    top = bottom + int(width / 2)
    while top - bottom > 1:
        guessid = bottom + int((top - bottom) /2 )
        try:
            if test_date(guessid) > target_date:
                top = guessid
            else:
                bottom = guessid
        except Exception as e:
            logging.exception(e)
            raise e
    return bottom



    
'''
Worker for ProcessPoolExecutor
'''
def thread_worker(url_str):

    html_tic = time.perf_counter()
    page = get_html(url_str)
    html_toc = time.perf_counter()
    if not page:
        return {'valid': False}
        

    soup_tic = time.perf_counter()
    soup = BeautifulSoup(page.text, 'html.parser')
    soup_toc = time.perf_counter()

    parse_tic = time.perf_counter()
    article = parse_article(soup)
    parse_toc = time.perf_counter()

    results = {}
    if article['text'] == '':
        logging.warning(f'Url {url_str} produced empty article.')
    else:
        results['article'] = article  
    results['html_time'] = (html_toc - html_tic)
    results['soup_time'] = (soup_toc - soup_tic)
    results['parse_time'] = (parse_toc - parse_tic)

    return results


'''
Parse many articles, given a list of article_ids.
'''
def parse_many(article_ids):
    url_base = 'https://www.lorientlejour.com/article/'
    url_list = [url_base + str(x) for x in article_ids]
    parsed_list = []
    html_times = []
    soup_times = []
    parse_times = []

    with tqdm(total=len(url_list)) as pbar:
        with cf.ProcessPoolExecutor(max_workers=16) as executor:
            futures = {executor.submit(thread_worker, arg): arg for arg in url_list}
            for future in cf.as_completed(futures):
                results = future.result()
                pbar.update(1)
                if 'article' in results.keys():
                    parsed_list.append(results['article'])
                    html_times.append(results['html_time'])
                    soup_times.append(results['soup_time'])
                    parse_times.append(results['parse_time'])
    
    #handle_failures(failed_urls)
    times = {'html': html_times, 
             'soup': soup_times,
             'parse': parse_times}
    return parsed_list, times


"""
Construct a parse_many() for scraping latest articles, process output
"""
def scrape_latest():
    with open('toc.json', 'r') as f:
        toc = json.load(f)
    max_id = int(toc['max_id'])
    max_datetime = datetime.strptime(toc['max_datetime'], '%Y-%m-%dT%H:%M%z')
    now = datetime.now(timezone.utc)
    delta = now - max_datetime
    print(f'Seeking farthest forward article...')
    latest_id = find_latest(max_id)
    print(f'Found! {latest_id}')
    articles, times = parse_many(range(max_id, latest_id + 1))

    performance_timing = [
    '\nPerformance timing:',
    f"\nget_html()\t\tMin: {min(times['html']):.6f}\tMax: {max(times['html']):.6f}\tAvg: {sum(times['html'])/len(times['html']):.6f}",
    f"get_soup()\t\tMin: {min(times['soup']):.6f}\tMax: {max(times['soup']):.6f}\tAvg: {sum(times['soup'])/len(times['soup']):.6f}",
    f"parse_article()\t\tMin: {min(times['parse']):.6f}\tMax: {max(times['parse']):.6f}\tAvg: {sum(times['parse'])/len(times['parse']):.6f}",
    ]
    for line in performance_timing:
        print(line)
        logging.info(line)

    filename = 'lorient-' + str(max_id + 1) + '-' + str(latest_id) + '.json'
    articles_to_json(articles, filename)

    toc['max_id'] = str(latest_id)
    toc['max_datetime'] = datetime.strftime(now, '%Y-%m-%dT%H:%M%z')

    print(f'Found {len(articles)} new articles over {delta.days + 1} days.')

    with open('toc.json', 'w', encoding='utf-8') as f:
        json.dump(toc, f, ensure_ascii=False, indent=4)


def scrape_backwards(cutoff):
    with open('toc.json', 'r') as f:
        toc = json.load(f)
    min_id = int(toc['min_id'])
    min_datetime = datetime.strptime(toc['min_datetime'], '%Y-%m-%dT%H:%M%z')
    delta = min_datetime - cutoff

    #Find the first article_id that is within the cutoff period.
    print(f'Seeking backwards to {datetime.strftime(cutoff, "%Y-%m-%d")}')
    first_id = find_backwards(min_id, min_datetime, cutoff)
    print(f'Found! {first_id}')

    #Parse out the articles
    articles, times = parse_many(range(first_id, min_id))

    performance_timing = [
    '\nPerformance timing:',
    f"\nget_html()\t\tMin: {min(times['html']):.6f}\tMax: {max(times['html']):.6f}\tAvg: {sum(times['html'])/len(times['html']):.6f}",
    f"get_soup()\t\tMin: {min(times['soup']):.6f}\tMax: {max(times['soup']):.6f}\tAvg: {sum(times['soup'])/len(times['soup']):.6f}",
    f"parse_article()\t\tMin: {min(times['parse']):.6f}\tMax: {max(times['parse']):.6f}\tAvg: {sum(times['parse'])/len(times['parse']):.6f}",
    ]
    for line in performance_timing:
        print(line)
        logging.info(line)

    filename = 'lorient-' + str(first_id) + '-' + str(min_id) + '.json'
    articles_to_json(articles, filename)

    toc['min_id'] = str(first_id)
    toc['min_datetime'] = datetime.strftime(cutoff, '%Y-%m-%dT%H:%M%z')

    print(f'Found {len(articles)} new articles over {delta.days + 1} days.')

    with open('toc.json', 'w', encoding='utf-8') as f:
        json.dump(toc, f, ensure_ascii=False, indent=4)


'''
Lookup min_date from toc.json for user interface.
'''
def min_date_lookup():
    with open('toc.json', 'r') as f:
        toc = json.load(f)
    min_datetime = datetime.strptime(toc['min_datetime'], '%Y-%m-%dT%H:%M%z')
    return min_datetime


'''
Combine parsed out article jsons and filter duplicates.
'''
def combine_lorient_json():
    lorient_filename = re.compile(r'lorient\-\d+\-\d+\.json')
    directory = os.listdir()
    lorient_files = [x for x in directory if re.match(lorient_filename, x) != None]
    all_titles = set()
    all_articles = []
    print(f'Reading in jsons...')
    for l_file in lorient_files:
        with open(l_file, 'r') as f:
            l_arts = json.load(f)
        titleset = set([x['title'] for x in l_arts])
        titleset = titleset - all_titles
        for article in l_arts:
            if article['title'] in titleset:
                all_articles.append(article)
        all_titles.update(titleset)
    print(f'Read complete!')
        
    print(f'Sorting articles...')
    articles_list = list(all_articles)
    results = sorted(articles_list, key=lambda k: datetime.strptime(k['datetime'], '%Y-%m-%dT%H:%M%z'))
    print(f'Sort complete!')

    articles_to_json(results, 'lorient_all.json', convert_datetimes=False)
    
    return
