import concurrent.futures as cf
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from random import randint
import html

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from tqdm import tqdm  # the One True Import of import
from urllib3.util.retry import Retry


class ValidationError(Exception):
    def __init__(self, *args):
        if args:
            self.message = args[0]
        else:
            self.message = None
    
    def __str__(self):
        if self.message:
            return f'ValidationError: {self.message}'
        else:
            return f'ValidationError'


the961log = 'the961-scraper-' + datetime.now().strftime('%Y-%m-%d-%H-%M-%S') + '.log'

logging.basicConfig(
    filename=the961log, 
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


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
    domain = re.compile(r'the961.com')
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
Write batch of articles to .json file
"""
def articles_to_json(parsed_articles:list, filename:str, convert_datetimes=True):
    if convert_datetimes:
        print('Converting datetimes to string...')
        for article in parsed_articles:
            article['mod_date'] = datetime.strftime(article['mod_date'], '%Y-%m-%dT%H:%M%z')
            article['pub_date'] = datetime.strftime(article['pub_date'], '%Y-%m-%dT%H:%M%z')
    print('Writing to file...')
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(parsed_articles, f, ensure_ascii=False, indent=4)
    print('Write successful!')
        

"""
Parse the961.com articles from soup
"""
def parse_article(soup, url):
    article = {}

    #Pull the metadata schema
    try:
        schema_graph = soup.find('script', class_='yoast-schema-graph').contents[0]
        schema_json = json.loads(schema_graph)
        schema = next((sub for sub in schema_json['@graph'] if 'Article' in sub['@type']), None)
    except Exception as e:
        logging.exception(e)
        raise ValidationError(f'Failed to parse schema for article: {url}')

    #Language
    try:
        if schema['inLanguage'] == 'en-US':
            article['language'] = 'en'
        else:
            raise ValidationError(f'Attribute inLanguage is not en-US: {url}')
    except Exception as e:
        logging.exception(e)
        raise ValidationError(f'Failed to parse inLanguage attribute in schema: {url}')
    

    #Date Modified
    try:
        mod_date_str = schema['dateModified']
        mod_date_str = mod_date_str[:-3] + mod_date_str[-2:]
        article['mod_date'] = datetime.strptime(mod_date_str, '%Y-%m-%dT%H:%M:%S%z')
    except Exception as e:
        logging.exception(e)
        raise ValidationError(f'Failed to parse dateModified attribute in schema: {url}')
    

    #Date Published
    try:
        pub_date_str = schema['datePublished']
        pub_date_str = pub_date_str[:-3] + pub_date_str[-2:]
        article['pub_date'] = datetime.strptime(pub_date_str, '%Y-%m-%dT%H:%M:%S%z')
    except Exception as e:
        logging.exception(e)
        raise ValidationError(f'Failed to parse dateModified attribute in schema: {url}')

    #Title
    try:
        article['title'] = html.unescape(schema['headline'])
    except Exception as e:
        logging.exception(e)
        raise ValidationError(f'Failed to parse headline attribute in schema: {url}')

    #Text
    try:
        article_body = soup.find('div', class_='body-color')
        paras = article_body.findChildren('p', recursive=False)
        article['text'] = ' '.join([x.text for x in paras])
    except Exception as e:
        logging.exception(e)
        raise ValidationError(f'Failed to parse article text: {url}')

    return article

    
'''
Worker for ProcessPoolExecutor
'''
def thread_worker(url):

    html_tic = time.perf_counter()
    page = get_html(url)
    html_toc = time.perf_counter()
    if not page:
        return {'valid': False}
        

    soup_tic = time.perf_counter()
    soup = BeautifulSoup(page.text, features='lxml')
    soup_toc = time.perf_counter()

    parse_tic = time.perf_counter()
    try:
        article = parse_article(soup, url)
    except ValidationError as e:
        logging.error(e)
        #Hacky way to gracefully skip an errored article
        article = {'text': ''}
    parse_toc = time.perf_counter()

    results = {}
    if article['text'] == '':
        logging.warning(f'Url {url} produced empty article.')
    else:
        results['article'] = article  
    results['html_time'] = (html_toc - html_tic)
    results['soup_time'] = (soup_toc - soup_tic)
    results['parse_time'] = (parse_toc - parse_tic)

    return results


"""
Collect article urls from one of the post sitemaps at the961.com
"""
def crawl_sitemap_the961(sitemap):
    print(f'Checking sitemap for articles: {sitemap}')
    page = get_html(sitemap)
    soup = BeautifulSoup(page.text)
    links = soup.find_all('loc')
    urls = [x.text for x in links]
    print('Success!')
    return urls


"""
Collect all parsable articles from the961.com
"""
def crawl_all_the961():
    with open('the961_toc.json', 'r') as f:
        toc = json.load(f)
    current_map = toc['current_sitemap']
    current_index = int(re.search(r'(\d)\.xml', current_map).groups()[0])
    hi_index = current_index
    hi_map = current_map
    completed_maps = set(toc['completed_sitemaps'])
    parsed_articles = set(toc['parsed_articles'])
    new_parsed = parsed_articles
    articles = set()

    mainmap = get_html("https://www.the961.com/sitemap_index.xml")
    soup = BeautifulSoup(mainmap.text)
    maps = soup.find_all(text=re.compile(r'post\-sitemap'))
    for sitemap in maps:
        if sitemap == current_map:
            urls = set(crawl_sitemap_the961(sitemap))
            articles.update(urls - parsed_articles)
            new_parsed.update(urls)
        elif not sitemap in completed_maps:
            index = int(re.search(r'(\d)\.xml', sitemap).groups()[0])
            urls = set(crawl_sitemap_the961(sitemap))
            articles.update(urls)
            if index > hi_index:
                completed_maps.add(hi_map)
                hi_index = index
                hi_map = sitemap
                new_parsed = urls
            else:
                completed_maps.add(sitemap)
    
    toc['current_sitemap'] = hi_map
    toc['completed_sitemaps'] = list(completed_maps)
    toc['parsed_articles'] = list(new_parsed)

    return articles, toc


'''
Parse many articles, given a list of article_ids.
'''
def parse_many(urls):
    url_list = list(urls)
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
    print(f'Checking for unparsed articles...')
    articles, toc = crawl_all_the961()
    print(f'Found {len(articles)} new articles!')
    articles, times = parse_many(articles)

    performance_timing = [
    '\nPerformance timing:',
    f"\nget_html()\t\tMin: {min(times['html']):.6f}\tMax: {max(times['html']):.6f}\tAvg: {sum(times['html'])/len(times['html']):.6f}",
    f"get_soup()\t\tMin: {min(times['soup']):.6f}\tMax: {max(times['soup']):.6f}\tAvg: {sum(times['soup'])/len(times['soup']):.6f}",
    f"parse_article()\t\tMin: {min(times['parse']):.6f}\tMax: {max(times['parse']):.6f}\tAvg: {sum(times['parse'])/len(times['parse']):.6f}",
    ]
    for line in performance_timing:
        print(line)
        logging.info(line)

    filename = 'the961-' + datetime.now().strftime('%Y-%m-%d-%H:%M:%S') + '.json'
    articles_to_json(articles, filename)

    print(f'Parsed {len(articles)} new articles!')

    with open('the961_toc.json', 'w', encoding='utf-8') as f:
        json.dump(toc, f, ensure_ascii=False, indent=4)

    return


