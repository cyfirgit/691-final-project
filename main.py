'''
MST961-EWS: Data Science Tools & Techniques
News Scraper Exercise
Cory Campbell, Josh Swain
14 April 2021

This script scrapes the CNN US Edition sitemap for a given year and month, and exports all articles to JSON as follows:
[
    {
        'headline': <article headline>,
        'modified': <date article was last modified>,
        'text':     <article full text>
    }, ...
]
'''

import concurrent.futures as cf
import json
import logging
import math
import re
import sys
import time
from datetime import datetime
from pprint import pprint as pp

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from tqdm import tqdm  # the One True Import of import
from urllib3.util.retry import Retry

import lorient_scraper as lorient

logfile = 'main-' + datetime.now().strftime('%Y-%m-%d-%H:%M:%S') + '.log'

logging.basicConfig(
    filename=logfile, 
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

'''
MAIN
'''

def main():
    try:
        print('Welcome to the <INSERT WITTY NAME FOR FINAL PROJECT HERE>\n')
        site = int(input("Choose a site to handle (1 - L'Orient-Le Jour):"))
        if site == 1:
            operation = int(input("Choose an operation (1 - Scrape Latest):"))
        else:
            raise Exception(f'Invalid input for "site": {site}')

        if operation == 1:
            lorient.scrape_latest()
        else:
            raise Exception(f'Invalid input for "operation": {operation}')
    except Exception as e:
        logging.exception(e)
        print(e)
    
if __name__ == '__main__':
    main()