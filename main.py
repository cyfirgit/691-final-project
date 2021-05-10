'''
MST961-EWS: Data Science Tools & Techniques
Final Exercise
Cory Campbell, Josh Swain
10 May 2021
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
import the961_scraper as the961

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
        site = int(input("Choose a site to handle (1 - L'Orient-Le Jour, 2 - the961.com):"))
        if site == 1:
            operation = int(input("Choose an operation (1 - Scrape Latest):"))
        elif site == 2:
            operation = int(input("Choose an operation (1 - Scrape Latest):"))
        else:
            raise Exception(f'Invalid input for "site": {site}')

        try:
            opset = site * 10 + operation
        except Exception as e:
            logging.exception(e)
            raise Exception(f'Invalid input for "operation": {operation}')

        if opset == 11:
            lorient.scrape_latest()
        elif opset == 21:
            the961.scrape_latest()
        else:
            raise Exception(f'Invalid input for "operation": {operation}')
    except Exception as e:
        logging.exception(e)
        print(e)
    
if __name__ == '__main__':
    main()