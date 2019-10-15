import sys
from tqdm import tqdm
import numpy as np
from bs4 import BeautifulSoup
import pandas as pd
import re
import requests
import time

import logging

logger = logging.getLogger()

import recursive_getsizeof


# getiing the list of companies in DOW_30 from wikipedia
def dow_30_companies_func():
    """

    :return:
    """
    url = 'https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average'
    response = requests.get(url)
    data = response.text
    soup = BeautifulSoup(data, 'lxml')
    table = soup.findChildren('table')[1]
    rows = table.find_all('tr')
    all_cols = []
    for row in rows:
        cols = row.find_all('td')
        cols = [x.text.strip() for x in cols]
        all_cols.append(cols)
    doq_30_df = pd.DataFrame(all_cols, columns=['Company', 'Exchange', 'Symbol', 'Industry', 'Date_Added', 'Notes'])
    doq_30_df.drop(doq_30_df.index[[0]], inplace=True)
    doq_30_df['Symbol'] = doq_30_df['Symbol'].str.replace(r'NYSE:\xa0', '')
    logger.info('Dow 30 Comapnies have been scraped')
    return doq_30_df


def ear_call_trans(i, cookies):
    """
    Getting the links of the DOW_30 ompanies
    :param i:
    :param cookies:
    :return:
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36',
        'cookie': cookies}
    temp_list = []
    url = 'https://seekingalpha.com/symbol/' + str(i).strip() + '/earnings/transcripts'
    # logger.info(url)
    response = requests.get(url, headers=headers)
    data = response.text
    soup = BeautifulSoup(data, 'html.parser')
    for a in soup.find_all('a', string=re.compile('Earnings Call Transcript')):
        temp_list.append('https://seekingalpha.com' + a['href'])
    return temp_list


def get_links(doq_30_df, cookies):
    """
    getting links for earning calls trnascripts for each company
    !!DEPRECATED!!
    :param doq_30_df:
    :param cookies:
    :return:
    """
    list_all = []
    for i in doq_30_df['Symbol'].tolist():
        res = ear_call_trans(i, cookies)
        list_all.append(res)
    logger.info(f'Got {len(list_all)} links!')
    df = pd.DataFrame(list_all).T
    df.columns = doq_30_df['Company'].tolist()[:]
    df = df.fillna('EMPTY')
    logger.info('Companies Earning calls links have been scraped')
    return df



def get_links_2(company_list, cookies):
    """
    getting links for earning calls trnascripts for each company
    :param company_list:
    :param cookies:
    :return:
    """
    list_all = []
    for i in company_list:
        res = ear_call_trans(i, cookies)
        list_all.append(res)
    df = pd.DataFrame(list_all).T
    df.columns = company_list[:]
    df = df.fillna('EMPTY')
    logger.info('Companies Earning calls links have been scraped')
    return df


def get_each_link(link, cookies):
    """
    Scarping the URL of each company to get the speaker and the paragraph
    :param self:
    :param link:
    :param cookies:
    :return:
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36',
        'cookie': cookies}
    response = requests.get(link, headers=headers)
    data = response.text
    # logger.info(f'size of data: {sys.getsizeof(data)}')
    soup = BeautifulSoup(data, 'lxml')
    para = soup.find_all('p')
    # logger.info(f'total size of paras: {recursive_getsizeof.total_size(para,verbose=False)}')
    return str(para)


def get_qtr_year(link):
    """
    Getting the quarter and year from the link
    :param self:
    :param link:
    :return:
    """
    qtr_year = []
    qtr_year.append(link[-40:-38])
    qtr_year.append(link[-37:-33])
    return qtr_year


def data_companies(df, cookies, link_set):
    """
    getting the data with HTML tags from each link for the comapanies
    :param self:
    :param df:
    :param cookies:
    :return:
    """
    list_a = []
    for col in tqdm(df.columns, desc='Scraping progress'):
        values = df[col].tolist()
        values[:] = [x for x in values if 'EMPTY' not in x]
        for link in values:
            if link not in link_set:
                qtr_year = get_qtr_year(link)
                qtr = qtr_year[0]
                year = qtr_year[1]
                paragraphs = get_each_link(link, cookies)
                list_a.append([col, link, qtr, year, paragraphs])
                time.sleep(5)
    df_store = pd.DataFrame(list_a, columns=['company', 'link', 'qtr', 'year', 'text'])
    logger.info('Companies Earning calls text have been scraped')
    return df_store


def is_raw_df_all_good(df):
    """
    This function returns True, if the input dataframe which contains all raw scrapped data is all good.
    Otherwise returns False for later calling "rerun_companies()"
    :param df:
    :return:
    """
    pattern = r'[^.]*please\ enable\ Javascript\ and\ cookies\ in\ your\ browser'
    if np.any(df['text'].str.match(pattern)):
        return False
    else:
        return True


def rerun_companies(df, cookies):
    """
    re-scraping the links which have some errors while scraping
    :param self:
    :param df:
    :param cookies:
    :return:
    """
    pattern = r'[^.]*please\ enable\ Javascript\ and\ cookies\ in\ your\ browser'
    rerun_list = df[df['text'].str.match(pattern)]['link'].tolist()

    rerun_company_list = df[df['text'].str.match(pattern)]['company'].tolist()

    list_a = []
    for link in rerun_list:
        logger.info(f'now re-scrapring link:{link}')
        qtr_year = get_qtr_year(link)
        qtr = qtr_year[0]
        year = qtr_year[1]
        paragraphs = get_each_link(link, cookies)
        list_a.append([link, qtr, year, paragraphs])
        time.sleep(5)

    # construct a new dataframe to return
    df_temp = pd.DataFrame(list_a, columns=['link', 'qtr', 'year', 'text'])
    df_temp['company'] = rerun_company_list
    df = df[~df['link'].isin(rerun_list)]
    df.append(df_temp)
    # rerun_list = df[df['text'].str.match(pattern)]['link'].tolist()
    # df = df[~df['link'].isin(rerun_list)]
    # logger.info('Failed to scrape companies have been scraped again')
    return df
