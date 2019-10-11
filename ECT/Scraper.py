from bs4 import BeautifulSoup
import pandas as pd
import re
import requests
import time


def dow_30_companies_func():
    """

    :return:
    """
    # getiing the list of companies in DOW_30
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
    doq_30_df['Symbol'] = doq_30_df['Symbol'].str.replace('NYSE:', u' ')
    print('Dow 30 Comapnies have been scraped')
    return doq_30_df


def ear_call_trans(i, cookies):
    """

    :param i:
    :param cookies:
    :return:
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36',
        'cookie': cookies}
    temp_list = []
    url = 'https://seekingalpha.com/symbol/' + str(i).strip() + '/earnings/transcripts'
    # print(url)
    response = requests.get(url, headers=headers)
    data = response.text
    soup = BeautifulSoup(data, 'html.parser')
    for a in soup.find_all('a', string=re.compile('Earnings Call Transcript')):
        temp_list.append('https://seekingalpha.com' + a['href'])
    return temp_list


def get_links(doq_30_df, cookies):
    """
    !!DEPRECATED!!
    :param doq_30_df:
    :param cookies:
    :return:
    """
    list_all = []
    for i in doq_30_df['Symbol'].tolist():
        res = ear_call_trans(i, cookies)
        list_all.append(res)
    df = pd.DataFrame(list_all).T
    df.columns = doq_30_df['Company'].tolist()[:]
    df = df.fillna('EMPTY')
    print('Companies Earning calls links have been scraped')
    return df


def get_links_2(company_list, cookies):
    list_all = []
    for i in company_list:
        res = ear_call_trans(i, cookies)
        list_all.append(res)
    df = pd.DataFrame(list_all).T
    df.columns = company_list[:]
    df = df.fillna('EMPTY')
    print('Companies Earning calls links have been scraped')
    return df


def get_each_link(link, cookies):
    """
    issue: find_all()returns objects, we want strings
    :param self:
    :param link:
    :param cookies:
    :return:
    """
    # Scarping the URL of each company to get the speaker and the paragraph
    str_para = ''
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36',
        'cookie': cookies}
    response = requests.get(link, headers=headers)
    data = response.text
    soup = BeautifulSoup(data, 'lxml')
    para = soup.find_all('p')
    return str(para)


def get_qtr_year(link):
    """

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

    :param self:
    :param df:
    :param cookies:
    :return:
    """
    list_a = []
    for col in df.columns:
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
    print('Companies Earning calls text have been scraped')
    return df_store


def rerun_companies(df, cookies):
    """

    :param self:
    :param df:
    :param cookies:
    :return:
    """
    list_a = []
    pattern = r'[^.]*please\ enable\ Javascript\ and\ cookies\ in\ your\ browser'
    rerun_list = df[df['text'].str.match(pattern)]['link'].tolist()
    rerun_company_list = df[df['text'].str.match(pattern)]['company'].tolist()
    for link in rerun_list:
        print(link)
        qtr_year = get_qtr_year(link)
        qtr = qtr_year[0]
        year = qtr_year[1]
        paragraphs = get_each_link(link, cookies)
        list_a.append([link, qtr, year, paragraphs])
        time.sleep(5)
    df_temp = pd.DataFrame(list_a, columns=['link', 'qtr', 'year', 'text'])
    df_temp['company'] = rerun_company_list
    df = df[~df['link'].isin(rerun_list)]
    df.append(df_temp)
    rerun_list = df[df['text'].str.match(pattern)]['link'].tolist()
    df = df[~df['link'].isin(rerun_list)]
    print('Failed to scrape companies have been scraped again')
    return df
