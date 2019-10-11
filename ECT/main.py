from pymongo import MongoClient
from bs4 import BeautifulSoup
import logging
import os, sys
import pandas as pd
import json
import Scraper
import TranscriptProcessor


def connect_to_db(database='Datapipelines'):
    client = MongoClient('localhost')
    db = client[database]
    return db

def get_raw_csv_list(raw_csv_path='data/raw_data'):
    raw_csv_ls = os.listdir(raw_csv_path)
    return [os.path.join(raw_csv_path,fn) for fn in raw_csv_ls]


def insert_companies(db, com_df, coll_name="companies"):
    """

    :param db:
    :param com_df:
    """
    com_json_str = com_df.to_json(orient='records')
    com_json = json.loads(com_json_str)
    coll = db[coll_name]
    coll.insert_many(com_json)


def get_archived_links(db, coll_name="raw"):
    """

    :param db:
    """
    coll = db[coll_name]
    links = coll.distinct("link")
    return set(links)


def get_company_list(db, coll_name="companies"):
    coll = db[coll_name]
    company_list = coll.distinct("Company")
    return company_list

def get_company_links(db, cookies):
    company_list = get_company_list(db)
    links_df = Scraper.get_links_2(company_list, cookies)
    return links_df


def insert_raw_data(db, df, coll_name="raw"):
    df['isProcessed'] = False
    df_json_str = df.to_json(orient='records')
    df_json = json.loads(df_json_str)
    coll = db[coll_name]
    coll.insert_many(df_json)# can be changed to batch inserting.

def get_unprocessed_raw(db, coll_name="raw"):
    coll = db[coll_name]
    coll.find({"isProcessed":False},{"text":1})
    return

def process_raw_html():
    data = pd.read_csv("companies.csv", index_col=0)
    list_all = []
    for each in data.loc[:, ['text']]:
        soup = BeautifulSoup(each, 'html.parser')
        list_all.append(TranscriptProcessor.get_mappings(soup))


def main():
    db = connect_to_db()

    update_company_list = False
    if update_company_list:
        dow_30_companies = Scraper.dow_30_companies_func()
        insert_companies(db, dow_30_companies)

    shiqi_cookie = 'machine_cookie=7952404426213; _gcl_au=1.1.1852272509.1569899012; _ga=GA1.2.1953747303.1569899012; _pxvid=0b7e782c-e3f8-11e9-b462-0242ac12000d; _fbp=fb.1.1569899012051.886186956; __adroll_fpc=aae1eb2e906a50f3949b56c1d8d42366-s2-1569899012247; h_px=1; __gads=ID=d63a506c6419e1b6:T=1569899012:S=ALNI_Ma0SgjcuKl1JA1c7RW99nLBYW7VRw; _gcl_aw=GCL.1570401017.Cj0KCQjwz8bsBRC6ARIsAEyNnvpAH7BQxZHxIKHWYLIWMAxz1p-SAGiDnoSVERLTrVsJIhr3fxTIqSsaApoAEALw_wcB; _gac_UA-142576245-1=1.1570401017.Cj0KCQjwz8bsBRC6ARIsAEyNnvpAH7BQxZHxIKHWYLIWMAxz1p-SAGiDnoSVERLTrVsJIhr3fxTIqSsaApoAEALw_wcB; _gid=GA1.2.1752854065.1570401017; __aaxsc=0; _pxff_tm=1; user_id=50729313; user_nick=shiqi; user_devices=; u_voc=; marketplace_author_slugs=; user_cookie_key=0; has_paid_subscription=false; user_perm=; sapu=101; user_remember_token=942f39e40b9d0a7ae094f54356c5178db00607bf; free_article=0; url_source_before_register=https%3A%2F%2Fseekingalpha.com%2Fsubscriptions; regsteps=vocation%2Cnewsletters%2Cstocks; _dc_gtm_UA-142576245-1=1; _gat_UA-142576245-1=1; portfolio_sort_type=a_z; __ar_v4=2EEQPRZIBZB7VIPPEX2IGK%3A20191005%3A1%7CULCHBRH4ZZGFXDWGQTC6RG%3A20190931%3A7%7CRFXAEISDJFDZDINVACZG6X%3A20190931%3A15%7CHWYEUMZG3RCB3IJESAMRSO%3A20190931%3A15%7CRQ5QC664UFDO7B7WUQLCWR%3A20190931%3A5%7CDZPINTYKVVC37LE5MJWGEE%3A20191005%3A1%7CF6X65CJ4K5E43AFRH5CGQD%3A20191005%3A1; gk_user_access=1**1570401330; gk_user_access_sign=ab3e4c0a5e8bfeae794ee871e69d658ad6543138; _igt=63111f8a-2cc0-4c8e-ef39-30312ad92a9f; _ig=23c932c9-df53-4b37-92ae-a42d6a8fd7e0; _hjid=1c813c65-9363-4ac1-bebb-f9ec98b3612b; _hjIncludedInSample=1; _px2=eyJ1IjoiOWFlMzZjMzAtZTg4OS0xMWU5LWIyZDYtZDE1MjkzZmI2ZTdiIiwidiI6IjBiN2U3ODJjLWUzZjgtMTFlOS1iNDYyLTAyNDJhYzEyMDAwZCIsInQiOjE1NzA0MDE4MzM0NTYsImgiOiIwYWY1MmNmMjMxMDVjODRiNGYyYmY3MWZhNWNiMTAxYzYwYzQ2MDVlOTQ0ZTI0MTExZDdhNTZjYmNiN2MyNTFlIn0=; _px=2aTrZ4YsWDPHKEWd8SkGSknh+MguaFO5v08Zp5FpuXihOKSBfCNW1kqz0Sh6TeY6yCxZB569T9wcqASHHLqO8w==:1000:ktdy5OvwfDY+9o/aY+fpibJo69IGspZ0rUiGyjqHr6mPvPrMzOeSncOiyE2DwR0n6XM9iFjSH45EQdDEtT7C7jywMISr/wig/QhIjCSIfjb6oN8OD5BSZIdDe9iOqI3pTIMDjAFBA8pN1OG2YClj7veSzw0gcFqQibMkTSUhXrlSWu0rtsixXA5AzmhXTic/oiIgfjYO+ArCArHmirIjyWPoomfbnd9sT/fxhC6Yha6uzK5qIw10BhAAUXUQU8oKhYP/hs+r3ZnuGOoyHPjtmg==; _pxde=f28ee823697b2a8c811f655dd2d1091c8bd93e6933895256adebef386d4e4bd0:eyJ0aW1lc3RhbXAiOjE1NzA0MDEzMzM0NTYsImZfa2IiOjB9; aasd=11%7C1570401017239'
    links_df = get_company_links(db, shiqi_cookie)

    link_set = get_archived_links(db)
    raw_df = Scraper.data_companies(links_df, shiqi_cookie, link_set)
    # TODO:need to add some conditional statement to decide if re-run
    rerun_df = Scraper.rerun_companies(raw_df, shiqi_cookie)

    #insert raw
    insert_raw_data(db, rerun_df, coll_name="raw")



    # dow_30_companies = Scraper.data_companies(dow_30_companies, cookies)
    # # cookies = input("Enter your cookies for the rerun ")
    # dow_30_companies.to_csv('companies.csv')
    # dow_30_companies = Scraper.rerun_companies(dow_30_companies, cookies)
    # dow_30_companies.to_csv('companies.csv')


def test():
    db = connect_to_db()

    df = pd.read_csv('data/raw_data/companies.csv',index_col='Unnamed: 0')

    insert_raw_data(db, df, coll_name="raw")


if __name__== "__main__":
    test()
    # main()
