import pandas as pd
from pymongo import MongoClient, ReturnDocument
from bs4 import BeautifulSoup
import logging
import os
import json
import Scraper
import TranscriptProcessor

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


def connect_to_db(database='Datapipelines'):
    client = MongoClient('localhost')
    db = client[database]
    return db


def get_raw_csv_list(raw_csv_path='data/raw_data'):
    raw_csv_ls = os.listdir(raw_csv_path)
    return [os.path.join(raw_csv_path, fn) for fn in raw_csv_ls]


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
    logger.info(f'set of archived links: {set(links)}')
    return set(links)


def get_company_list(db, coll_name="companies"):
    coll = db[coll_name]
    company_list = coll.distinct("Company")
    return company_list


def get_company_links(db, cookies):
    """
    Get links based on the tracking company list
    :param db:
    :param cookies:
    :return:
    """
    company_list = get_company_list(db)
    links_df = Scraper.get_links_2(company_list, cookies)
    return links_df


def insert_raw_data(db, df, coll_name="raw"):
    df['isProcessed'] = False
    df_json_str = df.to_json(orient='records')
    df_json = json.loads(df_json_str)
    coll = db[coll_name]
    coll.insert_many(df_json)  # can be changed to batch inserting.


def get_unprocessed_raw(db, coll_name="raw"):
    coll = db[coll_name]
    cursor = coll.find({"isProcessed": False}, {"isProcessed": 0, "link": 0})
    docs = [doc for doc in cursor]
    return docs


def process_raw_html(docs):
    raw_object_ids = []
    new_docs = []
    isProcessed_ls = []
    for doc in docs:
        raw_object_id = doc.pop('_id')
        raw_object_ids.append(raw_object_id)

        html_text = doc.pop('text', None)
        soup = BeautifulSoup(html_text, 'html.parser')
        try:
            doc_processed_list = TranscriptProcessor.get_mappings(soup)
        except Exception:
            logger.error(f"TranscriptProcessor has met an error, processing {raw_object_id} has failed!")
            isProcessed_ls.append(False)
            continue

        intro_ls = doc_processed_list[0]
        qa_ls = doc_processed_list[1]
        conclu_ls = doc_processed_list[2]

        para_id = 1 # initialize paragraph id
        for intro in intro_ls:
            doc['para_id'] = para_id
            doc['para_tpye'] = 'intro'
            doc['speaker'] = intro[0]
            doc['text'] = intro[1]
            para_id += 1
            new_docs.append(doc.copy())
            logger.debug(str(doc))

        for qa in qa_ls:
            doc['para_id'] = para_id
            doc['para_tpye'] = 'qa'
            doc['Q_A'] = qa[1]
            doc['speaker'] = qa[0]
            doc['text'] = qa[2]
            para_id += 1
            new_docs.append(doc.copy())
            logger.debug(str(doc))

        for conclu in conclu_ls:
            doc['para_id'] = para_id
            doc['para_tpye'] = 'conclu'
            doc['speaker'] = conclu[0]
            doc['text'] = conclu[1]
            para_id += 1
            new_docs.append(doc.copy())
            logger.debug(str(doc))

        del para_id
        isProcessed_ls.append(True)
        logger.info(f'{len(new_docs)} paragraphs in {raw_object_id}:')

    return new_docs, raw_object_ids, isProcessed_ls


def process_raw(db, coll_name="processed"):
    docs = get_unprocessed_raw(db)
    new_docs, raw_object_ids, isProcessed_ls = process_raw_html(docs)
    logger.info(f'length of returned new_docs: {len(new_docs)}')
    try:
        db[coll_name].insert_many(new_docs)
    except TypeError as e:
        logger.error(e)
        raise
    update_is_processed(db, raw_object_ids, isProcessed_ls)
    logger.info('Raw Documents processed!')


def update_is_processed(db, raw_object_ids, isProcessed_ls, coll_name="raw"):
    for i in range(len(raw_object_ids)):
        return_doc = db[coll_name].find_one_and_update({"_id":raw_object_ids[i]},
                                          {'$set':{"isProcessed":isProcessed_ls[i]}})


def main():
    db = connect_to_db()

    update_company_list = False
    if update_company_list:
        dow_30_companies = Scraper.dow_30_companies_func()
        insert_companies(db, dow_30_companies)

    shiqi_cookie = 'cookie: machine_cookie=7952404426213; _gcl_au=1.1.1852272509.1569899012; _ga=GA1.2.1953747303.1569899012; _pxvid=0b7e782c-e3f8-11e9-b462-0242ac12000d; _fbp=fb.1.1569899012051.886186956; __adroll_fpc=aae1eb2e906a50f3949b56c1d8d42366-s2-1569899012247; h_px=1; __gads=ID=d63a506c6419e1b6:T=1569899012:S=ALNI_Ma0SgjcuKl1JA1c7RW99nLBYW7VRw; _gcl_aw=GCL.1570401017.Cj0KCQjwz8bsBRC6ARIsAEyNnvpAH7BQxZHxIKHWYLIWMAxz1p-SAGiDnoSVERLTrVsJIhr3fxTIqSsaApoAEALw_wcB; _gac_UA-142576245-1=1.1570401017.Cj0KCQjwz8bsBRC6ARIsAEyNnvpAH7BQxZHxIKHWYLIWMAxz1p-SAGiDnoSVERLTrVsJIhr3fxTIqSsaApoAEALw_wcB; user_id=50729313; user_nick=shiqi; user_devices=; u_voc=; marketplace_author_slugs=; user_cookie_key=0; has_paid_subscription=false; user_perm=; sapu=101; user_remember_token=942f39e40b9d0a7ae094f54356c5178db00607bf; portfolio_sort_type=a_z; _hjid=1c813c65-9363-4ac1-bebb-f9ec98b3612b; _1ci_7ag23o86kjasbfd=5b6c7540-e890-11e9-8007-cb2998eae932; _MXBj_SURpRlk=0ce5de70-1086-3f4a-ba0a-429de44ad3a3; __aaxsc=0; _gid=GA1.2.1549442766.1570833290; _dc_gtm_UA-142576245-1=1; _pxff_tm=1; _igt=08fc42bf-c536-497e-f95a-8ec91ecffe08; _hjIncludedInSample=1; _px2=eyJ1IjoiNjFkODRlYTAtZWM3Ny0xMWU5LWE2ODMtZGIwYTU0MmI4Njg2IiwidiI6IjBiN2U3ODJjLWUzZjgtMTFlOS1iNDYyLTAyNDJhYzEyMDAwZCIsInQiOjE1NzA4MzM4MTM2MDksImgiOiI2MjgyMTA1Yjk2MjdhYWIzMzUxZGZkZjVlM2QxYjY5NzAyOTZkYTM1MjIwYjhiMmI2MzczYjNmYTA0ZDFlY2VlIn0=; _px=+CVau3LnmQem7QrZHv9U5UG8Ya+r6W6lZ7x2I02TOVgZPYnR2goTHwo/QMrORwiieYXFZM2KTqqUf6K3mug58w==:1000:CsltfYdeoil3Q4QwyLKvvZzKPm/DJVObri0Q840tavpPrXGDoOAQ+zrpzD/gQElMBakOsNmqeDqqxfbGanULaVpGZA6G08ISq8QRs4wpmsJLGoW2H4QU9tbgt9+CZyrYXvdHra2qFt2E3ISUZA/No5L3RPSrXzdUxIZrwS26HX8CZyZ9gDtakHbPXJokhRp4/IgAAq+TtSAewqkqsIJSK9eGm20aI0qPMU0JwSXZMOHrNTa7yosM1Vck/Y/evftmdM2jWaqpYGPA4aemWp60fQ==; _pxde=0512cdd84b54addbd59f1d9c9d59d697b3113882e1761b7072d31a190fbf4592:eyJ0aW1lc3RhbXAiOjE1NzA4MzMzMTM2MDksImZfa2IiOjB9; aasd=4%7C1570833290384; gk_user_access=1**1570833316; gk_user_access_sign=dd2d06b806b1c37ed28d8a3021cea0ece5616abb; __ar_v4=F6X65CJ4K5E43AFRH5CGQD%3A20191005%3A5%7CDZPINTYKVVC37LE5MJWGEE%3A20191005%3A1%7CRQ5QC664UFDO7B7WUQLCWR%3A20190931%3A5%7CHWYEUMZG3RCB3IJESAMRSO%3A20190931%3A21%7CRFXAEISDJFDZDINVACZG6X%3A20190931%3A21%7CULCHBRH4ZZGFXDWGQTC6RG%3A20190931%3A9%7C2EEQPRZIBZB7VIPPEX2IGK%3A20191005%3A1; _ig=23c932c9-df53-4b37-92ae-a42d6a8fd7e0'
    # get new links based on the tracking company list
    links_df = get_company_links(db, shiqi_cookie)

    # get archived links
    link_set = get_archived_links(db)

    # scrape new articles
    raw_df = Scraper.data_companies(links_df, shiqi_cookie, link_set)
    # need to add some conditional statement to decide if re-run
    # rerun_df = Scraper.rerun_companies(raw_df, shiqi_cookie)

    # insert raw
    insert_raw_data(db, raw_df, coll_name="raw")

    # process raw transcripts
    process_raw(db)


def test():

    db = connect_to_db()

    # df = pd.read_csv('data/raw_data/companies.csv',index_col='Unnamed: 0')
    #
    # insert_raw_data(db, df, coll_name="raw")
    #
    process_raw(db)


if __name__ == "__main__":
    test()
    # main()
