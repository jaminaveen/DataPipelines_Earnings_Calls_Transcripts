import pandas as pd
from pymongo import MongoClient, ReturnDocument
from bs4 import BeautifulSoup
import os
import json
import Scraper
import TranscriptProcessor

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


def connect_to_db(database='Datapipelines'):
    """
    Connect to MongoDB database
    :param database: name of the database
    :return: db client
    """
    client = MongoClient('localhost')
    db = client[database]
    return db


def get_raw_csv_list(raw_csv_path='data/raw_data'):
    """
    !!!DEPRECATED!!!
    List all csv files of raw data in the folder.
    :param raw_csv_path: string, relative path
    :return: list of file paths
    """
    raw_csv_ls = os.listdir(raw_csv_path)
    return [os.path.join(raw_csv_path, fn) for fn in raw_csv_ls]


def insert_companies(db, com_df, coll_name="companies"):
    """
    insert the company list as meta data.
    :param coll_name: String, collection name
    :param db: db client
    :param com_df: result dataframe of Scraper.dow_30_companies_func()
    """
    com_json_str = com_df.to_json(orient='records')
    com_json = json.loads(com_json_str)
    coll = db[coll_name]
    coll.insert_many(com_json)


def get_archived_links(db, coll_name="raw"):
    """
    from raw collection, get links we have and return as a set.
    :param coll_name: String, collection name
    :param db: db client
    """
    coll = db[coll_name]
    links = coll.distinct("link")
    logger.info(f'set of archived links: {set(links)}')
    return set(links)


def get_company_symbol_list(db, coll_name="companies"):
    """
    from "companies" collection, get values of "Symbol"
    :param db: db client
    :param coll_name: String, collection name
    :return: list of String.
    """
    coll = db[coll_name]
    company_list = coll.distinct("Symbol")
    logger.info(f'Get {len(company_list)} companies from the collection')
    return company_list


def get_company_links(db, cookies):
    """
    Get links based on the tracking company list
    :param db: db client
    :param cookies: String, http header cookie. Read from local txt file.
    :return: dataframe, links_df from "Scraper.get_links_2(company_list, cookies)"
    """
    company_list = get_company_symbol_list(db)
    links_df = Scraper.get_links_2(company_list, cookies)
    return links_df


def insert_raw_data(db, df, coll_name="raw"):
    """
    insert the result(dataframe) from data scraping into database
    :param db: db client
    :param df: dataframe, from "Scraper.data_companies(links_df, cookie, link_set)"
    :param coll_name: String, collection name
    """
    df['isProcessed'] = False
    df_json_str = df.to_json(orient='records')
    df_json = json.loads(df_json_str)
    coll = db[coll_name]
    coll.insert_many(df_json)  # can be changed to batch inserting.


def get_unprocessed_raw(db, coll_name="raw"):
    """
    query the database to get unprocessed raw data documents(Mongo Document)
    :param db: db client
    :param coll_name: String, collection name
    :return: List of Mongo Documents
    """
    coll = db[coll_name]
    cursor = coll.find({"isProcessed": False}, {"isProcessed": 0, "link": 0})
    docs = [doc for doc in cursor]
    return docs


def process_raw_html(docs):
    """
    process on mongo documents, mapping raw html to different types of information fragments
    :param docs: List of Mongo Documents
    :return: list of new documents, list of raw_object_ids, list of booleans
    """
    raw_object_ids = []
    new_docs = []
    isProcessed_ls = []
    for doc in docs:
        last_len = len(new_docs)
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
        logger.info(f'{len(new_docs) - last_len} paragraphs in {raw_object_id}:')

    return new_docs, raw_object_ids, isProcessed_ls


def process_raw(db, coll_name="processed"):
    """
    procuders to process raw transcripts and insert to db after processing
    :param db: db client
    :param coll_name: String, collection name
    """
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
    """
    update document status in "raw" collection to indicate if one document has been processed.
    :param db: db client
    :param raw_object_ids: list of raw_object_ids
    :param isProcessed_ls: list of booleans
    :param coll_name: String, collection name
    """
    for i in range(len(raw_object_ids)):
        return_doc = db[coll_name].find_one_and_update({"_id":raw_object_ids[i]},
                                          {'$set':{"isProcessed":isProcessed_ls[i]}})


def main():
    db = connect_to_db()

    update_company_list = True
    if update_company_list:
        dow_30_companies = Scraper.dow_30_companies_func()
        insert_companies(db, dow_30_companies)

    with open('cookies.txt','r') as f:
        cookie = f.readline()
        logger.info(f'Using cookie: {cookie}')
    # get new links based on the tracking company list
    links_df = get_company_links(db, cookie)

    # get archived links
    link_set = get_archived_links(db)

    raw_df = Scraper.data_companies(links_df, cookie, link_set)

    count = 0
    while (not Scraper.is_raw_df_all_good(raw_df)) and count <= 3: # try no more than 3 times.
        raw_df = Scraper.rerun_companies(raw_df, cookie)
        count += 1
        logger.info(f'rerun count: {count}')

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
    # test()
    main()
