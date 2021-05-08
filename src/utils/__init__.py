#! /home/evan/.conda/envs/rebel_manifesto/bin/python

import os
import re
import pymysql
import numpy as np
import dateparser
import pickle

__all__ = [
    'path_to_dict',
    'connect_to_db',
    'write_to_documents',
    'date_to_ymd'
    'save_object',
    'count_tokens',
    'del_list_numpy',
    'clean_string'
]


def path_to_dict(path, d):
    '''Converts a path structure to a nested dictionary'''
    name = os.path.basename(path)
    if os.path.isdir(path):
        if name not in d['dirs']:
            d['dirs'][name] = {'dirs': {}, 'files': []}
        for x in os.listdir(path):
            path_to_dict(os.path.join(path, x), d['dirs'][name])
    else:
        d['files'].append(name)
    return d


def connect_to_db(user: str, passwd: str, host='127.0.0.1',
                  db_flavor='mysql', charset='utf8'):
    """
    Creates a connection to a MySQL database using pymysql

    Parameters
    ----------
    user: string, username
    host: string, host location, defaults to 127.0.0.1
    passwd: string, user's mysql password
    db_flavor: string, database flavor to connect to, defaults to mysql

    returns tuple: (connection, cursor)
    """
    return pymysql.connect(host=host, user=user, passwd=passwd,
                           db=db_flavor, charset=charset)


def write_to_documents(conn, title: str, country: str, group_name: str,
                       doc_type: str, date, language, is_translated=None,
                       orig_text=None, trans_text=None, url=None,
                       n_tokens=None):
    """
    Writes to documents table in manifestos_db database

    Parameters
    ----------
    conn: a pymysql connection to a database
    title: string, must be 0-500 characters
    country: string, must be 0-500 characters
    group_name: string, must be 0-50 characters
    doc_type: string, must be 0-20 characters
    date: string, must be date in YYYY-MM-DD format
    language: string, must be 0-5 characters following ISO-639-1 Code format
    is_translated: int, 0/1 boolean
    orig_text: string, optional, default None
    trans_text: string, optional, default None
    url: string, optional, default None
    n_tokens: int, number of tokens in trans_text, optional, default None

    Returns
    --------
    None
    """
    cur = conn.cursor()
    cur.execute('USE manifestos_db')
    query = """
    INSERT INTO documents (title, country, group_name, doc_type, date,
    language, is_translated, orig_text, trans_text, url, n_tokens)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cur.execute(query, (title, country, group_name, doc_type, date, language,
                        is_translated, orig_text, trans_text, url, n_tokens))
    cur.connection.commit()
    cur.close()


def date_to_ymd(date_string):
    '''Takes date strings from most langauges and fuzzy translates to
    YYYY-mm-dd format'''
    return dateparser.parse(date_string).date().strftime('%Y-%m-%d')


def save_object(obj, filename):
    '''
    Saves python objects to specified filename. Will
    overwrite file
    Arguments
    ---------
    obj: python object
    filename: file path + name
    '''
    with open(filename, 'wb') as output:
        pickle.dump(obj, output, pickle.HIGHEST_PROTOCOL)


def count_tokens(str_list) -> int:
    """Counts total number of tokens in a list of strings."""
    if isinstance(str_list, str):
        count = len(str_list.split())
    elif isinstance(str_list, list):
        count = 0
        for s in str_list:
            n_tokens = len(s.split())
            count += n_tokens
    else:
        raise TypeError('str_list must be one of: string, list of strings')
    return count


def del_list_numpy(l: list, id_to_del: list) -> list:
    '''Delete items indexed by id_to_del from list l.'''
    arr = np.array(l)
    return list(np.delete(arr, id_to_del))


def clean_string(s: str):
    # Remove urls
    s = re.sub(r'https?:\/\/[^\s]+', '', s, flags=re.MULTILINE)
    s = re.sub(r' +', ' ', s)
    s = s.strip()
    return s
