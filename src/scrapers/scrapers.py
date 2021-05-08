from requests import Session
from google.cloud import translate as gc_trans
from google.oauth2 import service_account
from bs4 import BeautifulSoup
from ..utils import (
    write_to_documents,
    date_to_ymd,
    count_tokens,
    del_list_numpy,
    clean_string
)
from dateutil.parser import parse
from google_trans_new import google_translator

from pathlib import Path
from io import StringIO
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage

import requests
import os
import string
import re
import time
import pickle
import itertools
import logging

from six import with_metaclass
from abc import ABCMeta, abstractmethod


class Scraper(with_metaclass(ABCMeta, object)):
    """
    Base scraper template

    Parameters
    ----------
    base_dir: str
        Base file directory where group manifestos are written to

    urls: dict
        dictionary of URLs needed for scraping

    headers: dict
        dictionary of headers for 'POST', 'GET' methods, 'GET' required.

    pickle_file: str
        file path where pickled jobs file will be saved.

    verify: bool or str, optional, default=True
        A Boolean or a String indication to verify the
        servers TLS certificate or not.

    data: dict, optional, default=None
        header data dictionary.

    export_to_txt: bool, optional, default=False
        Whether extracted texts should be written to a txt
        file in the base directory.

    translate: bool, optional, default=False
        Whether google cloud translator should be used to
        translate non-English documents
        to English.
    """
    def __init__(self, base_dir, urls, headers, pickle_file, verify=True,
                 data=None, export_to_txt=False, translate=False,
                 credentials=None):
        # Meta data
        self.base_dir = base_dir
        self.urls = urls
        self.headers = headers
        self.pkl_file = pickle_file
        self.verify = verify
        self.data = data
        self.export_to_txt = export_to_txt
        self.translate = translate

        # Instantiate tools
        if self.translate:
            assert credentials is not None, \
                "Please provide Google Cloud service credentials."
            gc_creds = service_account.Credentials.from_service_account_file(
                credentials
            )
            self.parent = f'projects/{gc_creds.project_id}'
            self.translator = gc_trans.TranslationServiceClient()
        else:
            self.translator = google_translator(timeout=5)
        self.session = Session()
        self.punct_table = str.maketrans('', '', string.punctuation)
        self.space_table = str.maketrans('', '', string.whitespace)

        # Instantiate jobs
        self.jobs = []

    def soupify(self, url: str) -> BeautifulSoup:
        """Takes in url and returns soup object"""
        page = requests.get(
            url=url,
            headers=self.headers['GET'],
            verify=self.verify
        )
        html = page.text
        soup = BeautifulSoup(html, 'html.parser')
        return(soup)

    def translate_to_english(self, contents: list):
        """
        Takes a list of text and translates it into english
        using google cloud translator api.
        """
        response = self.translator.translate_text(
            request={
                "parent": self.parent,
                "contents": contents,
                "mime_type": "text/plain",
                "target_language_code": 'en-US'
            }
            )
        return response

    def write_file(self, job: dict):
        """
        Write list of paragraphs to txt file
        """
        title = job['title'].translate(
            self.punct_table).translate(self.space_table).title()
        file_name = title + '_' + job['doc_type'] + '_' + job['date'] + '.txt'
        path_to_file = os.path.join(self.base_dir, file_name)

        text = job['trans_text'] if job['trans_text'] is not None \
            else job['orig_text']
        with open(path_to_file, 'w') as f:
            f.writelines(text)

    def write_pdf(self, job: dict, convert_to_txt=False):
        """
        Download PDF file from url

        job: dict
            A scraping job

        convert: bool
            Whether to convert PDF to text string and return
        """
        title = job['title'].translate(
            self.punct_table).translate(self.space_table).title()
        file_name = title + '_' + job['doc_type'] + '_' + job['date'] + '.pdf'
        path_to_file = os.path.join(self.base_dir, file_name)
        filename = Path(path_to_file)

        # Get PDF and Save
        response = requests.get(
            url=job['url'],
            headers=self.headers['GET'],
            verify=self.verify
        )
        filename.write_bytes(response.content)

        if convert_to_txt:
            return convert(path_to_file)

    def delete_pdf(self, job: dict):
        """
        Delete PDF file
        """
        title = job['title'].translate(
            self.punct_table).translate(self.space_table).title()
        file_name = title + '_' + job['doc_type'] + '_' + job['date'] + '.pdf'
        path_to_file = os.path.join(self.base_dir, file_name)
        filename = Path(path_to_file)

        try:
            filename.unlink()
        except Exception:
            logging.exception("Exception occured")

    def save_jobs(self):
        """Pickles jobs to self.pkl_file"""
        logging.info('Saving jobs...')
        with open(self.pkl_file, 'wb') as output:
            pickle.dump(self.jobs, output, pickle.HIGHEST_PROTOCOL)

    @abstractmethod
    def get_jobs(self):
        """
        Get urls, titles, dates to scrape from urls in url dictionary
        """
        pass

    @abstractmethod
    def download_statements(self, connection, save: bool):
        """
        Takes url for statement page, extracts text, and
        saves it to pickle file location specific during instantiation.

        Parameters
        ----------
        connection: pymysql database connection object
            A connection to a database for storing scraping results.
            Currently, only works with MySQL databases and a pymysql interface.
        save: bool
            Whether to save jobs or not.
        """
        pass


class SudanJEMScraper(Scraper):
    """Scraper for Sudan - JEM group."""
    def __init__(self, base_dir, urls, headers, pickle_file, verify=True,
                 data=None, export_to_txt=False, translate=False,
                 credentials=False):
        super().__init__(base_dir, urls, headers, pickle_file,
                         verify, data, export_to_txt, translate, credentials)

    def get_jobs(self, save=True):
        '''
        Iterate through API article calls to get article title, link, and date
        '''
        logging.info('Getting jobs...')
        next_page = True
        while next_page:
            # Make API call
            request = self.session.post(
                url=self.urls['api'],
                data=self.data,
                headers=self.headers['POST']
            )

            # Extract title, date, and url
            html = request.json()['content']
            soup = BeautifulSoup(html, 'html.parser')
            posts = soup.find_all('div', class_='jeg_postblock_content')

            for post in posts:
                job = {}

                title_link_node = post.find('h3', class_='jeg_post_title')

                # Title
                title = title_link_node.get_text()
                job['title'] = title.title().encode('ascii', 'ignore').decode()

                # Links
                job['url'] = title_link_node.find('a')['href']

                # Date
                date = post.find('div', class_='jeg_meta_date').get_text()
                date = self.translator.translate(date,
                                                 lang_src='ar',
                                                 lang_tgt='en').strip()
                date = parse(date, fuzzy=True).strftime('%Y-%m-%d')
                job['date'] = date

                self.jobs.append(job)

                job_print(job=job)

            # update next_page
            next_page = request.json()['next']

            # Update page number
            self.data['data[current_page]'] += 1

        # Save
        if save:
            self.save_jobs()

    def download_statements(self, connection, save: bool):
        assert len(self.jobs) > 0, \
            "There are no jobs in queue. Must use get_jobs() first."
        jobs = self.jobs
        failed_jobs = []

        for i, job in enumerate(jobs):
            # Download statement
            dl_print(i=i, job=job)

            try:
                # Get original text
                # TODO: may be able to abstract this by passing splitting up
                # soup.find(...).get_text(separator=' ') into two lines
                # and passing dictionary to soup.find()
                soup = self.soupify(url=job['url'])
                s = soup.find('div',
                              class_='content-inner').get_text(separator=' ')
                job['orig_text'] = clean_string(s=s)

                # Count tokens
                job['n_tokens'] = count_tokens(job['orig_text'])

                # Write file to txt
                if self.export_to_txt:
                    self.write_file(job=job)

                # Write file to database
                write_to_documents(
                    conn=connection,
                    title=job['title'],
                    country='Sudan',
                    group_name='JEM',
                    doc_type='statement',
                    date=job['date'],
                    language='en',
                    is_translated=0,
                    orig_text=job['orig_text'],
                    url=job['url'],
                    n_tokens=job['n_tokens']
                )
                logging.info('...complete')
            except Exception:
                logging.exception("Exception occured")
                logging.info('...Download failed!')
                failed_jobs.append(i)
                continue

        # Remove failed jobs
        if len(failed_jobs) > 0:
            jobs = del_list_numpy(jobs, failed_jobs)

        # Save
        self.jobs = jobs
        if save:
            self.save_jobs()


class CanadaPQScraper(Scraper):
    """Scraper for Canada - PQ group."""
    def __init__(self, base_dir, urls, headers, pickle_file, verify=True,
                 data=None, export_to_txt=False, translate=False,
                 credentials=None):
        super().__init__(base_dir, urls, headers, pickle_file, verify,
                         data, export_to_txt, translate, credentials)

    def get_posts(self, soup, key: str):
        # TODO: Might be better off separate each
        # scraper instance into its own file and
        # adding this as a function, not a class method
        """
        Gets posts for given URL key

        Parameters
        ----------
        soup: beautiful soup object
            For blog or news listing page

        key: str
            Must be one of 'news' or 'blogs'

        Returns
        -------
        Parsed HTML beatiful soup object for news or blog posts.
        """
        assert key in ['news', 'blog'], \
            "Given key not supported. Must be one of 'news' or 'blog'"
        methods = {
            'news': soup.find_all('a',
                                  class_='elementor-post__thumbnail__link'),
            'blog': soup.find_all('header', class_='entry-header')
        }
        return methods[key]

    def has_next_page(self, soup, key: str) -> bool:
        """
        Gets next page condition from given page

        Parameters
        ----------
        soup: beautiful soup object
            For blog or news listing page

        key: str
            Must be one of 'news' or 'blog'

        Returns
        -------
        bool indicating whether next page is found.
        """
        assert key in ['news', 'blog'], \
            "Given key not supported. Must be one of 'news' or 'blog'"
        methods = {
            'news': soup.find('a', class_='next page-numbers'),
            'blog': soup.find('div', class_='right')
        }
        return methods[key] is not None

    def get_job(self, post, key: str):
        """
        Get scraping job from post

        Parameters
        ----------
        post: beautiful soup html object
            HTML for a post to be added to jobs queue

        key: str
            Must be one of 'news' or 'blog'

        Returns
        -------
        job: dict,
            A dictionary containing information for a scraping task
        """
        assert key in ['news', 'blog'], \
            "Given key not supported. Must be one of 'news' or 'blog'"
        if key == 'news':
            title = post.find(
                'h3',
                class_='elementor-post__title'
                ).get_text(strip=True)
            link = post['href']
            date = post.find(
                'span',
                class_='elementor-post-date'
                ).get_text(strip=True)
            date = date.translate(self.punct_table).title()
            doc_type = 'news'
        else:
            title = post.find('a').get_text(strip=True)
            link = post.find('a')['href']
            date = post.find('div', class_='blog_date').get_text()
            doc_type = 'statement'

        title = title.encode('ascii', 'ignore').decode()
        if self.translate:
            response = self.translate_to_english(contents=[title])
            title = response.translations[0].translated_text
        date = date_to_ymd(date)

        job = {
            'title': title,
            'url': link,
            'date': date,
            'doc_type': doc_type
        }
        return job

    def get_jobs(self, save=True):
        """
        Iterate through URL dictionary URLs and extract
        job titles, dates, and links.
        """
        logging.info('Getting jobs...')
        for key in self.urls.keys():
            i = 1
            next_page = True
            while next_page:
                # Load page
                soup = self.soupify(url=self.urls[key] + str(i))
                posts = self.get_posts(soup=soup, key=key)

                for post in posts:
                    job = self.get_job(post=post, key=key)
                    self.jobs.append(job)
                    job_print(job=job)

                # update next_page and i
                next_page = self.has_next_page(soup=soup, key=key)
                i += 1
        # Save
        if save:
            self.save_jobs()

    def get_paragraphs(self, soup, job) -> list:
        """
        Gets relevant paragraphs from document HTML

        Parameters
        ----------
        soup: beautiful soup object

        Returns
        -------
        List of strings
        """
        assert job['doc_type'] in ['news', 'statement'], \
            "Given doc_type not supported. Must be 'news' or 'statement'"
        methods = {
            'news': {'id': 'nouvelle-box'},
            'statement': {'id': 'boite_contenu'}
        }
        id_tag = methods[job['doc_type']]
        s = [p.get_text(strip=True) for p
             in soup.find('div', id_tag).find_all('p')]
        paragraphs = [clean_string(p) for p in s if p]
        return paragraphs

    def download_statements(self, connection, save: bool):
        assert len(self.jobs) > 0, \
            "There are no jobs in queue. Must use get_jobs() first."
        jobs = self.jobs
        failed_jobs = []

        for i, job in enumerate(jobs):
            # Download statement
            dl_print(i=i, job=job)
            try:
                # Get original text
                soup = self.soupify(url=job['url'])
                paragraphs = self.get_paragraphs(soup=soup, job=job)
                job['orig_text'] = '\n\n'.join(paragraphs)

                # Count tokens
                job['n_tokens'] = count_tokens(job['orig_text'])

                # Translate
                job['is_translated'] = 0
                job['trans_text'] = None
                if self.translate is True:
                    response = self.translate_to_english(contents=paragraphs)
                    paragraphs = [t.translated_text for t
                                  in response.translations]
                    job['trans_text'] = '\n\n'.join(paragraphs)
                    job['is_translated'] = 1

                # Write to text
                if self.export_to_txt:
                    self.write_file(job=job)

                # Write to database
                write_to_documents(
                    conn=connection,
                    title=job['title'],
                    country='Canada',
                    group_name='PQ',
                    doc_type=job['doc_type'],
                    date=job['date'],
                    language='fr',
                    is_translated=job['is_translated'],
                    orig_text=job['orig_text'],
                    trans_text=job['trans_text'],
                    url=job['url'],
                    n_tokens=job['n_tokens']
                )
                logging.info('...complete')
            except Exception:
                logging.exception("Exception occured")
                logging.info('Download Failed.')
                failed_jobs.append(i)
                continue

            # Slow down crawl a bit
            time.sleep(1)

        # Remove failed jobs
        if len(failed_jobs) > 0:
            jobs = del_list_numpy(jobs, failed_jobs)

        # Save
        self.jobs = jobs
        if save:
            self.save_jobs()


class MoldovaPMRScraper(Scraper):
    """Scraper for Moldova - PMR group."""
    def __init__(self, base_dir, urls, headers, pickle_file, verify=True,
                 data=None, export_to_txt=False, translate=False,
                 credentials=False):
        super().__init__(base_dir, urls, headers, pickle_file,
                         verify, data, export_to_txt, translate, credentials)

    def get_posts(self, soup, key: str):
        """
        Gets posts for given URL key

        Parameters
        ----------
        soup: beautiful soup object
            For blog or news listing page

        key: str
            Must be 'statement'

        Returns
        -------
        Parsed HTML beatiful soup object for news or blog posts.
        """
        assert key in ['statements'], \
            "Given key not supported. Must be 'statements'"
        methods = {
            'statements': {
                'posts': [t.find('a') for t
                          in soup.find_all('h1', class_='node-title')],
                'dates': [m.get_text() for m
                          in soup.find_all('div', class_='nodelist-date')]
            }
        }
        return zip(methods[key]['posts'], methods[key]['dates'])

    def has_next_page(self, soup, key: str) -> bool:
        """
        Gets next page condition from given page

        Parameters
        ----------
        soup: beautiful soup object
            For blog or news listing page

        key: str
            Must be 'statement'

        Returns
        -------
        bool indicating whether next page is found.
        """
        assert key in ['statements'], \
            "Given key not supported. Must be 'statements'"
        methods = {
            'statements': soup.find('li', class_='pager-next even')
        }
        return methods[key] is not None

    def get_jobs(self, save=True):
        '''
        Iterate through API article calls to get article title, link, and date
        '''
        logging.info('Getting jobs...')
        for key in self.urls.keys():
            i = 0
            next_page = True
            while next_page:
                # Load page
                soup = self.soupify(url=self.urls[key] + str(i))
                posts = self.get_posts(soup=soup, key=key)

                for post, d in posts:
                    if key == 'statements':
                        link = 'http://mfa-pmr.org' + post['href']
                        title = post.get_text()
                        date = d
                        doc_type = 'statement'

                    title = title.encode('ascii', 'ignore').decode()
                    if self.translate:
                        response = self.translate_to_english(contents=[title])
                        title = response.translations[0].translated_text
                    date = date_to_ymd(date)

                    job = {
                        'title': title,
                        'url': link,
                        'date': date,
                        'doc_type': doc_type
                    }

                    self.jobs.append(job)

                    job_print(job=job)

                # update next_page and i
                next_page = self.has_next_page(soup=soup, key=key)
                i += 1

        # Save
        if save:
            self.save_jobs()

    def get_paragraphs(self, soup, job) -> list:
        """
        Gets relevant paragraphs from document HTML

        Parameters
        ----------
        soup: beautiful soup object

        Returns
        -------
        List of strings
        """
        assert job['doc_type'] in ['statement'], \
            "Given doc_type not supported. Must be 'statement'"
        methods = {
            'statement': [p.get_text()
                          for p in soup.find(
                              'div', class_='field-item even'
                              ).find_all('p')]
        }
        s = methods[job['doc_type']]
        paragraphs = [clean_string(p) for p in s if p]
        return paragraphs

    def download_statements(self, connection, save: bool):
        assert len(self.jobs) > 0, \
            "There are no jobs in queue. Must use get_jobs() first."
        jobs = self.jobs
        failed_jobs = []

        for i, job in enumerate(jobs):
            # Download statement
            dl_print(i=i, job=job)
            try:
                # Get original text
                soup = self.soupify(url=job['url'])
                paragraphs = self.get_paragraphs(soup=soup, job=job)
                job['orig_text'] = '\n\n'.join(paragraphs)

                # Count tokens
                job['n_tokens'] = count_tokens(job['orig_text'])

                # Translate
                job['is_translated'] = 0
                job['trans_text'] = None
                if self.translate:
                    response = self.translate_to_english(contents=paragraphs)
                    paragraphs = [t.translated_text for t
                                  in response.translations]
                    job['trans_text'] = '\n\n'.join(paragraphs)
                    job['is_translated'] = 1

                # Write to text
                if self.export_to_txt:
                    self.write_file(job=job)

                # Write to database
                write_to_documents(
                    conn=connection,
                    title=job['title'],
                    country='Moldova',
                    group_name='PMR',
                    doc_type=job['doc_type'],
                    date=job['date'],
                    language='en',
                    is_translated=job['is_translated'],
                    orig_text=job['orig_text'],
                    trans_text=job['trans_text'],
                    url=job['url'],
                    n_tokens=job['n_tokens']
                )
                logging.info('...complete')
            except Exception:
                logging.exception("Exception occured")
                logging.info('Download Failed.')
                failed_jobs.append(i)
                continue

            # Slow down crawl a bit
            time.sleep(1)

        # Remove failed jobs
        if len(failed_jobs) > 0:
            jobs = del_list_numpy(jobs, failed_jobs)

        # Save
        self.jobs = jobs
        if save:
            self.save_jobs()


class EcuadorCONAIEScraper(Scraper):
    """Scraper for Ecuador - CONAIE group"""
    def __init__(self, base_dir, urls, headers, pickle_file, verify=True,
                 data=None, export_to_txt=False, translate=False,
                 credentials=None):
        super().__init__(base_dir, urls, headers, pickle_file, verify,
                         data, export_to_txt, translate, credentials)

    def get_posts(self, soup, key: str):
        """
        Gets posts for given URL key

        Parameters
        ----------
        soup: beautiful soup object
            For blog or news listing page

        key: str
            Must be one of 'news' or 'statements'

        Returns
        -------
        Parsed HTML beatiful soup object for news or blog posts.
        """
        assert key in ['news', 'statements'], \
            "Given key not supported. Must be one of 'news' or 'statements'"
        methods = {
            'news': soup.find_all('div', class_='entry-meta post-info'),
            'statements': soup.find_all('div', class_='entry-meta post-info')
        }
        return methods[key]

    def has_next_page(self, soup, key: str) -> bool:
        """
        Gets next page condition from given page

        Parameters
        ----------
        soup: beautiful soup object
            For blog or news listing page

        key: str
            Must be one of 'news' or 'statements'

        Returns
        -------
        bool indicating whether next page is found.
        """
        assert key in ['news', 'statements'], \
            "Given key not supported. Must be one of 'news' or 'statements'"
        methods = {
            'news': soup.find('a', class_='next page-numbers'),
            'statements': soup.find('a', class_='next page-numbers')
        }
        return methods[key] is not None

    def get_job(self, post, key: str):
        """
        Get scraping job from post

        Parameters
        ----------
        post: beautiful soup html object
            HTML for a post to be added to jobs queue

        key: str
            Must be one of 'news' or 'statements'

        Returns
        -------
        job: dict,
            A dictionary containing information for a scraping task
        """
        assert key in ['news', 'statements'], \
            "Given key not supported. Must be one of 'news' or 'statements'"
        if key in ['news', 'statements']:
            title = post.find('a').get_text(strip=True)
            link = post.find('a')['href']
            class_regex = re.compile('entry-date published*')
            date = post.find('time', class_=class_regex)['datetime']
        doc_type = 'news' if key == 'news' else 'statement'

        title = title.encode('ascii', 'ignore').decode()
        if self.translate:
            response = self.translate_to_english(contents=[title])
            title = response.translations[0].translated_text
        date = date_to_ymd(date)

        job = {
            'title': title,
            'url': link,
            'date': date,
            'doc_type': doc_type
        }
        return job

    def get_jobs(self, save=True):
        """
        Iterate through URL dictionary URLs and
        extract job titles, dates, and links.
        """
        logging.info('Getting jobs...')
        for key in self.urls.keys():
            i = 1
            next_page = True
            while next_page:
                # Load page
                soup = self.soupify(url=self.urls[key] + str(i))
                posts = self.get_posts(soup=soup, key=key)

                for post in posts:
                    job = self.get_job(post=post, key=key)
                    self.jobs.append(job)
                    job_print(job=job)

                # update next_page and i
                next_page = self.has_next_page(soup=soup, key=key)
                i += 1
        if save:
            self.save_jobs()

    def get_paragraphs(self, soup, job) -> list:
        """
        Gets relevant paragraphs from document HTML

        Parameters
        ----------
        soup: beautiful soup object

        Returns
        -------
        List of strings
        """
        assert job['doc_type'] in ['news', 'statement'], \
            "Given doc_type not supported. Must be 'news' or 'statement'"
        methods = {
            'news': soup.find('div', class_='entry-content').text,
            'statement': soup.find('div', class_='entry-content').text
        }
        s = methods[job['doc_type']]
        paragraphs = [clean_string(p) + '.' for p in s.split('.')]
        return paragraphs

    def download_statements(self, connection, save: bool):
        assert len(self.jobs) > 0, \
            "There are no jobs in queue. Must use get_jobs() first."
        jobs = self.jobs
        failed_jobs = []

        for i, job in enumerate(jobs):
            # Download statement
            dl_print(i=i, job=job)
            try:
                # Get original text
                soup = self.soupify(url=job['url'])
                paragraphs = self.get_paragraphs(soup=soup, job=job)
                job['orig_text'] = '\n\n'.join(paragraphs)

                # Count tokens
                job['n_tokens'] = count_tokens(job['orig_text'])

                # Translate
                job['is_translated'] = 0
                job['trans_text'] = None
                if self.translate:
                    response = self.translate_to_english(contents=paragraphs)
                    paragraphs = [t.translated_text for t
                                  in response.translations]
                    job['trans_text'] = '\n\n'.join(paragraphs)
                    job['is_translated'] = 1

                # Write to text
                if self.export_to_txt:
                    self.write_file(job=job)

                # Write to database
                write_to_documents(
                    conn=connection,
                    title=job['title'],
                    country='Ecuador',
                    group_name='CONAIE',
                    doc_type=job['doc_type'],
                    date=job['date'],
                    language='es',
                    is_translated=job['is_translated'],
                    orig_text=job['orig_text'],
                    trans_text=job['trans_text'],
                    url=job['url'],
                    n_tokens=job['n_tokens']
                )
                logging.info('...complete')
            except Exception:
                logging.exception("Exception occured")
                logging.info('Download Failed.')
                failed_jobs.append(i)
                continue

            # Slow down crawl a bit
            time.sleep(1)

        # Remove failed jobs
        if len(failed_jobs) > 0:
            jobs = del_list_numpy(jobs, failed_jobs)

        # Save
        self.jobs = jobs
        if save:
            self.save_jobs()


class IsraelPFLPScraper(Scraper):
    """Scraper for Israel - PFLP group"""
    def __init__(self, base_dir, urls, headers, pickle_file, verify=True,
                 data=None, export_to_txt=False, translate=False,
                 credentials=None):
        super().__init__(base_dir, urls, headers, pickle_file, verify,
                         data, export_to_txt, translate, credentials)

    def get_posts(self, soup, key: str):
        """
        Gets posts for given URL key

        Parameters
        ----------
        soup: beautiful soup object
            For blog or news listing page

        key: str
            Must be 'statements'

        Returns
        -------
        Zipped tuple of post list and dates list
        """
        assert key in ['statements'], \
            "Given key not supported. Must be 'statements'"

        # get dates
        if key == 'statements':
            months = [m.get_text() for m
                      in soup.find_all('span', class_='month')]
            days = [d.get_text() for d
                    in soup.find_all('span', class_='day')]
            years = [y.get_text() for y
                     in soup.find_all('span', class_='year')]
        methods = {
            'statements': {
                'posts': soup.find_all('a', class_='entry-title'),
                'dates': ['-'.join(x) for x in zip(years, months, days)]
            }
        }
        return zip(methods[key]['posts'], methods[key]['dates'])

    def has_next_page(self, soup, key: str) -> bool:
        """
        Gets next page condition from given page

        Parameters
        ----------
        soup: beautiful soup object
            For blog or news listing page

        key: str
            Must be 'statement'

        Returns
        -------
        bool indicating whether next page is found.
        """
        assert key in ['statements'], \
            "Given key not supported. Must be 'statements'"
        methods = {
            'statements': soup.find(
                'span', class_='previous-entries').find('a')
        }
        return methods[key] is not None

    def get_jobs(self, save=True):
        '''
        Iterate through API article calls to get article title, link, and date
        '''
        logging.info('Getting jobs...')
        for key in self.urls.keys():
            i = 1
            next_page = True
            while next_page:
                # Load page
                soup = self.soupify(url=self.urls[key] + str(i))
                posts = self.get_posts(soup=soup, key=key)

                for post, d in posts:
                    if key == 'statements':
                        link = post['href']
                        title = post.get_text()
                        date = d
                        doc_type = 'statement'
                    else:
                        ValueError(
                            "Given key not supported. Must be 'statements'"
                            )

                    title = title.encode('ascii', 'ignore').decode()
                    if self.translate:
                        response = self.translate_to_english(contents=[title])
                        title = response.translations[0].translated_text
                    date = date_to_ymd(date)

                    job = {
                        'title': title,
                        'url': link,
                        'date': date,
                        'doc_type': doc_type
                    }

                    self.jobs.append(job)

                    job_print(job=job)

                # update next_page and i
                next_page = self.has_next_page(soup=soup, key=key)
                i += 1
                time.sleep(6)

        # Save
        if save:
            self.save_jobs()

    def get_paragraphs(self, soup, job) -> list:
        """
        Gets relevant paragraphs from document HTML

        Parameters
        ----------
        soup: beautiful soup object

        Returns
        -------
        List of strings
        """
        assert job['doc_type'] in ['statement'], \
            "Given doc_type not supported. Must be 'statement'"
        methods = {
            'statement': soup.find('div', class_='entry').get_text()
        }
        s = methods[job['doc_type']]
        paragraphs = clean_string(s)
        return paragraphs

    def download_statements(self, connection, save: bool):
        assert len(self.jobs) > 0, \
            "There are no jobs in queue. Must use get_jobs() first."
        jobs = self.jobs
        failed_jobs = []

        for i, job in enumerate(jobs):
            # Download statement
            dl_print(i=i, job=job)
            try:
                # Get original text
                soup = self.soupify(url=job['url'])
                paragraphs = self.get_paragraphs(soup=soup, job=job)
                job['orig_text'] = paragraphs

                # Count tokens
                job['n_tokens'] = count_tokens(job['orig_text'])

                # Translate
                job['is_translated'] = 0
                job['trans_text'] = None
                if self.translate:
                    response = self.translate_to_english(contents=paragraphs)
                    paragraphs = [t.translated_text for t
                                  in response.translations]
                    job['trans_text'] = '\n\n'.join(paragraphs)
                    job['is_translated'] = 1

                # Write to text
                if self.export_to_txt:
                    self.write_file(job=job)

                # Write to database
                write_to_documents(
                    conn=connection,
                    title=job['title'],
                    country='Israel',
                    group_name='PFLP',
                    doc_type=job['doc_type'],
                    date=job['date'],
                    language='en',
                    is_translated=job['is_translated'],
                    orig_text=job['orig_text'],
                    trans_text=job['trans_text'],
                    url=job['url'],
                    n_tokens=job['n_tokens']
                )
                logging.info('...complete')
            except Exception:
                logging.exception("Exception occured")
                logging.info('Download Failed.')
                failed_jobs.append(i)
                continue

            # Slow down crawl a bit
            time.sleep(1)

        # Remove failed jobs
        if len(failed_jobs) > 0:
            jobs = del_list_numpy(jobs, failed_jobs)

        # Save
        self.jobs = jobs
        if save:
            self.save_jobs()


class IndiaCPIMaoistScraper(Scraper):
    """Scraper for India - CPI-Maoist group"""
    def __init__(self, base_dir, urls, headers, pickle_file, verify=True,
                 data=None, export_to_txt=False, translate=False,
                 credentials=None):
        super().__init__(base_dir, urls, headers, pickle_file, verify,
                         data, export_to_txt, translate, credentials)

    def get_jobs(self, save=True):
        logging.info('Getting jobs...')
        soup = self.soupify(url=self.urls['statements'])

        # Extract statement link nodes
        re_text = re.compile(r"^(English: PDF format|PDF format|PDF Pamphlet)")
        statement_tags = soup.find_all('a', href=True, text=re_text)

        # Filter links to statements and pamphlets
        links = [h['href'] for h in statement_tags
                 if not re.search(r'(^Cadre|^Nepal|Book)', h['href'])]
        links.remove('UrbanPerspective.pdf')

        # Get document type
        find = re.compile(r'(.*?)(?=[-/])')
        doc_type = [re.search(find, l).group() for l in links]
        doc_type = [re.sub(r's$', '', x) for x in doc_type]

        # Get dates
        # TODO could get more precise with regex, still have some NA
        find = re.compile(r'\d{6,8}')
        dates = [re.search(find, l).group()
                 if re.search(find, l) is not None
                 else None
                 for l in links]
        days = [fix_days(re.search(r'\d{2}$', d).group())
                if d is not None
                else None
                for d in dates]
        months = [re.search(r'\d{4}$', d).group()[0:2]
                  if d is not None
                  else None
                  for d in dates]
        years = [re.search(r'\d+', d).group()[0:-4]
                 if d is not None
                 else None
                 for d in dates]
        years = ['20' + y
                 if y is not None and len(y) == 2
                 else y
                 for y in years]
        date_strings = ['-'.join(x)
                        if x.count(None) != len(x)
                        else 'None'
                        for x in itertools.zip_longest(years, months, days)]
        dates = [date_to_ymd(d) if d != 'None' else None
                 for d in date_strings]

        # Get titles
        title_splits = [s.split('-') for s in links]
        outer_l = []
        for sublist in title_splits:
            inner_l = []
            for s in sublist:
                x = s.split('/')
                x = [re.sub(r'(^\d{4,6}|^[A-Z]{2,3}|Eng|\.pdf)', '', s) for s
                     in x]
                inner_l.extend(x)
            outer_l.append(inner_l[1:])
        titles = [' '.join(l) for l in outer_l]

        # Convert into list of jobs
        for title, date, link, d_type in zip(titles, dates, links, doc_type):
            job = {
                'title': title,
                'url': self.urls['statements'] + link,
                'date': date,
                'doc_type': d_type
            }
            self.jobs.append(job)
            job_print(job=job)

        if save:
            self.save_jobs()

    def download_statements(self, connection, save: bool):
        assert len(self.jobs) > 0, \
            "There are no jobs in queue. Must use get_jobs() first."
        jobs = self.jobs
        failed_jobs = []

        for i, job in enumerate(jobs):
            # Download statement
            dl_print(i=i, job=job)

            try:
                # Download PDF and convert to text string
                text = self.write_pdf(job=job, convert_to_txt=True)
                job['orig_text'] = text

                # Count tokens
                job['n_tokens'] = count_tokens(job['orig_text'])

                # Translate
                job['is_translated'] = 0
                job['trans_text'] = None
                if self.translate:
                    response = self.translate_to_english(contents=[text])
                    paragraphs = [t.translated_text for t
                                  in response.translations]
                    job['trans_text'] = '\n\n'.join(paragraphs)
                    job['is_translated'] = 1

                # Write to text or delete PDF
                if self.export_to_txt:
                    self.write_file(job=job)
                else:
                    self.delete_pdf(job=job)

                # Write to database
                write_to_documents(
                    conn=connection,
                    title=job['title'],
                    country='India',
                    group_name='CPI-M',
                    doc_type=job['doc_type'],
                    date=job['date'],
                    language='en',
                    is_translated=job['is_translated'],
                    orig_text=job['orig_text'],
                    trans_text=job['trans_text'],
                    url=job['url'],
                    n_tokens=job['n_tokens']
                )
                logging.info('...complete')
            except Exception:
                logging.exception("Exception occured")
                logging.info('Download Failed.')
                failed_jobs.append(i)
                continue

            # Slow down crawl a bit
            time.sleep(1)

        # Remove failed jobs
        if len(failed_jobs) > 0:
            jobs = del_list_numpy(jobs, failed_jobs)

        # Save
        self.jobs = jobs
        if save:
            self.save_jobs()


# ================ #
# Helper functions #
# ================ #

def dl_print(i, job):
    """Prints info about job being downloaded."""
    logging.info(
        'Downloading Statement: {} ... \
        \n\tLink: {}\n\tTitle: {}'.format(i+1, job['url'], job['title'])
    )


def job_print(job):
    """Prints info about job being added to queue."""
    logging.info('Getting job: {}'.format(job['url']))


def fix_days(day_string):
    """
    Takes a date string for days,
    checks to make sure in range 1-31, else fixes
    """
    if not int(day_string) in range(1, 31):
        if int(day_string) < 1:
            day_string = '01'
        if int(day_string) > 31:
            day_string = '31'
    return day_string


def convert(fname, pages=None):
    """Converts pdf and returns its text content as a string"""
    if not pages:
        pagenums = set()
    else:
        pagenums = set(pages)

    # Instantiate pdfminer
    output = StringIO()
    manager = PDFResourceManager()
    converter = TextConverter(manager, output, laparams=LAParams())
    interpreter = PDFPageInterpreter(manager, converter)

    # Open file and convert
    infile = open(fname, 'rb')
    for page in PDFPage.get_pages(infile, pagenums):
        interpreter.process_page(page)
    infile.close()
    converter.close()
    text = output.getvalue()
    output.close()
    return text
