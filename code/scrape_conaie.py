from requests import Session
from google.cloud import translate
from bs4 import BeautifulSoup
import requests
import os
import string
import re
import time
import dateparser
import pickle

class EcuadorCONAIEScraper(object):
    
    def __init__(self, pickle_file, export_to_txt=False, translate=False):
        
        self.pkl_file = pickle_file
        self.export_to_txt = export_to_txt
        
        # Instantiate tools
        self.punct_table = str.maketrans('', '', string.punctuation)
        self.space_table = str.maketrans('', '', string.whitespace)
        self.session = Session()
        
        # Google translate key
        self.translate = translate
        if translate:
            project_id = os.environ.get("PROJECT_ID", "")
            self.parent = f'projects/{project_id}'
            self.translator = translate.TranslationServiceClient()
        
        # Metadata
        self.base_dir = '/home/evan/Documents/projects/rebel_manifesto/manifestos/Ecuador/CONAIE/primary'
        self.url_dictionary = {
            'statements': 'https://conaie.org/category/boletines/page/',
            'news': 'https://conaie.org/category/noticias/page/',
            'programme': 'https://conaie.org/proyecto-politico/'
        }
        #self.verify = '/home/evan/Documents/projects/rebel_manifesto/.env/pq-org-chain.pem'
        self.headers = {
            'Host': 'conaie.org',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0',    
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Referer': 'https://conaie.org/',
            'Cookie': 'PHPSESSID=03784c9ff4643ec1596e2a86b0e1fe93; csbwfs_show_hide_status=active',
            'Upgrade-Insecure-Requests': '1',
            'DNT': '1',
            'Sec-GPC': '1'
        }
        
    def soupify(self, url: str) -> BeautifulSoup:
        """Takes in url and returns soup object"""
        page = requests.get(
            url=url,
            headers=self.headers,
            #verify=self.verify
        )
        html = page.text
        soup = BeautifulSoup(html, 'html.parser')
        return(soup)
    
    def translate_to_english(self, contents: list):
        response = self.translator.translate_text(
                        contents=contents, 
                        target_language_code='en', 
                        parent=self.parent
                    )
        return response
    
    def write_file(self, job: list, paragraphs: list):
        """
        Write list of paragraphs to file
        """
        file_name = job['title'] + '_' + job['doc_type'] + '_' + job['date'] + '.txt'
        path_to_file = os.path.join(self.base_dir, file_name)
        with open(path_to_file, 'w') as f:
            f.writelines("%s\n" % p for p in paragraphs) 

    def get_statements_jobs(self) -> list:
        '''
        Iteratre through bulletin pages to get title, link, date
        '''
        jobs = []
        
        i = 1
        next_page = True
        while next_page:
            # Load page
            soup = self.soupify(url=self.url_dictionary['statements'] + str(i))
            posts = soup.find_all('div', class_='entry-meta post-info')
            
            for post in posts:
                job = {}
                
                # Title
                title = post.find('a').get_text(strip=True)
                title = title.translate(self.punct_table).title()
                title = title.translate(self.space_table).encode('ascii', 'ignore').decode()
                if self.translate:
                    response = self.translate_to_english(contents=[title])
                    title = response.translations[0].translated_text
                job['title'] = title
                
                # Link
                job['link'] = post.find('a')['href']
        
                # Date
                date_regex = re.compile('entry-date published*')
                date = post.find('time', class_=date_regex)['datetime']
                job['date'] = date_to_ymd(date)
                
                # Document type
                job['doc_type'] = 'Statement'
                
                jobs.append(job)
            
            # update next_page and i
            next_page = soup.find('a', class_='next page-numbers') is not None
            i += 1
            
        return jobs
            
    def get_news_jobs(self):
        '''
        Get scraping links, titles, dates from news pages
        '''
        jobs = []
        
        i = 1
        next_page = True
        while next_page:
            # Load page
            soup = self.soupify(url=self.url_dictionary['news'] + str(i))
            posts = soup.find_all('div', class_='entry-meta post-info')
            
            for post in posts:
                job = {}
                
                # Title
                title = post.find('a').get_text(strip=True)
                title = title.translate(self.punct_table).title()
                title = title.translate(self.space_table).encode('ascii', 'ignore').decode()
                if self.translate:
                    response = self.translate_to_english(contents=[title])
                    title = response.translations[0].translated_text
                job['title'] = title
                
                # Link
                job['link'] = post.find('a')['href']
        
                # Date
                class_regex = re.compile('entry-date published*')
                date = post.find('time', class_=class_regex)['datetime']
                job['date'] = date_to_ymd(date)
                
                # Document type
                job['doc_type'] = 'News'
                
                jobs.append(job)
                
            # update next_page and i
            next_page = soup.find('a', class_='next page-numbers') is not None
            i += 1
            
        return jobs
        
    def download_statements(self, save: bool):
        """Takes url for statement page, extracts text, and 
        saves it to the specified directory/file."""
        
        # Get articles
        jobs = self.get_news_jobs()
        jobs.extend(self.get_statements_jobs())
        
        for i, job in enumerate(jobs):
            # Download statement
            print(
                'Downloading Statement: {} ... \
                  \n\tLink: {}\n\tTitle: {}'.format(i+1, job['link'], job['title'])
                  )
            try:
                soup = self.soupify(url=job['link'])
                if job['doc_type'] == 'Statement':
                    statement = [p.get_text(strip=True) for p 
                                 in soup.find('div', class_='entry-content').find_all('p')]
                    paragraphs = [p for p in statement if p]
                    if self.translate:
                        response = self.translate_to_english(contents=paragraphs)
                        paragraphs = [t.translated_text for t in response.translations]
                    if self.export_to_txt:
                        self.write_file(job=job, paragraphs=paragraphs)
                    else:
                        job['paragraphs'] = paragraphs
                        job['is_translated'] = False
                    job['n_tokens'] = count_tokens(str_list=job['paragraphs'])
                    print('...complete')  
                elif job['doc_type'] == 'News':
                    statement = [p.get_text(strip=True) for p 
                                 in soup.find('div', class_='entry-content').find_all('p')]
                    paragraphs = [p for p in statement if p]
                    if self.translate:
                        response = self.translate_to_english(contents=paragraphs)
                        paragraphs = [t.translated_text for t in response.translations]
                    if self.export_to_txt:
                        self.write_file(job=job, paragraphs=paragraphs)
                    else:
                        job['paragraphs'] = paragraphs
                        job['is_translated'] = False
                    job['n_tokens'] = count_tokens(str_list=job['paragraphs'])
                    print('...complete')
                else:
                    raise ValueError('Cannot read document of type: {}'.format(job['doc_type']))   
            except ValueError as err:
                print('...Download failed!:', err)

            time.sleep(5)
        
        if save:
            save_object(obj=jobs, filename=self.pkl_file)

            
# ================= #
# Utility functions #
# ================= #

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
        
def count_tokens(str_list: list) -> int:
    """Counts total number of tokens in a list of strings."""
    count = 0
    for s in str_list:
        n_tokens = len(s.split())
        count += n_tokens
    return count


# ======= #
# Program #
# ======= #

if __name__ == '__main__':
    scraper = EcuadorCONAIEScraper(pickle_file='/home/evan/Documents/projects/rebel_manifesto/data/conaie_jobs.pkl')

    # Download Statements
    print('Getting jobs...')
    scraper.download_statements(save=True)