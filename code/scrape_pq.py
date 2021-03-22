from requests import Session
import requests
import os
import string
import re
from google_trans_new import google_translator
from dateutil.parser import parse
from bs4 import BeautifulSoup


class CanadaPQScraper(object):
    
    def __init__(self):
        # Instantiate tools
        self.punct_table = str.maketrans('', '', string.punctuation)
        self.space_table = str.maketrans('', '', string.whitespace)
        self.translator = google_translator(timeout=5)
        self.session = Session()
        
        self.base_dir = '/home/evan/Documents/projects/rebel_manifesto/manifestos/Canada/PQ/primary'
        self.url_dictionary = {
            'blog': 'https://pq.org/blogues/page/',
            'news': 'https://pq.org/nouvelles/page/',
            'programme': 'https://pq.org/programme/'
        }
        self.verify = '/home/evan/Documents/projects/rebel_manifesto/.env/pq-org-chain.pem'
        self.headers = {
            'Host': 'pq.org',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0',    
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Referer': 'https://pq.org/',
            'DNT': '1',
            'Sec-GPC': '1'
        }
        
    def soupify(self, url: str) -> BeautifulSoup:
        """Takes in url and returns soup object"""
        page = requests.get(
            url=url,
            headers=self.headers,
            verify=self.verify
        )
        html = page.text
        soup = BeautifulSoup(html, 'html.parser')
        return(soup)
    
    def write_file(self, job: list, paragraphs: list):
        """
        Write list of paragraphs to file
        """
        file_name = job['title'] + '_' + job['doc_type'] + '_' + job['date'] + '.txt'
        path_to_file = os.path.join(self.base_dir, file_name)
        with open(path_to_file, 'w') as f:
            f.writelines("%s\n" % p for p in paragraphs) 

    def get_blog_jobs(self) -> list:
        '''
        Iteratre through blog pages to get title, link, date
        '''
        jobs = []
        
        i = 1
        next_page = True
        while next_page:
            # Load page
            soup = self.soupify(url=self.url_dictionary['blog'] + str(i))
            posts = soup.find_all('header', class_='entry-header')
            
            for post in posts:
                job = {}
                
                # Title
                title = post.find('a').get_text(strip=True)
                title = self.translator.translate(title, lang_src='fr', lang_tgt='en').strip()
                title = title.translate(self.punct_table).title()
                title = title.translate(self.space_table).encode('ascii', 'ignore').decode()
                job['title'] = title
                
                # Link
                job['link'] = post.find('a')['href']
        
                # Date
                date = post.find('div', class_='blog_date').get_text()
                date = self.translator.translate(date, lang_src='fr', lang_tgt='en').strip()
                date = parse(date, fuzzy=True).strftime('%Y-%m-%d')
                job['date'] = date
                
                # Document type
                job['doc_type'] = 'Statement'
                
                jobs.append(job)
            
            # update next_page and i
            next_page = soup.find('div', class_='right') is not None
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
            posts = soup.find_all('a', class_='elementor-post__thumbnail__link')
            
            for post in posts:
                job = {}
                
                # Title
                title = post.find('h3', class_='elementor-post__title').get_text(strip=True)
                title = self.translator.translate(title, lang_src='fr', lang_tgt='en').strip()
                title = title.translate(self.punct_table).title()
                title = title.translate(self.space_table).encode('ascii', 'ignore').decode()
                job['title'] = title
                
                # Link
                job['link'] = post['href']
        
                # Date
                date = post.find('span', class_='elementor-post-date').get_text(strip=True)
                date = date.translate(self.punct_table).title()
                if bool(re.search('avr\s', date, re.IGNORECASE)):
                    date = re.sub('avr', 'Apr', date, flags=re.IGNORECASE)
                date = self.translator.translate(date, lang_src='fr', lang_tgt='en').strip()
                print(date)
                date = parse(date, fuzzy=True).strftime('%Y-%m-%d')
                job['date'] = date
                
                # Document type
                job['doc_type'] = 'News'
                
                jobs.append(job)
            
            # update next_page and i
            next_page = soup.find('a', class_='next page-numbers') is not None
            i += 1
            
        return jobs
        
    
    def download_statements(self):
        """Takes url for statement page, extracts text, and 
        saves it to the specified directory/file."""
        
        # Get articles
        jobs = self.get_news_jobs()
        jobs.append(self.get_blog_jobs())
        
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
                                 in soup.find('div', {'id': 'boite_contenu'}).find_all('p')]
                    paragraphs = [self.transaltor.translate(p, lang_src='fr', lang_tgt='en')
                                  for p in statement if p]
                    self.write_file(job=job, paragraphs=paragraphs)
                    print('...complete')
                if job['doc_type'] == 'News':
                    statement = [p.get_text(strip=True) for p 
                                 in soup.find('div', {'id': 'nouvelle-box'}).find_all('p')]
                    paragraphs = [self.transaltor.translate(p, lang_src='fr', lang_tgt='en')
                                  for p in statement if p]
                    self.write_file(job=job, paragraphs=paragraphs)
                    print('...complete')
                else:
                    raise ValueError('Cannot read document of type: {}'.format(job['doc_type']))
            except:
                print('...Download failed!')
                

if __name__ == '__main__':
    # Currently, jobs acquired and downloaded wihtout saving state
    # If translator timeout, script fails.
    # TODO: Consider saving jobs in a queue, and splitting into two scripts:
    # 1. update queue with new jobs 
    # 2. script that downloads N documents
    # Cron job the scripts
    scraper = CanadaPQScraper()
    
    # Download Statements
    print('Getting jobs...')
    scraper.download_statements()
