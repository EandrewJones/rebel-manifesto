from requests import Session
import requests
import os
import string
#import pycld2 as cld2
from google_trans_new import google_translator
from dateutil.parser import parse
from bs4 import BeautifulSoup


class SudanJemScraper(object):
    
    def __init__(self):
        # Instantiate tools
        self.translator = google_translator(timeout=5)
        self.session = Session()
        
        self.base_dir = '/home/evan/Documents/projects/rebel_manifesto/manifestos/Sudan/JEM/primary'
        self.api_url = 'https://sudanjem.com/?ajax-request=jnews'
        self.post_headers = {
            'Host': 'sudanjem.com',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0',    
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'X-Requested-With': 'XMLHttpRequest',
            'Content-Length': '3318',
            'Origin': 'https://sudanjem.com',
            'Connection': 'keep-alive',
            'Referer': 'https://sudanjem.com/category/english/',
            'Cookie': '__cfduid=df014e02c4c5eda6777b937d7bd4368951613148537; darkmode=false',
            'DNT': '1',
            'Sec-GPC': '1'
        }
        self.get_headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "en-US,en;q=0.5", 
            "Cache-Control": "max-age=0",
            "Connection": "keep-alive",
            "Cookie": "__cfduid=dac92f6103e9d2a5b71f0f62c7d6fb2b51615571265; darkmode=false; zoom_position=1",
            "DNT": "1",
            "Host": "sudanjem.com",
            "Referer": "https://sudanjem.com/category/english/",
            "Sec-GPC": "1",
            "TE": "Trailers",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0"
        }
        self.data = {
            "lang": "ar",
            "action": "jnews_module_ajax_jnews_block_4",
            "module": "true",
            "data[filter]": "0",
            "data[filter_type]": "all",
            "data[current_page]": 1,
            "data[attribute][header_icon]": "",
            "data[attribute][first_title]": "",
            "data[attribute][second_title]": "",
            "data[attribute][url]": "",
            "data[attribute][header_type]": "heading_6",
            "data[attribute][header_background]": "",
            "data[attribute][header_secondary_background]": "",
            "data[attribute][header_text_color]": "",
            "data[attribute][header_line_color]": "",
            "data[attribute][header_accent_color]": "",
            "data[attribute][header_filter_category]": "",
            "data[attribute][header_filter_author]": "",
            "data[attribute][header_filter_tag]": "",
            "data[attribute][header_filter_text]": "All",
            "data[attribute][post_type]": "post",
            "data[attribute][content_type]": "all",
            "data[attribute][number_post]": "10",
            "data[attribute][post_offset]": "3",
            "data[attribute][unique_content]": "disable",
            "data[attribute][include_post]": "",
            "data[attribute][exclude_post]": "",
            "data[attribute][include_category]": "19",
            "data[attribute][exclude_category]": "",
            "data[attribute][include_author]": "",
            "data[attribute][include_tag]": "",
            "data[attribute][exclude_tag]": "",
            "data[attribute][sort_by]": "latest",
            "data[attribute][date_format]": "default",
            "data[attribute][date_format_custom]": "Y/m/d",
            "data[attribute][excerpt_length]": "20",
            "data[attribute][excerpt_ellipsis]": "...",
            "data[attribute][force_normal_image_load]": "",
            "data[attribute][pagination_mode]": "loadmore",
            "data[attribute][pagination_nextprev_showtext]": "",
            "data[attribute][pagination_number_post]": "10",
            "data[attribute][pagination_scroll_limit]": "0",
            "data[attribute][ads_type]": "disable",
            "data[attribute][ads_position]": "1",
            "data[attribute][ads_random]": "",
            "data[attribute][ads_image]": "",
            "data[attribute][ads_image_tablet]": "",
            "data[attribute][ads_image_phone]": "",
            "data[attribute][ads_image_link]": "",
            "data[attribute][ads_image_alt]": "",
            "data[attribute][ads_image_new_tab]": "",
            "data[attribute][google_publisher_id]": "",
            "data[attribute][google_slot_id]": "",
            "data[attribute][google_desktop]": "auto",
            "data[attribute][google_tab]": "auto",
            "data[attribute][google_phone]": "auto",
            "data[attribute][content]": "",
            "data[attribute][ads_bottom_text]": "",
            "data[attribute][boxed]": "false",
            "data[attribute][boxed_shadow]": "false",
            "data[attribute][el_id]": "",
            "data[attribute][el_class]": "",
            "data[attribute][scheme]": "",
            "data[attribute][column_width]": "auto",
            "data[attribute][title_color]": "",
            "data[attribute][accent_color]": "",
            "data[attribute][alt_color]": "",
            "data[attribute][excerpt_color]":  "",
            "data[attribute][css]": "",
            "data[attribute][paged]": "1",
            "data[attribute][pagination_align]": "left",
            "data[attribute][pagination_navtext]": "true",
            "data[attribute][pagination_pageinfo]": "true",
            "data[attribute][box_shadow]": "false",
            "data[attribute][push_archive]": "true",
            "data[attribute][video_duration]": "true",
            "data[attribute][post_meta_style]": "style_2",
            "data[attribute][author_avatar]": "true",
            "data[attribute][more_menu]": "true",
            "data[attribute][column_class]": "jeg_col_2o3",
            "data[attribute][class]": "jnews_block_4"
        }
    
    def soupify(self, url: str) -> BeautifulSoup:
        """Takes in url and returns soup object"""
        page = requests.get(
            url=url,
            headers=self.get_headers
        )
        html = page.text
        soup = BeautifulSoup(html, 'html.parser')
        return(soup)
        
    def get_statement_info(self) -> list:
        '''
        Iterate through API article calls to get article title, link, and date
        '''
        jobs = []
        
        next_page = True
        while next_page:
            # Make API call
            request = self.session.post(
                url=self.api_url,
                data=self.data,
                headers=self.post_headers
            )
            
            # Extract title, date, and url
            html = request.json()['content']
            soup = BeautifulSoup(html, 'html.parser')
            posts = soup.find_all('div', class_='jeg_postblock_content')
            
            for post in posts:
                job = {}
    
                title_link_node = post.find('h3', class_='jeg_post_title')

                # Title
                punct_table = str.maketrans('', '', string.punctuation)
                space_table = str.maketrans('', '', string.whitespace)
                title = title_link_node.get_text()
                title = title.translate(punct_table).title().translate(space_table).encode('ascii', 'ignore').decode()
                job['title'] = title[:230] # ensure filename < 256

                # Links
                job['link'] = title_link_node.find('a')['href']

                # Date
                date = post.find('div', class_='jeg_meta_date').get_text()
                date = self.translator.translate(date, lang_src='ar', lang_tgt='en').strip()
                date = parse(date, fuzzy=True).strftime('%Y-%m-%d')
                job['date'] = date

                jobs.append(job)
                
            # update next_page
            next_page = request.json()['next']
            
            # Update page number
            self.data['data[current_page]'] += 1
            
        return jobs
    
    def download_statements(self):
        """Takes url for statement page, extracts text, and 
        saves it to the specified directory/file."""
        
        # Get articles
        jobs = self.get_statement_info()
        
        for i, job in enumerate(jobs):
            # Download statement
            print('Downloading Statement: {} ...\n\tLink: {}\n\tTitle: {}'.format(i+1, job['link'], job['title']))
            try:
                soup = self.soupify(url=job['link'])
                statement = [p.get_text(strip=True) for p 
                             in soup.find('div', class_='content-inner').find_all('p')]
                paragraphs = [p for p in statement if p]

                # Write file
                file_name = job['title'] + '_statement_' + job['date'] + '.txt'
                path_to_file = os.path.join(self.base_dir, file_name)
                with open(path_to_file, 'w') as f:
                    f.writelines("%s\n" % p for p in paragraphs)                
                print('...complete')
            except:
                print('...Download failed!')
                

if __name__ == '__main__':
    scraper = SudanJemScraper()
    
    # Download Statements
    print('Getting jobs...')
    scraper.download_statements()
