from bs4 import BeautifulSoup
from urllib.request import urlopen
import pandas as pd
import os, string, time, re


# ========= #
# Functions #
# ========= #

def soupify(url):
    """Takes in url and returns soup object"""
    # TODO add try-except
    page = urlopen(url)
    html = page.read()
    soup = BeautifulSoup(html, 'html.parser')
    return(soup)


def download_statement(url, file_name, base_dir):
    """Takes url for statement page, extracts text, and 
    saves it to the specified directory/file."""
    
    # Soupify
    soup = soupify(url=url)
    
    # Write statement to file
    paragraphs = [p.get_text() 
                  for p in soup.find('div', class_='field-item even').find_all('p')] 
    path_to_file = os.path.join(base_dir, file_name)
    with open(path_to_file, 'w') as f:
        f.writelines("%s\n" % p for p in paragraphs)


# ================ #
# Scrape Documents #
# ================ #
    
# Setup working directory
os.chdir('/home/evan/Documents/projects/rebel_manifesto/manifestos')
base_dir = os.getcwd()

# Navigate to group directory
country = 'Moldova'
group = 'PMR'
source_type = 'primary'
directory = os.path.join(base_dir, country, group, source_type)

# Set base PFLP url
base_url = 'http://mfa-pmr.org'

# Instantiate containers and flags
group_list = []
date_list = []
title_list = []
link_list = []
file_list = []
i = 0
next_page = True

# Iterate through pages
while next_page:
    # Set page url
    url = base_url + '/en/statements?page=' + str(i)

    # Read page
    soup = soupify(url)

    # Locate statement html nodes
    article_tags = [t.find('a') for t in soup.find_all('h1', class_ = 'node-title')]

    # Get links
    links = [base_url + h['href'] for h in article_tags]
    
    # Get titles
    titles = [t.get_text()[:230] for t in article_tags]

    # Get dates
    dates = [m.get_text() for m in soup.find_all('div', class_='nodelist-date')]
    if i == 0: dates.insert(13, '04/26/20')
    dates = pd.to_datetime(pd.Series(dates), format='%m/%d/%y')
    date_strings = dates.dt.strftime('%Y-%m-%d')
    

    # Create file names
    short_titles = [re.sub('[^A-Za-z]+', '', t) for t in titles]
    file_names = [s + '_statement_' + d + '.txt'
                  for s, d in zip(short_titles, date_strings)]

    # update lists
    group_list.extend([group] * len(dates))
    date_list.extend(date_strings)
    title_list.extend(titles)
    link_list.extend(links)
    file_list.extend(file_names)

    # Update i and next_page
    i += 1
    nxt = soup.find('li', class_='pager-next even')
    next_page = nxt is not None

    # Sleep
    time.sleep(3)
    

# Create df
d = {
    'group': group_list,
    'date': date_list,
    'title': title_list,
    'link': link_list,
    'file_names': file_list
}
statement_links_df = pd.DataFrame(d)

# Iterate over link, file_name pairs and save statements
for _, row in statement_links_df.iterrows():
    print('Downloading Statement: {} ...\n\tLink: {}\n\tTitle: {}'.format(_, row['link'], row['title']))
    try:
        download_statement(url=row['link'], file_name=row['file_names'], base_dir=directory)
        print('...complete')
    except:
        print('Download failed!')
    time.sleep(3)