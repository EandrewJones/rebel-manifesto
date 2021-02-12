from bs4 import BeautifulSoup
from urllib.request import urlopen
import pandas as pd
import os, string, time


# ========= #
# Functions #
# ========= #

def soupify(url):
    """Takes in url and returns soup object"""
    page = urlopen(url)
    html = page.read().decode('utf-8')
    soup = BeautifulSoup(html, 'html.parser')
    return(soup)


def download_statement(url, file_name, base_dir):
    """Takes url for statement page, extracts text, and 
    saves it to the specified directory/file."""
    
    # Soupify
    soup = soupify(url=url)
    
    # Write statement to file
    statement = soup.find('div', class_='entry').get_text()
    path_to_file = os.path.join(base_dir, file_name)
    with open(path_to_file, 'w') as f:
        f.writelines(statement)
    
# ================ #
# Scrape Documents #
# ================ #
    
# Setup working directory
os.chdir('/home/evan/Documents/projects/rebel_manifesto/manifestos')
base_dir = os.getcwd()

# Navigate to group directory
country = 'Israel'
group = 'PFLP'
source_type = 'primary'
directory = os.path.join(base_dir, country, group, source_type)

# Set base PFLP url
base_url = 'https://english.pflp.ps/category/our-position/'

# Instantiate containers and flags
group_list = []
date_list = []
title_list = []
link_list = []
file_list = []
i = 1
next_page = True

# Iterate through pages
while next_page:
    # Set page url
    url = base_url + 'page/' + str(i)
    
    # Read page
    soup = soupify(url)

    # Locate statement html nodes
    article_tags = soup.find_all('a', class_ = 'entry-title')

    # Get links
    links = [h['href'] for h in article_tags]

    # Get titles
    titles = [t.get_text() for t in article_tags]

    # Get dates
    months = [m.get_text() for m in soup.find_all('span', class_ = 'month')]
    days = [d.get_text() for d in soup.find_all('span', class_ = 'day')]
    years = [y.get_text() for y in soup.find_all('span', class_ = 'year')]
    dates = ['-'.join(x) for x in zip(years, months, days)]
    dates = pd.to_datetime(pd.Series(dates), format='%Y-%b-%d')
    date_strings = dates.dt.strftime('%Y-%m-%d')

    # Create file names
    punct_table = str.maketrans('', '', string.punctuation)
    space_table = str.maketrans('', '', string.whitespace)
    short_titles = [t.translate(punct_table).title().translate(space_table) 
                    for t in titles]
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
    older_entries = soup.find('span', class_='previous-entries').find('a')
    next_page = older_entries is not None
    
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
    download_statement(url=row['link'], file_name=row['file_names'], base_dir=directory)
    time.sleep(3)
    
 