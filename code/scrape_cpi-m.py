from bs4 import BeautifulSoup
from urllib.request import urlopen
from pathlib import Path
from io import StringIO
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
import pandas as pd
import requests
import os, string, time, re, itertools, sys, getopt

# ========= #
# Functions #
# ========= #

def soupify(url):
    """Takes in url and returns soup object"""
    page = urlopen(url)
    html = page.read()
    soup = BeautifulSoup(html, 'html.parser')
    return(soup)


def fix_days(day_string):
    """Takes a date string for days, checks to make sure in range 1-31, else fixes"""
    if not int(day_string) in range(1, 31):
        if int(day_string) < 1:
            day_string = '01'
        if int(day_string) > 31:
            day_string = '31'
    return day_string


def download_statement(url, base_url, file_name, base_dir):
    """Takes url for statement pdf and 
    saves it to the specified directory/file."""
    
    # Create pdf filename
    path = os.path.join(base_dir, file_name)
    filename = Path(path)
    
    # Get PDF and save
    response = requests.get(base_url + url)
    filename.write_bytes(response.content)


# converts pdf, returns its text content as a string
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


# converts all pdfs in directory pdfDir, saves all resulting txt files to txtdir
def convert_all(pdf_dir, txt_dir):
    """Converts all pdfs in pdfDir and saves resulting txt files to txtdir """
    
    # If pdf_dir unspecified, set to current working directory
    if pdf_dir == "": 
        pdf_dir = os.getcwd() + "\\" 
    
    # Iteratre through pdfs in pdf directory
    for pdf in os.listdir(pdf_dir):
        fname, file_extension = pdf.split(".")
        if file_extension == "pdf":
            # Convert
            pdf_filename = os.path.join(pdf_dir, pdf) 
            text = convert(pdf_filename) 
            
            # Save
            text_filename = os.path.join(txt_dir, fname + ".txt")
            with open(text_filename, 'w') as tf:
                tf.write(text)


# ================ #
# Scrape Documents #
# ================ #
    
# Setup working directory
os.chdir('/home/evan/Documents/projects/rebel_manifesto/manifestos')
base_dir = os.getcwd()

# Navigate to group directory
country = 'India'
group = 'CPI-Maoist'
source_type = 'primary'
directory = os.path.join(base_dir, country, group, source_type)

# Set base PFLP url
base_url = 'http://www.bannedthought.net/India/CPI-Maoist-Docs/'

# Read page
soup = soupify(base_url)

# Extract statement link nodes
statement_tags = soup.find_all('a', href=True, 
                               text=re.compile(r"^(English: PDF format|PDF format|PDF Pamphlet)"))

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
         if re.search(find,l) is not None 
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

# Get titles
d_types = '|'.join(pd.unique(doc_type))
exclusions = '|'.join(['\.pdf', '\d+', '\w{2,3}'])
exclusions = '[^(' + d_types + '|' + exclusions + ')]'
find = re.compile(exclusions)
title_splits = [s.split('-') for s in links]

outer_l = []
for sublist in title_splits:
    inner_l = []
    for s in sublist:
        x = s.split('/')
        x = [re.sub(r'(^\d{4,6}|^[A-Z]{2,3}|Eng|\.pdf)', '', s) for s in x]
        inner_l.extend(x)
    outer_l.append(inner_l[1:])

punct_table = str.maketrans('', '', string.punctuation)
titles = [''.join(l).translate(punct_table) for l in outer_l]

# Create file names
file_names = [x + '_' + y + '_' + z + '.pdf'
              for x, y, z in zip(titles, doc_type, date_strings)]

# Create group name list
group_list = ['CPI-M'] * len(titles)

# Create df
d = {
    'group': group_list,
    'date': date_strings,
    'title': titles,
    'link': links,
    'file_names': file_names,
}
statement_links_df = pd.DataFrame(d)

# Download PDFs
for _, row in statement_links_df.iterrows():
    download_statement(url=row['link'], base_url=base_url, 
                       file_name=row['file_names'], base_dir=directory)
    
# Convert to PDFs
# NOTE could be sped up with parallel processing or async io
convert_all(pdf_dir=directory, txt_dir=directory)
