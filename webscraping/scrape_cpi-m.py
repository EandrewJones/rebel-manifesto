from src.utils import connect_to_db
from src.config import Config
from src.scrapers import IndiaCPIMaoistScraper
import json
import logging


if __name__ == '__main__':

    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        filename='logs/scrape_cpi-m.log',
        filemode='w',
        format='%(name)s - %(levelname)s - %(message)s'
    )

    # Instantiate scraper
    with open('webscraping/params/cpi-m_params.json') as infile:
        params = json.load(infile)

    scraper = IndiaCPIMaoistScraper(**params)

    # Create database connection
    config = Config()
    conn = connect_to_db(
        user=config.SQL_USER,
        passwd=config.SQL_PASSWORD,
        host=config.SQL_HOST,
        db_flavor=config.SQL_FLAVOR,
        charset=config.SQL_CHARSET
    )

    # Download Statements
    scraper.get_jobs(save=True)
    scraper.download_statements(connection=conn, save=True)

    # Close database connection
    conn.close()
