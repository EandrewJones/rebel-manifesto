"""Rebel Manifestos Project Config FIle."""
from dotenv import load_dotenv
from os import environ, path

# TODO: generalize for container
base_dir = '/home/evan/Documents/projects/rebel_manifesto/.env/'
load_dotenv(path.join(base_dir, 'config.env'))


class Config:
    """Sets base config variables"""

    # MySQL server
    SQL_USER = environ.get('SQL_USER')
    SQL_PASSWORD = environ.get('SQL_PASSWORD')
    SQL_HOST = environ.get('SQL_HOST', '127.0.0.1')
    SQL_FLAVOR = environ.get('SQL_DATABASE', 'mysql')
    SQL_CHARSET = environ.get('SQL_CHARSET', 'utf8')

    # Google Cloud SDK
    GCLOUD_CREDENTIALS = environ.get('GOOGLE_APPLICATION_CREDENTIALS')
