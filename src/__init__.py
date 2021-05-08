"""
Top-level package for project source code

Available submodules
--------------------

config
    Defines system-level configuration parameters for database, etc.
scrapers
    Scraping models for online manifesto collection
utils
    General utility functions
"""

import src.config
import src.scrapers
import src
import src.utils

__all__ = [
    'config',
    'scrapers',
    'src',
    'utils'
]
