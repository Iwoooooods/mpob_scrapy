
from setuptools import setup, find_packages
import os

os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name = 'malaysia_ap',
    version = '0.1.0',
    packages = find_packages(),
    entry_points = {'scrapy': ['settings = malaysia_ap.settings']},
    install_requires = ['mysql-python', 
                        'cx_Oracle', 
                        'scrapy',
                        'pandas',
                        'beautifulsoup4',
                        'html5lib',
                        ],
)