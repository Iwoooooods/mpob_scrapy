from scrapy import cmdline
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
cmdline.execute(['scrapy', 'crawl', 'mpob_production'])
