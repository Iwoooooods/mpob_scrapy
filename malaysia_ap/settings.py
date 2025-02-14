# -*- coding: utf-8 -*-

# Scrapy settings for canada_ap project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'malaysia_ap'

SPIDER_MODULES = ['malaysia_ap.spiders']
NEWSPIDER_MODULE = 'malaysia_ap.spiders'

TEMP_DATA_DIR = 'temp'
# dev
DATABASE_URI = 'user/password@localhost:1521/dbname'
# prod
# DATABASE_URI = 'user/password@localhost:1521/dbname'
FTP_SETTINGS = {
    'HOST': 'ftp.example.com',
    'PORT': 21,
    'USERNAME': 'ftpuser',
    'PASSWORD': 'ftppassword',
    'BASE_DIR': '/IndustDataCollection/AP/MPOB'
}

# MPOB Login credentials
MPOB_USERNAME = 'mpobuser'
MPOB_PASSWORD = 'mpobpassword'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'Malaysia_ap (+http://www.yourdomain.com)'

# Obey robots.txt rules
ROBOTSTXT_OBEY = True
