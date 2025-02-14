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
DATABASE_URI = 'stg/stg123@10.10.50.17:1534/db'
# prod
# DATABASE_URI = 'stg/stg123@findata.kffund.cn:1534/db'
FTP_SETTINGS = {
    'HOST': '192.168.8.234',
    'PORT': 21,
    'USERNAME': 'ftp4dce',
    'PASSWORD': 'DCEftp123',
    'BASE_DIR': '/IndustDataCollection/AP/MPOB'
}

# MPOB Login credentials
MPOB_USERNAME = 'kfagri'
MPOB_PASSWORD = 'KFfund2025'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'Malaysia_ap (+http://www.yourdomain.com)'

# Obey robots.txt rules
ROBOTSTXT_OBEY = True
