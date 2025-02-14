# coding=utf8
import scrapy
from scrapy.utils.project import get_project_settings
import logging
import pandas as pd
import os
import re
import six
import traceback
import datetime
from helper.database_helper import merge_db_oracle_dataframe
from helper.upload_helper import upload_csv_to_ftp
from helper.database_helper import insert_log_table


class PalmOilSummarySpider(scrapy.Spider):
    name = 'mpob_summary'
    DATA_SOURCE = 'https://bepi.mpob.gov.my/index.php/summary-2'
    DATA_SUPPLIER = 'MPOB'

    def temporary_dir(self):
        temp_dir = os.path.join(get_project_settings().get('TEMP_DATA_DIR'), self.name)
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)
        return temp_dir

    def start_requests(self):
        start_time = pd.Timestamp(pd.Timestamp.now())
        yield scrapy.http.Request(self.DATA_SOURCE, meta={'tag': self.name, 'start_time': start_time})

    def parse(self, response):
        """
            Summary Of The Malaysian Palm Oil Industry 2022
        """
        # title_list = response.xpath('//ul[@class="mod-articlescategory category-module mod-list"]/li/ul/li/a/text()').getall()
        # hlink_list = response.xpath('//ul[@class="mod-articlescategory category-module mod-list"]/li/ul/li/a/@href').getall()

        title_list = response.xpath('//*[@id="ca-1529739248826"]/main/div/div/div/div/ul/li/ul/li/a/text()').getall()
        hlink_list = response.xpath('//*[@id="ca-1529739248826"]/main/div/div/div/div/ul/li/ul/li/a/@href').getall()
        print (title_list)
        print (hlink_list)
        for pair in zip(title_list, hlink_list):
            title = pair[0].strip()
            url = response.urljoin(pair[1])
            if 'Summary Of The Malaysian Palm Oil Industry' in title:
                meta = {
                    'YEAR': title[-4:],
                    'CATEGORY': 'Summary Of The Malaysian Palm Oil Industry',
                    'TABLE_PARSER': self.parse_table
                }
                meta.update(response.meta)
                yield scrapy.http.Request(url, meta=meta, callback=self.parse_iframe)
            else:
                self.log('unknown title: %s' % title, level=logging.WARNING)

    def parse_iframe(self, rsp):
        src = rsp.xpath('//iframe/@src').get()
        url = rsp.urljoin(src.replace('../', ''))
        self.log('parse_iframe: %s' % url, level=logging.INFO)
        table_parser = rsp.meta.pop('TABLE_PARSER')
        yield scrapy.http.Request(url, meta=rsp.meta, callback=table_parser)

    def parse_table(self, rsp):
        self.log('parse_table:%s' % rsp, level=logging.INFO)
        start_time = rsp.meta['start_time']
        year = rsp.meta.get('YEAR')
        category = rsp.meta.get('CATEGORY')
        df = pd.read_html(rsp.text, header=0, flavor='bs4')[0]
        df = self.rename_columns(df, year)
        df = self.transform(df)
        df['DATADATE'] = pd.to_datetime(df['DATADATE'], format="%Y-%m")
        df['SOURCE'] = self.DATA_SOURCE
        df['SUPPLIER'] = self.DATA_SUPPLIER
        df.dropna(axis=0, subset=['VALUE'], inplace=True)
        df.set_index(['DATADATE', 'CATEGORY', 'PRODUCT'], inplace=True)
        filename = os.path.join(self.temporary_dir(), '%s_%s.csv' % (category, year))
        df.to_csv(filename)
        
        try:
            count = merge_db_oracle_dataframe(df, 'T_AP_MYS_INDUSTRY_SUMMARY', get_project_settings().get('DATABASE_URI'))
            insert_log_table('scrapy:malaysia:mpob_summary.py', 'T_AP_MYS_INDUSTRY_SUMMARY', start_time, '成功', '合入{count}条数据'.format(count=count), "")
        except Exception as e:
            buf = six.StringIO()
            traceback.print_exc(file=buf)
            error_info = buf.getvalue()
            insert_log_table('scrapy:malaysia:mpob_summary.py', 'T_AP_MYS_INDUSTRY_SUMMARY', start_time, '失败', '合入数据', str(error_info))
        
        upload_csv_to_ftp(filename, self.name, get_project_settings().get('FTP_SETTINGS'))

    @staticmethod
    def rename_columns(df_hor, year):
        months = ['NULL', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        columns = ['MIXTURE']
        for c in df_hor.columns[1:]:
            month = c.strip()[0:3]
            year_re = re.search(r'(\d{2,4})', c)
            if year_re:
                year_number = year_re.group(1)
                date = '20%s-%s' % (year_number, months.index(month))
            else:
                date = '%s-%s' % (year, months.index(month))
            columns.append(date)
        print(columns)
        df_hor.columns = columns
        return df_hor

    @staticmethod
    def transform(df_hor):
        category = None
        unit = None
        datas = []
        for r in df_hor.itertuples():
            product = None
            if str(r[1]) == str(r[2]) or pd.isnull(r[2]):
                # PRODUCTION (TONNES)/CLOSING STOCK (TONNES)/EXPORT (TONNES)/IMPORT (TONNES)/PRICE (1% OER EQUIVALENT)
                first_col_value = str(r[1]).strip()
                category = first_col_value[0:first_col_value.index('(')].strip()
                unit = first_col_value[first_col_value.index('(')+1:first_col_value.index(')')].strip()
            else:
                for i, c in enumerate(df_hor.columns):
                    if c == 'MIXTURE':
                        product = r[i+1]
                    else:
                        datas.append({
                            'CATEGORY': category,
                            'PRODUCT': product,
                            'DATADATE': c,
                            'VALUE': r[i+1],
                            'UNIT': unit
                        })
        return pd.DataFrame(datas)
