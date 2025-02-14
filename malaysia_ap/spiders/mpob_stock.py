# coding=utf8
import datetime
import traceback
import scrapy
from scrapy.utils.project import get_project_settings
import logging
import pandas as pd
import os
from helper.database_helper import merge_db_oracle_dataframe
from helper.upload_helper import upload_csv_to_ftp
from helper.database_helper import insert_log_table
import six

class PalmOilStockSpider(scrapy.Spider):
    name = 'mpob_stock'
    DATA_SOURCE = 'https://bepi.mpob.gov.my/index.php/stock'
    DATA_SUPPLIER = 'MPOB'
    login_url = 'https://bepi.mpob.gov.my/index.php/component/users/login'

    def temporary_dir(self):
        temp_dir = os.path.join(get_project_settings().get('TEMP_DATA_DIR'), self.name)
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)
        return temp_dir

    def start_requests(self):
        start_time = pd.Timestamp(pd.Timestamp.now())
        yield scrapy.http.Request(self.login_url, callback=self.parse_login, meta={'tag': self.name, 'start_time': start_time})
        
    def parse_login(self, response):
        self.log('parse login page to get token: %s' % response.url, level=logging.INFO)
        script_json = response.xpath('//script[@type="application/json"]/text()').get()
        crsf_token = None
        
        if script_json:
            import json
            try:
                data = json.loads(script_json)
                crsf_token = data.get('csrf.token')
            except:
                self.logger.error('Failed to extract token')
        
        if crsf_token:
            form_data = {
                'username': get_project_settings().get('MPOB_USERNAME'),
                'password': get_project_settings().get('MPOB_PASSWORD'),
                'return': '',
                crsf_token: '1'
            }
        else: 
            self.logger.error('Csrf token not found')
            
        return scrapy.FormRequest.from_response(
            response,
            formdata=form_data,
            callback=self.after_login,
            meta=response.meta
        )
        
    def after_login(self, response):
        self.log('after login: %s' % response.url, level=logging.INFO)
        if response.xpath('//form[contains(@class, "com-users-login__form")]'):
            self.logger.error('Login failed')
            return
        
        yield scrapy.Request(self.DATA_SOURCE, callback=self.parse, meta=response.meta)

    def parse(self, response):
        """
            Monthly Closing Stock of Oil Palm Products 2022
            Stock of Selected Processed Palm Oil at Refinery 2022
        """
        # check if authenticated
        if response.xpath('//form[contains(@class, "com-users-login__form")]'):
            self.logger.info('Got redirected to login page, need to authenticate')
            yield scrapy.Request(
                self.login_url,
                callback=self.parse_login,
                dont_filter=True
            )
            return
        
        table_parsers = {
            'Oil Palm Products': self.parse_region_table,
            'Selected Processed Palm Oil at Refinery': self.parse_refinery_table,
        }
        title_list = response.xpath('//ul[@class="mod-articlescategory category-module mod-list"]/li/ul/li/a/text()').getall()
        hlink_list = response.xpath('//ul[@class="mod-articlescategory category-module mod-list"]/li/ul/li/a/@href').getall()
        for pair in zip(title_list, hlink_list):
            title = pair[0].strip()
            url = response.urljoin(pair[1])
            year = title[-4:]
            category = title[title.index('Stock of'):].replace('Stock of', '').replace(year, '').strip()
            self.log('title=%s, url=%s' % (title, url), level=logging.INFO)
            if category in table_parsers:
                meta = {
                    'YEAR': year,
                    'CATEGORY': category,
                    'TABLE_PARSER': table_parsers[category]
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

    def parse_region_table(self, rsp):
        year = rsp.meta.get('YEAR')
        category = rsp.meta.get('CATEGORY')
        self.log('parse_region_table: [%s] [%s] %s' % (year, category, rsp), level=logging.INFO)
        header = 'Products'  # column Products is mixture
        df_list = pd.read_html(rsp.text, header=0, flavor='bs4')
        df_list = [self.trim_header(df, header) for df in df_list if header in df.columns]
        df = pd.merge(df_list[0], df_list[1], on=header, how='outer')
        # remove blank row
        df = df[df[df.columns[0]] != df[df.columns[1]]]
        df = self.transpose_date(df, header)
        # create PRODUCT and REGION
        df['PRODUCT'] = df[header].apply(lambda x: 'CRUDE PALM OIL' if x in ['PENINSULAR', 'SABAH', 'SARAWAK'] else x.strip())
        df['REGION'] = df[header].apply(lambda x: x if x in ['PENINSULAR', 'SABAH', 'SARAWAK'] else 'MALAYSIA')
        df.drop(columns=header, inplace=True)
        df['DATADATE'] = pd.to_datetime(df['DATADATE'], format="%Y-%m")
        df['UNIT'] = 'TONNES'
        df['SOURCE'] = self.DATA_SOURCE
        df['SUPPLIER'] = self.DATA_SUPPLIER
        df.dropna(axis=0, subset=['VALUE'], inplace=True)
        df.set_index(['DATADATE', 'PRODUCT', 'REGION'], inplace=True)
        filename = os.path.join(self.temporary_dir(), '%s_%s.csv' % (category, year))
        df.to_csv(filename)
        try:
            merge_db_oracle_dataframe(df, 'T_AP_MYS_STOCK_REGION', get_project_settings().get('DATABASE_URI'))
            insert_log_table('scrapy:malaysia:mpob_stock.py', 'T_AP_MYS_STOCK_REGION', rsp.meta['start_time'], '成功', '合入{count}条数据'.format(count=len(df)), "")
        except Exception as e:
            buf = six.StringIO()
            traceback.print_exc(file=buf)
            error_info = buf.getvalue()
            insert_log_table('scrapy:malaysia:mpob_stock.py', 'T_AP_MYS_STOCK_REGION', rsp.meta['start_time'], '失败', '合入数据', str(error_info))
        upload_csv_to_ftp(filename, self.name, get_project_settings().get('FTP_SETTINGS'))

    def parse_refinery_table(self, rsp):
        year = rsp.meta.get('YEAR')
        category = rsp.meta.get('CATEGORY')
        self.log('parse_refinery_table: [%s] [%s] %s' % (year, category, rsp), level=logging.INFO)
        df_list = pd.read_html(rsp.text, header=0, flavor='bs4')
        df_list = [self.trim_header(df, 'Products') for df in df_list if 'Products' in df.columns]
        df = pd.merge(df_list[0], df_list[1], on='Products', how='outer')
        df = self.transpose_date(df, 'Products')
        df.rename(columns={'Products': 'PRODUCT'}, inplace=True)
        df['DATADATE'] = pd.to_datetime(df['DATADATE'], format="%Y-%m")
        df['UNIT'] = 'TONNES'
        df['SOURCE'] = self.DATA_SOURCE
        df['SUPPLIER'] = self.DATA_SUPPLIER
        df.dropna(axis=0, subset=['VALUE'], inplace=True)
        df.set_index(['DATADATE', 'PRODUCT'], inplace=True)
        filename = os.path.join(self.temporary_dir(), '%s_%s.csv' % (category, year))
        df.to_csv(filename)
        try:
            merge_db_oracle_dataframe(df, 'T_AP_MYS_STOCK_REFINERY', get_project_settings().get('DATABASE_URI'))
            insert_log_table('scrapy:malaysia:mpob_stock.py', 'T_AP_MYS_STOCK_REFINERY', rsp.meta['start_time'], '成功', '合入{count}条数据'.format(count=len(df)), "")
        except Exception as e:
            buf = six.StringIO()
            traceback.print_exc(file=buf)
            error_info = buf.getvalue()
            insert_log_table('scrapy:malaysia:mpob_stock.py', 'T_AP_MYS_STOCK_REFINERY', rsp.meta['start_time'], '失败', '合入数据', str(error_info))
        
        upload_csv_to_ftp(filename, self.name, get_project_settings().get('FTP_SETTINGS'))

    @staticmethod
    def trim_header(df_hor, header):
        months = ['NULL', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        # remove last 2 columns
        df = df_hor.iloc[:, :-2]
        month_to_year = df[df[header] == header].iloc[:, 1:].to_dict(orient='records')[0]
        # print(month_to_year)
        # remove first row
        df = df[df[header] != header]
        columns = []
        for c in df.columns:
            if c in month_to_year:
                new_name = '%s-%s' % (int(month_to_year[c]), months.index(c[0:3]))
            else:
                new_name = c
            columns.append(new_name)
        df.columns = columns
        return df

    @staticmethod
    def transpose_date(df_hor, header):
        datas = []
        for r in df_hor.itertuples():
            header_value = None
            for i, c in enumerate(df_hor.columns):
                if c == header:
                    header_value = r[i+1]
                else:
                    datas.append({
                        'DATADATE': c,
                        header: header_value,
                        'VALUE': r[i+1],
                    })
        return pd.DataFrame(datas)
