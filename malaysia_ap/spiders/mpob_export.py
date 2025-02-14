# coding=utf8
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

class PalmOilExportSpider(scrapy.Spider):
    name = 'mpob_export'
    DATA_SOURCE = 'https://bepi.mpob.gov.my/index.php/export'
    login_url = 'https://bepi.mpob.gov.my/index.php/component/users/login'
    DATA_SUPPLIER = 'MPOB'

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
            Export of Palm Oil by Destinations 2022
            Monthly Export of Oil Palm Products 2022
            Palm Oil Export by Major Ports 2022
        """
        table_parsers = {
            'Destinations': self.parse_export_destinations_table,
            'Products': self.parse_export_products_table,
            'Ports': self.parse_export_ports_table,
        }
        title_list = response.xpath('//ul[@class="mod-articlescategory category-module mod-list"]/li/ul/li/a/text()').getall()
        hlink_list = response.xpath('//ul[@class="mod-articlescategory category-module mod-list"]/li/ul/li/a/@href').getall()
        self.log('hlink_list=%s, hlink_list=%s' % (title_list, hlink_list), level=logging.INFO)
        for pair in zip(title_list, hlink_list):
            title = pair[0].strip()
            url = response.urljoin(pair[1])
            year = title.split()[-1]
            category = title.split()[-2]
            self.log('title=%s, url=%s' % (title, url), level=logging.INFO)
            # print(title, category, year)
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

    def parse_export_destinations_table(self, rsp):
        self.log('parse_export_destinations_table: %s' % rsp, level=logging.INFO)
        year = rsp.meta.get('YEAR')
        category = rsp.meta.get('CATEGORY')
        df_list = pd.read_html(rsp.text, header=0, flavor='bs4')
        # GLOBAL
        df1 = self.transform(df_list[0].iloc[:, :-2], 'COUNTRY', year)
        df1['REGION'] = 'GLOBAL'
        # EU Country
        df2 = self.transform(df_list[1].iloc[:, :-2], 'COUNTRY', year)
        df2['REGION'] = 'EU Country'
        # concat
        df = pd.concat([df1, df2])
        df['UNIT'] = 'TONNES'
        df['DATADATE'] = pd.to_datetime(df['DATADATE'], format='%Y-%m')
        df['SOURCE'] = self.DATA_SOURCE
        df['SUPPLIER'] = self.DATA_SUPPLIER
        df.dropna(axis=0, subset=['VALUE'], inplace=True)
        df.set_index(['DATADATE', 'REGION', 'COUNTRY'], inplace=True)
        filename = os.path.join(self.temporary_dir(), '%s_%s.csv' % (category, year))
        df.to_csv(filename)
        try:
            merge_db_oracle_dataframe(df, 'T_AP_MYS_EXPORT_DEST', get_project_settings().get('DATABASE_URI'))
            insert_log_table('scrapy:malaysia:mpob_export.py', 'T_AP_MYS_EXPORT_DEST', rsp.meta.get('start_time'), '成功', '合入{count}条数据'.format(count=len(df)), "")
        except Exception as e:
            buf = six.StringIO()
            traceback.print_exc(file=buf)
            error_info = buf.getvalue()
            insert_log_table('scrapy:malaysia:mpob_export.py', 'T_AP_MYS_EXPORT_DEST', rsp.meta.get('start_time'), '失败', '合入数据', str(error_info))
        
        upload_csv_to_ftp(filename, self.name, get_project_settings().get('FTP_SETTINGS'))

    def parse_export_products_table(self, rsp):
        self.log('parse_export_products_table: %s' % rsp, level=logging.INFO)
        year = rsp.meta.get('YEAR')
        category = rsp.meta.get('CATEGORY')
        df = pd.read_html(rsp.text, header=0, flavor='bs4')[0]
        df = df.iloc[:, :-1]
        # Unit 'Tonnes'
        df1 = df[df['UNIT'] == 'Tonnes'].copy()
        df1.drop(columns=['UNIT'], inplace=True)
        df1 = self.transform(df1, 'PRODUCT', year)
        df1['UNIT'] = 'TONNES'
        #  Unit 'RM Mil'
        df2 = df[df['UNIT'] == 'RM Mil'].copy()
        df2.drop(columns=['UNIT'], inplace=True)
        df2 = self.transform(df2, 'PRODUCT', year)
        df2['UNIT'] = 'RM MIL'
        # concat
        df = pd.concat([df1, df2])
        df['DATADATE'] = pd.to_datetime(df['DATADATE'], format="%Y-%m")
        df['SOURCE'] = self.DATA_SOURCE
        df['SUPPLIER'] = self.DATA_SUPPLIER
        df.dropna(axis=0, subset=['VALUE'], inplace=True)
        df.set_index(['DATADATE', 'PRODUCT', 'UNIT'], inplace=True)
        filename = os.path.join(self.temporary_dir(), '%s_%s.csv' % (category, year))
        df.to_csv(filename)
        try:
            merge_db_oracle_dataframe(df, 'T_AP_MYS_EXPORT_PRODUCT', get_project_settings().get('DATABASE_URI'))
            insert_log_table('scrapy:malaysia:mpob_export.py', 'T_AP_MYS_EXPORT_PRODUCT', rsp.meta.get('start_time'), '成功', '合入{count}条数据'.format(count=len(df)), "")
        except Exception as e:
            buf = six.StringIO()
            traceback.print_exc(file=buf)
            error_info = buf.getvalue()
            insert_log_table('scrapy:malaysia:mpob_export.py', 'T_AP_MYS_EXPORT_PRODUCT', rsp.meta.get('start_time'), '失败', '合入数据', str(error_info))
        upload_csv_to_ftp(filename, self.name, get_project_settings().get('FTP_SETTINGS'))

    def parse_export_ports_table(self, rsp):
        self.log('parse_export_ports_table: %s' % rsp, level=logging.INFO)
        year = rsp.meta.get('YEAR')
        category = rsp.meta.get('CATEGORY')
        df = pd.read_html(rsp.text, header=0, flavor='bs4')[0]
        df = self.transform(df.iloc[:, :-1], 'PORT', year)
        df['UNIT'] = 'TONNES'
        df['DATADATE'] = pd.to_datetime(df['DATADATE'], format="%Y-%m")
        df['SOURCE'] = self.DATA_SOURCE
        df['SUPPLIER'] = self.DATA_SUPPLIER
        df.dropna(axis=0, subset=['VALUE'], inplace=True)
        df.set_index(['DATADATE', 'PORT'], inplace=True)
        filename = os.path.join(self.temporary_dir(), '%s_%s.csv' % (category, year))
        df.to_csv(filename)
        try:
            merge_db_oracle_dataframe(df, 'T_AP_MYS_EXPORT_PORT', get_project_settings().get('DATABASE_URI'))
            insert_log_table('scrapy:malaysia:mpob_export.py', 'T_AP_MYS_EXPORT_PORT', rsp.meta.get('start_time'), '成功', '合入{count}条数据'.format(count=len(df)), "")
        except Exception as e:
            buf = six.StringIO()
            traceback.print_exc(file=buf)
            error_info = buf.getvalue()
            insert_log_table('scrapy:malaysia:mpob_export.py', 'T_AP_MYS_EXPORT_PORT', rsp.meta.get('start_time'), '失败', '合入数据', str(error_info))
        upload_csv_to_ftp(filename, self.name, get_project_settings().get('FTP_SETTINGS'))

    @staticmethod
    def transform(df_hor, header, year):
        months = ['NULL', 'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUNE', 'JULY', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
        datas = []
        for r in df_hor.itertuples():
            header_value = None
            for i, c in enumerate(df_hor.columns):
                if c == header:
                    header_value = r[i+1]
                else:
                    datas.append({
                        header: header_value,
                        'DATADATE': '{0}-{1}'.format(year, months.index(c)),
                        'VALUE': r[i+1],
                    })
        df = pd.DataFrame(datas)
        df.dropna(axis=0, subset=['VALUE'], inplace=True)
        return df
