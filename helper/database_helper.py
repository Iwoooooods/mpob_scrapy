# -*- coding: utf-8 -*-
import os
import logging
import datetime
import cx_Oracle
os.environ["NLS_LANG"] = ".UTF8"
import pandas as pd
from scrapy.utils.project import get_project_settings


def merge_db_oracle(datas, table, meta, conn, insert=False, istimestamp=False):
    '''
    update or insert
    :param datas: a list of dict
    :param table: tablename
    :param meta: like {'col1': 'key', 'col2': 'key', 'col3': None, 'col4': None]
    :param conn: 'user/pwd@ip:port/db'
    :param insert: direct insert, performance better than update_or_insert
    :param istimestamp: cx_Oracle datetime type default date (no microsecond), set true to support timestamp
    :return:
    '''
    keys = []
    cols = []
    cols_update = []
    count = 0

    for k, v in meta.items():
        cols.append(k)
        if v is not None and v == 'key':
            keys.append(k)
        else:
            cols_update.append(k)

    sql = '''
        MERGE INTO {0} T
        USING (SELECT {1} FROM DUAL) S
           ON ({2})
         WHEN MATCHED THEN
            UPDATE
               SET {3}
         WHEN NOT MATCHED THEN
            INSERT ({4})
            VALUES ({5})
    '''.format(
        table,
        ','.join([':{0} as {0}'.format(field) for field in cols]),
        ' AND '.join(['(T.{0} = S.{0} OR (T.{0} IS NULL AND S.{0} IS NULL))'.format(field) for field in keys]),
        ','.join(['T.{0} = S.{0}'.format(field) for field in cols_update]),
        ','.join(['{0}'.format(col) for col in cols]),
        ','.join(['S.{0}'.format(field) for field in cols])
    )
    if insert:
        sql = '''
            INSERT INTO {0} 
                ({1}) 
              VALUES 
                ({2})
        '''.format(
            table,
            ','.join(['{0}'.format(col) for col in cols]),
            ','.join([':{0}'.format(field) for field in cols])
        )
        
    conn = cx_Oracle.connect(conn)
    cur = conn.cursor()
    cur.prepare(sql)
    if istimestamp:
        cur2 = conn.cursor()
        cur2.execute('SELECT * FROM {0} WHERE 1=2'.format(table))

        params = {}
        metakeys = [k.lower() for k in meta.keys()]
        for d in cur2.description:
            if d[0].lower() in metakeys:
                params[d[0].lower()] = d[1]

        cur.setinputsizes(**params)
        for row in datas:
            cur.executemany(None, [row,])
            count += 1
    else:
        cur.executemany(None, datas)
        count = len(datas)
        
    conn.commit()
    # conn.close()
    return count


def merge_db_oracle_tmp(datas, table, meta, conn, istimestamp=False, onlyupdate=False, rebuildtmp=False):
    '''
    update or insert
    :param datas: a list of dict
    :param table: tablename
    :param meta: like {'col1': 'key', 'col2': 'key', 'col3': None, 'col4': None]
    :param conn: 'user/pwd@ip:port/db'
    :param insert: direct insert, performance better than update_or_insert
    :param istimestamp: cx_Oracle datetime type default date (no microsecond), set true to support timestamp
    :return:
    '''
    keys = []
    cols = []
    cols_update = []
    count = 0
    
    for k, v in meta.items():
        cols.append(k)
        if v is not None and v == 'key':
            keys.append(k)
        else:
            cols_update.append(k)

    table_nospace = table.split('.')[-1]

    sqlinsert = '''
        INSERT INTO TMP_{0} 
            ({1}) 
          VALUES 
            ({2})
    '''.format(
        table_nospace,
        ','.join(['{0}'.format(col) for col in cols]),
        ','.join([':{0}'.format(field) for field in cols])
    )
    sqlmerge = '''
        MERGE INTO {0} T
        USING (SELECT * FROM TMP_{1}) S
           ON ({2})
         WHEN MATCHED THEN
            UPDATE
               SET {3}
         WHEN NOT MATCHED THEN
            INSERT ({4})
            VALUES ({5})
    '''.format(
        table,
        table_nospace,
        ' AND '.join(['(T.{0} = S.{0} OR (T.{0} IS NULL AND S.{0} IS NULL))'.format(field) for field in keys]),
        ','.join(['T.{0} = S.{0}'.format(field) for field in cols_update]),
        ','.join(['{0}'.format(col) for col in cols]),
        ','.join(['S.{0}'.format(field) for field in cols])
    )
    if onlyupdate:
        sqlmerge = '''
            MERGE INTO {0} T
            USING (SELECT * FROM TMP_{1}) S
               ON ({2})
             WHEN MATCHED THEN
                UPDATE
                   SET {3}
        '''.format(
            table,
            table_nospace,
            ' AND '.join(['(T.{0} = S.{0} OR (T.{0} IS NULL AND S.{0} IS NULL))'.format(field) for field in keys]),
            ','.join(['T.{0} = S.{0}'.format(field) for field in cols_update]),
        )
    conn = cx_Oracle.connect(conn)
    cur = conn.cursor()

    try:
        cur.execute('SELECT * FROM TMP_{0} WHERE 1=2'.format(table_nospace))
        if rebuildtmp:
            cur.execute('DROP TABLE TMP_{0}'.format(table_nospace))
            cur.execute('SELECT * FROM TMP_{0} WHERE 1=2'.format(table_nospace))
    except:
        logging.info('create tmp_{0}'.format(table_nospace))
        cur.execute('CREATE GLOBAL TEMPORARY TABLE TMP_{0} ON COMMIT DELETE ROWS AS SELECT * FROM {1} WHERE 1=2'.format(table_nospace, table))
        cur.execute("SELECT COLUMN_NAME FROM USER_TAB_COLS WHERE USER_TAB_COLS.TABLE_NAME = 'TMP_{0}' AND NULLABLE = 'N'".format(table_nospace))
        cur.execute("ALTER TABLE TMP_{0} {1}".format(table_nospace, ' '.join(['MODIFY {0} NULL'.format(r[0]) for r in cur])))
    cur.prepare(sqlinsert)
    if istimestamp:
        cur2 = conn.cursor()
        cur2.execute('SELECT * FROM TMP_{0} WHERE 1=2'.format(table_nospace))
        cur.setinputsizes(**{d[0].lower(): d[1] for d in cur2.description})
        for row in datas:
            cur.executemany(None, [row,])
            count += 1
    else:
        cur.executemany(None, datas)
        count = len(datas)

    cur.execute(sqlmerge)
    conn.commit()
    # conn.close()
    return count


def merge_db_oracle_dataframe(df, table, conn, insert=False, istimestamp=False, istmp=False, onlyupdate=False, rebuildtmp=False):
    '''
    update or insert from dataframe
    :param df: dataframe
    :param table: tablename
    :param conn: 'user/pwd@ip:port/db'
    :param insert: direct insert, performance better than update_or_insert
    :param istimestamp: cx_Oracle datetime type default date (no microsecond), set true to support timestamp
    :param istmp: insert into Oracle temporary table, then merge into real table
    :param onlyupdate: just update, dont insert
    :return:
    '''
    meta = {v: None for v in df.columns}
    keys = {v: 'key' for v in df.index.names if v is not None}
    meta.update(keys)
    df = df.reset_index(drop=len(keys)==0)

    rows = df.to_dict(orient='records')
    for row in rows:
        for key in meta.keys():
            if pd.isnull(row[key]):
                row[key] = None
    if istmp:
        return merge_db_oracle_tmp(rows, table, meta, conn, istimestamp, onlyupdate, rebuildtmp)
    else:
        return merge_db_oracle(rows, table, meta, conn, insert, istimestamp)


def merge_db_mysql(datas, table, conn):
    '''
    update or insert
    :param datas: a list of dict
    :param table: table name
    :param conn: 'user/pwd@ip:port/db'
    :return:
    '''
    import pymysql
    cols = datas[0].keys()
    sql = '''
        REPLACE INTO {0}({1}) VALUES ({2})
    '''.format(
        table,
        ','.join(cols),
        ','.join(['%({0})s'.format(field) for field in cols]),
    )
    conn = pymysql.connect(**conn)
    cur = conn.cursor()
    cur.executemany(sql, datas)
    conn.commit()
    conn.close()


def merge_db_sqlite(datas, table, conn):
    import sqlite3
    cols = datas[0].keys()
    sql = '''
        REPLACE INTO {0}({1}) VALUES ({2})
    '''.format(
        table,
        ','.join(cols),
        ','.join([':{0}'.format(field) for field in cols]),
    )
    conn = sqlite3.connect(conn)
    cur = conn.cursor()
    sel = cur.execute("SELECT COUNT(*) AS COUNT FROM SQLITE_MASTER WHERE TYPE='table' AND NAME='{0}' COLLATE NOCASE".format(table))
    if sel.fetchone()[0] == 0:
        logging.warning('no table {0}'.format(table))
        pass

    cur.executemany(sql, datas)
    conn.commit()
    conn.close()


def execute_sql(sql_str, conn_str):
    try:
        conn = cx_Oracle.connect(conn_str)
        cur = conn.cursor()
        cur.execute(sql_str)
        conn.commit()
        # conn.close()
    except Exception as e:
        logging.error(e)



if __name__ =="__main__":
    # print(get_daily_so(datetime.date(2020, 1, 8)))
    datas = [
        {'id': 1, 'fundaccount': 10, 'enable': 'False'},
        {'id': 2, 'fundaccount': 'absbsbs', 'enable': 1},
    ]
    conn = {
        'host': '10.10.50.61',
        'user': 'ids',
        'password': 'idsdev123',
        'database': 'ktsops_dev',
        'charset': 'utf8'
    }
    merge_db_mysql(datas, 't_fund_info', conn)

    datas = [
        {'Time': 1, 'DateTime': datetime.datetime.now(), 'TradingDay': '20200715'},
        {'Time': 2, 'DateTime': datetime.datetime.now(), 'TradingDay': '20200716'},
    ]
    merge_db_sqlite(datas, 'MinuteOne2', r'D:\data\test.db')

def insert_log_table(script_name,data_table_name,start_time,result,action,remark):
    end_time = pd.Timestamp(pd.Timestamp.now())
    sys_date = pd.to_datetime(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    run_duration = round((end_time - start_time).total_seconds(), 6)
    log_value = [script_name,
                 data_table_name,
                 'dolphinscheduler',
                 str(start_time),
                 str(end_time),
                 run_duration,
                 action,
                 result,
                 str(sys_date),
                 remark]

    conn = get_project_settings().get('DATABASE_URI')
    conn = cx_Oracle.connect(conn)
    cur = conn.cursor()

    insert_sql = """INSERT INTO SCRIPT_RUN_LOG (SCRIPT_NAME, TABLE_NAME, SERVER_IP, START_TIME, END_TIME, DURATION, 
    ACTIONS, RESULT, INSERT_DT, REMARK) VALUES (:1, :2, :3, TO_TIMESTAMP(:4, 'YYYY-MM-DD HH24:MI:SS.FF6'),
    TO_TIMESTAMP(:5, 'YYYY-MM-DD HH24:MI:SS.FF6'), :6, :7, :8, TO_TIMESTAMP(:9, 'YYYY-MM-DD HH24:MI:SS'), :10)"""
    cur.execute(insert_sql, log_value)
    conn.commit()
    conn.close()
    print result,'日志插入数据库'