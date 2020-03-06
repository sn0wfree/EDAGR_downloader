# coding=utf-8
import time
import uuid

import os
import pandas as pd
import requests

from EDAGR_downloader.db import Source
from EDAGR_downloader.header import Header


def get_task(obj, db, table):
    sql = f'select * from {db}.{table} where status = 0 limit 1'
    rest = obj.sql2data(sql)
    if rest.empty:
        msg = 'no available task'
        print(msg)
        return 'empty', msg
    else:
        return 'no empty', rest


class TaskCreatorYrs(object):
    @staticmethod
    def list_links_year(base_url='https://www.sec.gov/Archives/edgar/daily-index/', start_year='1994', end_year='now'):
        base_url = base_url.rstrip('/')
        if end_year.lower() == 'now':
            end_year = pd.datetime.now().year
        else:
            end_year = int(end_year)
        start_year = int(start_year)
        for year in range(start_year, end_year + 1):
            yield f'{base_url}/{year}/', year

    @staticmethod
    def list_links_qtr(base_url, xpath="//*[@id='main-content']/table/tr"):
        obj = Header.get(base_url)
        return obj.xpath(xpath)

    @staticmethod
    def _detect_obj(tr_obj):
        children = tr_obj.getchildren()

        if children[0].tag == 'th':
            for ch in children:
                yield ch.tag, ch.text
        elif children[0].tag == 'td':
            for ch in children:
                c = ch.getchildren()
                if len(c) == 0:
                    text = ch.text
                else:

                    text = c[0].attrib['href']
                yield ch.tag, text
        else:
            raise ValueError('children tag is not td or th! but get {}'.format(children[0].tag))

    @classmethod
    def detect_obj_all(cls, tr_objs):
        res = []
        cols = ['col1', 'col2', 'col3']
        for tr_obj in tr_objs:
            rty = list(cls._detect_obj(tr_obj))
            if rty[0][0] == 'th':
                cols = list(map(lambda x: x[1], rty))
            else:
                res.append(list(map(lambda x: x[1], rty)))

        return pd.DataFrame(res, columns=cols)

    @classmethod
    def get_links_qtr_df(cls, url, yrs):
        c = list(cls.list_links_qtr(url))
        g = cls.detect_obj_all(c)
        g['url'] = g['Name'].map(lambda x: url + x)
        g['yrs'] = yrs
        return g

    @classmethod
    def get_links_year_df(cls, base_url='https://www.sec.gov/Archives/edgar/daily-index/', start_year='1994',
                          end_year='now'):
        tasks = cls.list_links_year(base_url=base_url, start_year=start_year, end_year=end_year)
        h = [cls.get_links_qtr_df(url, yr) for url, yr in tasks]

        # f['yrs'] = f['yrs'].astype(np.int32)
        g = pd.concat(h)
        g['Last Modified'] = pd.to_datetime(g['Last Modified'])
        g = g.rename(columns={'Last Modified': 'Last_Modified', 'Name': 'QTR'})
        g['uuid'] = g['url'].map(lambda x: uuid.uuid5(uuid.NAMESPACE_DNS, x).hex)

        return g

    @classmethod
    def upload(cls, df, db='EDAGR', table='tasks_links_yrs'):
        Source.tasks_links_yrs.df2sql(df, db=db, table=table, csv_store_path='./')
        # table, db = None

    @classmethod
    def auto_update_year(cls, base_url='https://www.sec.gov/Archives/edgar/daily-index/'):
        max_yrs = Source.tasks_links_yrs.sql2data('select max(yrs) as yrs from EDAGR.tasks_links_yrs')['yrs'].values[0]

        g = cls.get_links_year_df(base_url=base_url, start_year=str(max_yrs))
        # print(len(str(g['uuid'].values[0])))
        cls.upload(g, db='EDAGR', table='tasks_links_yrs')


class TaskCreatorQtr(object):
    @classmethod
    def get_1_yrs_task(cls, obj=Source.tasks_links_yrs, db='EDAGR', table='tasks_links_yrs'):
        sql = f'select * from {db}.{table} where status = 0 limit 1'
        rest = obj.sql2data(sql)
        if rest.empty:
            msg = 'no available task'
            print(msg)
            return 'empty', msg
        else:
            return 'no empty', rest

    @classmethod
    def get_1_yrs_task_with(cls, obj=Source.tasks_links_yrs, db='EDAGR', table='tasks_links_yrs'):

        status, res = cls.get_1_yrs_task(obj=obj, db=db, table=table)
        try:
            yield status, res
        finally:
            # uuid = res['uuid'].values[0]
            3  # sql = f"select * from {db}.{table} where status = 0 and uuid='{uuid}' "

    @classmethod
    def get(cls, task_df):
        url = task_df['url'].values[0]
        uu_id = task_df['uuid'].values[0]
        qtr = task_df['QTR'].values[0]
        # resp = Header.get(url, session=None)
        yrs = task_df['yrs'].values[0]
        cf = TaskCreatorYrs.get_links_qtr_df(url, yrs)
        cf['uuid_yrs'] = uu_id
        cf['QTR'] = qtr
        cf['uuid_file'] = cf['url'].map(lambda x: uuid.uuid5(uuid.NAMESPACE_DNS, str(x)).hex)
        cf = cf.rename(columns={'Last Modified': 'Last_Modified'})
        cf['Last_Modified'] = pd.to_datetime(cf['Last_Modified'])

        return cf  # uuid, resp

    @classmethod
    def upload(cls, df, db, table, obj):
        obj.df2sql(df, table=table, db=db, csv_store_path='./')

    @classmethod
    def update(cls, db='EDAGR', source_table='tasks_links_yrs', target_table='tasks_links_file'):
        while 1:
            status, task = cls.get_1_yrs_task(db=db, table=source_table)
            if status != 'empty':
                cf = cls.get(task)
                cls.upload(cf, db, target_table, Source.tasks_links_yrs)
                # sql = f'select * from {db}.{table} where status = 0 limit 1'
                print('task: {} done with info[ yrs:{} | qtr:{} ] '.format(task['uuid'][0], task['yrs'][0],
                                                                           task['QTR'][0]))
                uuid = task['uuid'].values[0]
                a = f"UPDATE {db}.{source_table} SET status = 1 WHERE uuid = '{uuid}' and status = 0 "
                # sql = f'select * from {db}.{source_table} where status = 0 limit 1'
                Source.tasks_links_yrs.Excutesql(a)
            else:
                break
            time.sleep(1)


class TaskCreatorfile(object):
    @staticmethod
    def rm_task(res, obj=Source.tasks_links_yrs, db='EDAGR', table='tasks_links_file'):
        uuid = res['uuid_file'].values[0]
        sql = f"UPDATE {db}.{table} SET status = 1 WHERE  status =  0 and uuid_file='{uuid}' "
        obj.Excutesql(sql)
        print(uuid)

    @classmethod
    def download(cls, obj=Source.tasks_links_yrs, db='EDAGR', table='tasks_links_file'):
        while 1:
            status, res = get_task(obj=obj, db=db, table=table)
            if status != 'empty':
                cls.download_file(res)
                cls.rm_task(res, obj=obj, db=db, table=table)
            else:
                break

    # return get_task(obj=obj,db=db,table=table)
    @staticmethod
    def download_file(res):
        url = res['url'][0]
        filename = res['Name'][0]
        yrs = int(res['yrs'][0])
        qtr = res['QTR'][0].rstrip('/')
        current_path = os.getcwd()
        path = f'{current_path}/{yrs}/{qtr}'
        if os.path.exists(path):
            pass
        else:
            os.makedirs(path)
        r = requests.get(url, stream=True)

        file_path = f'{path}/{filename}'
        with open(file_path, "wb") as code:
            for chunk in r.iter_content(chunk_size=1024):  # 边下载边存硬盘
                if chunk:
                    code.write(chunk)


if __name__ == '__main__':
    base_url = 'https://www.sec.gov/Archives/edgar/daily-index/'
    TaskCreatorYrs.auto_update_year(base_url=base_url)
    TaskCreatorQtr.update(db='EDAGR', source_table='tasks_links_yrs', target_table='tasks_links_file')
    # TaskCreatorfile.get_1_task()
    # sql = 'SELECT Size FROM `tasks_links_file`'
    # c = Source.tasks_links_yrs.sql2data(sql)
    TaskCreatorfile.download()
    pass
