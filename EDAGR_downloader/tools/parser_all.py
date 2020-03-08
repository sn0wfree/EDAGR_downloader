# coding=utf-8
import datetime
import os
import time
import uuid

import pandas as pd
import requests
import requests_cache
from lxml.etree import HTML

refresh = 24 * 60 * 60
now_ym = datetime.datetime.now().strftime("%Y-%m")
requests_cache.install_cache(f'{now_ym}.db', expire_after=refresh)
from EDAGR_downloader.tools.db import Source


def load(file_path):
    with open(file_path, 'rb') as f:
        return f.readlines()


class ParserFormBeFore2015(object):
    @staticmethod
    def get_sitemap(sitemap='https://www.sec.gov/Archives/edgar/daily-index/sitemap.xml', tofile=False):
        name = os.path.split(sitemap)[-1]
        r = requests.get(sitemap, stream=True)
        if tofile:
            with open(name, 'wb') as f:
                f.write(r.content)
        else:
            return r.content

    @staticmethod
    def parser_sitemap(sitemap):
        sitemap_obj = HTML(sitemap)
        sites = sitemap_obj.xpath('*/*/sitemap')
        h = []
        for site in sites:
            lastmod = site.xpath('lastmod/text()')[0]
            loc = site.xpath('loc/text()')[0]
            category = loc.split('/')[5]
            yrs = loc.split('/')[6]
            qtr = loc.split('/')[7]

            h.append((lastmod, loc, category, yrs, qtr))

        df = pd.DataFrame(h, columns=['lastmod', 'sitemap_url', 'category', 'yrs', 'qtr'])
        df['lastmod'] = pd.to_datetime(df['lastmod'])
        df['uuid'] = df['sitemap_url'].map(lambda x: uuid.uuid5(uuid.NAMESPACE_DNS, str(x)).hex)
        return df

    @staticmethod
    def upload(df, obj=Source.tasks_links_yrs, table='sitemap_before2015', db='EDAGR'):
        obj.df2sql(df, table=table, db=db, csv_store_path='../')

    @classmethod
    def update(cls, sitemap='https://www.sec.gov/Archives/edgar/daily-index/sitemap.xml', table='sitemap_before2015',
               db='EDAGR'):
        sitemap = cls.get_sitemap(sitemap=sitemap, tofile=False)
        sitemap_df = cls.parser_sitemap(sitemap)
        cls.upload(sitemap_df, obj=Source.tasks_links_yrs, table=table, db=db)


class ParseFullIndex(object):
    @staticmethod
    def get_all(obj, table='sitemap_before2015', db='EDAGR'):
        sql = f'select * from {db}.{table} where status = 0 and category = "full-index" order by yrs asc'
        return obj.sql2data(sql)

    @staticmethod
    def parser_urlset(urlset):
        sitemap_obj = HTML(urlset)
        sites = sitemap_obj.xpath('*/*/url')
        h = []
        for site in sites:
            lastmod = site.xpath('lastmod/text()')[0]
            loc = site.xpath('loc/text()')[0]
            changefreq = site.xpath('changefreq/text()')[0]
            priority = site.xpath('priority/text()')[0]
            h.append((lastmod, loc, changefreq, priority))

        df = pd.DataFrame(h, columns=['lastmod', 'sitemap_url', 'changefreq', 'priority'])
        df['lastmod'] = pd.to_datetime(df['lastmod'])
        df['single_uuid'] = df['sitemap_url'].map(lambda x: uuid.uuid5(uuid.NAMESPACE_DNS, str(x)).hex)
        return df

    @classmethod
    def run_task(cls, tasks, session=None):
        session = requests if session is None else session
        for row, task in tasks.iterrows():
            sitemap_url = task['sitemap_url']
            uuid = task['uuid']
            yrs = task['yrs']
            qtr = task['qtr']
            r = session.get(sitemap_url)
            df = cls.parser_urlset(r.content)
            df['sitemap_uuid'] = uuid
            df['yrs'] = yrs
            df['qtr'] = qtr
            yield df, uuid

    @classmethod
    def main_task(cls, obj=Source.tasks_links_yrs, tasks_table='sitemap_before2015', tasks_db='EDAGR'):
        session = requests.sessions.session()
        tasks = cls.get_all(obj, table=tasks_table, db=tasks_db)
        for df, uu_id in cls.run_task(tasks, session=session):
            obj.df2sql(df, 'final_file_url', db=tasks_db, csv_store_path='/tmp/')
            sql = f"UPDATE {tasks_db}.{tasks_table} SET status = 1 WHERE  status =  0 and uuid='{uu_id}' "
            print(f'downloaded {uu_id}')
            obj.Excutesql(sql)
            print('sleep 5s')
            time.sleep(5)


if __name__ == '__main__':
    # ParserFormBeFore2015.update(sitemap='https://www.sec.gov/Archives/edgar/daily-index/sitemap.xml',
    #                             table='sitemap_before2015', db='EDAGR')
    obj = Source.tasks_links_yrs
    ParseFullIndex.main_task(obj, tasks_table='sitemap_before2015', tasks_db='EDAGR')
    pass
