# coding=utf-8
import datetime
import os
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

            h.append((lastmod, loc,category,yrs,qtr))

        df = pd.DataFrame(h, columns=['lastmod', 'sitemap_url','category','yrs','qtr'])
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
    def get_one(obj,table='sitemap_before2015', db='EDAGR'):
        sql = f'select * from {db}.{table} where status = 0 '
        return obj.sql2data(sql)
if __name__ == '__main__':
    # ParserFormBeFore2015.update(sitemap='https://www.sec.gov/Archives/edgar/daily-index/sitemap.xml',
    #                             table='sitemap_before2015', db='EDAGR')
    ParseFullIndex
    pass
