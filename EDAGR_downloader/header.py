# coding=utf-8
# -*- coding:utf-8 -*-
import datetime

import requests_cache
from lxml.etree import HTML

refresh = 24 * 60 * 60
now_ym = datetime.datetime.now().strftime("%Y-%m")
requests_cache.install_cache(f'{now_ym}.db', expire_after=refresh)
import requests

# -------------------
__version__ = "2"
__author__ = "sn0wfree"


# -------------------


# url = 'https://www.sec.gov/Archives/edgar/daily-index/'


class HeaderTools(object):
    @staticmethod
    def get_url(url, session=None):
        if session is None:
            session = requests
        resp = session.get(url)
        if resp.status_code != 200:
            raise ValueError(f'status_code is not 200, get {resp.status_code}')
        else:
            print(f'status_code: {resp.status_code}')
        return resp

    @staticmethod
    def parser(resp_text: str):
        c = HTML(resp_text)
        return c

    @staticmethod
    def get_trs(t_obj, xpath):
        return t_obj.xpath(xpath)


class Header(HeaderTools):

    @classmethod
    def get(cls, url, session=None):
        resp = cls.get_url(url, session=session)
        return cls.parser(resp.text)


if __name__ == '__main__':
    url = 'https://www.sec.gov/Archives/edgar/daily-index/2020/'
    resp = Header.get_url(url, session=None)

    c = HTML(resp.text)
    d = c.xpath("//*[@id='main-content']/table/tr")
    print(1)
