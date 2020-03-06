# coding=utf-8
from EDAGR_downloader.utils.MySQLConn_v004_node import MySQLNode

mysql_settings = dict(host='106.13.205.210', port=3306, user='linlu', passwd='Imsn0wfree', db='EDAGR')


class Source(object):
    tasks_links_yrs = MySQLNode('MySQLNode', **mysql_settings)


if __name__ == '__main__':
    pass
