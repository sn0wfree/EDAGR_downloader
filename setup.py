# coding=utf-8
from setuptools import setup, find_packages

__version__, __author__ = '0.0.1', 'snowfree'

setup(
    name="EDAGR_downloader",
    version=__version__,
    keywords=("EDAGR_downloader", "Databases"),
    description="EDAGR_downloader programming",
    long_description='wsss',
    license="MIT Licence",

    url="http://github.com/sn0wfree",
    author=__author__,
    author_email="snowfreedom0815@gmail.com",

    packages=find_packages(),
    include_package_data=True,
    platforms="any",
    install_requires=['SQLAlchemy>=1.3.13', 'PyMySQL>=0.9.3',
                      'requests>=2.22.0',

                      'requests_cache==0.5.2',
                      'pandas>=0.25.3',

                      ],

    )
