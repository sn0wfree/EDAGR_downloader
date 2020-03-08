# coding=utf-8
import sys
sys.path.append('./')
from EDAGR_downloader.tools.tasks import run
if __name__ == '__main__':
    run(base_url='https://www.sec.gov/Archives/edgar/daily-index/', download_base_url=False, download_qtr_url=False, download_file=True)
    pass


