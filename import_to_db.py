import sys
import time
import logging
import datetime
from pathlib import Path
import numpy as np
import pandas as pd
from mongoengine import *

# import dask
# import dask.dataframe as dd
# import dask.array as da
# from dask.diagnostics import ProgressBar

from models import *

# 配置dask
# dask.config.set(scheduler='processes')
# pbar = ProgressBar()
# pbar.register()

# 设置logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(name)s: %(levelname)s: %(message)s',
                    datefmt='%y-%m-%d %H:%M:%S', stream=sys.stdout)
logger = logging.getLogger()

# 连接数据库
URI_ticks = 'mongodb://127.0.0.1:6007/ticks'
# URI_kline = 'mongodb://127.0.0.1:6007/kline'
connect(host=URI_ticks,  alias='ticks')
# connect(host=URI_kline,  alias='kline')


def load_df(tmpPath):
    pd_data = pd.read_csv(tmpPath, names=[
                        'InstrumentID', 'MarketID', 'LastPrice', 'LastVolume', 'hhmmss', 'Reserved', 'UpdateTime', 'AskPrice1',
                        'AskVolume1', 'BidPrice1', 'BidVolume1', 'AskPrice2', 'AskVolume2', 'BidPrice2', 'BidVolume2',
                        'AskPrice3', 'AskVolume3', 'BidPrice3', 'BidVolume3', 'AskPrice4', 'AskVolume4', 'BidPrice4', 'BidVolume4',
                        'AskPrice5', 'AskVolume5', 'BidPrice5', 'BidVolume5', 'OpenInterest', 'Turnover', 'AvePrice', 'invol', 'outvol',
                        'Attr1', 'Volume1', 'Attr2', 'Volume2', 'HighestPrice', 'LowestPrice', 'SettlePrice', 'OpenPrice', 'fill'
                    ], low_memory=False, parse_dates=['UpdateTime'], dtype={'hhmmss': str})
    pd_data.drop([
                    'Reserved', 'AskPrice2', 'AskVolume2', 'BidPrice2', 'BidVolume2',
                    'AskPrice3', 'AskVolume3', 'BidPrice3', 'BidVolume3', 'AskPrice4', 'AskVolume4', 'BidPrice4', 'BidVolume4',
                    'AskPrice5', 'AskVolume5', 'BidPrice5', 'BidVolume5', 'invol', 'outvol',
                    'Attr1', 'Volume1', 'Attr2', 'Volume2', 'SettlePrice', 'fill'], axis=1, inplace=True)

    pd_data['subID'] = pd_data.InstrumentID.str[-4:]

    # pd_data = pd_data.drop_duplicates(['hhmmss'], keep='last')
    # pd_data_market = pd_data.reset_index(drop=True)
    # pd_data_market.drop(['hhmmss', 'LastVolume', 'OpenInterest', 'Turnover', 'AvePrice'],axis=1,inplace=True)
    # a = pd_data_market.iloc[-1]
    # c = pd_data_market.UpdateTime[0]
    # d = c.split(' ')
    # a.UpdateTime = d[0] + ' ' + '23:30:00.000'
    # pd_data_market = pd_data_market.append(a,ignore_index=True)

    return pd_data


if __name__ == "__main__":
    dbg = False
    dbg = True

    cat = 'HC'
    d_doc = get_dyn_ticks_doc(cat)

    for year in ['2014', '2015', '2016', '2017', '2018_whole']:
        base_dir = Path(f'/home/history_data/tick/{year}/4/{cat}/')
        dir_num = len(list(base_dir.iterdir()))
        logger.info(f'[{year}-{cat}]: dir_num={dir_num}].')

        dir_cnt = 0
        spt_cnt = 0
        print_cnt = 0
        st_dir = time.time()
        for subdir in base_dir.iterdir():
            dir_cnt += 1

            # 9999是主力，0000是指数
            if subdir.stem[-4:] in ['0000', '9999']:
                logger.info(f'[{year}][{dir_cnt}/{dir_num}]: Ignore {subdir}!')
                continue

            res = subdir.glob('*/*/*.spt')
            res = list(res)
            total = len(res)
            logger.info(f'[{year}][{dir_cnt}/{dir_num}]: Process {subdir}, total={total}.')

            cnt = 0
            st = time.time()
            for _file in res:
                cnt += 1
                spt_cnt += 1
                print_cnt += 1
                try:
                    pd_data = load_df(_file)
                except Exception:
                    if dbg:
                        logger.error(f'{_file}', exc_info=0)
                    else:
                        logger.error(f'[{year}] Exception: {_file}', exc_info=1)
                        continue

                if not dbg:
                    for i in range(pd_data.shape[0]):
                        try:
                            d_doc(**pd_data.iloc[i]).save()
                        except Exception:
                            logger.error(f'[{year}] Exception: {_file}: Line={cnt}. {pd_data.iloc[i]}')
                            continue

                    exec_time = time.time() - st
                    if print_cnt >= 20 or exec_time > 80:
                        logger.info(f'[{year}][{dir_cnt}/{dir_num}][{cnt}/{total}]: subdir={subdir}, spt_cnt={spt_cnt}, Time={"%.1fs" % exec_time}')
                        st = time.time()
                        print_cnt = 0

            if dbg:
                logger.info(f'[{year}][{dir_cnt}/{dir_num}]: {subdir}: spt_cnt={spt_cnt}, Time={"%.1fs" % (time.time() - st_dir)}')
                st_dir = time.time()
