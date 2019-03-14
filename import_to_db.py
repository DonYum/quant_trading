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

    base_dir = Path('/home/history_data/tick/2015/4/CU/')

    d_doc = get_dyn_ticks_doc('CU')

    res = base_dir.glob('*/*/*/*.spt')
    res = list(res)
    total = len(res)

    cnt = 0
    st = time.time()
    for _file in res:
        cnt += 1
        try:
            pd_data = load_df(_file)
        except Exception:
            logger.error(f'{_file}', exc_info=1)
            raise

        if not dbg:
            for i in range(pd_data.shape[0]):
                d_doc(**pd_data.iloc[i]).save()

            logger.info(f'[{cnt}/{total}]: {_file}: Time={"%.1fs" % (time.time() - st)}')
            st = time.time()
