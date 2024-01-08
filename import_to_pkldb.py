import sys
import time
import logging
import datetime
from pathlib import Path
import numpy as np
import pandas as pd
from pymongo.errors import DuplicateKeyError
from mongoengine import *

from models import *

# 设置logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(name)s: %(levelname)s: %(message)s',
                    datefmt='%y-%m-%d %H:%M:%S', stream=sys.stdout)
logger = logging.getLogger()

# 连接数据库
URI_ticks = 'mongodb://ticks:11112222@mongodb:27017/ticks'
# URI_kline = 'mongodb://127.0.0.1:6007/kline'
connect(host=URI_ticks,  alias='ticks')


year = '2019'
mkt = '5'
cat = "*"
# import_cats = ['RR']
files = []
base_dir = Path(f'/data/raw_data/')
for _file in base_dir.glob(f'{year}/{mkt}/{cat}/*/*/*/*.spt'):     # {inst}/{cat}/
    # print(_file)
    # continue
    # '/data/raw_data/2022/5/UR/UR2208/2022/202206/20220606.spt'
    relative_file = str(_file.relative_to(base_dir))
    _year, _mkt, _cat, _inst, _, _month, _day = relative_file.split('/')
    _day = _day.split('.')[0]

    doc = TickFilesDoc.objects(path=relative_file).first()
    if not doc or not doc.zip_path:
        print(_file)
        
    if doc:
        if not doc.zip_path:
            doc.csv_to_pickle()
    else:
        # ('2022', '5', 'UR', 'UR2208', '2022', '202206', '20220606')
        try:
            doc = TickFilesDoc(**dict(
                MarketID = int(_mkt),                   # 市场代码(上证1, 深证2, 中金所3, 上期4, 郑商5, 大商6)
                category = _cat,                # 合约品种: AU/AG/CU...
                InstrumentID = _inst,           # 合约代码
                subID = _inst[-4:],                   # 子代码(日期)，从InstrumentID中提取。2210
            
                data_type = 'tick',               # tick/9999/0000/1day_k... subID: 9999表示主力dominant，0000表示指数index
                year = _year,                   # '2019'
                month = _month,                  # '201909'
                day = _day,                  # '20190925'
                # 原始文件
                path = relative_file,         # 存放相对路径
                size = _file.stat().st_size,
            )).save()
            doc.csv_to_pickle()
            # doc.save()
        except NotUniqueError:    # SystemError
            logger.warning(f'{_file} has been imported.')
        
    files.append(_file)
    # break
    
