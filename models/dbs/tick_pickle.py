import datetime
import logging
import pandas as pd
from pathlib import Path
from tqdm import tqdm_notebook as tqdm
from .tick_file_doc import TickFilesDoc
from .conf import *

__all__ = (
        'PickleDbTick', 'PickleDbTicks',
    )

logger = logging.getLogger()


class PickleDbException(Exception):
    pass


# tick存储的操作方法，主要功能是：清洗、入库、读取等
# 提供的方法有：
#   - zip_exists()
#   - del_zip()
#   - load_ticks(): 从pkl文件加载数据
#   - csv_to_pickle(): 根据文件路径保存到pkl文件
#   - _load_df_from_csv(): 加载、清洗csv原始数据
# 入库的方法：
#   - 先生成TickFilesDoc，然后使用`PickleDbTick(tick_doc).csv_to_pickle()`保存。
class PickleDbTick():
    def __init__(self, tick_doc):
        self.tick_doc = tick_doc

        self.rel_file = None   # 相对路径，to save in db
        self.file = None       # 绝对路径
        if tick_doc.zip_path:
            self.rel_file = Path(tick_doc.zip_path)   # 相对路径，to save in db
            self._rel_path = self.rel_file.parent
            self.abs_path = TICKS_PATH / self._rel_path
            self.f_name = self.rel_file.name
            self.file = self.abs_path / self.f_name
        else:
            # mkt / cat / 合约 / day / f'{合约}_{day}_{zip_ver}.pkl'
            # 4/AG/AG1401/20140108/AG1401_20140108_1.pkl
            self._rel_path = Path(f'{tick_doc.MarketID}/{tick_doc.category}/{tick_doc.InstrumentID}/{tick_doc.month}/')
            self.abs_path = TICKS_PATH / self._rel_path
            self.f_name = f'{tick_doc.InstrumentID}_{tick_doc.day}.pkl'
            self.rel_file = self._rel_path / self.f_name   # to save in db
            self.file = self.abs_path / self.f_name

    # 检查zip文件是否存在
    def zip_exists(self):
        zip_exists = self.file.exists()
        if bool(self.tick_doc.zip_path) and not zip_exists:
            logger.warning(f'Pls check: zip_exists={zip_exists}, tick_doc.zip_path={self.tick_doc.zip_path}')
        return zip_exists

    # 删除zip文件
    def del_zip(self):
        _path = self.file
        try:
            while _path.exists():
                if _path.is_dir():
                    _path.rmdir()
                else:
                    _path.unlink()
                _path = _path.parent
        except Exception:
            pass
        self.tick_doc.update(set__zip_line_num=0, set__zip_path=None)
        self.tick_doc.reload()

    # 加载ticks
    def load_ticks(self):
        if not self.file.exists():
            if self.tick_doc.zip_path and 'empty_df' not in self.tick_doc.tags:
                logger.error(f'ERROR: tick[{self.tick_doc.pk}] has not stored, but file[{self.tick_doc.zip_path}] exists!')
            return None
        _df = pd.read_pickle(self.file, compression=PICKLE_COMPRESSION)
        return _df

    # 保存tick到pkl文件
    def csv_to_pickle(self, force=False):
        if self.tick_doc.MarketID == 3:         # 中金所不处理
            return
        pkl = PickleDbTick(self.tick_doc)

        if not force:
            if 'empty_df' in self.tick_doc.tags or 'load_df_fail' in self.tick_doc.tags:        # self.tick_doc.diff_sec < 0
                logger.warn(f'Pls check tick_doc: {self.tick_doc!r}')
                return
            if pkl.zip_exists():
                logger.warn(f'pkl already exists: {self.tick_doc!r}')
                return

        try:
            df, line_num = self._load_df_from_csv()
            self.tick_doc.update(set__line_num=line_num)
            self.tick_doc.reload()
        except Exception:
            logger.error(f'Load df fail: {self.tick_doc!r}')
            self.tick_doc.update(add_to_set__tags='load_df_fail')
            self.tick_doc.reload()
            return

        if df.empty:
            logger.error(f'df.empty error: {self.tick_doc!r}')
            self.tick_doc.update(add_to_set__tags='empty_df', set__doc_num=0)
            self.tick_doc.reload()
            return

        # 处理时间信息
        start = df['UpdateTime'].min()
        end = df['UpdateTime'].max()
        try:
            diff = end - start
            diff_sec = diff.total_seconds()
        except Exception:
            logger.error(f'calc diff_sec error: {self.tick_doc!r}, {start}, {end}')
            self.tick_doc.update(add_to_set__tags='diff_sec_error', set__doc_num=0)
            self.tick_doc.reload()
            return
        self.tick_doc.update(set__start=start, set__end=end, set__diff_sec=diff_sec)
        self.tick_doc.reload()

        # 将日夜盘交易时间拉会到同一天处理：
        #    09:00:00 -> 05:40:00
        #    10:15:00 -> 06:55:00
        #    10:30:00 -> 07:10:00
        #    11:30:00 -> 08:10:00
        #    13:30:00 -> 10:10:00
        #    15:00:00 -> 11:40:00
        #    21:00:00 -> 17:40:00
        #    02:30:00 -> 23:10:00
        df['UpdateTime_delay'] = df['UpdateTime'] + datetime.timedelta(hours=-3, minutes=-20)
        df['_UpdateTime_hour'] = df['UpdateTime_delay'].map(lambda x: (x.hour))
        time_period = dict(
            fam=(4, 6),     # 05:40:00 <= _ <= 06:55:00,    4 <= _ <= 6
            bam=(7, 8),     # 07:10:00 <= _ <= 08:10:00,    7 <= _ <= 8
            pm=(9, 14),     # 10:10:00 <= _ <= 11:40:00,    9 <= _ <= 14
            night=(15, 24), # 17:40:00 <= _ <= 23:10:00,   15 <= _ <= 24
        )

        df['time_type'] = 'unknow'
        for _type, _time in time_period.items():
            _start, _end = _time
            # print(_type, _start, _end)
            df.loc[(df._UpdateTime_hour >= _start) & (df._UpdateTime_hour <= _end), ['time_type']] = _type

        unknow_num = df[df.time_type == 'unknow'].shape[0]
        if unknow_num:
            logger.error(f'calc time_type error: {self.tick_doc!r}, unknow_num={unknow_num}')
            self.tick_doc.update(add_to_set__tags='time_error', set__doc_num=0)
            self.tick_doc.reload()
            return

        df = df.drop(['UpdateTime_delay', '_UpdateTime_hour'], axis=1)

        # save pickle
        self._to_pkl(df)
        # self.abs_path.mkdir(parents=True, exist_ok=True)
        # df.to_pickle(self.file, compression=PICKLE_COMPRESSION)

        # update tick_doc
        self.tick_doc.update(set__zip_line_num=df.shape[0], set__zip_path=str(self.rel_file))
        self.tick_doc.reload()

    def _to_pkl(self, df):
        self.abs_path.mkdir(parents=True, exist_ok=True)
        df.to_pickle(self.file, compression=PICKLE_COMPRESSION)

    # 从源数据中加载数据
    def _load_df_from_csv(self):
        tmpPath = RAW_DATA_PATH / self.tick_doc.path
        pd_data = pd.read_csv(
                    tmpPath,
                    names=['InstrumentID', 'MarketID', 'LastPrice', 'LastVolume', 'hhmmss', 'Reserved', 'UpdateTime', 'AskPrice1',
                           'AskVolume1', 'BidPrice1', 'BidVolume1', 'AskPrice2', 'AskVolume2', 'BidPrice2', 'BidVolume2',
                           'AskPrice3', 'AskVolume3', 'BidPrice3', 'BidVolume3', 'AskPrice4', 'AskVolume4', 'BidPrice4', 'BidVolume4',
                           'AskPrice5', 'AskVolume5', 'BidPrice5', 'BidVolume5', 'OpenInterest', 'Turnover', 'AvePrice', 'invol', 'outvol',
                           'Attr1', 'Volume1', 'Attr2', 'Volume2', 'HighestPrice', 'LowestPrice', 'SettlePrice', 'OpenPrice', 'mainID', 'fill'],
                    low_memory=False, parse_dates=['UpdateTime'], dtype={'hhmmss': str}, error_bad_lines=False, warn_bad_lines=False
                )

        drop_cols = [
                        # 'AskPrice1', 'AskVolume1', 'BidPrice1', 'BidVolume1',
                        'AskPrice2', 'AskVolume2', 'BidPrice2', 'BidVolume2',
                        'AskPrice3', 'AskVolume3', 'BidPrice3', 'BidVolume3',
                        'AskPrice4', 'AskVolume4', 'BidPrice4', 'BidVolume4',
                        'AskPrice5', 'AskVolume5', 'BidPrice5', 'BidVolume5',
                        'Reserved', 'invol', 'outvol',
                        'Attr1', 'Volume1', 'Attr2', 'Volume2',
                        'fill',
                    ]

        # 删掉不需要的列
        if self.tick_doc.subID == '0000':           # 指数
            drop_cols += ['HighestPrice', 'LowestPrice', 'OpenPrice', 'SettlePrice', 'Turnover', 'AvePrice', 'mainID']
        elif self.tick_doc.subID == '9999':         # 主力
            drop_cols += ['SettlePrice']
        else:                                       # ticks
            drop_cols += ['SettlePrice', 'mainID']
        pd_data.drop(drop_cols, axis=1, inplace=True)

        line_num = pd_data.shape[0]

        # 处理时间字段
        pd_data = pd_data[(pd_data.UpdateTime < MAX_DATE) & (pd_data.UpdateTime > MIN_DATE)]
        pd_data['UpdateTime'] = pd.to_datetime(pd_data.UpdateTime)
        # 处理交易量字段
        pd_data = pd_data[pd_data.LastVolume >= 0]

        if line_num > 7000 and line_num != pd_data.shape[0]:
            logger.info(f'[{self.tick_doc.path}]: after washing: {line_num}->{pd_data.shape[0]}.')

        # pd_data['subID'] = pd_data.InstrumentID.str[-4:]

        return pd_data, line_num


# 批量加载tick数据
class PickleDbTicks():
    def __init__(self, q_f, main_cls='main'):
        self.main_cls = main_cls
        if main_cls == 'main':
            self.ticks = TickFilesDoc.main(**q_f)
        elif main_cls == 'sub_main':
            self.ticks = TickFilesDoc.sub_main(**q_f)
        else:
            self.ticks = TickFilesDoc.objects(**q_f)
        self.total = self.ticks.count()

        # logger.info(f'total={self.total}')

    # 加载`q_f`筛选到的数据
    def load_ticks(self, with_clean=True):
        df_l = []
        with tqdm(total=self.total, desc=f'Progress:', disable=True) as pbar:
            for tick in self.ticks:
                pbar.update(1)
                # pkl = PickleDbTick(tick)
                _df = tick.load_ticks()
                if _df.empty:
                    logger.warning(f'{tick.path} load df fail.')
                    continue
                _df['day'] = tick.day

                df_l.append(_df)

        # df.sort_values('UpdateTime', inplace=True)
        df = pd.concat(df_l, sort=False)

        # clean df
        if with_clean:
            drop_cols = [
                # 'InstrumentID', 'MarketID', 'mainID', 'day',
                # 'AskPrice2', 'AskPrice3', 'AskPrice4', 'AskPrice5',
                # 'AskVolume2', 'AskVolume3', 'AskVolume4', 'AskVolume5',
                # 'BidPrice2', 'BidPrice3', 'BidPrice4', 'BidPrice5',
                # 'BidVolume2', 'BidVolume3', 'BidVolume4', 'BidVolume5',
                'hhmmss',
                'Reserved',
                'Attr1', 'Volume1',
                'Attr2', 'Volume2',
                'AvePrice',
                'OpenPrice', 'HighestPrice', 'LowestPrice',
                'SettlePrice',
                'invol', 'outvol',
                'fill',
            ]
            df = df.drop(drop_cols, axis=1)
            for col in ['InstrumentID', 'MarketID', 'mainID', 'day', 'time_type']:
                df[col] = df[col].astype('category')

        df.sort_values('UpdateTime', inplace=True)
        return df

    # 从`TickFilesDoc`加载日K
    def load_days(self):
        buf = []
        for tick in self.ticks:
            item = tick.to_mongo().to_dict()
            for _key in ['_id', 'tags', 'zip_ver', 'zip_path', 'stored', 'data_type']:    # , 'subID', 'category', 'MarketID', 'isDominant'
                del item[_key]
            buf.append(item)

        df = pd.DataFrame.from_dict(buf)

        return df
