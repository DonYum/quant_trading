import datetime
import logging
from mongoengine import *
import pandas as pd
from pathlib import Path
from ..apis.apis import format_file_size, format_time
from tqdm import tqdm_notebook as tqdm

__all__ = (
        'STORED_CATEGORY_LIST', 'KLINE_BINS_LIST',
        'get_dyn_ticks_doc', 'get_dyn_dominant_ticks_doc', 'KlineDoc', 'StatisDayDoc',
        'TickFilesDoc', 'ModelException', 'TickSplitPklFilesDoc',
    )

logger = logging.getLogger()

# STORED_CATEGORY_LIST = ['AG', 'AL', 'AU', 'BU', 'CU', 'FU', 'HC', 'NI', 'PB', 'RB', 'RU', 'SN', 'ZN', 'WR']
STORED_CATEGORY_LIST = ['AG', 'AL', 'AU', 'BU', 'CU', 'FU', 'HC', 'NI', 'PB', 'RB', 'RU', 'SN', 'WR', 'ZN']

KLINE_BINS_LIST = ['3min', '5min', '15min', '30min', '1H', '2H']

# logger = logging.getLogger()

# URI_ticks = 'mongodb://127.0.0.1:6007/ticks'
# URI_d_ticks = 'mongodb://127.0.0.1:6007/d_ticks'
# URI_kline = 'mongodb://127.0.0.1:6007/kline'
# URI_statistic = 'mongodb://127.0.0.1:6007/statistic'
# connect(host=URI_ticks,  alias='ticks')
# connect(host=URI_d_ticks,  alias='d_ticks')
# connect(host=URI_kline,  alias='kline')
# connect(host=URI_statistic,  alias='statistic')


class ModelException(Exception):
    pass


# tick数据集。实现分表存储。
def get_dyn_ticks_doc(_collection_name):
    if _collection_name not in STORED_CATEGORY_LIST:
        raise Exception(f'Can not get ticks table[{_collection_name}].')

    class TicksDoc(Document):
        meta = {
            'collection': f'ticks_{_collection_name}',
            'db_alias': 'ticks',
            'index_background': True,
            'auto_create_index': True,          # 每次操作都检查。TODO: Disabling this will improve performance.
            'indexes': [
                'InstrumentID',
                'MarketID',
                'LastPrice',
                'LastVolume',
                'UpdateTime',
                'day',
                'time_type',
                'tags',
            ]
        }
        # id = SequenceField(db_alias='ticks', primary_key=True)

        InstrumentID = StringField()            # 合约代码
        # category = StringField()                # 合约品种
        subID = StringField()                   # 子代码(日期)
        MarketID = IntField()                   # 市场代码(上证1, 深证2, 中金所3, 上期4, 郑商5, 大商6)

        LastPrice = FloatField()                # 最新价
        LastVolume = FloatField()    # 现量

        UpdateTime = DateTimeField()
        hhmmss = StringField()                  # 时间(6位，时分秒 hhmmss)
        day = StringField()                     # 日期(8位，yyyymmdd)，InstrumentID+day可以索引到TickFiles里面的信息。
        time_type = StringField()                   # 日盘 / 夜盘。 fam(front of a.m.) / bam(back of a.m.) / pm(p.m.) / night
        tags = ListField(StringField())         # 标记信息

        AskPrice1 = FloatField()
        AskVolume1 = IntField()
        BidPrice1 = FloatField()
        BidVolume1 = IntField()
        # AskPrice2 = FloatField()              # 只有上期所股指期货IF才会有五档数据，其他的只有一档数据
        # AskVolume2 = IntField()
        # BidPrice2 = FloatField()
        # BidVolume2 = IntField()
        # AskPrice3 = FloatField()
        # AskVolume3 = IntField()
        # BidPrice3 = FloatField()
        # BidVolume3 = IntField()
        # AskPrice4 = FloatField()
        # AskVolume4 = IntField()
        # BidPrice4 = FloatField()
        # BidVolume4 = IntField()
        # AskPrice5 = FloatField()
        # AskVolume5 = IntField()
        # BidPrice5 = FloatField()
        # BidVolume5 = IntField()

        OpenInterest = IntField()               # 持仓量
        Turnover = FloatField()                 # 成交总额
        AvePrice = FloatField()                 # 均价
        # invol = IntField()                    # 内盘
        # outvol = IntField()                   # 外盘
        # Attr1 = IntField()                    # 性质1(多开1,多换2,空平3,空开4,空换5,多平6)
        # Volume1 = IntField()
        # Attr2 = IntField()
        # Volume2 = IntField()                  # 数量2(多开1,多换2,空平3,空开4,空换5,多平6)

        HighestPrice = FloatField()             # 最高价
        LowestPrice = FloatField()              # 最低价
        # SettlePrice = FloatField()              # 结算价
        OpenPrice = FloatField()                # 开盘价
        # Reserved = StringField()
        # fill = StringField()

        # @queryset_manager
        # def valid(doc_cls, queryset):
        #     return queryset.filter(status__ne='drop')

        @classmethod
        def get_cat(cls):
            return _collection_name

        @classmethod
        def get_mkt(cls):
            mkts = cls.objects().distinct('MarketID')
            if len(mkts) != 1:
                raise ModelException(f'[{_collection_name}]: MarketID({mkts}) is invalid!')
            return mkts[0]

        @classmethod
        def get_ids(cls, q_f={}):
            return cls.objects(**q_f).distinct('InstrumentID')

    TicksDoc.get_mkt()

    return TicksDoc


# 主力合约(dominant contract) tick数据集。实现分表存储。
# 没必要单独存储！！！在`KlineDoc`中加`isDominant`字段即可。
def get_dyn_dominant_ticks_doc(_collection_name):
    if _collection_name not in STORED_CATEGORY_LIST:
        raise Exception(f'Can not get d_ticks table[{_collection_name}].')

    class TicksDoc(Document):
        meta = {
            'collection': f'd_ticks_{_collection_name}',
            'db_alias': 'd_ticks',
            'index_background': True,
            'auto_create_index': True,          # 每次操作都检查。TODO: Disabling this will improve performance.
            'indexes': [
                'InstrumentID',
                'MarketID',
                'LastPrice',
                'LastVolume',
                'UpdateTime',
                'tags',
            ]
        }
        # id = SequenceField(db_alias='ticks', primary_key=True)

        InstrumentID = StringField()            # 合约代码
        # category = StringField()              # 合约品种
        subID = StringField()                   # 子代码(日期)
        time_type = StringField()               # 日盘 / 夜盘
        MarketID = IntField()                   # 市场代码(上证1, 深证2, 中金所3, 上期4, 郑商5, 大商6)

        LastPrice = FloatField()                # 最新价
        LastVolume = FloatField()    # 现量

        hhmmss = StringField()                  # 时间(6位,时分秒 hhmmss)
        UpdateTime = DateTimeField()
        tags = ListField(StringField())         # 标记信息

        AskPrice1 = FloatField()
        AskVolume1 = IntField()
        BidPrice1 = FloatField()
        BidVolume1 = IntField()

        OpenInterest = IntField()               # 持仓量
        Turnover = FloatField()                 # 成交总额
        AvePrice = FloatField()                 # 均价

        HighestPrice = FloatField()             # 最高价
        LowestPrice = FloatField()              # 最低价
        # SettlePrice = FloatField()            # 结算价
        OpenPrice = FloatField()                # 开盘价

        # @queryset_manager
        # def valid(doc_cls, queryset):
        #     return queryset.filter(status__ne='drop')

    return TicksDoc


# K线数据集。没必要分表存储。
class KlineDoc(Document):
    meta = {
        'collection': 'kline_tab',
        'db_alias': 'kline',
        'index_background': True,
        'auto_create_index': True,          # 每次操作都检查。TODO: Disabling this will improve performance.
        'indexes': [
            'TradingTime',
            'InstrumentID',
            'level',
            'category',
            'MarketID',
            'TotalVolume',
            'isDominant',
        ]
    }
    TradingTime = DateTimeField()

    InstrumentID = StringField()            # 合约代码
    level = StringField()                   # K线粒度
    category = StringField()                # 合约品种
    time_type = StringField()               # 日盘 / 夜盘
    MarketID = IntField()                   # 市场代码(上证1, 深证2, 中金所3, 上期4, 郑商5, 大商6)

    isDominant = BooleanField(default=False)    # 是否是主力合约

    TotalVolume = FloatField()              # 总成交量
    volume_std = FloatField()               # 成交量std

    # hhmmss = StringField()                # 时间(6位,时分秒 hhmmss)

    open = FloatField()                 # 开盘价
    high = FloatField()                 # 最高价
    low = FloatField()                  # 最低价
    close = FloatField()                # 收盘价

    OpenPrice = FloatField()            # 开盘价
    HighestPrice = FloatField()         # 最高价
    LowestPrice = FloatField()          # 最低价
    # SettlePrice = FloatField()        # 收盘价

    # Volume1 = IntField()              # 成交量
    OpenInterest = IntField()           # 持仓量
    Turnover = FloatField()             # 成交总额
    Turnover_new = FloatField()         # 计算出来的成交总额：k1d_df['Turnover_new'] = (df.LastPrice * df.LastVolume * 10).resample('1d').sum()
    AvePrice = FloatField()             # 均价

    tick_num = IntField()               # Ticks数量

    # invol = IntField()                # 内盘
    # outvol = IntField()               # 外盘
    # Attr1 = IntField()                # 性质1(多开1,多换2,空平3,空开4,空换5,多平6)
    # Attr2 = IntField()
    # Volume2 = IntField()              # 数量2(多开1,多换2,空平3,空开4,空换5,多平6)

    # @queryset_manager
    # def valid(doc_cls, queryset):
    #     return queryset.filter(status__ne='drop')


class StatisDayDoc(Document):
    meta = {
        'collection': 'day_related',
        'db_alias': 'statistic',
        'index_background': True,
        'auto_create_index': True,          # 每次操作都检查。TODO: Disabling this will improve performance.
        'indexes': [
            'TradingTime',
            'InstrumentID',
            'category',
            'MarketID',
            'TotalVolume',
            'tags',
            'isDominant',
        ]
    }
    # id = SequenceField(db_alias='ticks', primary_key=True)

    TradingTime = DateTimeField()           # 时间，日

    InstrumentID = StringField()            # 合约代码
    category = StringField()                # 合约品种
    time_type = StringField()               # 日盘 / 夜盘
    MarketID = IntField()                   # 市场代码(上证1, 深证2, 中金所3, 上期4, 郑商5, 大商6)

    isDominant = BooleanField(default=False)    # 是否是主力合约

    TotalVolume = FloatField()                  # 总成交量
    volume_std = FloatField()                   # 成交量std

    # LastPrice = FloatField()                  # 最新价
    # LastVolume = FloatField(default=0.0)      # 现量

    # OHLC结果
    open = FloatField()                 # 开盘价
    high = FloatField()                 # 最高价
    low = FloatField()                  # 最低价
    close = FloatField()                # 收盘价

    # ticks数据里统计到的值，和OHLC数据有出入。实际应用看情况使用。
    OpenPrice = FloatField()                # 开盘价
    HighestPrice = FloatField()             # 最高价
    LowestPrice = FloatField()              # 最低价
    # SettlePrice = FloatField()            # 收盘价

    OpenInterest = IntField()               # 持仓量 ***
    Turnover = FloatField()                 # 成交总额
    Turnover_new = FloatField()             # 计算出来的成交总额：k1d_df['Turnover_new'] = (df.LastPrice * df.LastVolume * 10).resample('1d').sum()
    AvePrice = FloatField()                 # 均价

    tags = ListField(StringField())         # 标记信息
    tick_num = IntField()                   # Ticks数量

    # @queryset_manager
    # def valid(doc_cls, queryset):
    #     return queryset.filter(status__ne='drop')


class StatisInstrumentDoc(Document):
    meta = {
        'collection': 'statis_instrument',
        'db_alias': 'statistic',
        'index_background': True,
        'auto_create_index': True,          # 每次操作都检查。TODO: Disabling this will improve performance.
        'indexes': [
            'InstrumentID',
            'category',
            # 'MarketID',
        ]
    }
    InstrumentID = StringField(primary_key=True)    # 合约代码
    category = StringField()                    # 合约品种
    # plate = StringField()                     # 日盘 / 夜盘

    tick_num = IntField()                       #
    highest_price = FloatField()                #


class TickFilesDoc(Document):
    meta = {
        'collection': 'tick_files',
        'db_alias': 'ticks',
        'index_background': True,
        'auto_create_index': True,          # 每次操作都检查。TODO: Disabling this will improve performance.
        'indexes': [
            'InstrumentID',
            'category',
            'diff_sec',
            'tags',
            'day',
            'isDominant',
            'is2ndDominant',
            'zip_line_num',
        ]
    }
    MarketID = IntField()                   # 市场代码(上证1, 深证2, 中金所3, 上期4, 郑商5, 大商6)
    category = StringField()                # 合约品种: AU/AG/CU...
    InstrumentID = StringField()            # 合约代码
    subID = StringField()                   # 子代码(日期)，从InstrumentID中提取。

    tags = ListField(StringField())

    data_type = StringField()               # tick/9999/0000/1day_k... subID: 9999表示主力dominant，0000表示指数index
    year = StringField()                    # '2019'
    month = StringField()                   # '201909'
    day = StringField()                     # '20190925'

    isDominant = BooleanField(default=False)        # 是否是主力合约：交易量（OpenInterest）最大
    is2ndDominant = BooleanField(default=False)     # 是否是次主力合约：交易量大于当天最大交易量一半的合约

    # tick数据的时间信息
    start = DateTimeField()
    end = DateTimeField()
    diff_sec = IntField()

    # 原始文件
    path = StringField(unique=True)         # 存放相对路径
    size = IntField()                       # bytes
    line_num = IntField()

    # pkl压缩文件
    zip_path = StringField()                # 存放清理后的数据
    zip_line_num = IntField()
    zip_ver = IntField(default=1)

    stored = BooleanField(default=False)    # 暂时没有使用
    doc_num = IntField()

    # 一些统计量
    open = FloatField()
    close = FloatField()
    high = FloatField()
    low = FloatField()
    mean = FloatField()

    OpenInterest = IntField()
    Turnover = FloatField()                 # 成交总额
    Turnover_calc = FloatField()            # 计算出来的成交总额：k1d_df['Turnover_new'] = (df.LastPrice * df.LastVolume * 10).resample('1d').sum()

    volume_sum = IntField()                 # 总成交量

    @queryset_manager
    def wait_import(doc_cls, queryset):     # 增量添加了文件，但没有生成pkl文件
        return queryset.filter(line_num=None)

    @queryset_manager
    def df_valid(doc_cls, queryset):        # 正常pkl化的数据
        return queryset.filter(tags__nin=['too_small', ])

    @queryset_manager
    def valid(doc_cls, queryset):        # 有意义的数据
        return queryset.filter(tags__nin=['invalid_day', 'too_small', 'dup_time', 'time_no_ms'])

    @queryset_manager
    def main(doc_cls, queryset):            # 主力
        return queryset.filter(zip_path__ne=None, high__ne=None, subID__nin=['0000', '9999'], tags__nin=['invalid_day', 'too_small', 'dup_time', 'time_no_ms'], isDominant=True)

    @queryset_manager
    def sub_main(doc_cls, queryset):        # 次主力
        return queryset.filter(zip_path__ne=None, high__ne=None, subID__nin=['0000', '9999'], tags__nin=['invalid_day', 'too_small', 'dup_time', 'time_no_ms'], is2ndDominant=True)

    def __repr__(self):
        return f'[{self.InstrumentID}-{self.day}]: file={self.path}({format_file_size(self.size)}), {self.zip_line_num}/{self.line_num}, tags={self.tags}, time_len={format_time(self.diff_sec)}'

    ###############################################
    dst_root = Path('/ticks')
    src_root = Path('/data/tick')

    compression = 'zip'     # 测试下来读写性能综合考虑zip是最优的方法
    _zip_ver = 1

    # rel_file = None   # 相对路径，to save in db
    # abs_path = None   # 绝对路径，用于创建目录
    # _f_name = None     # 文件名
    # file = None       # 文件绝对路径
    @property
    def _rel_path(self):
        return Path(f'{self.MarketID}/{self.category}/{self.InstrumentID}/{self.month}/')
    @property
    def abs_path(self):
        return self.dst_root / self._rel_path
    @property
    def _f_name(self):
        return f'{self.InstrumentID}_{self.day}_{self._zip_ver}.pkl'
    @property
    def rel_file(self):
        return self._rel_path / self._f_name   # to save in db
    @property
    def file(self):
        return self.abs_path / self._f_name

    # 检查zip文件是否存在
    def zip_exists(self):
        zip_exists = self.file.exists()
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
        self.update(set__zip_line_num=0, set__zip_path=None)
        self.reload()

    # 加载ticks
    def load_ticks(self):
        if not self.zip_exists():
            logger.warn(f'ERROR: Not Exists: {self!r}')
            return None
        _df = pd.read_pickle(self.file, compression=self.compression)
        return _df

    # 按照日夜盘存储
    def save_splited(self):
        if 'too_small' in self.tags or 'invalid_day' in self.tags:
            return None

        df = self.load_ticks()
        if df is None or df.empty:
            logger.warn(f'ERROR: df not exists: {self!r}')
            return None

        # 删除已经存在的数据
        __ticks = TickSplitPklFilesDoc.objects(file_doc=self)
        if __ticks.count():
            for __tick in __ticks:
                __tick.del_zip()
            TickSplitPklFilesDoc.objects(file_doc=self).delete()
            # self.update(pull__tags='splited', set__doc_num=0)
            self.reload()

        # 去重
        _pre_shape = df.shape[0]
        df = df.drop_duplicates()
        _diff_shape = _pre_shape - df.shape[0]
        if _diff_shape:
            logger.info(f'DropDup, Droped={_diff_shape}, Left={df.shape[0]}: {self!r}')

        # 对于早期数据，有时间上的问题，比如millisecond=1，参见：[AG1406][20140217]
        # 这造成的问题是asfreq('500ms')得到一半无用数据，所以应该在数据源头做处理。
        df = df.set_index('UpdateTime')
        _ms = df.index.microsecond.value_counts().to_dict()
        if set(_ms.keys()) ^ set([500000, 0]):
            logger.info(f'Time.microsecond: {_ms}: {self!r}')
            df.index = df.index.map(lambda x: x.replace(microsecond=500000) if x.microsecond != 0 else x)

        _ms = df.index.microsecond.value_counts().to_dict()
        if len(_ms) < 2:
            logger.warn(f'Time.microsecond: {_ms}: {self!r}')
            self.update(add_to_set__tags='time_no_ms')
            self.reload()
            return

        # assert not (set(_ms.keys()) ^ set([500000, 0])), f'Time.microsecond Exception: {_ms}: {self!r}'

        df = df.sort_index().reset_index()      # 排序、重置index
        # 按时间去重
        _pre_shape = df.shape[0]
        df = df.drop_duplicates('UpdateTime', keep='first')
        _diff_shape = _pre_shape - df.shape[0]
        if _diff_shape:
            logger.info(f'DropDupByTime, Droped={_diff_shape}, Left={df.shape[0]}: {self!r}')
        if _diff_shape > 100:
            logger.warn(f'DropDupByTime: Drop too many, set Invalid. {self!r}')
            self.update(add_to_set__tags='dup_time')
            self.reload()
            return

        # 日夜盘切分
        #   fam: 09:00 - 10:15, 1:15, 75' ,  ticks=9000
        #   bam: 10:30 - 11:30, 1:00, 60' ,  ticks=7200
        #    pm: 13:30 - 15:00, 1:30, 90' ,  ticks=10800
        # night: 21:00 - 02:30, 5:00, 300',  ticks=36000
        df = df.set_index('UpdateTime')
        df['time_type'] = 'unknow'

        time_period = dict(
            fam=('09:00', '10:15'),
            bam=('10:30', '11:30'),
            pm=('13:00', '15:01'),
            night=('21:00', '03:00'),
        )
        for _type, _time in time_period.items():
            _start, _end = _time
            df.loc[df.between_time(_start, _end).index, 'time_type'] = _type

        unknow_num = df[df.time_type == 'unknow'].shape[0]
        if unknow_num:
            logger.error(f'calc time_type error: {self!r}, unknow_num={unknow_num}')
            # update_d = {**update_d, **dict(add_to_set__tags='time_error', set__doc_num=0)}
            # self.update(**update_d)
            # self.reload()
            return

        df['UpdateTime_delay'] = df['UpdateTime'] + datetime.timedelta(hours=-3, minutes=-20)
        df['UpdateTime_day'] = df['UpdateTime_delay'].map(lambda x: x.strftime('%Y%m%d'))
        df = df.drop(['UpdateTime_delay'], axis=1)

        days = df['UpdateTime_day'].unique()
        time_types = df['time_type'].unique()

        # logger.info(f'DELETE({cnt}): {self!r}')

        cnt = 0
        for day in days:
            for time_type in time_types:
                # 相关路径的计算
                __f_name = f'{self.InstrumentID}_{self.day}_{self._zip_ver}_{day}_{time_type}.pkl'
                _rel_file = self._rel_path / __f_name   # to save in db
                _file = self.abs_path / __f_name

                _df = df[(df.UpdateTime_day==day) & (df.time_type==time_type)]
                if _df.empty:
                    continue
                if _df.shape[0] < 200:
                    continue

                _df = _df.sort_values('UpdateTime')     # 一定要排序！！！

                start = _df['UpdateTime'].min()
                end = _df['UpdateTime'].max()
                try:
                    diff = end - start
                    diff_sec = diff.total_seconds()
                except Exception:
                    logger.error(f'[{day}-{time_type}]: calc diff_sec error: {self!r}, {start}, {end}')
                    continue

                # 一些统计量
                try:
                    statics_d = dict(
                        zip_line_num = _df.shape[0],
                        open = _df.iloc[0]['LastPrice'],
                        close = _df.iloc[-1]['LastPrice'],
                        high = _df['LastPrice'].max(),
                        low = _df['LastPrice'].min(),
                        mean = _df['LastPrice'].mean(),

                        OpenInterest = _df.iloc[-1]['OpenInterest'],
                        # Turnover = _df.iloc[-1]['Turnover'],                 # 成交总额
                        volume_sum = _df['LastVolume'].sum(),
                    )
                except Exception:
                    raise Exception(f'get statistic error: {self!r}')

                _df.to_pickle(_file, compression=self.compression)

                TickSplitPklFilesDoc(
                    file_doc = self,
                    MarketID = self.MarketID,
                    category = self.category,
                    InstrumentID = self.InstrumentID,
                    data_type = self.data_type,
                    isDominant = self.isDominant,
                    is2ndDominant = self.is2ndDominant,

                    year = start.strftime('%Y'),
                    month = start.strftime('%Y%m'),
                    day = day,
                    time_type = time_type,

                    start = start,
                    end = end,
                    diff_sec = diff_sec,

                    zip_path = str(_rel_file),
                    **statics_d,
                ).save()

                cnt += 1
        self.update(add_to_set__tags='splited', set__doc_num=cnt)
        self.reload()

    # 保存tick到pkl文件
    def csv_to_pickle(self, force=False):
        if self.MarketID == 3:         # 中金所不处理
            return
        # pkl = PickleDbTick(self.tick_doc)

        update_d = dict(set__doc_num=0)

        if not force:
            if 'empty_df' in self.tags or 'load_df_fail' in self.tags:        # self.tick_doc.diff_sec < 0
                logger.warn(f'Pls check: {self!r}')
                return
            if self.zip_exists():
                logger.warn(f'pkl already exists: {self!r}')
                return

        try:
            df, line_num = self._load_df_from_csv()
            update_d['set__line_num'] = line_num
            # self.update(set__line_num=line_num)
            # self.reload()
        except Exception:
            logger.error(f'Load df fail: {self!r}')
            self.update(add_to_set__tags='load_df_fail')
            self.reload()
            return

        if df.empty:
            logger.error(f'df.empty error: {self!r}')
            self.update(add_to_set__tags='empty_df', set__doc_num=0)
            self.reload()
            return

        # 处理时间信息
        start = df['UpdateTime'].min()
        end = df['UpdateTime'].max()
        try:
            diff = end - start
            diff_sec = diff.total_seconds()
        except Exception:
            logger.error(f'calc diff_sec error: {self!r}, {start}, {end}')
            update_d = {**update_d, **dict(add_to_set__tags='diff_sec_error', set__doc_num=0)}
            self.update(**update_d)
            self.reload()
            return

        if df.shape[0] < 200:
            update_d['add_to_set__tags'] = 'too_small'

        update_d = {**update_d, **dict(set__start=start, set__end=end, set__diff_sec=diff_sec)}
        # self.update(set__start=start, set__end=end, set__diff_sec=diff_sec)
        # self.reload()

        # save pickle
        self._to_pkl(df)
        # self.abs_path.mkdir(parents=True, exist_ok=True)
        # df.to_pickle(self.file, compression=self.compression)

        # update tick_doc
        update_d = {**update_d, **dict(set__zip_line_num=df.shape[0], set__zip_path=str(self.rel_file), pull__tags='time_error')}
        self.update(**update_d)
        self.reload()

    def _to_pkl(self, df):
        self.abs_path.mkdir(parents=True, exist_ok=True)
        df.to_pickle(self.file, compression=self.compression)

    # 从源数据中加载数据
    def _load_df_from_csv(self):
        tmpPath = self.src_root / self.path
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
        if self.subID == '0000':           # 指数
            drop_cols += ['HighestPrice', 'LowestPrice', 'OpenPrice', 'SettlePrice', 'Turnover', 'AvePrice', 'mainID']
        elif self.subID == '9999':         # 主力
            drop_cols += ['SettlePrice']
        else:                                       # ticks
            drop_cols += ['SettlePrice', 'mainID']
        pd_data.drop(drop_cols, axis=1, inplace=True)

        line_num = pd_data.shape[0]

        # 处理时间字段
        pd_data = pd_data[(pd_data.UpdateTime < '2022-12-12') & (pd_data.UpdateTime > '2012-12-12')]
        pd_data['UpdateTime'] = pd.to_datetime(pd_data.UpdateTime)
        # 处理交易量字段
        pd_data = pd_data[pd_data.LastVolume >= 0]

        if line_num > 7000 and line_num != pd_data.shape[0]:
            logger.info(f'[{self!r}]: after washing: {line_num}->{pd_data.shape[0]}.')

        # pd_data['subID'] = pd_data.InstrumentID.str[-4:]

        return pd_data, line_num


# 因为tick文件存在一个文件包含多天的问题，所以需要拆分存储。
# 可以看作是对原始数据的进一步处理。
# 按交易时间段拆分，见`time_type`字段。
class TickSplitPklFilesDoc(Document):
    meta = {
        'collection': 'tick_split_pkl_files',
        'db_alias': 'ticks',
        'index_background': True,
        'auto_create_index': True,          # 每次操作都检查。TODO: Disabling this will improve performance.
        'indexes': [
            'InstrumentID',
            'category',
            # 'diff_sec',
            'tags',
            'day',
            'isDominant',
            'is2ndDominant',
            # 'zip_line_num',
        ]
    }
    file_doc = ReferenceField(TickFilesDoc)

    MarketID = IntField()                   # 市场代码(上证1, 深证2, 中金所3, 上期4, 郑商5, 大商6)
    category = StringField()                # 合约品种: AU/AG/CU...
    InstrumentID = StringField()            # 合约代码

    tags = ListField(StringField())

    data_type = StringField()               # tick/9999/0000/1day_k... subID: 9999表示主力dominant，0000表示指数index
    # 下面的时间是计算出来的
    year = StringField()                    # '2019'
    month = StringField()                   # '201909'
    day = StringField()                     # '20190925'
    time_type = StringField()               # 日盘 / 夜盘。 fam(front of a.m.) / bam(back of a.m.) / pm(p.m.) / night

    isDominant = BooleanField(default=False)        # 是否是主力合约：交易量（OpenInterest）最大
    is2ndDominant = BooleanField(default=False)     # 是否是次主力合约：交易量大于当天最大交易量一半的合约

    # tick数据的时间信息
    start = DateTimeField()
    end = DateTimeField()
    diff_sec = IntField()

    # # 原始文件
    # path = StringField(unique=True)         # 存放相对路径
    # size = IntField()                       # bytes
    # line_num = IntField()

    # pkl压缩文件
    zip_path = StringField()                # 存放清理后的数据
    zip_line_num = IntField()
    # zip_ver = IntField()

    # 特征文件
    # feature_path = StringField()           # 提取特征后的文件保存路径
    feature_files = ListField(StringField())           # 提取特征后的文件
    feature_num = IntField()
    feature_time = DateTimeField()

    # stored = BooleanField(default=False)    # 暂时没有使用
    # doc_num = IntField()

    # 一些统计量
    open = FloatField()
    close = FloatField()
    high = FloatField()
    low = FloatField()
    mean = FloatField()

    OpenInterest = IntField()
    Turnover = FloatField()                 # 成交总额
    Turnover_calc = FloatField()            # 计算出来的成交总额：k1d_df['Turnover_new'] = (df.LastPrice * df.LastVolume * 10).resample('1d').sum()

    volume_sum = IntField()                 # 总成交量

    @queryset_manager
    def valid(doc_cls, queryset):        # 正常pkl化的数据
        return queryset.filter(tags__nin=['too_small'])

    @queryset_manager
    def main(doc_cls, queryset):            # 主力
        return queryset.filter(zip_path__ne=None, high__ne=None, tags__nin=['too_small'], isDominant=True)

    @queryset_manager
    def sub_main(doc_cls, queryset):        # 次主力
        return queryset.filter(zip_path__ne=None, high__ne=None, tags__nin=['too_small'], is2ndDominant=True)

    def __repr__(self):
        return f'[{self.InstrumentID}-{self.day}-{self.time_type}]: file={self.zip_path}, {self.zip_line_num}, tags={self.tags}, time_len={format_time(self.diff_sec)}, f_num={self.feature_num}'

    ###############################################
    dst_root = Path('/ticks')

    compression = 'zip'     # 测试下来读写性能综合考虑zip是最优的方法
    # _zip_ver = 1

    @property
    def file(self):
        return self.dst_root / self.zip_path
    @property
    def _f_name(self):
        return self.file.name
    @property
    def feature_path(self):
        return self.file.parent / f'{self.file.stem}_feature'
    # @property
    # def _feature_rel_path(self):        # 相对路径，写入数据库会用到
    #     return self.file.relative_to(self.dst_root)

    # 检查zip文件是否存在
    def zip_exists(self):
        zip_exists = self.file.exists()
        return zip_exists

    # 删除zip文件
    def del_zip(self):
        # 先删特征文件
        self.reset_features()

        # 删数据
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
        # 处理存储信息
        self.file_doc.update(pull__tags='splited', set__doc_num=0)
        self.delete()

    # 加载ticks
    def load_ticks(self):
        if not self.zip_exists():
            logger.warn(f'ERROR: Not Exists: {self!r}')
            return None
        _df = pd.read_pickle(self.file, compression=self.compression)
        return _df

    # def _to_pkl(self, df):
    #     # self.abs_path.mkdir(parents=True, exist_ok=True)
    #     df.to_pickle(self.file, compression=self.compression)

    def reset_features(self):
        for _f in self.feature_files:
            _path = self.feature_path / _f
            try:
                while _path.exists():
                    if _path.is_dir():
                        _path.rmdir()
                    else:
                        _path.unlink()
                    _path = _path.parent
            except Exception:
                pass
        self.update(set__feature_files=[], set__feature_num=0)
        self.reload()

    def save_features(self, df, name):
        self.feature_path.mkdir(parents=True, exist_ok=True)
        df.to_pickle(self.feature_path/name, compression=self.compression)
        self.update(push__feature_files=name, inc__feature_num=1, set__feature_time=datetime.datetime.now())
        self.reload()

    def load_feature(self):
        pass
