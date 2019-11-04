import datetime
import logging
from mongoengine import *

__all__ = (
        'STORED_CATEGORY_LIST', 'KLINE_BINS_LIST',
        'get_dyn_ticks_doc', 'get_dyn_dominant_ticks_doc', 'KlineDoc', 'StatisDayDoc',
        'TickFilesDoc', 'ModelException',
    )

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
        day = StringField()                  # 日期(8位，yyyymmdd)，InstrumentID+day可以索引到TickFiles里面的信息。
        time_type = StringField()                   # 日盘 / 夜盘。 fam(front of a.m.)/bam(back of a.m.)/pm(a.m.)/night
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
        # invol = IntField()                      # 内盘
        # outvol = IntField()                     # 外盘
        # Attr1 = IntField()                      # 性质1(多开1,多换2,空平3,空开4,空换5,多平6)
        # Volume1 = IntField()
        # Attr2 = IntField()
        # Volume2 = IntField()                    # 数量2(多开1,多换2,空平3,空开4,空换5,多平6)

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
        # category = StringField()                # 合约品种
        subID = StringField()                   # 子代码(日期)
        time_type = StringField()                   # 日盘 / 夜盘
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
        # SettlePrice = FloatField()              # 结算价
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
    level = StringField()                  # K线粒度
    category = StringField()                # 合约品种
    time_type = StringField()                   # 日盘 / 夜盘
    MarketID = IntField()                   # 市场代码(上证1, 深证2, 中金所3, 上期4, 郑商5, 大商6)

    isDominant = BooleanField(default=False)    # 是否是主力合约

    TotalVolume = FloatField()              # 总成交量
    volume_std = FloatField()              # 成交量std

    # hhmmss = StringField()                  # 时间(6位,时分秒 hhmmss)

    open = FloatField()                 # 开盘价
    high = FloatField()                 # 最高价
    low = FloatField()                  # 最低价
    close = FloatField()                # 收盘价

    OpenPrice = FloatField()                # 开盘价
    HighestPrice = FloatField()             # 最高价
    LowestPrice = FloatField()              # 最低价
    # SettlePrice = FloatField()              # 收盘价

    # Volume1 = IntField()                  # 成交量
    OpenInterest = IntField()               # 持仓量
    Turnover = FloatField()                 # 成交总额
    Turnover_new = FloatField()             # 计算出来的成交总额：k1d_df['Turnover_new'] = (df.LastPrice * df.LastVolume * 10).resample('1d').sum()
    AvePrice = FloatField()                 # 均价

    tick_num = IntField()                   # Ticks数量

    # invol = IntField()                      # 内盘
    # outvol = IntField()                     # 外盘
    # Attr1 = IntField()                      # 性质1(多开1,多换2,空平3,空开4,空换5,多平6)
    # Attr2 = IntField()
    # Volume2 = IntField()                    # 数量2(多开1,多换2,空平3,空开4,空换5,多平6)

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

    TradingTime = DateTimeField()                     # 时间，日

    InstrumentID = StringField()            # 合约代码
    category = StringField()                # 合约品种
    time_type = StringField()                   # 日盘 / 夜盘
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
    # SettlePrice = FloatField()              # 收盘价

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
    InstrumentID = StringField(primary_key=True)            # 合约代码
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
            'zip_line_num',
        ]
    }
    MarketID = IntField()                   # 市场代码(上证1, 深证2, 中金所3, 上期4, 郑商5, 大商6)
    category = StringField()                # 合约品种: AU/AG/CU...
    InstrumentID = StringField()            # 合约代码
    subID = StringField()                   # 子代码(日期)，从InstrumentID中提取。

    tags = ListField(StringField())

    data_type = StringField()                    # tick/3min_k/1day_k... subID: 9999表示主力dominant，0000表示指数index
    year = StringField()
    month = StringField()
    day = StringField()

    start = DateTimeField()
    end = DateTimeField()
    diff_sec = IntField()

    path = StringField(unique=True)            # 存放相对路径
    size = IntField()       # bytes
    line_num = IntField()

    zip_path = StringField()            # 存放清理后的数据
    zip_line_num = IntField()
    zip_ver = IntField()

    stored = BooleanField(default=False)
    doc_num = IntField()

    @queryset_manager
    def is_stored(doc_cls, queryset):
        return queryset.filter(stored=True)

    @queryset_manager
    def not_stored(doc_cls, queryset):
        return queryset.filter(stored=False)
