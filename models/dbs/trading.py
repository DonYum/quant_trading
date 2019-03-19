import datetime
import logging
from mongoengine import *

__all__ = (
        'get_dyn_ticks_doc', 'get_dyn_kline_doc', 'STORED_CATEGORY_LIST',
    )

STORED_CATEGORY_LIST = ['AL', 'BU', 'CU', 'FU', 'HC', 'NI', 'PB', 'RB', 'RU', 'SN', 'ZN', 'AG', 'AU', 'WR']

# logger = logging.getLogger()


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

        hhmmss = StringField()                  # 时间(6位,时分秒 hhmmss)
        UpdateTime = DateTimeField()
        tags = ListField(StringField())         # 标记信息

        AskPrice1 = FloatField()
        AskVolume1 = IntField()
        BidPrice1 = FloatField()
        BidVolume1 = IntField()
        # AskPrice2 = FloatField()
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

    return TicksDoc


# K线数据集。实现分表存储。
def get_dyn_kline_doc(_collection_name):
    if _collection_name not in STORED_CATEGORY_LIST:
        raise Exception(f'Can not get kline table[{_collection_name}].')

    class KlineDoc(Document):
        meta = {
            'collection': f'kline_{_collection_name}',
            'db_alias': 'kline',
            'index_background': True,
            'auto_create_index': True,          # 每次操作都检查。TODO: Disabling this will improve performance.
            'indexes': [
                'InstrumentID',
                'MarketID',
                'UpdateTime',
            ]
        }
        InstrumentID = StringField()            # 合约代码
        category = StringField()                # 合约品种
        subcat = StringField()                  # 交易品种
        MarketID = IntField()                   # 市场代码(上证1, 深证2, 中金所3, 上期4, 郑商5, 大商6)

        LastPrice = FloatField()                # 最新价
        LastVolume = FloatField(default=0.0)    # 现量

        # hhmmss = StringField()                  # 时间(6位,时分秒 hhmmss)
        UpdateTime = DateTimeField()

        OpenPrice = FloatField()                # 开盘价
        HighestPrice = FloatField()             # 最高价
        LowestPrice = FloatField()              # 最低价
        SettlePrice = FloatField()              # 收盘价
        # Volume1 = IntField()                  # 成交量
        OpenInterest = IntField()               # 持仓量
        Turnover = FloatField()                 # 成交总额
        AvePrice = FloatField()                 # 均价

        # invol = IntField()                      # 内盘
        # outvol = IntField()                     # 外盘
        # Attr1 = IntField()                      # 性质1(多开1,多换2,空平3,空开4,空换5,多平6)
        # Attr2 = IntField()
        # Volume2 = IntField()                    # 数量2(多开1,多换2,空平3,空开4,空换5,多平6)

        # @queryset_manager
        # def valid(doc_cls, queryset):
        #     return queryset.filter(status__ne='drop')

    return KlineDoc
