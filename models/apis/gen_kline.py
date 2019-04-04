import logging
import numpy as np
import pandas as pd
from pandas.tseries.offsets import *
from tqdm import tqdm_notebook as tqdm

from ..dbs.trading import *
from .apis import *

__all__ = ('load_ticks_by_id', 'gen_kline_from_pd', 'save_kline_by_id', 'save_1day_kline_by_id', 'calc_kline_by_id', 'load_kline_to_df', 'load_1day_kline_to_df', )

logger = logging.getLogger(__name__)


def load_ticks_by_id(_id):
    d_doc = get_dyn_ticks_doc(_id[:2])

    trads = d_doc.objects(InstrumentID=_id).only('LastPrice', 'LastVolume', 'UpdateTime', 'OpenInterest', 'Turnover', 'AvePrice', 'HighestPrice', 'LowestPrice', 'OpenPrice')
    total = trads.count()
    if total < 2:
        logger.warn(f'{_id} has less than 2 items, Ignore.')
        return pd.DataFrame()

    dicts = []
    with tqdm(total=total, desc=f'{_id}:') as pbar:
        for trad in trads:
            pbar.update(1)
            t_dict = trad.to_mongo().to_dict()
            del t_dict['_id']
            del t_dict['tags']
            dicts.append(t_dict)

    with Dbg_Timer(f'from_dict-{_id}', 10):
        df = pd.DataFrame.from_dict(dicts)
        del dicts

        df['TradingTime'] = df.UpdateTime + DateOffset(hours=6)
        df = df.set_index('TradingTime')
        df = df.sort_index()

        # 计算中间变量

        # 数据清洗
        df_desc = df.describe()

        unexpect = df_desc.loc['std'].loc[['LastPrice', 'LastVolume', 'HighestPrice', 'LowestPrice', 'OpenPrice']]
        unexpect = unexpect[unexpect > 50000]
        if not unexpect.empty:
            logger.info(f'{_id}: \n{unexpect}')

        # 1. LastPrice

        # 2. LastVolume

        # 3. AvgPrice: db.getCollection('ticks_CU').find({LastVolume: {$gt: 200000}})，如果超过或低于LastPrice 30%，则用前一个填充
        # 2019.03.30：不需要处理

        # 4. HighestPrice

        # 5. LowestPrice

        # 6. OpenPrice

        # 7. OpenInterest
        std = df_desc.OpenInterest['std']
        if std > 20000000:
            unexcept_idx = df.OpenInterest > std * 2
            num = df[unexcept_idx].shape[0]
            df.loc[unexcept_idx, 'OpenInterest'] = np.NaN
            df.OpenInterest.fillna(method='ffill', inplace=True)

            df_desc = df.describe()
            new_std = df_desc.OpenInterest['std']
            logger.info(f'{_id}: There are {num} unexcept items. std: {std} -> {new_std}.')

        # 8. Turnover

        # check results
        # logger.info(f"{_id}: \n{df_desc.loc['std']}")
    return df


# 生成kline df
def gen_kline_from_pd(_id, df, level, MarketID=4, dbg=False):
    if df.empty:
        logger.warn(f'[{level}-{_id}]: Get empty df.')
        return pd.DataFrame()

    # 生成日K
    kline_df = df.LastPrice.resample(level).ohlc()

    # kline_df.columns = ['OpenPrice', 'HighestPrice', 'LowestPrice', ]
    kline_df['InstrumentID'] = _id
    kline_df['category'] = _id[:2]
    # kline_df['subID'] = _id[2:]
    kline_df['MarketID'] = MarketID
    kline_df['level'] = level

    kline_df['TotalVolume'] = df.LastVolume.resample(level).sum()
    kline_df['volume_std'] = df.LastVolume.resample(level).std()
    kline_df.volume_std.fillna(1.0, inplace=True)

    kline_df['OpenPrice'] = df.OpenPrice.resample(level).first()
    kline_df['HighestPrice'] = df.HighestPrice.resample(level).last()
    kline_df['LowestPrice'] = df.LowestPrice.resample(level).last()
    kline_df['AvePrice'] = df.AvePrice.resample(level).last()
    kline_df['OpenInterest'] = df.OpenInterest.resample(level).last()
    kline_df['Turnover'] = df.Turnover.resample(level).last()
    kline_df['Turnover_new'] = (df.LastPrice * df.LastVolume * 10).resample(level).sum()

    kline_df['tick_num'] = df.Turnover.resample(level).count()

    kline_df.reset_index(inplace=True)
    kline_df.dropna(inplace=True)

    return kline_df


# bulk save
def save_kline_by_id(_id, kline_df, level):
    # 删除之前的数据
    cnt = KlineDoc.objects(InstrumentID=_id, level=level).delete()
    if cnt:
        logger.info(f'{level}-{_id}: delete {cnt} already exists items.')

    if kline_df.empty:
        logger.warn(f'[{level}-{_id}]: Get empty kline_df.')
        return pd.DataFrame()

    # save
    cnt = 0
    day_ks = []
    with Dbg_Timer(f'save_kline-{level}-{_id}', 15):
        for i in range(kline_df.shape[0]):
            cnt += 1
            day_ks.append(KlineDoc(**kline_df.iloc[i].to_dict()))
            if cnt > 1000:
                if day_ks:
                    KlineDoc.objects.insert(day_ks, load_bulk=False)
                cnt = 0
                day_ks = []
        if day_ks:
            KlineDoc.objects.insert(day_ks, load_bulk=False)


# bulk save 日K
def save_1day_kline_by_id(_id, kline_df):
    level = '1d'

    # 删除之前的数据
    cnt = StatisDayDoc.objects(InstrumentID=_id).delete()
    if cnt:
        logger.info(f'{level}-{_id}: delete {cnt} already exists items.')

    if kline_df.empty:
        logger.warn(f'[{level}-{_id}]: Get empty kline_df.')
        return pd.DataFrame()

    # save
    cnt = 0
    day_ks = []
    with Dbg_Timer(f'save_kline-{level}-{_id}', 15):
        for i in range(kline_df.shape[0]):
            cnt += 1
            day_ks.append(StatisDayDoc(**kline_df.iloc[i].to_dict()))
            if cnt > 1000:
                if day_ks:
                    StatisDayDoc.objects.insert(day_ks, load_bulk=False)
                cnt = 0
                day_ks = []
        if day_ks:
            StatisDayDoc.objects.insert(day_ks, load_bulk=False)


def calc_kline_by_id(_ids):
    for _id in _ids:
        df = load_ticks_by_id(_id)
        for level in ['3min', '5min', '15min', '30min', '1H']:
            kline_df = gen_kline_from_pd(_id, df, level, MarketID=4)
            save_kline_by_id(_id, kline_df, level)

        kline_df = gen_kline_from_pd(_id, df, '1d', MarketID=4)
        save_1day_kline_by_id(_id, kline_df)


# 从数据库中load指定的kline数据
def load_kline_to_df(cat, level):
    day_k = KlineDoc.objects(category=cat, level=level)
    total = day_k.count()

    dicts = []
    with tqdm(total=total, desc=f'process:') as pbar:
        for trad in day_k:
            pbar.update(1)
            t_dict = trad.to_mongo().to_dict()
            del t_dict['_id']
            # del t_dict['tags']
            dicts.append(t_dict)

    with Dbg_Timer(f'from_dict', 5):
        df = pd.DataFrame.from_dict(dicts)
        del dicts

        df = df.set_index('TradingTime')
        df = df.sort_index()
        df = df.reset_index()

    return df


# 从数据库中load指定的日K数据
def load_1day_kline_to_df(cat):
    day_k = StatisDayDoc.objects(category=cat)
    total = day_k.count()

    dicts = []
    with tqdm(total=total, desc=f'process:') as pbar:
        for trad in day_k:
            pbar.update(1)
            t_dict = trad.to_mongo().to_dict()
            del t_dict['_id']
            del t_dict['tags']
            dicts.append(t_dict)

    with Dbg_Timer(f'from_dict', 5):
        df = pd.DataFrame.from_dict(dicts)
        del dicts

        df = df.set_index('TradingTime')
        df = df.sort_index()
        df = df.reset_index()

    return df


# Demo: 并发计算Kline数据并入库
if '__main__' == __name__:
    cat_ids = STORED_CATEGORY_LIST
    # cat_ids = ['AG', 'AL', 'AU', 'BU', 'HC', 'NI', 'PB', 'RB', 'RU', 'SN', 'ZN']

    instIDs = []
    for cat_id in cat_ids:
        d_doc = get_dyn_ticks_doc(cat_id)
        instIDs += d_doc.objects().distinct('InstrumentID')

    ids = list(chunks_from_array(instIDs, 120))

    parallel_process_grps(ids, calc_1H_kline_by_id, pool_size=6, spawn=False)
