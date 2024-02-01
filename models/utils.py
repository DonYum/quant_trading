import os
import time
import random
import datetime
import logging
from .dbs.tick_pickle import PickleDbTicks


__all__ = ('is_in_time_range', 'calc_reward', 'random_load_data', )

logger = logging.getLogger(__name__)


def is_in_time_range(c_time):
    __trade_time = [
        # ('09:03:00', '11:28:00'),
        # ('13:33:00', '14:58:00'),
        # ('21:03:00', None),
        # (None, '02:29:00'),
        ('06:03:00', '08:28:00'),
        ('10:33:00', '11:58:00'),
        ('18:03:00', '23:28:00'),
    ]
    c_time = c_time - datetime.timedelta(hours=3)
    c_time = c_time.strftime('%H:%M:%S')
    for _start, _end in __trade_time:
        if _start < c_time < _end:
            # print(c_time)
            return True
    return False


val_rolling = 0
def calc_highest_lowest(df_s, type='high'):
    global val_rolling
    val_rolling = 0 if type == 'high' else 1e20
    def highest(x):
        global val_rolling
        val_rolling = max(x, val_rolling)
        return val_rolling
    def lowest(x):
        global val_rolling
        val_rolling = min(x, val_rolling)
        return val_rolling

    func = highest if type == 'high' else lowest
    return df_s.apply(func)


def calc_reward(df_op, op_type, stop_loss_th, multiplier):
    '''
    使用移动止损。先找有没有被止损，如果被止损则取止损价，如果没有止损则取最后一个价格。
    最后返回reward。
    '''
    op_price = df_op['LastPrice'].iloc[0]    # 买入价
    if op_type == 'buy':
        df_op['highest'] = calc_highest_lowest(df_op['LastPrice'], 'high')
        df_op['stop_loss'] = df_op['highest'] * (1 - stop_loss_th)      # - df_op.index*0.5/10000
        df_op['stop_loss'] = df_op['stop_loss'] - (df_op['highest'] - op_price) * (1/5)      # - df_op.index*0.5/10000
        _df_stop = df_op[df_op['LastPrice'] <= df_op['stop_loss']]
    else:
        df_op['lowest'] = calc_highest_lowest(df_op['LastPrice'], 'low')
        df_op['stop_loss'] = df_op['lowest'] * (1 + stop_loss_th)       # + df_op.index*0.5/10000
        df_op['stop_loss'] = df_op['stop_loss'] + (op_price - df_op['lowest']) * (1/5)      # - df_op.index*0.5/10000
        _df_stop = df_op[df_op['LastPrice'] >= df_op['stop_loss']]
    if not _df_stop.empty:
        end_price = _df_stop['LastPrice'].iloc[0]
    else:
        end_price = df_op['LastPrice'].iloc[-1]
    if op_type == 'buy':
        reward = (end_price - op_price) * multiplier
    else:
        reward = (op_price - end_price) * multiplier
    # print(op_price, end_price)
    return reward


def random_load_data(cat):
    # 查找该主力一共有哪些天
    ticks = PickleDbTicks(dict(category=cat, subID='9999'), main_cls='')
    all_days = ticks.ticks.distinct('day')
    all_days = sorted(all_days)
    # sel_day = random.choice(all_days)
    sel_day_idx = random.randint(0, len(all_days)-2)
    sel_day = all_days[sel_day_idx:sel_day_idx+2]
    
    # 加载数据
    ticks = PickleDbTicks(dict(category=cat, subID='9999', day__in=sel_day), main_cls='')
    df = ticks.load_ticks()
    drop_cols = [
        'InstrumentID', 'MarketID', 'mainID',
        # 'AskPrice2', 'AskPrice3', 'AskPrice4', 'AskPrice5',
        # 'AskVolume2', 'AskVolume3', 'AskVolume4', 'AskVolume5',
        'BidPrice2', 'BidPrice3', 'BidPrice4', 'BidPrice5',
        'BidVolume2', 'BidVolume3', 'BidVolume4', 'BidVolume5',
    ]
    df = df.drop(drop_cols, axis=1)
    df = df.reset_index(drop=True)
    # return df, sel_day

    # night数据是分开两天存储的
    try:
        _df = df[(df.day==sel_day[1]) & (df.time_type!='night')]
        df = df.iloc[:_df.index[0]]
    except:
        print(f'sth went wrong.')

    # 随机选择上午、下午、晚上
    t_type = random.choice(['night', 'am_pm'])
    if t_type == 'am_pm':
        df = df[(df.time_type == 'fam') | (df.time_type == 'bam') | (df.time_type == 'pm')]
    else:
        df = df[df.time_type == t_type]

    return df, sel_day, t_type
