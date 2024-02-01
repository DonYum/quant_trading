import os
import time
import logging

# __all__ = ('chunks_from_array', 'chunks_from_array_by_size', 'sample_arr_by_fix_step', 'Dbg_Timer', )
__all__ = ('Dbg_Timer', )

logger = logging.getLogger(__name__)


def format_file_size(size, batch=1024):
    if not size:
        return '0'
    size_dis = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
    _s = size
    cnt = 0
    while _s > batch:
        cnt += 1
        _s /= 1024

    return f'{round(_s, 1)}{size_dis[cnt]}'


# 用于list分页的生成器
# 考虑到对齐，每组的长度可能不一样。
# 参数：
#   - arr: list
#   - pages: 分成num组
#   - page_size: 每组cnt个元素
# 返回：
#   list
def chunks_from_array(arr, pages):
    if not pages:
        pages = 1

    _len = len(arr)
    if not arr or not isinstance(arr, list):
        return []

    # 处理超出数组长度的情况
    if pages > _len:
        pages = _len

    for i in range(pages):
        yield arr[i::pages]


def chunks_from_array_by_size(arr, page_size):
    if not page_size:
        raise ValueError(f'Invalid params.')

    _len = len(arr)
    if not arr or not isinstance(arr, list):
        return []

    # 处理超出数组长度的情况
    if page_size > _len:
        page_size = _len

    pages = _len // page_size

    for i in range(pages):
        yield arr[i::pages]


# 从Array中等间距抽样
# 参数：
#   - arr
#   - size: 抽取个数
#   - omit: 取非交集
def sample_arr_by_fix_step(arr, size, omit=False):
    _len = len(arr)
    if omit:
        size = _len - size

    if size <= 0:
        return []

    step = (_len + 1) / size
    # print(step)
    if step > 1:
        idx = 0
        while idx < _len:
            yield arr[int(idx)]
            idx += step
    else:
        for i in arr:
            yield i


# 时间相关接口
time_map = ['s', 'ms', 'us', 'ns']


def format_time(time_sec):
    if not time_sec:
        return ''
    for i in range(len(time_map)):
        if time_sec > 1:
            break
        time_sec *= 1000
    res = f'{round(time_sec, 1)}{time_map[i]}'
    return res


# 用于debug代码执行时间
class Dbg_Timer():
    def __enter__(self):
        self.start = time.time()
        return self

    def __init__(self, tag, time_th=3):
        self.start = time.time()
        self.tag = tag
        self.time_th = time_th

    def _flied(self):
        return time.time() - self.start

    def flied(self):
        return format_time(self._flied())

    def timeout(self, time_th=None):
        time_th = time_th or self.time_th
        return self._flied() > time_th

    def __exit__(self, *args):
        if self.timeout():
            logger.warning(f'[{self.tag}]: Exec Time={self.flied()}')
