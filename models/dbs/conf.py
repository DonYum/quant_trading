from pathlib import Path

TICKS_PATH = Path('/data/ticks')
RAW_DATA_PATH = Path('/data/raw_data')

PICKLE_COMPRESSION = 'zip'     # 测试下来读写性能综合考虑zip是最优的方法
PICKLE_COMPRESSION_VER = 1

MAX_DATE = '2025-12-12'
MIN_DATE = '2015-12-12'
