import datetime
import logging
import pandas as pd
from pathlib import Path
from tqdm import tqdm_notebook as tqdm

__all__ = (
        'StockBasicDoc', 'Stock1dOps', 'StockConfDoc',
    )

logger = logging.getLogger()


class StockDbException(Exception):
    pass


class StockBasicDoc(Document):
    meta = {
        'collection': 'stock_basic',
        'db_alias': 'stock',
        'index_background': True,
        'auto_create_index': True,          # 每次操作都检查。TODO: Disabling this will improve performance.
        'indexes': [
            'ts_code',
            'name',
        ]
    }
    ts_code = StringField()                   # 000001.SZ
    symbol = StringField()                   # 000001
    name = StringField()                   # 平安银行
    area = StringField()                   # 深圳
    industry = StringField()                   # 银行
    market = StringField()                   # 主板
    list_date = StringField()            # 19910403

    # 自定义
    tags = ListField(StringField())
    init_time = DateTimeField()            # 上市时间


class Stock1dOps():
    def __init__(self, ts_code, root_dir='/ticks/stock/1d_Kline'):
        self.ts_code = ts_code
        self.root_dir = Path(root_dir)

        self.tag = 'got_1dK'

        self.pro_bar_param_name = 'qfq'
        self.pro_bar_param = StockConfDoc.get_env(self.pro_bar_param_name)
        if not self.pro_bar_param:
            raise StockDbException(f'{ts_code}: get None param: {self.pro_bar_param}')

        self.f_name = f'{self.ts_code}.pkl'
        self.f_path = self.root_dir / self.pro_bar_param_name / self.f_name

    # 检查pkl文件是否存在
    def pkl_exists(self):
        return self.f_path.exists()

    # 删除pkl文件
    def del_pkl(self):
        try:
            self.f_path.unlink()
        except Exception:
            pass
        StockBasicDoc.objects(ts_code=self.ts_code).update(pull__tags=self.tag)

    # 加载1d K to df
    def load_df(self):
        if not self.pkl_exists():
            return None
        df = pd.read_pickle(self.f_path)
        return df

    def dl_save_data(self):
        item = StockBasicDoc.objects(ts_code=self.ts_code, tags__nin=['ing', self.tag]).first()
        if not item:
            return False

        item.update(add_to_set__tags='ing')

        df = ts.pro_bar(ts_code=item.ts_code, **self.pro_bar_param)
        if df.empty:
            logger.warn(f'{item.ts_code} is empty')
            item.update(pull__tags='ing')
            return False

        df.to_pickle(self.f_path)

        item.update(add_to_set__tags=self.tag)
        item.update(pull__tags='ing')

        return True


class StockConfDoc(Document):
    meta = {
        'collection': 'stock_conf',
        'db_alias': 'stock',
    }
    key = StringField(primary_key=True)
    value = DynamicField()
    tag = StringField()

    @classmethod
    def set_env(cls, key, value, **kwargs):
        try:
            cls(key=key, value=value, **kwargs).save()
        except:
            print(f'set key=`{key}` failed: value={value}.')
            return False
        return True

    @classmethod
    def get_env(cls, key, default=None):
        env = cls.objects(key=key).first()
        if not env:
            return default
        else:
            return env.value
