# 交易数据存储方案

业余时间做的一套期货数据存储方案。

## 概述

原始数据包含tick、9999、0000，以csv按特定目录结构存放，数据格式基本统一，主要区别如下：

- 中金所有五档数据，其余只有一档；
- 9999有最后一个字段是主力合约；

这些数据还存在数据校验问题，比如：

- 时间无效；
- 格式混乱；
- volumes为负值；

为了使用方便，我们需要把数据进行预清洗并统一存放。

## 存储方案的选择

开始我们使用MongoDB存储，上面的问题都能满足，但读取速度很慢很慢（一个AG tick要好几分钟）。
在优化性能的时候，想到了直接将df存储为csv、pickle、hdf5、feather等格式。从保存、读取时间、压缩大小等几个角度测试，得到如下结论：

测试文件：`2015/4/AG/AG1506/2015/201503/20150326.spt`(load_df time: `18.9 s`)

| ID | 保存格式  | 压缩格式  |  压缩level  |  大小 |  保存时间 |  读取时间  |  备注  |
|:-----| ----: | :----: | :----: | :----: | :----: | :----: |  :----:  |
| 1 | 原始 | 无| | ~~56M~~  |   |  ~~18.9 s~~  |  没有经过处理  |
| 2 | csv | 无    |    |  35M  | ~~2.59 s~~  |  349 ms  |    |
| 3 | pkl | 无    |    |  25M  | `116 ms`  |  `71 ms`  |  读写性能次优  |
| 4 | pkl | xz    |    |  `2.8M` | ~~6.9 s~~   | 335 ms  |    |
| 5 | pkl | gzip  |    |  `4.3M`  | ~~9.97 s~~  |  137 ms  |    |
| 6 | pkl | bz2   |    |  `4.4M`  | ~~3.36 s~~  |  ~~673 ms~~  |    |
| 7 | pkl | zip |  |  `4.4M`  | 946 ms  |  149 ms  |  `综合性能最优`  |
| 8 | h5 | 无   |      |   26M | 656 ms  |  157 ms  |    |
| 9 | h5 | zlib | 0    |  26M | 289 ms  |  166 ms  |    |
| 10| h5 | zlib | 5    |  14M  | 540 ms  |  174 ms  |    |
| 11| h5 | zlib | 9    |  14M  | 759 ms  |  298 ms  |    |
| 12| h5 | blosc | 5   |  16M | 265 ms  |  104 ms  |    |
| 13| h5 | bzip2 | 5   |  14M | ~~2.31 s~~  |  346 ms  |    |
| 14| h5 | lzo  | 5    |  16M  | 236 ms  |  218 ms  |    |
| 15| feather | 无  |  |  ~~38M~~  |  `83.7 ms` |  `46.2 ms`  | 读写性能最好，但没压缩  |
| 16| MongoDB | 无 |     |     |  ~~30s+~~ |  ~~30s+~~  |    |

PS：~~排除~~，`优势`

结论：**`pickle zip从压缩比、读写速度综合考虑是最好的。`**

**PS：**详见`pandas_save_test.ipynb`

2019.12.11：
在人脸特征测试上feather跟raw pkl写性能相差无几，读性能feather好。
SSD盘

2019.12.12：
还有joblib、cPickle，比pd.pickle差。

## 数据描述

### 原始数据

使用`TickFilesDoc`记录了所有原始文件信息，以及预处理后的信息。

### tick数据

原始的MongoDB存储方案已废弃，新方案使用pickle zip方式。使用方法如下：

```python
q_f = dict(InstrumentID='AG1812', tags=[])
ticks = PickleDbTicks(q_f, main_cls='')
df = ticks.load_ticks()
```

### 主力（9999）

```python
q_f = dict(InstrumentID='AG9999')
ticks = PickleDbTicks(q_f)
df = ticks.load_ticks()
```

### 指数（0000）

### K线

### 数据库使用

#### MongoDB

初始化：

```python
import mongoengine
from models.dbs import *

# _sock = '127.0.0.1:6007'
_auth = 'xxx:xxx'
# URI_statistic = f'mongodb://{_sock}/statistic'

for _doc in ['ticks', 'd_ticks', 'kline', 'statistic']:
    _host = f'mongodb://{_auth}@{_sock}/{_doc}?authSource=admin'
#     _host = f'mongodb://{_sock}/{_doc}'
    print(_host)
    connect(host=_host,  alias=_doc)

cu_doc = get_dyn_ticks_doc('CU')
res = cu_doc.objects()
print(res.count())   # 获取'CU'合约的记录数。
```

## TODOs

- 找主力/可用于回测的数据
- 插值
- 定义指数
