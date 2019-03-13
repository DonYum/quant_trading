# 数据平台Image/Annotation数据模型设计

## 设计原则

按照NoSQL的设计理念，出于增删查补的便利性和效率考虑，所有“包含”性质的信息都应该尽量由一个Document维护（前提是小于16M）。

另，系统功能明确后，我们的Model（或Schema）也应该固定下来，也就是 **我们的model应该直观的反映我们数据结构，以及我们数据库中存有什么信息。**

虽然看上去有点不够“NoSQL”，但请注意这一点 ———

> ***NoSQL不意味着数据随便放，不意味着没有约束。***
> `Explicit is better than implicit!`

必要的约束是极其有重要的：

1. 我们的Model应该适配我们的数据平台系统，并从全局反映我们目前存放的数据树形结构。
2. 我们**可以在Field字段中通过字段类型、choices参数、require参数进行隐形约束，减小系统开发复杂性。**
3. 这样做还有个好处，那就是还可以反映我们的接口信息，比如PreLabel、Label都是和预标、标注功能模块对接的信息，而这些信息从PreLabel、Label的标签结构可以直接看出。

## image和Annotation分开存放及放到一起两方案的对比：

### 分开存放：

好处是逻辑清晰。
但致命缺点如下：

1. 使用mongoengine做不到关联查询

    ```python
    ano = Annotation.objects(image__source='liveme')
    >>> InvalidQueryError: Cannot perform join in mongoDB: image__source
    ```

2. 使用pymongo手写关联查询（借助$lookup指令，该指令在mongoDB 3.4之后才加入）非常复杂。
3. 在mongoengine里使用变通方法可以实现关联查询的效果，但效率极低。

    方法是先查一个表，然后使用in操作查另一个表
    ```python
    imgs = Image.objects(source='liveme')
    ano = Annotation.objects(image__in=imgs)
    ```
    当 imgs.count()=15W 的时候，处理时间需要30s+！

### 放一起：

1. NoSQL的设计思想是把“包含”性质的信息都应该由一个Document维护，img、prelabel、label信息的对应关系是1:M:N，后面两者都是“隶属于”img的，因此是“包含”关系，放一起很好处理。
2. 从查询角度来考虑，放一起之后不需要用到联合查询，所有的查询性能问题都可以通过index设计来解决。
3. 从插入难易程度来看，放一起并不比分开放难多少，而且这种难往往是“心理”层面导致的，实际上由于只操作一个Document，应该会更简单。

## 关于“如果新加入一种训练任务，那我们的model还需要改，这不应该。”的争论

常被大家提到这个问题，争论焦点也往往在此，所以这里做一下解释。

其实讨论的焦点在PreLabel、Label两个标签的设计上，这两个标签分别对应“预标注”和“标注”模块。
首先，以预标注来讲，不管是meitu人脸多属性、ssd人脸检测、质量检测，还是以后要上的各种前向算法，接口都是固定的，把这些输出数据固定成标准数据结构本身是没有问题的。
即使需要扩展新前向算法，那也是系统升级的一种，这时候添加新的PreLabel model很正常。
再来看标注系统对应的Label，我们现在把标注系统提供的数据接口信息抽象化形成Label标签（目前包括分类、属性、物体检测、关键点），发现结构是固定的，存放成固定格式完全没问题。
如果需要扩展（比如多标签），那么这个修改是标注系统和数据系统都要修改的，在Label model里添加新标签信息自然也很正常。

总之，我们的原则是在Model里进行约束，通过model设计反映我们目前拥有的数据信息，以及索引路径。

## 文件结构

```txt
models                  # 存放mongoDB对应的schema/model
├── __init__.py           # 用于暴露model接口
├── migrate               # 用于**数据迁移**
│   ├── create_testdb.py    # 创建测试数据库
│   ├── migrate_img.py      # 迁移Image数据
│   └── migrate_anno.py     # 迁移Annotation数据
├── db_v1                 # 旧版本schema
│   ├── __init__.py
│   ├── annotation.py
│   └── image.py
├── dbs                 # 新版本schema
│   ├── __init__.py         # 初始化工作、暴露接口
│   ├── image_anno.py         # **当前使用的schema**
│   ├── image.py
│   ├── import_task.py        # 导入任务管理
│   ├── label.py              # 标注model
│   ├── prelabel.py           # 预标model
│   ├── label_task.py         # 预标、标注任务管理
│   ├── outer_tabs.py         # 外部约束表，比如src、cat、tags等
│   ├── schema_templ.py       # schema模版，用于指导设计，实际使用中不起作用。
│   ├── version.py            # 版本信息
│   └── worldtree.py          # worldtree
├── perf_anal             # 性能量化数据
└── README.md
```

## 关于命名规则

为了规范model名称、方便区分Document和EmbededDocument，命名规则如下：

1. model使用驼峰命名；
2. 使用“xxxDoc”和“xxxEmDoc”区分Document和EmbededDocument，Document有save方法，EmbededDocument必须嵌入到Document中去使用；
3. 有从属关系的会将所属class名称放置到前面，比如LabelAttrEmDoc从属于LabelEmDoc；

### 按照此命名规则得到的数据结构class——

```python
'ImageAnnoDoc',
    'ImageEmDoc',
    'PreLabelEmDoc',
        'Pl_YoutuEmDoc',
        'Pl_SsdEmDoc',
        'Pl_QualEmDoc',
    'LabelEmDoc',
        'LabelKpEmDoc',
        'LabelObjDetEmDoc',
        'LabelClsEmDoc',
        'LabelMultiEmDoc',
        'LabelAttrEmDoc',
            'LabelAttrFaceEmDoc',
            'LabelAttrBagEmDoc',
            'LabelAttrCatEmDoc',
    'PreLabelingEmDoc',
    'LabelingEmDoc'

# 导入、标记、预标任务管理
'ImportTaskDoc',
'PreLabelTaskDoc',
'LabelTaskDoc',

# 约束表
'SourceDoc',
'CategoryDoc',
'TypeDoc',
'DataSetDoc',
```

## 新老数据模型使用方法

### 调用方法

调用最新数据模型：

```python
from models import *
# 或
from models.dbs import *
```

调用老的数据模型：

```python
from models.db_v1 import *
```

### 数据库连接

#### MongoDB

新老数据模型通过`db_alias`方式指定，因此在连接数据库的时候一定要指定**`alias`**参数！

标准连接方式如下：

新数据库：

```python
import mongoengine
from http_service.models.dbs import *

URI_v2 = 'mongodb://orion:orion2019@10.60.242.96:20000,10.60.242.97:20000,10.60.242.98:20000/data_pf?authSource=admin&readPreference=secondaryPreferred'
connect(host=URI_v2,  alias='dbs', connect=False)

imgs = ImageAnnoDoc.objects(category='face')
print(imgs.count())   # 获取category='face'的记录数。具体操作方式请参看mongoengine资料。
```

老数据库（已废弃）：

```python
import mongoengine
from http_service.models.db_v1 import *

URI_v1 = 'mongodb://orion:orion2019@10.60.242.96:20000,10.60.242.97:20000,10.60.242.98:20000/data_manager?authSource=admin&readPreference=secondaryPreferred'
connect(host=URI_v1,  alias='db_v1', connect=False)
```

其中`alias='db_v1'`对应于老数据模型，`alias='dbs'`对应于新数据模型。

#### Ceph

```python
from http_service.models.ceph import CephS3

md5 = '62ae831bc75e24c4e3ed4d8a4f3d2980'        # md5对应于MongoDB的ImageAnnoDoc.image.md5
ceph_s3 = CephS3('/mnt/cephfs_online/ceph_bk_storage')
img_content = ceph_s3.read(md5)
```

## 数据迁移

详见`/modelsmigrate/README.md`

## 其他细节

### 关于连接多个数据库

1. `meta`信息中声明`'db_alias': 'db_v1'`和`'db_alias': 'dbs'`；
2. `connect(host=config['MONGODB_URI'], alias='db_v1')`注册两个连接。
3. 2018.07.20：如果使用`SequenceField`字段，记得一定要设置`db_alias='dbs'`参数！因为该字段会生成一个表`mongoengine.counters`，然后往这个表里填写累加值。

**[Issue 8]**: <http://gitcv.ainirobot.com/data_platform/data_platform/issues/8>

### 关于插入性能

批量写与单条写差了一个数量级：<https://www.jianshu.com/p/a9b96d840d37> <br>
`bulkwrite/insert_many`对应于mongoengine的`insert(_list, load_bulk=False)`方法。<br>
Ref: <http://docs.mongoengine.org/apireference.html?highlight=insert#mongoengine.queryset.QuerySet.insert>

**查看是否是SSD！**
索引会降低插入速度，但是影响非常小！

### 关于`Cursor not found (namespace: 'data_manager.image', id: 1327549972637571375).`

64000<br>
两种尝试方法：

1. `batch_size(5000)`，设置batch_size返回文档数，可以设置小一些。根据处理时间自定即可，跟多个因素有关，目前使用5000没问题。

    Ref: <http://docs.mongoengine.org/apireference.html?highlight=batch#mongoengine.queryset.QuerySet.batch_size>
2. 设置`timeout(False)`，（对应于pymongo的`no_cursor_timeout=True`），永不超时，游标连接不会主动关闭，需要手动关闭。

    Ref: <http://docs.mongoengine.org/apireference.html?highlight=cursor#mongoengine.queryset.QuerySet.timeout>

**[Issue 9]**: <http://gitcv.ainirobot.com/data_platform/data_platform/issues/9>

### 关于limit()在count()和update()不起作用的问题

By default, count() ignores limit() and counts the results in the entire query. <br>
count()可以使用`with_limit_and_skip=True`参数。<br>
对于update()，可以使用list记录_id，然后使用in判断。

Ref:

- <https://stackoverflow.com/questions/15300882/limiting-results-in-mongodb-but-still-getting-the-full-count>
- <https://github.com/MongoEngine/mongoengine/issues/943>
- <https://stackoverflow.com/questions/6590890/how-to-limit-number-of-updating-documents-in-mongodb>

### 关于ceph图片备份

请参考`backuped@ImageEmDoc`字段。

### 关于递增ID做主键的设置

默认的情况会使用objectID，但该字段太重，在存储少量信息的情况下，使用递增ID即可。<br>
但是MongoDB原生不支持递增ID，好在mongoengine支持，使用SequenceField()可做到。<br>
那么使用递增ID做PK的方法也就有了：`_id = SequenceField(primary_key=True)`（必要的使用填写`db_alias`参数，见“连接多个数据库”）。

### 关于字段默认值的设置方法

**default参数不仅可以设置常量，还可以设置函数——在插入的时候执行该函数。** <br>
具体使用方法请参考`importid@ImageAnnoDoc`的设置方法。

### 关于`OperationError: Some documents have ObjectIds use doc.update() instead.`

```python
    if doc.pk and not doc._created:
        msg = 'Some documents have ObjectIds use doc.update() instead'
        raise OperationError(msg)
```

解决方法：`imgtest._created = True`

## ChangeLog

### V1

根据大家的意见初步设计了数据结构，img和anno分开，见`schema_templ.py`。

### V2

根据自己的测试结果，发现img和anno分开存储最大的问题是关联查询。另外，img和anno是明确的包含关系，完全可以存储到一起！

### V3

2018.07.27：对label结构做了大调整。

调整是基于下面前提进行的：

1. 对于人脸多属性、keypoint数据来讲，都是局部图，只会存在一个关键要素（人脸或身体）（bbox不存也没关系），上传数据的时候会先抠图处理；
2. 对于物体检测数据来讲，是同时存在多个对象的（人脸、身体、水果、动物等），是一个`bbox+tag+label`串。

    调整的内容包括：
    1. 使用规范化的bbox——[x1, y1, x2, y2] + width + conf，注意一下conf的值=2说明没有设置conf。使用`norm_bbox()`生成即可；
    2. Label中包含两类数据类型——局部图对应的标签（人脸多属性、keypoint等数据）、多物体数据（物体检测等）。

2018.08.01（和振辉讨论）：关于label结构的制定，需要明确的是——要考虑数据存储的抽象层面，数据接口或任务种类是一种检验标准（是否能cover住）。

## 备注

非常欢迎大家提需求。
感谢。

## 参考：

**请审阅该Model的人员一定认真读一下《Thinking in Documents》**，链接如下：

- <https://www.mongodb.com/blog/post/thinking-documents-part-1>
- <https://www.mongodb.com/blog/post/thinking-documents-part-2>
- 关于性能：<https://blog.csdn.net/xtj332/article/details/41314741>

## TODOs

2018.09.15：自动导入完成后需要执行一下`update_src_cat_stat_tab(...)`来更新关联表。
