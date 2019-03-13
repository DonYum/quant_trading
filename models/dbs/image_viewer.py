import datetime
from mongoengine import *

__all__ = (
        'ImagesBatchViewTaskDoc', 'ImagesBatchViewCacheDoc', 'ImageCacheDoc', 'set_img_viewer_task_status',
        'FaceGrpsNeedManuallyMergeDoc', 'FaceBatchGrpsNeedManuallyMergeDoc', 'WorkLoadingStaticsDoc',
    )


# 用于批量筛选图片功能
class ImagesBatchViewTaskDoc(Document):
    meta = {
        'collection': 'imgs_batch_view_task',
        'db_alias': 'dbs',
    }
    _id = SequenceField(db_alias='dbs', primary_key=True)
    name = StringField()
    tag = StringField()             # 用途：标记注册图数据、标记街拍日期
    dataset = IntField()                            # 每一个任务中必须设置一个dataset，和`query_filter`中的必须一一对应。
    sort_field = StringField()
    data_type = StringField(choices=['face'])       # 数据类型，比如face。目前只用于face数据
    query_filter = DictField()
    review_mode = BooleanField(default=False)

    # grp_fields = ListField(StringField())   # 用于分组的字段，目前只支持最多两个字段
    grp_field = StringField()   # 用于分组的字段，'__'格式，没必要支持多个字段，如果有多个则合并起来
    # grp_field_with_ = StringField()   # 用于分组的字段，没必要支持多个字段，如果有多个则合并起来

    # 统计量
    img_num = IntField()
    grp_num = IntField()
    sampled_img_num = IntField(default=0)   # 抽样得到的图片数
    sampled_grp_num = IntField(default=0)   # 抽样得到的分组数
    page_num = IntField(default=0)          # 使用后加1并存储
    sample_page_num = IntField(default=0)   # 抽样后的页码数
    taged_img_num = IntField()

    # 计数器
    grp_cnt = IntField(default=0)
    page_cnt = IntField(default=0)
    split_grp_cnt = IntField(default=0)
    sampled_img_cnt = IntField(default=0)   # 抽样得到的图片数

    # 自动化处理工作量
    grp_num_before_split = IntField()
    grp_num_before_merge = IntField()

    auto_split_grp_inc_num = IntField(default=0)    # 分组增加数量
    auto_drop_grp_num = IntField(default=0)         # drop分组数
    auto_drop_img_num = IntField(default=0)         # drop图片数
    auto_merge_grp_dec_num = IntField(default=0)    # 合并前后分组数差值

    # 自动处理进度统计：auto_process_cnt / auto_process_num
    auto_process_cnt = IntField(default=0)
    auto_process_num = IntField(default=0)
    auto_process_display = BooleanField(default=False)

    # 人工处理工作量
    manual_drop_grp_num = IntField(default=0)
    manual_drop_img_num = IntField(default=0)
    manual_merge_grp_dec_num = IntField(default=0)

    # 限制阈值
    # 多进程并发
    pool_size = IntField(default=41)
    # 显示
    imgs_per_page_th = IntField(default=200)                    # 每页显示图片数量限制
    grps_per_page_th = IntField(default=40)                     # 每页显示分组数量限制
    imgs_display_per_id_max_th = IntField(default=200)         # 每个ID下最多显示的图片数量
    # 合并ID功能下图片显示数量的阈值
    imgs_per_id_manual_merge_th = IntField(default=20)
    # ID下图片去重
    imgs_per_id_remain_after_drop_th = IntField(default=800)    # 对于图片数较多的ID进行抽样，保留下来的图片数量
    # 较少图片的ID为无效的
    imgs_per_id_min_th = IntField(default=20)                   # 图片最小数量阈值，也可以通过配置获取
    imgs_per_id_min_cautious_th = IntField(default=2)           # 图片最小数量阈值，也可以通过配置获取（保守方法）
    # ID下抽取图片数量的上限
    imgs_per_id_sample_th = IntField(default=50)                # 最终根据分布抽取图片
    # 合并ID使用的距离参数
    grps_dis_merge_th = FloatField(default=0.45)        # 分组间距离小于该值则自动合并
    # 做自动、手动合并计算的时候，抽取的分组数量（太大会造成内存溢出）
    merge_sample_size = IntField(default=70000)        # 分组间距离小于该值则合并
    # 切分ID使用的DBSCAN算法参数
    split_dbscan_eps = FloatField(default=0.59)         # DBSCAN eps参数
    split_dbscan_MinPts = IntField(default=1)           # DBSCAN MinPts参数
    # 生成手动merge tab用到的参数
    manual_dist_max_th = FloatField(default=0.6)
    batch_manual_dist_max_th = FloatField(default=0.5)
    batch_manual_dist_step = FloatField(default=0.05)
    manual_dist_min_th = FloatField(default=0.4)        # TODO: DELETE

    filter_tag = StringField()              # 目前用于控制显示，比如：yc

    process_step = StringField(default='init')
    process_status = StringField(default='done', choices=['done', 'ing', 'fail', 'finish'])
    process_hist = ListField(DictField())

    status = StringField(default='init')
    # , choices=[
    #         'init', 'drop', 'fail',
    #         'paging', 'paged',                          # 分页
    #         'caching', 'cached',                        # 缓存
    #         'spliting', 'splited',                      # 拆分grp
    #         'dropping_imgs', 'dropped_imgs',            # grp内图片去重
    #         'merging', 'merged',                        # 合并分组
    #         'dropping_grps', 'dropped_grps',            # 丢弃无效grps
    #         'dropping_d_grps', 'dropped_d_grps',            # 丢弃无效grps
    #         'sampling_imgs', 'sampled_imgs',            # 通过聚类筛选20张图片
    #         'ok',
    #     ])
    status_hist = ListField(StringField())

    description = StringField()
    creater = StringField()
    time = DateTimeField(default=datetime.datetime.now)


# 设置状态，记录状态切换历史
def set_img_viewer_task_status(task_id_or_doc, status):
    if isinstance(task_id_or_doc, int):
        task = ImagesBatchViewTaskDoc.objects.get(pk=task_id_or_doc)
    else:
        task = task_id_or_doc
        task.reload()

    if task:
        task.status_hist.append(task.status)
        task.status = status
        task.save()


# 用于批量筛选图片功能
class ImagesBatchViewCacheDoc(Document):
    meta = {
        'collection': 'imgs_batch_view_cache',
        'db_alias': 'dbs',
        'index_background': True,
        'auto_create_index': False,          # 每次操作都检查。TODO: Disabling this will improve performance.
        'indexes': [
            'task',
            'tag',
            'grp_value',
            'status',
            '-img_num',
            'page',
            'reg_img_num',
        ]
    }
    task = IntField()
    # task = ReferenceField(ImagesBatchViewTaskDoc)
    tag = StringField()                     # 用于批量任务
    grp_value = StringField()               # 与task.grp_field对应

    status = StringField(choices=['taged', 'glanced', 'init', 'drop'])
    img_num = IntField()
    page = IntField()                       # 页码
    reg_img_num = IntField(default=0)       # 注册图的数量

    # 云从识别结果
    target_id = StringField()               # 注册图ID
    yc_score = FloatField(default=0.0)      # 识别score

    all_imgs = ListField(StringField())     # 图片MD5缓存
    del_imgs = ListField(StringField())     # 被排除的图片MD5缓存
    feature_avg = ListField(StringField())  # 特征向量的平均值
    new_grp_idx = IntField(default=1)       # 新建分组的时候在`grp_value`后面串接`_{new_grp_idx}`，然后++new_grp_idx

    avg_features = ListField(FloatField())  # 人脸特征向量的平均值
    distances = ListField(DictField())      # 人脸特征向量和均值的距离
    avg_dist = FloatField()                 # 均值距离的平均值
    std_dist = FloatField()                 # 均值距离的方差

    sort_index = FloatField()

    @queryset_manager
    def valid(doc_cls, queryset):
        return queryset.filter(status__ne='drop')


# 图片缓存
class ImageCacheDoc(Document):
    meta = {
        'collection': 'face_image_caches',
        'db_alias': 'dbs',
    }
    id = SequenceField(db_alias='dbs', primary_key=True)
    name = StringField()
    cache_info = DictField()


class FaceGrpsNeedManuallyMergeDoc(Document):
    meta = {
        'collection': 'face_grps_need_manually_merge',
        'db_alias': 'dbs',
        'index_background': True,
        'auto_create_index': False,          # 每次操作都检查。TODO: Disabling this will improve performance.
        'indexes': [
            'task',
            'src',
            'dst',
            'dist',
            'score',
            'user',
            'merge_status',
            'status',
        ]
    }
    task = IntField()
    src = StringField()
    dst = StringField()
    dist = FloatField()
    score = FloatField()
    user = StringField()
    same_id = BooleanField(default=False)
    called_yc = BooleanField(default=False)
    merge_status = StringField(default='init', choices=['init', 'no', 'yes', 'hide'])       # 初始状态是init，需要标注人员标记是否需要merge。
    status = StringField(default='ok', choices=['ok', 'drop'])      # 用于备份之前的结果

    @queryset_manager
    def valid(doc_cls, queryset):
        return queryset.filter(status='ok')


class FaceBatchGrpsNeedManuallyMergeDoc(Document):
    meta = {
        'collection': 'face_batch_grps_need_manually_merge',
        'db_alias': 'dbs',
        'index_background': True,
        'auto_create_index': False,          # 每次操作都检查。TODO: Disabling this will improve performance.
        'indexes': [
            'task',
            'key',
            'score',
            'merge_status',
            'status',
        ]
    }
    task = IntField()
    key = StringField()
    grps = ListField(StringField())
    score = FloatField()
    user = StringField()
    merge_status = StringField(default='init', choices=['init', 'no', 'yes'])       # 初始状态是init，需要标注人员标记是否需要merge。
    status = StringField(default='ok', choices=['ok', 'drop'])      # 用于备份之前的结果

    @queryset_manager
    def valid(doc_cls, queryset):
        return queryset.filter(status='ok')


class WorkLoadingStaticsDoc(Document):
    meta = {
        'collection': 'work_loading_statics',
        'db_alias': 'dbs',
        'index_background': True,
        'auto_create_index': False,          # 每次操作都检查。TODO: Disabling this will improve performance.
        'indexes': [
            'user',
            'task',
            'operation',
            'method',
        ]
    }
    user = StringField()
    task = IntField()
    operation = StringField()
    method = StringField()
    cnt = IntField()
    del_num = IntField()
    params = DictField()

    time = DateTimeField(default=datetime.datetime.now)

    # @queryset_manager
    # def valid(doc_cls, queryset):
    #     return queryset.filter(status='ok')
