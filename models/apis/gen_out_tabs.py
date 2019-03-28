import logging
import time
from .fields_statics import get_doc_statics
# from ..dbs.image_anno import ImageAnnoDoc
# from ..dbs.outer_tabs import *

logger = logging.getLogger(__name__)

__all__ = (
        'update_notes_tab', 'update_dataset_tab', 'flush_dataset_tab',
        'regen_src_tab', 'regen_cat_tab',
        # 'update_src_stat_tab', 'update_cat_stat_tab', 'update_src_cat_stat_tab',
        'update_all_related_src_cat_tab', 'regen_all_related_src_cat_tab',
        'fake_all_related_src_cat_tab',
    )


# 更新notes字段统计信息
def update_notes_tab(notes=[]):
    query_filter = {}      # 用于过滤出需要统计的数据
    delete_filter = {}     # 用于过滤需要删除的数据

    if notes:
        query_filter['notes_key__in'] = notes
        delete_filter['note__in'] = notes

    # 设置默认过滤参数
    if not query_filter:
        query_filter['notes__exists'] = True
        query_filter['notes__ne'] = {}

    logger.info(f'[update_notes]: filter={query_filter}.')

    # 执行aggregate
    fields = {'notes_key': 'note'}
    unwind = ['notes_key']

    starttime = time.time()
    res = list(get_doc_statics(query_filter, fields, unwind=unwind, sample_size=0, preserveNull=False, dbg=False))

    # 更新notes统计信息表
    to_del = NotesStaticsDoc.objects(**delete_filter)
    del_cnt = to_del.count()
    to_del.delete()

    for i in res:
        NotesStaticsDoc(**i).save()

    logger.info(f'[notes_key]: get new_cnt={len(res)}, del_cnt={del_cnt}, del_query={delete_filter}, Time={"%.2fs" % (time.time()-starttime)}.')


# 更新Dataset关联表单条记录
def update_dataset_tab(ds_id):
    ds = DataSetDoc.objects(pk=ds_id).first()
    imgs_cnt = ImageAnnoDoc.objects(dataset=ds_id).count()
    ds.count = imgs_cnt
    ds.save()


# 更新Dataset关联表整个表
def flush_dataset_tab():
    dses = DataSetDoc.objects()
    for ds in dses:
        imgs_cnt = ImageAnnoDoc.objects(dataset=ds.pk).count()
        ds.count = imgs_cnt
        ds.save()


# 老版本更新CategoryDoc的代码。
# 由于更新SourceDoc和更新CategoryDoc的代码common的比较多，所以重构了。
# def update_cat_tab(category=None):
#     if not category:
#         logger.warn(f'[update_cat_tab]: Invalid `category`.')
#         return

#     if category:
#         query_filter['category'] = category

#     imgs_cnt = ImageAnnoDoc.objects(**query_filter).count()
#     logger.info(f'[update_cat_tab]: Query={query_filter}. Found {imgs_cnt} valid imgs.')

#     cat = CategoryDoc.objects(name=category).first()
#     if cat:
#         cat.update(count=imgs_cnt)
#     else:
#         CategoryDoc(name=category, count=imgs_cnt).save()
#     logger.info(f'Update `CategoryDoc` finished.')


# 根据条件更新SourceDoc或CategoryDoc中的数据
def _update_src_or_cat_tab(kind, value):
    if kind not in ['source', 'category'] or not value:
        logger.warn(f'[update_src_or_cat_tab]: Invalid parameters: kind={kind}, value={value}.')
        return
    # logger.info(f'[update_src_or_cat_tab]: parameters: kind={kind}, value={value}.')

    query_filter = {}
    meta_cls = None
    if kind == 'source':
        query_filter['source'] = value
        meta_cls = SourceDoc
    elif kind == 'category':
        query_filter['category'] = value
        meta_cls = CategoryDoc
    else:
        logger.warn(f'[update_src_or_cat_tab]: Invalid parameters: kind={kind}, value={value}.')
        return

    imgs_cnt = ImageAnnoDoc.objects(**query_filter).count()
    logger.info(f'[update_{kind}_docs]: Query={query_filter}. Found {imgs_cnt} valid imgs.')

    res = meta_cls.objects(name=value).first()
    if res:
        res.update(count=imgs_cnt)
    else:
        meta_cls(name=value, count=imgs_cnt).save()
    # logger.info(f'[update_{kind}_docs]: Update finished.')


# 更新SourceDoc中的数据
def update_src_tab(source):
    _update_src_or_cat_tab('source', source)


# 更新CategoryDoc中的数据
def update_cat_tab(category):
    _update_src_or_cat_tab('category', category)


# 重新生成SourceDoc表
def regen_src_tab():
    SourceDoc.objects().delete()
    srcs = ImageAnnoDoc.objects().distinct('source')
    for source in srcs:
        _update_src_or_cat_tab('source', source)


# 重新生成CategoryDoc表
def regen_cat_tab():
    CategoryDoc.objects().delete()
    cats = ImageAnnoDoc.objects().distinct('category')
    for category in cats:
        _update_src_or_cat_tab('category', category)


# 生成source统计关联表
# 如果参数都为None，那么删表重建，否则只更新相关信息。
def update_src_stat_tab(source=None, subsource=None):
    query_filter = {}

    if source:
        query_filter['source'] = source
    if subsource:
        query_filter['subsource'] = subsource

    logger.info(f'[src_stat]: filter={query_filter}.')
    starttime = time.time()
    res = ImageAnnoDoc.objects().aggregate(*[
        {'$match': query_filter},
        # {'$sample': {'size': 100000}},
        {'$project': {
                'source': 1, 'subsource': 1
        }}, {'$group': {
                '_id': {'source': '$source', 'subsource': '$subsource'},
                'count': {'$sum': 1}
        }}, {'$project': {
                'source': '$_id.source', 'subsource': '$_id.subsource',
                'count': 1, '_id': 0
        }}, {'$sort': {'count': -1}}
    ])

    res = list(res)
    logger.info(f'[src_stat]: res_cnt={len(res)}, Time={"%.2fs" % (time.time()-starttime)}.')

    # 创建/更新source统计信息表
    SourceStaticsDoc.objects(**query_filter).delete()

    for i in res:
        SourceStaticsDoc(**i).save()


# 生成category统计关联表
# 如果参数都为None，那么删表重建，否则只更新相关信息。
def update_cat_stat_tab(category=None, subcategory=None, input_tag=None):
    query_filter = {}

    if category:
        query_filter['category'] = category
    if subcategory:
        query_filter['subcategory'] = subcategory
    if input_tag:
        query_filter['input_tag'] = input_tag

    logger.info(f'[cat_stat]: filter={query_filter}.')
    starttime = time.time()
    res = ImageAnnoDoc.objects().aggregate(*[
        {'$match': query_filter},
        # {'$sample': {'size': 100000}},
        {'$project': {
                'category': 1, 'subcategory': 1, 'input_tag': 1
        }}, {'$group': {
                '_id': {'category': '$category', 'subcategory': '$subcategory', 'input_tag': '$input_tag'},
                'count': {'$sum': 1}
        }}, {'$project': {
                'category': '$_id.category', 'subcategory': '$_id.subcategory', 'input_tag': '$_id.input_tag',
                'count': 1, '_id': 0
        }}, {'$sort': {'count': -1}}
    ])

    res = list(res)
    logger.info(f'[cat_stat]: res_cnt={len(res)}, Time={"%.2fs" % (time.time()-starttime)}.')

    # 创建/更新source统计信息表
    CategoryStaticsDoc.objects(**query_filter).delete()

    for i in res:
        CategoryStaticsDoc(**i).save()


# 生成category统计关联表
# 如果参数都为None，那么删表重建，否则只更新相关信息。
def update_src_cat_stat_tab(source=None, subsource=None, category=None, subcategory=None, input_tag=None):
    query_filter = {}

    if source:
        query_filter['source'] = source
    if subsource:
        query_filter['subsource'] = subsource
    if category:
        query_filter['category'] = category
    if subcategory:
        query_filter['subcategory'] = subcategory
    if input_tag:
        query_filter['input_tag'] = input_tag

    logger.info(f'[src_cat_stat]: filter={query_filter}.')
    starttime = time.time()
    res = ImageAnnoDoc.objects().aggregate(*[
        {'$match': query_filter},
        # {'$sample': {'size': 100000}},
        {'$project': {
            'source': 1, 'subsource': 1, 'category': 1, 'subcategory': 1, 'input_tag': 1
        }}, {'$group': {
            '_id': {
                'source': '$source', 'subsource': '$subsource', 'category': '$category', 'subcategory': '$subcategory',
                'input_tag': '$input_tag'
            },
            'count': {'$sum': 1}
        }}, {'$project': {
            'source': '$_id.source', 'subsource': '$_id.subsource', 'category': '$_id.category',
            'subcategory': '$_id.subcategory', 'input_tag': '$_id.input_tag',
            'count': 1, '_id': 0
        }}, {'$sort': {'count': -1}}
    ])

    res = list(res)
    logger.info(f'[src_cat_stat]: res_cnt={len(res)}, Time={"%.2fs" % (time.time()-starttime)}.')
    logger.debug(f'[results]: {res}')

    # 创建/更新source统计信息表
    # if not query_filter:
    #     SrcCatStaticsDoc.drop_collection()
    # else:
    SrcCatStaticsDoc.objects(**query_filter).delete()

    for i in res:
        SrcCatStaticsDoc(**i).save()


# 根据输入参数情况自动`更新`统计关联表
# regen: False——只会更新关联信息。如果其他参数都为空，则不更新！
#        True——不管其他参数是否为空，都会更新。如果其他参数为空，则重构相关关联表！
def update_all_related_src_cat_tab(source=None, subsource=None, category=None, subcategory=None, input_tag=None, regen=False):
    tag1 = tag2 = False

    if regen:
        regen_src_tab()
        regen_cat_tab()
        update_src_stat_tab(source=source, subsource=subsource)
        update_cat_stat_tab(category=category, subcategory=subcategory, input_tag=input_tag)
        update_src_cat_stat_tab(source=source, subsource=subsource, category=category, subcategory=subcategory, input_tag=input_tag)
    else:
        if source:
            update_src_tab(source=source)

        if category:
            update_cat_tab(category=category)

        if source or subsource:
            tag1 = True
            update_src_stat_tab(source=source, subsource=subsource)

        if category or subcategory or input_tag:
            tag2 = True
            update_cat_stat_tab(category=category, subcategory=subcategory, input_tag=input_tag)

        if tag1 or tag2:
            update_src_cat_stat_tab(source=source, subsource=subsource, category=category, subcategory=subcategory, input_tag=input_tag)


# 根据输入参数情况自动`重建`统计关联表
# 不管其他参数是否为空，都会更新。如果其他参数为空，则重构相关关联表！
def regen_all_related_src_cat_tab(source=None, subsource=None, category=None, subcategory=None, input_tag=None):
    update_all_related_src_cat_tab(source=source, subsource=subsource, category=category, subcategory=subcategory, input_tag=input_tag, regen=True)


# 插入faked src/cat 统计信息
def fake_all_related_src_cat_tab(source, subsource, category, subcategory, input_tag, count=100):
    # SourceDoc(**i).save()
    # CategoryDoc(**i).save()
    SourceStaticsDoc(source=source, subsource=subsource, count=count).save()
    CategoryStaticsDoc(category=category, subcategory=subcategory, input_tag=input_tag, count=count).save()
    SrcCatStaticsDoc(source=source, subsource=subsource, category=category, subcategory=subcategory, input_tag=input_tag, count=count).save()
