# from mongoengine import ValidationError
import json
import logging
from collections import OrderedDict

from ..dbs.trading import TickFilesDoc

__all__ = ('get_doc_statics', )

logger = logging.getLogger(__name__)


# input:
#   - query_filter: dict. 查询过滤条件。
#   - fields: dict. key是fields，value是重命名的值。
#   - document: Document. 待统计的Document，默认是TickFilesDoc。
#   - unwind: list. fields中是List的字段。
#   - sum_field: 指定后会$sum该字段。
#   - sample_size: int. 采样大小，加速运算。默认为0，表示不采样。
# return:
#   - generator
def get_doc_statics(query_filter, fields, document=None, unwind=None, sum_field=None, preserveNull=True, sample_size=0, sort=True, dbg=False):
    # 使用有序字典排序
    proj1 = OrderedDict()
    proj2 = OrderedDict()
    grp = OrderedDict()
    for key, rename in fields.items():
        key_name = key
        if rename:
            key_name = rename
        if '.' in key and not rename:
            key_name = key.replace('.', '__')
        proj1[key] = 1
        proj2[key_name] = '$_id.' + key_name
        grp[key_name] = '$' + key

    if sum_field:
        proj1[sum_field] = 1

    proj2['count'] = 1
    proj2['_id'] = 0

    # 构建aggregation表达式
    agg = []
    # if query_filter:
    #     agg.append({'$match': query_filter})
    if sample_size:
        agg.append({'$sample': {'size': sample_size}})
    agg += [{'$project': proj1}]
    if unwind:
        for key in unwind:
            if preserveNull:
                agg.append({'$unwind': {'path': '$' + key, 'preserveNullAndEmptyArrays': True}})
            else:
                agg.append({'$unwind': '$' + key})

    sum_tag = 1
    if sum_field:
        sum_tag = f'${sum_field}'
    agg += [
            {'$group': {'_id': grp, 'count': {'$sum': sum_tag}}},
            {'$project': proj2}
        ]

    if sort:
        agg += [{'$sort': {'count': -1}}]

    if dbg:
        logger.info(json.dumps(agg, indent=4, separators=(',', ':')))

    # 执行聚合运算。
    # 先过滤和aggregate里使用$match过滤效果一样、性能相差不大。
    res = document.objects(**query_filter).aggregate(*agg)
    res = filter(lambda x: x[list(fields.keys())[0]], res)
    return res


# Demo
if '__main__' == __name__:
    fields = {'InstrumentID': 0}
    unwind = []
    query_filter = dict(MarketID=4)
    res = list(get_doc_statics(query_filter, fields, unwind=unwind, sum_field=None, document=TickFilesDoc, preserveNull=True, sample_size=0, dbg=False))
