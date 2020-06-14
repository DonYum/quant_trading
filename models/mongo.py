# import logging
from contextlib import contextmanager
from mongoengine import connect, get_connection, disconnect_all

__all__ = ('conn_mongo', 'conn_protect', )

# logger = logging.getLogger(__name__)


# 连接数据库
# _sock = '127.0.0.1:6007'
_sock = '192.168.9.13:6007'
_auth = 'eric:11112222'

_conn_map = {}
for _doc in ['ticks', 'd_ticks', 'kline', 'statistic']:
    _host = f'mongodb://{_auth}@{_sock}/{_doc}?authSource=admin'
    _conn_map[_doc] = _host


# connect(host=_host,  alias=_doc, connect=True)
# conn_mongo('ticks')
def conn_mongo(alias):
    try:
        connect(host=_conn_map[alias],  alias=alias, connect=False)
    except Exception:
        pass


@contextmanager
def conn_protect():
    conned = set()
    for a in _conn_map.keys():
        try:
            get_connection(alias=a)
            conned.add(a)
        except Exception:
            pass

    print(conned)
    disconnect_all()
    yield

    for a in conned:
        conn_mongo(a)
