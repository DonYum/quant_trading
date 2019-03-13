import logging
import cv2
from pathlib import Path
from PIL import Image as PILImage
# from http_service.config import global_config

logger = logging.getLogger(__name__)


# ceph S3配置
class Struct:
    def __init__(self, **entries):
        self.__dict__.update(entries)

_settings = dict(
            CEPH_URL_BASE='http://10.60.242.105:5566/static/ceph/',
            CEPH_BK_STORAGE_DIR="/data/ceph_bk/"
        )
_settings = Struct(**_settings)
# _settings = global_config()


def _gen_tmp_dir(f_name):
    # return f_name[:2]
    return '/'.join([f_name[:2], f_name[2:4], f_name[4:6], f_name[6:8]])


# Ceph对象存储的备份方案。
class CephS3(object):
    def __init__(self, base_dir=''):
        self.url_base = _settings.CEPH_URL_BASE
        if base_dir:
            self.base_dir = Path(base_dir)
        else:
            self.base_dir = Path(_settings.CEPH_BK_STORAGE_DIR)

    def generate_url(self, fn):
        return self.url_base + fn

    # 上传
    # fn：md5
    def upload(self, fn, content):
        f_dir = self.base_dir / _gen_tmp_dir(fn)
        f_dir.mkdir(parents=True, exist_ok=True, mode=0o777)
        f_name = f_dir / fn
        with f_name.open('wb') as fd:
            fd.write(content)
        f_name.chmod(0o666)
        return self.generate_url(fn)

    # 下载
    # fn：md5
    def read(self, fn):
        content = None
        f_name = self.base_dir / _gen_tmp_dir(fn) / fn
        try:
            with f_name.open('rb') as fd:
                content = fd.read()
        except Exception:
            logger.error(f"read file fail.", exc_info=True)
        return content

    # 读取cv2格式
    # fn：md5
    def imread(self, fn):
        content = None
        f_name = self.base_dir / _gen_tmp_dir(fn) / fn
        try:
            content = cv2.imread(str(f_name))
        except:
            pass
        return content

    # 判断
    # fn：md5
    def exists(self, fn):
        content = None
        f_name = self.base_dir / _gen_tmp_dir(fn) / fn
        return f_name.exists()

    # 删除key
    def delete(self, fn):
        f_name = self.base_dir / _gen_tmp_dir(fn) / fn
        try:
            f_name.unlink()
        except:
            pass

    # 获取PIL的open句柄
    def get_pil_fp(self, fn):
        f_name = self.base_dir / _gen_tmp_dir(fn) / fn
        try:
            fp = PILImage.open(str(f_name))
        except:
            pass
        return fp

ceph_s3 = CephS3()
