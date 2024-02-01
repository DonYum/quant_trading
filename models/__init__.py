
# 针对新版本model写的init文件，如果需要使用旧版本，请自行修改init配置。
from .dbs import *
from .apis import *
from .utils import *
# from .mongo import *

__all__ = (dbs.__all__ + apis.__all__ + utils.__all__)
# __all__ = (dbs.__all__ + mongo.__all__)
