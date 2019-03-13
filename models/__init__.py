
# 针对新版本model写的init文件，如果需要使用旧版本，请自行修改init配置。
from .dbs import *
# from .apis import *

# __all__ = (dbs.__all__ + apis.__all__)
__all__ = (dbs.__all__)
