import os
import time
import logging
from ..dbs.tick_file_doc import *

__all__ = ('process_imgs', 'parallel_process_grps', )

logger = logging.getLogger(__name__)


# 批量处理图片的并发方案
def process_imgs(cat_id, q_filter, process_img_func, pool_size=0, init_func=None, init_task_func=None, batch_func=None, spawn=False, time_low_th=60):
    if init_func:
        # logger.info(f'exec `init_func`.')
        init_func(q_filter)

    import multiprocessing as mp
    if spawn:
        mp.set_start_method('spawn')              # 解决因为并发引起死锁的问题

    # 动态调整pool_size大小
    if pool_size == 0:
        pool_size = mp.cpu_count() - 1

    doc = get_dyn_ticks_doc(cat_id)
    total = doc.objects(**q_filter).count()
    if total == 0:
        logger.info(f'There is no imgs to process.')
        return

    if total < 500 * pool_size:
        pool_size = pool_size // 500 or 1

    # 设置参数
    pages = pool_size
    page_size = total // pages + 1
    params = []
    for p in range(pages):
        params.append(
            {
                'q_filter': q_filter,
                'process_img_func': process_img_func,
                'page': p,
                'page_size': page_size,
                'time_low_th': time_low_th,
                'init_task_func': init_task_func,
                'batch_func': batch_func,
            }
        )
    logger.info(f'q_filter={q_filter}, pool_size={pool_size}, imgs cnt={total}, pages={pages}, page_size={page_size}.')

    # 启动多进程
    st = time.time()
    thread_pool = mp.Pool(pool_size)
    thread_pool.map(_process_imgs_task, params)
    # for x in thread_pool.imap(_process_imgs_task, params):
    #     logger.info(f'[{x}]: Finished!. Time={"%.1fs" % (time.time() - st)}.')

    thread_pool.close()
    thread_pool.join()

    logger.info(f'[Finished]: {total}: Time={"%.1fs" % (time.time() - st)}.')


def _process_imgs_task(params):
    process_imgs_task(
            q_filter=params['q_filter'],
            process_img_func=params['process_img_func'],
            page=params['page'],
            page_size=params['page_size'],
            time_low_th=params['time_low_th'],
            init_task_func=params['init_task_func'],
            batch_func=params['batch_func']
        )


# 批量处理图片
# 说明：page_size==0的情况下处理所有图片
def process_imgs_task(cat_id, q_filter, process_img_func, page=0, page_size=0, time_low_th=60, init_task_func=None, batch_func=None):
    batch = 1000
    time_low_th = time_low_th
    time_high_th = 200

    if not process_img_func:
        logger.error(f'[{page}]: `process_img_func` is invalid.')

    if init_task_func:
        # logger.info(f'exec `init_task_func`.')
        init_task_func(q_filter, page)

    doc = get_dyn_ticks_doc(cat_id)
    imgs = doc.objects(**q_filter).timeout(False)
    if page_size:
        imgs = imgs.skip(page * page_size).limit(page_size)
    total = imgs.count(with_limit_and_skip=True)
    logger.info(f'[{page}]: q_filter={q_filter}, imgs cnt={total}.')

    cnt = 0
    starttime = st = time.time()
    for img in imgs:
        cnt += 1

        # 处理单张图片
        process_img_func(img, page)

        if cnt % batch == 0:
            if batch_func:
                # logger.info(f'exec `batch_func`.')
                batch_func(q_filter, page)

            time_per_1K = 1000 * ((time.time() - starttime) / cnt)
            exec_time = time.time() - st
            logger.info(f'[{page}]: {cnt}/{total}: Time_per_1K={round(time_per_1K, 1)}, batch_time={round(exec_time, 1)}, batch={batch}.')

            # batch打印间隔保持在`time_low_th`和`time_high_th`之间。
            if exec_time < time_low_th:
                batch *= 2
            if exec_time > time_high_th:
                batch //= 2

            st = time.time()

    logger.info(f'[{page}]: Finished! total={total}. Time={"%.0fs" % (time.time() - starttime)}.')

    return page             # 用于显示


# 并发处理
def parallel_process_grps(grp_ids, process_func, pool_size=0, init_func=None, spawn=False, use_thread=False):
    if init_func:
        # logger.info(f'exec `init_func`.')
        init_func(grp_ids)

    import multiprocessing as mp
    if use_thread:
        from multiprocessing.dummy import Pool
    else:
        from multiprocessing import Pool
        if spawn:
            mp.set_start_method('spawn')              # 解决因为并发引起死锁的问题

    # 动态调整pool_size大小
    if pool_size == 0:
        pool_size = mp.cpu_count() - 1

    total = len(grp_ids)
    if total == 0:
        logger.info(f'There is no doc to process.')
        return

    if total < pool_size:
        pool_size = total

    logger.info(f'len(grp_ids)={total}, pool_size={pool_size}.')

    # 启动多进程
    st = time.time()
    thread_pool = Pool(pool_size)
    thread_pool.map(process_func, grp_ids)
    # for x in thread_pool.imap(_process_imgs_task, params):
    #     logger.info(f'[{x}]: Finished!. Time={"%.1fs" % (time.time() - st)}.')

    thread_pool.close()
    thread_pool.join()

    logger.info(f'[Finished]: Time={"%.1fs" % (time.time() - st)}.')
