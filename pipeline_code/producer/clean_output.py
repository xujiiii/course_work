'''
it will clean all files in /tmp/pipeline_output/
'''

from pipeline_script import clean_pipeline_output
from celery import chain,group
import sys
import os
from celery import chord,Celery
from kombu.common import Broadcast
import uuid
import pandas as pd
app = Celery('tasks', broker='amqp://pipeline:pipeline123@localhost:5672//', backend='redis://localhost:6379/0')

app.conf.task_queues = (
    Broadcast('map_broadcast'), # 定义广播队列
)


def clean():
    #ress=get_results.apply_async(args=[None,name], queue='map_broadcast')
    inspect = app.control.inspect()
    nodes = inspect.active_queues()
    worker_names = sorted(list(set(nodes.keys())))
    # 关键：手动建立 Group，但通过 destination 锁定每台机器
    # 这样 worker-02 绝对抢不到发给 worker-05 的任务
    tasks = [
        clean_pipeline_output.apply_async( 
            queue='tasks', 
            task_per_request_limit=1
        ) for worker in worker_names
    ]
    # 手动收集结果
    res_list = [t.get(timeout=30) for t in tasks]
    print(res_list)
    
if __name__ == "__main__":
    try:
        os.remove("output_name.csv")
    except:
        pass
    print(f"output_name.csv has been removed")
    clean()