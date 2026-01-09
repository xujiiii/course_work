from pipeline_script import get_results#,together
from celery import chain,group
import sys
import os
from celery import chord,Celery
from kombu.common import Broadcast
import uuid
import pandas as pd
import math
import pandas as pd
app = Celery('tasks', broker='amqp://pipeline:pipeline123@10.134.12.57:5672//', backend='redis://10.134.12.57:6379/0')

app.conf.task_queues = (
    Broadcast('map_broadcast'), # 定义广播队列
)


def together(name):
    #ress=get_results.apply_async(args=[None,name], queue='map_broadcast')
    inspect = app.control.inspect()
    nodes = inspect.active_queues()
    worker_names = sorted(list(set(nodes.keys())))
    #worker_names = list(nodes.keys())
    print(f"Detected workers: {worker_names}")
    # 关键：手动建立 Group，但通过 destination 锁定每台机器
    # 这样 worker-02 绝对抢不到发给 worker-05 的任务
    tasks = [
        get_results.apply_async(
            args=[None, name], 
            queue=worker, # 强制锁定到指定worker
            priority=9
        ) for worker in worker_names
    ]
    
    # 手动收集结果
    res_list = [t.get(timeout=300) for t in tasks]
    
    if not res_list:
        print("错误：未接收到数据")
        return

    all_rows = []
    total_count = 0
    sum_w_std = 0
    sum_w_gmean = 0

    # 1. 遍历收集
    for re in res_list:
        print(f'Received from worker: {re["worker"]}')
        try:
            output=re['output']
        except KeyError:
            print("Empty output from a worker, skipping.")
            continue

        all_rows.extend(output)

    df=pd.DataFrame(all_rows)
    df= df.sort_values(by='score_gmean', ascending=False)
    df.drop_duplicates(subset=['query_id'], keep='first', inplace=True)
    df.reset_index(drop=True,inplace=True)

    avg_gmean=df['score_gmean'].mean()
    avg_std=df['score_std'].mean()
    df_means = pd.DataFrame([{
    'avg': avg_gmean,
    'std': avg_std
    }])
    df_means.to_csv(f"{name}_summary.csv",index=False)

    df=df[["query_id","best_hit"]]
    df.rename(columns={"query_id": "fasta_id", "best_hit": "hit_id"}, inplace=True)
    df.to_csv(f"{name}_hits.csv",index=False)
    
if __name__ == "__main__":
    name=sys.argv[1]
    together(name)
    