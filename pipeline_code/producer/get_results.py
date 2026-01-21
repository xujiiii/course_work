from pipeline_script import get_results
from celery import chain,group
import sys
import os
from celery import chord,Celery
from kombu.common import Broadcast
import uuid
import pandas as pd
import math
import pandas as pd
app = Celery('tasks', broker='amqp://pipeline:pipeline123@localhost:5672//', backend='redis://localhost:6379/0')
app.conf.task_queues = (
    Broadcast('map_broadcast'), 
)

def together(name):
    """Get results and create two results file in producer folder
    
    After receiving the output from each online worker,aggregation will be applied. 
    Two file name.hits_output.csv and name_profile_output.csv will be created under producer folder.
    
    Args:
        name: The output name user used to represent the tasks, the same as the name used to run apply.py
    """
    inspect = app.control.inspect()
    nodes = inspect.active_queues()
    worker_names = sorted(list(set(nodes.keys())))
    print(f"Detected workers: {worker_names}")
    tasks = [
        get_results.apply_async(
            args=[name], 
            queue=worker, 
            priority=9
        ) for worker in worker_names
    ]
    res_list = [t.get(timeout=300) for t in tasks]

    if not res_list:
        print("Errors: Not data received")
        return
    
    
    all_rows = []
    total_count = 0
    sum_w_std = 0
    sum_w_gmean = 0

    
    for re in res_list:
        if not re:
            print("Warning: Received empty result from a worker, skipping.")
            continue
        print(f'Received from worker: {re["worker"]}')
        try:
            output=re['output']
        except KeyError:
            print("Empty output from a worker, skipping.")
            continue

        all_rows.extend(output)
    
    df=pd.DataFrame(all_rows)
    if df.empty:
        print("No data in received results,plese check if the name of output is correct")
        sys.exit(1)
    df= df.sort_values(by='score_gmean', ascending=False)
    df.drop_duplicates(subset=['query_id'], keep='first', inplace=True)
    df.reset_index(drop=True,inplace=True)

    avg_gmean=df['score_gmean'].mean()
    avg_std=df['score_std'].mean()
    df_means = pd.DataFrame([{
    'avg_gmean': avg_gmean,
    'avg_std': avg_std
    }])
    df_means.to_csv(f"{name}_profile_ouput.csv",index=False)
    df=df[["query_id","best_hit"]]
    df.rename(columns={"query_id": "fasta_id", "best_hit": "best_hit_id"}, inplace=True)
    df.to_csv(f"{name}_hits_output.csv",index=False)
    print(f"{name}_hits_output.csv and {name}_profile_output.csv are created sucessfully")
if __name__ == "__main__":
    try:
        name=sys.argv[1]
    except IndexError as e:
        print("Errors: Please give exact one argument of name of output you want to get")
        sys.exit(1)
    together(name)
    