from pipeline_script import get_results#,together
from celery import chain,group
import sys
import os
from celery import chord,Celery
from kombu.common import Broadcast
import uuid
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
            queue='tasks', 
            task_per_request_limit=1#,#the most important line, which makes sure no repeatition
            #options={'destination': [worker]} # 强制锁定
        ) for worker in worker_names
    ]
    
    # 手动收集结果
    res_list = [t.get(timeout=30) for t in tasks]
    for re in res_list:
        print(re)
    
    if not res_list:
        print("错误：未接收到数据")
        return

    all_rows = []
    total_count = 0
    sum_w_std = 0
    sum_w_gmean = 0

    # 1. 遍历收集
    for re in res_list:
        # 合并详细记录
        all_rows.extend(re['profile_output'])
        
        # 统计值加权计算
        stats = re['hits_output']
        c = stats['count']
        if c > 0:
            total_count += c
            sum_w_std += stats['avg_std'] * c
            sum_w_gmean += stats['avg_gmean'] * c

    # 2. 计算全局平均值
    final_std = sum_w_std / total_count if total_count > 0 else 0
    final_gmean = sum_w_gmean / total_count if total_count > 0 else 0

    # --- 表格 1: 最终平均值汇总表 ---
    summary_data = {
        'Project_Name': [name],
        'Total_Records': [total_count],
        'Final_Avg_Std': [round(final_std, 4)],
        'Final_Avg_Gmean': [round(final_gmean, 4)]
    }
    df_summary = pd.DataFrame(summary_data)
    
    # --- 表格 2: 合并后的详细记录表 ---
    df_combined = pd.DataFrame(all_rows)

    # 3. 保存结果
    summary_file = f"summary_{name}_metrics.csv"
    combined_file = f"combined_{name}_data.csv"
    
    df_summary.to_csv(summary_file, index=False)
    df_combined.to_csv(combined_file, index=False)

    # 4. 屏幕打印输出
    print("\n--- 表格 1: 全局平均值 ---")
    print(df_summary.to_string(index=False))
    
    print("\n--- 表格 2: 合并详细数据 (前5行) ---")
    print(df_combined.head().to_string(index=False))
    
    print(f"\n[完成] 指标文件: {summary_file}")
    print(f"[完成] 合并文件: {combined_file}")
    
if __name__ == "__main__":
    name=sys.argv[1]
    together(name)
    