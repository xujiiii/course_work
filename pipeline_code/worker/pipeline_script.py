from celery import Celery, shared_task
import numpy as np
import torch 
import sys
from subprocess import Popen, PIPE
from Bio import SeqIO
import shutil
import os.path
import psycopg2
from celery.utils.log import get_task_logger
from kombu.common import Broadcast
import csv
import os
from celery.signals import worker_init
import shutil
from pathlib import Path
app = Celery('tasks', broker='amqp://pipeline:pipeline123@10.134.12.57:5672//', backend='redis://10.134.12.57:6379/0')

app.conf.task_queues = (
    Broadcast('map_broadcast'), # 定义广播队列
)

logger = get_task_logger(__name__)
#ps aux | grep celery
#celery -A pipeline_script worker   -Q tasks,map_broadcast   --loglevel=info   --concurrency=1   --prefetch-multiplier=1
# celery -A pipeline_script worker   -Q tasks   --loglevel=info   --concurrency=2   --prefetch-multiplier=1
#--concurrency=2,让一个worker同时处理两个chain，--prefetch-multiplier=1，防止worker预取过多任务，一个worker最多取一个
# pkill -HUP -f "celery" kill celey
"""
usage: python pipeline_script.py INPUT.fasta  
approx 5min per analysis
"""

tmp_file = "tmp.fas"
horiz_file = "tmp.horiz"
a3m_file = "tmp.a3m"
hhr_file = "tmp.hhr"

@shared_task(bind=True,acks_late=True)
def reduce_worker(self,msg, base_dir="/tmp/pipeline", output_file="/tmp/pipeline/output_summary.csv"):
    """
    A simple task to reduce the number of workers
    """
    logger.info("Reducing Process is running")
    print("Map is finished, now reducing is running")
    print(msg)
    all_rows = []
    header = None

    # os.walk 会遍历所有子目录
    for root, dirs, files in os.walk(base_dir):
        if "hhr_parse.out" in files:
            file_path = os.path.join(root, "hhr_parse.out")
            
            with open(file_path, 'r', encoding='utf-8') as f:
                # 使用 csv.reader 处理 CSV 格式
                reader = csv.reader(f)
                try:
                    current_header = next(reader) # 读取第一行（表头）
                    
                    # 第一次读取时，保存表头
                    if header is None:
                        header = current_header
                    
                    # 读取数据行
                    for row in reader:
                        if row: # 确保不是空行
                            all_rows.append(row)
                except StopIteration:
                    # 如果文件是空的，跳过
                    continue

    if not all_rows:
        print("未发现有效数据。")
        return

    # 将汇总结果写入新文件
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)  # 写入统一的表头
        writer.writerows(all_rows) # 写入所有数据行

    print(f"聚合完成！汇总了 {len(all_rows)} 条记录到 {output_file}")
    return "Worker reduced by 1"


def run_parser(location):
    """
    Run the results_parser.py over the hhr file to produce the output summary
    """
    #hhr=os.path.join(location,hhr_file)
    fuc_location='/home/almalinux/results_parser.py'
    cmd = ['python3.12', fuc_location, hhr_file]
    logger.info(f'STEP 6: RUNNING PARSER: {" ".join(cmd)}')
    p = Popen(cmd, stdin=PIPE,stdout=PIPE, stderr=PIPE,cwd=location)
    out, err = p.communicate()
    logger.info(out.decode("utf-8"))
    return f"All {location} success!!!"


def run_hhsearch(location):
    """
    Run HHSearch to produce the hhr file
    """
    #hhsearch_location='/data/student/miniforge3/envs/test_xu/bin/hhsearch'
    global a3m_file
    #a3m=os.path.join(location,a3m_file)
    search_data='Data/pdb70/pdb70'
    cmd= ["sudo","docker","run","--rm",
          "-v","/home/almalinux:/app",
          "-v", f"{location}:/output", 
          "-w", "/app",
          "soedinglab/hh-suite:latest",
          "hhsearch", "-i", f"/output/{a3m_file}", "-cpu", "1", "-d", f"/app/{search_data}", 
          "-o", f"/output/tmp.hhr"
          ] 
    logger.info(f'STEP 5: RUNNING HHSEARCH: {" ".join(cmd)}')
    p = Popen(cmd, stdin=PIPE,stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    return location


def read_horiz(location):#(self,horiz_file, tmp_file,a3m_file,*args, **kwargs):
    """
    Parse horiz file and concatenate the information to a new tmp a3m file
    """
    global tmp_file, horiz_file, a3m_file
    pred = ''
    conf = ''
    tmp=os.path.join(location,tmp_file)
    horiz=os.path.join(location,horiz_file)
    a3m=os.path.join(location,a3m_file)
    logger.info("STEP 4: REWRITING INPUT FILE TO A3M")
    with open(horiz) as fh_in:
        for line in fh_in:
            if line.startswith('Conf: '):
                conf += line[6:].rstrip()
            if line.startswith('Pred: '):
                pred += line[6:].rstrip()
    with open(tmp) as fh_in:
        contents = fh_in.read()
    with open(a3m, "w") as fh_out:
        fh_out.write(f">ss_pred\n{pred}\n>ss_conf\n{conf}\n")
        fh_out.write(contents)
    return location

def run_s4pred(location):
    """
    Runs the s4pred secondary structure predictor to produce the horiz file
    """
    model_location='/home/almalinux/s4pred/Applications/s4pred/run_model.py'
    if os.path.exists(model_location):
        logger.info(f'location for s4pred exists')
    else: 
        print(f'no s4pred model exists')
        raise FileNotFoundError
    input_file=os.path.join(location,tmp_file)
    out_file=os.path.join(location,horiz_file)
    cmd = ['python3.12', model_location,
           '-t', 'horiz', '-T', '1', input_file]
    logger.info(f'STEP 3: RUNNING S4PRED: {" ".join(cmd)}')
    p = Popen(cmd, stdin=PIPE,stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    with open(out_file, "w") as fh_out:
        fh_out.write(out.decode("utf-8"))
    return location

def read_input(location):
    """
    Function reads a fasta formatted file of protein sequences
    """
    logger.info("step2:READING FASTA FILES")
    file=os.path.join(location,"tmp.fas")
    sequences = {}
    ids = []
    for record in SeqIO.parse(file, "fasta"):
        sequences[record.id] = record.seq
        ids.append(record.id)
        
    for k, v in (sequences).items(): 
        logger.info(f'Now analysing input: {k}')
        with open(file, "w") as fh_out:
            fh_out.write(f">{k}\n") 
            fh_out.write(f"{v}\n")
    return location

def derive_fasta_from_db(fasta_id):
    logger.info(f"Step1:Get the fasta file {fasta_id} form posgresql")
    try:
        # 1. 连接数据库
        conn = psycopg2.connect(
            database="pipeline",
            user="postgres",
            host="127.0.0.1", # 如果是远程连接需要指定
            password="pipeline123",
        )
        cur = conn.cursor()

        # 2. 执行查询 (使用占位符 %s 防止 SQL 注入)
        query = "SELECT description, sequence FROM fasta_records WHERE seq_id = %s"
        cur.execute(query, (fasta_id,))
        
        row = cur.fetchone()

        if row:
            description, sequence = row
            # 3. 拼接成 FASTA 格式
            fasta_content = f">{description}\n{sequence}\n"
            
            # 输出到屏幕
            logger.info(fasta_content)
            
            # 写入文件
            with open(f"/tmp/pipeline/{fasta_id}/tmp.fas", "w") as f:
                f.write(fasta_content)
        else:
            print(f"未找到 ID 为 {fasta_id} 的记录")

    except Exception as e:
        print(f"发生错误: {e}")
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

    return os.path.join("/tmp/pipeline", fasta_id)

def create_folder(fasta_id):
    """
    Create a folder to run the pipeline with given id 
    """
    try:
        logger.info(f"Step0:Creating folder for pipeline run: {fasta_id}")
        location=os.path.join("/tmp/pipeline",fasta_id)
        os.makedirs(location, exist_ok=True)
    except Exception as e:
        print(f"Error creating folder: {e}")
        
    return fasta_id


@shared_task(bind=True,acks_late=True)
def workflow(self,fasta_id):
    """
    The complete pipeline workflow
    """
    folder_location = create_folder(fasta_id)
    fasta_location = derive_fasta_from_db(folder_location)
    input_location = read_input(fasta_location)
    s4pred_location = run_s4pred(input_location)
    horiz_location = read_horiz(s4pred_location)
    hhsearch_location = run_hhsearch(horiz_location)
    final_result = run_parser(hhsearch_location)
    return final_result

def clear_folder_contents(folder_path):
    """
    删除指定文件夹下的所有内容（文件和子文件夹），但保留根目录本身。
    """
    path = Path(folder_path)
    
    if not path.exists():
        print(f"提示：路径 {folder_path} 不存在，无需清理。")
        return

    print(f"开始清理文件夹: {folder_path}")
    
    # 遍历文件夹下的所有项
    for item in path.iterdir():
        try:
            if item.is_file() or item.is_symlink():
                item.unlink()  # 删除文件或符号链接
                print(f"已删除文件: {item.name}")
            elif item.is_dir():
                shutil.rmtree(item)  # 递归删除子文件夹
                print(f"已删除目录: {item.name}")
        except Exception as e:
            # 这里的异常处理非常重要，防止 Worker 因为一个文件删不掉而死循环重启
            print(f"错误：无法删除 {item}，原因: {e}")

#before celery started,it will clean the work folder /tmp/pipeline
@worker_init.connect
def bootstrap_worker(sender, **kwargs):
    """
    sender: 指向当前的 Worker 实例
    这个函数会在每个 Worker 进程启动后、开始接收任务前执行
    """
    print(f"信号触发：Worker {sender} 正在进行前置准备...")
    
    clear_folder_contents("/tmp/pipeline")
    
    print(f"Worker {sender} is ready, work folder is clean")