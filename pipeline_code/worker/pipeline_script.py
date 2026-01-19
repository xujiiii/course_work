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
import glob
import pandas as pd
import os
from celery.signals import worker_init
import shutil
from pathlib import Path
import fcntl
import time
import pandas as pd
import socket
app = Celery('tasks', broker='amqp://pipeline:pipeline123@10.134.12.237:5672//', backend='redis://10.134.12.237:6379/0')
app.conf.task_queues = (
    Broadcast('map_broadcast'), 
)

logger = get_task_logger(__name__)

tmp_file = "tmp.fas"
horiz_file = "tmp.horiz"
a3m_file = "tmp.a3m"
hhr_file = "tmp.hhr"

@shared_task(bind=True,acks_late=True)
def reduce_worker(self,msg,output_file):
# 1. 获取所有以 .out 结尾的文件路径
    output_file=os.path.join("/tmp/pipeline_output",output_file)
    search_path = os.path.join(output_file, "*.out")
    all_files = glob.glob(search_path)
    
    if not all_files:
        logger.info("未找到任何 .out 文件")
        return

    # 2. 读取并合并
    # 使用列表推导式一次性读取所有文件
    df_list = [pd.read_csv(f) for f in all_files]
    combined_df = pd.concat(df_list, ignore_index=True)
    
    #calculate the hits results
    hits_output=combined_df[['query_id','best_hit']].copy()
    hits_output=hits_output.rename(columns={"query_id":'fasta_id','best_hit':'best_hit_id'})
    #calculate avg_mean and avg_std 
    ave_std = combined_df['score_std'].mean()
    ave_gmean = combined_df['score_gmean'].mean()
    profile_output= pd.DataFrame({
        'avg_std': [ave_std],
        'avg_gmean': [ave_gmean],
        'count': [len(combined_df)]
    })
    
    # 3. 结果汇总 1：保存总表
    combined_df.to_csv(os.path.join(output_file,"output.csv"), index=False)
    hits_output.to_csv(os.path.join(output_file,"hits_output.csv"), index=False)
    profile_output.to_csv(os.path.join(output_file,"profile_output.csv"), index=False)
    # 4. 结果汇总 2：计算平均值并打印（供 Host 查看）
    overall_avg_score = combined_df['best_score'].mean()
    print(f"聚合完成！共处理 {len(all_files)} 个文件。")
    print(f"所有结果的 best_score 平均值: {overall_avg_score:.2f}")
    return "hahahahaah"

def run_parser(location,output_location,fasta_id):
    """Run the results_parser.py over the hhr file to produce the output summary
    
    Derived hhr_parse.out from tmp.hhr under location(like /tmp/pipeline/'sp|Q80US4|ARP5_MOUSE')
    
    Args:
        location: It is the locatoion for one protein to run the whole pipeline and store the middle output during running.
            For example, /tmp/pipeline/'sp|Q80US4|ARP5_MOUSE'.
        fasta_id: The fasta id of the protein to run the whole pipeline
        output_location: The same as the output name user run apply.py, 
            which is the folder under /tmp/pipeline_output to store results.
    
    Returns:
        location: The same as arg location.    
    """

    base_dir = os.path.dirname(os.path.abspath(__file__))
    fuc_location=os.path.join(base_dir,'results_parser.py')

    cmd = ['python3.12', fuc_location, hhr_file]
    logger.info(f'STEP 6: RUNNING PARSER: {" ".join(cmd)}')
    p = Popen(cmd, stdin=PIPE,stdout=PIPE, stderr=PIPE,cwd=location)
    out, err = p.communicate()
    logger.info(out.decode("utf-8"))
    src_path=os.path.join(location,"hhr_parse.out")
    dest_path=os.path.join("/tmp/pipeline_output",output_location)
    dest_path=os.path.join(dest_path,f"{fasta_id}.out")
    shutil.copy(src_path, dest_path)
    logger.info(f"Sucessfully copy out file to output location")
    return f"All {location} success!!!"


def run_hhsearch(location):
    """Run HHSearch to produce the hhr file
    
    For example,it will use Docker to apply HHsearch with given search target 
    to derive a new tmp.hhr file from tmp.a3m under location folder(like /tmp/pipeline/'sp|Q80US4|ARP5_MOUSE')
    
    Args:
        location: It is the locatoion for one protein to run the whole pipeline and store the middle output during running.
            For example, /tmp/pipeline/'sp|Q80US4|ARP5_MOUSE'.
            
    Returns:
        location: The same as arg location.    
    """

    global a3m_file
    base_dir = os.path.dirname(os.path.abspath(__file__))
    search_data='Data/pdb70/pdb70'
    cmd= ["sudo","docker","run","--rm",
          "-v",f"{base_dir}:/app",
          "-v", f"{location}:/output", 
          "-w", "/app",
          "soedinglab/hh-suite:latest",
          "hhsearch", "-i", f"/output/{a3m_file}", "-cpu", "1", "-d", f"/app/{search_data}", 
          "-o", f"/output/tmp.hhr"
          ] 
    logger.info(f'STEP 5: RUNNING HHSEARCH: {" ".join(cmd)}')
    p = Popen(cmd, stdin=PIPE,stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    if os.path.exists(os.path.join(location,hhr_file)):
        logger.info(f'HHSEARCH completed and hhr file generated')
        return location
    else:
        logger.error(f'HHSEARCH failed to generate hhr file')
        raise FileNotFoundError(f'{hhr_file} not found,error in run_hhsearch function')


def read_horiz(location):#(self,horiz_file, tmp_file,a3m_file,*args, **kwargs):
    """Parse horiz file and concatenate the information to a new tmp a3m file
    
    Derived /tmp/pipeline/'sp|Q80US4|ARP5_MOUSE'/tmp.a3m from /tmp/pipeline/'sp|Q80US4|ARP5_MOUSE'/tmp.horiz 
    
    Args:
        location: It is the locatoion for one protein to run the whole pipeline and store the middle output during running.
            For example, /tmp/pipeline/'sp|Q80US4|ARP5_MOUSE'.
    Returns:
        location: The same as arg location.    
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
    if not os.path.exists(a3m):
        logger.error(f'A3M file not generated')
        raise FileNotFoundError(f'{a3m} not found,error in read_horiz function')
    return location

def run_s4pred(location):
    """Runs the s4pred secondary structure predictor to produce the horiz file
    
    For example, creates /tmp/pipeline/'sp|Q80US4|ARP5_MOUSE'/tmp.horiz
    from /tmp/pipeline/'sp|Q80US4|ARP5_MOUSE'/tmp.fas
    
    Args:
        location: It is the locatoion for one protein to run the whole pipeline and store the middle output during running.
            For example, /tmp/pipeline/'sp|Q80US4|ARP5_MOUSE'.
        
    Returns:
        location: The same as arg location.   
    """
    workername = socket.gethostname()
    base_dir = os.path.dirname(os.path.abspath(__file__))
    model_location = os.path.join(base_dir,'s4pred/Applications/s4pred/run_model.py')
    if os.path.exists(model_location):
        logger.info(f'location for s4pred exists')
    else: 
        logger.error(f'no s4pred model exists')
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
    
    if not os.path.exists(out_file):
        logger.error(f'S4PRED failed to generate horiz file')
        raise FileNotFoundError(f'{out_file} not found,error in run_s4pred function',
                                f"\n{workername} failed to run s4pred")
    return location

def read_input(location):
    """Function reads a fasta formatted file of protein sequences 
    
    For example, the function creates /tmp/pipeline/'sp|Q80US4|ARP5_MOUSE'/tmp.fas 
    from /tmp/pipeline/'sp|Q80US4|ARP5_MOUSE'/tmp.fasta
    
    Args:
        location: It is the locatoion for one protein to run the whole pipeline and store the middle output during running.
            For example, /tmp/pipeline/'sp|Q80US4|ARP5_MOUSE'.
        
    Returns:
        location: The same as arg location.
    """
    logger.info("step2:READING FASTA FILES")
    input_file=os.path.join(location,"tmp.fasta")
    output_file=os.path.join(location,"tmp.fas")
    sequences = {}
    ids = []
    for record in SeqIO.parse(input_file, "fasta"):
        sequences[record.id] = record.seq
        ids.append(record.id)
        
    for k, v in (sequences).items(): 
        logger.info(f'Now analysing input: {k}')
        with open(output_file, "w") as fh_out:
            fh_out.write(f">{k}\n") 
            fh_out.write(f"{v}\n")
    
    if not os.path.exists(input_file):
        logger.error(f'Fasta file not found at {input_file}')
        raise FileNotFoundError(f'{input_file} not found,error in read_input function')
    
    return location

def derive_fasta_from_db(fasta_id):
    """Select the amino acid sequence with given id in posgresql 
    
    The sequence will be selected and store as /tmp/pipeline/fasta_id/tmp.fasta
    
    Args:
        fasta_id: The id of the protein to run the whole pipeline.
    
    Returns:
        os.path.join("/tmp/pipeline", fasta_id): It is the locatoion for one protein to run the whole pipeline
            and store the middle output during running.
    """
    logger.info(f"Step1:Get the fasta file {fasta_id} form posgresql")
    workername = socket.gethostname()
    #Log in posgresql
    try:
        conn = psycopg2.connect(
            database="pipeline",
            user="postgres",
            host="127.0.0.1", 
            password="pipeline123",
        )
        cur = conn.cursor()
    except Exception as e:
        logger.error(f"{workername} failed to connect to database: {e}")
        raise e(f"{workername} failed to connect to database: {e}")
    
    try:
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
            with open(f"/tmp/pipeline/{fasta_id}/tmp.fasta", "w") as f:
                f.write(fasta_content)
    except Exception as e:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()
        logger.error(f"Can't find {fasta_id} on the database")
        raise FileNotFoundError(f"{workername} 在数据库中未找到 ID 为 {fasta_id} 的记录")

    return os.path.join("/tmp/pipeline", fasta_id)

def create_folder(fasta_id,output_location):
    """Create the folders to store all results and to run the one task with specific fasta_id
    
    Make sure the folder to store all results exists, which is tmp/pipeline_output/output_location,
    for example, tmp/pipeline_output/test_fasta
    Also create a folder tmp/pipeline/fasta_id for the task with specific protein to run the pipeline.
    for example, tmp/pipeline/'sp|Q80US4|ARP5_MOUSE'/tmp.fasta
    
    Args:
        fasta_id: The id of the protein to run the whole pipeline.
        output_location: The same as the output name user run apply.py, 
            which is the folder under /tmp/pipeline_output to store results.
    
    Returns:
        fasta_id: The same as input, which will be passed to the next function in workflow.
    """
    
    logger.info(f"Step0:Creating folder for pipeline run: {fasta_id}")
    location=os.path.join("/tmp/pipeline",fasta_id)
    os.makedirs(location, exist_ok=True)
    logger.info(f"Step0:check output folder is created: {output_location}")
    location=os.path.join("/tmp/pipeline_output",output_location)
    os.makedirs(location, exist_ok=True)

    return fasta_id


@shared_task(bind=True, acks_late=True, autoretry_for=(Exception,),task_reject_on_worker_lost=True, max_retries=2, default_retry_delay=10)
def workflow(self,fasta_id,output_location):
    """The complete pipeline workflow with failure recovery
    

    Args:
        self: An instance of celery.app.task.Task set by @shared_task(bind=True...)
        fasta_id: The fasta id of the protein to run the whole pipeline
        output_location: The same as the output name user run apply.py, 
            which is the folder under /tmp/pipeline_output to store results.
        
    Returns:

        
    
    失败时会自动重试，最多重试1次，每次重试间隔10秒
    """
    hostname = socket.gethostname()
    folder_location = create_folder(fasta_id,output_location)
    fasta_location = derive_fasta_from_db(folder_location) 
    input_location = read_input(fasta_location)
    s4pred_location = run_s4pred(input_location)
    horiz_location = read_horiz(s4pred_location)
    hhsearch_location = run_hhsearch(horiz_location)
    final_result = run_parser(hhsearch_location,output_location,fasta_id)
        
    logger.info(f"[{hostname}] FASTA ID {fasta_id} 处理完成，生成输出文件")
    return f"[{hostname}] FASTA ID {fasta_id} 处理成功"
    


def clear_folder_contents(folder_path):
    """
    Delete all files under a folder
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
    """This function will run every time every time worker restart
    sender: 指向当前的 Worker 实例
    
    这个函数会在每个 Worker 进程启动后、开始接收任务前执行
    """
    logger.info(f"信号触发：Worker {sender} 正在进行前置准备...")
    clear_folder_contents("/tmp/pipeline")
    logger.info(f"Worker {sender} is ready, work folder is clean")

@shared_task(bind=True)
def clean_pipeline_output(self):
    """
    清理 /tmp/pipeline_output/ 文件夹下的所有内容
    """
    folder_path="/tmp/pipeline_output"
    if not os.path.exists(folder_path):
        print(f"目录 {folder_path} 不存在，跳过清理。")
        return f"{folder_path} do not exists, don't need clean"

    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)  # 删除文件或软链接
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)  # 删除子目录
            print(f"已清理: {file_path}")
        except Exception as e:
            return f"清理 {file_path} 失败，原因: {e}"
    return "finish cleaning"



@shared_task(bind=True)
def get_results(self,msg,name):

    hostname = socket.gethostname()
    local_path = f"/tmp/pipeline_output/{name}"
    search_path = os.path.join(local_path, "*.out")
    all_files = glob.glob(search_path)
    if not all_files:
        print("未找到任何 .out 文件")
        return

    # 2. 读取并合并
    # 使用列表推导式一次性读取所有文件
    df_list = [pd.read_csv(f) for f in all_files]
    combined_df = pd.concat(df_list, ignore_index=True)
    combined_df.to_csv(os.path.join(local_path, "output.csv"), index=False)
    output = f"{local_path}/output.csv"
    df_out = pd.read_csv(output)

    # 返回内存对象
    return {
        "worker": hostname,
        "output": df_out[['query_id', 'best_hit','score_std','score_gmean']].to_dict('records')
        }
    
    """简短的一句话总结函数作用。

    这里可以写详细的描述。如果函数逻辑很复杂，
    可以在这里解释它的工作原理、背景或者是为什么这么写。

    Args:
        worker_name: 目标 Worker 的名称，例如 "worker1_10.134.12.241"。
        limit: 每次获取日志的最大行数。默认为 100。

    Returns:
        返回一个包含日志字符串的列表。例如：
        ["Starting...", "Running task...", "Success"]

    Raises:
        ConnectionError: 当无法连接到目标 Worker 节点时抛出。
        ValueError: 当 limit 参数小于 0 时抛出。
    """    