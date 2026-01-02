from celery import Celery, shared_task
import numpy as np
app = Celery('tasks', broker='amqp://pipeline:pipeline123@10.134.12.57:5672//', backend='rpc://')
import torch 
import sys
from subprocess import Popen, PIPE
from Bio import SeqIO
import shutil
import os.path
import psycopg2
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)
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