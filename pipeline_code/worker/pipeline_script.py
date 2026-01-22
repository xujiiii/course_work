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
    cmd = ['python3.12', fuc_location, "tmp.hhr"]
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
    base_dir = os.path.dirname(os.path.abspath(__file__))
    search_data='Data/pdb70/pdb70'
    cmd= ["sudo","docker","run","--rm",
          "-v",f"{base_dir}:/app",
          "-v", f"{location}:/output", 
          "-w", "/app",
          "soedinglab/hh-suite:latest",
          "hhsearch", "-i", f"/output/tmp.a3m", "-cpu", "1", "-d", f"/app/{search_data}", 
          "-o", f"/output/tmp.hhr"
          ] 
    logger.info(f'STEP 5: RUNNING HHSEARCH: {" ".join(cmd)}')
    p = Popen(cmd, stdin=PIPE,stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    if os.path.exists(os.path.join(location,"tmp.hhr")):
        logger.info(f'HHSEARCH completed and hhr file generated')
        return location
    else:
        logger.error(f'HHSEARCH failed to generate hhr file')
        raise FileNotFoundError(f'tmp.hhr not found,error in run_hhsearch function')


def read_horiz(location):
    """Parse horiz file and concatenate the information to a new tmp a3m file
    
    Derived /tmp/pipeline/'sp|Q80US4|ARP5_MOUSE'/tmp.a3m from /tmp/pipeline/'sp|Q80US4|ARP5_MOUSE'/tmp.horiz 
    
    Args:
        location: It is the locatoion for one protein to run the whole pipeline and store the middle output during running.
            For example, /tmp/pipeline/'sp|Q80US4|ARP5_MOUSE'.
    
    Returns:
        location: The same as arg location.    
    """
    pred = ''
    conf = ''
    tmp=os.path.join(location,"tmp.fas")
    horiz=os.path.join(location,"tmp.horiz")
    a3m=os.path.join(location,"tmp.a3m")
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
    input_file=os.path.join(location,"tmp.fas")
    out_file=os.path.join(location,"tmp.horiz")
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
    
    #Select sequence from posgresql and store it as fasta file.
    try:
        query = "SELECT description, sequence FROM fasta_records WHERE seq_id = %s"
        cur.execute(query, (fasta_id,))
        row = cur.fetchone()
        if row:
            description, sequence = row
            fasta_content = f">{description}\n{sequence}\n"
            logger.info(fasta_content)
            with open(f"/tmp/pipeline/{fasta_id}/tmp.fasta", "w") as f:
                f.write(fasta_content)
    except Exception as e:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()
        logger.error(f"Can't find {fasta_id} on the database")
        raise FileNotFoundError(f"{workername}: can't find sequence with ID as {fasta_id}")
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

    By @shared_task(...), the celery work has the following features 
    1.Each workflow task will retry twice if errors appear
    2.If worker dead, the tasks running in the worker will be assigned to other workers later
    3.Even the celery in a worker accepts kill signals from user, the tasks running in the worker will still be assigned to others later.
     
    Args:
        self: An instance of celery.app.task.Task set by @shared_task(bind=True...)
        fasta_id: The fasta id of the protein to run the whole pipeline
        output_location: The same as the output name user run apply.py, 
            which is the folder under /tmp/pipeline_output to store results.
    """
    hostname = socket.gethostname()
    folder_location = create_folder(fasta_id,output_location)
    fasta_location = derive_fasta_from_db(folder_location) 
    input_location = read_input(fasta_location)
    s4pred_location = run_s4pred(input_location)
    horiz_location = read_horiz(s4pred_location)
    hhsearch_location = run_hhsearch(horiz_location)
    final_result = run_parser(hhsearch_location,output_location,fasta_id)
    logger.info(f"[{hostname}] FASTA ID {fasta_id}: Analysis finished")
    return f"[{hostname}] FASTA ID {fasta_id}: Analysis finished"
    
def clear_folder_contents(folder_path):
    """Delete all files under a folder
    
    Args:
        folder_path: The folder that you want to delete all files under it.
    """
    path = Path(folder_path)
    if not path.exists():
        logger.info(f"{folder_path} does not exist,no cleaning needed")
        return

    logger.info(f"Start cleaning: {folder_path}")
    for item in path.iterdir():
        try:
            if item.is_file() or item.is_symlink():
                item.unlink()  
                logger.info(f"Deleted file: {item.name}")
            elif item.is_dir():
                shutil.rmtree(item)  
                logger.info(f"Deleted folder: {item.name}")
        except Exception as e:
            logger.error(f"Errors in {item}: {e}")

@worker_init.connect
def bootstrap_worker(sender, **kwargs):
    """This function will run every time every time worker restart
    
    Args:
        sender: It is the instance of Worker set by @@worker_init.connect
    """
    logger.info(f"Worker started:Worker {sender} is running")
    clear_folder_contents("/tmp/pipeline")
    logger.info(f"Worker {sender} is ready, work folder is clean")

@shared_task(bind=True)
def clean_pipeline_output(self):
    """Clean all files under /tmp/pipeline_output/ 
    
    Args:
        self: An instance of celery.app.task.Task set by @shared_task(bind=True...)
    """
    folder_path="/tmp/pipeline_output"
    if not os.path.exists(folder_path):
        logger.info(f"folder {folder_path} does not exist, no cleaning needed")
        return f"{folder_path} do not exists, don't need clean"

    #Delete files and folders
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)  
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)  
            logger.info(f"Cleaned: {file_path}")
        except Exception as e:
            return f"Cleaning {file_path} fail, reasons: {e}"
    return "finish cleaning"

@shared_task(bind=True)
def get_results(self,name):
    """Get the result and return to host machine
    
    For example, it calculate the results under /tmp/pipeline_output/'sp|Q80US4|ARP5_MOUSE'
    and return the results to host machine.
    
    Args:
        name: The output name user used to represent the tasks, the same as the name used to run apply.py
    
    Returns:
        For example, it returns a dictionary containing the output in /tmp/pipeline_output/'sp|Q80US4|ARP5_MOUSE'
    """
    hostname = socket.gethostname()
    local_path = f"/tmp/pipeline_output/{name}"
    search_path = os.path.join(local_path, "*.out")
    all_files = glob.glob(search_path)
    if not all_files:
        logger.error(f"Not any .out file in {local_path} ")
        return
    df_list = [pd.read_csv(f) for f in all_files]
    combined_df = pd.concat(df_list, ignore_index=True)
    combined_df.to_csv(os.path.join(local_path, "output.csv"), index=False)
    output = f"{local_path}/output.csv"
    df_out = pd.read_csv(output)
    return {
        "worker": hostname,
        "output": df_out[['query_id', 'best_hit','score_std','score_gmean']].to_dict('records')
        }