from celery import Celery, shared_task
import numpy as np
app = Celery('tasks', broker='amqp://pipeline:pipeline123@10.134.12.57:5672//', backend='rpc://')
import torch 
import sys
from subprocess import Popen, PIPE
from Bio import SeqIO
import shutil
import os.path

"""
usage: python pipeline_script.py INPUT.fasta  
approx 5min per analysis
"""

tmp_file = "tmp.fas"
horiz_file = "tmp.horiz"
a3m_file = "tmp.a3m"
hhr_file = "tmp.hhr"

@shared_task(bind=True)
def run_parser(self,hhr_file):
    """
    Run the results_parser.py over the hhr file to produce the output summary
    """
    cmd = ['python3.12', './results_parser.py', hhr_file]
    print(f'STEP 4: RUNNING PARSER: {" ".join(cmd)}')
    p = Popen(cmd, stdin=PIPE,stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    print(out.decode("utf-8"))
    return hhr_file

@shared_task(bind=True)
def run_hhsearch(self,a3m_file):
    """
    Run HHSearch to produce the hhr file
    """
    #hhsearch_location='/data/student/miniforge3/envs/test_xu/bin/hhsearch'
    
    search_data='Data/pdb70/pdb70'
   # cmd = [hhsearch_location,
   #        '-i', a3m_file, '-cpu', '1', '-d', 
  #         search_data]
    
    cmd= ["sudo","docker","run","--rm",
          "-v","/home/almalinux:/app",
          "-w", "/app",
          "soedinglab/hh-suite:latest",
          "hhsearch", "-i", a3m_file, "-cpu", "1", "-d", f"/app/{search_data}", 
          "-o", "tmp.hhr"
          ] 
    print(f'STEP 3: RUNNING HHSEARCH: {" ".join(cmd)}')
    p = Popen(cmd, stdin=PIPE,stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    return hhr_file

@shared_task(bind=True)
def read_horiz(*args, **kwargs):#(self,horiz_file, tmp_file,a3m_file,*args, **kwargs):
    """
    Parse horiz file and concatenate the information to a new tmp a3m file
    """
    pred = ''
    conf = ''
    print("STEP 2: REWRITING INPUT FILE TO A3M")
    with open(horiz_file) as fh_in:
        for line in fh_in:
            if line.startswith('Conf: '):
                conf += line[6:].rstrip()
            if line.startswith('Pred: '):
                pred += line[6:].rstrip()
    with open(tmp_file) as fh_in:
        contents = fh_in.read()
    with open(a3m_file, "w") as fh_out:
        fh_out.write(f">ss_pred\n{pred}\n>ss_conf\n{conf}\n")
        fh_out.write(contents)
    return a3m_file

@shared_task(bind=True)
def run_s4pred(self,input_file, out_file):
    """
    Runs the s4pred secondary structure predictor to produce the horiz file
    """
    model_location='/home/almalinux/s4pred/Applications/s4pred/run_model.py'
    if os.path.exists(model_location):
        print(f'location for s4pred exists')
    else:
        print(f'no s4pred model exists')
        raise FileNotFoundError
    
    cmd = ['python3.12', model_location,
           '-t', 'horiz', '-T', '1', input_file]
    print(f'STEP 1: RUNNING S4PRED: {" ".join(cmd)}')
    p = Popen(cmd, stdin=PIPE,stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    with open(out_file, "w") as fh_out:
        fh_out.write(out.decode("utf-8"))
    return 11

@shared_task(bind=True)
def read_input(self,file):
    """
    Function reads a fasta formatted file of protein sequences
    """
    print("READING FASTA FILES")
    sequences = {}
    ids = []
    for record in SeqIO.parse(file, "fasta"):
        sequences[record.id] = record.seq
        ids.append(record.id)
        
    for k, v in (sequences).items(): 
        print(f'Now analysing input: {k}')
        with open(tmp_file, "w") as fh_out:
            fh_out.write(f">{k}\n")
            fh_out.write(f"{v}\n")
    return "tmp.fas"

@shared_task(bind=True)
def derive_fasta_from_db(self,fasta_id):
    pass

@shared_task(bind=True)
def create_folder(self,location):
    """
    Create a folder to run the pipeline with given id
    """
    pass
