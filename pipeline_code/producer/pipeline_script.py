from celery import Celery, shared_task
app = Celery('tasks', broker='amqp://pipeline:pipeline123@10.134.12.57:5672//', backend='redis://localhost:6379/0')

"""
usage: python pipeline_script.py INPUT.fasta  
approx 5min per analysis
"""

tmp_file = "tmp.fas"
horiz_file = "tmp.horiz"
a3m_file = "tmp.a3m"
hhr_file = "tmp.hhr"

@shared_task(bind=True,acks_late=True)
def reduce_worker(self):
    pass

'''
@shared_task(bind=True)
def run_parser(self,hhr_file):
    """
    Run the results_parser.py over the hhr file to produce the output summary
    """
    pass

@shared_task(bind=True)
def run_hhsearch(self,a3m_file):
    """
    Run HHSearch to produce the hhr file
    """
    #hhsearch_location='/data/student/miniforge3/envs/test_xu/bin/hhsearch'
    
    pass

@shared_task(bind=True)
def read_horiz(self,horiz_file, tmp_file,a3m_file):
    """
    Parse horiz file and concatenate the information to a new tmp a3m file
    """
    pass

@shared_task(bind=True)
def run_s4pred(self,input_file, out_file):
    """
    Runs the s4pred secondary structure predictor to produce the horiz file
    """
    pass

@shared_task(bind=True)    
def read_tmp(self,sequences,tmp_file):
    pass

@shared_task(bind=True)
def read_input(self,file):
    """
    Function reads a fasta formatted file of protein sequences
    """
    pass

@shared_task(bind=True)
def derive_fasta_from_db(self,fasta_id):
    pass

@shared_task(bind=True)
def create_folder(self,location):
    """
    Create a folder to run the pipeline with given id
    """
    pass
'''
@shared_task(bind=True,acks_late=True)
def workflow(self,fasta_id):
    pass