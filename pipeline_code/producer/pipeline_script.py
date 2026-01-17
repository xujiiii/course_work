from celery import Celery, shared_task
app = Celery('tasks', broker='amqp://pipeline:pipeline123@localhost:5672//', backend='redis://localhost:6379/0')

"""
usage: python pipeline_script.py INPUT.fasta  
approx 5min per analysis
celery -A pipeline_script control revoke all terminate=True 删除任务
"""

tmp_file = "tmp.fas"
horiz_file = "tmp.horiz"
a3m_file = "tmp.a3m"  
hhr_file = "tmp.hhr" 
   
@shared_task(bind=True,acks_late=True)
def reduce_worker(self,msg,output_file):
    pass

@shared_task(bind=True)  
def clean_pipeline_output(self):
    pass


@shared_task(bind=True, acks_late=True, autoretry_for=(Exception,), max_retries=1, default_retry_delay=10)
def workflow(self,fasta_id,output_location):
    pass

@shared_task(bind=True,acks_late=True)
def get_results(self,msg,name):
    pass