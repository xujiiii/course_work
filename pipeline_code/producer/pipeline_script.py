from celery import Celery, shared_task
app = Celery('tasks', broker='amqp://pipeline:pipeline123@localhost:5672//', backend='redis://localhost:6379/0')

@shared_task(bind=True)  
def clean_pipeline_output(self):
    pass

@shared_task(bind=True, acks_late=True, autoretry_for=(Exception,), max_retries=1, default_retry_delay=10)
def workflow(self,fasta_id,output_location):
    pass

@shared_task(bind=True,acks_late=True)
def get_results(self,name):
    pass