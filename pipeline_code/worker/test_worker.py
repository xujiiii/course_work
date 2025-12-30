from celery import Celery, shared_task
import numpy as np
app = Celery('tasks', broker='amqp://pipeline:pipeline123@10.134.12.57:5672//', backend='rpc://')
import torch 
@shared_task(bind=True)
def add(self,x, y):
    x=np.ones(x)
    y=np.ones(y)
    print(x)
    print(y)
    return 1 