from celery import Celery, shared_task

app = Celery('tasks', broker='amqp://pipeline:pipeline123@10.134.12.57:5672//', backend='rpc://')

@shared_task(bind=True)
def add(self,x, y):
    pass

#if __name__ == '__main__':
    