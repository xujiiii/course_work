from celery import Celery

app = Celery('tasks', broker='amqp://pipeline:pipeline123@10.134.12.57:5672//', backend='rpc://')

@app.task
def add(x, y):
    pass

#if __name__ == '__main__':
    