from pipeline_script import clean_pipeline_output
import sys
import os
from celery import Celery
from kombu.common import Broadcast
app = Celery('tasks', broker='amqp://pipeline:pipeline123@localhost:5672//', backend='redis://localhost:6379/0')
app.conf.task_queues = (
    Broadcast('map_broadcast'),
)

def clean():
    """assign clean_pipeline_ouput tasks to every online worker.
    """
    inspect = app.control.inspect()
    nodes = inspect.active_queues()
    worker_names = sorted(list(set(nodes.keys())))
    tasks = [
        clean_pipeline_output.apply_async( 
            queue=worker
        ) for worker in worker_names
    ]
    res_list = [t.get(timeout=30) for t in tasks]
    print(res_list)
    
if __name__ == "__main__":
    try:
        os.remove("output_name.csv")
    except:
        pass
    print(f"output_name.csv has been removed")
    clean()