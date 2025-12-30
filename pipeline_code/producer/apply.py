from pipeline_script import read_input, run_s4pred, read_horiz, run_hhsearch, run_parser
from celery import chain
res = chain(
    read_input.si("/home/almalinux/pipeline_example/test.fa").set(queue='tasks'),
    run_s4pred.si("tmp.fas","tmp.horiz").set(queue='tasks'),
    read_horiz.si("tmp.horiz","tmp.fas", "tmp.a3m").set(queue='tasks'),
    run_hhsearch.si("tmp.a3m").set(queue='tasks'),
    run_parser.si("tmp.hhr").set(queue='tasks')
).apply_async(queue='tasks')

print("pipeline id:", res.id)
#print("final result:", res.get()) 
import time
current_node = res
while current_node:
    print(f"Task {current_node.id} state: {current_node.state}")
    if current_node.state == 'FAILURE':
        print("Error details:", current_node.result) # 这里会打印 NameError 或其他错误
        break
    if current_node.parent:
        current_node = current_node.parent
    else:
        break