from pipeline_script import read_input, run_s4pred, read_horiz, run_hhsearch, run_parser, derive_fasta_from_db, create_folder
from celery import chain
res = chain(
    create_folder.s("sp|Q9D4C1|LMTD1_MOUSE").set(queue='tasks'),
    derive_fasta_from_db.s().set(queue='tasks'),
    read_input.s().set(queue='tasks'),
    run_s4pred.s().set(queue='tasks'),
    read_horiz.s().set(queue='tasks'),
    run_hhsearch.s().set(queue='tasks'),
    run_parser.s().set(queue='tasks')
).apply_async(queue='tasks')

print("pipeline id:", res.id)


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