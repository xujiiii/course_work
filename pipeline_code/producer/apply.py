#from pipeline_script import read_input, run_s4pred, read_horiz, run_hhsearch, run_parser, derive_fasta_from_db, create_folder
from pipeline_script import workflow
from celery import chain
import sys
import os
'''
def apply(clean_line):
    res = chain(
        create_folder.s(clean_line).set(queue='tasks'),
        derive_fasta_from_db.s().set(queue='tasks'),
        read_input.s().set(queue='tasks'),
        run_s4pred.s().set(queue='tasks'),
        read_horiz.s().set(queue='tasks'),
        run_hhsearch.s().set(queue='tasks'),
        run_parser.s().set(queue='tasks')
    ).apply_async(queue='tasks')

    print("pipeline id:", res.id)
'''
#def apply(clean_line):
 #   workflow.apply_async(args=[clean_line], queue='tasks')

if __name__ == "__main__":
    fasta_ids_location=sys.argv[1]
    if os.path.exists(fasta_ids_location)==False:
        print(f"File {fasta_ids_location} does not exist.")
        sys.exit(1)
    with open(fasta_ids_location, 'r', encoding='utf-8') as file:
        for line in file:
            # 使用 strip() 去掉每一行末尾的换行符 \n
            clean_line = line.strip()
            
            # 如果不是空行，则运行函数
            if clean_line:
                res = workflow.apply_async(args=[clean_line], queue='tasks')
                print(f"已分发 ID {clean_line[0:6]} -> Task ID: {res.id}")
                
