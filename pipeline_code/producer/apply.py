#from pipeline_script import read_input, run_s4pred, read_horiz, run_hhsearch, run_parser, derive_fasta_from_db, create_folder
from pipeline_script import workflow,reduce_worker
from celery import chain
import sys
import os
from celery import chord
# python3.12 ./apply.py /home/almalinux/course/course_work/test_id.txt hiii

#def apply(clean_line):
 #   workflow.apply_async(args=[clean_line], queue='tasks')

'''
1.创建前chain的第一个函数将nfs里面的储存文件夹创建好，
2.每个worker运行pipeline并把{fastaid}.out写进nfs里面
3.最后由一个worker计算出最终的output table返回给hosts
'''

if __name__ == "__main__":
    fasta_ids_location=sys.argv[1]
    output_table=sys.argv[2]
    if os.path.exists(fasta_ids_location)==False:
        print(f"File {fasta_ids_location} does not exist.")
        sys.exit(1)
    with open(fasta_ids_location, 'r', encoding='utf-8') as file:
        res = [workflow.s(line.strip(),output_table).set(queue='tasks') for line in file if line.strip()]
        reduce= reduce_worker.s(output_table).set(queue='map_broadcast')
        chord(res)(reduce) 
    print(f"output table name is {output_table}")
