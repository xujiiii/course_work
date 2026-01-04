#from pipeline_script import read_input, run_s4pred, read_horiz, run_hhsearch, run_parser, derive_fasta_from_db, create_folder
from pipeline_script import workflow,reduce_worker
from celery import chain
import sys
import os
from celery import chord
import csv
# python3.12 ./apply.py /home/almalinux/course/course_work/test_id.txt hiii

#def apply(clean_line):
 #   workflow.apply_async(args=[clean_line], queue='tasks')

'''
1.创建前chain的第一个函数将nfs里面的储存文件夹创建好，
2.每个worker运行pipeline并把{fastaid}.out写进nfs里面
3.最后由一个worker计算出最终的output table返回给hosts
'''
def check():
    filename="output_name.csv"
    if not os.path.exists(filename): 
        # 创建文件并写入表头（可选）
        with open(filename, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["names"])
        print(f"文件 '{filename}' 不存在，已成功创建。")
    else:
        print(f"文件 '{filename}' 已存在，无需创建。")
       
def check_output_name(name): 
    filename="output_name.csv"
    col_name="names"
    value=str(name)
    exists = False
    with open(filename, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row[col_name] == str(value):
                exists = True
                print(f"信息：{value} 已存在于 {filename} 中,请更换名字，或运行clean_output.py删除全部数据")
                return True

    # 2. 如果不存在，直接追加
    with open(filename, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([value])
    print(f"Done: {value} 插入成功")
    return False
    
    

if __name__ == "__main__":
    fasta_ids_location=sys.argv[1]
    output_table=sys.argv[2]
    check()
    if check_output_name(output_table):
        sys.exit(1)

    if os.path.exists(fasta_ids_location)==False:
        print(f"File {fasta_ids_location} does not exist.")
        sys.exit(1)
        
    with open(fasta_ids_location, 'r', encoding='utf-8') as file:
        res = [workflow.s(line.strip(),output_table).set(queue='tasks') for line in file if line.strip()]
        reduce= reduce_worker.s(output_table).set(queue='map_broadcast')
        
        full=chord(res)(reduce)
        print(full.get()) 
    print(f"output table name is {output_table}")

