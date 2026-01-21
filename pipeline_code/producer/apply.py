from pipeline_script import workflow
from celery import chain
import sys
import os
from celery import chord,Celery
import csv
app = Celery('tasks', broker='amqp://pipeline:pipeline123@localhost:5672//', backend='redis://localhost:6379/0')

def check():
    """Create output_name.csv if it doesn't exist.
    
    Each output name can only be used once,
    ouput_name.csv stores the output names that have been used.
    This function ensure the output_name.csv exists.
    """
    filename="output_name.csv"
    if not os.path.exists(filename): 
        with open(filename, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["names"])
        print(f"File '{filename}' is created successfully.")
    else:
        print(f"File '{filename}' already exists")
       
def check_output_name(name): 
    '''Check if output name was used
    
    if output name is used, it will raise instruction message and return True
    otherwisse it will return False.
    
    Args:
        name: The output name user used to represent the tasks
        
    Return:
        True : if name was used before.
        False: if name wasn't used before.
    '''
    filename="output_name.csv"
    col_name="names"
    value=str(name)
    exists = False
    with open(filename, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row[col_name] == str(value):
                exists = True
                print(f"Output name:{value} exists in {filename} ,please change the output name, or use clean_output.py")
                return True

    with open(filename, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([value])
    print(f"Done: {value} is valid output name")
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
        res = [workflow.s(line.strip(),output_table).set(queue='tasks') 
               for line in file if line.strip()]
        for task in res:
            task.apply_async()
            
    print(f"output table name is {output_table}, please use grafana/flower to check the progress of running")

