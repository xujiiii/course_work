import sys
from subprocess import Popen, PIPE
from Bio import SeqIO
import shutil
import os.path

"""
usage: python pipeline_script.py INPUT.fasta  
approx 5min per analysis
"""

def run_parser(hhr_file):
    """
    Run the results_parser.py over the hhr file to produce the output summary
    """
    cmd = ['python3.12', './results_parser.py', hhr_file]
    print(f'STEP 4: RUNNING PARSER: {" ".join(cmd)}')
    p = Popen(cmd, stdin=PIPE,stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    print(out.decode("utf-8"))

def run_hhsearch(a3m_file):
    """
    Run HHSearch to produce the hhr file
    """
    #hhsearch_location='/data/student/miniforge3/envs/test_xu/bin/hhsearch'
    
    search_data='Data/pdb70/pdb70'
   # cmd = [hhsearch_location,
   #        '-i', a3m_file, '-cpu', '1', '-d', 
  #         search_data]
    
    cmd= ["sudo","docker","run","--rm",
          "-v","/home/almalinux:/app",
          "-w", "/app",
          "soedinglab/hh-suite:latest",
          "hhsearch", "-i", a3m_file, "-cpu", "1", "-d", f"/app/{search_data}", 
          "-o", "tmp.hhr"
          ] 
    print(f'STEP 3: RUNNING HHSEARCH: {" ".join(cmd)}')
    p = Popen(cmd, stdin=PIPE,stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()

def read_horiz(tmp_file, horiz_file, a3m_file):
    """
    Parse horiz file and concatenate the information to a new tmp a3m file
    """
    pred = ''
    conf = ''
    print("STEP 2: REWRITING INPUT FILE TO A3M")
    with open(horiz_file) as fh_in:
        for line in fh_in:
            if line.startswith('Conf: '):
                conf += line[6:].rstrip()
            if line.startswith('Pred: '):
                pred += line[6:].rstrip()
    with open(tmp_file) as fh_in:
        contents = fh_in.read()
    with open(a3m_file, "w") as fh_out:
        fh_out.write(f">ss_pred\n{pred}\n>ss_conf\n{conf}\n")
        fh_out.write(contents)

def run_s4pred(input_file, out_file):
    """
    Runs the s4pred secondary structure predictor to produce the horiz file
    """
    model_location='/home/almalinux/s4pred/Applications/s4pred/run_model.py'
    if os.path.exists(model_location):
        print(f'location for s4pred exists')
    else:
        print(f'no s4pred model exists')
        raise FileNotFoundError
    
    cmd = ['python3.12', model_location,
           '-t', 'horiz', '-T', '1', input_file]
    print(f'STEP 1: RUNNING S4PRED: {" ".join(cmd)}')
    p = Popen(cmd, stdin=PIPE,stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    with open(out_file, "w") as fh_out:
        fh_out.write(out.decode("utf-8"))

    
def read_input(file):
    """
    Function reads a fasta formatted file of protein sequences
    """
    print("READING FASTA FILES")
    sequences = {}
    ids = []
    for record in SeqIO.parse(file, "fasta"):
        sequences[record.id] = record.seq
        ids.append(record.id)
    return(sequences)


if __name__ == "__main__":
    
    sequences = read_input(sys.argv[1])
    tmp_file = "tmp.fas"
    horiz_file = "tmp.horiz"
    a3m_file = "tmp.a3m"
    hhr_file = "tmp.hhr"
    for k, v in sequences.items():
        print(f'Now analysing input: {k}')
        with open(tmp_file, "w") as fh_out:
            fh_out.write(f">{k}\n")
            fh_out.write(f"{v}\n")
        run_s4pred(tmp_file, horiz_file)
        read_horiz(tmp_file, horiz_file, a3m_file)
        run_hhsearch(a3m_file)
        run_parser(hhr_file)
        shutil.move("hhr_parse.out", f'{k}_parse.out')