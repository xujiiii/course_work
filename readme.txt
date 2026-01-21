1.worker单独重启命令需要设置 ansible  ok --limit
9.写个将authority-key加入全部机器的ansible

#celery -A pipeline_script worker   -Q tasks,map_broadcast  --loglevel=info   --concurrency=1   --prefetch-multiplier=1
#--concurrency=2,让一个worker同时处理两个chain，--prefetch-multiplier=1，防止worker预取过多任务，一个worker最多取一个


1.
sudo dnf install python3-pip
python3 -m pip install --user ansible
copy your ssh key to hosts
chmod 600 comp

2. sudo dnf install git
   git clone <>
   
3. go to ansible_use/inventory.yaml to write your worker address 

4.got to /pipeline_code/worker/pipeline_script.py to change the hosts broker bd backend to your hosts ip

5.run ansible-playbook -i inventory.yaml full.yaml

6.go to pipeline_code/producer,please run the command in this folder
  1.python3.12 apply <the address of your experiments ids file.txt> <give a name of your output>
  2.python3.12 get_results.py <the name> to get the output
  3.python3.12 clean_output.py, to delete all output file in workers
  tips:each name can only be used one times