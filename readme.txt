1.worker单独重启命令需要设置 ansible  ok --limit
4.之前出现数据库连接失败，原因未知  
6.完善celery自动重启
7.完善hosts的rabbit，redis，celery，prometheus，flower重启
8.完善posgresql重启
9.写个将authority-key加入全部机器的ansible

test：
均用小于40的测试集进行
0.暂未出现重复运行一个id或结果返回重复
1.单个掉点并重连，会有avgload波动，结果ok
2.全部掉点重连，负载超过4，结果ok    warning   
3.tmux运行 不退出vscode，结果ok
4.tmux运行，推出vscode,有worker掉线(只有一个)，结果在不重新worker登录时正确  16个测试集
5.第二次测试是否会掉线，二次测试没掉线，只要不在ssh连接worker，单纯在hosts上操作就不会掉线，maybe，
 prometheus和flower都在线，数据结果有微小区别

1.
sudo dnf install python3-pip
python3 -m pip install --user ansible

copy your ssh key to hosts
chmod 600 comp

2. sudo dnf install git

3. go to ansible_use/inventory.yaml to write your worker address 

4. go to ansible_use/roles/install_prometheus/tasks/config  to add the ip of your workers

5.got to /pipeline_code/worker/pipeline_script.py to change the hosts broker bd backend to your hosts ip

5.run ansible-playbook -i inventory.yaml full.yaml

6.go to pipeline_code/producer,please run the command in this folder
  1.python3.12 apply <the address of your experiments ids file.txt> <give a name of your output>
  2.python3.12 get_results.py <the name> to get the output
  3.python3.12 clean_output.py, to delete all output file in workers
  tips:each name can only be used one times