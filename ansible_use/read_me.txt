use inventory.yaml to set 
the tar.gz url that download, 
the place to download,
the place to decode

ansible.cfg
create the folder for ansible to store temporal file

hiiiiiii

some nan includ in the results, dont know if it is bug or normal

1. pip install ansible
2. go to ansible_use/inventory.yaml to write your worker address
3. go to ansible_use/roles/istall_prometheus/tasks/config_pro.yaml
   add your grafana user name password url and set your worker node address

4. run ansible-playbook -i inventory.yaml full.yaml

5.got to /pipeline_code/producer, use python3.12 apply.py experiment_ids.txt your_output_name to give tasks to celery workers

6. go to grafana to ncheck if the tasks is finished

7. python3.12 get_results.py your_output_name  to get results.