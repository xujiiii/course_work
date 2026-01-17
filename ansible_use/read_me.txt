
1. sudo dnf install ansib
1.1 sudo dnf install git
2. go to ansible_use/inventory.yaml to write your worker address and ssh key location


4. run ansible-playbook -i inventory.yaml full.yaml

5.got to /pipeline_code/producer, use python3.12 apply.py experiment_ids.txt your_output_name to give tasks to celery workers

6. go to grafana to ncheck if the tasks is finished

7. python3.12 get_results.py your_output_name  to get results.


when change grafana password, the ansibleplaybook will have error since the password is no longer admin