import paramiko.client
from abc import abstractmethod, ABC
import os
import subprocess
import time

class Deploy(ABC):
    def __init__(self):
        self.workers_file_path = None
        self.executable_file_path = None
        self.stop_file_path = None
        self.client = None

    def reserve_nodes(self, num_nodes, reservation_time):
        subprocess.check_output(f'preserve -# {num_nodes} -t 00:{reservation_time}:00', shell=True)
        status = subprocess.check_output("preserve -llist | grep ddps2201", shell=True).decode("utf-8").split()
        while status[6] != 'R':
            time.sleep(1)
            status = subprocess.check_output("preserve -llist | grep ddps2201", shell=True).decode("utf-8").split()

        node_list = status[8:]
        return node_list

    def update_workers(self, workers):
        with open(self.workers_file_path, 'w') as f:
            f.writelines(workers)

    def connect_to_master(self, master):
        self.client = paramiko.client.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.client.connect(master)

    def start_system(self):
        self.client.exec_command(f'{self.executable_file_path}')

    def stop_system(self):
        self.client.exec_command(f'{self.stop_file_path}')
        self.client.close()


class DeploySpark(Deploy):
    def __init__(self):
        super().__init__()
        self.executable_file_path = '/var/scratch/ddps2201/spark-3.3.0-bin-hadoop3/sbin/start-all.sh'
        self.stop_file_path = '/var/scratch/ddps2201/spark-3.3.0-bin-hadoop3/sbin/stop-all.sh'
        self.workers_file_path = '/var/scratch/ddps2201/spark-3.3.0-bin-hadoop3/sbin/workers'


if __name__ == '__main__':
    print("Testing Spark Deployment")
    d = DeploySpark()
    nodes = d.reserve_nodes(2, 10)
    print(f'Reserved nodes {nodes}')
    d.update_workers(nodes[1:])
    print('Updates workers file')
    d.connect_to_master(nodes[0])
    print('Connected to master')
    d.start_system()
    print('Started system')

