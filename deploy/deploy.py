import paramiko.client
from abc import abstractmethod, ABC
import os
import subprocess


class Deploy(ABC):
    def __init__(self):
        self.workers_file_path = None
        self.executable_file_path = None
        self.stop_file_path = None
        self.client = None

    def reserve_nodes(self, num_nodes, time):
        subprocess.check_output(f'preserve -# {num_nodes} -t 00:{time}:00', shell=True)
        status = subprocess.check_output("preserve -llist | grep ddps2201", shell=True).decode("utf-8").split()
        while status[6] != 'R':
            time.wait(1)

        node_list = status[8:]
        return node_list

    def update_workers(self, workers):
        with open(self.workers_file_path, 'w') as f:
            f.writelines(workers)

    def connect_to_master(self, master):
        self.client = paramiko.client.SSHClient(self.master)

    def start_system(self):
        self.client.exec_command(f'{self.executable_file_path}')

    def stop_system(self):
        self.client.exec_command(f'{self.stop_file_path}')
        self.client.close()


class DeploySpark(Deploy):
    def __init__(self):
        super().__init__()
