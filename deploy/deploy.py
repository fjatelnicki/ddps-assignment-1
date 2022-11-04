import paramiko.client
from abc import abstractmethod, ABC
import os
import subprocess
import time

class Deploy(ABC):
    def __init__(self):
        self.start_master_path = None
        self.start_worker_path = None
        self.executable_file_path = None
        self.stop_file_path = None
        self.master_client = None
        self.workers_clients = []

    def reserve_nodes(self, num_nodes, reservation_time):
        subprocess.check_output(f'preserve -# {num_nodes} -t 00:{reservation_time}:00', shell=True)
        status = subprocess.check_output("preserve -llist | grep ddps2201", shell=True).decode("utf-8").split()
        while status[6] != 'R':
            time.sleep(1)
            status = subprocess.check_output("preserve -llist | grep ddps2201", shell=True).decode("utf-8").split()

        node_list = status[8:]
        return node_list

    def connect_to_master(self, master):
        self.master_client = paramiko.client.SSHClient()
        self.master_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.master_client.connect(master)

    def connect_to_workers(self, workers):
        for worker in workers:
            client = paramiko.client.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(worker)
            self.workers_clients.append(client)

    @abstractmethod
    def start_system(self):
        pass

class DeploySpark(Deploy):
    def __init__(self):
        super().__init__()
        self.start_master_path = '/var/scratch/ddps2201/spark-3.3.0-bin-hadoop3/sbin/start-master.sh'
        self.start_worker_path = '/var/scratch/ddps2201/spark-3.3.0-bin-hadoop3/sbin/start-worker.sh'
        self.stop_file_path = '/var/scratch/ddps2201/spark-3.3.0-bin-hadoop3/sbin/stop-all.sh'
        self.workers_file_path = '/var/scratch/ddps2201/spark-3.3.0-bin-hadoop3/conf/workers'
    
    def start_system(self, master, workers):
        stdin, stdout, stderr = self.master_client.exec_command(f'{self.start_master_path} -h {master}', get_pty=True)
        for line in iter(stdout.readline, ""):
            print(line, end="")
        for i in range(len(self.workers_clients)):
            stdin, stdout, stderr = self.workers_clients[i].exec_command(f'rm -rf /tmp/spark*', get_pty=True)
            for line in iter(stdout.readline, ""):
                print(line, end="")
            stdin, stdout, stderr = self.workers_clients[i].exec_command(f'{self.start_worker_path} spark://{master}:7077 -h {workers[i]}', get_pty=True)
            for line in iter(stdout.readline, ""):
                print(line, end="")
    

class DeployFlink(Deploy):
    def __init__(self):
        super().__init__()
        self.start_cluster_path = '/home/ddps2201/scratch/flink-master/build-target/bin/start-cluster.sh'
        self.workers_file_path = '/home/ddps2201/scratch/flink-master/build-target/conf/workers'
        self.masters_file_path = '/home/ddps2201/scratch/flink-master/build-target/conf/masters'
        self.conf_file_path = '/home/ddps2201/scratch/flink-master/build-target/conf/flink-conf.yaml'
        self.out_files_path = '/home/ddps2201/scratch/flink-master/build-target/log'
    
    def connect_to_workers(self, workers):
        super().connect_to_workers(workers)
        workers_file = ''

        for worker in workers:
            workers_file += worker + '\n'

        with open(self.workers_file_path, 'w') as f:
            f.write(workers_file)
        
    def start_system(self, master, workers):
        os.system(f'ssh {master} /home/ddps2201/scratch/flink-master/build-target/bin/start-cluster.sh &')
        time.sleep(15)
    
    def shutdown(self, master, workers):
        os.system(f'ssh {master} /home/ddps2201/scratch/flink-master/build-target/bin/stop-cluster.sh &')

if __name__ == '__main__':
    print("Testing Spark Deployment")
    d = DeploySpark()
    nodes = d.reserve_nodes(2, 10)
    print(f'Reserved nodes {nodes}')
    d.connect_to_master(nodes[0])
    print('Connected to master')
    d.connect_to_workers(nodes[1:])
    d.start_system(nodes[0], nodes[1:])
    print('Started system')

