from benchmark.benchmark import Benchmark
from deploy.deploy import DeploySpark
import os


if __name__ == '__main__':
    print("Spark Deployment")
    d = DeploySpark()
    nodes = d.reserve_nodes(2, 10)
    print(f'Reserved nodes {nodes}')
    d.connect_to_master(nodes[0])
    print('Connected to master')
    d.connect_to_workers(nodes[1:])
    d.start_system(nodes[0], nodes[1:])
    print('Started system')

    os.system(f'ssh {nodes[0]} python ~/ddps-assignment-1/driver/data_manager.py &')
    os.system(f'ssh {nodes[0]} python ~/ddps-assignment-1/benchmark/benchmark.py &')
    
    
