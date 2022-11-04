from benchmark.benchmark import Benchmark
from deploy.deploy import DeployFlink
import time
import argparse
import os

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--num-nodes', type=int)
    parser.add_argument('--rate', type=int)
    parser.add_argument('--num-events', type=int)
    parser.add_argument('--results-path', type=str)

    return parser.parse_args()


def node_nade_to_infiniband(node_name):
    return node_name
    base_ip = '10.149.1.'
    return base_ip + str(int(node_name[5:]))


if __name__ == '__main__':
    os.system('scancel -u ddps2201')
    print("Flink Deployment")
    args = parse_args()
    os.makedirs(args.results_path, exist_ok=True)
    d = DeployFlink()
    num_generators_nodes = 32 // 8
    nodes = d.reserve_nodes(args.num_nodes + num_generators_nodes + 1, '06')
    print(f'Reserved nodes {nodes}')
    master_node = nodes[0]

    generator_nodes = [node for node in nodes[1: num_generators_nodes + 1]]
    worker_nodes = [node for node in nodes[num_generators_nodes + 1: ]]
    d.connect_to_master(master_node)
    print('Connected to master')
    d.connect_to_workers(worker_nodes)
    d.start_system(master_node, worker_nodes)
    print('Started system')
    
    i = 0
    # os.makedirs(args.results_path)
    for generator_node in generator_nodes:
        
        os.system(f'ssh {generator_node} python ~/ddps-assignment-1/driver/data_manager.py --master {generator_node}' +
                  f' --port 9999 --rate {args.rate // num_generators_nodes} --num-events {args.num_events} --save-path {args.results_path}/th_node{i} &')
        i += 1

    generators_ips = ' '.join(generator_nodes)
    os.system(f'ssh {master_node} /home/ddps2201/scratch/flink-master/build-target/bin/flink' +
              f' run -pyexec ~/scratch/flink/bin/python -py ~/ddps-assignment-1/flink/sum.py  --n-gens 8' +
              f' --generators-ips {generators_ips} --port 9999 &') # -pyexec ~/scratch/flink
    
    time.sleep(5 * 60)

    d.shutdown(master_node, worker_nodes)
    os.system(f'cp ~/scratch/flink-master/build-target/log/*.out* {args.results_path}')
    os.system(f'rm ~/scratch/flink-master/build-target/log/*')
    
    
