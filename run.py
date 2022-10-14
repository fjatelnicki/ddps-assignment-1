from benchmark.benchmark import Benchmark
from deploy.deploy import DeploySpark
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
    return base_ip + node_name[5:]


if __name__ == '__main__':
    print("Spark Deployment")

    args = parse_args()

    d = DeploySpark()
    num_generators_nodes = 32 // 8
    nodes = d.reserve_nodes(args.num_nodes + num_generators_nodes + 1, 10)
    print(f'Reserved nodes {nodes}')
    master_node = node_nade_to_infiniband(nodes[0])

    generator_nodes = [node_nade_to_infiniband(node) for node in nodes[1: num_generators_nodes + 1]]
    worker_nodes = [node_nade_to_infiniband(node) for node in nodes[num_generators_nodes + 1: ]]
    d.connect_to_master(master_node)
    print('Connected to master')
    d.connect_to_workers(worker_nodes)
    d.start_system(master_node, worker_nodes)
    print('Started system')

    for generator_node in generator_nodes:
        os.system(f'ssh {generator_node} python ~/ddps-assignment-1/driver/data_manager.py --master {generator_node}' +
                  f' --port 9999 --rate {args.rate // num_generators_nodes} --num-events {args.num_events} &')

    generators_ips = ' '.join(generator_nodes)
    os.system(f'ssh {nodes[0]} python ~/ddps-assignment-1/benchmark/benchmark.py --master {master_node} --n-gens 16' +
              f' --generators-ips {generators_ips} --port 9999 --results-path {args.results_path} &')
    
    
