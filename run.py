from benchmark.benchmark import Benchmark
from deploy.deploy import DeploySpark
import argparse
import os

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--num-nodes', type=int)
    parser.add_argument('--rate', type=int)
    parser.add_argument('--num-events', type=int)

    return parser.parse_args()


if __name__ == '__main__':
    print("Spark Deployment")

    args = parse_args()

    d = DeploySpark()
    nodes = d.reserve_nodes(args.num_nodes, 10)
    print(f'Reserved nodes {nodes}')
    d.connect_to_master(nodes[0])
    print('Connected to master')
    d.connect_to_workers(nodes[1:])
    d.start_system(nodes[0], nodes[1:])
    print('Started system')

    os.system(f'ssh {nodes[0]} python ~/ddps-assignment-1/driver/data_manager.py --master {nodes[0]} --port 9999 --rate {args.rate} --num-events {args.num_events} &')
    os.system(f'ssh {nodes[0]} python ~/ddps-assignment-1/benchmark/benchmark.py --master {nodes[0]} --port 9999 &')
    
    
