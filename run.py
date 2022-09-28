from benchmark.benchmark import Benchmark
from driver.data_manager import DataManager
from deploy.deploy import DeploySpark


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

    d.master_client.exec_command('python ~/dps/driver/data_manager.py')