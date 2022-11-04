import time
import os

if __name__ == '__main__':
    ths = {2: 2000_000, 4: 2000_000, 8: 2000_000}
    for nodes in [2, 4, 8]:
        for pres in [1, 0.9]:
            for rep in range(10):
                total_ths = int(ths[nodes] * pres)
                os.system(f'python run_flink.py --num-nodes {nodes} --rate {total_ths} --num-events 100000000 --results-path /var/scratch/ddps2201/results_flink_ibb2/res_sum_{nodes}_{pres}_{rep}/results')
                os.system('scancel -u ddps2201')
