import time
import os

if __name__ == '__main__':
    ths = {2: 600_000, 4: 1_100_000, 8: 2_000_000}
    for nodes in [2, 4, 8]:
        for pres in [1, 0.9]:
            for rep in range(10):
                total_ths = int(ths[nodes] * pres)
                os.system(f'python run.py --num-nodes {nodes} --rate {total_ths} --num-events 100000000 --results-path /var/scratch/ddps2201/results_spark/res_sum_{nodes}_{pres}_{rep}/results')
                time.sleep(6.5 * 60)
                os.system('scancel -u ddps2201')
