import glob
import os
import json

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def analye(in_files, out_file):
    parts = glob.glob(in_files)
    files = []
    for part in parts:
        sub_parts = glob.glob(os.path.join(part, 'part-*'))
        for sub in sub_parts:
            files.append(sub)

    files = np.array(files).flatten()
    data = []
    for file in files:
        with open(file, 'r') as f:
            lines = f.readlines()
            for line in lines:
                parts = line.split()
                data.append(parts)
    df = pd.DataFrame(data, columns=['id', 'sum', 'start', 'end'])
    times = df['end'].astype(float) - df['start'].astype(float)
    plt.plot(range(len(times)), times)
    plt.savefig(out_file)

if __name__ == '__main__':
    # analye('../sum_1/*', 'th1.png')
    # analye('../sum_1_2/*', 'th2.png')
    # analye('../sum_1_3/*', 'th2.png')
    # analye('../sum_1_4/*', 'th2.png')
    analye('../test/*', 'th3.png')



