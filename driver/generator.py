import time
import random
import numpy as np
from multiprocessing import Process, Queue, Value, Manager
import ctypes


class Generator(Process):
    def __init__(self, purchase_probability, rate, num_events):
        super().__init__()
        self.users = list(range(100))
        self.prices = np.linspace(1, 20, 40)
        self.packs = {i: self.prices[i] for i in range(len(self.prices))}
        self.pack_ids = [pack_id for pack_id in self.packs.keys()]
        self.rate = rate

        self.interval = 1 / rate

        print(f'Rate: {self.rate}, interval {self.interval}', flush=True)
        self.num_events = num_events
        self.done = Value(ctypes.c_bool, False)
        self.queue = Queue(maxsize=100)
        self.purchase_probability = purchase_probability

    def run(self):
        start_time = time.time()
        last_time = time.time()
        sleep_interval = 3
        for i in range(self.num_events):
            self.queue.put(self.generate_purchase())

            if i % sleep_interval == 0:
                cur_time = time.time()
                # print(cur_time - last_time, self.interval * 100, flush=True)

                if cur_time - last_time < self.interval * sleep_interval:
                    # print('sleeping', flush=True)
                    time.sleep(self.interval * sleep_interval - (cur_time - last_time))
                last_time = cur_time

                if i % 30000:
                    if cur_time - start_time > 5.5 * 60:
                        break
                
        self.done.value = True

    def generate_purchase(self):
        pack_id = int(random.choice(self.pack_ids))
        return f'{int(random.choice(self.users))}\t{pack_id}\t{self.packs[pack_id]}\t{float(time.time())}'

