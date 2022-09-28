import time
import random
from typing import List, Dict
from multiprocessing import Process, Queue, Value


class Generator(Process):
    def __init__(self, purchase_probability, rate, num_events):
        super().__init__()
        self.users = list(range(10))
        self.prices = [1.0, 2.0, 3.0, 4.0, 5.0]
        self.packs = {i: self.prices[i] for i in range(len(self.prices))}
        self.pack_ids = [pack_id for pack_id in self.packs.keys()]
        self.rate = rate
        self.interval = 1 / rate
        self.num_events = num_events
        self.done = Value('done', False)
        self.queue = Queue()
        self.purchase_probability = purchase_probability

    def run(self):
        last_time = time.time()
        for _ in range(self.num_events):
            if random.random() > self.purchase_probability:
                self.queue.put(self.generate_purchase())
            else:
                self.queue.put(self.generate_ad())

            cur_time = time.time()
            if cur_time - last_time < self.interval:
                time.sleep(self.interval - (cur_time - last_time))
        self.done.value = True

    def generate_purchase(self):
        pack_id = int(random.choice(self.pack_ids))
        return f'{int(random.choice(self.users))}\t{pack_id}\t{self.packs[pack_id]}\t{float(time.time())}'

    def generate_ad(self):
        return f'{int(random.choice(self.users))}\t{int(random.choice(self.pack_ids))}\t{float(time.time())}'
