import time
import random
from typing import List, Dict
from multiprocessing import Queue


class Generator:
    def __init__(self, purchase_probability, ad_probability):
        self.users = list(range(10))
        self.prices = [1.0, 2.0, 3.0, 4.0, 5.0]
        self.packs = {i: self.prices[i] for i in range(len(self.prices))}
        self.pack_ids = [pack_id for pack_id in self.packs.keys()]
        self.queue = Queue()
        self.generator_functions = {
            "purchase": self.generate_purchase,
            "ad": self.generate_ad
        }
        self.probability = {
            "purchase_probability": purchase_probability,
            "ad_probability": ad_probability
        }

    def generate_purchase(self):
        pack_id = int(random.choice(self.pack_ids))
        return f'{int(random.choice(self.users))}\t{pack_id}\t{self.packs[pack_id]}\t{float(time.time())}'

    def generate_ad(self):
        return f'{int(random.choice(self.users))}\t{int(random.choice(self.pack_ids))}\t{float(time.time())}'
