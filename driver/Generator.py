import time
import numpy as np
from typing import List, Dict


class Generator:
    """ Generator class to fill the DataQueue"""
    users: List[int]
    prices: List[float]
    pack_ids: List[int]
    packs: Dict[int, float]

    def __init__(self, purchase_probability, ad_probability):
        self.users = list(range(10))
        self.prices = [1.0, 2.0, 3.0, 4.0, 5.0]
        self.packs = {i: self.prices[i] for i in range(len(self.prices))}
        self.pack_ids = [pack_id for pack_id in self.packs.keys()]
        self.generator_functions = {
            "purchase": self.generate_purchase,
            "ad": self.generate_ad
        }
        self.probability = {
            "purchase_probability": purchase_probability,
            "ad_probability": ad_probability
        }

    def generate_purchase(self):
        """ Generates a purchase instance"""
        pack_id = int(np.random.choice(self.pack_ids))
        return {
            'userID': int(np.random.choice(self.users)),
            'gemPackID': pack_id,
            'price': self.packs[pack_id],
            'time': float(time.time())
        }

    def generate_ad(self):
        """ Generates an ad instance"""
        return {
            "userID": int(np.random.choice(self.users)),
            "gemPackID": int(np.random.choice(self.pack_ids)),
            "time": float(time.time())
        }
