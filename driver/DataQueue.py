import math
from multiprocessing import Queue, Process


class DataQueue:
    budget: int
    generators_count: int
    queue: Queue
    generators: []

    def __init__(self, generator, budget, generators_count, rate):
        self.generators_count = generators_count
        self.budget = budget
        self.generators = [
            Process(target=generator.generate,
                    args=(self.queue, math.ceil(rate / self.generators_count)),
                    daemon=True) for _ in generators_count]

    def start(self):
        """Starts generators"""
        for generator in self.generators:
            generator.start()
