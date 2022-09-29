import math
from multiprocessing import Queue, Process
from driver.generator import Generator
from sut_source import SUTSource


class DataManager:

    def __init__(self, num_events, generators_count, rate):
        self.generators_count = generators_count
        self.num_events = num_events
        self.generators = [Generator(1.0, math.ceil(rate / self.generators_count),
                                     math.ceil(num_events / self.generators_count))
            ]
        self.sut_sources = [SUTSource('localhost', 9999, self.generators[i]) for i in generators_count]

    def start(self):
        for i in range(self.generators_count):
            self.sut_sources[i].start()


if __name__ == '__main__':
    DataManager(1000, 5, 3000).start()