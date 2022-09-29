import math
from multiprocessing import Queue, Process
from generator import Generator
from sut_source import SUTSource


class DataManager:

    def __init__(self, num_events, generators_count, rate):
        self.generators_count = generators_count
        self.num_events = num_events
        self.generators = [Generator(1.0, math.ceil(rate / self.generators_count),
                                     math.ceil(num_events / self.generators_count))
                                     for i in range(generators_count)
            ]
        self.sut_sources = [SUTSource('node102', 9999, self.generators[i]) for i in range(generators_count)]

    def start(self):
        print('Starting DMS')
        for i in range(self.generators_count):
            print('starting', i)
            self.sut_sources[i].start()
            self.generators[i].start()


if __name__ == '__main__':
    print('DM')
    DataManager(100_000_000, 1, 3000).start()