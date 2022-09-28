import math
from multiprocessing import Queue, Process
from generator import Generator
from sut_source import SUTSource


class DataManager:

    def __init__(self, generator, num_events, generators_count, rate):
        self.generators_count = generators_count
        self.num_events = num_events
        self.generators = [Generator(0.5, math.ceil(rate / self.generators_count),
                                     math.ceil(num_events / self.generators_count))
            ]
        self.sut_sources = [SUTSource('localhost', 9999, self.generators[i]) for i in generators_count]

    def start(self):
        for i in range(self.generators_count):
            self.sut_sources[i].start()


