import math
import argparse
from generator import Generator
from sut_source import SUTSource


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--num-events', type=int)
    parser.add_argument('--master', type=str)
    parser.add_argument('--port', type=int)
    parser.add_argument('--rate', type=int)
    parser.add_argument('--save-path', type=str)

    return parser.parse_args()

class DataManager:

    def __init__(self, master, port, num_events, generators_count, rate, save_path):
        self.generators_count = generators_count
        self.num_events = num_events
        self.generators = [Generator(1.0, math.ceil(rate / self.generators_count),
                                     math.ceil(num_events))
                                     for i in range(generators_count)
            ]
        self.sut_sources = [SUTSource(master, port - i, self.generators[i], save_path + f'_{i}.npy') for i in range(generators_count)]

    def start(self):
        print('Starting DMS')
        for i in range(self.generators_count):
            print('starting', i)
            self.sut_sources[i].start()
            self.generators[i].start()


if __name__ == '__main__':
    print('DM')
    args = parse_args()

    DataManager(master=args.master, port=args.port, num_events=args.num_events, generators_count=8, 
                rate=args.rate, save_path=args.save_path).start()
