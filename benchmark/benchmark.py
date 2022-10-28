
import time
import os
import shutil
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
import argparse


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--master', type=str)
    parser.add_argument('--port', type=int)
    parser.add_argument('--num-gens', type=int)
    parser.add_argument('--results-path', type=str)
    parser.add_argument('--n-gens', type=int)
    parser.add_argument('--generators-ips', nargs='+')

    return parser.parse_args()

def process_data(data):
    data = data.split('\t')
    print(data)
    return (data[1], (data[2], data[3]))


def aggregate_tuples(d1, d2):
    price1, time1 = d1
    price2, time2 = d2

    total_price = float(price1) + float(price2)
    max_time = max(time1,
                   time2)  # In a windowed join operation, the containing tuples’ event time is set of be the maximum event-time of their win

    return total_price, max_time


def data_to_csv(data):
    cur_time = time.time()
    pack_id, (price, prev_time) = data
    # print('Processing', flush=True)
    return f'{pack_id}\t{price}\t{prev_time}\t{cur_time}'


class Benchmark:
    def __init__(self, master, generators_ips, port, num_gens, batch_duration, results_path):
        self.master = master
        self.generators_ips = generators_ips
        self.ports = [port - i for i in range(num_gens)]
        self.batch_duration = batch_duration
        self.results_path = results_path

    def run_benchmark(self):
        # if os.path.exists('/local/ddps2201/spark'):
        #     shutil.rmtree('/local/ddps2201/spark')
        # os.makedirs('/local/ddps2201/spark')
        # conf = conf=SparkConf()
        # conf.set('spark.local.dir', '/local/ddps2201/spark')
        spark_context = SparkContext(f'spark://{self.master}:7077')
        # spark_context.setLogLevel('info')
        streaming_context = StreamingContext(spark_context, self.batch_duration)

        streams = []
        first = True
        for generator_node in self.generators_ips:
            for port in self.ports:
                if first:
                    stream = streaming_context.socketTextStream(generator_node, port)
                else:
                    stream = stream.union(streaming_context.socketTextStream(generator_node, port))
                print(f'Listening {generator_node}:{self.ports}', flush=True)
                stream = stream.map(process_data)
                stream = stream.reduceByKeyAndWindow(aggregate_tuples, invFunc=None, windowDuration=8 ,slideDuration=4)
                stream = stream.map(data_to_csv)
        stream = stream.saveAsTextFiles(self.results_path)
        # stream = streams[0]
        # for other_stream in streams[0: ]:
            # stream = stream.union(other_stream)

            # stream.start()
            # stream.awaitTermination()

        streaming_context.start()

        start_time = time.time()

        while time.time() - start_time < 5 * 60:
            time.sleep(1)
        # streaming_context.awaitTermination()
        streaming_context.stop()

if __name__ == '__main__':
    args = parse_args()
    Benchmark(args.master, args.generators_ips, args.port, 8, 4, args.results_path).run_benchmark()
