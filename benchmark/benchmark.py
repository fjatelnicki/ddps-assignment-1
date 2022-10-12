import time
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
import argparse


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--master', type=str)
    parser.add_argument('--port', type=int)

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

    return f'{pack_id}\t{price}\t{prev_time}\t{cur_time}'


class Benchmark:
    def __init__(self, master, port, batch_duration, results_path):
        self.master = master
        self.port = port
        self.batch_duration = batch_duration
        self.results_path = results_path

    def run_benchmark(self):
        spark_context = SparkContext(f'spark://{args.master}:7077')
        spark_context.setLogLevel('off')
        streaming_context = StreamingContext(spark_context, self.batch_duration)
        print(f'Listening {self.master}:{self.port}', flush=True)
        stream = streaming_context.socketTextStream(self.master, self.port)
        stream = stream.map(process_data)
        stream = stream.reduceByKey(aggregate_tuples)
        stream = stream.map(data_to_csv)
        stream = stream.saveAsTextFiles(self.results_path)

        streaming_context.start()
        streaming_context.awaitTermination()


if __name__ == '__main__':
    args = parse_args()
    Benchmark(args.master, args.port, 4, 'sum/results').run_benchmark()