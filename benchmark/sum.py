import argparse
import os.path

import json
import time

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext


class WindowedAggregation:
    ssc: StreamingContext
    spark: SparkSession

    def __init__(self, master, host, port, storage):
        self.master = master
        self.host = host
        self.port = port
        self.storage = storage
        self.sc = SparkContext(self.master, "Windowed Aggregation Query")
        self.sc.addPyFile('ddps-assignment-1/benchmark/sum.py')
        self.ssc = StreamingContext(self.sc, 4)  # 4 second window as specified in the paper

    @staticmethod
    def aggregate(a, b) -> (float, float):
        """
        This function returns the sum of the price and the max of the event time
        :rtype: (float, float)
        """
        a = (0, 0) if a is None else a
        b = (0, 0) if b is None else b
        return a[0] + b[0], max(a[1], b[1])

    @staticmethod
    def wrap(aggregated_result):
        end_time = time.time()
        return {'gemPackID': aggregated_result[0],
                'price': aggregated_result[1][0],
                'time': end_time,
                'latency': end_time - aggregated_result[1][1]}

    def run(self):
        self.sc.setLogLevel('ALL')
        self.ssc.socketTextStream(self.host, self.port) \
            .map(json.loads) \
            .map(lambda purchase: (str(purchase['gemPackID']), (purchase['price'], purchase['time']))) \
            .reduceByKey(self.aggregate) \
            .map(self.wrap) \
            .saveAsTextFiles(self.storage)

        self.ssc.start()
        self.ssc.awaitTermination()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Distributed data processing asignment 1')
    parser.add_argument('--host', type=str, default='localhost')
    parser.add_argument('--port', type=int, default=9999)
    parser.add_argument('--storage', type=str, default='dps1/results/windowed_aggregation/')
    parser.add_argument('--master', type=str, required=True,
                        help='the name of the spark master node (example: spark://node102.cm.cluster:7077)')

    args = parser.parse_args()

    os.makedirs(args.storage, exist_ok=True)

    aggregation = WindowedAggregation(args.master, args.host, args.port, args.storage)
    aggregation.run()