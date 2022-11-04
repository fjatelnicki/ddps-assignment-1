from pyflink.datastream import DataStream, StreamExecutionEnvironment
from pyflink.datastream.window import SlidingEventTimeWindows, SlidingProcessingTimeWindows
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy
from pyflink.common import Types, WatermarkStrategy, Time, Encoder
from pyflink.common import Types, WatermarkStrategy, Time, Encoder
from pyflink.common.watermark_strategy import TimestampAssigner
import argparse
import time


def process_data(data):
    data = data.split('\t')

    return data[1], data[2], data[3]

def aggregate_tuples(d1, d2):
    
    print(d1, d2)
    id1, price1, time1 = d1
    id2, price2, time2 = d2

    total_price = float(price1) + float(price2)
    max_time = max(time1,
                   time2)  # In a windowed join operation, the containing tuples’ event time is set of be the maximum event-time of their win
    assert id1 == id2
    return id1, total_price, max_time

def data_to_csv(data):
    cur_time = time.time()
    pack_id, price, prev_time = data

    return f'{pack_id}\t{price}\t{prev_time}\t{cur_time}'

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Distributed data processing asignment 1')
    parser.add_argument('--port', type=int)
    parser.add_argument('--results-path', type=str)
    parser.add_argument('--n-gens', type=int)
    parser.add_argument('--generators-ips', nargs='+')

    args = parser.parse_args()
    ports = [args.port - i for i in range(args.n_gens)]

    s_env = StreamExecutionEnvironment.get_execution_environment()
    first = True
    streams = []
    for generator_node in args.generators_ips:
        for port in ports:
            stream = DataStream(s_env._j_stream_execution_environment.socketTextStream(generator_node, port))
            streams.append(stream)
            print(f'Listening {generator_node}:{port}', flush=True)
    stream = streams[0].union(*streams[1: ])
    # stream = DataStream(s_env._j_stream_execution_environment.socketTextStream(args.generators_ips[0], ports[0]))
    
    stream = stream.map(process_data) \
            .key_by(lambda value: value[0])  \
            .window(SlidingProcessingTimeWindows.of(Time.seconds(8), Time.seconds(4)))  \
            .reduce(aggregate_tuples)  \
            .map(data_to_csv)  \
            .print()

    s_env.execute('socket_stream')
