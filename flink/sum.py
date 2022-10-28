# from flink.plan.Environment import get_environment
# from flink.plan.Constants import WriteMode
# from flink.functions.GroupReduceFunction import GroupReduceFunction
from pyflink.datastream import DataStream, StreamExecutionEnvironment
from pyflink.datastream.window import SlidingEventTimeWindows, SlidingProcessingTimeWindows
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy
from pyflink.common import Types, WatermarkStrategy, Time, Encoder
from pyflink.common import Types, WatermarkStrategy, Time, Encoder
from pyflink.common.watermark_strategy import TimestampAssigner

import time

import sys

# class Adder(GroupReduceFunction):
#     def reduce(self, iterator, collector):
#         count, word = iterator.next()
#         count += sum([x[0] for x in iterator])
#         collector.collect((count, word))
def process_data(data):
    data = data.split('\t')
    # print(data)
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
    # print(data)
    pack_id, price, prev_time = data
    # print('Processing', flush=True)
    return f'{pack_id}\t{price}\t{prev_time}\t{cur_time}'
    # return data
if __name__ == "__main__":
    s_env = StreamExecutionEnvironment.get_execution_environment()
    socket_stream = DataStream(s_env._j_stream_execution_environment.socketTextStream('node105', 9999))
    # print('klkjkl')
    # socket_stream
    socket_stream = socket_stream.map(process_data) \
            .key_by(lambda value: value[0]).window(SlidingProcessingTimeWindows.of(Time.seconds(8), Time.seconds(4))).reduce(aggregate_tuples).map(data_to_csv).print()

    s_env.execute('socket_stream')
    # get the base path out of the runtime params
    # base_path = sys.argv[0]

    # # we have to hard code in the path to the output because of gotcha detailed in readme
    # output_file = 'file://' + base_path + '/word_count/out.txt'
    
    # # set up the environment with a single string as the environment data
    # env = get_environment()
    # data = env.from_elements("Who's there? I think I hear them. Stand, ho! Who's there?")

    # # we first map each word into a (1, word) tuple, then flat map across that, and group by the key, and sum
    # # aggregate on it to get (count, word) tuples, then pretty print that out to a file.
    # data \
    #     .flat_map(lambda x, c: [(1, word) for word in x.lower().split()]) \
    #     .group_by(1) \
    #     .reduce_group(Adder(), combinable=True) \
    #     .map(lambda y: 'Count: %s Word: %s' % (y[0], y[1])) \
    #     .write_text(output_file, write_mode=WriteMode.OVERWRITE)

    # # execute the plan locally.
    # env.execute(local=True)