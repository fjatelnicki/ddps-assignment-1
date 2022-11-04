import socket
import time
import numpy as np
from multiprocessing import Process


class SUTSource(Process):

    def __init__(self, host, port, generator, throughput_save_path):
        super().__init__()
        self.host = host
        self.port = port
        self.generator = generator
        self.throughput_save_path = throughput_save_path
        print(throughput_save_path, flush=True)

    def run(self):
        print('Starting SUT Source', flush=True)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            print(f'Binding Socket {self.host}, {self.port}', flush=True)
            sock.bind((self.host, self.port))
            print('Listening', flush=True)
            sock.listen(0)
            
            conn, _ = sock.accept()

            start = time.time()
            print('Connected!', self.host, self.port, self.generator.queue.qsize(), self.generator.done.value, flush=True)

            i = 1
            print(self.generator.queue.qsize(), self.generator.done.value)
            # while not self.generator.done.value or self.generator.queue.qsize() > 0:
            times = []
            timeout = False
            
            while not timeout and i < self.generator.num_events:
                # print('getting data', flush=True)
                # p = time.time()
                data = self.generator.queue.get()
                # print(f'get time {time.time()-p}')
                # print(data, flush=True)
                # print('Sending', flush=True)
                conn.sendall((data + '\n').encode())

                if i % 10000 == 0:
                    print(f'sent {i}, {self.generator.queue.qsize()}, Rate {i / (time.time() - start)}', flush=True)
                    times.append(time.time())
                    if time.time() - start > 5 * 60:
                        break
                
                if i % 100_000 == 0:
                    np.save(self.throughput_save_path, times)
                i += 1
            
            np.save(self.throughput_save_path, times)
            total_time = time.time() - start
            actual_rate = self.generator.num_events / total_time
            print(f'DONE! Total time {total_time}, Observed rate: {actual_rate}', flush=True)
            conn.close()
            sock.close()
