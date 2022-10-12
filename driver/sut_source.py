import socket
import time
from multiprocessing import Process


class SUTSource(Process):

    def __init__(self, host, port, generator):
        super().__init__()
        self.host = host
        self.port = port
        self.generator = generator

    def run(self):
        print('Starting SUT Source', flush=True)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            print('Binding Socket', flush=True)
            sock.bind((self.host, self.port))
            print('Listening', flush=True)
            sock.listen(0)
            
            conn, _ = sock.accept()

            start = time.time()
            print('Connected!', self.generator.queue.qsize(), self.generator.done.value, flush=True)

            i = 0
            print(self.generator.queue.qsize(), self.generator.done.value)
            while not self.generator.done.value or self.generator.queue.qsize() > 0:
                # print('getting data', flush=True)
                data = self.generator.queue.get()
                # print(data, flush=True)
                # print('Sending', flush=True)
                conn.sendall((data + '\n').encode())

                if i % 1000 == 0:
                    print(f'sent {i}, {self.generator.queue.qsize()}', flush=True)
                i += 1

            total_time = time.time() - start
            actual_rate = self.generator.num_events / total_time
            print(f'DONE! Observed rate: {actual_rate}', flush=True)
            conn.close()
            sock.close()
