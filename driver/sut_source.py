import socket
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
            print('Connected!', self.generator.queue.qsize(), self.generator.done.value, flush=True)

            while self.generator.queue.qsize() > 0 and not self.generator.done.value:
                # print('getting data', flush=True)
                data = self.generator.queue.get()
                # print(data, flush=True)
                # print('Sending', flush=True)
                conn.sendall((data + '\n').encode())
            print('DONE!', flush=True)
            conn.close()
            sock.close()
