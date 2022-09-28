import socket
from multiprocessing import Process


class SUTSource(Process):

    def __init__(self, host, port, generator):
        super().__init__()
        self.host = host
        self.port = port
        self.generator = generator

    def run(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind((self.host, self.port))
            sock.listen(0)

            conn, _ = sock.accept()

            self.generator.start()
            while self.generator.queue.qsize() == 0 and not self.generator.done.value:
                data = self.generator.queue.get()
                conn.sendall(data.encode() + '\n')

            conn.close()
            sock.close()
