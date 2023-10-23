# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY
import struct
from concurrent.futures import ThreadPoolExecutor
import time
import hashlib
from threading import Timer, Lock

DATA_CHUNK_SIZE=1448
ACK_TIMEOUT=0.25

class Streamer:
    FIN_WAIT_TIMEOUT=2

    def __init__(self, dst_ip, dst_port,
                 src_ip=INADDR_ANY, src_port=0):
        """Default values listen on all network interfaces, chooses a random source port,
           and does not introduce any simulated packet loss."""
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port

        self.sender = Sender(self.socket, dst_ip, dst_port)
        self.receiver = Receiver(self.socket, dst_ip, dst_port)

        self.closed = False
        self.fin_acked = False
        self.fin_seq = -1
        self.fin_received = False

        executor = ThreadPoolExecutor(max_workers=1)
        executor.submit(self.listener)


    def listener(self) :
        while not self.closed: # a later hint will explain self.closed
            try:
                data, addr = self.socket.recvfrom()
                if not data:
                    break
                header = struct.unpack("<ii", data[:8])
                num = header[0]
                type = header[1]
                hash = data[8:24]
                body = data[24:]

                if type == 1 and hash == hashlib.md5(bytes(num)).digest():
                    if num == self.fin_seq:
                        self.fin_acked = True
                    else:
                        self.sender.receive_ack(num)

                elif type == 2:
                    if self.receiver.receive_fin(num, hash):
                        self.fin_received=True

                elif type == 0:
                    self.receiver.receive_data(num, hash, body)

            except Exception as e:
                print("listener died!")
                print(e)

    def send(self, data_bytes: bytes) -> None:
        self.sender.send(data_bytes)     
        

    def recv(self) -> bytes:
        return self.receiver.buffer_pop()


    def close(self) -> None:
        while not self.fin_acked:
            self.fin_seq = self.sender.send_fin() + 1
            time.sleep(ACK_TIMEOUT)

        while not self.fin_received:
            time.sleep(0.01)

        time.sleep(2)

        self.closed = True and self.socket.stoprecv()



class Sender:

    def __init__(self, socket, dst_ip, dst_port):
        self.socket = socket
        self.dst_ip = dst_ip
        self.dst_port = dst_port

        self.seq = 0
        self.in_flight = []
        self.timer = None

    def send(self, data_bytes: bytes) -> None:
        if not self.timer:
            self.timer = Timer(ACK_TIMEOUT, self.handle_timeout)
            self.timer.start()
        for chunk in [data_bytes[i:i+DATA_CHUNK_SIZE] for i in range(0, len(data_bytes), DATA_CHUNK_SIZE)]:
            header = struct.pack("<ii", self.seq, 0) + hashlib.md5(chunk).digest()
            packet = header + chunk
            self.in_flight.append((self.seq, packet))
            self.socket.sendto(packet, (self.dst_ip, self.dst_port))
            self.seq += len(chunk)

    def receive_ack(self, num):
        if self.in_flight and self.in_flight[0][0] < num:
            if self.timer:
                self.timer.cancel()
            self.timer = Timer(ACK_TIMEOUT, self.handle_timeout)
            self.timer.start()
            while self.in_flight and self.in_flight[0][0] < num:
                self.in_flight.pop(0)
        if num == self.seq:
            if self.timer:
                self.timer.cancel()
            self.timer = None
        
            
    def handle_timeout(self):
        for sequence, packet in self.in_flight:
            self.socket.sendto(packet, (self.dst_ip, self.dst_port))
        self.timer = Timer(ACK_TIMEOUT, self.handle_timeout)
        self.timer.start()

    def send_fin(self):
            while self.in_flight:
                time.sleep(0.01)
            header=struct.pack("<ii", self.seq, 2) + hashlib.md5(bytes(self.seq)).digest()
            self.socket.sendto(header, (self.dst_ip, self.dst_port))
            return self.seq



class Receiver:

    def __init__(self, socket, dst_ip, dst_port):
        self.socket = socket
        self.dst_ip = dst_ip
        self.dst_port = dst_port
        self.buffer = []

        self.next = 0


    def buffer_pop(self) -> bytes:
        while True:
            if self.buffer:
                return self.buffer.pop(0)
            
    def receive_data(self, seq, hash, body):
        if seq == self.next and hash == hashlib.md5(body).digest():
            self.next += len(body)
            self.buffer.append(body)
        newheader = struct.pack("<ii", self.next, 1) + hashlib.md5(bytes(self.next)).digest()
        self.socket.sendto(newheader, (self.dst_ip, self.dst_port))

    def receive_fin(self, seq, hash):
        if seq == self.next and hash == hashlib.md5(bytes(seq)).digest():
            newheader=struct.pack('<ii', self.next + 1, 1) + hashlib.md5(bytes(self.next + 1)).digest()
            self.socket.sendto(newheader, (self.dst_ip, self.dst_port))
            return True