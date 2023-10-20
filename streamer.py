# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY
import struct
from concurrent.futures import ThreadPoolExecutor
import time


class Streamer:
    def __init__(self, dst_ip, dst_port,
                 src_ip=INADDR_ANY, src_port=0):
        """Default values listen on all network interfaces, chooses a random source port,
           and does not introduce any simulated packet loss."""
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port
        self.seq = 0
        self.expecting = 0
        self.buffer = {}
        self.closed = False

        executor = ThreadPoolExecutor(max_workers=1)
        executor.submit(self.listener)

    def listener(self) :
        while not self.closed: # a later hint will explain self.closed
            try:
                data, addr = self.socket.recvfrom()
                header = struct.unpack("<ii", data[:8])
                body = data[8:]
                seq = header[0]
                type = header[1]
                if type == 1:
                    self.ack = True
                else:
                    self.buffer[seq] = body
                    newheader = struct.pack("<ii", self.seq, 1)
                    self.socket.sendto(newheader, (self.dst_ip, self.dst_port))

            except Exception as e :
                print("listener died!")
                print(e)

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        self.ack = False
        # Your code goes here!  The code below should be changed!
        for start in range(0, len(data_bytes), 1464):
            header = struct.pack("<ii", self.seq, 0)
            end = min(start + 1464, len(data_bytes))
            self.socket.sendto(header + data_bytes[start:end], (self.dst_ip, self.dst_port))
            self.seq += end - start
        while not self.ack:
            time.sleep(0.01)

    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        # your code goes here!  The code below should be changed!
        while True:
            if self.expecting in self.buffer:
                output = self.buffer[self.expecting]
                del self.buffer[self.expecting]
                self.expecting += len(output)
                return output  


    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # your code goes here, especially after you add ACKs and retransmissions.
        self.closed = True
        self.socket.stoprecv()
