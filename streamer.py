# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY
import struct
from concurrent.futures import ThreadPoolExecutor
import time


class Streamer:
    ACK_TIMEOUT=0.25
    FIN_WAIT_TIMEOUT=2

    DATA_CHUNK_SIZE=1464

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
        self.ack=False
        self.fin_received=False

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
                elif type == 2:
                    self.fin_received=True
                    newheader=struct.pack('<ii', 0, 1)
                    self.socket.sendto(newheader, (self.dst_ip, self.dst_port))
                else:
                    self.buffer[seq] = body
                    # print(self.buffer)
                    newheader = struct.pack("<ii", self.seq, 1)
                    self.socket.sendto(newheader, (self.dst_ip, self.dst_port))
            except Exception as e :
                print("listener died!")
                print(e)

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        chunks = [data_bytes[i:i+self.DATA_CHUNK_SIZE] for i in range(0, len(data_bytes), self.DATA_CHUNK_SIZE)]
        # Your code goes here!  The code below should be changed!
        # initialseq = self.seq
        for chunk in chunks:
            self.ack=False
            # self.seq = initialseq
            while not self.ack:
                header = struct.pack("<ii", self.seq, 0)
                self.socket.sendto(header + chunk, (self.dst_ip, self.dst_port))
                time.sleep(self.ACK_TIMEOUT)
            self.seq += len(chunk)
            

            
        

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
        start_time=time.time()

        while not self.ack and (time.time() - start_time) < self.ACK_TIMEOUT:
            FIN_HEADER=-1
            header=struct.pack("<ii", FIN_HEADER, 2)
            self.socket.sendto(header, (self.dst_ip, self.dst_port))
            time.sleep(self.ACK_TIMEOUT)

        start_time=time.time()

        while not self.fin_received and (time.time() -start_time) < self.FIN_WAIT_TIMEOUT:
            time.sleep(0.01)

        self.closed = True
        self.socket.stoprecv()
