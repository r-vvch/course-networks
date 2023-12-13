import socket
import os
import math


class UDPBasedProtocol:
    def __init__(self, *, local_addr, remote_addr):
        self.udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.remote_addr = remote_addr
        self.udp_socket.bind(local_addr)

    def sendto(self, data):
        return self.udp_socket.sendto(data, self.remote_addr)

    def recvfrom(self, n):
        msg, addr = self.udp_socket.recvfrom(n)
        return msg
    

MAX_SEND_SIZE = 10000
REPEAT_NUM = 10


class Packet:

    id_len = 10
    part_num_len = 2
    num_parts_len = 2
    packet_len = MAX_SEND_SIZE
    data_len = packet_len - id_len - part_num_len - num_parts_len

    def __init__(self, data: bytes, send=True):
        self.current_part = 0
        if send == True:
            self.id = os.urandom(self.id_len)
            self.num_parts = math.ceil(len(data) / self.data_len)
            self.data = [data[i * self.data_len: (i + 1) * self.data_len] 
                            for i in range(self.num_parts)]
        else:
            self.id = data[:self.id_len]
            self.num_parts = int.from_bytes(data[self.id_len: self.id_len + self.num_parts_len],
                                            'big')
            current_part = int.from_bytes(data[self.id_len + self.num_parts_len: self.id_len + 
                                            self.num_parts_len + self.part_num_len], 'big')
            self.data = [None] * self.num_parts
            self.data[current_part] = data[self.id_len + self.num_parts_len + self.part_num_len:]


    def __str__(self):
        return f"id={self.id}, num_parts={self.num_parts}, data={self.data}"
    
    def __iter__(self):
        return self
    
    def is_full(self):
        return all(self.data)

    def __next__(self):
        if self.current_part < self.num_parts:
            result = b''.join([
                self.id,
                self.num_parts.to_bytes(self.num_parts_len, 'big'),
                self.current_part.to_bytes(self.part_num_len, 'big'),
                self.data[self.current_part]
                ])
            self.current_part += 1
            return result
        else:
            self.current_part = 0
            raise StopIteration
        
    def to_bytes(self):
        if self.is_full():
            return b''.join(self.data)
        else:
            raise ValueError("to_bytes() method can't be called without full packet\n")
        
    def extend_from_bytes(self, data: bytes):
        id = data[:self.id_len]
        num_parts = int.from_bytes(data[self.id_len: self.id_len + self.num_parts_len], 'big')
        current_part = int.from_bytes(data[self.id_len + self.num_parts_len: self.id_len + 
                                            self.num_parts_len + self.part_num_len], 'big')
        if id == self.id and self.data[current_part] is None:
            self.data[current_part] = data[self.id_len + self.num_parts_len + self.part_num_len:]
            return True
        else:
            return False


class MyTCPProtocol(UDPBasedProtocol):
    
    repeat_num_curr = REPEAT_NUM

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen_ids = set()

    def send(self, data: bytes):

        packet = Packet(data)
        for packet_part in packet:
            for i in range(self.repeat_num_curr):
                assert self.sendto(packet_part) == len(packet_part)

        return len(data)

    def recv(self, n: int):

        while True:
            packet_bytes = self.recvfrom(MAX_SEND_SIZE)
            packet = Packet(packet_bytes, send=False)
            
            if packet.id not in self.seen_ids:
                self.seen_ids.add(packet.id)
                while not packet.is_full():
                    packet_bytes = self.recvfrom(MAX_SEND_SIZE)
                    packet.extend_from_bytes(packet_bytes)
                    
                return packet.to_bytes()
