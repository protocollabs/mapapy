#!/usr/bin/env python3


import socket
import sys

PORT = 64321

def init_socket():
    sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_address = ('localhost', 10000)
    sk.bind(server_address)
    return sk

def loop(sk):
    while True:
        data, address = sk.recvfrom(4096)
        print('received %s bytes from %s' % (len(data), address))
        #if data:
        #    sent = sk.sendto(data, address)

def main():
    sk = init_socket()
    loop(sk)

if __name__ == '__main__':
    main()
