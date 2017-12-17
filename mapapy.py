#!/usr/bin/env python3

import socket
import sys
import argparse
import struct


PORT = 64321

PROTOCOL_NULL_REQUEST_CODE = 1
PROTOCOL_NULL_REPLY_CODE   = 2


NULL_MSG_SIZE = 512

def init_socket(ctx):
    sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sk.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sk.bind(('localhost', PORT))
    return sk

def srv_msg_null_request(ctx, data, address):
    # resend message, change code to reply
    b = struct.pack('>I', PROTOCOL_NULL_REPLY_CODE) + data[4:]
    ctx['sk'].sendto(b, address)

def server_recv_loop(ctx):
    while True:
        data, address = ctx['sk'].recvfrom(4096)
        if len(data) <= 4:
            print('message to short, should header and at least one byte')
            continue
        code = struct.unpack('>I', data[0:4])[0]
        if code == 1:
            srv_msg_null_request(ctx, data, address)
        else:
            print('unhandled message code: {}'.format(code))
            continue


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--daemon", action='store_true', default=False)
    parser.add_argument('--addr', action="store",  type=str, default="localhost")
    parser.add_argument("-v", "--verbose", action="count", default=0,
                        help="increase output verbosity")
    return parser.parse_args()

def init_ctx():
    return { 'args' : parse_args() }

def mode_server(ctx):
    ctx['sk'] = init_socket(ctx)
    server_recv_loop(ctx)

def msg_create_null_msg(ctx):
    b = struct.pack('>I', PROTOCOL_NULL_REQUEST_CODE)
    return b + bytearray(NULL_MSG_SIZE - 4)

def client_process_null_reply(ctx, data, address):
    if len(data) <= 4:
        raise Exception('message to short, should header and at least one byte')
    code = struct.unpack('>I', data[0:4])[0]
    if code == 2 and len(data) == NULL_MSG_SIZE:
        print('receive valid NULL message')
        return True
    else:
        raise Exception('unexpected reply from client!')


def client_process_null_message(ctx):
    # tx phase
    msg = msg_create_null_msg(ctx)
    print('send NULL message')
    ctx['sk'].sendto(msg, (ctx['args'].addr, PORT))
    # rx phase
    data, address = ctx['sk'].recvfrom(4096)
    return client_process_null_reply(ctx, data, address)


def mode_client(ctx):
    ctx['sk'] = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    ok = client_process_null_message(ctx)
    if not ok:
        print("failed to process null message")
        return

def main():
    ctx = init_ctx()
    if ctx['args'].daemon:
        mode_server(ctx)
    else:
        mode_client(ctx)

if __name__ == '__main__':
    main()
