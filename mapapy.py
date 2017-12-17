#!/usr/bin/env python3

import socket
import sys
import argparse
import struct
import uuid
import datetime
import json


PORT = 64321

PROTOCOL_NULL_REQUEST_CODE = 1
PROTOCOL_NULL_REPLY_CODE   = 2
PROTOCOL_INFO_REQUEST_CODE = 3
PROTOCOL_INFO_REPLY_CODE   = 4


NULL_MSG_SIZE = 512

def maparo_date():
    dt = datetime.datetime.utcnow()
    return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')

def maparo_date_parse(string):
    return datetime.datetime.strptime(string, '%Y-%m-%dT%H:%M:%S.%f')

def init_socket(ctx):
    sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sk.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sk.bind(('localhost', PORT))
    return sk

def srv_msg_null_request(ctx, data, address):
    # resend message, change code to reply
    b = struct.pack('>I', PROTOCOL_NULL_REPLY_CODE) + data[4:]
    ctx['sk'].sendto(b, address)

def srv_msg_info_request(ctx, data, address):
    json_len = struct.unpack('>I', data[4:8])[0]
    if len(data) - (4 + 4) < json_len:
        raise Exception('packet is smaller as expected')
    json_bytes = data[8:json_len + 8]
    request_data = json.loads(json_bytes.decode())
    reply_data = dict()
    reply_data['id'] = "{}={}".format(socket.gethostname(), uuid.uuid4())
    reply_data['seq-rp'] = request_data['seq']
    reply_data['ts-rp'] = request_data['ts']
    reply_data['ts'] = maparo_date()
    reply_data['arch'] = 'unknown'
    reply_data['os'] = 'unknown'
    json_bytes = str.encode(json.dumps(reply_data))
    b = struct.pack('>II', PROTOCOL_INFO_REPLY_CODE, len(json_bytes))
    buf = b + json_bytes
    ctx['sk'].sendto(buf, address)


def server_recv_loop(ctx):
    while True:
        data, address = ctx['sk'].recvfrom(4096)
        if len(data) <= 4:
            print('message to short, should header and at least one byte')
            continue
        code = struct.unpack('>I', data[0:4])[0]
        if code == PROTOCOL_NULL_REQUEST_CODE:
            srv_msg_null_request(ctx, data, address)
        elif code == PROTOCOL_INFO_REQUEST_CODE:
            srv_msg_info_request(ctx, data, address)
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

def timedelta_ms(timedelta):
    return timedelta.total_seconds() * 1000.0

def human_timedelta(timedelta):
    return '{} ms'.format(timedelta_ms(timedelta))

def client_process_null_message(ctx):
    # tx phase
    msg = msg_create_null_msg(ctx)
    print('send NULL message')
    ctx['sk'].sendto(msg, (ctx['args'].addr, PORT))
    # rx phase
    data, address = ctx['sk'].recvfrom(4096)
    return client_process_null_reply(ctx, data, address)

def check_time_sync(ctx, reply_data):
    request_date = maparo_date_parse(reply_data['ts-rp'])
    now = datetime.datetime.utcnow()

    rtt = now - request_date
    print("rtt: {}".format(human_timedelta(rtt)))

    time_server = maparo_date_parse(reply_data['ts'])

    ideal_time_server = request_date - (time_server - (rtt / 2.0))
    print("time delta to server: {}".format(human_timedelta(ideal_time_server)))
    print("[note: smaller 0: server clock is before client clock, otherwise larger]")

def client_process_info_reply(ctx, data, address):
    if len(data) <= 8:
        raise Exception('message to short, should header and at least one byte')
    code, length = struct.unpack('>II', data[0:8])
    if code != PROTOCOL_INFO_REPLY_CODE:
        print('receive invlid info reply message')
        return False
    assert len(data) == length + 8
    json_bytes = data[8:length + 8]
    reply_data = json.loads(json_bytes.decode())
    check_time_sync(ctx, reply_data)
    return True

def msg_create_info_msg(ctx):
    msg = dict()
    msg['id'] = "{}={}".format(socket.gethostname(), uuid.uuid4())
    msg['seq'] = 0
    msg['ts'] = maparo_date()
    json_bytes = str.encode(json.dumps(msg))
    b = struct.pack('>II', PROTOCOL_INFO_REQUEST_CODE, len(json_bytes))
    return b + json_bytes

def client_process_info_message(ctx):
    # tx phase
    msg = msg_create_info_msg(ctx)
    print('send INFO message')
    ctx['sk'].sendto(msg, (ctx['args'].addr, PORT))
    # rx phase
    data, address = ctx['sk'].recvfrom(4096)
    return client_process_info_reply(ctx, data, address)


def mode_client(ctx):
    ctx['sk'] = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    ok = client_process_null_message(ctx)
    if not ok:
        print("failed to process null message")
        return
    ok = client_process_info_message(ctx)
    if not ok:
        print("failed to process info message")
        return

def main():
    ctx = init_ctx()
    if ctx['args'].daemon:
        mode_server(ctx)
    else:
        mode_client(ctx)

if __name__ == '__main__':
    main()
