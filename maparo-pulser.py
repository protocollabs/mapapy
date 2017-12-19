#!/usr/bin/env python3

import socket
import sys
import argparse
import struct
import uuid
import datetime
import json
import pprint


PORT = 64321

PROTOCOL_NULL_REQUEST_CODE  = 1
PROTOCOL_NULL_REPLY_CODE    = 2
PROTOCOL_INFO_REQUEST_CODE  = 3
PROTOCOL_INFO_REPLY_CODE    = 4
PROTOCOL_START_REQUEST_CODE = 5
PROTOCOL_START_REPLY_CODE   = 6

MODULE_UDP_PULSER_NAME = '_udp-pulser'

NULL_MSG_SIZE = 512

def maparo_id():
    return "{}={}".format(socket.gethostname(), uuid.uuid4())

def maparo_date():
    dt = datetime.datetime.utcnow()
    return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')

# XXX strptime seem to consume WAY to much resourches.
# on amd64 box it takes 2 milliseconds
def maparo_date_parse(string):
    return datetime.datetime.strptime(string, '%Y-%m-%dT%H:%M:%S.%f')

def init_socket(ctx):
    sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sk.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sk.bind(('', PORT))
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
    reply_data['id'] = maparo_id()
    reply_data['seq-rp'] = request_data['seq']
    reply_data['ts-rp'] = request_data['ts']
    reply_data['ts'] = maparo_date()
    reply_data['arch'] = 'unknown'
    reply_data['os'] = 'unknown'
    reply_data['modules'] = { MODULE_UDP_PULSER_NAME : {} }
    json_bytes = str.encode(json.dumps(reply_data))
    b = struct.pack('>II', PROTOCOL_INFO_REPLY_CODE, len(json_bytes))
    buf = b + json_bytes
    ctx['sk'].sendto(buf, address)

def init_v4_rx_fd(port):
	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
	s.setblocking(False)
	s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	if hasattr(s, "SO_REUSEPORT"):
		s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
	s.bind(('', int(port)))
	return s

def srv_msg_start_request_process(ctx, request_data):
    module_name = request_data['module']['name']
    if module_name != MODULE_UDP_PULSER_NAME:
        return False, 'module ({}) not supported'.format(module_name)
    ctx['db-srv'] = dict()
    for entry in request_data['module']['configuration']['streams']:
        port = entry['port']
        ctx['db-srv'][port] = dict()
        ctx['db-srv'][port]['name'] = entry['stream']
        ctx['db-srv'][port]['fd'] = init_v4_rx_fd(port)
        ctx['db-srv'][port]['sequence-expected'] = 0
        ctx['db-srv'][port]['packets-reqeived'] = 0
        ctx['db-srv'][port]['bytes-received'] = 0
    return True, ""

def srv_msg_start_request(ctx, data, address):
    json_len = struct.unpack('>I', data[4:8])[0]
    if len(data) - (4 + 4) < json_len:
        raise Exception('packet is smaller as expected')
    json_bytes = data[8:json_len + 8]
    request_data = json.loads(json_bytes.decode())
    ok, msg = srv_msg_start_request_process(ctx, request_data)
    reply_data = dict()
    reply_data['seq-rq'] = request_data['seq']
    if not ok:
        # return with failure
        reply_data['status'] = 'failed'
        reply_data['message'] = msg
    else:
        # return with success
        reply_data['status'] = 'ok'
    json_bytes = str.encode(json.dumps(reply_data))
    b = struct.pack('>II', PROTOCOL_START_REPLY_CODE, len(json_bytes))
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
        elif code == PROTOCOL_START_REQUEST_CODE:
            srv_msg_start_request(ctx, data, address)
        else:
            print('unhandled message code: {}'.format(code))
            continue


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--configuration", help="configuration", type=str, default=None)
    parser.add_argument("--daemon", action='store_true', default=False)
    parser.add_argument('--addr', action="store",  type=str, default="localhost")
    parser.add_argument("-v", "--verbose", action="count", default=0,
                        help="increase output verbosity")
    return parser.parse_args()

def load_configuration_file(args):
    if not args.configuration:
        print("client mode but no --configuration <file> given ...")
        sys.exit(0)
    config = dict()
    exec(open(args.configuration).read(), None, config)
    return config

def init_ctx():
    args = parse_args()
    conf = None
    if not args.daemon:
        conf = load_configuration_file(args)
    return { 'args' : args, 'conf' : conf }

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
    if code == PROTOCOL_NULL_REPLY_CODE and len(data) == NULL_MSG_SIZE:
        print('receive valid null-reply message')
        return True
    else:
        raise Exception('unexpected reply from client!')

def timedelta_ms(timedelta):
    return timedelta.total_seconds() * 1000.0

def human_timedelta(timedelta):
    return '{0:.3f} ms'.format(timedelta_ms(timedelta))

def client_process_null_message(ctx):
    # tx phase
    msg = msg_create_null_msg(ctx)
    print('send null-request message')
    ctx['sk'].sendto(msg, (ctx['args'].addr, PORT))
    # rx phase
    data, address = ctx['sk'].recvfrom(4096)
    return client_process_null_reply(ctx, data, address)

def check_time_sync(ctx, reply_data, rx_time):
    now = rx_time
    request_date = maparo_date_parse(reply_data['ts-rp'])
    rtt = now - request_date
    print("rtt: {}".format(human_timedelta(rtt)))
    time_server = maparo_date_parse(reply_data['ts'])
    ideal_time_server = request_date - (time_server - (rtt / 2.0))
    print("time delta to server: {}".format(human_timedelta(ideal_time_server)))
    print("[Note: smaller 0: server clock is before client clock, otherwise behind]")

def check_server_info(ctx, reply_data):
    print("server name: {}".format(reply_data['id'].split("=")[0]))
    print("arch:        {}".format(reply_data['arch']))
    print("os:          {}".format(reply_data['os']))
    print("modules supported: {}".format(",".join(reply_data['modules'].keys())))
    if not MODULE_UDP_PULSER_NAME in reply_data['modules']:
        print("udp pulser module not suported by server")
        return False
    return True

def client_process_info_reply(ctx, data, address, rx_time):
    if len(data) <= 8:
        raise Exception('message to short, should header and at least one byte')
    code, length = struct.unpack('>II', data[0:8])
    if code != PROTOCOL_INFO_REPLY_CODE:
        print('receive invalid info reply message')
        return False
    assert len(data) == length + 8
    print('receive valid info-reply message')
    json_bytes = data[8:length + 8]
    reply_data = json.loads(json_bytes.decode())
    check_time_sync(ctx, reply_data, rx_time)
    return check_server_info(ctx, reply_data)

def msg_create_info_msg(ctx):
    msg = dict()
    msg['id'] = maparo_id()
    msg['seq'] = 0
    msg['ts'] = maparo_date()
    json_bytes = str.encode(json.dumps(msg))
    b = struct.pack('>II', PROTOCOL_INFO_REQUEST_CODE, len(json_bytes))
    return b + json_bytes

def client_process_info_message(ctx):
    # tx phase
    msg = msg_create_info_msg(ctx)
    print('send info-request message')
    ctx['sk'].sendto(msg, (ctx['args'].addr, PORT))
    # rx phase
    data, address = ctx['sk'].recvfrom(4096)
    rx_time = datetime.datetime.utcnow()
    return client_process_info_reply(ctx, data, address, rx_time)

def client_start_request_prep_conf(ctx):
    d = dict()
    streams = []
    port = int(ctx['conf']['start_port'])
    for i, stream in enumerate(ctx['conf']['streams']):
        entry = dict()
        entry['port'] = str(port)
        entry['stream'] = str(i)
        port += 1
        streams.append(entry)
    d['streams'] = streams
    return d

def msg_create_start_request(ctx):
    msg = dict()
    msg['id'] = maparo_id()
    msg['seq'] = 0
    msg['module'] = dict()
    msg['module']['name'] = MODULE_UDP_PULSER_NAME
    msg['module']['configuration'] = client_start_request_prep_conf(ctx)
    pprint.pprint(msg)
    json_bytes = str.encode(json.dumps(msg))
    b = struct.pack('>II', PROTOCOL_START_REQUEST_CODE, len(json_bytes))
    return b + json_bytes

def client_check_start_reply(ctx, msg):
    if msg['status'] != 'ok':
        print('failed to connect to server')
        print('server message: \"{}\"'.format(msg['message']))
        return False
    return True

def client_process_start_reply(ctx, data, address):
    if len(data) <= 8:
        raise Exception('message to short, should header and at least one byte')
    code, length = struct.unpack('>II', data[0:8])
    if code != PROTOCOL_START_REPLY_CODE:
        print('receive invalid start reply message')
        return False
    assert len(data) == length + 8
    print('receive valid start-reply message')
    json_bytes = data[8:length + 8]
    reply_data = json.loads(json_bytes.decode())
    return client_check_start_reply(ctx, reply_data)

def client_process_start_message(ctx):
    msg = msg_create_start_request(ctx)
    print('send start-request message')
    ctx['sk'].sendto(msg, (ctx['args'].addr, PORT))
    data, address = ctx['sk'].recvfrom(4096)
    return client_process_start_reply(ctx, data, address)

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
    ok = client_process_start_message(ctx)
    if not ok:
        print("failed to process start message")
        return

def main():
    ctx = init_ctx()
    if ctx['args'].daemon:
        mode_server(ctx)
    else:
        mode_client(ctx)

if __name__ == '__main__':
    main()
