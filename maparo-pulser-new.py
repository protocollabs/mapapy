#!/usr/bin/env python3

import asyncio
import socket
import sys
import argparse
import struct
import uuid
import datetime
import json
import pprint
import functools
import time
import ipaddress
import enum

CTRL_MCAST_ADDR_V4 = '224.0.0.1'
CTRL_MCAST_ADDR_V6 = 'FF02::1'

# don't recognize own mcast transmissions
# by default, can be changed for debugging
CTRL_MCAST_LOOP = 0

CTRL_MCAST_TTL = 1

CTRL_PORT = 64321

CTRL_DGRAM_BUF_SIZE = 4096

# how many probes to send within a row to
# probe for time differences between client
# and server.
CTRL_RTT_ROUNDS = 5

# If server did not answer, retry within
# n seconds again.
CTRL_RTT_TIMEOUT = 3

PROTOCOL_RTT_REQUEST_CODE   = 1
PROTOCOL_RTT_REPLY_CODE     = 2
PROTOCOL_INFO_REQUEST_CODE  = 3
PROTOCOL_INFO_REPLY_CODE    = 4
PROTOCOL_START_REQUEST_CODE = 5
PROTOCOL_START_REPLY_CODE   = 6

MODULE_UDP_PULSER_NAME = '_udp-pulser'

def maparo_id():
    return "{}={}".format(socket.gethostname(), uuid.uuid4())

def maparo_date():
    dt = datetime.datetime.utcnow()
    return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')

# XXX strptime seem to consume WAY to much resourches.
# on amd64 box it takes 2 milliseconds
def maparo_date_parse(string):
    return datetime.datetime.strptime(string, '%Y-%m-%dT%H:%M:%S.%f')


class Client(object):

    STATES = enum.Enum('States', 'RTT INFO MODULE_START MODULE_STOP')

    def __init__(self, args):
        self.args = args
        self.rtt_round = 0
        self.rtt_round_max = CTRL_RTT_ROUNDS
        self.rtt_db = list()
        self.state = None
        self.loop = asyncio.get_event_loop()
        self.time_diff = args.time_difference
        self.task_rtt_timeout = None

    def init_ctrl_mcast(self, addr):
        if addr.version == 4:
            proto = socket.AF_INET
        else:
            proto = socket.AF_INET6
        sock = socket.socket(proto, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, CTRL_MCAST_TTL)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return sock

    def init_ctrl_unicast(self, addr):
        if addr.version == 4:
            proto = socket.AF_INET
        else:
            proto = socket.AF_INET6
        sock = socket.socket(proto, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return sock

    def ctrl_process_rtt_reply_check(self, reply_data):
        if reply_data['seq-rp'] != self.state_ctx['seq']:
            print('sequence number not identical')
            return False
        return True

    def ctrl_process_rtt_reply_check_time_sync(self, reply_data, now):
        request_date = maparo_date_parse(reply_data['ts-rp'])
        rtt = now - request_date
        print("rtt: {}".format(human_timedelta(rtt)))
        time_server = maparo_date_parse(reply_data['ts'])
        ideal_time_server = request_date - (time_server - (rtt / 2.0))
        print("time delta to server: {}".format(human_timedelta(ideal_time_server)))
        print("[Note: smaller 0: server clock is before client clock, otherwise behind]")
        self.rtt_db.append((rtt, ideal_time_server))

    def ctrl_process_rtt_reply(self, data, addr, now):
        # first of all, cancel the timer if one was
        # registered
        assert self.task_rtt_timeout != None
        self.task_rtt_timeout.cancel()
        if len(data) <= 8:
            raise Exception('message to short, should header and at least one byte')
        code, length = struct.unpack('>II', data[0:8])
        if code != PROTOCOL_RTT_REPLY_CODE:
            print('receive invalid rtt reply message')
            return False
        assert len(data) == length + 8
        print('receive valid rtt reply message')
        json_bytes = data[8:length + 8]
        reply_msg = json.loads(json_bytes.decode())
        ok = self.ctrl_process_rtt_reply_check(reply_msg)
        if not ok:
            return False
        self.ctrl_process_rtt_reply_check_time_sync(reply_msg, now)
        if self.rtt_round < self.rtt_round_max - 1:
            self.rtt_round += 1
            asyncio.ensure_future(self.tx_msg_rtt(self.rtt_round))
        else:
            # ok, we now check for the smallest diff, this is probably
            # now the sanest way to do, but what is a better way anyway?
            # Take the diff where the the rtt is the smallest?
            min_diff_abs = None
            min_diff = None
            for i in self.rtt_db:
                if min_diff_abs == None or min_diff_abs > abs(i[1]):
                    min_diff_abs = abs(i[1])
                    min_diff = i[1]
            if self.time_diff:
                print("Your time difference: {} ms".format(self.time_diff))
                print(' measuremtn time difference {}'.format(human_timedelta(min_diff)))
                print(' But will take the given time as difference calculation!')
            else:
                self.time_diff = timedelta_ms(min_diff)
                print("Calculated time difference: {} ms".format(self.time_diff))

        return True


    def ctrl_multiplex_msg(self, data, addr, now):
        if len(data) <= 4:
            raise Exception('message to short, should header and at least one byte')
        code = struct.unpack('>I', data[0:4])[0]
        if code == PROTOCOL_RTT_REPLY_CODE and self.state == Client.STATES.RTT:
            ok = self.ctrl_process_rtt_reply(data, addr, now)
            if not ok:
                return
        else:
            print('unexpected message')


    def ctrl_multiplex(self, fd):
        try:
            data, addr = fd.recvfrom(CTRL_DGRAM_BUF_SIZE)
            now = datetime.datetime.utcnow()
            ok = self.ctrl_multiplex_msg(data, addr, now)
            if not ok:
                return
        except:
            raise

    def finish(self):
        self.loop.stop()

    def init_ctrl(self):
        # now check for ctrl channel
        if self.args.ctrl_addr != None:
            addr = ipaddress.ip_address(self.args.ctrl_addr)
        else:
            addr = ipaddress.ip_address(self.args.addr)
        is_mcast = addr.is_multicast
        self.ctrl_addr = (str(addr), CTRL_PORT)
        if is_mcast:
            self.ctrl_sock = self.init_ctrl_mcast(addr)
        else:
            self.ctrl_sock = self.init_ctrl_unicast(addr)
        self.loop.add_reader(self.ctrl_sock,
                functools.partial(self.ctrl_multiplex, self.ctrl_sock))

    def msg_create_rtt_request(self, seq):
        msg = dict()
        msg['id'] = maparo_id()
        msg['seq'] = seq
        msg['ts'] = maparo_date()
        json_bytes = str.encode(json.dumps(msg))
        b = struct.pack('>II', PROTOCOL_RTT_REQUEST_CODE, len(json_bytes))
        return b + json_bytes, msg

    async def ctrl_rtt_timeout(self, seconds):
        await asyncio.sleep(seconds)
        print('server did not respone within {} seconds, retry again now'.format(seconds))
        asyncio.ensure_future(self.tx_msg_rtt(self.rtt_round))


    async def tx_msg_rtt(self, seq):
        msg, meta = self.msg_create_rtt_request(seq)
        self.ctrl_sock.sendto(msg, self.ctrl_addr)
        self.state = Client.STATES.RTT
        self.state_ctx = meta
        # ok, wait maximum 3 seconds for reply messages
        self.task_rtt_timeout = asyncio.ensure_future(self.ctrl_rtt_timeout(CTRL_RTT_TIMEOUT))


    def run(self):
        self.init_ctrl()
        asyncio.ensure_future(self.tx_msg_rtt(self.rtt_round))
        try:
            self.loop.run_forever()
        finally:
            self.loop.close()



class Server(object):

    def __init__(self, args, conf):
        self.args = args
        self.conf = conf
        self.loop = asyncio.get_event_loop()

    def run(self):
        self.init_ctrl_channels()
        try:
            self.loop.run_forever()
        finally:
            self.loop.close()

    def init_ctrl_channel_v6_mcast(self):
        sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(sock, "SO_REUSEPORT"):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_MULTICAST_LOOP, CTRL_MCAST_LOOP)
        sock.bind(('', CTRL_PORT))
        group = canonical(sock, CTRL_MCAST_ADDR_V6)
        iface = canonical(sock, '')
        struct = socket.inet_pton(sock.family, group) + \
                 socket.inet_pton(sock.family, iface)
        sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_JOIN_GROUP, struct)
        return sock

    def init_ctrl_channel_v4_mcast(self, addr=None):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(sock, "SO_REUSEPORT"):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, CTRL_MCAST_LOOP)
        #sock.bind(('', CTRL_PORT))
        sock.bind((CTRL_MCAST_ADDR_V4, CTRL_PORT))
        host = socket.gethostbyname(socket.gethostname())
        sock.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_IF, socket.inet_aton(host))
        sock.setsockopt(socket.SOL_IP, socket.IP_ADD_MEMBERSHIP,
                        socket.inet_aton(CTRL_MCAST_ADDR_V4) + socket.inet_aton(host))
        return sock

    def ctrl_process_rtt_request_pkt(self, data):
        json_len = struct.unpack('>I', data[4:8])[0]
        if len(data) - (4 + 4) < json_len:
            return False, None
        json_bytes = data[8:json_len + 8]
        msg = json.loads(json_bytes.decode())
        return True, msg

    def ctrl_create_rtt_reply(self, msg):
        reply_data = dict()
        reply_data['id'] = maparo_id()
        reply_data['seq-rp'] = msg['seq']
        reply_data['ts-rp'] = msg['ts']
        reply_data['ts'] = maparo_date()
        json_bytes = str.encode(json.dumps(reply_data))
        b = struct.pack('>II', PROTOCOL_RTT_REPLY_CODE, len(json_bytes))
        return b + json_bytes

    def ctrl_process_rtt_request(self, fd, data, addr):
        ok, msg = self.ctrl_process_rtt_request_pkt(data)
        if not ok:
            return
        reply_data = self.ctrl_create_rtt_reply(msg)
        fd.sendto(reply_data, addr)

    def ctrl_multiplex(self, name, fd):
        try:
            data, addr = fd.recvfrom(CTRL_DGRAM_BUF_SIZE)
        except:
            raise
        if len(data) <= 4:
            print('message to short, should header and at least one byte')
            return
        code = struct.unpack('>I', data[0:4])[0]
        if code == PROTOCOL_RTT_REQUEST_CODE:
            self.ctrl_process_rtt_request(fd, data, addr)
        else:
            print('unknown message type: {}'.format(code))
        #elif code == PROTOCOL_INFO_REQUEST_CODE:
        #    srv_msg_info_request(ctx, data, address)
        #fd.sendto(bytearray(11), addr)
        #print(len(data))

    def init_ctrl_channels(self):
        """ open a unicast and multicast udp server socket """
        # INFO: https://github.com/mcfletch/mcastsocket/blob/master/mcastsocket/mcastsocket.py
        fd = self.init_ctrl_channel_v4_mcast()
        self.loop.add_reader(fd, functools.partial(self.ctrl_multiplex, "v4-mcast", fd))
        fd = self.init_ctrl_channel_v6_mcast()
        self.loop.add_reader(fd, functools.partial(self.ctrl_multiplex, "v6-mcast", fd))


def timedelta_ms(timedelta):
    return timedelta.total_seconds() * 1000.0

def human_timedelta(timedelta):
    return '{0:.3f} ms'.format(timedelta_ms(timedelta))

def canonical(sock, ip):
    family = getattr(sock, 'family', sock)
    if family == socket.AF_INET6:
        if ip == '':
            ip = '::'
    else:
        if ip == '':
            ip = '0.0.0.0'
    try:
        return socket.inet_ntop(family, socket.inet_pton(family, ip))
    except Exception as err:
        err.args += (sock, ip)
        raise

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--configuration", help="configuration", type=str, default=None)
    parser.add_argument("--daemon", action='store_true', default=False)
    parser.add_argument('--addr', action="store",  type=str, default="::1")
    parser.add_argument('--ctrl-addr', action="store",  type=str, default=None)
    parser.add_argument('--time-difference', action="store",  type=float, default=None)
    parser.add_argument("-v", "--verbose", action="count", default=0,
                        help="increase output verbosity")
    args = parser.parse_args()
    return args

def load_configuration_file(args):
    if not args.configuration:
        print("client mode but no --configuration <file> given ...")
        sys.exit(0)
    config = dict()
    exec(open(args.configuration).read(), None, config)
    return config

def init_ctx():
    args = parse_args()
    if not args.daemon:
        conf = load_configuration_file(args)
        return False, args, conf
    return True, args, None

def main():
    is_server, args, conf = init_ctx()
    if is_server:
        handle = Server(args, conf)
    else:
        handle = Client(args)
    handle.run()

if __name__ == '__main__':
    main()
