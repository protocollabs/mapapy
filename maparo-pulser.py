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
import os
import logging

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


# normally one round will be find. But packets
# may get lost for some servers so we send several
# packets to increase robustness. The number of
# transmissions is n, there is no time in between
# the transmission. All packets are send in one
# flush
CTRL_INFO_ROUNDS = 5

# Wait for n seconds after which all received
# messages should be processed. Note that the
# number do not correltate to CTRL_INFO_ROUNDS
CTRL_INFO_TIMEOUT = 1

# wait max n seconds until the reply must be received
# if not resend the message
CTRL_MEASUREMENT_START_TIMEOUT = 1


PROTOCOL_INFO_REQUEST_CODE  = 1
PROTOCOL_INFO_REPLY_CODE    = 2

PROTOCOL_MEASUREMENT_START_REQUEST_CODE = 3
PROTOCOL_MEASUREMENT_START_REPLY_CODE   = 4

PROTOCOL_MEASUREMENT_STOP_REQUEST_CODE = 5
PROTOCOL_MEASUREMENT_STOP_REPLY_CODE   = 6

PROTOCOL_MEASUREMENT_INFO_REQUEST_CODE = 7
PROTOCOL_MEASUREMENT_INFO_REPLY_CODE   = 8

PROTOCOL_RTT_REQUEST_CODE   = 9
PROTOCOL_RTT_REPLY_CODE     = 10


MODULE_UDP_PULSER_NAME = '_udp-pulser'

_INSTANCE_ID = "{}={}".format(socket.gethostname(), uuid.uuid4())

def maparo_id():
    return _INSTANCE_ID

def maparo_date():
    dt = datetime.datetime.utcnow()
    return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')

# XXX strptime seem to consume WAY to much resourches.
# on amd64 box it takes 2 milliseconds
def maparo_date_parse(string):
    return datetime.datetime.strptime(string, '%Y-%m-%dT%H:%M:%S.%f')


class Client(object):

    STATES = enum.Enum('States', 'RTT INFO MEASUREMENT_START MEASUREMENT MEASURMENT_STOP')

    def __init__(self, args, conf):
        self.args = args
        self.conf = conf
        self.rtt_round = 0
        self.rtt_round_max = CTRL_RTT_ROUNDS
        self.info_round = 0
        self.info_round_max = CTRL_INFO_ROUNDS
        self.rtt_db = list()
        self.state = None
        self.loop = asyncio.get_event_loop()
        self.time_diff = args.time_difference
        self.task_rtt_timeout = None
        self.server_db = dict()
        self.dst_addr = None

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
            warn('sequence number not identical')
            return False
        return True

    def ctrl_process_rtt_reply_check_time_sync(self, reply_data, now):
        request_date = maparo_date_parse(reply_data['ts-rp'])
        rtt = now - request_date
        warn("rtt: {}".format(human_timedelta(rtt)))
        time_server = maparo_date_parse(reply_data['ts'])
        ideal_time_server = request_date - (time_server - (rtt / 2.0))
        warn("time delta to server: {}".format(human_timedelta(ideal_time_server)))
        warn("[Note: smaller 0: server clock is before client clock, otherwise behind]")
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
            warn('receive invalid rtt reply message')
            return False
        assert len(data) == length + 8
        warn('receive valid rtt reply message')
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
                warn("Your time difference: {} ms".format(self.time_diff))
                warn(' measuremtn time difference {}'.format(human_timedelta(min_diff)))
                warn(' But will take the given time as difference calculation!')
            else:
                self.time_diff = timedelta_ms(min_diff)
                warn("Calculated time difference: {} ms".format(self.time_diff))
            # ok, last round, now switch to next mode
            asyncio.ensure_future(self.measurment_start())
        return True

    def ctrl_process_info_reply(self, data, addr, now):
        if len(data) <= 8:
            raise Exception('message to short, should header and at least one byte')
        _, length = struct.unpack('>II', data[0:8])
        assert len(data) == length + 8
        warn('receive valid info reply message')
        json_bytes = data[8:length + 8]
        reply_msg = json.loads(json_bytes.decode())
        self.server_db[reply_msg['id']] = reply_msg
        self.server_db[reply_msg['id']]['_addr'] = addr
        return True

    def client_start_request_prep_conf(self):
        d = dict()
        streams = []
        port = int(self.conf['start_port'])
        for i, stream in enumerate(self.conf['streams']):
            entry = dict()
            entry['port'] = str(port)
            entry['stream'] = str(i)
            port += 1
            streams.append(entry)
        d['streams'] = streams
        return d

    def create_measurment_start_msg(self):
        msg = dict()
        msg['id'] = maparo_id()
        msg['seq'] = 0
        msg['measurement'] = dict()
        msg['measurement']['name'] = MODULE_UDP_PULSER_NAME
        msg['measurement']['type'] = 'module'
        msg['measurement']['configuration'] = self.client_start_request_prep_conf()
        #pprint.pprint(msg)
        json_bytes = str.encode(json.dumps(msg))
        b = struct.pack('>II', PROTOCOL_MEASUREMENT_START_REQUEST_CODE, len(json_bytes))
        return b + json_bytes

    def tx_measurment_start(self):
        msg = self.create_measurment_start_msg()
        warn('send measurement start request to {}'.format(self.dst_addr))
        self.ctrl_sock.sendto(msg, self.dst_addr)

    async def ctrl_measurement_start_timeout(self, seconds):
        await asyncio.sleep(seconds)
        warn('server did not respone within {} seconds, retry again now'.format(seconds))
        asyncio.ensure_future(self.measurment_start())

    async def measurment_start(self):
        warn("process MEASURMENT START")
        self.state = Client.STATES.MEASUREMENT_START
        # send measurment start request messages until a
        # a measurement start reply is received.
        self.tx_measurment_start()
        self.task_measurement_start_timeout = asyncio.ensure_future(
                self.ctrl_measurement_start_timeout(CTRL_MEASUREMENT_START_TIMEOUT))

    def ctrl_process_measurement_start_reply(self, data, addr, now):
        # first of all, cancel the timer if one was
        # registered
        assert self.task_measurement_start_timeout != None
        self.task_measurement_start_timeout.cancel()
        if len(data) <= 8:
            raise Exception('message to short, should header and at least one byte')
        code, length = struct.unpack('>II', data[0:8])
        assert len(data) == length + 8
        warn('receive valid measurment start reply message')
        json_bytes = data[8:length + 8]
        reply_msg = json.loads(json_bytes.decode())
        if reply_msg['status'] == 'ok':
            warn('server returned ok, start measurement now')
        else:
            warn('server problem with measurement start, cancel now')
            return False
        return True

    def measurement_tx(self, i):
        print_data = dict()
        addr = self.conf['destination_addr']
        port = self.conf['start_port'] + int(i)
        #msg, seq_no = message(ctx, d, i)
        self.measurement_v4_fd.sendto(bytearray(10), (addr, port))
        #print_data['tx-time'] = high_res_timestamp()
        #print_data['stream'] = stream_name
        #print_data['seq-no'] = seq_no
        #print_data['payload-size'] = len(msg)
        #print_tx(print_data)

    async def measurement_burst_mode(self, i, stream):
        no_packets = stream['bursts-packets']
        for packet_counter in range(0, no_packets):
            self.measurement_tx(i)
            if packet_counter == no_packets - 1:
                # after the last transmission we do not wait
                return
            await asyncio.sleep(stream['burst-intra-time'])

    async def measurement_tx_thread(self, i, stream):
        if 'initial-waittime' in stream:
            await asyncio.sleep(float(stream['initial-waittime']))
        while True:
            await self.measurement_burst_mode(i, stream)
            await asyncio.sleep(stream['burst-inter-time'])

    def init_v4_tx_fd(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s

    def init_v6_tx_fd(self):
        s = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s

    def start_measurement(self):
        self.state = Client.STATES.MEASUREMENT
        self.measurement_v4_fd = self.init_v4_tx_fd()
        self.measurement_v6_fd = self.init_v6_tx_fd()
        for i, stream in enumerate(self.conf['streams']):
            asyncio.ensure_future(self.measurement_tx_thread(i, stream))

    def ctrl_multiplex_msg(self, data, addr, now):
        if len(data) <= 4:
            raise Exception('message to short, should header and at least one byte')
        code = struct.unpack('>I', data[0:4])[0]
        if code == PROTOCOL_RTT_REPLY_CODE:
            if self.state == Client.STATES.RTT:
                ok = self.ctrl_process_rtt_reply(data, addr, now)
                if not ok:
                    return
            else:
                warn('error, receive rtt but not in state rtt processing')
        elif code == PROTOCOL_INFO_REPLY_CODE:
            if self.state == Client.STATES.INFO:
                ok = self.ctrl_process_info_reply(data, addr, now)
                if not ok:
                    return
            else:
                warn('error, receive rtt but not in state rtt processing')
        elif code == PROTOCOL_MEASUREMENT_START_REPLY_CODE:
            if self.state == Client.STATES.MEASUREMENT_START:
                ok = self.ctrl_process_measurement_start_reply(data, addr, now)
                if not ok:
                    self.finish()
                    return
                self.start_measurement()
            else:
                warn('error, receive measurment start but not in current state')
        else:
            warn('receive unknown message [type: {}]'.format(code))


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
        warn('server did not respone within {} seconds, retry again now'.format(seconds))
        asyncio.ensure_future(self.tx_msg_rtt(self.rtt_round))

    async def tx_msg_rtt(self, seq):
        msg, meta = self.msg_create_rtt_request(seq)
        self.ctrl_sock.sendto(msg, self.dst_addr)
        self.state = Client.STATES.RTT
        self.state_ctx = meta
        # ok, wait maximum 3 seconds for reply messages
        self.task_rtt_timeout = asyncio.ensure_future(self.ctrl_rtt_timeout(CTRL_RTT_TIMEOUT))

    def msg_create_info_request(self, info_round):
        msg = dict()
        msg['id'] = maparo_id()
        msg['seq'] = info_round
        msg['ts'] = maparo_date()
        json_bytes = str.encode(json.dumps(msg))
        b = struct.pack('>II', PROTOCOL_INFO_REQUEST_CODE, len(json_bytes))
        return b + json_bytes, msg

    async def ctrl_info_timeout(self):
        warn('wait {} seconds for info reply messages'.format(CTRL_INFO_TIMEOUT))
        await asyncio.sleep(CTRL_INFO_TIMEOUT)
        warn('info timeout reached, now process all received info replies')
        maparo_servers = len(self.server_db)
        if maparo_servers > 1:
            warn('{} maparo servers discovered'.format(maparo_servers))
            for k, v in self.server_db.items():
                warn(k)
        elif maparo_servers == 1:
            k = list(self.server_db.keys())[0]
            v = self.server_db[k]
            warn('exactly one maparo server discovered - great')
            warn("ID: {}".format(v['id']))
            warn("Arch: {}".format(v['arch']))
            warn("OS: {}".format(v['os']))
            warn("Banner: {}".format(v['banner']))
            warn("Address: {}".format(v['_addr']))
            self.dst_addr = v['_addr']
            asyncio.ensure_future(self.tx_msg_rtt(self.rtt_round))
        else:
            warn('no maparo server discovered, giving up')
            self.finish()

    async def tx_msg_info(self, info_round):
        msg, meta = self.msg_create_info_request(info_round)
        self.ctrl_sock.sendto(msg, self.ctrl_addr)
        self.state = Client.STATES.INFO

    def print_pre_info(self):
        warn('control address: {}'.format(self.args.ctrl_addr))
        warn('data address: {}'.format(self.args.addr))

    def run(self):
        self.print_pre_info()
        self.init_ctrl()
        for i in range(self.info_round_max):
            asyncio.ensure_future(self.tx_msg_info(self.info_round))
            self.info_round += 1
        asyncio.ensure_future(self.ctrl_info_timeout())
        try:
            self.loop.run_forever()
        finally:
            self.loop.close()



class Server(object):

    def __init__(self, args):
        self.args = args
        self.info_seq = 0
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

    def ctrl_create_info_reply(self, msg):
        msg = dict()
        msg['id'] = maparo_id()
        msg['seq'] = self.info_seq; self.info_seq += 1
        msg['seq-rp'] = msg['seq']
        msg['arch'] = 'unknown'
        msg['os'] = 'unknown'
        msg['banner'] = "{}/{}".format(os.path.basename(__file__), sys.version)
        msg['modules'] = { MODULE_UDP_PULSER_NAME : {} }
        json_bytes = str.encode(json.dumps(msg))
        b = struct.pack('>II', PROTOCOL_INFO_REPLY_CODE, len(json_bytes))
        return b + json_bytes

    def ctrl_process_info_request_pkt(self, data):
        json_len = struct.unpack('>I', data[4:8])[0]
        if len(data) - (4 + 4) < json_len:
            return False, None
        json_bytes = data[8:json_len + 8]
        msg = json.loads(json_bytes.decode())
        return True, msg

    def ctrl_process_info_request(self, fd, data, addr):
        ok, reqest_msg = self.ctrl_process_info_request_pkt(data)
        if not ok:
            return
        reply_msg = self.ctrl_create_info_reply(reqest_msg)
        fd.sendto(reply_msg, addr)

    def init_v4_rx_fd(self, port):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        s.setblocking(False)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(s, "SO_REUSEPORT"):
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        s.bind(('', int(port)))
        return s

    def handle_pulses(self, port, now, msg):
        warn('handle pulse')

    def srv_cb_v4_rx(self, fd, port):
        try:
            msg, addr = fd.recvfrom(16600)
            now = maparo_date()
        except socket.error as e:
            warn('Expection')
        self.handle_pulses(port, now, msg)

    def srv_msg_start_request_process(self, request_data):
        module_name = request_data['measurement']['name']
        assert request_data['measurement']['type'] == 'module'
        if module_name != MODULE_UDP_PULSER_NAME:
            return False, 'module ({}) not supported'.format(module_name)
        self.db_srv = dict()
        for entry in request_data['measurement']['configuration']['streams']:
            port = entry['port']
            self.db_srv[port] = dict()
            self.db_srv[port]['name'] = entry['stream']
            self.db_srv[port]['fd'] = self.init_v4_rx_fd(port)
            self.db_srv[port]['sequence-expected'] = 0
            self.db_srv[port]['packets-reqeived'] = 0
            self.db_srv[port]['bytes-received'] = 0
            fd = self.db_srv[port]['fd']
            self.loop.add_reader(fd, functools.partial(self.srv_cb_v4_rx, fd, port))
        return True, ""

    def ctrl_process_measurement_start_request_pkt(self, data):
        json_len = struct.unpack('>I', data[4:8])[0]
        if len(data) - (4 + 4) < json_len:
            return False, None
        json_bytes = data[8:json_len + 8]
        msg = json.loads(json_bytes.decode())
        return True, msg

    def ctrl_create_measurement_start_request_reply(self, msg_request, ok, err_msg):
        msg = dict()
        msg['id'] = maparo_id()
        msg['seq'] = self.info_seq; self.info_seq += 1
        msg['seq-rp'] = msg_request['seq']
        if not ok:
            # return with failure
            msg['status'] = 'failed'
            msg['message'] = err_msg
        else:
            # return with success
            msg['status'] = 'ok'
        json_bytes = str.encode(json.dumps(msg))
        b = struct.pack('>II', PROTOCOL_MEASUREMENT_START_REPLY_CODE, len(json_bytes))
        return b + json_bytes

    def ctrl_process_measurement_start_request(self, fd, data, addr):
        ok, reqest_msg = self.ctrl_process_measurement_start_request_pkt(data)
        if not ok:
            return
        ok, msg = self.srv_msg_start_request_process(reqest_msg)
        reply_msg = self.ctrl_create_measurement_start_request_reply(reqest_msg, ok, msg)
        fd.sendto(reply_msg, addr)

    def ctrl_multiplex(self, name, fd):
        try:
            data, addr = fd.recvfrom(CTRL_DGRAM_BUF_SIZE)
        except:
            raise
        if len(data) <= 4:
            warn('message to short, should header and at least one byte')
            return
        code = struct.unpack('>I', data[0:4])[0]
        if code == PROTOCOL_RTT_REQUEST_CODE:
            self.ctrl_process_rtt_request(fd, data, addr)
        elif code == PROTOCOL_INFO_REQUEST_CODE:
            self.ctrl_process_info_request(fd, data, addr)
        elif code == PROTOCOL_MEASUREMENT_START_REQUEST_CODE:
            self.ctrl_process_measurement_start_request(fd, data, addr)
        else:
            warn('unknown message type: {}'.format(code))
        #fd.sendto(bytearray(11), addr)
        #warn(len(data))

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
        warn("client mode but no --configuration <file> given ...")
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

def warn(msg):
    logging.warning(msg)

def info(msg):
    logging.info(msg)

def debug(msg):
    logging.debug(msg)

def init_logging(args):
    levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    level = levels[min(len(levels) - 1, args.verbose)]
    logging.basicConfig(level=level, format="%(message)s")

def main():
    is_server, args, conf = init_ctx()
    init_logging(args)
    warn('maparo-pulser©')
    if is_server:
        handle = Server(args)
    else:
        handle = Client(args, conf)
    handle.run()

if __name__ == '__main__':
    main()
