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

CTRL_MCAST_ADDR_V4 = '224.0.0.1'
CTRL_MCAST_ADDR_V6 = 'FF02::1'

# don't recognize own mcast transmissions
# by default, can be changed for debugging
CTRL_MCAST_LOOP = 0

CTRL_MCAST_TTL = 1

CTRL_PORT = 64321

CTRL_DGRAM_BUF_SIZE 4096

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


class Client(object):

    STATES = [ 'RTT1', 'RTT2', 'INFO', 'MODULE-START', 'MODULE-STOP' ]

    def __init__(self, args):
        self.args = args
        self.state = None
        self.loop = asyncio.get_event_loop()

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

    def ctrl_multiplex(self, fd):
        try:
            print("receive control data")
            data, addr = fd.recvfrom(CTRL_DGRAM_BUF_SIZE)
        except:
            raise

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

    async def main_loop(self):
        while True:
            self.ctrl_sock.sendto(bytearray(10), self.ctrl_addr)
            await asyncio.sleep(1.)
            print("wait")

    def run(self):
        self.init_ctrl()
        asyncio.ensure_future(self.main_loop())
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
        #mreq = struct.pack("4sl", socket.inet_aton(addr), socket.INADDR_ANY)
        #sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        sock.setsockopt(socket.SOL_IP, socket.IP_ADD_MEMBERSHIP,
                        socket.inet_aton(CTRL_MCAST_ADDR_V4) + socket.inet_aton(host))
        return sock

    def ctrl_multiplex(self, name, fd):
        try:
            data, addr = fd.recvfrom(CTRL_DGRAM_BUF_SIZE)
        except:
            raise
        fd.sendto(bytearray(11), addr)
        print(len(data))

    def init_ctrl_channels(self):
        """ open a unicast and multicast udp server socket """
        # INFO: https://github.com/mcfletch/mcastsocket/blob/master/mcastsocket/mcastsocket.py
        fd = self.init_ctrl_channel_v4_mcast()
        self.loop.add_reader(fd, functools.partial(self.ctrl_multiplex, "v4-mcast", fd))
        fd = self.init_ctrl_channel_v6_mcast()
        self.loop.add_reader(fd, functools.partial(self.ctrl_multiplex, "v6-mcast", fd))






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
