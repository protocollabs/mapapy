#!/usr/bin/env python3

import sys
import argparse
import datetime
import json
import pprint

def maparo_date_parse(string):
    return datetime.datetime.strptime(string, '%Y-%m-%dT%H:%M:%S.%f')

def check_data(data):
    if not 'client' in data:
        raise Exception('no client data in data')
    if not 'server' in data:
        raise Exception('no server data in data')
    if not 'header' in data:
        raise Exception('no header data in data')
    if not 'module' in data['header']:
        raise Exception('no module key in header data')
    if data['header']['module'] not in ('_udp-pulser', 'udp-pulser'):
        raise Exception('not valid udp-pulser data')
    if not 'time-diff' in data['header']:
        raise Exception('no time-diff key in header data')
    if not 'time-diff-unit' in data['header']:
        raise Exception('no time-diff-unit key in header data')
    if data['header']['time-diff-unit'] != 'ms':
        raise Exception('time diff unit not ms')

def db_container():
    d = dict()
    d['client'] = None
    d['server'] = None
    d['template'] = None
    return d

def db_entry(data):
    d = dict()
    d['time'] = maparo_date_parse(data['time'])
    if 'payload-size' in data:
        d['payload-size'] = data['payload-size']
    return d

def correlate_data(data):
    db = dict()
    # client side
    for stream_id, stream_data in data['client']['stream'].items():
        if not stream_id in db:
            db[stream_id] = dict()
        for stream_entry in stream_data:
            # FIXME: this script handled no duplicated packets
            # nor on sender nor on receiver side.
            seq_no = stream_entry['seq-no']
            assert(seq_no not in db[stream_id])
            if not seq_no in db[stream_id]:
                db[stream_id][seq_no] = db_container()
            db[stream_id][seq_no]['client'] = db_entry(stream_entry)
    # server side
    for stream_id, stream_data in data['server']['stream'].items():
        if not stream_id in db:
            db[stream_id] = dict()
        for stream_entry in stream_data:
            # FIXME: this script handled no duplicated packets
            # nor on sender nor on receiver side.
            seq_no = stream_entry['seq-no']
            if not seq_no in db[stream_id]:
                db[stream_id][seq_no] = db_container()
            db[stream_id][seq_no]['server'] = db_entry(stream_entry)
    return db

def data_stats(data):
    stats = dict()

def normalize(data, raw_data):
    """ normalize time by adding (possible negative) recorded delta to server time"""
    time_diff_ms = raw_data['header']['time-diff']
    time_diff = datetime.timedelta(milliseconds=time_diff_ms)
    for stream_id, stream_data in data.items():
        for seq_no in sorted(stream_data):
            entry = stream_data[seq_no]
            # make a copy of "original" time
            entry['server']['time-unmodified'] = entry['server']['time']
            entry['server']['time'] = entry['server']['time'] + time_diff


def print_correlated(correlated):
    for stream_id, stream_data in correlated.items():
        print('Stream: {}'.format(stream_id))
        for seq_no in sorted(stream_data):
            print('  seq: {}'.format(seq_no))
            entry = stream_data[seq_no]
            diff = entry['server']['time'] - entry['client']['time']
            print("    client -> [{:.3f} ms] -> server".format(diff.total_seconds() * 1000.0))

def process(data):
    check_data(data)
    correlated = correlate_data(data)
    normalize(correlated, data)
    print_correlated(correlated)
    stats = data_stats(correlated)

def stdin_read():
    d = ''
    for line in sys.stdin:
        d += line
    return json.loads(d)

def main():
    data = stdin_read()
    process(data)

if __name__ == '__main__':
    main()
