#!/usr/bin/env python3

import sys
import argparse
import datetime
import json
import pprint

import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
from pylab import *
import numpy as np


class SVG:
    def __init__(self):
        self.d = '<svg xmlns="http://www.w3.org/2000/svg">'
        self.load_defs()

    def load_defs(self):
        self.raw('<defs>')
        self.raw('  <marker id="head" orient="auto" markerWidth="2" markerHeight="4" refX="0.1" refY="2">')
        self.raw('    <path d="M0,0 V4 L2,2 Z" fill="black" />')
        self.raw('  </marker>')
        self.raw('</defs>')

    def raw(self, s):
        self.d += '\t{}\n'.format(s)

    def line(self, x1, y1, x2, y2, stroke="#000", stroke_width="1", marker_end="url(#head)"):
        self.raw('<line x1="{}" y1="{}" x2="{}" y2="{}" stroke="{}" stroke-width="{}" marker-end="{}" />'.format(
            x1, y1, x2, y2, stroke, stroke_width, marker_end))

    def write(self, filepath="drawing.svg"):
        self.d += "\n</svg>\n"
        with open(filepath, 'w') as fd:
            fd.write(self.d)


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

def correlate(data):
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

def statistics(data):
    stats = dict()
    stats['time-min'] = datetime.datetime(5000, 1, 1, 13, 37)
    stats['time-max'] = datetime.datetime(   1, 1, 1, 13, 37)
    stats['duration-min'] = datetime.timedelta(1000, 0, 0)
    stats['duration-max'] = datetime.timedelta(0, 0, 0)
    stats['duration-avg'] = 0
    stats['tx-packets'] = 0
    stats['rx-packets'] = 0
    for stream_id, stream_data in data.items():
        for seq_no in sorted(stream_data):
            entry = stream_data[seq_no]
            assert(entry['client']) # failure if received but not transmitted
            stats['tx-packets'] += 1
            if not entry['server']:
                print("packet loss")
                continue
            stats['rx-packets'] += 1
            stats['time-min'] = min(stats['time-min'], entry['server']['time'])
            stats['time-min'] = min(stats['time-min'], entry['client']['time'])

            stats['time-max'] = max(stats['time-max'], entry['server']['time'])
            stats['time-max'] = max(stats['time-max'], entry['client']['time'])

            duration = entry['server']['time'] - entry['client']['time']
            duration_sec = duration.total_seconds()
            if duration_sec < 0:
                print("packet was received before it was transmited")
                print(" stream: {}, sequence number: {}".format(stream_id, seq_no))
            stats['duration-min'] = min(stats['duration-min'], duration)
            stats['duration-max'] = max(stats['duration-max'], duration)

            stats['duration-avg'] += duration_sec

    stats['duration-avg'] /= stats['rx-packets']

    print("Measurement Duration: {} seconds".format( (stats['time-max'] - stats['time-min']).total_seconds()))
    print("Transmission Duration Min: {} ms".format(stats['duration-min'].total_seconds() * 1000.0))
    print("Transmission Duration Max: {} ms".format(stats['duration-max'].total_seconds() * 1000.0))
    print("Transmission Duration Avg: {} ms".format(stats['duration-avg'] * 1000.0))
    print("Date Min: {}".format(stats['time-min']))
    print("Date Max: {}".format(stats['time-max']))
    print("Packets Transmitted: {}".format(stats['tx-packets']))
    print("Packets Received: {}".format(stats['rx-packets']))
    return stats



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

def offset(timedelta):
    margin_top = 20
    return timedelta.total_seconds() * 20.0 + margin_top

def paint(data, stats):
    svg = SVG()
    for stream_id, stream_data in data.items():
        #print('Stream: {}'.format(stream_id))
        for seq_no in sorted(stream_data):
            #print('  seq: {}'.format(seq_no))
            entry = stream_data[seq_no]
            y1 = offset(entry['client']['time'] - stats['time-min'])
            y2 = offset(entry['server']['time'] - stats['time-min'])
            svg.line(20, y1, 500, y2)
            #print("    client -> [{:.3f} ms] -> server".format(diff.total_seconds() * 1000.0))
    svg_path = 'drawing.svg'
    print("save SVG image to {}".format(svg_path))
    svg.write(filepath=svg_path)

def graph_delay(data, stats):
    fig = plt.figure()
    subplots_adjust(hspace=0.000)
    number_of_subplots = len(data.items())
    v = 1
    for stream_id, stream_data in data.items():
        x = list(); y = list()
        for seq_no in sorted(stream_data):
            entry = stream_data[seq_no]

            # packet from start measurement to start packet transmission
            start_start_delta = entry['server']['time'] - stats['time-min']
            start_start_delta_sec = start_start_delta.total_seconds()
            x.append(start_start_delta_sec)

            # packet duration
            duration = entry['server']['time'] - entry['client']['time']
            duration_sec = duration.total_seconds()
            y.append(duration_sec)

        ax1 = fig.add_subplot(number_of_subplots, 1, v)
        #ax1.bar(x, y, width=0.05)
        ax1.set_ylim(0, stats['duration-max'].total_seconds())
        ax1.plot(x, y)
        ax1.scatter(x, y, color='blue')
        v += 1
    svg_path = 'graph-delay.png'
    print("save delay graph to {}".format(svg_path))
    fig.savefig(svg_path, bbox_inches='tight')

def graph_histogram(data, stats):
    bins = 30
    x = list()
    for stream_id, stream_data in data.items():
        for seq_no in sorted(stream_data):
            entry = stream_data[seq_no]
            duration = entry['server']['time'] - entry['client']['time']
            duration_sec = duration.total_seconds()
            x.append(duration_sec * 1000)
    fig = plt.figure()
    plt.hist(x, bins)
    plt.xlabel('Packet Delay [ms]')
    svg_path = 'graph-histogram.png'
    print("save historgram graph to {}".format(svg_path))
    fig.savefig(svg_path, bbox_inches='tight')

def graph(data, stats):
    graph_delay(data, stats)
    graph_histogram(data, stats)

def process(data):
    check_data(data)
    correlated = correlate(data)
    normalize(correlated, data)
    #print_correlated(correlated)
    stats = statistics(correlated)
    paint(correlated, stats)
    graph(correlated, stats)

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
