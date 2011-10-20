#!/usr/bin/env python

import subprocess
import time
import zmq
import random
import sys


g_heartbeat_addr = 'ipc://heartbeat'
g_TO = 1        # Time between heartbeats


def main():
    """A program which randomly dies, and produces an erratic heartbeat."""
    ctx = zmq.Context()

    out_hb = ctx.socket(zmq.PUSH)
    out_hb.connect(g_heartbeat_addr)

    while True:
        r = random.random() * g_TO * 3 # Uniformly distributed over 4 Time out periods
        print '[naughty_worker] Working (sleeping) for ' + str(r) + ' seconds.'
        time.sleep(r)
        out_hb.send('')

        if random.random() < 0.1: # With 10% chance, just crash
            print '[naughty_worker] Randomly crashing ...'
            sys.exit(-1)

        if random.random() < 0.1: 
            print '[naughty_worker] Enough work ...'
            sys.exit(0)


if __name__ == '__main__':
    main()
