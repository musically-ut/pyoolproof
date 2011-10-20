#!/usr/bin/env python

import subprocess
import time
import zmq
import random
import sys

g_command_port = 'ipc://command'
g_TO = 1        # Time between heartbeats


def main():
    """Send the kill command to the monitor"""
    ctx = zmq.Context()

    out_cmd = ctx.socket(zmq.PUSH)
    out_cmd.connect(g_command_port)
    out_cmd.send('')


if __name__ == '__main__':
    main()
