#!/usr/bin/env python

import subprocess
import sys
import time
import signal
import zmq
import traceback
import argparse

def _kill(p):
    """Kills the process and returns True if it succeeded,
    False if there was a problem."""

    try:
        p.kill()
        return True
    except OSError:
        return False

def main(exec_cmd, g_TO, g_N, g_heartbeat_addr, g_command_port):
    """This runs the given application in a wrapper which restarts
    it on any of the following signs:
    <ul>
        <li> It misses 1 heart beat. </li>
        <li> It crashes with a non-zero error code. </li>
        <li> An external command requests so. </li>
    </ul>

    @param exec_cmd The command line of the executable.
    @param g_TO The period for the heartbeats (in sec)
    @param g_N The number of heartbeats which can be missed
    @param g_heartbeat_addr Port to bind to for listening to heartbeats
    @param g_command_port Port to bind to to receive the kill command

    @returns The error code returned by the application, or None.
    """

    ctx = zmq.Context()

    # Connection to the heartbeats
    in_hb = ctx.socket(zmq.PULL)
    in_hb.bind(g_heartbeat_addr)

    # Connection to the outside world
    in_cmd = ctx.socket(zmq.PULL)
    in_cmd.bind(g_command_port)

    retcode = 1
    
    # Assuming the command string is program name followed by arguments.
    exec_prog_args = [elem for elem in exec_cmd.split() if len(elem) > 0]

    while retcode:  # While the process does not exit cleanly
        print
        print '[monitor] Running ' + exec_cmd + ' ... '

        # Disable catching Ctrl-C by the subprocess module 
        # Passing it to the application instead.
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        try:
            p = subprocess.Popen(exec_prog_args)
        except OSError as e:
            print '*** Popen exception: ' + str(e)
            return None
        
        end_time = time.time() + 2 * g_TO
        all_is_well = True
        missed_TO = 0

        while True:
            try:
                (r, w, e) = zmq.select(
                        [in_hb, in_cmd], [], [], timeout=end_time - time.time())

                for sock in r:
                    msg = sock.recv() 

                    if sock == in_hb:
                        print '[monitor] Got heartbeat.'
                        end_time = time.time() + g_TO
                        missed_TO = 0
                    elif sock == in_cmd:
                        print '[monitor] Order to execute'
                        all_is_well = False
                        retcode = 'Killed by order'

                if end_time <= time.time():
                    end_time = time.time() + g_TO
                    print '[monitor] Missing a timeout ...'
                    missed_TO += 1

                if missed_TO >= g_N:
                    print '[monitor] Killing off the process ...'
                    retcode = 'Missed ' + str(missed_TO) + ' heartbeats'
                    all_is_well = False

            except zmq.ZMQError as e:
                print '[monitor] *** ZMQ had a problem: ' + str(e)
                traceback.print_exc()

            if not all_is_well:
                if not _kill(p):
                    print '[monitor] Probably already dead.'
                break 

            retcode = p.poll() # Check on the application

            if retcode is not None: # The application exited
                break

        # Re-enable Ctrl-C handler to exit this wrapper
        signal.signal(signal.SIGINT, signal.default_int_handler)

        if retcode:
            print >> sys.stderr, 'Error = ' + str(retcode) + '\n'
            time.sleep(1)

    print "[monitor] Exiting with return code 0"
    return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
            description='Wrapper for reliably executing programs within.')
    parser.add_argument(
            'exec_program',
            help='the program to run in this wrapper')
    parser.add_argument(
            '--heartbeat-addr',
            '-b',
            default='ipc://heartbeat',
            help='address for receiving heartbeats')
    parser.add_argument(
            '--command-addr',
            '-c',
            default='ipc://command',
            help='address for receiving kill/restart command')
    parser.add_argument(
            '--heartbeat-timeout',
            '-t',
            type=float,
            default=1,
            help='period (in sec) of the heartbeats given by the program')
    parser.add_argument(
            '--misses-allowed',
            '-m',
            type=int,
            default=2,
            help='how many heartbeats missed before the program is killed')

    parsed_args = parser.parse_args()

    main(
            exec_cmd = parsed_args.exec_program,
            g_TO = parsed_args.heartbeat_timeout,
            g_N = parsed_args.misses_allowed,
            g_command_port = parsed_args.command_addr,
            g_heartbeat_addr = parsed_args.heartbeat_addr
            )


