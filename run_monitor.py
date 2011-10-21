#!/usr/bin/env python

import subprocess
import sys
import time
import signal
import os
import zmq


g_heartbeat_addr = 'ipc://heartbeat'
g_command_port = 'ipc://command'
g_N = 2         # How many heart beats are alright?
g_TO = 1        # Time between heartbeats

def _kill(p):
    """Kills the process and returns True if it succeeded,
    False if there was a problem."""

    try:
        p.kill()
        return True
    except OSError:
        return False

def main(exec_name, args):
    """This runs the given application in a wrapper which restarts
    it on any of the following signs:
    <ul>
        <li> It misses 1 heart beat. </li>
        <li> It crashes with a non-zero error code. </li>
        <li> An external command requests so. </li>
    </ul>

    @param exec_name The name of the executable.
    @param args The arguments to pass to the file.

    @returns The error code returned by the application, or None.
    """

    global g_TO, g_N, g_command_port, g_heartbeat_addr

    ctx = zmq.Context()

    # Connection to the heartbeats
    in_hb = ctx.socket(zmq.PULL)
    in_hb.bind(g_heartbeat_addr)

    # Connection to the outside world
    in_cmd = ctx.socket(zmq.PULL)
    in_cmd.bind(g_command_port)

    retcode = 1

    while retcode:  # While the process does not exit cleanly
        print
        print '[monitor] Running ' + exec_name + ' ... '

        # Disable catching Ctrl-C by the subprocess module 
        # Passing it to the application instead.
        # signal.signal(signal.SIGINT, signal.SIG_IGN)

        try:
            p = subprocess.Popen([exec_name] + args)
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
                    # Drain out all messages (if the wrapper is too slow)
                    while(msg): msg = sock.recv(zmq.NOBLOCK)

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

            if not all_is_well:
                if not _kill(p):
                    print '[monitor] Probably already dead.'
                break 

            retcode = p.poll() # Check on the application

            if retcode is not None: # The application exited
                break

        # Re-enable Ctrl-C handler to exit this wrapper
        # signal.signal(signal.SIGINT, signal.default_int_handler)

        if retcode:
            print >> sys.stderr, 'Error = ' + str(retcode) + '\n'
            time.sleep(1)

    print "[monitor] Exiting with return code 0"
    return 0


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Usage: ' + sys.argv[0] + ' <exec name> [arguments]'
    else:
        main(sys.argv[1], sys.argv[2:])


