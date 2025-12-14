import os, sys

def run():
    print()

def daemonize():
    pid = os.fork()
    if pid != 0: # 1
        # this requires sudo...
        os.setsid() #  detach from any terminal and create an independent session.
    else: # 2 
        # ensure that the daemon can never
        # re-acquire a terminal again. (This is relevant if the program — and
        # all its dependencies — does not carefully specify `O_NOCTTY` on each
        # and every single `open()` call that might potentially open a TTY
        # device node.)
        pid = os.fork()
        if pid != 0: # 2
            sys.exit(0)
        else: # 3
            run() # the daemon

daemonize()
print(os.getpid())
