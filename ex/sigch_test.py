import os,signal,time

def main():
    signal.signal(signal.SIGCHLD, sigchld)
    n = 10
    for i in range(0, n):
        pid = os.fork()
        if pid == 0:
            # child
            os.execvp("hostname", ["hostname"])
    for i in range(0, n):
        os.wait()
    time.sleep(1.0)
    print "%d signals" % len(L)

L = []

def sigchld(num, frame):
    L.append(1)

main()
