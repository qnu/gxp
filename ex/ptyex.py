#!/usr/bin/env python

import os,pty

def main():
    # fork with pty
    pid,master = pty.fork()
    assert pid != -1
    if pid == 0:
        os.execvp("python", [ "python", "foo.py" ])
    else:
        while 1:
            s = os.read(master, 1000)
            os.write(1, s)


if __name__ == "__main__":
    main()
