#!/usr/bin/env python

import errno,os,pty,re,select,signal,string,sys

#
# (1) read a line from stdin and get it as a passphrase
# (2) try ssh (ssh variable below)
# (3) input passphrase if asked (search 'Enter passphrase')
# (4) get stdout of the child process
#
# Each read waits at most 'work_timeout' seconds (see below)
#
# It may kill the child process when it waited until timeout
# got a strange string from the child process.
#
# Exit status is normally that of the child process (ssh), or 
# 255 when the child was killed.
#

ssh = [ "ssh", "newcamelx",
        "-o", "StrictHostkeyChecking no", "echo", "hello" ]

#
# Timeout for each 'read' operation.
# It should be long enough so distant hosts can respond.
#
read_timeout = 5.0

# Probably you need not modify parameters below.
write_timeout = 1.0

def safe_read(fd, bytes, timeout, show_error):
    """
    Read string from child or timeout.
    """
    R,W,E = select.select([fd],[],[],timeout)
    if len(R) == 0:
        os.write(2, "error: read timeout\n")
        return None
    try:
        return os.read(fd, bytes)
    except OSError,e:
        if show_error:
            os.write(2, "error: %s\n" % e.args[1])
        return None

def safe_write(fd, str, timeout, show_error):
    """
    Write a string to child or timeout.
    """
    R,W,E = select.select([],[fd],[],timeout)
    if len(W) == 0:
        os.write(2, "error: write timeout\n")
        return None
    try:
        return os.write(fd, str)
    except OSError,e:
        if show_error:
            os.write(2, "error: %s\n" % e.args[1])
        return None

def cleanup(pid):
    """
    Check if pid exists. If so, kill it with SIGKILL.
    """
    try:
        os.kill(pid, 0)
    except OSError,e:
        if e.args[0] == errno.ESRCH:    # no such process
            return
    try:
        os.kill(pid, signal.SIGKILL)
    except OSError,e:
        if e.args[0] == errno.ESRCH:    # no such process
            return

def parent(fd, passphrase):
    """
    Main procedure of the parent.
    Input passphrase if asked.
    """
    bytes_to_read = 10 * 1024
    output = []
    while 1:
        s = safe_read(fd, bytes_to_read, read_timeout, 0)
        if s is None: break
        if re.match("Enter passphrase", s):
            if passphrase is None: break
            s = safe_write(fd, "%s\n" % passphrase, write_timeout, 1)
            passphrase = None
            if s is None: break
            s = safe_read(fd, bytes_to_read, read_timeout, 1)
            if s is None: break
            assert s == "\r\n", s
        else:
            output.append(s)
    return string.join(output, "")
    
def main(argv):
    if len(argv) > 1 and argv[1] == "-n": # no passphrase
        passphrase = None
    else:
        # first read a line containing the passphrase
        passphrase = string.strip(sys.stdin.readline())
    # fork with pty
    pid,master = pty.fork()
    assert pid != -1
    if pid == 0:
        # child. run ssh
        os.execvp("ssh", ssh)
    else:
        # parent. talk to child.
        s = parent(master, passphrase)
        # ensure child is gone 
        cleanup(pid)
        # write whatever we get from child
        os.write(1, s)
        # wait for child to disappear
        qid,status = os.wait()
        assert pid == qid
        if os.WIFEXITED(status):
            # child normally exited. forward its status
            os._exit(os.WEXITSTATUS(status))
        else:
            # child was killed. return 255
            os._exit(255)

if __name__ == "__main__":
    main(sys.argv)
