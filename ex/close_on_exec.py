import fcntl,socket,os,fcntl,sys

#
# demonstrate that a socket is not closed on exec.
#
# see close_on_exec_client.py
# it connects to the socket below and waits for the socket
# to be closed.
# 
# the parent closes the socket immediately after accept below.
# nevertheless, the client does not see it until 5 sec later
# (when sleep 1 finishes below).
#

def Ws(s):
    sys.stdout.write(s)
    sys.stdout.flush()

def set_close_on_exec_fd(fd, close_on_exec):
    """
    make fd non blocking
    """
    if close_on_exec:
        fcntl.fcntl(fd, fcntl.F_SETFD, fcntl.FD_CLOEXEC)
    else:
        fcntl.fcntl(fd, fcntl.F_SETFD, 0)

def main():
    so = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    so.bind(("",0))
    so.listen(10)
    Ws("%s\n" % (so.getsockname(),))
    new_so,addr = so.accept()
    pid = os.fork()
    if pid == 0:
        # if we do the following, the client immediately gets EOF
        # new_so.close()
        set_close_on_exec_fd(new_so.fileno(), 1)
        os.execvp("sleep", [ "sleep", "5" ])
    else:
        new_so.close()
        os.wait()
    
main()
