# Copyright (c) 2005-2009 by Kenjiro Taura. All rights reserved.
# Copyright (c) 2005-2009 by Yoshikazu Kamoshida. All rights reserved.
#
# THIS MATERIAL IS PROVIDED AS IS, WITH ABSOLUTELY NO WARRANTY 
# EXPRESSED OR IMPLIED.  ANY USE IS AT YOUR OWN RISK.
# 
# Permission is hereby granted to use or copy this program
# for any purpose,  provided the above notices are retained on all 
# copies. Permission to modify the code and to distribute modified
# code is granted, provided the above notices are retained, and
# a notice that the code was modified is included with the above
# copyright notice.
#
# $Header: /cvsroot/gxp/gxp3/ioman.py,v 1.18 2013/10/25 12:00:54 ttaauu Exp $
# $Name:  $
#

import errno,fcntl,os,random,re,resource,select,signal,socket,string,sys
import time,types
# import profile,pstats

# -------------------------------------------------------------------
# debug flag
# -------------------------------------------------------------------

dbg=0

# -------------------------------------------------------------------
# constants
# -------------------------------------------------------------------

INF = ""                                # infty 

#
#
#

class logger:
    def __init__(self, filename=None):
        self.fd = None
        if filename is None:
            self.filename = self.default_log_filename()
        else:
            self.filename = filename
        self.set_log_filename(self.filename)
        self.set_log_header(None)
        self.set_log_base_time()
        self.set_show_time()

    def default_log_filename(self):
        return "log_%s" % socket.gethostname()

    def set_log_filename(self, filename):
        self.requested_file = filename

    def delete_log_file(self):
        if self.filename is not None:
            try:
                os.remove(self.filename)
                self.filename = None
            except EnvironmentError,e:
                pass
        
    def set_log_header(self, header):
        self.header = header

    def set_show_time(self):
        self.show_time = 1

    def set_log_base_time(self):
        self.base_time = time.time()

    def log(self, msg):
        if self.requested_file != self.filename:
            if self.fd is not None:
                os.close(self.fd)
                self.fd = None
            self.filename = self.requested_file
        assert self.filename == self.requested_file, \
               (self.filename, self.requested_file)
        if self.fd is None:
            self.fd = os.open(self.filename,
                              os.O_CREAT|os.O_WRONLY|os.O_TRUNC,
                              0644)
            portability.set_close_on_exec_fd(self.fd, 1)
        assert self.fd is not None
        if self.header is not None:
            os.write(self.fd, ("%s : " % self.header))
        if self.show_time:
            os.write(self.fd,
                     ("%.6f : " % (time.time() - self.base_time)))
        os.write(self.fd, msg)

the_logger = logger()
def LOG(msg):
    the_logger.log(msg)
def set_log_header():
    the_logger.set_log_header()
def set_log_base_time():
    the_logger.set_log_base_time()
def set_log_filename(filename):
    the_logger.set_log_filename(filename)
def delete_log_file():
    the_logger.delete_log_file()
    

# -------------------------------------------------------------------
# infrastructure for IO that does not return with signals or
# keyboard interrupt
# -------------------------------------------------------------------

def apply_no_intr(f, args):
    keyboard = 0
    for i in range(0, 500):
        try:
            return apply(f, args)
        except EnvironmentError,e:
            pass
        except socket.error,e:
            pass
        except select.error,e:
            pass
        except KeyboardInterrupt,e:
            keyboard = 1
            pass
        if keyboard == 0 and e.args[0] != errno.EINTR: raise
    raise

class non_interruptible_os_io:
    def write(self, fd, s):
        return apply_no_intr(os.write, (fd, s))
            
    def read(self, fd, sz):
        return apply_no_intr(os.read, (fd, sz))

    def close(self, fd):
        return apply_no_intr(os.close, (fd, ))

nointr_os = non_interruptible_os_io()

class non_interruptible_socket:
    def __init__(self, so):
        self.so = so

    def nointr_send(self, s, flags=0):
        return apply_no_intr(self.so.send, (s, flags))
            
    def nointr_recv(self, sz, flags=0):
        return apply_no_intr(self.so.recv, (sz, flags))

    def nointr_connect(self, name):
        return apply_no_intr(self.so.connect, (name, ))

    def nointr_accept(self):
        conn,addr =  apply_no_intr(self.so.accept, ())
        return non_interruptible_socket(conn),addr

    def nointr_close(self):
        # return apply_no_intr(self.so.close, ())
        return self.so.close()

    def fileno(self):
        return self.so.fileno()

    def setblocking(self, x):
        return self.so.setblocking(x)

    def setsockopt(self, level, opt, val):
        return self.so.setsockopt(level, opt, val)

    def getsockopt(self, level, opt):
        return self.so.getsockopt(level, opt)

    def bind(self, name):
        return self.so.bind(name)

    def listen(self, qlen):
        return self.so.listen(qlen)
            
    def getsockname(self):
        return self.so.getsockname()

    def getpeername(self):
        return self.so.getpeername()

def mk_non_interruptible_socket(af, type):
    return non_interruptible_socket(socket.socket(af, type))

class non_interruptible_select:
    def select(self, R, W, E, T=None):
	if T is None:
            return apply_no_intr(select.select, (R, W, E))
        else:
            return apply_no_intr(select.select, (R, W, E, T))
        
class non_interruptible_select_by_poll:
    def select(self, R, W, E, T=None):
        d = {}
        for f in R:
            fd = f.fileno()
            d[fd] = select.POLLIN
        for f in W:
            fd = f.fileno()
            d[fd] = d.get(fd, 0) | select.POLLOUT
        for f in E:
            fd = f.fileno()
            d[fd] = d.get(fd, 0) | select.POLLIN | select.POLLOUT
        p = select.poll()
        for (fd, mask) in d.items():
            p.register(fd, mask)
        if T is None:
            list = apply_no_intr(p.poll, ())
        else:
            list = apply_no_intr(p.poll, (T*1000,))
        R0 = []
        W0 = []
        E0 = []
        d = dict(list)
        for f in R:
            if (d.get(f.fileno(), 0) & (select.POLLIN|select.POLLHUP)) != 0:
                R0.append(f)
        for f in W:
            if (d.get(f.fileno(), 0) & (select.POLLOUT)) != 0:
                W0.append(f)
        for f in E:
            if (d.get(f.fileno(), 0) & select.POLLERR) != 0:
                E0.append(f)
        return R0,W0,E0


if hasattr(select, "poll"):
    nointr_select = non_interruptible_select_by_poll()
else:
    nointr_select = non_interruptible_select()

# -------------------------------------------------------------------
# a simple layer hiding some of the platform/python version
# dependences
# -------------------------------------------------------------------

class portability_class:
    """
    1. Some versions of Python define F_SETFL and F_GETFL in FCNTL,
    while others in fcntl.
    """
    def __init__(self):
        self.set_fcntl_constants()

    def set_fcntl_constants(self):
        self.F_GETFL = fcntl.F_GETFL
        self.F_SETFL = fcntl.F_SETFL
        self.F_GETFD = fcntl.F_GETFD
        self.F_SETFD = fcntl.F_SETFD
        ok = 0
        if fcntl.__dict__.has_key("FD_CLOEXEC"):
            self.FD_CLOEXEC = fcntl.FD_CLOEXEC
            ok = 1
        if ok == 0:
            try:
                FCNTL_ok = 0
                import warnings
                warnings.filterwarnings("ignore", "", DeprecationWarning)
                import FCNTL
                FCNTL_ok = 1
                warnings.resetwarnings()
            except ImportError:
                pass
            if FCNTL_ok and FCNTL.__dict__.has_key("FD_CLOEXEC"):
                self.FD_CLOEXEC = FCNTL.FD_CLOEXEC
                ok = 1
        if ok == 0:
            # assume FD_CLOEXEC = 1. see 
            # http://mail.python.org/pipermail/python-bugs-list/2001-December/009360.html
            self.FD_CLOEXEC = 1
            ok = 1

        if ok == 0:
            LOG("This platform provides no ways to set "
                "close-on-exec flag. abort\n")
            os._exit(1)

    def set_blocking_fd(self, fd, blocking):
        """
        make fd non blocking
        """
        flag = fcntl.fcntl(fd, self.F_GETFL)
        if blocking:
            new_flag = flag & ~os.O_NONBLOCK
        else:
            new_flag = flag | os.O_NONBLOCK
        fcntl.fcntl(fd, self.F_SETFL, new_flag)

    def set_close_on_exec_fd(self, fd, close_on_exec):
        """
        make fd non blocking
        """
        if close_on_exec:
            fcntl.fcntl(fd, self.F_SETFD, self.FD_CLOEXEC)
        else:
            fcntl.fcntl(fd, self.F_SETFD, 0)

portability = portability_class()

# -------------------------------------------------------------------
# abstract primitive asynchronous channel classes
# -------------------------------------------------------------------

class primitive_channel:
    def fileno(self):
        """
        the number passed to select syscall
        """
        should_be_implemented_in_subclasses()

    def read(self, sz):
        """
        Try to read <= sz bytes asynchronously and return whatever
        is read. Return (-1,error_msg) or (n,data), where n is
        the size of data read.
        """
        should_be_implemented_in_subclasses()
    
    def write(self, data):
        """
        Try to write data asynchronously and return the number of
        bytes written. Return (-1,error_msg) or (n,"") where n is
        the size of data written.
        """
        should_be_implemented_in_subclasses()

    def close(self):
        """
        close the underlying descriptor or socket
        """
        should_be_implemented_in_subclasses()

    def is_closed(self):
        """
        1 if this channel has been closed
        """
        should_be_implemented_in_subclasses()
        


# -------------------------------------------------------------------
# primitive asynchronous channel
# -------------------------------------------------------------------

class primitive_channel_fd:
    """
    Primitive channels based on file descriptors (integer).
    """
    def __init__(self, fd, blocking):
        portability.set_blocking_fd(fd, blocking)
        portability.set_close_on_exec_fd(fd, 1)
        self.fd = fd
        self.closed = 0

    def fileno(self):
        return self.fd

    def read(self, sz):
        """
        Asynchronously read at most sz bytes.
        When successful, return len(whatever_is_read),
        whatever_is_read.
        'whatever_is_read' should not be an empty string.
        Otherwise return -1,error_msg.
        """
        try:
            frag = nointr_os.read(self.fd, sz)
            return len(frag),0,frag
        except EnvironmentError,e:
            return -1,e.args[0],e.args[1]
    
    def write(self, frag):
        """
        Try to write a string FRAG. The entire msg may not be
        written. When successful, return number_of_bytes_written,"".
        Otherwise, return -1,error_msg.
        """
        try:
            return nointr_os.write(self.fd, frag),0,""
        except EnvironmentError,e:
            return -1,e.args[0],e.args[1]
    
    def close(self):
        if self.closed == 0:
            self.closed = 1
            nointr_os.close(self.fd)

    def is_closed(self):
        return self.closed

class primitive_channel_socket:
    """
    Primitive channels based on sockets (socket objects).
    """
    def __init__(self, so, blocking):
        so.setblocking(blocking)
        portability.set_close_on_exec_fd(so.fileno(), 1)
        self.so = so
        self.closed = 0

    def fileno(self):
        return self.so.fileno()

    def read(self, sz):
        """
        Asynchronously read at most sz bytes.
        When successful, return len(whatever_is_read),
        whatever_is_read.
        'whatever_is_read' should not be an empty string.
        Otherwise return -1,error_msg.
        """
        try:
            frag = self.so.nointr_recv(sz)
            return len(frag),0,frag
        except socket.error,e:
            return -1,e.args[0],e.args[1]
        except EnvironmentError,e:
            return -1,e.args[0],e.args[1]
    
    def write(self, frag):
        """
        Try to write a string FRAG. The entire msg may not be
        written. When successful, return number_of_bytes_written,"".
        Otherwise, return -1,error_msg.
        """
        try:
            return self.so.nointr_send(frag),0,""
        except socket.error,e:
            return -1,e.args[0],e.args[1]
        except EnvironmentError,e:
            return -1,e.args[0],e.args[1]
    
    def accept(self):
        try:
            conn,addr = self.so.nointr_accept()
        except socket.error,e:
            return -1,e.args[0],e.args[1]
        except EnvironmentError,e:
            return -1,e.args[0],e.args[1]
        portability.set_close_on_exec_fd(conn.fileno(), 1)
        return 0,0,(conn,addr)
    
    def connect(self, name):
        try:
            self.so.nointr_connect(name)
        except socket.error,e:
            return -1,e
        except EnvironmentError,e:
            return -1,e
        return 0,None
    
    def getsockname(self):
        return self.so.getsockname()

    def close(self):
        if self.closed == 0:
            self.closed = 1
            self.so.nointr_close()

    def is_closed(self):
        return self.closed
    
# -------------------------------------------------------------------
# priority queue to make high level channel interface
# -------------------------------------------------------------------

class prio_queue:
    """
    Dumb implementation of heapq.
    FIXME: use heap

    priority : 0 is lowest
    """
    def __init__(self):
        self.data = []                  # list of (prio,val)

    def try_get(self, default):
        """
        Try to get an (element,priority) from the list. If the
        list is empty return (default,None)
        """
        if len(self.data) > 0:
            prio,datum = self.data.pop(0)
            return datum,prio
        else:
            return default,None

    def get(self):
        """
        Get (element,priority). Raise an error if the queue is empty
        """
        datum,prio = self.try_get(None)
        assert prio is not None
        return datum,prio

    def put(self, val, prio):
        """
        put a value VAL to the queue for priority PRIO
        """
        if prio == 0:
            self.data.append((prio, val))
        else:
            idx = 0
            for _,p in self.data:
                if prio > p: break
                idx = idx + 1
            self.data.insert(idx, (prio, val))

    def push_back(self, val, prio):
        """
        put a value VAL to the queue for priority PRIO.
        """
        idx = 0
        for _,p in self.data:
            if prio >= p: break
            idx = idx + 1
        self.data.insert(idx, (prio, val))

    def put_low(self, val):
        """
        put a value VAL to the queue for priority PRIO
        """
        self.data.append((0, val))

    def empty(self):
        """
        1 if empty
        """
        if len(self.data) == 0:
            return 1
        else:
            return 0
        
# -------------------------------------------------------------------
# string buffer
# -------------------------------------------------------------------

class string_buffer:
    """
    Java-like string buffer object.
    s = string_buffer()
    s.extend('abc')
    Also support:
      len(s), s[i], s[i:j], s[i] = 'a', s[i:j] = 'abc',
      del s[i], del s[i:j]
    """
    def __init__(self):
        self.S = []                     # list of strings
        self.l = 0                      # total length
    def extend(self, a):
        self.S.append(a)
        self.l = self.l + len(a)
    def __len__(self):                  # len(S)
        return self.l
    def __getitem__(self, idx):         # S[i] or S[i:j]
        return self.aux__(idx, None)
    def __setitem__(self, i, val):      # S[i] = val or S[i:j] = val
        self.aux__(i, val)
    def __delitem__(self, i):           # del S[i] or del S[i:j]
        self.aux__(i, "")
    def delete(self, i, j):
        # like del self[i:j] but return the deleted item
        return self.aux__(slice(i,j), "")

    def aux__(self, idx, val):
        """
        access idx-th element of th string buffer.
        it is used to implement all of buf[i], buf[i:j],
        buf[i] = x, buf[i:j] = X, del buf[i], and and buf[i:j]

        if val is not None, set (replace) the value.
        """
        if type(idx) is types.SliceType:
            start,stop,step = idx.start,idx.stop,idx.step
            if stop > self.l: stop = self.l
            if start < 0: start = 0
        else:
            start,stop,step = idx,idx+1,None
            if not (0 <= idx < self.l):
                raise IndexError("string buffer index out of range")
        assert step is None,(start,stop,step)

        if 1 and (start == 0 and stop == self.l):
            X = string.join(self.S, "")
            if val is not None:
                self.S[:] = []
                self.l = 0
            return X
        
        x = 0
        A = []
        B = []
        C = []
        len_A = 0
        len_B = 0
        len_C = 0
        for s in self.S:
            y = x + len(s)
            # intersect [x,y] with [0,start]
            a = x
            b = min(y, start)
            if a < b:
                A.append(s[a-x:b-x])
                len_A = len_A + b - a
            # intersect [x,y] with [start,stop]
            a = max(x, start)
            b = min(y, stop)
            if a < b:
                B.append(s[a-x:b-x])
                len_B = len_B + b - a
            # intersect [x,y] with [stop,n]
            a = max(x, stop)
            b = y
            if a < b:
                C.append(s[a-x:b-x])
                len_C = len_C + b - a

            x = x + len(s)
        assert len_A + len_B + len_C == self.l
        X = string.join(B, "")
        assert len(X) == stop - start, (start, stop)
        if val is not None:
            self.S[:] = []
            self.S.extend(A)
            if val != "": self.S.append(val)
            self.S.extend(C)
            self.l = len_A + len(val) + len_C
        return X

    if sys.hexversion < 0x02000000:
        # version_info < (2,0)
        def __getslice__(self, i, j):
            return self[max(0, i):max(0, j):]
        def __setslice__(self, i, j, seq):
            self[max(0, i):max(0, j):] = seq
        def __delslice__(self, i, j):
            del self[max(0, i):max(0, j):]
    

# -------------------------------------------------------------------
# stream searcher to find matching msgs in the pending queue
# -------------------------------------------------------------------

class pattern_searcher:
    def search(self, frag):
        """
        search(frag) should search a prescribed pattern in frag,
        and return a,b,prio
        """
        should_be_implemented_in_subclasses()
    def get_overlap(self):
        return INF
    def get_payload(self, s):
        return s

class pattern_searcher_any(pattern_searcher):
    def search(self, frag):
        if dbg>=2:
            LOG("pattern_searcher_any : "
                "search any in %d bytes [%s ...]\n" % \
                (len(frag), frag[0:30]))
            LOG("pattern_searcher_any : "
                "found %d bytes [%s ...]\n" % \
                (len(frag), frag[0:30]))
        return None,len(frag),0
    def get_overlap(self):
        return 0

class pattern_searcher_sz(pattern_searcher):
    def __init__(self, sz):
        self.sz = sz
    def search(self, frag):
        if dbg>=2:
            LOG("pattern_searcher_sz : "
                "search %d bytes in %d bytes [%s ...]\n" % \
                (self.sz, len(frag), frag[0:30]))
        if len(frag) >= self.sz:
            if dbg>=2:
                LOG("pattern_searcher_sz : "
                    "found %d bytes [%s ...]\n" % \
                    (self.sz, frag[0:30]))
            return None,self.sz,0
        else:
            if dbg>=2:
                LOG("pattern_searcher_sz : not enough data\n")
            return None
    def get_overlap(self):
        return self.sz - 1

class pattern_searcher_exact(pattern_searcher):
    def __init__(self, needle):
        self.needle = needle
        self.overlap = len(needle) - 1
    def search(self, frag):
        if dbg>=2:
            LOG("pattern_searcher_exact : "
                "search [%s] in %d bytes [%s ...]\n" % \
                (self.needle, len(frag), frag[0:30]))
        # idx = string.find(frag, self.needle)
        idx = string.rfind(frag, self.needle)
        if idx != -1:
            if dbg>=2:
                LOG("pattern_searcher_exact : "
                    "found %d bytes [%s ...]\n" % \
                    (idx+len(self.needle), frag[0:30]))
            return None,idx+len(self.needle),0
        else:
            if dbg>=2:
                LOG("pattern_searcher_exact : pattern not found\n")
            return None
    def get_overlap(self):
        return self.overlap

class pattern_searcher_regexp(pattern_searcher):
    def __init__(self, regexp, overlap):
        self.regexp_str = regexp
        self.regexp = re.compile(self.regexp_str)
        self.overlap = overlap
    def search(self, frag):
        if dbg>=2:
            LOG("pattern_searcher_regexp : "
                "search [%s] in %d bytes [%s ...]\n" % \
                (self.regexp_str, len(frag), frag[0:30]))
        m = self.regexp.search(frag)
        if m:
            # lowest priority = 0, highest size = INF
            a,b = m.span(0)
            if dbg>=2:
                LOG("pattern_searcher_regexp : "
                    "found %d bytes [%s ...]\n" % (b, frag[0:30]))
            return None,b,0
        else:
            if dbg>=2:
                LOG("pattern_searcher_regexp : pattern not found\n")
            return None
    def get_overlap(self):
        return self.overlap

class pattern_searcher_msg(pattern_searcher):
    h_temp = "HEADER len %20d prio %20d HEADER_END"
    h_pat  = "HEADER len +(\d+) prio +(\d+) HEADER_END"
    t_temp = "TRAIL len %20d prio %20d sum %20d TRAIL_END"
    t_pat  = "TRAIL len +(\d+) prio +(\d+) sum +(\d+) TRAIL_END"
    header_temp  = h_temp
    header_len   = len(h_temp % (0, 0))
    header_pat   = re.compile(h_pat)
    trailer_temp = t_temp
    trailer_len  = len(t_temp % (0, 0, 0))
    trailer_pat  = re.compile(t_pat)

    def get_payload(self, s):
        a = pattern_searcher_msg.header_len
        b = pattern_searcher_msg.trailer_len
        return s[a:-b]
    
    def search(self, frag):
        if dbg>=2:
            LOG("pattern_searcher_msg : "
                "search msg in %d bytes [%s ...]\n" % \
                (len(frag), frag[0:30]))
        m = pattern_searcher_msg.trailer_pat.search(frag)
        if m:
            b = m.end(0)
            sz = string.atoi(m.group(1))
            prio = string.atoi(m.group(2))
            total_sz = pattern_searcher_msg.header_len + sz \
                       + pattern_searcher_msg.trailer_len
            a = b - total_sz
            if dbg>=2:
                LOG("pattern_searcher_msg : "
                    "found %d bytes [%s ...]\n" % \
                    (total_sz, frag[a:a + 30]))
            return a,b,prio
        else:
            return None

    def get_overlap(self):
        return pattern_searcher_msg.trailer_len - 1

# -------------------------------------------------------------------
# events for high level channels
# -------------------------------------------------------------------

class ch_event:
    OK       = 0                        # got OK data
    IO_ERROR = -1                       # got IO error
    TIMEOUT  = -2                       # got timeout
    EOF      = -3                       # got EOF
    kind_to_str = {
        OK       : "OK",
        IO_ERROR : "IO_ERROR",
        TIMEOUT  : "TIMEOUT",
        EOF      : "EOF"
        }
    def __init__(self, kind):
        self.kind = kind

    def kind_str(self):
        return ch_event.kind_to_str[self.kind]

class revent(ch_event):
    """
    a read event represents an event in which an amount of data
    has been received
    """
    def __init__(self, kind, data):
        ch_event.__init__(self, kind)
        self.data = data
        self.err_msg = ""

class wevent(ch_event):
    pass

class aevent(ch_event):
    pass

class revent_OK(revent):
    def __init__(self, data):
        revent.__init__(self, ch_event.OK, data)

class revent_IO_ERROR(revent):
    def __init__(self, data, err_msg):
        revent.__init__(self, ch_event.IO_ERROR, data)
        self.err_msg = err_msg

class revent_EOF(revent):
    def __init__(self, data):
        revent.__init__(self, ch_event.EOF, data)

class revent_TIMEOUT(revent):
    def __init__(self, data):
        revent.__init__(self, ch_event.TIMEOUT, data)

class wevent_OK(wevent):
    def __init__(self, tag, written):
        wevent.__init__(self, ch_event.OK)
        self.tag = tag
        self.written = written

class wevent_IO_ERROR(wevent):
    def __init__(self, err_msg):
        wevent.__init__(self, ch_event.IO_ERROR)
        self.err_msg = err_msg

class wevent_TIMEOUT(wevent):
    def __init__(self):
        wevent.__init__(self, ch_event.TIMEOUT)
        self.err_msg = "timeout"

class aevent_OK(aevent):
    def __init__(self, new_so, addr):
        aevent.__init__(self, ch_event.OK)
        self.new_so = new_so
        self.addr = addr

class aevent_IO_ERROR(aevent):
    def __init__(self, err_msg):
        aevent.__init__(self, ch_event.IO_ERROR)
        self.err_msg = err_msg

class aevent_TIMEOUT(aevent):
    def __init__(self):
        aevent.__init__(self, ch_event.TIMEOUT)

# -------------------------------------------------------------------
# high level channel interface
# -------------------------------------------------------------------


class channel:
    def __init__(self, pch):
        # pch : underlying primitive channel
        self.pch = pch
        # timelimit
        self.x_timelimit = INF
        # deamon managing this channel
        self.iom = None
        # self.preference = 0

    def set_ioman(self, iom):
        self.iom = iom
        
    def close(self):
        """
        Close the underlying primitive fd/socket.
        This may double-close because a primitive channel may
        be shared by multiple (high-level) channels.
        primitive_channel.close prevents double close of fd/socket
        """
        self.pch.close()

    def is_closed(self):
        return self.pch.is_closed()

    def fileno(self):
        """
        Called by select. we must ensure closed channels are
        never passed to select, so we never pass -1 to select.
        Just for the sake of logging purposes, we gently return
        -1 when called on closed channels
        """
        if self.is_closed():
            return -1
        else:
            return self.pch.fileno()
        
    def set_timeout(self, timeout):
        if timeout == INF:       # infinite
            self.x_timelimit = INF
        else:
            self.x_timelimit = time.time() + timeout

    def is_garbage(self):
        """
        1 if this channel will never generate further events,
        so it does not have to be checked by IO manager.
        Basically, a closed channel is usually a garbage, but even
        if it is closed, it is not a garbage if there are some
        pending events (buffered items previously read).
        """
        if self.is_closed() and self.has_pending_events() == 0:
            return 1
        else:
            return 0
        
    # --------- abstract stuff to be implemented in subclass

    def discard(self):
        """
        make this channel garbage.
        """
        should_be_implemented_in_subclasses()

    def has_pending_events(self):
        """
        1 if there are some pending events that can be generated
        without performing any IO. For read channels, it is true
        when there are some buffered iterms previously read.
        If there are some channels who have pending events,
        the IO manager should not block waiting for IO.
        """
        should_be_implemented_in_subclasses()

    def want_io(self):
        should_be_implemented_in_subclasses()
        
    def do_io(self):
        should_be_implemented_in_subclasses()
        
    def do_timeout(self):
        should_be_implemented_in_subclasses()
        
    def process_event(self, ev):
        should_be_implemented_in_subclasses()
        
# -------------------------------------------------------------------
# read channel
# -------------------------------------------------------------------

class rchannel(channel):
    """
    abstract base class for high-level channels to read
    """
    # default_read_gran = 150000
    default_read_gran = 10000
    def __init__(self, pch):
        channel.__init__(self, pch)
        # receiver's expectation
        self.expected = []
        # we know pending[:cursor] does not match any expected msg
        self.cursor = 0
        # msgs received, but does not match receiver's expectation
        self.pending = string_buffer()
        # set to an appropriate status when got EOF/IO_ERROR/TIMEOUT
        self.pending_close = []
        # msgs received and match 
        # self.deliver_q = prio_queue()
        # read granularity (approx. 10K)
        self.set_read_gran(rchannel.default_read_gran)

    def set_read_gran(self, g):
        if g < 1: g = 1
        self.read_gran = g

    def discard(self):
        self.close()
        del self.pending[:]
        del self.pending_close[:]
        self.cursor = 0
        assert self.is_garbage()

    def has_pending_events(self):
        if self.cursor < len(self.pending):
            return 1
        else:
            return 0

    def want_io(self):
        return 1
        
    def can_read(self):
        """
        return 1 if the underlying primitive channel
        is ready for io
        """
        if self.is_closed(): return 0
        R,_,_ = nointr_select.select([self], [], [], 0.0)
        if len(R) > 0:
            return 1
        else:
            return 0

    def do_io(self):
        """
        Perform actual IO for read, compare the pending stream
        with what the client is waiting for (regexp, msgs,
        fixed bytes, etc.), and put matched stuff into deliver_q.
        """
        if dbg>=2:
            LOG("rchannel.do_io(%d) : begin\n" % self.fileno())
        if self.can_read():
            # TODO: We may not like to read,
            # when we have already buffered too much data.
            # But if we have examined the pending data until the
            # end and still does not have a matching pattern,
            # we should not stop reading. For now we play safe
            # (always read if we can).
            if dbg>=2:
                LOG("rchannel.do_io(%d) : can read\n" \
                   % self.fileno())
            if self.cursor == len(self.pending):
                self.pull_some_data__()
        # Search all expected patterns and get the one with
        # the highest preference. Preference is determined as
        # follows.
        # (1) Matches of higher priorities have higher preferences.
        # (2) Otherwise matches found nearer to the head of 
        #     the stream have higher preferences.
        # (3) Otherwise larger matches have higher preferences.
        results = []
        for pat in self.expected:
            check_start = self.cursor - pat.get_overlap()
            if check_start < 0: check_start = 0
            s = self.pending[check_start:]
            if s == "":
                m = None
            else:
                m = pat.search(s)
            if m is not None:
                # The pattern found
                #   a    : begin position of the match
                #   b    : end position of the match
                #   prio : priority of the message
                # a and b are relative within the given string s.
                # a is None if the match is always a whole prefix
                # up to position b (i.e., a is the beginning of
                # the entire stream, not s).
                a,b,prio = m
                # translate them into the absolute position in the
                # entire pending stream
                match_end = check_start + b
                if a is None:
                    match_start = 0 # beginning of the entre pending
                else:
                    match_start = check_start + a
                sz = match_end - match_start
                # Preference: The higher priority the better.
                # The smaller the end position the better.
                # The larger in the size the better.
                pref = (prio, -match_end, sz)
                results.append((pref, pat, match_start, match_end, prio))
            # idx = idx + 1
        if len(results) > 0:
            # A match found. Get the one with the highest preference.
            # _,idx,match_start,match_end,prio = max(results)
            _,pat,match_start,match_end,prio = max(results)
            s = self.pending.delete(match_start, match_end)
            if dbg>=1 and match_start != 0:
                LOG("rchannel.do_io(%d) : leave data [%s] in the "
                    "head of the pending queue\n" \
                    % (self.fileno(), self.pending[:]))
            # next time, continue from the point following the match
            self.cursor = match_start
            if dbg>=2:
                LOG("rchannel.do_io(%d) : "
                    "had data %d bytes [%s ...]\n" \
                    % (self.fileno(), len(s), s[0:30]))
            ev = revent_OK(pat.get_payload(s))
            self.set_timeout(INF)
            return ev
        elif len(self.pending_close) > 0:
            # No match found and we have gotten EOF/ERROR.
            # Return whatever remains in the pending to the app.
            assert len(self.pending_close) == 1,self.pending_close
            status,data = self.pending_close.pop(0)
            s = self.pending.delete(0, len(self.pending))
            self.cursor = 0    # Does not matter. For clarity.
            if status == ch_event.IO_ERROR:
                if dbg>=1:
                    LOG("rchannel.do_io(%d) : had error [%s]\n" \
                       % (self.fileno(), data))
                ev = revent_IO_ERROR(s, data)
            elif status == ch_event.EOF:
                if dbg>=2:
                    LOG("rchannel.do_io(%d) : got EOF\n" \
                        % self.fileno())
                ev = revent_EOF(s)
            else:
                bomb()
            self.discard()
            return ev
        else:
            # Advance the cursor.
            if dbg>=2:
                LOG("rchannel.do_io(%d) : no data\n" \
                    % self.fileno())
            self.cursor = len(self.pending)
            return None

    def do_timeout(self):
        assert self.x_timelimit <= time.time() + 0.1
        # Timeout
        s = self.pending.delete(0, len(self.pending))
        self.discard()
        if dbg>=2:
            LOG("rchannel.do_timeout(%d) : timeout\n" \
                % self.fileno())
        ev = revent_TIMEOUT(s)
        return ev

    def set_expected_aux(self, expected, timeout=INF):
        self.expected = expected
        self.cursor = 0                 # invalidate cursor
        self.set_timeout(timeout)

    def is_msg_mode(self):
        for e in self.expected:
            if isinstance(e, pattern_searcher_msg):
                return 1
        return 0

    def set_expected(self, expected):
        """
        A more user-friendly syntax for setting expected patterns.
        Expected is a list of string or a tuple.
        A string represents an exact match.
        A tuple is either ('*',), ('M',), ('SZ', n),
        ('RE', regexp, max_len).
        """
        timeout=INF
        expected_ = []
        for e in expected:
            if type(e) is types.StringType:
                ex = pattern_searcher_exact(e)
            elif type(e) is types.TupleType:
                kind = e[0]
                if kind == "*":
                    # ("*",)       anything
                    ex = pattern_searcher_any()
                elif kind == "SZ":
                    # ("SZ", 100)  fixed sized block
                    ex = pattern_searcher_sz(e[1])
                elif kind == "M":
                    # ("M",)       msg
                    ex = pattern_searcher_msg()
                elif kind == "RE":
                    # ("RE", "abc.*xyz", 25)
                    regexp,overlap = e[1:]
                    ex = pattern_searcher_regexp(regexp, overlap)
                elif kind == "TIMEOUT":
                    # ("TIMEOUT", 30.0)
                    timeout = min(e[1], timeout)
                    ex = None
                else:
                    bomb()
            else:
                assert 0, e
            if ex is not None: expected_.append(ex)
        self.set_expected_aux(expected_, timeout)
        
    # ------ callbacks that should be implemented ------

    def process_event(self, ev):
        pass

    # ------ internal methods ------

    def pull_some_data__(self):
        """
        Pull some bytes from the underlying primitive channel,
        and put what is received into pending or pending_close.
        """
        n,err_code,data = self.pch.read(self.read_gran)
        if n == -1:
            # got io error => put it in the pending queue
            if dbg>=2:
                LOG("rchannel.pull_some_data__(%d) : "
                    "ERROR [%s]\n" % (self.fileno(), data))
            self.close()
            self.pending_close.append((ch_event.IO_ERROR, data))
        elif n == 0:
            # got EOF => put it in the pending queue
            if dbg>=2:
                LOG("rchannel.pull_some_data__(%d) : EOF\n" \
                    % self.fileno())
            self.close()
            self.pending_close.append((ch_event.EOF, ""))
        else:
            # got some data => put it in the pending queue
            if dbg>=2:
                LOG("rchannel.pull_some_data__(%d) : "
                    "OK %d bytes [%s ...]\n" \
                    % (self.fileno(), len(data), data[0:30]))
            self.pending.extend(data)

# -------------------------------------------------------------------
# write channel
# -------------------------------------------------------------------

class wchannel(channel):
    """
    abstract base class for high-level channels to write
    """
    
    def __init__(self, pch):
        channel.__init__(self, pch)
        self.reset_pending_q()

    def reset_pending_q(self):
        self.write_q = prio_queue()
        self.buf_len = 0                # size of pending writes
        # If not None, a list of strings being sent. These strings
        # must be sent atomically (without interleaving other msgs)
        self.cur_msg = None           
        self.cur_tag = None
        
    def discard(self):
        self.close()
        self.reset_pending_q()
        assert self.is_garbage()

    def has_pending_events(self):
        """
        """
        if self.is_closed() == 0:
            return 0
        elif self.want_io():
            return 1
        else:
            return 0
        
    def want_io(self):
        if self.cur_msg is not None:
            return 1
        elif self.write_q.empty() == 0:
            return 1
        else:
            return 0
        
    def can_write(self):
        if self.is_closed(): return 0
        _,W,_ = nointr_select.select([], [self], [], 0.0)
        if len(W) > 0:
            return 1
        else:
            return 0

    def do_io(self):
        if dbg>=2:
            LOG("wchannel.do_io(%d) : begin\n" % self.fileno())
        # we have just completed a write in the previous call,
        # pick up the next request from the queue.
        if self.cur_msg is None:
            (tag,msg),prio = self.write_q.get()
            if msg is None:
                # meaning this is an EOF request
                if dbg>=2:
                    LOG("wchannel.do_io(%d) : "
                        "close channel\n" % self.fileno())
                self.discard()
                ev = wevent_OK(tag, 0)
                self.set_timeout(INF)
                return ev
            self.cur_msg = msg
            self.cur_tag = tag
        written = 0                     # of bytes written
        # try to push cur_msg entirely into the channel
        while len(self.cur_msg) > 0:
            if self.is_closed():
                # some data want to be put, but the channel is closed
                if dbg>=2:
                    LOG("wchannel.do_io(%d) : "
                        "lost %d bytes msg in write channels\n" \
                        % (self.fileno(), self.buf_len))
                self.discard()
                ev = wevent_IO_ERROR("channel closed before write")
                return ev
            elif self.can_write() == 0:
                if dbg>=2:
                    LOG("wchannel.do_io(%d) : "
                        "channel blocked and we retry\n" \
                        % self.fileno())
                return None
            # peek (not take) the first fragment
            frag = self.cur_msg[0]
            n,err_code,err_msg = self.pch.write(frag)
            if n == -1:
                # got io error => deliver it
                if err_code == errno.EWOULDBLOCK:
                    return None
                else:
                    if dbg>=2:
                        LOG("wchannel.do_io(%d) : ERROR [%s] "
                            "payload: %d bytes [%s ...]\n" \
                            % (self.fileno(), err_msg,
                               len(frag), frag[0:30]))
                    ev = wevent_IO_ERROR(err_msg)
                    self.discard()       # close?
                    return ev
            else:
                if dbg>=2:
                    LOG("wchannel.do_io(%d) : WRITTEN "
                        "[%d/%d]\n" % (self.fileno(), n, len(frag)))
                written = written + n
                self.buf_len = self.buf_len - n
                if n < len(frag):
                    # Partially written. Push back the rest.
                    # TODO: this may be slow (see above)
                    if dbg>=1:
                        LOG("wchannel.do_io(%d) : PARTIAL WRITE "
                            "[%d/%d]\n" \
                            % (self.fileno(), n, len(frag)))
                    self.cur_msg[0] = frag[n:]
                    return None
                else:
                    # Successfully written a fragment. Continue
                    assert n == len(frag), (n, len(frag))
                    self.cur_msg.pop(0)
        # now we should have written the entire msg.
        assert len(self.cur_msg) == 0, len(self.cur_msg)
        if dbg>=2:
            LOG("wchannel.do_io(%d) : OK\n" \
                % (self.fileno()))
        ev = wevent_OK(self.cur_tag, written)
        self.set_timeout(INF)
        self.cur_msg = None
        self.cur_tag = None
        return ev

    def do_timeout(self):
        assert self.x_timelimit <= time.time() + 0.1
        # for write timeout, we close the channel
        if dbg>=2:
            LOG("wchannel.do_io(%d) : "
                "channel blocked and timeout\n" % self.fileno())
        ev = wevent_TIMEOUT()
        self.discard()
        return ev

    # ------ 3 ways to write stuff ------

    def write_stream(self, data, prio=0, tag=None):
        if dbg>=2:
            LOG("wchannel.write_stream(%d) : %d bytes\n" \
                % (self.fileno(), len(data)))
        if self.is_garbage():
            if dbg>=1:
                LOG("wchannel.write_stream(%d) : wchannel closed\n" \
                    % self.fileno())
            return -1
        self.write_q.put((tag, [data]), prio)
        self.buf_len = self.buf_len + len(data)
        self.iom.mark_hot(self)
        return 0

    def write_fixed(self, data, prio=0, tag=None):
        if dbg>=2:
            LOG("wchannel.write_fixed(%d) : %d bytes\n" \
                % (self.fileno(), len(data)))
        if self.is_garbage():
            if dbg>=1:
                LOG("wchannel.write_fixed(%d) : wchannel closed\n" \
                    % self.fileno())
            return -1
        self.write_q.put((tag, [data]), prio)
        self.buf_len = self.buf_len + len(data)
        self.iom.mark_hot(self)
        return 0

    def mk_msg(self, data, prio):
        sz = len(data)
        sum = self.check_sum_msg__(data)
        htemp = pattern_searcher_msg.header_temp
        header = htemp % (sz, prio)
        ttemp = pattern_searcher_msg.trailer_temp
        trailer = ttemp % (sz, prio, sum)
        return (header,data,trailer)
        
    def write_msg(self, data, prio=0, tag=None):
        if dbg>=2:
            LOG("wchannel.write_msg(%d) : %d bytes\n" \
                % (self.fileno(), len(data)))
        if self.is_garbage():
            if dbg>=1:
                LOG("wchannel.write_msg(%d) : wchannel closed\n" \
                    % self.fileno())
            return -1
        header,data,trailer = self.mk_msg(data, prio)
        self.write_q.put((tag, [ header, data, trailer ]), prio)
        self.buf_len = self.buf_len + len(header) \
                       + len(data) + len(trailer)
        self.iom.mark_hot(self)
        return 0

    def write_eof(self, prio=0, tag=None):
        if dbg>=2:
            LOG("wchannel.write_eof(%d)\n" % self.fileno())
        if self.is_garbage():
            if dbg>=1:
                LOG("wchannel.write_eof(%d) : wchannel closed\n" \
                    % self.fileno())
            return -1
        self.write_q.put((tag, None), prio)
        self.iom.mark_hot(self)
        return 0

    # ------ callbacks that should be implemented ------

    def process_event(self, ev):
        pass

    # ------ internal methods ------

    def check_sum_msg__(self, data):
        # FIXME : do we really want to check sum?
        return 0

# -------------------------------------------------------------------
# listen (accept) channel
# -------------------------------------------------------------------

class achannel(channel):
    def discard(self):
        self.close()
        assert self.is_garbage()

    def has_pending_events(self):
        return 0

    def want_io(self):
        return 1
    
    def can_accept(self):
        if self.is_closed(): return 0
        R,_,_ = nointr_select.select([self], [], [], 0.0)
        if len(R) > 0:
            return 1
        else:
            return 0
        
    def do_io(self):
        if dbg>=2:
            LOG("achannel.do_io(%d) : begin\n" % self.fileno())
        assert self.can_accept()
        n,err_code,data = self.pch.accept()
        if n == -1:
            # got error
            if dbg>=2:
                LOG("achannel.do_io(%d) : "
                    "had error [%s]\n" % (self.fileno(), data))
            ev = aevent_IO_ERROR(data)
            self.discard()
        else:
            assert n == 0,n
            if dbg>=2:
                LOG("achannel.do_io(%d) : "
                    "accepted [%s]\n" % (self.fileno(), data))
            new_so,addr = data
            # new_so.so.send("heeeeeee\n")
            ev = aevent_OK(new_so, addr)
            self.set_timeout(INF)
        return ev
        
    def do_timeout(self):
        assert self.x_timelimit <= time.time() + 0.1
        if dbg>=2:
            LOG("achannel.do_io(%d) : timeout\n" % self.fileno())
        ev = aevent_TIMEOUT()
        self.discard()
        return ev

    def getsockname(self):
        return self.pch.getsockname()

    def process_event(self, ev):
        pass
        

# -------------------------------------------------------------------
# special channel for getting notification of child death
# -------------------------------------------------------------------

class resource_usage:
    def __init__(self):
        self.r = None
        import resource
        self.getrusage = resource.getrusage
        self.RUSAGE_CHILDREN = resource.RUSAGE_CHILDREN

    def get_child_rusage(self):
        r = self.getrusage(self.RUSAGE_CHILDREN)
        if self.r is None:
            self.r = (0,) * len(r)
        dr = []
        for i in range(len(self.r)):
            dr.append(r[i] - self.r[i])
        self.r = r
        return dr

class rchannel_wait_child(rchannel):
    """
    a special channel to get notification from the sigchld handler
    that a child has terminated
    """
    def __init__(self, pch):            # , daemon
        rchannel.__init__(self, pch)
        # self.daemon = daemon
        self.set_expected([("*",)])
        self.ru = resource_usage()

    def process_event(self, ev):
        assert ev.kind == ch_event.OK, ev.kind
        dead_processes = []
        while 1:
            try:
                pid,term_status = os.waitpid(-1, os.WNOHANG)
                if pid == 0: break
            except OSError,e:
                if e.args[0] == errno.ECHILD:
                    break
                else:
                    raise
            time_end = time.time()
            p = self.iom.del_process(pid)
            p.term_status = term_status
            p.rusage = self.ru.get_child_rusage()
            p.time_end = time_end
            dead_processes.append(p)
        ev.dead_processes = dead_processes
        return ev
            
    
# -------------------------------------------------------------------
# pipe constructor
# -------------------------------------------------------------------
#
# pipe_constructor (abstract class)
#  |
#  +-- pipe_constructor_pipe
#  +-- pipe_constructor_sockpair
#  +-- pipe_constructor_pty
#

class pipe_constructor:
    """
    pipe_constructor is an abstract class representing
    pipe-like communication medium between processes.

    a typical sequence for invoking subprocess is as follows.

     create some pipes
     fork
     child:
     C1: remap the child end of each pipe to a designated descriptor
     C2: close the parent end of each pipe
     C3: exec
     parent:
     P1: close the child end of each pipe
     P2: wrap the parent end of each pipe
          by a primitive_channel class
    
    This class has a set of methods to implement each of them.

    NOTE: in C1, a single instance may be remapped many times.
    NOTE: in C1, a designated file descriptor may be used, so
          we need to move that one to another descriptor.

    """
    def fileno_child(self):
        """
         return file number of the child end of this pipe
        """
        should_be_implemented_in_subclass()

    def dup_child_side(self):
        """
        remap (dup) the child end of this pipe to a free
        (new) file descriptor
        """
        should_be_implemented_in_subclass()

    def dup2_child_side(self, fd):
        """
        remap to a specific fd (dup2) the child end of
        this pipe
        """
        should_be_implemented_in_subclass()

    def close_child(self, descriptors):
        should_child_be_implemented_in_subclass()

    def close_parent(self, descriptors):
        should_be_implemented_in_subclass()

    def mk_primitive_channel(self, descriptors, blocking):
        should_be_implemented_in_subclass()
    
class pipe_constructor_pipe(pipe_constructor):
    def init(self, bufsz):
        self.r,self.w = os.pipe()
        return 0,""             # OK

    def child_file_numbers(self, usage):
        # usage : list of (r/w, file_descriptor_number)
        names = {}
        for mode,name in usage:
            if mode == "r":
                # since child reads, dup read end
                names[self.r] = 1
            elif mode == "w":
                # since child writes, dup write end
                names[self.w] = 1
            else:
                assert 0,mode
        assert len(names) == 1, usage
        return names.keys()

    def child_dup(self):
        """
        given the child uses the created pipe for the specified mode
        (read or write), child dups the appropriate file descriptor
        to fd.
        """
        if mode == "r":
            # since child reads, dup read end
            self.r = os.dup(self.r)
        elif mode == "w":
            # since child writes, dup read end
            self.w = os.dup(self.w)
        else:
            assert 0,mode

    def child_dup2(self, fd, mode):
        """
        given the child uses the created pipe for the specified mode
        (read or write), child dups the appropriate file descriptor
        to fd.
        """
        if mode == "r":
            # since child reads, dup read end
            os.dup2(self.r, fd)
        elif mode == "w":
            # since child writes, dup read end
            os.dup2(self.w, fd)
        else:
            assert 0,mode

    def child_close(self):
        """
        closes both ends of the pipe after duplicating them
        as necessary
        """
        nointr_os.close(self.r)
        nointr_os.close(self.w)

    def parent_close(self, parent_use):
        mode = parent_use[0][0]         # tricky and ugly
        if mode == "r":
            nointr_os.close(self.w)
        elif mode == "w":
            nointr_os.close(self.r)
        else:
            assert 0,mode

    def parent_mk_primitive_channel(self, mode, blocking):
        if mode == "r":
            return primitive_channel_fd(self.r, blocking)
        elif mode == "w":
            return primitive_channel_fd(self.w, blocking)
        else:
            assert 0,mode
    
class pipe_constructor_sockpair(pipe_constructor):
    """
    """
    max_bufsz = 1024 * 1024
    def get_user_name(self):
        return os.environ.get("USER", "unknown")
        
    def init(self, bufsz):
        sa = mk_non_interruptible_socket(socket.AF_UNIX,
                                         socket.SOCK_STREAM)
        name = "/tmp/%s_%d_%d_%d" \
               % (self.get_user_name(), os.getpid(), sa.fileno(),
                  random.randint(0, 1000000))
        try:
            sa.bind(name)
        except socket.error,e:
            # /tmp not writable
            if e.args[0] == errno.ENOENT or e.args[0] == errno.EACCES:
                msg = ("could not create a socket %s %s" % (name, e))
                return -1,msg
            else:
                raise
        sa.listen(1)
        w = mk_non_interruptible_socket(socket.AF_UNIX,
                                         socket.SOCK_STREAM)
        w.nointr_connect(name)
        r,_ = sa.nointr_accept()
        sa.nointr_close()
        os.remove(name)
        if bufsz == "max":
            bufsz = pipe_constructor_sockpair.max_bufsz
        if bufsz is not None:
            l = socket.SOL_SOCKET
            self.safe_setsockopt(w, l, socket.SO_SNDBUF, bufsz)
            self.safe_setsockopt(w, l, socket.SO_RCVBUF, bufsz)
            self.safe_setsockopt(r, l, socket.SO_SNDBUF, bufsz)
            self.safe_setsockopt(r, l, socket.SO_RCVBUF, bufsz)
        self.r = r
        self.w = w
        return 0,""

    def safe_setsockopt(self, so, level, buftype, target_sz):
        ok = so.getsockopt(level, buftype)
        sz = target_sz
        # INV:
        #  <= ok is safe
        #  
        for i in range(0, 10):
            try:
                # try to increase up to sz
                so.setsockopt(level, buftype, sz)
            except socket.error,e:
                if e.args[0] == errno.ENOBUFS:
                    sz = (ok + sz) / 2
                else:
                    raise
            break
        return so.getsockopt(level, buftype)

    def child_file_numbers(self, usage):
        # usage : list of (r/w, file_descriptor_number)
        names = {}
        for mode,name in usage:
            if mode == "r":
                # since child reads, dup read end
                names[self.r.fileno()] = 1
            elif mode == "w":
                # since child writes, dup write end
                names[self.w.fileno()] = 1
            else:
                assert 0,mode
        assert len(names) == 1, usage
        return names.keys()

    def xxx_child_file_number(self, usage):
        modes = {}
        for mode,n in usage:
            modes[mode] = 1
        assert len(modes) == 1
        mode = modes.keys()[0]
        if mode == "r":
            return self.r.fileno()
        elif mode == "w":
            return self.w.fileno()
        else:
            assert 0,mode
        
    def child_dup(self):
        if mode == "r":
            self.r = socket.from_fd(os.dup(self.r.fileno()), "r")
        elif mode == "w":
            self.w = socket.from_fd(os.dup(self.w.fileno()), "w")
        else:
            assert 0,mode
        
    def child_dup2(self, fd, mode):
        if mode == "r":
            os.dup2(self.r.fileno(), fd)
        elif mode == "w":
            os.dup2(self.w.fileno(), fd)
        else:
            assert 0,mode

    def child_close(self):
        self.r.nointr_close()
        self.w.nointr_close()

    def parent_close(self, parent_use):
        # parent_use : list of ("r", ???)
        mode = parent_use[0][0]         # tricky and ugly
        if mode == "r":
            self.w.nointr_close()
        elif mode == "w":
            self.r.nointr_close()
        else:
            assert 0,mode

    def parent_mk_primitive_channel(self, mode, blocking):
        if mode == "r":
            return primitive_channel_socket(self.r, blocking)
        elif mode == "w":
            return primitive_channel_socket(self.w, blocking)
        else:
            assert 0,mode
    

class pipe_constructor_pty(pipe_constructor):
    """
    """
    def init(self, bufsz):
        self.m,self.s = os.openpty()
        self.pch_m = None
        return 0,""

    def child_file_numbers(self, usage):
        return [ self.s ]
        
    def child_dup(self):
        self.s = os.dup(self.s)
        
    def child_dup2(self, fd, mode):
        os.dup2(self.s, fd)

    def child_close(self):
        nointr_os.close(self.m)
        nointr_os.close(self.s)

    def parent_close(self, parent_use):
        nointr_os.close(self.s)

    def parent_mk_primitive_channel(self, mode, blocking):
        if self.pch_m is None:
            self.pch_m = primitive_channel_fd(self.m, blocking)
        return self.pch_m
    
# -------------------------------------------------------------------
# process
# -------------------------------------------------------------------
#
# process_base
#  | |
#  | +-- child_process
#  |       |
#  |       +-- pipe_process
#  |       +-- sockpair_process

class process_base:
    """
    The process_base class is an abstract class that represents
    whatever this process talks to via some communication medium.
    It may be a child process of it, the parent process,
    or another process which connects to it.

    In terms of data structure, it simply holds a set of channels
    that are `connected to' the other process.
    """
    def __init__(self):
        self.w_channels = {}            # name -> channel
        self.w_channels_rev = {}        # channel -> name
        self.r_channels = {}            # name -> channel
        self.r_channels_rev = {}        # channel -> name
        
    def add_r_channel(self, ch, name):
        assert not self.r_channels_rev.has_key(ch)
        self.r_channels[name] = ch
        self.r_channels_rev[ch] = name

    def add_w_channel(self, ch, name):
        assert not self.w_channels_rev.has_key(ch)
        self.w_channels[name] = ch
        self.w_channels_rev[ch] = name

    def discard(self):
        should_be_implemented_in_subclasses()

    def is_garbage(self):
        should_be_implemented_in_subclasses()

    def write_channel(self):
        should_be_implemented_in_subclasses()

    def set_write_channel(self):
        should_be_implemented_in_subclasses()

    def write_stream(self, s):
        ch = self.write_channel()
        return ch.write_stream(s)
        
    def write_msg(self, m):
        ch = self.write_channel()
        return ch.write_msg(m)
        
    def write_eof(self):
        ch = self.write_channel()
        return ch.write_eof()
        
class child_process(process_base):
    """
    child_process is a child of this process, which is resulted
    by running a command. An instance is created by specifying
    a command line     (list of strings passed to exec syscall)
    and a description about how this process (i.e., parent) and
    the new process should be connected via pipes or sockets.
    """
    exec_failed = 255
    def __init__(self, cmd, pipe_desc, env, cwds, rlimits):
        """
        cmd : list given to execvp (i.e., os.execvp(cmd[0], cmd))

        pipe_desc : A list of (n, mode, channel_constructor,
        other_args).
        This entry indicates file descriptor n of the children
        should be a pipe to the parent and have the specified
        mode ('r' for the child to read, 'w' for the child
        to write). channel_constructor is used by the parent
        to make a channel object out of the parent
        side of the pipe. i.e., a channel is constructed by
        channel_constructor(primitive_channel_fd(x)) where
        x is the parent-side file descricptor number of the
        pipe connected to n of the children.

        mode = 'r':   n of the child <--- channel_constructor(...)
        mode = 'w':   n of the child ---> channel_constructor(...)
        """
        process_base.__init__(self)
        self.cmd = cmd
        self.pipe_desc = pipe_desc
        self.env = env          # environment (may be None)
        self.cwds = cwds        # list of directories 
        self.rlimits = rlimits
        self.pid = None       # process id
        self.term_status = None
        self.rusage = None
        self.time_start = None
        self.time_end = None

    def is_garbage(self):
        # a process is garbage when it terminated and
        # all read channels are garbage
        if self.term_status is None: return 0
        for ch in self.r_channels_rev.keys():
            if not ch.is_garbage(): return 0
        return 1

    def discard(self):
        self.term_status = -1
        for ch in self.r_channels_rev.keys():
            ch.discard()
        # from 2007/11/27. this fixed a bug that
        # gxpc trim did not delete child gxpd in certain
        # cases. 
        for ch in self.w_channels_rev.keys():
            ch.discard()
        self.kill()
        assert self.is_garbage()

    def write_channel(self):
        return self.w_channels[0]

    def set_write_channel(self, ch):
        self.w_channels[0] = ch

    def remap_file_descriptors(self, pipes):
        """
        pipes : list of 
        (pipe_constructor, parent_use, child_use)
        parent_use : list of (mode, name, channel_constructor)
        child_use : list of (mode, name)
        """
        # first examine which numbers are currently used
        protected = {}
        for pipe,parent_use,child_use in pipes:
            for name in pipe.child_file_numbers(child_use):
                protected[name] = pipe
        
        # pipes that can now be safely closed
        safe = {}
        for pipe,parent_use,child_use in pipes:
            for mode,name in child_use:
                if protected.has_key(name):
                    p = protected[name]
                    if not safe.has_key(p):
                        p.child_dup()
                        safe[p] = 1
                pipe.child_dup2(name, mode)
            pipe.child_close()
            safe[pipe] = 1

    def parse_smart_size(self, x):
        m = re.match("(-?[\.\d]*)([kKmMgGtTpPeE])?$", x)
        if m is None: return None
        a = m.group(1)
        b = m.group(2)
        if b is not None:
            b = string.lower(b)
        try:
            a = float(a)
        except:
            return None
        if b is None:
            pass
        elif b == "k":
            a = a * (1 << 10)
        elif b == "m":
            a = a * (1 << 20)
        elif b == "g":
            a = a * (1 << 30)
        elif b == "t":
            a = a * (1 << 40)
        elif b == "p":
            a = a * (1 << 50)
        elif b == "e":
            a = a * (1 << 60)
        else:
            assert 0
        return int(a)

    def impose_rlimits(self, rlimits):
        for rlimit in rlimits:
            self.impose_rlimit(rlimit)

    def impose_rlimit(self, rlimit):
        # rlimit is a string of the form
        # KIND:VALUE.  right now we only understand
        # strings passable to setrlimit. more specifically,
        # KIND is a string such that RLIMIT_KIND is a legitimate
        # argument to setrlimit (e.g., AS, CORE, CPU, DATA, FSIZE, ...)
        # VALUE is either NUM or NUM:NUM
        kind_value = string.split(rlimit, ":", 2)
        if len(kind_value) == 2:
            [ kind,soft_ ] = kind_value
            soft = self.parse_smart_size(soft_)
            hard = -1
        elif len(kind_value) == 3:
            [ kind,soft_,hard_ ] = kind_value
            soft = self.parse_smart_size(soft_)
            hard = self.parse_smart_size(hard_)
        else:
            soft = hard = None
        if soft is None or hard is None:
            os.write(2, 
                     "ignored invalid rlimit string (%s). "
                     "must be kind:num[:num]\n"
                     % rlimit)
            return
        k = resource.__dict__.get(string.upper(kind))
        if k is None:
            os.write(2, 
                     "ignored invalid resource (%s). must be RLIMIT_XXX "
                     "understood by setrlimit\n"
                     % kind)
            return
        try:
            resource.setrlimit(k, (soft,hard))
        except ValueError,e:
            os.write(2, 
                     "could not set resource limit %s to %s %s\n"
                     % (k, (soft, hard), e.args))
        os.write(2, 
                 "set resource limit %s to %s\n"
                 % (k, (soft, hard)))

    def run(self):
        if dbg>=2:
            LOG("child_process.run : running %s %s %s\n" \
                % (string.join(self.cmd), self.cwds, self.env))
        pipes = []
        for pipe_con,parent_use,child_use in self.pipe_desc:
            # close-on-exec flags?
            # create pipes
            pipe = pipe_con()
            err,msg = pipe.init("max")
            if err == -1:
                LOG("child_process.run : %s\n" % msg)
                return err,msg
            pipes.append((pipe,parent_use,child_use))
        # pipes.sort()
        pid = os.fork()
        if pid == -1: 
            msg = "could not fork a process"
            LOG("child_process.run : %s\n" % msg)
            return -1,msg
        if pid == 0:
            # child:
            # Arrange file descriptors so that for each entry
            # n -> 'r',
            # it has descriptor n for reading, and for each 
            # entry n -> 'w, it has descriptor n for writing.
            self.remap_file_descriptors(pipes)
            # environment
            if self.env is None:
                env = None
            else:
                env = os.environ
                for k,v in self.env.items():
                    env[k] = v
            # change dir to one of those given
            # check an error
            errors = []
            for cwd in self.cwds:  # if cwd is not None:
                # cwd = os.path.expandvars(os.path.expanduser(self.cwd))
                # cwd = self.cwd
                try:
                    os.chdir(cwd)
                    errors = [] # forget past errors
                    break
                except OSError,e:
                    errors.append((cwd, e))
                    continue
            if len(errors) > 0:
                os.write(2, ("Could not set current directory to any of %s\n" %
                             self.cwds))
                for cwd,e in errors:
                    os.write(2, "%s %s\n" % (cwd, e.args))
                os._exit(1)
            # impose rlimit
            self.impose_rlimits(self.rlimits)
            # 
            os.setpgrp()
            # From 2007. 10.25 and on, we create a process group
            # for each subprocess created by gxpd.py
            # cmd
            cmd = self.cmd
            try:
                if env is None:
                    os.execvp(cmd[0], cmd)
                else:
                    os.execvpe(cmd[0], cmd, env)
            except OSError,e:
                os._exit(child_process.exec_failed)
        else:
            # parent
            self.pid = pid
            # 5/19
            self.time_start = time.time()
            for pipe,parent_use,_ in pipes:
                # Close the useless end of the pipe.
                # mode is either "r" or "w". mode == "r" means
                # the CHILD uses the pipe to read stuff from.
                # If child uses r, we do not use r so we close
                # it. And make a channel from the used end
                # of the pipe.
                for mode,name,channel_con in parent_use: # tricky
                    pch = pipe.parent_mk_primitive_channel(mode, 0)
                    ch = channel_con(pch, self, name)
                pipe.parent_close(parent_use)
            return 0,""

    def kill(self):
        if dbg>=2:
            LOG("child_process.kill : killing %s\n" \
                % self.pid)
        if self.pid is None: return -1
        try:
            os.kill(self.pid, signal.SIGKILL)
            # os.kill(self.pid, signal.SIGINT)
            return 0
        except OSError,e:
            if dbg>=2:
                LOG("child_process.kill : failed %s\n" \
                    % (e.args,))
            return -1
        
    def kill_x(self, sig):
        if dbg>=2:
            LOG("child_process.kill : killing %s with %d\n" \
                % (self.pid, sig))
        if self.pid is None: return -1
        try:
            os.kill(self.pid, sig)
            return 0
        except OSError,e:
            if dbg>=2:
                LOG("child_process.kill : failed %s\n" \
                    % (e.args,))
            return -1
        
# -------------------------------------------------------------------
# channel for communicating with processes
# -------------------------------------------------------------------

class rchannel_process(rchannel):
    def __init__(self, pch, proc, name):
        rchannel.__init__(self, pch)
        self.proc = proc
        proc.add_r_channel(self, name)

class wchannel_process(wchannel):
    def __init__(self, pch, proc, name):
        wchannel.__init__(self, pch)
        self.proc = proc
        proc.add_w_channel(self, name)

# -------------------------------------------------------------------
# io manager
# -------------------------------------------------------------------

class ioman:
    def __init__(self):
        # INVARIANT:
        # ready_channels are channels who have pending events
        # or can perform some IOs
        self.ready_channels = []        # ready channels (read/write)
        self.timeout_channels = []      # timeout channels (read/write)
        self.hot_channels = {}          # 
        self.cold_channels = {}         # 
        self.xall_channels = {}          # w channels no msg waiting
        self.max_buf_len = 10000        # default 10KB

        # child processes (pid -> process object)
        self.processes = {}
        self.init_child_death_channel()
        
    def to_timeout(self, timelimit):    # private
        """
        timelimit -> timeout conversion. return 0.0 if
        timelimit has passed (i.e., never return negative numbers)
        """
        if timelimit == INF:
            return INF
        else:
            to = timelimit - time.time()
            if to < 0.0: to = 0.0
            return to

    def to_timelimit(self, timeout):    # private
        """
        timeout -> timelimit conversion
        """
        if timeout == INF:
            return INF
        else:
            return timeout + time.time()

    def add_wchannel(self, ch):
        """
        add a new channel to watch
        """
        if 0: self.all_channels[ch] = 1
        ch.set_ioman(self)
        self.mark_dormant(ch)

    def add_rchannel(self, ch):
        """
        add a new channel to watch
        """
        if 0: self.all_channels[ch] = 1
        ch.set_ioman(self)
        self.mark_cold(ch)              # hot?

    def mark_garbage(self, ch):
        if dbg>=2:
            LOG("mark_garbage %s\n" % ch)
        if self.hot_channels.has_key(ch):
            del self.hot_channels[ch]
        if self.cold_channels.has_key(ch):
            del self.cold_channels[ch]
        if 0: del self.all_channels[ch]

    def mark_dormant(self, ch):
        if 0: assert self.all_channels.has_key(ch)
        if self.hot_channels.has_key(ch):
            del self.hot_channels[ch]
        if self.cold_channels.has_key(ch):
            del self.cold_channels[ch]

    def mark_cold(self, ch):
        if 0: assert self.all_channels.has_key(ch)
        if self.hot_channels.has_key(ch):
            del self.hot_channels[ch]
        if not self.cold_channels.has_key(ch):
            self.cold_channels[ch] = 1

    def mark_hot(self, ch):
        if 0: assert self.all_channels.has_key(ch)
        if not self.hot_channels.has_key(ch):
            self.hot_channels[ch] = 1
        if not self.cold_channels.has_key(ch):
            self.cold_channels[ch] = 1

    def get_all_channels(self):
        """
        return list of all channels
        """
        return self.cold_channels.keys()

    def add_process(self, p):
        """
        add a new process object to watch for its death
        """
        if dbg>=2:
            LOG("ioman.add_process : "
                "added child process %d\n" % p.pid)
        self.processes[p.pid] = p

    def del_process(self, pid): # , term_status, rusage
        """
        delete a process of pid from interesting processes.
        set its status to term_status.
        """
        if not self.processes.has_key(pid):
            # This happens when the process was not created
            # by spawn_generic. ifconfig module does this.
            # In this case there is nothing we can do but ignore.
            if dbg>=2:
                LOG("ioman.del_process : "
                    "non ioman child process %d terminated\n" % pid)
            return None
        p = self.processes[pid]
        # p.term_status = term_status
        # p.rusage = rusage
        del self.processes[pid]
        if dbg>=2:
            LOG("ioman.del_process : "
                "child process %d terminated (%d left)\n" \
                % (pid, len(self.processes)))
        return p

    def spawn_generic(self, mk_process, cmd, pipe_desc, env, cwds, rlimits):
        """
        mk_process : constructor of a subclass of child_process
        cmd : a list like [ 'ssh', 'istbs001', 'hostname' ]
        pipe_desc : like [ (0,'r',constructor) ]

        spawn a new process of type child_process (or one of
        its subclass). connect to the new process via pipes,
        specified via pipe_desc. give it environment env.
        
        """
        p = mk_process(cmd, pipe_desc, env, cwds, rlimits)
        err,msg = p.run()
        if err == 0:            # OK
            self.add_process(p)
            for ch in p.w_channels_rev.keys():
                self.add_wchannel(ch)
            for ch in p.r_channels_rev.keys():
                self.add_rchannel(ch)
            return p,msg
        else:
            if dbg>=0:
                LOG("ioman.spawn_generic failed to run [%s] %s\n" % (cmd, msg))
            return None,msg

    def sigchld(self, num, frame):
        """
        Handler of SIGCHLD. See init_child_death_notification
        below.
        """
        if dbg>=2:
            LOG("ioman.sigchld : SIGCHLD received\n")
        os.write(self.fd_notify_child_death, "x")

    def init_child_death_channel(self):
        """

        A mechanism for noticing a child termination. We should
        ensure we never leave zombie for a long time and never
        busy-wait for IO.

        For this, we set a handler for SIGCHLD. When a signal
        is delivered when we are blocking-IO, the IO (select)
        will get interrupted (i.e., return) and we have a chance
        to call wait (waitpid). However, when a signal is delivered
        after we made the last call to wait and before we block
        on select, we will lose a chance to call wait for it.

        To solve this race condition, we make a pipe, let the
        signal handler put one byte msg into the pipe, and let the
        main thread watch for the other end of it.  This way, the
        delivery of a signal keeps the pipe readable until the main
        thread reads it.
        
        """
        r,w = os.pipe()                 # create pipe
        # The signal handler of SIGCHLD writes a msg to w.
        # The main loop (select) watches for r.
        pch_r = primitive_channel_fd(r, 1)
        ch_r = rchannel_wait_child(pch_r) # , self
        self.add_rchannel(ch_r)
        self.fd_notify_child_death = w
        self.ch_receive_child_death = ch_r
        signal.signal(signal.SIGCHLD, self.sigchld)

    def reset_ready_channels_aux(self, hot_only):
        """
        0 : read from parent
        1 : write to parent
        """
        if dbg>=2:
            LOG("ioman.reset_ready_channels(hot_only=%d)\n" \
                % hot_only)
        assert len(self.ready_channels) == 0, self.ready_channels
        Y = []                     # channels having pending events
        R = []                     # channels to check for readability
        W = []                     # channels to check for writability
        T = INF                    # earliest time limit
        timelimit_table = {}
        if hot_only:
            channels = self.hot_channels
        else:
            channels = self.cold_channels

        if dbg>=2:
            LOG("ioman.reset_ready_channels : checking %d channels\n" \
                % len(channels))
        total_buf_len = 0
        for ch in channels.keys():
            if ch.is_garbage():
                # delete channels 'discarded' by previous events
                self.mark_garbage(ch)
            elif ch.has_pending_events():
                # this channel may generate an event without IO
                Y.append(ch)
            elif ch.want_io() == 0:
                self.mark_dormant(ch)
            else:
                # why this is true?
                # this channel is not garbage and has no pending
                # events.
                assert ch.is_closed() == 0
                self.mark_cold(ch)
                if isinstance(ch, wchannel):
                    total_buf_len = total_buf_len + ch.buf_len
                    W.append(ch)
                else:
                    assert (isinstance(ch, rchannel) or \
                            isinstance(ch, achannel)), ch
                    R.append(ch)
                # calc earliest timelimit
                T = min(T, ch.x_timelimit)
                if T == ch.x_timelimit != INF:
                    if not timelimit_table.has_key(ch.x_timelimit):
                        timelimit_table[ch.x_timelimit] = []
                    timelimit_table[ch.x_timelimit].append(ch)
        if len(R) + len(W) + len(Y) == 0: return -1
        if hot_only or len(Y) > 0:
            timeout = 0.0
            # timeout = 0.02
        else:
            timeout = self.to_timeout(T)
            if timeout == INF: timeout = None
        if dbg>=2:
            LOG("%d has pending events "
                "%d to check read "
                "%d to check write "
                "[%s] to timeout\n" % (len(Y), len(R), len(W),
                                       timeout))
        if len(R) + len(W) == 0:
            R1 = []
            W1 = []
        elif len(W) > 0 and total_buf_len > self.max_buf_len:
            # there are some channels that want to write.
            # we try to evacuate those write channels first
            if timeout is None:
                w_timeout = 0.1
                r_timeout = None
            else:
                w_timeout = min(timeout, 0.1)
                r_timeout = timeout - w_timeout
            if dbg>=2:
                LOG("trying to evacuate %d wchans [%s]\n" \
                    % (len(W), w_timeout))
            R1,W1,_ = nointr_select.select([], W, [], w_timeout)
            if dbg>=2:
                LOG("returned with %d reads %d writes\n" % (len(R1), len(W1)))
            if len(W1) == 0:
                if dbg>=2:
                    LOG("trying to select %d rchans %d wchans [%s]\n" \
                        % (len(R), len(W), timeout))
                R1,W1,_ = nointr_select.select(R, W, [], r_timeout)
                if dbg>=2:
                    LOG("returned with %d reads %d writes\n" \
                        % (len(R1), len(W1)))
        else:
            if dbg>=2:
                LOG("trying to select %d rchans %d wchans [%r]\n" \
                    % (len(R), len(W), timeout))
            assert timeout is None or type(timeout) is types.FloatType, (timeout, type(timeout))
            R1,W1,_ = nointr_select.select(R, W, [], timeout)
            if dbg>=2:
                LOG("returned with %d reads %d writes\n" % (len(R1), len(W1)))
        if hot_only or len(R1) + len(W1) + len(Y) > 0:
            # set ready channels
            for ch in R1: self.mark_hot(ch)
            for ch in W1: self.mark_hot(ch)
            for ch in Y:  self.mark_hot(ch)
            self.ready_channels = W1 + Y + R1
            self.timeout_channels = []
            if dbg>=2:
                LOG("%d pending events "
                    "%d readable "
                    "%d writable\n" % (len(Y), len(R1), len(W1)))
        else:
            self.ready_channels = []
            cur = time.time()
            assert T != ""
            assert (T <= cur + 0.1), (T, cur)
            assert len(timelimit_table[T]) > 0
            self.timeout_channels = timelimit_table[T]
            if dbg>=2:
                LOG("no ready channels %d timeouts\n" \
                    % len(self.timeout_channels))
        return 0
        
    def reset_ready_channels(self):
        return self.reset_ready_channels_aux(0)
        if self.reset_ready_channels_aux(1) == 0:
            return 0
        return self.reset_ready_channels_aux(0)

    def process_an_event(self):
        """
        return a tuple (channel,event) indicating the event
        happened on channel.
        """
        if dbg>=2:
            LOG("ioman.process_an_event : %d ready %d timeouts\n" \
                % (len(self.ready_channels),
                   len(self.timeout_channels)))
        while 1:
            if len(self.ready_channels) > 0:
                ch = self.ready_channels.pop(0)
                # this happens when we have gotten action_close
                if ch.is_garbage(): continue
                # assert ch.is_garbage() == 0
                ev = ch.do_io()
                if ev is None:
                    continue
                else:
                    break
            
            if len(self.timeout_channels) > 0:
                ch = self.timeout_channels.pop(0)
                # this happens when we have gotten action_close
                if ch.is_garbage(): continue
                # assert ch.is_garbage() == 0
                ev = ch.do_timeout()
                break
                
            if self.reset_ready_channels() == -1:
                ch,ev = None,None
                break
        if ev is not None: ch.process_event(ev)
        return ch,ev

    # somewhat higher-level API
    def add_read_fd(self, fd):
        ch = rchannel(primitive_channel_fd(fd, 1))
        self.add_rchannel(ch)
        return ch
    def add_write_fd(self, fd):
        ch = wchannel(primitive_channel_fd(fd, 1))
        self.add_wchannel(ch)
        return ch
    def add_sock(self, so):
        pso = primitive_channel_socket(so, 1)
        rch = rchannel(pso)
        wch = rchannel(pso)
        self.add_rchannel(rch)
        self.add_wchannel(wch)
        return rch,wch
    def make_client_sock(self, so_af, so_type, addr):
        so = mk_non_interruptible_socket(so_af, so_type)
        pso = primitive_channel_socket(so, 1)
        r,e = pso.connect(addr)
        if r != 0:
            raise e
        else:
            rch = rchannel(pso)
            wch = wchannel(pso)
            self.add_rchannel(rch)
            self.add_wchannel(wch)
            return rch,wch
    def make_server_sock(self, so_af, so_type, addr, qlen):
        so = mk_non_interruptible_socket(so_af, so_type)
        so.bind(addr)
        bound_addr = so.getsockname()
        so.listen(qlen)
        ch = achannel(primitive_channel_socket(so, 1))
        self.add_rchannel(ch)
        return ch

    def make_child_proc(self, cmd, make_pipe=1, env=None, cwds=None, rlimits=None):
        if env is None: env = {}
        if cwds is None: cwds = []
        if rlimits is None: rlimits = []
        if make_pipe == 1:
            # pipe_desc : list of (pipe_con,parent_use,child_use)
            # parent_use : list of (mode,name,channel_con)
            # child_use : list of (mode,name)
            pipe_desc = [ (pipe_constructor_pipe, 
                           [ ('w', 0, wchannel_process) ],
                           [ ('r', 0) ]),
                          (pipe_constructor_pipe, 
                           [ ('r', 1, rchannel_process) ],
                           [ ('w', 1) ]),
                          (pipe_constructor_pipe, 
                           [ ('r', 2, rchannel_process) ], 
                           [ ('w', 2) ]) ]
        else:
            pipe_desc = []
        proc,msg = self.spawn_generic(child_process, cmd, pipe_desc, env, cwds, rlimits)
        if proc is None:
            raise Exception(msg)
        else:
            return proc

#
# test functions
#

def test_achan():
    global dbg
    dbg = 2
    m = ioman()
    so = mk_non_interruptible_socket(socket.AF_INET,
                                     socket.SOCK_STREAM)
    so.bind(("",0))
    so.listen(1)
    ch = achannel(primitive_channel_socket(so, 1))
    m.add_rchannel(ch)
    print so.getsockname()
    m.process_an_event()

def mk_msg(data):
    h,d,t = wchannel(None).mk_msg(data, 0)
    return h + d + t

def test_recv_msg():
    global dbg
    dbg = 2
    m = ioman()
    fd = os.open("msg", os.O_RDONLY)
    ch = rchannel(primitive_channel_fd(fd, 0))
    ch.set_expected([("M",)])
    m.add_rchannel(ch)
    ch,ev = m.process_an_event()
    print "got %s <%s>" % (ev.kind, ev.data)

def test_recv_lines():
    global dbg
    dbg = 2
    m = ioman()
    fd = os.open("lines", os.O_RDONLY)
    ch = rchannel(primitive_channel_fd(fd, 0))
    ch.set_expected(["\n"])
    m.add_rchannel(ch)
    while 1:
        ch,ev = m.process_an_event()
        if ev.kind == ch_event.EOF: break
        print "got %s <%s>" % (ev.kind, ev.data)

if 0 and __name__ == "__main__":
    # just for test
    test_recv_msg()

# $Log: ioman.py,v $
# Revision 1.18  2013/10/25 12:00:54  ttaauu
# added higher-level APIs to ioman and a document for it
#
# Revision 1.17  2010/09/30 18:40:02  ttaauu
# *** empty log message ***
#
# Revision 1.16  2010/05/25 18:13:58  ttaauu
# support --translate_dir src,dst1,dst2,... and associated changes. ChangeLog 2010-05-25
#
# Revision 1.15  2010/05/20 14:56:56  ttaauu
# e supports --rlimit option. e.g., --rlimit rlimit_as:2g ChangeLog 2010-05-20
#
# Revision 1.14  2010/05/20 05:53:29  ttaauu
# *** empty log message ***
#
# Revision 1.13  2010/05/19 03:41:10  ttaauu
# gxpd/gxpc capture time at which processes started/ended at remote daemons. xmake now receives and displays them. xmake now never misses IO from jobs. ChangeLog 2010-05-19
#
# Revision 1.12  2010/05/13 13:48:59  ttaauu
# work_db_mem becomes smart and now default
#
# Revision 1.11  2010/05/12 14:00:30  ttaauu
# added select_by_poll implementation by Kamoshida. ChangeLog 2010-05-12
#
# Revision 1.10  2010/05/11 08:02:35  ttaauu
# *** empty log message ***
#
# Revision 1.9  2010/05/09 04:55:29  ttaauu
# *** empty log message ***
#
# Revision 1.8  2009/09/17 18:47:53  ttaauu
# ioman.py,gxpm.py,gxpd.py,gxpc.py,xmake: changes to track rusage of children and show them in state.txt
#
# Revision 1.7  2009/09/06 20:05:46  ttaauu
# lots of changes to avoid creating many dirs under ~/.gxp_tmp of the root host
#
# Revision 1.6  2009/06/06 14:06:23  ttaauu
# added headers and logs
#
