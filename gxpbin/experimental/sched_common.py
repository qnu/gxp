#import commands
#import getopt
import Queue
import errno,os,random,re,select,socket,string,sys,time,struct,stat,threading,signal

debug = 0

def Es(s):
    sys.stderr.write(s)
    sys.stderr.flush()

def Ws(s):
    sys.stdout.write(s)
    sys.stdout.flush()


def foo():
    Es("...\n")
    time.time()
    Es("...\n")


def safe_atoi(s):
    try:
        n = string.atoi(s)
        return n
    except ValueError:
        return None


def open_for_read(f):
    if f == "-": return sys.stdin
    try:
        fp = open(f, "rb")
        return fp
    except IOError:
        Es("Could not open %s for reading\n" % f)
        os._exit(1)
        

def open_for_write(f):
    if f == "-": return sys.stdin
    try:
        fp = open(f, "wb")
        return fp
    except IOError:
        Es("Could not open %s for writing\n" % f)
        os._exit(1)
        

def open_for_append(file):
    try:
        fp = open(file, "ab")
        return fp
    except IOError:
        Es("Could not open %s for writing\n" % file)
        os._exit(1)


def mk_host_regexp(p):
    # gxp like istbs** --> istbs.*
    # quote '.'
    p1 = string.replace(p,   ".", "\.")
    # ** matches everytihng including '.'
    p2 = string.replace(p1, "**", r".+")
    # * matches everytihng except '.'
    p3 = string.replace(p2, "*", r"[^\.]+")
    # ? matches a character except '.'
    p4 = string.replace(p3, "?", r"[^\.]")
    #
    p5 = string.replace(p4, "+", r"*")
    # return re.compile(p5 + "$")
    return re.compile(p5)


class GxpPtn:
    def __init__(self, ptn_str):
        self.ptns = []
        self.values = []
        if ptn_str == "": return
        for s in ptn_str.split():
            try:
                A = string.split(s, ":", 1)
                if len(A) == 1:
                    self.append("**", A[0])
                else:
                    self.append(A[0],A[1])
            except:
                Es("Failed to parse %s\n"%ptn_str)
            
    def append(self, ptn_s, value):
        ptn = mk_host_regexp(ptn_s)
        self.ptns.append(ptn)
        self.values.append(value)

            
    def get(self, s):
        for i,ptn in enumerate(self.ptns):
            if ptn.match(s):
                return self.values[i]
            
                                                                                                    
#------- SOCKET

class Socket:
    """ Wrapped Socket """
    def __init__(self, sock=None, peer=None):
        """ Create a socket """
        self.sock = sock
        self.peer = peer
        if self.sock is None:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    def connect(self, peer):
        """ Establish a connection (Peer: (host, port))"""
        assert peer is not None
        self.peer = peer
        self.sock.connect(self.peer)
        if self.sock is None:
            raise "Could not establish a connection to %s:%d"%(self.peer[0], self.peer[1])
        if debug >= 2:
            Es("New connection to %s:%d\n"%(self.peer[0], self.peer[1]))
        
    def close(self):
        """ Close the socket """
        self.sock.close()
        
    def send(self, msg, msglen = -1):
        """ Safe send() """
        if msglen == -1: 
            msglen = len(msg)
        totalsent = 0
        while totalsent < msglen:
            sent = self.sock.send(msg[totalsent:])
            if sent == 0:
                raise RuntimeError, \
                      "socket connection broken"
            totalsent = totalsent + sent

    def recv(self, msglen):
        """ Safe recv() """
        msg = ''
        while len(msg) < msglen:
            chunk = self.sock.recv(msglen-len(msg))
            if chunk == '':
                raise RuntimeError, \
                    "socket connection broken"
            msg = msg + chunk
        return msg

    def rawrecv(self, msglen):
        try:
            return self.sock.recv(msglen)
        except:
            Es("failed to rawrecv (msglen=%d)"% msglen)
            sys.exit(1)

    def sendInt(self, value):
        self.send(struct.pack('i', value))

    def recvInt(self):
        msg = self.recv(struct.calcsize('i'))
        return struct.unpack('i', msg)[0]


class ServerSocket:
    def __init__(self, initial_port=10000):
        """ Create a socket """
        """  peer : (hostname, port) """
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        for self.port in range(initial_port, 65535):
            try:
                self.sock.bind((socket.gethostbyname(socket.gethostname()), self.port))
            except socket.error, msg:
                #Es("Err: on bind(): %s\n"%msg)
                continue
            break
        if self.port == 65534:
            raise Exception("Failed to bind any port")
        self.sock.listen(5) # arg : number of backlog

        
    def accept(self):
        conn,peer = self.sock.accept()
        #if debug >= 2 :
        #    Es("Accepted from %s:%d\n"%(peer[0], peer[1]))
        return Socket(sock=conn, peer=peer)

    def close(self):
        self.sock.close()

#-------END OF SOCKET


#-------TMPDIR
# class Fileman:
#     """ File Manager """
#     def __init__(self, datadir, tmpdir):
#         self.hostnames = ifconfig().get_my_addrs()
#         self.home = os.environ["HOME"]
#         self.user = os.environ["USER"]
#         self.createDataDir()
#         self.createOutputDir()
        
    
#     def getNowstr(self):
#         return reduce(lambda x, y:x+'_'+repr(y), time.localtime()[0:6], "")[1:]
    
#     def createDataDir(self):
#         # [basedir]/sched_tmp/[date]_[time]/**
#         base = self.chooseDatadir()
#         now_str = self.getNowstr()
#         dn = "%s/%s/sched_tmp/%s"%(base, self.user, now_str)
#         self.ensure_dir(dn)
#         self.datadir = dn


#     def createOutputDir(self):
#         # [homedir]/sched_out/[date]_[time]/**
#         base = self.home
#         now_str = self.getNowstr()
#         dn = "%s/output"%(base)
#         #dn = "output"
#         self.ensure_dir(dn)
#         self.outputdir = dn







class ifconfig:
    """
    ifconfig().get_my_addrs() will return a list of
    IP addresses of this host
    """
    def __init__(self):
        self.v4ip_pat_str = "(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})"
        self.v4ip_p = re.compile(self.v4ip_pat_str)
        self.ifconfig_list = [ "LANG=C /sbin/ifconfig -a 2> /dev/null" ]

    def parse_ip_addr(self, addr):
        """
        '12.34.56.78' --> (12,34,56,78)
        """
        m = re.match(self.v4ip_pat_str, addr)
        if m is None: return None
        A,B,C,D = m.group(1,2,3,4)
        try:
            return string.atoi(A),string.atoi(B),string.atoi(C),string.atoi(D)
        except ValueError,e:
            return None

    def is_global_ip_addr(self, addr):
        a,b,c,d = self.parse_ip_addr(addr)
        if a == 192 and b == 168: return 0
        if a == 172 and 16 <= b < 32: return 0
        if a == 10: return 0
        if a == 127: return 0
        return 1

    def is_local_ip_addr(self, addr):
        if addr == "127.0.0.1":
            return 1
        else:
            return 0

    def is_private_ip_addr(self, addr):
        a,b,c,d = self.parse_ip_addr(addr)
        if a == 192 and b == 168: return 1
        if a == 172 and 16 <= b < 32: return 1
        if a == 10: return 1
        return 0

    def is_global_ip_addr(self, addr):
        if self.is_local_ip_addr(addr): return 0
        if self.is_private_ip_addr(addr): return 0
        return 1

    def in_same_private_subnet(self, P, Q):
        """
        true when (1) both P and Q are private, and (2) they belong
        to the same subnets.
        """
        if self.is_private_ip_addr(P) == 0: return 0
        if self.is_private_ip_addr(Q) == 0: return 0
        a,b,c,d = self.parse_ip_addr(P)
        A,B,C,D = self.parse_ip_addr(Q)
        if a == 192 and b == 168     and A == 192 and B == 168: return 1
        if a == 172 and 16 <= b < 32 and A == 172 and 16 <= B < 32: return 1
        if a == 10 and A == 10: return 1
        return 0

    def guess_connectable(self, P, Q):
        """
        P : address, Q : endpoint name.
        (e.g.,
        P = '133.11.238.3',
        Q = ('tcp', (('157.82.246.104', '192.168.1.254'), 59098))
        )
        """
        # consider * -> private is allowed
        # Qas is like ('157.82.246.104', '192.168.1.254')
        proto, (Qas, Qp) = Q            
        Qa = Qas[0]
        if self.is_global_ip_addr(Qa): return 1
        # consider global -> private is blocked
        if self.is_global_ip_addr(P): return 0
        if self.in_same_private_subnet(P, Qa): return 1
        return 0

    def get_my_addrs_by_ifconfig(self, hint=None):
        patterns = [ re.compile("inet addr:([\d|\.]+)"),
                     re.compile("inet ([\d|\.]+)") ]
        addrs = []
        for c in self.ifconfig_list:
            r = os.popen(c)
            # blocking=1
            # pr = ioman.primitive_channel_fd(r.fileno(), 1)
            # ifconfig_out = r.read()
            if 0:
                ifconfig_outs = []
                while 1:
                    l,err,msg = pr.read(1000)
                    if l <= 0: break
                    ifconfig_outs.append(msg)
                ifconfig_out = string.join(ifconfig_outs, "")
            ifconfig_out = r.read()
            for p in patterns:
                found = p.findall(ifconfig_out)
                if len(found) > 0: break
            for addr in found:
                if self.parse_ip_addr(addr) is not None:
                    addrs.append(addr)
            # ??? we got:
            # "close failed: [Errno 9] Bad file descriptor"
            r.close()
            # pr.close()
            if len(addrs) > 0: return addrs
        return []

    def get_my_addrs_by_lookup(self, hint=None):
        if hint is None:
            hostname = socket.gethostname()
        else:
            hostname = hint
        try:
            can_name,aliases,ip_addrs = socket.gethostbyname_ex(hostname)
        except socket.error,e:
            return []
        return ip_addrs
        
    def sort_addrs(self, ip_addrs):
        """
        1. remove duplicates.
        2. remove loopback addrs (127.0.0.1).
        3. sort IP addresses.
           global addrs first
        """
        a = {}
        for ip in ip_addrs:
            a[ip] = 1
        to_sort = []
        for ip in a.keys():
            if self.is_local_ip_addr(ip):
                # exclude '127.0.0.1'
                continue
            elif self.is_private_ip_addr(ip):
                # put addrs like '192.168.XX.XX' at the end
                to_sort.append((1, len(to_sort), ip))
            else:
                # put global ip addrs first
                to_sort.append((0, len(to_sort), ip))
        to_sort.sort()
        sorted_ip_addrs = []
        for _,_,ip in to_sort:
            sorted_ip_addrs.append(ip)
        return sorted_ip_addrs

    def get_my_addrs(self, hint=None):
        """
        get my ip address the clients are told to connect to.
        """
        # first by ifconfig
        A = self.get_my_addrs_by_ifconfig(hint=hint)
        if len(A) > 0: return self.sort_addrs(A)
        # second by looking up my hostname
        A = self.get_my_addrs_by_lookup(hint=hint)
        if len(A) > 0: return self.sort_addrs(A)
        return []


#----------END OF TMPDIR


class Task:
    # the format of a line:
    #    taskname [on regexp]; [after taskname1 taskname2 ..] ;\
    #     [require host0:filename0 host1:filename1]; \
    #     [generate host0:file0] ; command
    #
    # Example:
    # t1 on .+; after t0; 
    #
    # comment line starts with "#"

    # match against 'taskname whatever_is_remaining'
    p = re.compile("\s*([^\s:#]+)\s*:(.*)")

    # match a single option
    #   :on XXXX
    #   :after XXXX
    #   :require XXXX XXXX
    q = re.compile("\s*(on|after|require|generate)\s+(.*)")

    def __init__(self, task_expr):
        self.line = task_expr
        self.parse(task_expr)


    def parse(self, line):
        try:
            # parse the entire line
            m = Task.p.match(line)
            if m is None:
                self.name = ""
                return
            name,rest = m.group(1,2) # Split the taskname and the rest
            elems = rest.split(";")

            cmd = ""
            opts = { "on" : "**", "after" : [], "require" : [], "generate" : [] }
            
            for i,elem in enumerate(elems):
                # parse elem
                m = Task.q.match(elem)
                if m is None:
                    # The command starts here to the end
                    cmd = (";".join(elems[i:])).strip()
                    break
                kw,opt = m.group(1,2)
                opt = string.strip(opt)
                if kw == "on":
                    opts[kw] = opt
                elif kw == "after":
                    opts[kw] = string.split(opt)
                elif kw == "require":
                    # The first expression does not contain $TMPDIR kind of expressions
                    # but it uses istbs:/tmp/hoge kind. 
                    # In this class, we convert hostname to address but does not convert 
                    # it to a path including tmpdir 
                    # 
                    # When the task is executed at a certain node, and when it tries 
                    # to open remote file expressed like "111.1.11.1:/tmp/hoge", the path
                    # is converted to the real path (replica path) like
                    # /my/tmp/dir/111_1_11_1+_tmp_hoge
                    for f in string.split(opt):
                        host,path = f.split(":",1)
                        #TODO: cache the result
                        addr = socket.gethostbyname(host)
                        opts[kw].append("%s:%s"%(addr,path))

                elif kw == "generate":
                    for f in string.split(opt):
                        # Convert hostanme to addr
                        host,path = f.split(":",1)
                        addr = socket.gethostbyname(host)
                        opts[kw].append("%s:%s"%(addr,path))
                        
                else:
                    Es("Invalid tag: %s\n"%kw)
                    self.name = ""
                    return
                    
            self.name = name 
            self.constraint_regexp = mk_host_regexp(opts["on"])
            self.constraint_str = opts["on"]
            self.predecessors = opts["after"]
            self.reqs = opts["require"]
            self.gens = opts["generate"]
            self.cmd = cmd
            
        except IOError, e:
            Es("Invalid line\n")
            self.name = ""


    def __str__(self):
        line = ""
        line += "%s: "%self.name
        line += "on %s;"%self.constraint_str
        line += "after %s;"%(" ".join(self.predecessors))
        line += "require %s;"%(" ".join(self.reqs))
        line += "generate %s;"%(" ".join(self.gens))
        line += self.cmd
        return line


class Log: #OKAY
    def __init__(self, fn):
        self.fn = fn
        self.fp = open_for_append(self.fn)

    def close(self):
        self.fp.close()
        
    def write(self, line):
        """
        record that a task NAME has finished with status (and some other info) = val
        """
        self.fp.write(line)
        self.fp.flush()



#def task_test():
#    s = "x11: on .+;after ;require ;generate ;hostname; date"
#    t = Task(s)
#    print t.cmd

#task_test()

