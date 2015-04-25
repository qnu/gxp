############################################################
# from util.py
############################################################
import sys, os, socket, re, string, time, stat, struct, select, math, urllib


## OPTIONS
debug=0

TOUCHGRAPH_LOG_FN = "ucp_tg.dat" # Output log for touchgraph (if None or "" are set )
TIME_1 = False  # Calculate bandwidth only after the first packet has arrived
PRETEND = False # Send dummy data, and do not read/write a file
SNDBUF_SIZE = 32 * 1024 * 1024
RCVBUF_SIZE = 32 * 1024 * 1024
CHUNKSIZE = 10 * 1024 * 1024

#################################################################
## consts
LARGE_VALUE = 10000000


class MyException(Exception):
    def __init__(self, msg=None):
        self.msg = msg
    def __str__(self):
        return "Error: %s"%(self.msg)

class MWException(Exception):
    pass


#############################################
# UTIL FUNCTIONS 
#############################################
hostname = None

def M(msg):
    global hostname
    if hostname is None:
        hostname = socket.gethostname()
    sys.stderr.write(hostname+': '+msg)
    sys.stderr.flush()


def Es(msg):
    sys.stderr.write(msg)
    sys.stderr.flush()


def cmd_readlines(cmd):
    fp = popen(cmd)
    ret = fp.readlines()
    fp.close()
    os.wait()
    return ret


def atoi_ex(s, msg):
    try:
        n = string.atoi(s)
        return n
    except ValueError:
        raise MyException("%s is not a number (%s)"%(msg,s))


def atof_ex(s, msg):
    try:
        n = string.atof(s)
        return n
    except ValueError:
        raise MyException("%s is not a float (%s)"%(msg,s))


def open_for_read(f):
    if f == "-": return sys.stdin
    try:
        fp = open(f, "rb")
        return fp
    except IOError:
        M("Could not open %s for reading\n" % f)
        os._exit(1)

        
def open_for_append(f):
    try:
        fp = open(f, "ab")
        return fp
    except IOError:
        M("Could not open %s for reading\n" % f)
        os._exit(1)
        

def open_for_write(f):
    if f == "-": return sys.stdin
    try:
        fp = open(f, "wb")
        return fp
    except IOError:
        M("Could not open %s for writing\n" % f)
        os._exit(1)

def open_for_readwrite(f):
    if f == "-": return sys.stdin
    try:
        fp = open(f, "r+w")
        return fp
    except IOError:
        M("Could not open %s for readwrite\n" % f)
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


                                                            
# ----------------------------------------------------
# get my IP address (from kiwi:/home/tau/cvs/co )
# ----------------------------------------------------

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
        true when (1) both P and Q are private, and (2) they belong to
        different subnets.
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
        proto, (Qas, Qp) = Q            # Qas is like ('157.82.246.104', '192.168.1.254')
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
            ifconfig_out = r.read()
            for p in patterns:
                found = p.findall(ifconfig_out)
                if len(found) > 0: break
            for addr in found:
                if self.parse_ip_addr(addr) is not None:
                    addrs.append(addr)
            r.close()
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


def my_gethostname():
    return socket.gethostname()


def my_getaddr(name = None):
    if name is None: name = my_gethostname()
    conv = {"kyoto-charlie" : "130.54.8.63",
            "kyoto000" : "130.54.8.64",
            "kyoto001" : "130.54.8.65",
            "kyoto002" : "130.54.8.66",
            "kyoto003" : "130.54.8.67",
            "imade-charlie" : "130.54.22.80",
            "imade000" : "130.54.22.81",
            "imade001" : "130.54.22.82",
            "imade002" : "130.54.22.83"            
            }
    for h in conv:
        if name.startswith(h):
            return conv[h]
    #return socket.gethostbyname(name)
    return ifconfig().get_my_addrs()[0]
        



class Comm:
    def __init__(self, to_fno, from_fno):
        self.to_fno = to_fno
        self.from_fno = from_fno
        
    def read(self):
        try:
            alen = os.read(self.from_fno, struct.calcsize("i"))
            if len(alen) == 0: 
                #M("MW Error\n")
                sys.exit(1)
            ilen = struct.unpack("i",alen)[0]
            msg = os.read(self.from_fno, ilen)
            return msg
        except Exception, e:
            M(str(e))
            #M("MW Error\n")
            sys.exit(1)
            
    def write(self, msg):
        try:
            os.write(self.to_fno, struct.pack("i",len(msg)))
            os.write(self.to_fno, msg)
        except:
            M("MW Error\n")
            sys.exit(1)

