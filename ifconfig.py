# Copyright (c) 2009 by Kenjiro Taura. All rights reserved.
# Copyright (c) 2008 by Kenjiro Taura. All rights reserved.
# Copyright (c) 2007 by Kenjiro Taura. All rights reserved.
# Copyright (c) 2006 by Kenjiro Taura. All rights reserved.
# Copyright (c) 2005 by Kenjiro Taura. All rights reserved.
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
# $Header: /cvsroot/gxp/gxp3/ifconfig.py,v 1.4 2010/07/01 10:33:57 ttaauu Exp $
# $Name:  $
#

import fcntl,os,re,socket,string,sys

try:
    import ioman
    # gxp3 directory is in your PYTHONPATH
except ImportError,e:
    # gxp3 directory not in your PYTHONPATH
    # perhaps run as a standalone program
    ioman = None

class ifconfig:
    """
    ifconfig().get_my_addrs() will return a list of
    IP addresses of this host
    """
    def __init__(self):
        self.v4ip_pat_str = "(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})"
        self.v4ip_p = re.compile(self.v4ip_pat_str)
        self.ifconfig_cmds = [ "LANG=C /sbin/ifconfig -a 2> /dev/null" ]

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

    def is_localhost_ip_addr(self, addr):
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
        if self.is_localhost_ip_addr(addr): return 0
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

    def get_addr_of_if_by_proc(self, ifname):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            result = fcntl.ioctl(s.fileno(), 0x8915, #SIOCGIFADDR
                                 (ifname+'\0'*32)[:32])
        except IOError:
            s.close()
            return None
        s.close()
        return socket.inet_ntoa(result[20:24])

    def get_my_addrs_by_proc_net_dev(self):
        dev = "/proc/net/dev"
        if not os.path.exists(dev): return []
        fp = open(dev, "rb")
        # skip header(2 lines)
        line = fp.readline()
        line = fp.readline()
        addrs = []
        while 1:
            line = fp.readline()
            if line == "": break
            # line looks like:
            # "  eth0:323457502 5053463    0    0    0     0   ..."
            ifname_rest = string.split(string.strip(line), ":", 1)
            if len(ifname_rest) == 2:
                addr = self.get_addr_of_if_by_proc(ifname_rest[0])
                if addr is not None:
                    addrs.append(addr)
        fp.close()
        return addrs

    def get_my_addrs_by_ifconfig(self):
        patterns = [ re.compile("inet addr:([\d|\.]+)"),
                     re.compile("inet ([\d|\.]+)") ]
        addrs = []
        for c in self.ifconfig_cmds:
            r = os.popen(c)
            if ioman is None:
                ifconfig_out = r.read()
            else:
                pr = ioman.primitive_channel_fd(r.fileno(), 1)
                ifconfig_outs = []
                while 1:
                    l,err,msg = pr.read(1000)
                    if l <= 0: break
                    ifconfig_outs.append(msg)
                ifconfig_out = string.join(ifconfig_outs, "")
            for p in patterns:
                found = p.findall(ifconfig_out)
                if len(found) > 0: break
            for addr in found:
                if self.parse_ip_addr(addr) is not None:
                    addrs.append(addr)
            # ??? we got:
            # "close failed: [Errno 9] Bad file descriptor"
            # r.close()
            # pr.close()
            if len(addrs) > 0: return addrs
        return []

    def get_my_addrs_by_lookup(self):
        hostname = socket.gethostname()
        try:
            can_name,aliases,ip_addrs = socket.gethostbyname_ex(hostname)
        except socket.error,e:
            return []
        return ip_addrs
        
    def get_addr_prio(self, ip, prio):
        """
        prio: list of compiled regexps.
        return the first index of regexp in prio
        that match ip. if none match, return the 
        length of the list.
        hereby we give priority to ip
        """
        i = 0
        for sign,regexp in prio:
            if regexp in ("p", "P"):
                m = self.is_private_ip_addr(ip)
            elif regexp in ("g", "G"):
                m = self.is_global_ip_addr(ip)
            elif regexp in ("l", "L"):
                m = self.is_localhost_ip_addr(ip)
            else:
                m = regexp.match(ip)
            if (sign and m) or (sign == 0 and not m): return i
            i = i + 1
        return len(prio)

    def sort_addrs(self, ip_addrs, addrs_prio):
        """
        addrs_prio is a list of regexps that will be
        matched against ip_addrs

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
            # p1 : primary priority
            # p2 : secondary priority (127.0.0.1 has a lower prio)
            # p3 : ip string itself (to avoid non-determinism)
            p1 = self.get_addr_prio(ip, addrs_prio)
            if self.is_localhost_ip_addr(ip):
                p2 = 1
            else:
                p2 = 0
            to_sort.append((p1, p2, ip))
        to_sort.sort()
        sorted_ip_addrs = []
        for _,_,ip in to_sort:
            sorted_ip_addrs.append(ip)
        return sorted_ip_addrs

    def get_my_addrs_aux(self):
        """
        get my ip address the clients are told to connect to.

        addr_filters is a list of strings, each of which is the form

           +REGEXP
           -REGEXP
           REGEXP   (equivalent to +REGEXP)
        """
        # look at /proc/net/dev on Linux
        A = self.get_my_addrs_by_proc_net_dev()
        if len(A) > 0: return A
        # use ifconfig command
        A = self.get_my_addrs_by_ifconfig()
        if len(A) > 0: return A
        # second by looking up my hostname
        A = self.get_my_addrs_by_lookup()
        if len(A) > 0: return A
        return []

    def get_my_addrs(self, addr_prio):
        addrs = self.get_my_addrs_aux()
        return self.sort_addrs(addrs, addr_prio)

    def compile_prio(self, addr_prio_str):
        prio = []
        for pat in string.split(addr_prio_str, ","):
            pat = string.strip(pat)
            if pat == "": continue
            if pat[0] == "+":
                sign = 1
                pat = pat[1:]
            elif pat[0] == "-":
                sign = 0
                pat = pat[1:]
            else:
                sign = 1

            if pat in ("p", "P", "g", "G", "l", "L"):
                c = pat
            else:
                try:
                    c = re.compile(pat)
                except re.error,e:
                    msg = ("invalid addr_prio %s in %s %s"
                           % (pat, addr_prio_str, e.args))
                    return None,msg
            prio.append((sign, c))
        return prio,None

ifobj = ifconfig()

def get_my_addrs(addr_prio):
    return ifobj.get_my_addrs(addr_prio)

def compile_prio(addr_prio_str):
    return ifobj.compile_prio(addr_prio_str)

def get_my_addrs2(addr_prio_str):
    prio,msg = ifobj.compile_prio(addr_prio_str)
    assert (msg is None), msg
    return ifobj.get_my_addrs(prio)

# $Log: ifconfig.py,v $
# Revision 1.4  2010/07/01 10:33:57  ttaauu
# *** empty log message ***
#
# Revision 1.3  2009/06/06 14:06:23  ttaauu
# added headers and logs
#
