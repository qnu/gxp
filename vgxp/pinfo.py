#!/usr/bin/env python

import sys, os, os.path, socket, time, random, glob, re, pwd, errno, select, urllib

"""
/proc/stat
cpu <user> <nice> <system> <idle> <iowait> <irq> <softirq> ...
- user: normal processes executing in user mode
- nice: niced processes executing in user mode
- system: processes executing in kernel mode
- idle: twiddling thumbs
- iowait: waiting for I/O to complete
"""

DELETE_TIMEOUT = 20
flg_print_meminfo = True
flg_print_stateinfo = True
flg_print_netinfo = True
flg_print_diskinfo = True
flg_print_pinfo = True
poll_interval = 2.0

class procinfo:
    def __init__ ( self, _u, _p, _g, _c ):
        self.uid = _u
        self.ppid = _p
        self.pgid = _g
        self.cmd = _c
        self.sent = None

class pinfo_default_log_handler:
    def handle_log ( self, line ):
        print line
        sys.stdout.flush ()

class pinfo_common:
    def __init__ ( self, handler = pinfo_default_log_handler () ):
        self.hn = socket.gethostname ().split ( '.', 1 ) [ 0 ]
        self.process_table = {}
        self.cputimes = [ {}, {} ]
        self.btime = self.get_btime ()
        self.log_handler = handler
        self.netbytes = [(0,0),(0,0)]
        self.diskpages = [None, None]
        self.nfork = [None, None]
        self.nfork_diff = 0
        self.cpustat = [None, None]
        self.cpustat_diff = [0,0,0,0,0]
        self.uid_table = {}

        self.state_dir = '/data/local/yuuki-s/tmp/'
        self.stateconfig_mtime = 0
        self.stateconfig_content = ''
        self.state_mtime = 0
        self.state_content = ''
        self.pgid_mtime = 0
        self.pgid_content = ''
        try:
            os.stat(self.state_dir + 'state.log')
        except:
            self.state_dir = '/home/yuuki-s/charlie-log/tmp/'
            try:
                os.stat(self.state_dir + 'state.log')
            except:
                # sys.stderr.write("state_dir not found!\n");
                self.state_dir = None
        #sys.stderr.write("state_dir is %s\n" % (self.state_dir))

    def get_btime ( self ):
        return time.time ()
        #fp = open ( '/proc/stat', 'r' );
        #btime = None
        #while True:
        #    line = fp.readline ()
        #    if line == '':
        #        break
        #    v = line.split ()
        #    if v [ 0 ] == 'btime':
        #        btime = long ( v [ 1 ] )
        #        break
        #fp.close ()
        #assert btime != None
        #return btime

    def prg_cache_load ( self, idx, ts ):
        self.cputimes [ idx ] = {}
        procs = glob.glob ( '/proc/*' )
    
        for dname in procs:
            try:
                if not os.path.isdir ( dname ):
                    continue
                if re.compile ( '\d+' ).match ( dname, 6 ) == None:
                    continue
                pid = int ( dname [ 6: ] )
        
                fd = open ( dname + '/stat', 'r' )
                if fd == None:
                    continue
                statarray = fd.read ().split ()
                fd.close ()
        
                if not self.process_table.has_key ( pid ):
                    uid = os.stat ( dname ).st_uid
                    ppid = int ( statarray [ 3 ] )
                    pgid = int ( statarray [ 4 ] )
                    if ( ppid < 0 or pgid < 0 ):
                        continue
                    fd = open ( dname + '/cmdline', 'r' )
                    if fd == None:
                        continue
                    cmdline = fd.read ().split ( '\0' )
                    if len ( cmdline [ 0 ] ) > 0:
                        firstchar = cmdline [ 0 ] [ 0 ]
                        if firstchar == '.' or firstchar == '/':
                            cmdline [ 0 ] = os.path.split ( cmdline [ 0 ] ) [ 1 ]
                        cmdline [ 0 ] = re.compile ( '\s' ).sub ( '_', cmdline [ 0 ] )
                    else:
                        fd = open ( dname + '/status', 'r' )
                        cmdline [ 0 ] = '[' + fd.readline().split()[1] + ']'
                    pi = procinfo ( uid, ppid, pgid, cmdline [ 0 ] )
                    self.process_table [ pid ] = pi
                putime = long ( statarray [ 13 ] )
                pstime = long ( statarray [ 14 ] )
                self.cputimes [ idx ] [ pid ] = putime + pstime
            except OSError:
                continue
            except IOError:
                continue
    
    def calc_netdifference ( self, idx, ts ):
        cur = self.netbytes [ idx ]
        prev = self.netbytes [ idx ^ 1 ]
        if prev[0] == 0:
            # we send nothing for the first time
            return "N 0 0"
        else:
            return "N %d %d" % (cur[0]-prev[0],cur[1]-prev[1])

    def calc_difference ( self, idx, ts ):
        buf = ''
        cur = self.cputimes [ idx ]
        prev = self.cputimes [ idx ^ 1 ]
        delete_list = []
        loadavg = 0
        for pid, pi in self.process_table.iteritems ():
            if cur.has_key ( pid ):
                t = cur [ pid ]
                if prev.has_key ( pid ):
                    t -= prev [ pid ]
                if t > 0:
                    if pi.sent == None:
                        user = pi.uid
                        try:
                            if not self.uid_table.has_key ( pi.uid ):
                                user = pwd.getpwuid ( pi.uid ) [ 0 ]
                                self.uid_table [ pi.uid ] = user
                            user = self.uid_table [ pi.uid ]
                        except:
                            pass
                        buf += "P %d %s %s %d " % ( pid, pi.cmd, user, t )
                    else:
                        buf += "C %d %d " % ( pid, t )
                    pi.sent = ts
                    loadavg += t
                else:
                    if pi.sent != None and ( ts - pi.sent > DELETE_TIMEOUT ):
                        buf += "T %d " % pid
                        delete_list.append ( pid )
            elif prev.has_key ( pid ):
                # old process
                delete_list.append ( pid )
                if pi.sent != None:
                    buf += "T %d " % pid
        for pid in delete_list:
            del self.process_table [ pid ]
        return ( buf, loadavg / (poll_interval * 100.0) )
    
    def get_netinfo ( self, idx ):
        retry_flag = True
        while retry_flag:
            retry_flag = None
            fp = open('/proc/net/dev', 'r')
            rbytes = 0
            wbytes = 0
            if fp:
                line = fp.readline()
                line = fp.readline()
                line_number = 2
                while True:
                    line = fp.readline()
                    line_number += 1
                    if line == '':
                        break
                    values = []
                    try:
                        map(lambda x:values.extend(x.split()), line.split(":"))
                        if values[0].find("eth") == 0:
                            rbytes += long(values[1])
                            wbytes += long(values[9])
                    except IndexError:
                        sys.stderr.write("line %d: [[%s]]\n" % (line_number, line))
                        retry_flag = True
                        break
        self.netbytes[idx] = (rbytes,wbytes)
        
    def get_meminfo ( self ):
        while True:
            try:
                m0 = m1 = m2 = 0
                fp = open ( '/proc/meminfo', 'r' )
                if fp == None:
                    return None
                m_total = -1
                m_free = -1
                m_buffers = -1
                m_cached = -1
                s_total = -1
                s_free = -1
                while True:
                    line = fp.readline ()
                    if line == '':
                        break
                    ( k, v ) = line.split ( None, 2 ) [ :2 ]
                    if k == "SwapTotal:":
                        s_total = long ( v )
                    elif k == "SwapFree:":
                        s_free = long ( v )
                    elif k == "MemTotal:":
                        m_total = long ( v )
                    elif k == "MemFree:":
                        m_free = long ( v )
                    elif k == "Buffers:":
                        m_buffers = long ( v )
                    elif k == "Cached:":
                        m_cached = long ( v )
                fp.close ()
                m0 = float ( m_total - m_free - m_buffers - m_cached ) / m_total;
                m1 = float ( m_buffers + m_cached ) / m_total;
                if ( s_total < -1 ) or ( s_free < -1 ):
                    m2 = -1
                elif s_total == 0:
                    m2 = 0
                else:
                    m2 = float ( s_total - s_free ) / s_total
                return "M %.2f %.2f %.2f" % ( m0, m1, m2 )
            except ValueError:
                pass

    def get_diskinfo ( self, idx ):
        pgin = -1
        pgout = -1
        swin = -1
        swout = -1
        fp = None
        try:
            fp = open ( '/proc/vmstat', 'r' )
        except IOError,(err,desc):
            if err == errno.ENOENT:
                pass
            else:
                raise
        if fp:
            # kernel 2.6
            while True:
                line = fp.readline ()
                if line == '':
                    break
                ( k, v ) = line.split ( None, 2 ) [ :2 ]
                if k == "pgpgin":
                    pgin = long ( v )
                elif k == "pgpgout":
                    pgout = long ( v )
                elif k == "pswpin":
                    swin = long ( v )
                elif k == "pswpout":
                    swout = long ( v )
            fp.close ()
        if ( pgin == -1 ) or ( pgout == -1 ) or ( swin == -1 ) or ( swout == -1 ):
            # kernel 2.4
            fp = open ( '/proc/stat', 'r' );
            if fp:
                while True:
                    line = fp.readline ()
                    if line == '':
                        break
                    a = line.split ()
                    if len(a) != 3:
                        continue
                    ( k, v1, v2 ) = a
                    if k == 'page':
                        pgin = long(v1)
                        pgout = long(v2)
                    elif k == 'swap':
                        swin = long(v1)
                        swout = long(v2)
        self.diskpages[idx] = ( pgin, pgout, swin, swout )

    def calc_diskdifference ( self, idx, ts ):
        cur = self.diskpages [ idx ]
        prev = self.diskpages [ idx ^ 1 ]
        if prev == None:
            # we send nothing for the first time
            return "D 0 0 0 0"
        else:
            return "D %d %d %d %d" % (cur[0]-prev[0],cur[1]-prev[1],cur[2]-prev[2],cur[3]-prev[3])

    def get_stateinfo ( self ):
        if self.state_dir == None:
            return ''
        flag = False
        fname = self.state_dir + 'state.log'
        try:
            mtime = os.stat ( fname ).st_mtime
            if mtime != self.state_mtime:
                content = ''
                fp = open ( fname, 'r' )
                content = fp.read()
                fp.close()
                if content != '' and content != self.state_content:
                    flag = True
                    self.state_mtime = mtime
                    self.state_content = content
            fname = self.state_dir + 'pgidgroup.log'
            mtime = os.stat ( fname ).st_mtime
            if mtime != self.pgid_mtime:
                content = ''
                fp = open ( fname, 'r' )
                content = fp.read()
                fp.close()
                content = urllib.urlencode({'a': content})[2:]
                if content != '' and content != self.pgid_content:
                    flag = True
                    self.pgid_mtime = mtime
                    self.pgid_content = content
            if flag:
                return 'S %s %s' % (self.state_content, self.pgid_content)
        except:
            pass
        return ''

    def get_stateconfig ( self ):
        if self.state_dir == None:
            return ''
        flag = False
        fname = self.state_dir + 'state_config.dat'
        mtime = os.stat ( fname ).st_mtime
        if mtime != self.stateconfig_mtime:
            content = ''
            fp = open ( fname, 'r' )
            content = fp.read()
            fp.close()
            content = urllib.urlencode({'a': content})[2:]
            if content != '' and content != self.stateconfig_content:
                flag = True
                self.stateconfig_mtime = mtime
                self.stateconfig_content = content
        if flag:
            return 'SS %s' % (self.stateconfig_content)
        return ''

    def get_procinfo ( self, idx ):
        p = 0
        fp = open ( '/proc/stat', 'r' );
        if fp:
            while True:
                line = fp.readline ()
                if line == '':
                    break
                ( k, v ) = line.split ( None, 1 ) [ :2 ]
                # we assume "cpu" always appears earlier than "processes"
                if k == 'processes':
                    p = long(v)
                    break
                elif k == 'cpu':
                    self.cpustat [ idx ] = map(lambda x:long(x), v.split ( None, 5 ) [ :5 ])
                    if len(self.cpustat [ idx ]) == 4:
                        self.cpustat [ idx ].append(0L);
        self.nfork [ idx ] = p

    def calc_procdifference ( self, idx, ts ):
        cur = self.nfork [ idx ]
        prev = self.nfork [ idx ^ 1 ]
        if prev == None:
            # we send nothing for the first time
            self.nfork_diff = 0
            self.cpustat_diff = [ 0, 0, 0, 0, 0 ]
        else:
            self.nfork_diff = cur - prev
            for i in range(5):
                self.cpustat_diff [ i ] = self.cpustat [ idx ] [ i ] - self.cpustat [ idx ^ 1 ] [ i ]

    def print_line ( self, l, ts, m, s, buf, netdiff, diskdiff ):
        a = os.getloadavg()
        curtime = long ( ts * 100 )
        l = (self.cpustat_diff[0] + self.cpustat_diff[1]) / (poll_interval * 100.0)
        line = "%s %d C %.2f %.2f %.2f %.2f %d " % ( self.hn, curtime, l, a[0], a[1], a[2], self.nfork_diff )
        for v in self.cpustat_diff:
            line += "%d " % (v)
        if flg_print_meminfo and m != '':
            line += "%s " % (m)
        if flg_print_stateinfo and s != '':
            line += "%s " % (s)
        if flg_print_netinfo and netdiff != '':
            line += "%s " % (netdiff)
        if flg_print_diskinfo and diskdiff != '':
            line += "%s " % (diskdiff)
        if flg_print_pinfo:
            line += "P %s" % (buf)
        self.log_handler.handle_log ( line )

    def main_loop ( self ):
        i = 0
        ts = time.time () - self.btime
        self.prg_cache_load ( i, ts )
        while True:
            # comsume stdin
            rfds,_,_ = select.select([sys.stdin],[],[],0)
            if len(rfds) > 0:
                for rfd in rfds:
                    _ = rfd.read(8192)
            
            try:
                time.sleep ( poll_interval )
            except:
                return
            i ^= 1
            ts = time.time () - self.btime
            self.prg_cache_load ( i, ts );
            self.get_procinfo(i);
            self.get_netinfo(i);
            self.get_diskinfo(i);
            ( buf, l ) = self.calc_difference ( i, ts )
            netdiff = self.calc_netdifference ( i, ts )
            diskdiff = self.calc_diskdifference ( i, ts )
            self.calc_procdifference ( i, ts )
            m = self.get_meminfo ()
            s = self.get_stateinfo () 
            ss = self.get_stateconfig ()
            if ss != '':
                s += ' ' + ss
            self.print_line ( l, ts, m, s, buf, netdiff, diskdiff )

class pinfo_session:
    def __init__ ( self ):
        start_time = time.time ()

def main ():
    try:
        random.seed ()
        time.sleep ( random.random () * poll_interval )
        pinfo_common ().main_loop ()
    except IOError,(err,desc):
        if err == errno.EPIPE:
            pass
        else:
            raise

if __name__ == "__main__":
    main()
