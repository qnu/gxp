#!/usr/bin/env python

# Python File System Benchmark
# Copyright (C) 2008  Nan Dun <dunnan@yl.is.s.u-tokyo.ac.jp>
#
# This program can be distributed under the terms of the GNU GPL.
# See the file COPYING.

import errno
import optparse
import os
import pwd
import random
import shutil
import socket
import stat
import sys
import textwrap
import threading
import time

__all__ = ['KB', 'MB', 'GB', 'OPSET_META', 'OPSET_IO', 'parse_data_size',
           'PyFileSystemBenchmark', 'DataGenerator', 
           'OptionParserHelpFormatter'] 

PFSB_VERSION = 0.7
PFSB_DATE = "2009.02.27"

KB = 1024
MB = 1048576
GB = 1073741824

OPSET_META = ['mkdir', 'rmdir', 'creat', 'access', 'open', 'open+close',
              'stat_EXIST', 'stat_NONEXIST', 'utime', 'chmod', 'rename', 
              'unlink']
OPSET_IO = ['read', 'reread', 'write', 'rewrite', 'fread', 'freread', 'fwrite',
            'frewrite']

if sys.platform == "win32":
    timer = time.clock
else:
    timer = time.time

def ws(s):
    sys.stdout.write(s)
    sys.stdout.flush()

def es(s):
    sys.stderr.write(s)
    sys.stderr.flush()

def parse_data_size(size):
    size = size.upper()
    if size.isdigit():
        return eval(size)
    if size.endswith('B'):
        size = size[0:-1]
    if size.endswith('K'):
        return eval(size[0:-1]) * KB
    if size.endswith('M'):
        return eval(size[0:-1]) * MB
    if size.endswith('G'):
        return eval(size[0:-1]) * GB

class DataGenerator:
    def __init__(self, base):
        self.base = base
        self.dirs = None
        self.files = None
        self.dir = None
        self.file = None
        self.tempdir = "%s/pfsb-%s-%d-%03d" % \
                       (self.base, socket.gethostname(), os.getpid(),
                       random.randint(0, 999))

    def gen_dirs(self, num, factor=16):
        assert num > 0
        self.dirs = []
        queue = [ self.tempdir ]
        i = l = 0
        while i < num:
            if i % factor == 0:
                parent = queue.pop(0)
                l = l + 1
            child = "%s/L%d-%d" % (parent, l, i)
            self.dirs.append(child)
            queue.append(child)
            i = i + 1

        self.dir = self.dirs[0]
        return self.dirs

    def gen_files(self, num):
        assert num > 0
        self.files = []
        for i in range(0, num): 
            self.files.append("%s/%d.dat" % (self.tempdir, i))

        self.file = self.files[0]
        return self.files

    def shuffle(self, shuffle="random", round=1):
        if self.dirs is not None:
            if shuffle == "random":
                random.shuffle(self.dirs)
            elif shuffle == "round":
                for i in range(0, round):
                    self.dirs.append(self.dirs.pop(0))
            self.dir = self.dirs[0]
        if self.files is not None:
            if shuffle == "random":
                random.shuffle(self.files)
            elif shuffle == "round":
                for i in range(0, round):
                    self.files.append(self.files.pop(0))
            self.file = self.files[0]
    
    def clean(self):
        shutil.rmtree(self.tempdir)

class PyFileSystemBenchmark:
    def __init__(self, opts=None, **kw):
        # environmental variables, opts and **kw should not modify them
        self.uid = os.getuid()
        self.pid = os.getpid()
        self.user = pwd.getpwuid(self.uid)[0]
        self.hostname = socket.gethostname()
        self.start = None
        self.end = None
        self.cmd = None
        
        # configurable variables
        self.mode = "io"
        self.wdir = "/tmp"
        self.opset = OPSET_IO
        self.opcnt = 10
        self.factor = 16
        self.threads = 1
        self.filesize = MB
        self.blksize = KB
        self.unit = "MB"
        self.sync = False
        self.shuffle = None 
        self.round = 1
        self.closetime = True
        self.sleep = 0.0
        self.verbosity = 0
        self.log = False
        self.logfile = None
        self.keep = False
        self.dryrun = False

        # initial from opts and **kw
        if opts is not None:
            for k, v in opts.__dict__.items():
                if self.__dict__.has_key(k):
                    self.__dict__[k] = v

        for k, v in kw.items():
            if self.__dict__.has_key(k):
                self.__dict__[k] = v
        
        # post processing
        self.ws = sys.stdout
        if self.log:
            if self.logfile is None:
                self.logfile = "pfsb-%s-%s.log" % (self.mode,
                                time.strftime("%y-%m-%d-%H-%M-%S",
                                time.localtime()))
                self.ws = open(self.logfile, "w")
            else:
                self.ws = open(self.logfile, "a")

        self.vcnt = 0
        self.size = 1
        self.res = {}
        self.aggres = {} # final aggregated result
        self.data = DataGenerator(self.wdir)
    
    def __del__(self):
        if self.log:
            self.ws.close()

    def verbose(self, msg):
        self.ws.write("[%9s#%5d:%05d] %s\n" % \
           (self.hostname, self.pid, self.vcnt, msg))
        self.ws.flush()
        self.vcnt += 1

    def ensure_dir(self, path):
        if os.path.isdir(path):
            return 0
        
        if self.verbosity >= 2:
            self.verbose("ensure_dir: os.makedirs(%s)" % path)
        
        if not self.dryrun:
            try:
                os.makedirs(path)
            except OSError, err:
                if err.errno != errno.EEXIST: #TODO: check if it is a file
                    es("failed to create %s: %s\n" % \
                       (path, os.strerror(err.errno)))
                    return -1
        return 0
    
    # input/output
    def read(self, file, tid=None):
        if self.verbosity >= 1:
            if tid is None:
                self.verbose("read: os.read(%s, %d) * %d" % 
                    (file, self.blksize, self.filesize/self.blksize))
            else:
                self.verbose("%s read: os.read(%s, %d) * %d" % 
                    (tid, file, self.blksize, self.filesize/self.blksize))
                
        if self.dryrun:
            return None
        
        count = 0
        flags = os.O_CREAT | os.O_RDWR
        if self.sync:
            flags = flags | os.O_SYNC
        
        all_start = start = timer()
        fd = os.open(file, flags)
        elapsed = timer() - start
        while count * self.blksize < self.filesize:
            start = timer()
            ret = os.read(fd, self.blksize)
            elapsed += timer() - start
            assert len(ret) == self.blksize
            count += 1
        start = timer()
        os.close(fd)
        end = timer()

        if self.closetime:
            elapsed += end - start
        
        return (elapsed, elapsed, elapsed, self.filesize/elapsed,
                all_start, end)
    
    def reread(self, file, tid=None):
        if self.verbosity >= 1:
            if tid is None:
                self.verbose("reread: os.read(%s, %d) * %d" % 
                    (file, self.blksize, self.filesize/self.blksize))
            else:
                self.verbose("%s reread: os.read(%s, %d) * %d" % 
                    (tid, file, self.blksize, self.filesize/self.blksize))
                
        if self.dryrun:
            return None
        
        count = 0
        flags = os.O_CREAT | os.O_RDWR
        if self.sync:
            flags = flags | os.O_SYNC
        
        all_start = start = timer()
        fd = os.open(file, flags)
        elapsed = timer() - start
        while count * self.blksize < self.filesize:
            start = timer()
            ret = os.read(fd, self.blksize)
            elapsed += timer() - start
            assert len(ret) == self.blksize
            count += 1
        start = timer()
        os.close(fd)
        end = timer()

        if self.closetime:
            elapsed += end - start

        return (elapsed, elapsed, elapsed, self.filesize/elapsed,
                all_start, end)

    def write(self, file, tid=None):
        if self.verbosity >= 1:
            if tid is None:
                self.verbose("write: os.write(%s, %d) * %d" % 
                    (file, self.blksize, self.filesize/self.blksize))
            else:
                self.verbose("%s write: os.write(%s, %d) * %d" % 
                    (tid, file, self.blksize, self.filesize/self.blksize))
        
        if self.dryrun:
            return None
       
        block = '0' * self.blksize
        count = 0
        flags = os.O_CREAT | os.O_RDWR
        if self.sync:
            flags = flags | os.O_SYNC
        
        all_start = start = timer()
        fd = os.open(file, flags, 0600)
        elapsed = timer() - start
        while count * self.blksize < self.filesize:
            start = timer()
            ret = os.write(fd, block)
            elapsed += timer() - start
            assert ret == self.blksize
            count += 1
        start = timer()
        os.close(fd)
        end = timer()

        if self.closetime:
            elapsed += end - start
        
        return (elapsed, elapsed, elapsed, self.filesize/elapsed,
                all_start, end)
    
    def rewrite(self, file, tid=None):
        if self.verbosity >= 1:
            if tid is None:
                self.verbose("rewrite: os.write(%s, %d) * %d" % 
                    (file, self.blksize, self.filesize/self.blksize))
            else:
                self.verbose("%s rewrite: os.write(%s, %d) * %d" % 
                    (tid, file, self.blksize, self.filesize/self.blksize))
                
        if self.dryrun:
            return None
        
        block = '1' * self.blksize
        count = 0
        flags = os.O_CREAT | os.O_RDWR
        if self.sync:
            flags = flags | os.O_SYNC
        
        all_start = start = timer()
        fd = os.open(file, flags)
        elapsed = timer() - start
        while count * self.blksize < self.filesize:
            start = timer()
            ret = os.write(fd, block)
            elapsed += timer() - start
            assert ret == self.blksize
            count += 1
        start = timer()
        os.close(fd)
        end = timer()

        if self.closetime:
            elapsed += end - start

        return (elapsed, elapsed, elapsed, self.filesize/elapsed,
                all_start, end)
    
    def fread(self, file, tid=None):
        if self.verbosity >= 1:
            if tid is None:
                self.verbose("fread: f.read(%s, %d) * %d" % 
                    (file, self.blksize, self.filesize/self.blksize))
            else:
                self.verbose("%s fread: f.read(%s, %d) * %d" % 
                    (tid, file, self.blksize, self.filesize/self.blksize))
        
        if self.dryrun:
            return None

        count = 0
        all_start = start = timer()
        f = open(file, 'r')
        elapsed = timer() - start
        while count * self.blksize < self.filesize:
            start = timer()
            ret = f.read(self.blksize)
            elapsed += timer() - start
            assert len(ret) == self.blksize
            count += 1
        start = timer()
        f.close()
        end = timer()

        if self.closetime:
            elapsed += end - start
        
        return (elapsed, elapsed, elapsed, self.filesize/elapsed,
                all_start, end)
    
    def freread(self, file, tid=None):
        if self.verbosity >= 1:
            if tid is None:
                self.verbose("freread: f.read(%s, %d) * %d" % 
                    (file, self.blksize, self.filesize/self.blksize))
            else:
                self.verbose("%s freread: f.read(%s, %d) * %d" % 
                    (tid, file, self.blksize, self.filesize/self.blksize))
        
        if self.dryrun:
            return None

        count = 0
        all_start = start = timer()
        f = open(file, 'r')
        elapsed = timer() - start
        while count * self.blksize < self.filesize:
            start = timer()
            ret = f.read(self.blksize)
            elapsed += timer() - start
            assert len(ret) == self.blksize
            count += 1
        start = timer()
        f.close()
        end = timer()

        if self.closetime:
            elapsed += end - start
        
        return (elapsed, elapsed, elapsed, self.filesize/elapsed,
                all_start, end)
    
    def fwrite(self, file, tid=None):
        if self.verbosity >= 1:
            if tid is None:
                self.verbose("fwrite: f.write(%s, %d) * %d" % 
                    (file, self.blksize, self.filesize/self.blksize))
            else:
                self.verbose("%s fwrite: f.write(%s, %d) * %d" % 
                    (tid, file, self.blksize, self.filesize/self.blksize))
        
        if self.dryrun:
            return None

        block = '2' * self.blksize
        count = 0
        all_start = start = timer()
        f = open(file, 'w')
        elapsed = timer() - start
        while count * self.blksize < self.filesize:
            start = timer()
            f.write(block)
            end = timer()
            elapsed += timer() - start
            count += 1
        start = timer()
        f.close()
        end = timer()

        if self.closetime:
            elapsed += end - start

        return (elapsed, elapsed, elapsed, self.filesize/elapsed,
                all_start, end)
    
    def frewrite(self, file, tid=None):
        if self.verbosity >= 1:
            if tid is None:
                self.verbose("frewrite: f.write(%s, %d) * %d" % 
                    (file, self.blksize, self.filesize/self.blksize))
            else:
                self.verbose("%s frewrite: f.write(%s, %d) * %d" % 
                    (tid, file, self.blksize, self.filesize/self.blksize))
                
        if self.dryrun:
            return None

        block = '3' * self.blksize
        count = 0
        all_start = start = timer()
        f = open(file, 'w')
        elapsed = timer() - start
        while count * self.blksize < self.filesize:
            start = timer()
            f.write(block)
            elapsed += timer() - start
            count += 1
        start = timer()
        f.close()
        end = timer()

        if self.closetime:
            elapsed += end - start

        return (elapsed, elapsed, elapsed, self.filesize/elapsed,
                all_start, end)
    
    # parallel input/output 
    class ThreadedIO(threading.Thread):
        def __init__(self, pfsb, file, op):
            threading.Thread.__init__(self)
            self.pfsb = pfsb
            self.file = file
            self.op = op
            self.res = None

        def run(self):
            tid = self.getName()
            if self.op == 'write':
                self.res = self.pfsb.write(self.file, tid)
            elif self.op == 'rewrite':
                self.res = self.pfsb.rewrite(self.file, tid)
            elif self.op == 'read':
                self.res = self.pfsb.read(self.file, tid)
            elif self.op == 'reread':
                self.res = self.pfsb.reread(self.file, tid)
            elif self.op == 'fread':
                self.res = self.pfsb.fread(self.file, tid)
            elif self.op == 'freread':
                self.res = self.pfsb.freread(self.file, tid)
            elif self.op == 'fwrite':
                self.res = self.pfsb.fwrite(self.file, tid)
            elif self.op == 'frewrite':
                self.res = self.pfsb.frewrite(self.file, tid)
    
    def threaded_io(self, files, op):
        threads = []
        tid = 0
        for f in files:
            t = self.ThreadedIO(self, f, op)
            t.setName("t%02d" % tid)
            t.setDaemon(True)
            threads.append(t)
            tid += 1
        
        start = timer()
        for t in threads:
            t.start()

        for t in threads:
            t.join()
        end = timer()
        elapsed = end - start
        
        if self.dryrun:
            return None

        # aggregate results
        a_min = ""
        a_max = -1
        a_sum = 0
        for t in threads:
            t_elapsed, t_min, t_max, t_sum, t_start, t_end, = t.res
            a_min = min(a_min, t_min)
            a_max = max(a_max, t_max)
            a_sum += t_sum
        
        return (elapsed, a_min, a_max, a_sum, start, end)
    
    # metadata operations
    # result: (total_time, min_time, max_time, start, end)
    def mkdir(self, dirs):
        t_min = ""
        t_max = -1
        t_total = ops = 0
        
        all_start = timer()
        for dir in dirs:
            if self.verbosity >= 2:
                self.verbose("mkdir: os.mkdir(%s)" % dir)
            if not self.dryrun:
                start = timer()
                os.mkdir(dir)
                end = timer()
                elapsed = end - start
                t_min = min(t_min, elapsed)
                t_max = max(t_max, elapsed)
                t_total += elapsed
                ops += 1
        
        if self.dryrun:
            return None
        
        assert ops == len(dirs)
        return (t_total, t_min, t_max, all_start, end)
    
    def rmdir(self, dirs):
        t_min = ""
        t_max = -1
        t_total = ops = 0
        
        dirs.reverse()
        all_start = timer()
        for dir in dirs:
            if self.verbosity >= 2:
                self.verbose("rmdir: os.rmdir(%s)" % dir)
            if not self.dryrun:
                start = timer()
                os.rmdir(dir)
                end = timer()
                elapsed = end - start
                t_min = min(t_min, elapsed)
                t_max = max(t_max, elapsed)
                t_total += elapsed
                ops += 1
        
        if self.dryrun:
            return None
        
        assert ops == len(dirs)
        return (t_total, t_min, t_max, all_start, end)

    def creat(self, files):
        t_min = ""
        t_max = -1
        t_total = ops = 0
        
        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
        mode = 0600
        all_start = timer()
        for file in files:
            if self.verbosity >= 2:
                self.verbose("creat: os.open(%s, O_WRONLY|O_CREAT|O_TRUNC, "
                             "0600)" % file)
            if not self.dryrun:
                start = timer()
                fd = os.open(file, flags, mode)
                os.close(fd)
                end = timer()
                elapsed = end - start
                t_min = min(t_min, elapsed)
                t_max = max(t_max, elapsed)
                t_total += elapsed
                ops += 1

        if self.dryrun:
            return None
        
        assert ops == len(files)
        return (t_total, t_min, t_max, all_start, end) 
    
    def access(self, files):
        t_min = ""
        t_max = -1
        t_total = ops = 0

        mode = os.F_OK
        all_start = timer()
        for file in files:
            if self.verbosity >= 2:
                self.verbose("access: os.access(%s, F_OK)"  % file)
            if not self.dryrun:
                start = timer()
                ret = os.access(file, mode)
                end = timer()
                assert ret == True
                # Calculate time
                elapsed = end - start
                t_min = min(t_min, elapsed)
                t_max = max(t_max, elapsed)
                t_total += elapsed
                ops += 1
        
        if self.dryrun:
            return None
        
        assert ops == len(files)
        return (t_total, t_min, t_max, all_start, end) 

    def open(self, files):
        t_min = ""
        t_max = -1
        t_total = ops = 0
        
        flags = os.O_RDONLY
        all_start = timer()
        for file in files:
            if self.verbosity >= 2:
                self.verbose("open: os.open(%s, O_RDONLY)"  % file)
            if not self.dryrun:
                start = timer()
                fd = os.open(file, flags)
                end = timer()
                os.close(fd)
                # Calculate time
                elapsed = end - start
                t_min = min(t_min, elapsed)
                t_max = max(t_max, elapsed)
                t_total += elapsed
                ops += 1
        
        if self.dryrun:
            return None
        
        assert ops == len(files)
        return (t_total, t_min, t_max, all_start, end) 
    
    def open_close(self, files):
        t_min = ""
        t_max = -1
        t_total = ops = 0

        flags =  os.O_RDONLY
        all_start = timer()
        for file in files:
            if self.verbosity >= 2:
                self.verbose("open+close: os.open(%s, O_RDONLY)" % file)
            if not self.dryrun:
                start = timer()
                fd = os.open(file, flags)
                os.close(fd)
                end = timer()
                # Calculate time
                elapsed = end - start
                t_min = min(t_min, elapsed)
                t_max = max(t_max, elapsed)
                t_total += elapsed
                ops += 1
        
        if self.dryrun:
            return None

        assert ops == len(files)
        return (t_total, t_min, t_max, all_start, end) 
    
    def stat(self, files):
        t_min = ""
        t_max = -1
        t_total = ops = 0
        
        all_start = timer()
        for file in files:
            if self.verbosity >= 2:
                self.verbose("stat: os.stat(%s)" % file)
            if not self.dryrun:
                start = timer()
                os.stat(file)
                end = timer()
                elapsed = end - start
                t_min = min(t_min, elapsed)
                t_max = max(t_max, elapsed)
                t_total += elapsed
                ops += 1
        
        if self.dryrun:
            return None
        
        assert ops == len(files)
        return (t_total, t_min, t_max, all_start, end) 
    
    def stat_non(self, files):
        t_min = ""
        t_max = -1
        t_total = ops = 0
        
        all_start = timer()
        for file in files:
            f = file + "n"
            if self.verbosity >= 2:
                self.verbose("stat_non: os.stat(%s)" % f)
            if not self.dryrun:
                start = timer()
                try:
                    os.stat(file)
                except:
                    pass
                end = timer()
                elapsed = end - start
                t_min = min(t_min, elapsed)
                t_max = max(t_max, elapsed)
                t_total += elapsed
                ops += 1
        
        if self.dryrun:
            return None
        
        assert ops == len(files)
        return (t_total, t_min, t_max, all_start, end) 
        
    def utime(self, files):
        t_min = ""
        t_max = -1
        t_total = ops = 0
        
        all_start = timer()
        for file in files:
            if self.verbosity >= 2:
                self.verbose("utime: os.utime(%s, None)" % file)
            if not self.dryrun:
                start = timer()
                os.utime(file, None)
                end = timer()
                elapsed = end - start
                t_min = min(t_min, elapsed)
                t_max = max(t_max, elapsed)
                t_total += elapsed
                ops += 1
        
        if self.dryrun:
            return None
        
        assert ops == len(files)
        return (t_total, t_min, t_max, all_start, end) 
        
    def chmod(self, files):
        t_min = ""
        t_max = -1
        t_total = ops = 0
        
        all_start = timer()
        for file in files:
            if self.verbosity >= 2:
                self.verbose("chmod: os.chmod(%s, S_IEXEC)" % file)
            if not self.dryrun:
                start = timer()
                os.chmod(file, stat.S_IEXEC)
                end = timer()
                elapsed = end - start
                t_min = min(t_min, elapsed)
                t_max = max(t_max, elapsed)
                t_total += elapsed
                ops += 1
        
        if self.dryrun:
            return None
        
        assert ops == len(files)
        return (t_total, t_min, t_max, all_start, end) 
        
    def rename(self, files):
        t_min = ""
        t_max = -1
        t_total = ops = 0
        
        all_start = timer()
        for file in files:
            tofile = file + ".to"
            if self.verbosity >= 2:
                self.verbose("rename: os.rename(%s, %s)" % (file, tofile))
            if not self.dryrun:
                start = timer()
                os.rename(file, tofile)
                end = timer()
                elapsed = end - start
                t_min = min(t_min, elapsed)
                t_max = max(t_max, elapsed)
                t_total += elapsed
                ops += 1
        
        if self.dryrun:
            return None
        
        assert ops == len(files)
        
        # rename back
        for file in files:
            tofile = file + ".to"
            if self.verbosity >= 3:
                self.verbose("rename_back: os.rename(%s, %s)" % 
                             (tofile, file))
            os.rename(tofile, file)

        return (t_total, t_min, t_max, all_start, end) 

    def unlink(self, files):
        t_min = ""
        t_max = -1
        t_total = t_avg = ops = 0
        
        all_start = timer()
        for file in files:
            if self.verbosity >= 2:
                self.verbose("unlink: os.unlink(%s)" % file)
            if not self.dryrun:
                start = timer()
                os.unlink(file)
                end = timer()
                elapsed = end - start
                t_min = min(t_min, elapsed)
                t_max = max(t_max, elapsed)
                t_total += elapsed
                ops += 1
        
        if self.dryrun:
            return None
        
        assert ops == len(files)
        return (t_total, t_min, t_max, all_start, end) 
    
    def pre_processing(self):
        if self.verbosity >= 3:
            self.verbose("pre_processing: ensure_dir(%s)" % self.data.tempdir)
        self.ensure_dir(self.data.tempdir)
        
        if self.mode == 'io':
            if self.verbosity >= 3:
                self.verbose("pre_processing: self.data.gen_files()")
            self.data.gen_files(self.threads)
        elif self.mode == 'meta':
            if self.verbosity >= 3:
                self.verbose("pre_processing: self.data.gen_dirs()")
                self.verbose("pre_processing: self.data.gen_files()")
            self.data.gen_dirs(self.opcnt, self.factor)
            self.data.gen_files(self.opcnt)
        
    def inter_processing(self):
        if self.shuffle:
            if self.verbosity >=3:
                self.verbose("inter_processing: self.data.shuffle(%s, %d)" 
                             % (self.shuffle, self.round))
            self.data.shuffle(self.shuffle, self.round)

        if self.verbosity >= 3:
            self.verbose("inter_processing: time.sleep(%f)" % self.sleep)
        if not self.dryrun and self.sleep > 0:
            time.sleep(self.sleep)

    def post_processing(self):
        # cleanup
        if self.verbosity >= 3:
            self.verbose("post_processing: self.data.clean()")
        if not self.dryrun and not self.keep:
            start = timer()
            self.data.clean()
            self.res['cleanup_time'] = timer() - start
        else:
            self.res['cleanup_time'] = 0
    
    def perform_tests(self):
        self.start = (time.localtime(), timer())
        if self.mode == 'io':
            res = self.perform_io_tests()
        elif self.mode == 'meta':
            res = self.perform_meta_tests()
        self.end = (time.localtime(), timer())
        return res
    
    def aggregate_results(self):
        if self.dryrun:
            return
        
        if self.mode == 'io':
            for o in self.opset:
                o_total, o_min, o_max, o_sum, o_start, o_end = self.res[o]
                self.aggres[o] = (o_min, o_max, o_sum, o_total, 
                                  o_start, o_end)
        elif self.mode == 'meta':
            for o in self.opset:
                o_total, o_min, o_max, o_start, o_end = self.res[o]
                self.aggres[o] = (o_min, o_max, self.opcnt/o_total, o_total, 
                                  o_start, o_end)
                
        self.aggres['cleanup_time'] = self.res['cleanup_time']

    def print_results(self, res=None, header=None):
        if header is None:
            header = "Python File System Benchmark (version %.1f, %s)\n" \
                     "             Run began: %s\n" \
                     "               Run end: %s\n" \
                     "     Command line used: %s\n" \
                     "     Working directory: %s\n" % \
                     (PFSB_VERSION, PFSB_DATE,
                     time.strftime("%a, %d %b %Y %H:%M:%S %Z", self.start[0]),
                     time.strftime("%a, %d %b %Y %H:%M:%S %Z", self.end[0]),
                     self.cmd, self.wdir)
        self.ws.write(header)
        
        if self.dryrun:
            self.ws.write("dryrun, nothing was executed.\n")
            return

        if self.mode == "io":
            self.print_io_results(res)
        if self.mode == "meta":
            self.print_meta_results(res)
         
    def perform_io_tests(self):
        self.pre_processing()

        if self.threads <= 1:
            self.res['write'] = self.write(self.data.file)
        else:
            self.res['write'] = self.threaded_io(self.data.files, 'write')
        self.inter_processing()

        if 'rewrite' in self.opset:
            if self.threads <= 1:
                self.res['rewrite'] = self.rewrite(self.data.file)
            else:
                self.res['rewrite'] = self.threaded_io(self.data.files, 'rewrite')
            self.inter_processing()
        
        if 'read' in self.opset:
            if self.threads <= 1:
                self.res['read'] = self.read(self.data.file)
            else:
                self.res['read'] = self.threaded_io(self.data.files, 'read')
            self.inter_processing()
        
        if 'reread' in self.opset:
            if self.threads <= 1:
                self.res['reread'] = self.reread(self.data.file)
            else:
                self.res['reread'] = self.threaded_io(self.data.files, 'reread')
            self.inter_processing()
        
        if 'fwrite' in self.opset:
            if self.threads <= 1:
                self.res['fwrite'] = self.fwrite(self.data.file)
            else:
                self.res['fwrite'] = self.threaded_io(self.data.files, 'fwrite')
            self.inter_processing()
        
        if 'frewrite' in self.opset:
            if self.threads <= 1:
                self.res['frewrite'] = self.frewrite(self.data.file)
            else:
                self.res['frewrite'] = self.threaded_io(self.data.files, 'frewrite')
            self.inter_processing()
        
        if 'fread' in self.opset:
            if self.threads <= 1:
                self.res['fread'] = self.fread(self.data.file)
            else:
                self.res['fread'] = self.threaded_io(self.data.files, 'fread')
            self.inter_processing()

        if 'freread' in self.opset:
            if self.threads <= 1:
                self.res['freread'] = self.freread(self.data.file)
            else:
                self.res['freread'] = self.threaded_io(self.data.files, 'freread')
            self.inter_processing()
        
        self.post_processing()
        
        return self.res

    def perform_meta_tests(self):
        self.pre_processing()

        if 'mkdir' in self.opset or 'rmdir' in self.opset:
            self.res['mkdir'] = self.mkdir(self.data.dirs)
            self.inter_processing()

        if 'rmdir' in self.opset:
            self.res['rmdir'] = self.rmdir(self.data.dirs)
            self.inter_processing()
        
        if 'creat' in self.opset or 'access' in self.opset or \
           'stat_EXIST' in self.opset or 'stat_NONEXIST' in self.opset or \
           'utime' in self.opset or 'chmod' in self.opset or \
           'rename' in self.opset or 'unlink' in self.opset:
            self.res['creat'] = self.creat(self.data.files)
            self.inter_processing()

        if 'access' in self.opset:
            self.res['access'] = self.access(self.data.files)
            self.inter_processing()

        if 'open' in self.opset:
            self.res['open'] = self.open(self.data.files)
            self.inter_processing()
        
        if 'open+close' in self.opset:
            self.res['open+close'] = self.open_close(self.data.files)
            self.inter_processing()

        if 'stat_EXIST' in self.opset:
            self.res['stat_EXIST'] = self.stat(self.data.files)
            self.inter_processing()

        if 'stat_NONEXIST' in self.opset: 
            self.res['stat_NONEXIST'] = self.stat_non(self.data.files)
            self.inter_processing()

        if 'utime' in self.opset:
            self.res['utime'] = self.utime(self.data.files)
            self.inter_processing()

        if 'chmod' in self.opset:
            self.res['chmod'] = self.chmod(self.data.files)
            self.inter_processing()

        if 'rename' in self.opset:
            self.res['rename'] = self.rename(self.data.files)
            self.inter_processing()
        
        if 'unlink' in self.opset:
            self.res['unlink'] = self.unlink(self.data.files)
            self.inter_processing()
        
        self.post_processing()
        
        return self.res
    
    def print_io_results(self, res=None):
        k = float(KB)
        m = float(MB)
        g = float(GB)
        if self.filesize < KB:
            filesize = "%d Bytes" % self.filesize
        elif self.filesize < MB:
            filesize = "%d KB" % (self.filesize / k)
        elif self.filesize < GB:
            filesize = "%d MB" % (self.filesize / m)
        else:
            filesize = "%d GB" % (self.filesize / g)
        
        if self.blksize < KB:
            blksize = "%d Bytes" % self.blksize
        elif self.blksize < MB:
            blksize = "%d KB" % (self.blksize / k)
        elif self.blksize < GB:
            blksize = "%d MB" % (self.blksize / m)
        else:
            blksize = "%d GB" % (self.blksize / g)

        if self.unit == "KB":
            unit = k
        if self.unit == "MB":
            unit = m
        if self.unit == "GB":
            unit = g

        if res is None:
            res = self.aggres
        
        str = "             Test mode: input/output\n" \
              "            Throughput: %s/sec\n" \
              "          Time elapsed: %f seconds\n" \
              "             File size: %s\n" \
              "            Block size: %s\n" \
              "                 Nodes: %s\n" \
              "             Processes: %d\n" \
              "\n%9s%15s%15s%15s%15s%15s%15s\n" % \
              (self.unit, self.end[1]-self.start[1], 
               filesize, blksize, self.size, self.size*self.threads,
               'Operation', 'ExecTime', 'Min/process', 'Max/process',
               'Avg/process', 'Summation', 'Aggregate')

        for o in self.opset:
            a_min, a_max, a_sum, a_agg, a_start, a_end = res[o]
            #agg = self.filesize * size / (end - start)
            str = str + "%9s" % o + \
                  "%15.6f%15.6f%15.6f%15.6f%15.6f%15.6f\n" % \
                  (a_end-a_start, self.filesize/a_max/unit, self.filesize/a_min/unit, 
                   a_sum/(self.size*self.threads)/unit, a_sum/unit,
                   self.filesize*self.size*self.threads/a_agg/unit)
        
        str = str + "Cleanup took %.6f seconds.\n\n" % res['cleanup_time']
        self.ws.write(str)

    def print_meta_results(self, res=None):
        str = "             Test mode: metadata\n" \
              "            Throughput: ops/sec\n" \
              "          Time elapsed: %f seconds\n" \
              "       Operation count: %d\n" \
              "      Directory factor: %d\n" \
              "                 Nodes: %s\n" \
              "             Processes: %s\n" \
              " Total operation count: %s\n" \
              "\n%13s%15s%15s%15s%15s%15s%15s\n" % \
              (self.end[1]-self.start[1],
               self.opcnt, self.factor, self.size, self.size, 
               self.opcnt*len(self.opset)*self.size,
              'Operation', 'ExecTime', 'Min/process', 'Max/process', 
              'Avg/process', 'Summation', 'Aggregate')
        
        if res is None:
            res = self.aggres
        
        for o in self.opset:
            a_min, a_max, a_sum, a_agg, a_start, a_end = res[o]
            # agg = self.opcnt * size / (end - start)   # not accurate here
            str = str + "%13s" % o + \
                  "%15.6f%15.3f%15.3f%15.3f%15.3f%15.3f\n" % \
                  (a_end-a_start, 1.0/a_max, 1.0/a_min, a_sum/self.size, a_sum, 
                   self.opcnt*self.size/a_agg)

        str = str + "Cleanup took %.6f seconds.\n\n" % res['cleanup_time']
        self.ws.write(str)

# OptionParser help string workaround
# adapted from Tim Chase's code from following thread
# http://groups.google.com/group/comp.lang.python/msg/09f28e26af0699b1
class OptionParserHelpFormatter(optparse.IndentedHelpFormatter):
    def format_description(self, desc):
        if not desc: return ""
        desc_width = self.width - self.current_indent
        indent = " " * self.current_indent
        bits = desc.split('\n')
        formatted_bits = [
            textwrap.fill(bit, desc_width, initial_indent=indent,
                susequent_indent=indent)
            for bit in bits]
        result = "\n".join(formatted_bits) + "\n"
        return result

    def format_option(self, opt):
        result = []
        opts = self.option_strings[opt]
        opt_width = self.help_position - self.current_indent - 2
        if len(opts) > opt_width:
            opts = "%*s%s\n" % (self.current_indent, "", opts)
            indent_first = self.help_position
        else:
            opts = "%*s%-*s  " % (self.current_indent, "", opt_width, opts)
            indent_first = 0
        result.append(opts)
        if opt.help:
            help_text = self.expand_default(opt)
            help_lines = []
            for para in help_text.split("\n"):
                help_lines.extend(textwrap.wrap(para, self.help_width))
            result.append("%*s%s\n" % (indent_first, "", help_lines[0]))
            result.extend(["%*s%s\n" % (self.help_position, "", line)
                for line in help_lines[1:]])
        elif opts[-1] != "\n":
            result.append("\n")
        return "".join(result)

def parse_argv(argv):
    usage = "usage: %prog [options]"
    parser = optparse.OptionParser(usage=usage,
                formatter=OptionParserHelpFormatter())
    
    # command options
    parser.remove_option("-h")
    parser.add_option("-h", "--help", action="store_true",
                      dest="help", default=False,
                      help="show the help message and exit")
    
    # control options, keep consitent with Pfsb's variable
    parser.add_option("-m", "--mode", action="store", type="string",
                dest="mode", metavar="MODE", default="io",
                help="set test mode\n"
                     "  io: I/O throughput test mode (default)\n"
                     "  meta: metadata operations test mode")
    
    parser.add_option("-w", "--wdir", action="store", type="string",
                dest="wdir", metavar="DIR", default=os.getcwd(),
                help="working directory (default: cwd)")
    
    parser.add_option("-i", action="append", type="int",
                dest="test", metavar="NUM",
                default=[], # appended later
                help="tests to run (default: 0)\n"
                     "I/O mode:\n"
                     " 0=all, 1=read, 2=reread, 3=write, 4=rewrite\n"
                     " 5=fread, 6=freread, 7=fwrite, 8=frewrite\n"
                     "Meta mode: \n"
                     " 0=all, 1=mkdir, 2=rmdir, 3=creat, 4=access,\n"
                     " 5=open, 6=open+close, 7=stat_EXIST, \n"
                     " 8=stat_NONEXIST, 9=utime, 10=chmod, 11=unlink\n")
    
    parser.add_option("-s", "--filesize", action="store", type="string",
                dest="filesize", metavar="NUM", default="1MB",
                help="file size (default: 1MB)")
    
    parser.add_option("-b", "--blocksize", action="store", type="string",
                dest="blksize", metavar="NUM", default="1KB",
                help="block size (default: 1KB)")
    
    parser.add_option("-t", "--threads", action="store", type="int",
                dest="threads", default=1,
                help="number of concurrent threads in I/O mode (default: 1)")
    
    parser.add_option("-u", "--unit", action="store", type="string",
                dest="unit", metavar="KB/MB/GB", default="MB",
                help="unit of throughput (default: MB)")
    
    parser.add_option("-c", "--count", action="store", type="int",
                dest="opcnt", metavar="NUM", default=10,
                help="number of meta operations (default: 10)")
    
    default_factor = 16
    parser.add_option("-f", "--factor", action="store", type="int",
                dest="factor", metavar="NUM", default=default_factor,
                help="factor of directory tree (default: %d)" % \
                     default_factor)
    
    parser.add_option("-v", "--verbosity", action="store", type="int",
                dest="verbosity", metavar="NUM", default=0,
                help="verbosity level: 0/1/2/3 (default: 0)")
    
    parser.add_option("-d", "--dryrun", action="store_true",
                dest="dryrun", default=False,
                help="dry run, do not execute (default: disabled)")
    
    parser.add_option("--without-close", action="store_false",
                dest="closetime", default=True,
                help="exclude close in timing (default: disable)")
    
    parser.add_option("--syncio", action="store_true",
                dest="sync", default=False,
                help="synchronized I/O (default: disabled)")
    
    parser.add_option("--shuffle", action="store", type="string",
                dest="shuffle", default=None,
                help="shuffle: random/round (default: disabled)")
    
    parser.add_option("--round", action="store", type="int",
                dest="round", default=1,
                help="offset in round shuffle (default: 1)")
    
    parser.add_option("--sleep", action="store", type="float",
                dest="sleep", metavar="SECONDS", default=0.0,
                help="sleep between operations (default: 0.0)")
    
    parser.add_option("--log", action="store_true",
                dest="log", default=False,
                help="forward output to log file (default: disabled)")

    parser.add_option("--logfile", action="store", type="string",
                dest="logfile", metavar="FILE", default=None,
                help="specify the file to which log is appended")
    
    parser.add_option("--keep", action="store_true",
                      dest="keep", default=False,
                      help="keep temparary files (default: disabled)")
    
    opts, args = parser.parse_args(argv)

    opts.print_help = parser.print_help
    
    if not opts.test or 0 in opts.test:
        if opts.mode == 'io':
            opts.opset = list(OPSET_IO)
        elif opts.mode == 'meta': 
            opts.opset = list(OPSET_META)
    else:
        opts.test.sort()
        try:
            if opts.mode == 'io':
                opts.opset = map(lambda x:OPSET_IO[x-1], opts.test)
            elif opts.mode == 'meta':
                opts.opset = map(lambda x:OPSET_META[x-1], opts.test)
        except IndexError, err:
            es("Unknown test operation\n")
            sys.exit(1)

    opts.wdir = os.path.abspath(opts.wdir)
    if opts.factor <= 0:
        es("warning: invalid factor %d\n" % opts.factor)
        sys.exit(1)
    opts.cmd = " ".join(sys.argv)

    opts.filesize = parse_data_size(opts.filesize)
    opts.blksize = parse_data_size(opts.blksize)
    opts.unit = opts.unit.upper()
    if not opts.unit.endswith('B'):
        opts.unit = opts.unit + 'B'

    return opts

def main():
    opts = parse_argv(sys.argv[1:])
    if opts.help:
        opts.print_help()
        return 0

    fsb = PyFileSystemBenchmark(opts)
    fsb.perform_tests()
    fsb.aggregate_results()
    fsb.print_results()
    return 0

if __name__ == "__main__":
    sys.exit(main())
