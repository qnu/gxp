#!/usr/bin/env python

#
# AWS S3 I/O benchmark Working for 85GB/sec
#

import errno
import fcntl
import argparse
import logging
import os
import os.path
import sys
import time
import random
import socket
import datetime
import hashlib
import logging
from collections import defaultdict
import uuid
import random
import string
import boto
import boto.s3.connection
import boto.s3.key
import signal

DEFAULT_S3_CREDENTIALS = "~/.aws/credentials"
DEFAULT_S3_BUCKET = ""

if sys.platform == "win32":
    timer = time.clock
else:
    timer = time.time

def stdout(msg):
    sys.stdout.write(msg)
    sys.stdout.flush()

def stderr(msg):
    sys.stderr.write(msg)
    sys.stderr.flush()

def signal_handler(signum, frame):
        fail
        #raise Exception("Timed out!")

# GXP Environment
class Host:
    def __init__(self, hostname, fqdn, ip, rank):
        self.hostname = hostname      # hostname
        self.fqdn = fqdn              # fqdn
        self.ip = ip                  # ip address
        self.rank = rank              # GXP_EXEC_IDX
        self.key = hostname           # misc usage

    def __repr__(self):
        return ("Host(%(hostname)r, %(fqdn)r, %(ip)r, %(rank)r)" % self.__dict__)

    def match_regexp(self, regexp):
        return regexp.match(self.fqdn)

def get_rank():
    return int(os.environ.get('GXP_EXEC_IDX', '0'))

def get_size():
    return int(os.environ.get('GXP_NUM_EXECS', '1'))

def get_my_host():
    hostname = socket.gethostname()
    fqdn = socket.getfqdn()
    try:
        ip = socket.gethostbyname(fqdn)
    except socket.gaierror:
        #stderr("warning: failed to get ip address of %s\n" % fqdn)
        ip = '127.0.0.1'
    rank = get_rank()
    return Host(hostname, fqdn, ip, rank)

def get_all_hosts(wp, fp):
    wp.write("%r\n" % get_my_host())
    wp.flush()
    hosts = []
    for i in range(get_size()):
        line = fp.readline()
        assert line != ""
        host = eval(line.strip())
        hosts.append((host.rank, host))
    hosts.sort()
    hosts_list = map(lambda (rank, host): host, hosts)
    hosts_map = {}
    for h in hosts_list:
        hosts_map[h.key] = h
    return hosts_list, hosts_map

def set_close_on_exec():
    try:
        fd_3 = fcntl.fcntl(3, fcntl.F_GETFD)
        fd_4 = fcntl.fcntl(4, fcntl.F_GETFD)
    except IOError:
        fd_3 = fcntl.FD_CLOEXEC
        fd_4 = fcntl.FD_CLOEXEC
    fd_3 = fd_3 | fcntl.FD_CLOEXEC
    fd_4 = fd_4 | fcntl.FD_CLOEXEC
    fcntl.fcntl(3, fcntl.F_SETFD, fd_3)
    fcntl.fcntl(4, fcntl.F_SETFD, fd_4)

class ArgumentParserError(Exception): pass

#class TimeOutException(Exception): pass

class MyArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        raise ArgumentParserError(message)

def parse_args():
    # Read credentials from AWS
    aws_access_key = None
    aws_secret_access_key = None

    try:
        with open(os.path.expanduser(DEFAULT_S3_CREDENTIALS)) as f:
            for line in f.readlines():
                l = line.strip()
                if l.startswith("aws_access_key_id"):
                    _, aws_access_key = l.split('=')
                    aws_access_key = aws_access_key.strip()
                if l.startswith("aws_secret_access_key"):
                    _, aws_secret_access_key = l.split('=')
                    aws_secret_access_key = aws_secret_access_key.strip()
    except IOError as e:
        pass

    logging.basicConfig()
    parser = MyArgumentParser()
    parser.add_argument('--access-key', type=str, default=aws_access_key,
        dest='access_key', metavar='ACCESS_KEY', help='access key')
    parser.add_argument('--secret-key', type=str, default=aws_secret_access_key,
        dest='secret_key', metavar='SECRET_KEY', help='secret key')
    parser.add_argument('--bucket', type=str, default=DEFAULT_S3_BUCKET,
        dest='bucket', metavar='BUCKET', help='bucket name')
    parser.add_argument('--suffix', type=str, default='s3io',
        dest='suffix', metavar='STRING', help='object suffix (default: s3io)')
    parser.add_argument('-s', '--size', type=str,
        default='64MB', dest='object_size', metavar='SIZE',
        help='object size in bytes, KB, MB, or GB (default: 64MB)')
    parser.add_argument('-k', '--objects-per-rank', type=int,
        default=1, dest='objects_per_rank', metavar='NUM',
        help='number of objects per thread (default: 1)')
    parser.add_argument('-w', '--write', action="store_const",
        dest='action', const='write', default=None, help='write/create objects')
    parser.add_argument('-r', '--read', action="store_const",
        dest='action', const='read', default=None, help='read objects')
    parser.add_argument('-l', '--list', action="store_const",
        dest='action', const='list', default=None, help='list objects in bucket')
    parser.add_argument('-t', '--trials', type=int, dest='trials', metavar='INTEGER', default=10,
        help='number of identical runs (default: 10)')
    parser.add_argument('-i', '--interval', type=int, dest='interval',
        default=0, metavar='INT', help='interval between trials (default: 0)')
    parser.add_argument('--human', dest='human', action='store_true',
        default=False, help='output human readable (default: false)')
    parser.add_argument('--clean', dest='clean', action='store_true',
        default=False, help='clean bucket after benchmarking')
    parser.add_argument('--dryrun', dest='dryrun', action='store_true',
        default=False, help='do nothing, dryrun')
    parser.add_argument('-v', '--verbose', type=int,
        dest='verbose', default=0, help='verbose level')
    parser.add_argument('--boto-log', dest='boto_log', action='store_true',
        default=False, help='enable boto logging')

    args = parser.parse_args()

    return args

def human2bytes(size):
    num = int(size.strip('KMGB'))
    if size.upper().endswith("KB"):
        return num * 1024
    elif size.upper().endswith("MB"):
        return num * 1024 * 1024
    elif size.upper().endswith("GB"):
        return num * 1024 * 1024 * 1024

def bytes2human(num, round2int=False):
    format_str = "%.3f%s"
    if round2int: format_str = "%d%s"
    for unit in ['B','KB','MB','GB','TB','PB']:
        if abs(num) < 1024.0:
            return format_str % (num, unit)
        num /= 1024.0
    return format_str % (num, 'EB')

class S3IOBenchmark:
    def __init__(self, hosts, rank, wp, fp, args, **kw):
        self.hosts = hosts
        self.rank = rank
        self.host = hosts[rank]
        self.size = get_size()
        self.wp = wp
        self.fp = fp
        self.args = args
        self.objects = self.size * self.args.objects_per_rank
        self.object_size = human2bytes(self.args.object_size)

        self.conn = boto.s3.connection.S3Connection(
                aws_access_key_id = args.access_key,
                aws_secret_access_key = args.secret_key,
                host='s3-us-west-2.amazonaws.com',
                is_secure=False, debug=args.verbose)
        self.bucket = self.conn.get_bucket(args.bucket)

        self.res = defaultdict(list)
        self.trial = 0
        self.logfile = None

        if self.args.boto_log:
            logger = logging.getLogger('boto')
            logger.propagate = False
            logger.setLevel(logging.DEBUG)
            filename = 'boto_%s_%s_%d.log' % (
                self.args.action, bytes2human(self.object_size, True), self.size)
            handler = logging.FileHandler(filename, 'w')
            logger.addHandler(handler)

    def msg(self, s):
        if self.args.verbose >= 0:
            stdout('[%d/%d]%s: %s\n' %
                (self.rank, self.size, self.host.hostname, s))

    def info(self, s):
        if self.args.verbose >= 1:
            stdout('[%d/%d]%s: %s\n' %
                (self.rank, self.size, self.host.hostname, s))

    def debug(self, s):
        if self.args.verbose >= 2:
            stdout('[%d/%d]%s: %s\n' %
                (self.rank, self.size, self.host.hostname, s))

    def barrier(self):
        self.wp.write('\n')
        self.wp.flush()
        for i in range(self.size):
            r = self.fp.readline()
            if r == "":
                return -1
        return 0

    def broadcast(self, msg):
        self.wp.write(msg)
        self.wp.write('\n')
        self.wp.flush()

    def receive(self):
        msg = self.fp.readline()
        assert msg != ""
        return msg.strip()

    def logging_start(self):
        now = datetime.datetime.now()
        filename = '%s_s%s_k%d_p%d_%s.s3io' % (
            self.args.action, bytes2human(self.object_size, True),
            self.args.objects_per_rank, self.size,
            now.strftime("%y%m%d%H%M%S"))
        self.logfile = open(filename, 'w')

        self.logfile.write('%d,' % int(time.time()))
        self.logfile.write('%s,' % self.bucket.name)
        self.logfile.write('%s,' % self.args.action)
        self.logfile.write('%d,' % self.object_size)
        self.logfile.write('%d,' % self.args.objects_per_rank)
        self.logfile.write('%d' % self.size)
        self.logfile.write('\n')

    def logging_end(self):
        self.logfile.close()

    def execute(self):
        if self.rank == 0:
            objects = range(self.size * self.args.objects_per_rank)
            random.shuffle(objects)
            self.broadcast(repr(objects))
        objects = eval(self.receive())

        start_idx = self.rank * self.args.objects_per_rank
        end_idx = start_idx + self.args.objects_per_rank - 1
        objects = objects[start_idx:end_idx+1]
        self.info("%s %d %s objects: %r" %
                (self.args.action, self.args.objects_per_rank,
                bytes2human(self.object_size), objects))

        res = []
        self.barrier()
        elapsed = timer()

        if self.args.action == 'write':
            res.append(self.write(objects))
        elif self.args.action == 'read':
            res.append(self.read(objects))
        elif self.args.action == 'list':
            self.list()

        self.barrier()
        elapsed = timer() - elapsed
        self.broadcast(repr(res))

        self.res[self.args.action] = []
        for i in range(self.size):
            self.res[self.args.action].extend(eval(self.receive()))
        self.res[self.args.action].sort(key=lambda x:x[0])

        if self.rank == 0:
            total_bytes = 0
            for record in self.res[self.args.action]:
                self.logfile.write("%d,%d,%f,%s\n" % record)
                total_bytes += record[1]
            throughput = total_bytes / elapsed
            self.logfile.write('%s,%d,%d,%f\n' %
                (self.args.action, self.trial+1, total_bytes, elapsed))
            self.msg("%s total %d objects of %s in %.2f seconds, or %s/sec." %
                (self.args.action, self.size * self.args.objects_per_rank,
                 bytes2human(total_bytes), elapsed, bytes2human(throughput)))
            return throughput

        return None

    def create_keys(self, objects):
        keys = []
        for o in objects:
            key = boto.s3.key.Key(self.bucket)
            key.key = "%s-%s-%d.%s.%s" % (
                #hashlib.md5(str(99999999 - o)).hexdigest()[0:32],
                str(uuid.uuid4().get_hex()).upper()[0:32],
                bytes2human(self.object_size, True), o,
                self.args.suffix, self.trial)
            keys.append(key)
        return keys

    def generate_string(self, rand=False):
        return '*' * self.object_size

    def write(self, objects):
        written_bytes = 0
        contents = self.generate_string()
        elapsed = []
        for k in self.create_keys(objects):
            if self.args.dryrun: continue
            t = timer()
            try:
                signal.signal(signal.SIGALRM, signal_handler)
                signal.alarm(5)   # seconds
                b = k.set_contents_from_string(contents, headers=None, replace=False,
                    cb=None, num_cb=10, policy=None, md5=None, reduced_redundancy=True,
                    encrypt_key=False)
                signal.alarm(0)
            except Exception, msg:
                self.info("Timed out!!")
                signal.alarm(0)
                continue
            t = timer() - t
            if b is None: b = 0
            self.debug("wrote %d bytes to %s in %f seconds" % (b, k.key, t))
            if b:
                written_bytes = written_bytes + b
                elapsed.append(t)
        return (self.rank, written_bytes, sum(elapsed),
                ','.join(map(str, elapsed)))

    def read(self, objects):
        read_bytes = 0
        with open(os.devnull, 'w') as f:
            elapsed = []
            for k in self.create_keys(objects):
                self.debug("read %s" % k.key)
                if self.args.dryrun: continue
                t = timer()
                k.get_file(f)
                t = timer() - t
                read_bytes = read_bytes + self.object_size
                self.debug("read %d bytes to %s in %f seconds" %
                        (self.object_size, k.key, t))
                elapsed.append(t)
        return (self.rank, read_bytes, sum(elapsed),
                ','.join(map(str, elapsed)))

    def list(self):
        stdout(repr(map(lambda x:str(x.key), self.bucket.list()))+'\n')

    def clean(self):
        objects = range(self.objects)
        self.trial = 0
        for i in range(self.args.trials):
            for k in self.create_keys(objects):
                self.debug("clean %s" % k.key)
                k.delete()
            self.trial += 1

    def run(self):
        if self.rank == 0:
            self.logging_start()

        results = []
        for i in range(self.args.trials):
            if self.rank == 0:
                self.logfile.write("%d: trial %d\n" % (int(time.time()), self.trial))
            results.append(self.execute())
            self.trial += 1
            if self.trial <  self.args.trials:
                time.sleep(self.args.interval)
        if self.rank == 0:
            min_thpt = results[0]
            max_thpt = results[0]
            sum_thpt = 0
            for t in results:
                if t < min_thpt: min_thpt = t
                if t > max_thpt: max_thpt = t
                sum_thpt += t
            avg_thpt = sum_thpt / len(results)
            self.logfile.write("avg: %f, min: %f, max %f\n" %
                (avg_thpt, min_thpt, max_thpt))
            self.msg("Throughput: avg %s/sec, min %s/sec, max %s/sec in %d trials." %
                (bytes2human(avg_thpt), bytes2human(min_thpt),
                 bytes2human(max_thpt), len(results)))
            self.logging_end()

            if self.args.clean:
                self.clean()

def main():
    try:
        set_close_on_exec()
    except IOError:
        stderr('usage: gxpc mw s3io.py\n')
        return 1

    wp = os.fdopen(3, 'wb')
    fp = os.fdopen(4, 'rb')
    hosts, hosts_map = get_all_hosts(wp, fp)
    if hosts is None:
        stderr('Error: failed to get all hosts')
        return 1

    rank = get_rank()

    try:
       args = parse_args()
    except ArgumentParserError as e:
        if rank == 0: stdout('s3io.py: error: %s\n' % e)
        return 1

    s3io = S3IOBenchmark(hosts, rank, wp, fp, args)
    s3io.run()
    return 0

if __name__ == '__main__':
    sys.exit(main())
