#!/usr/bin/env python

#
# AWS S3 Bucket Copy
#

import errno
import fcntl
import argparse
import logging
import os
import os.path
import sys
import time
import socket
import datetime
import itertools
import string
import Queue
import threading
from collections import defaultdict

import boto
import boto3
import boto3.s3.transfer
import boto.s3.connection
import boto.s3.key
import botocore

DEFAULT_S3_CREDENTIALS = "~/.aws/credentials"
CHAR_SET = ''.join(string.letters + string.digits + string.punctuation)

timer = time.time

def curr_time():
    return datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

def stdout(msg):
    sys.stdout.write(msg)
    sys.stdout.flush()

def stderr(msg):
    sys.stderr.write(msg)
    sys.stderr.flush()

def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]

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
    parser = MyArgumentParser(
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--access-key', type=str, default=aws_access_key,
        dest='access_key', metavar='ACCESS_KEY', help='access key')
    parser.add_argument('--secret-key', type=str, default=aws_secret_access_key,
        dest='secret_key', metavar='SECRET_KEY', help='secret key')
    parser.add_argument('-s', '--src-bucket', type=str, required=True,
        dest='src_bucket', metavar='SOURCE_BUCKET',
        help='source bucket to copy from')
    parser.add_argument('-d', '--dst-bucket', type=str, required=True,
        dest='dst_bucket', metavar='DESTINATION_BUCKET',
        help='destination bucket to be copied to')
    parser.add_argument('-t', '--threads', type=int, dest='threads',
        metavar='NUM', help="number of threads", default=16)
    parser.add_argument('-x', '--max-concurrency', type=int, dest='max_concurrency',
        metavar='NUM', help="maxium concurrency", default=32)
    parser.add_argument('--prefix-length', type=int,
        dest='prefix_length', metavar='PREFIX_LENGTH',
        help='length of prefix', default=2)
    parser.add_argument('--prefix-list', type=str,
        dest='prefix_list', metavar='PREFIX1,PREFIX2,...',
        help='prefix list to override auto generation', default='')
    parser.add_argument('--overwrite', action='store_true',
        help='overwrite if destination object exists', default=False)
    parser.add_argument('--retry', type=int, dest='retry',
        help='number of retries when copy failed', default=5)
    parser.add_argument('--aws-config', dest='aws_config', action='store_true',
        help='generate aws configure files')
    parser.add_argument('--dryrun', dest='dryrun', action='store_true',
        default=False, help='do nothing, dryrun')
    parser.add_argument('-v', '--verbose', type=int,
        dest='verbose', default=0, help='verbose level')

    args = parser.parse_args()

    return args

def aws_configure(args):
    aws_home = os.path.expanduser("~") + "/.aws"
    if not os.path.exists(aws_home):
        os.makedirs(aws_home)

    if not args.access_key or not args.secret_key:
        if self.rank == 0:
            self.info("AWS credentias not found, please see help to specify access key and secret key.")
        return

    with open(aws_home + "/credentials", "w+") as f:
        f.write("[default]\n")
        f.write("aws_access_key_id=%s\n" % args.access_key)
        f.write("aws_secret_access_key=%s\n" % args.secret_key)

    with open(aws_home + "/config", "w+") as f:
        f.write("[default]\n")
        f.write("region=us-west-2\n")
        f.write("output=json\n")
        f.write("""s3=
    max_concurrent_requests = 36
""")

class S3Copy:
    def __init__(self, hosts, rank, wp, fp, args, **kw):
        self.hosts = hosts
        self.rank = rank
        self.host = hosts[rank]
        self.size = get_size()
        self.wp = wp
        self.fp = fp
        self.args = args

        self.conn = boto.s3.connection.S3Connection(
                aws_access_key_id = args.access_key,
                aws_secret_access_key = args.secret_key,
                is_secure=False)
        self.src = self.conn.get_bucket(args.src_bucket)
        self.dst = self.conn.get_bucket(args.dst_bucket)

        # multi-treading copy
        self.tasks = Queue.Queue(self.args.threads * 2)

    def msg(self, s):
        if self.args.verbose >= 0:
            stdout('[%d/%d] %s: %s\n' %
                (self.rank, self.size, self.host.hostname, s))

    def info(self, s):
        if self.args.verbose >= 1:
            stdout('[%d/%d] %s: %s\n' %
                (self.rank, self.size, self.host.hostname, s))

    def debug(self, s):
        if self.args.verbose >= 2:
            stdout('[%d/%d] %s: %s\n' %
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

    def _thread_copy_key(self, tid):
        thread_id = "t%d" % tid
        client_config = \
            botocore.config.Config(max_pool_connections=self.args.max_concurrency)
        copy_config = boto3.s3.transfer.TransferConfig(multipart_threshold=8388608,
            max_concurrency=self.args.max_concurrency, multipart_chunksize=8388608,
            num_download_attempts=5, max_io_queue=100, io_chunksize=262144)
        session = boto3.session.Session()
        client = session.client('s3', config=client_config)

        while True:
            key = self.tasks.get()

            if self.args.verbose >= 2:
                self.debug('%s: copy %s' % (thread_id, key.name))

            # Determine if copy
            if not self.args.overwrite:
                dst_key_info = None
                try:
                    dst_key_info = client.get_object(Bucket=self.dst.name, Key=key.name)
                except botocore.exceptions.ClientError as err:
                    if not 'NoSuchKey' in err.response['Error']['Code']: raise

                if dst_key_info and dst_key_info['ContentLength'] == key.size:
                    self.debug("%s: s3://%s/%s exists, skipping..." % \
                       (thread_id, self.dst.name, key.name))
                    self.tasks.task_done()
                    continue

            if self.args.dryrun:
                self.tasks.task_done()
                continue

            # Copy and retry on errors
            retry = 0
            while retry < self.args.retry:
                client.copy(CopySource={'Bucket': self.src.name, 'Key': key.name},
                    Bucket=self.dst.name, Key=key.name, Config=copy_config)
                # post check
                dst_key_info = None
                try:
                    dst_key_info = client.get_object(Bucket=self.dst.name, Key=key.name)
                    if dst_key_info['ContentLength'] == key.size: break
                except botocore.exceptions.ClientError as err:
                    self.info("%s: copy failed: %s: %s, retry %d\n" % \
                        (thread_id, key.name, err.response['Error']['Message'], retry))
                retry += 1

            self.tasks.task_done()

    def copy(self, prefixes):
        if self.rank == 0:
            self.info("%s: start copy with %d threads and %d connections..." % \
                (curr_time(), self.args.threads, self.args.max_concurrency))

        for i in range(self.args.threads):
            t = threading.Thread(target=self._thread_copy_key, args=(i,))
            t.daemon = True
            t.start()

        for prefix in prefixes:
            for key in self.src.get_all_keys(prefix=prefix):
                self.tasks.put(key)

        self.tasks.join()

    def get_prefix_list(self, length=2):
        start_time = timer()

        self.info("Creating prefix list, this may take a few minutes...")
        all_prefixes = self.args.prefix_list.split(',')
        if not all_prefixes[0]:
            all_prefixes = [''.join(comb) for comb in itertools.combinations(CHAR_SET, length)]

        # filter out non-existing prefix
        prefixes = []
        for p in all_prefixes:
            if len(self.src.get_all_keys(prefix=p, max_keys=1)) > 0:
                prefixes.append(p)

        if len(prefixes) < self.size:
            self.info("Warning: # of prefixes %d less than # of workers %d" % (len(prefixes), self.size))
            return []

        chunk_list = list(chunks(prefixes, len(prefixes) / self.size))
        prefix_list = list(chunk_list[0:self.size])
        if len(chunk_list) > self.size:
            reminders = list(itertools.chain.from_iterable(chunk_list[self.size:]))
            for i, p in enumerate(reminders):
                prefix_list[i].append(p)

        self.info("Done with creating prefix list, took %.3f seconds." % (timer() - start_time))
        return prefix_list

    def run(self):
        if self.rank == 0:
            self.info("%s: copy s3://%s to s3://%s" % (curr_time(), self.src.name, self.dst.name))
            prefix_list = self.get_prefix_list(self.args.prefix_length)
            self.broadcast(repr(prefix_list))

        prefix_list = eval(self.receive())
        if not prefix_list: return

        self.info("Prefix list to copy: %r" % prefix_list[self.rank])

        self.barrier()
        elapsed = timer()

        self.copy(prefix_list[self.rank])

        self.barrier()
        elapsed = timer() - elapsed

        if self.rank == 0:
            self.info("Done, took %.3f seconds" % elapsed)

def main():
    try:
        set_close_on_exec()
    except IOError:
        stderr('usage: gxpc mw s3cp.py\n')
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
        if rank == 0: stdout('s3cp.py: error: %s\n' % e)
        return 1

    if args.aws_config:
        aws_configure(args)
        return 0

    s3cp = S3Copy(hosts, rank, wp, fp, args)
    s3cp.run()
    return 0

if __name__ == '__main__':
    sys.exit(main())
