#!/usr/bin/env python
#
#
#
'''makesched.py
'''
__version__ = '$Revision: 1.1.1.1 $'
__date__ = '$Date: 2008/07/09 14:41:06 $'
__author__ = '$Author: ttaauu $'
__credit__ = ''

import socket
import string
import pickle
import re
import Queue
import threading
import os
import sys
import popen2
import random
import glob
import signal
import time
import stat

MAX_COUNT = 10
TIME_LIMIT = 60
REALLOC_LIMIT = 10
NCARRIERS = 10
BACKLOG = 256
HOSTNAME = socket.gethostname()
USER = os.environ['USER']

def eprint(str_):
    '''eprint'''
    sys.stderr.write('%s' % str_)
    sys.stderr.flush()

def randstr(num):
    '''randstr'''
    alphabets = string.digits + string.letters
    str_ = ''
    while len(str_) < num:
        str_ += alphabets[random.randrange(len(alphabets))] 
    return str_

def select_input_files(file_like):
    '''select_input_files'''
    return filter(lambda x: not os.access(x, os.R_OK), file_like)

def get_nonecho_command(command):
    '''get_nonecho_command'''
    lines = command.splitlines()
    printf_lines = '\n'.join(filter(lambda x: '/usr/bin/printf' in x, lines))
    child = os.popen(printf_lines)
    return child.read()
    
def get_exits_files(command):
    '''get_exits_files'''
    pieces = command.split()
    file_like = []
    for piece in pieces:
        file_like.extend(glob.glob(piece))
    return filter(lambda x: os.access(x, os.R_OK), file_like)

def mkdir_remote(host, path):
    '''mkdir_remote'''
    cmd = "gxpc e -h %s 'mkdir -p %s'" % (host, path)
    child = popen2.Popen4(cmd)
    status = child.wait()
    assert status == 0, (cmd, status)

class Message:
    '''Message'''
    def __init__(self, type_):
        self.type = type_
        self.status = None
        self.body = None
        self.dst = None
        self.master_to_worker = None
        self.filename = None
        self.tid = None
        self.realloc = False

    def __str__(self):
        str_ = str(self.__class__) + '('
        for attr in dir(self):
            if attr[0] != '_':
                str_ += '%s=%s,' % (attr, getattr(self, attr))
        return str_ + ')'



class MyQueue(list):
    '''MyQueue'''
    def pop(self):
        '''pop'''
        return super(MyQueue, self).pop(0)

    def push(self, elem):
        '''push'''
        return self.append(elem)


class Carrier(threading.Thread):
    '''Carrier'''
    def __init__(self, fcq, tcq, i):
        self.__from_carriers_queue = fcq
        self.__to_carriers_queue = tcq
        pat = '(.*)-[a-z_]*-\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}-\d*'
        self.__node_pat = re.compile(pat)
        self.nat = ('imade', 'kyoto')
        self.th_id = i
        threading.Thread.__init__(self)

    def get_parameter(self):
        '''get_parameter'''
        return self.__to_carriers_queue.get()

    def __get_short_name(self, hostname):
        '''__get_short_name'''
        return self.__node_pat.match(hostname).groups()[0]

    def make_option(self, path):
        '''make_option'''
        if path[-1][:5] not in self.nat:
            path = (path[0], path[-1])
        elif path[-1][5:8] == '000':
            path = (path[0], path[-1])
        option = ''
        if len(path):
            pre_node = self.__get_short_name(path[0])
            for longname in path[1:]:
                node = self.__get_short_name(longname)
                option += '--ssh=%s:%s ' % (pre_node, node)
                pre_node = node
        return option

    def make_command(self, target_file, path):
        src = self.__get_short_name(path[0])
        dst = self.__get_short_name(path[-1])
        master_to_worker = (src == HOSTNAME)
        option = self.make_option(path)
        hosts = '"' + reduce(lambda x, y: x+'|'+y, path) + '"'
        body = ''
        src_file = '%s:%s' % (src, target_file)
        dst_dir = '%s:%s' % (dst, target_file)
        if master_to_worker:
            mode = "--mode=%d" % os.stat(target_file)[stat.ST_MODE]
        else:
            mode = ''
        return 'gxpc mw -h %s xcp %s %s %s %s' % \
               (hosts, option, mode, src_file, dst_dir)

    def do_task(self, parameter):
        '''do_task'''
        target_file, worker_gid, path = parameter
        cmd = self.make_command(target_file, path)
        eprint('xcp%d: %s\n' % (self.th_id, cmd))
        start_time = time.time()
        child = popen2.Popen4(cmd)
        status = -1
        while time.time() - start_time < TIME_LIMIT:
            status = child.poll()
            if status != -1:
                break
        if status == -1:
            os.kill(child.pid, signal.SIGINT)
            eprint("xcp%d: killed %s\n" % (self.th_id, cmd))
        else:
            eprint("xcp%d: finished %s\n" % (self.th_id, cmd))
        log = child.fromchild.read()
        #eprint("xcp%d: %s" % (self.th_id, log))
        msg = Message('carrier')
        msg.master_to_worker = (HOSTNAME in path[0])
        msg.dst = worker_gid
        msg.status = status
        msg.body = target_file
        msg.parameter = parameter
        self.__from_carriers_queue.put(msg)

    def run(self):
        '''run'''
        while True:
            parameter = self.get_parameter()
            self.do_task(parameter)

class Carriers:
    '''Carriers'''
    def __init__(self, fcq):
        self.__to_carriers_queue = Queue.Queue()
        self.__carriers = []
        for i in range(NCARRIERS):
            car = Carrier(fcq, self.__to_carriers_queue, i)
            car.setDaemon(1)
            car.start()
            self.__carriers.append(car)

    def submit_task(self, args):
        '''submit_task'''
        self.__to_carriers_queue.put(args)

    def qsize(self):
        return self.__to_carriers_queue.qsize()

class Task:
    '''Task'''
    def __init__(self, msg):
        self.tid = msg.tid
        self.filename = msg.filename
        self.command = msg.body
        self.count = 0
        self.dst = []
        self.output_lock = False
        self.time = 0

    def count_up(self):
        '''count_up'''
        self.count += 1

    def get_filename(self):
        '''get_filename'''
        return self.filename

    def get_command(self):
        '''get_command'''
        return self.command

    def get_count(self):
        '''get_count'''
        return self.count

    def set_dst(self, dst):
        self.dst.append(dst)

    def time_count_up(self):
        self.time += 1


class ResourceManager:
    '''ResourceManager'''
    def __init__(self, fcq):
        self.__resource_pool = MyQueue()
        self.__task_pool = MyQueue()
        self.__to_worker_fd = sys.stdout
        self.__issued_task = {}
        self.__stat_out = None
        self.__gxpc_stat()
        self.file_to_filename = {}
        self.__carriers = Carriers(fcq)
        self.__carrying_files = {}
        __scripts_dir = '%s/.dmake/' % os.environ['HOME']
        self.file_pat = re.compile('(%s[a-zA-z0-9.-]*)' % __scripts_dir)
        self.uid = os.getuid()
        self.realloc_maneger = ScriptFileExtractor()

    def __gxpc_stat(self):
        '''__gxpc_stat'''
        cmd = 'gxpc stat'
        child = popen2.Popen4(cmd)
        child.wait()
        output = child.fromchild.readlines()
        output.reverse()
        self.__stat_out = output

    def __send_msg(self, msg):
        '''__send_msg'''
        msg.src = 'master'
        self.__to_worker_fd.write(str((pickle.dumps(msg), )) + '\n')
        self.__to_worker_fd.flush()

    def __try_issue_task(self):
        '''__try_issue_task'''
        while len(self.__task_pool) > 0:
            if len(self.__resource_pool) > 0:
                msg = self.__task_pool.pop()
                assert msg.type == 'task', msg
                msg.dst = self.__resource_pool.pop()
                if msg.tid in self.__issued_task:
                    for dst in self.__issued_task[msg.tid].dst:
                        # avoiding that 2 same processes work on the same host
                        if dst[:-8] == msg.dst[:-8]:
                            self.__resource_pool.push(msg.dst)
                            return
                    self.__issued_task[msg.tid].count_up()
                else:
                    self.__issued_task[msg.tid] = Task(msg)
                self.__issued_task[msg.tid].set_dst(msg.dst)
                nonecho_command = get_nonecho_command(msg.body)
                eprint("#---------------------------------------\n")
                eprint('tid = %s, hostname = %s\n' % (msg.tid, msg.dst))
                eprint(nonecho_command)
                eprint("#---------------------------------------\n")
                exits_files = get_exits_files(nonecho_command)
                msg.exits_files = filter(os.path.isfile, exits_files)
                # mkdir if not exits on remote
                exits_dir = filter(os.path.isdir, exits_files)
                map(lambda x: mkdir_remote(msg.dst[:-8], x), exits_dir)
                self.__send_msg(msg)
            else:
                break
        return True

    def handle_avail_msg(self, msg):
        '''handle_avail_msg'''
        self.__resource_pool.push(msg.src)
        return self.__try_issue_task()

    def __get_pid(self, file_):
        '''get_filename'''
        if file_ not in self.file_to_filename:
            pid_list = os.listdir('/proc/')
            my_pid_list = []
            for pid in pid_list:
                try:
                    uid = os.stat('/proc/'+pid)[stat.ST_UID]
                except OSError:
                    continue
                if uid == self.uid:
                    my_pid_list.append(pid)
            for pid in my_pid_list:
                cmdline = '/proc/%s/cmdline' % pid
                if os.access(cmdline, os.R_OK):
                    file_obj = open(cmdline)
                    body = file_obj.read()
                    match_obj = self.file_pat.search(body)
                    if match_obj:
                        script_file = match_obj.groups()[0]
                        self.file_to_filename[script_file] = int(pid)
        return self.file_to_filename.get(file_, None)

    def __send_stop_msg(self, dst):
        msg = Message('stop')
        msg.dst = dst
        self.__send_msg(msg)

    def handle_status_msg(self, msg):
        '''handle_status_msg'''
        eprint('########################################\n')
        eprint('%s\n' % msg.body)
        eprint('########################################\n')
        task = self.__issued_task[msg.tid]
        filename = task.get_filename()
        count = task.get_count()
        command = task.get_command()
        task.dst.remove(msg.src)
        if len(task.dst) == 0:
            del self.__issued_task[msg.tid]
        if msg.status == -1:
            return None
        if msg.status == 0 or (count > MAX_COUNT and len(task.dst) == 0):
            # success or give up
            pid = self.__get_pid(filename)
            assert type(pid) == int, (filename, pid, self.__issued_task)
            try:
                if msg.status == 0:
                    os.kill(pid, signal.SIGUSR1)
                else:
                    os.kill(pid, signal.SIGUSR2)
                eprint('killed: pid=%s status=%s\n' % (pid, msg.status))
            except OSError:
                eprint('duplicate: pid=%s status=%s\n' % (pid, msg.status))
            while True:
                if os.access(filename, os.F_OK):
                    time.sleep(0.01)
                else:
                    break
            if msg.status == 0:
                map(self.__send_stop_msg, task.dst)
        else:
            # retry
            task_msg = Message('task')
            task_msg.tid = msg.tid
            task_msg.body = command
            task_msg.filename = filename
            self.__task_pool.push(task_msg)
            self.__try_issue_task()
        return None

    def __unescape_sharp(self, str_):
        '''replace '\#' -> '#'

        DMAKE doesn't get off the backslash in front of the sharp.
        (different from MAKE)
        '''
        return str_.replace('\\\\#', '#')

    def handle_task_msg(self, msg):
        '''handle_task_msg'''
        msg.body = self.__unescape_sharp(msg.body)
        self.__task_pool.push(msg)
        return self.__try_issue_task()

    def parse_tree(self, output, worker):
        '''parse_tree'''
        depth = -1
        path = []
        pat = re.compile('\(= (.*)\)')
        for line in output:
            if line[0] == '/':
                continue
            if depth == -1:
                index = line.find(worker)
                if index != -1:
                    depth = re.search('\S', line).start()
                    path.append(pat.search(line).groups()[0])
            else:
                if len(line) - len(line.lstrip()) == depth - 1:
                    depth -= 1
                    path.append(pat.search(line).groups()[0])
        path.reverse()
        return path
        
    def __start_carrier(self, files_, worker_gid, input_):
        '''__start_carrier'''
        if files_ == []:
            return 0
        files = []
        map(lambda x: x in files or files.append(x), files_)
        path = self.parse_tree(self.__stat_out, worker_gid[:-8])
        if path == []:
            self.__gxpc_stat()
            path = self.parse_tree(self.__stat_out, worker_gid[:-8])
            if path == []:
                raise RuntimeError, self.__stat_out
        if not input_ == 'input_req':
            path.reverse()
        for file_ in files:
            self.__carriers.submit_task((file_, worker_gid, path))
        self.__carrying_files[(worker_gid, input_=='input_req')] = files
        return len(files)

    def handle_input_req_msg(self, msg):
        '''handle_input_req_msg'''
        nfiles = self.__start_carrier(msg.body, msg.src, 'input_req')
        assert nfiles > 0, (msg.body, msg.src, 'input_req')
        return nfiles

    def handle_output_req_msg(self, msg):
        '''handle_output_req_msg'''
        task = self.__issued_task[msg.tid]
        if task.output_lock:
            msg_ = Message('output_ng')
            msg_.dst = msg.src
            self.__send_msg(msg_)
            return None
        else:
            task.output_lock = True
        for dir_ in msg.outdir:
            mkdir_remote(msg.src[:-8], dir_)
        files = select_input_files(msg.body)
        nfiles = self.__start_carrier(files, msg.src, 'output_req')
        if nfiles == 0:
            msg_ = Message('output_ok')
            msg_.dst = msg.src
            msg_.body = None
            self.__send_msg(msg_)
        return nfiles

    def handle_carrier_msg(self, msg):
        '''handle_carrier_msg'''
        key = (msg.dst, msg.master_to_worker)
        if msg.status != 0:
            return_msg = Message('stop')
            return_msg.dst = msg.dst
            self.__send_msg(return_msg)
            del self.__carrying_files[key]
            #self.__carriers.submit_task(msg.parameter)
            return
        try:
            self.__carrying_files[key].remove(msg.body)
        except ValueError:
            eprint('handle_carrier_msg error\n')
            eprint('%s\n' % self.__carrying_files)
            eprint('%s\n' % str(key))
            eprint('%s\n' % msg.body)
            return
        if len(self.__carrying_files[key]) == 0:
            if msg.master_to_worker:
                return_msg = Message('input_ok')
            else:
                return_msg = Message('output_ok')
            return_msg.dst = msg.dst
            return_msg.body = msg.body
            self.__send_msg(return_msg)
            del self.__carrying_files[key]

    def handle_error_msg(self, msg):
        '''handle_error_msg'''
        eprint(msg.body)
        sys.exit(1)
        
    def handle_msg(self, msg):
        '''Handle message from worker or qrsh

        This method pulls out message TYPE and calls handle_TYPE_msg.  
        '''
        method = getattr(self, "handle_%s_msg" % msg.type)
        return method(msg)

    def try_reallocate_task(self):
        if len(self.__resource_pool) == 0 or len(self.__task_pool) > 0 \
               or self.__carriers.qsize() > 0:
            return
        self.realloc_maneger.processed = []
        tasks = self.realloc_maneger.get_tasks()
        
        for task in tasks:
            task.body = self.__unescape_sharp(task.body)
        if len(tasks) > len(self.__resource_pool):
            self.__task_pool.extend(random.sample(tasks, \
                                                  len(self.__resource_pool)))
        else:
            self.__task_pool.extend(tasks)
        self.__try_issue_task()
            

class FdEater(threading.Thread):
    '''FdEater'''
    def __init__(self, queue, fd):
        self.__queue = queue
        self.__from_worker_fd = fd
        threading.Thread.__init__(self)

    def run(self):
        '''run'''
        while True:
            line = self.__from_worker_fd.readline()
            if line == '':
                msg = Message('error')
                msg.body = '%s: All workers may exit.\n' % self.__class__
                self.__queue.put(msg)
                return
            try:
                tuple_ = eval(line)
            except SyntaxError:
                self.__queue.put(None)
            assert isinstance(tuple_, tuple), tuple_
            string_ = tuple_[0]
            assert isinstance(string_, str), string_
            msg = pickle.loads(string_)
            self.__queue.put(msg)


class ScriptFileExtractor:
    '''ScriptFileExtractor'''
    def __init__(self):
        self.__scripts_dir = '%s/.dmake/' % os.environ['HOME']
        self.is_script_file = lambda x: 'dmake.script.' in x
        self.processed = []

    def get_tasks(self):
        '''get_tasks'''
        scripts_like = os.listdir(self.__scripts_dir)
        script_files = filter(self.is_script_file, scripts_like)
        if len(script_files) < 10:
            eprint('script_files = %s\n' % script_files)
        else:
            eprint('len(script_files) = %s\n' % len(script_files))
        unprocessed = filter(lambda x: x not in self.processed, script_files)
        self.processed = script_files
        tasks = []
        for script_file in unprocessed:
            abspath = os.path.join(self.__scripts_dir + script_file)
            assert abspath != None, (self.__scripts_dir, script_file)
            if os.access(abspath, os.F_OK):
                msg = Message('task')
                msg.body = open(abspath).read()
                msg.tid = script_file[-6:]
                msg.filename = abspath
                tasks.append(msg)
        return tasks


class CentralCommunicator:
    '''CentralCommunicator'''
    def __init__(self):
        self.__from_carriers_queue = Queue.Queue()
        self.__resource_manager = ResourceManager(self.__from_carriers_queue)
        self.__from_worker_queue = Queue.Queue()
        self.__stdin_eater = FdEater(self.__from_worker_queue, sys.stdin)
        self.__extructor = ScriptFileExtractor()

    def run(self):
        '''run'''
        self.__stdin_eater.setDaemon(True)
        self.__stdin_eater.start()
        count = 0
        while True:
            while not self.__from_carriers_queue.empty():
                msg = self.__from_carriers_queue.get()
                self.__resource_manager.handle_msg(msg)
            while not self.__from_worker_queue.empty():
                msg = self.__from_worker_queue.get()
                self.__resource_manager.handle_msg(msg)
            tasks = self.__extructor.get_tasks()
            for msg in tasks:
                self.__resource_manager.handle_msg(msg)
                count = 0
            if count > REALLOC_LIMIT > -1:
                self.__resource_manager.try_reallocate_task()
                count = 0
                carriers = self.__resource_manager._ResourceManager__carriers
                carriers_set = carriers._Carriers__carriers
                living = filter(lambda x: x.isAlive(), carriers_set)
                eprint('carriers queue size = %s, # of livings = %s\n' % \
                       (carriers._Carriers__to_carriers_queue.qsize(), \
                        len(living)))
            time.sleep(0.5)
            count += 1


def main():
    '''main'''
    central_communicator = CentralCommunicator()
    try:
        central_communicator.run()
    except:
        sys.stdout.write('\n')
        sys.stdout.flush()
        raise
    return True

if __name__ == '__main__':
    if main():
        sys.exit(0)
    else:
        sys.exit(1)
