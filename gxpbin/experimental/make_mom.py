#!/usr/bin/env python
__version__ = '$Revision: 1.1.1.1 $'
__date__ = '$Date: 2008/07/09 14:41:05 $'
__author__ = '$Author: ttaauu $'
__credit__ = ''

import sys
import pickle
import popen2
import time
import Queue
import signal
while True:
    try:
        from make_sched import *
    except EOFError, ValueError:
        time.sleep(0.1)
    else:
        break

random.seed(os.environ.get('GXP_EXEC_IDX', None))
hostname = os.environ.get('GXP_HOSTNAME', socket.gethostname()) + randstr(8)

def heprint(str_):
    eprint('%s: %s\n' % (hostname, str_))

class Worker:
    def __init__(self):
        self.__from_master_fd = os.fdopen(4, 'r')
        self.__to_master_fd = os.fdopen(3, 'w')
        self.__from_master_queue = Queue.Queue()
        self.__from_master_eater = FdEater(self.__from_master_queue, \
                                           self.__from_master_fd)
        self.__from_master_eater.setDaemon(1)
        self.__from_master_eater.start()
        self.tid = None

    def send_msg(self, msg):
        msg.src = hostname
        self.__to_master_fd.write(str((pickle.dumps(msg), )) + '\n')
        self.__to_master_fd.flush()

    def send_avail_msg(self):
        self.tid = None
        msg = Message('avail')
        self.send_msg(msg)

    def get_stdin(self):
        line = self.__from_master_fd.readline()
        if line == '':
            raise RuntimeError, 'Master Dead'
        try:
            tuple_ = eval(line)
        except SyntaxError:
            eprint("%s: Master may be dead. Exit.\n" % hostname)
            sys.stderr.flush()
            sys.exit(1)            
        assert isinstance(tuple_, tuple), tuple_
        string_ = tuple_[0]
        assert isinstance(string_, str), string_
        msg = pickle.loads(string_)
        return msg

    def is_for_me(self, dst):
        return hostname == dst

    def get_command(self):
        while True:
            msg = self.__from_master_queue.get()
            if msg == None:
                eprint("%s: Master may be dead. Exit.\n" % hostname)
                sys.exit(1)            
            if self.is_for_me(msg.dst):
                if msg.type == 'task':
                    return msg.body, msg.tid, msg.exits_files
                else:
                    eprint("%s: received %s message\n" % (hostname, msg.type))

    def cleanup(self, pid, command):
        os.kill(pid, signal.SIGKILL)
        eprint('%s: killed %d\n' % (hostname, pid))
        '''
        nonecho_command = get_nonecho_command(command)
        exits_files = get_exits_files(nonecho_command)
        for exits_file in exits_files:
            os.remove(exits_file)
            eprint('%s: removed %s\n' % (hostname, exits_file))
        '''

    def do_cmd(self, command, tid):
        child = popen2.Popen4(command)
        status = -1
        body = ''
        while True:
            try:
                msg = self.__from_master_queue.get_nowait()
            except Queue.Empty:
                pass
            else:
                if msg == None:
                    eprint("%s: Master may be dead. Exit.\n" % hostname)
                    self.cleanup(child.pid, command)
                    sys.exit(1)            
                if self.is_for_me(msg.dst):
                    if msg.type == 'stop':
                        eprint("%s: received stop message\n" % hostname)
                        self.cleanup(child.pid, command)
                        child.wait()
                        break
                    else:
                        raise RuntimeError, msg
            status = child.poll()
            if status != -1:
                body = child.fromchild.read()
                break
            time.sleep(0.5)
        eprint('%s: status=%s\n' % (tid, status))
        nonecho_command = get_nonecho_command(command)
        exits_files = get_exits_files(nonecho_command)
        return '%s: return code %d\n'% (hostname, status) + body, \
               status, exits_files

    def send_status_msg(self, body, status, tid):
        msg = Message('status')
        msg.tid = tid
        msg.body = body
        msg.status = status
        self.send_msg(msg)

    def send_input_req_msg(self, infiles, tid):
        msg = Message('input_req')
        msg.tid = tid
        msg.body = infiles
        self.send_msg(msg)

    def recv_input_ok_msg(self):
        while True:
            msg = self.__from_master_queue.get()
            if msg == None:
                eprint("%s: Master may be dead. Exit.\n" % hostname)
                sys.exit(1)            
            if self.is_for_me(msg.dst):
                return msg.type

    def send_output_req_msg(self, outfiles, outdirs, tid):
        msg = Message('output_req')
        msg.tid = tid
        msg.body = outfiles
        msg.outdir = outdirs
        self.send_msg(msg)

    def recv_output_ok_msg(self):
        while True:
            msg = self.__from_master_queue.get()
            if msg == None:
                eprint("%s: Master may be dead. Exit.\n" % hostname)
                sys.exit(1)            
            if self.is_for_me(msg.dst):
                return msg.type
        
    def run(self):
        while True:
            self.send_avail_msg()
            heprint('send_avail_msg')
            command, tid, file_like = self.get_command()
            self.tid = tid
            infiles = select_input_files(file_like)
            if len(infiles) > 0:
                self.send_input_req_msg(infiles, tid)
                heprint('send_input_req_msg %s' % tid)
                type_ = self.recv_input_ok_msg()
                heprint('recv_input_ok_msg %s' % tid)
                if type_ == 'stop':
                    continue
                assert type_ == 'input_ok', type_
            body, status, outfiles = self.do_cmd(command, tid)
            heprint('do_cmd %s' % tid)
            if len(outfiles) > 0 and status == 0:
                outdirs = filter(os.path.isdir, outfiles)
                map(outfiles.remove, outdirs)
                self.send_output_req_msg(outfiles, outdirs, tid)
                heprint('send_output_req_msg %s' % tid)
                type_ = self.recv_output_ok_msg()
                heprint('recv_output_ok_msg %s' % tid)
                if type_ == 'stop':
                    continue
                if type_ == 'output_ng':
                    status = -1
                    body = None
                assert type_ == 'output_ok' or type_ == 'output_ng', type_
            self.send_status_msg(body, status, tid)
            heprint('send_status_msg %s' % tid)
            
def main():
    w = Worker()
    try:
        w.run()
    except RuntimeError:
        raise
        return False
    return True

if __name__ == '__main__':
    if main():
        sys.exit(0)
    else:
        sys.exit(1)
