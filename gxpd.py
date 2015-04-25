# Copyright (c) 2005-2009 by Kenjiro Taura. All rights reserved.
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
# $Header: /cvsroot/gxp/gxp3/gxpd.py,v 1.25 2013/10/25 12:00:53 ttaauu Exp $
# $Name:  $
#


#
# TODO:
#   mw ... requires clients to write newlines. otherwise the msg 
#   does not get through. this stems from the follwing.
#    action_create_proc_or_peer -> 
#    set_pipe_atomicity -> set_expected, where pipe is set
#    to have line atomicity.

enable_connection_upgrade = 0

import errno,fcntl,os,random,re,select,signal,socket
import stat,string,sys,time,types
# import profile,pstats

import ioman,gxpm,opt,ifconfig,this_file

dbg=0

# see ChangeLog entry of 2007-12-25
fix_2007_12_25 = 1

# -------------------------------------------------------------------
# 
# -------------------------------------------------------------------

# ioman.process_base
#  | |
#  | +-- ioman.child_process
#  |       |
#  |       +-- ioman.pipe_process
#  |       +-- ioman.sockpair_process
#  |                         |
#  +---- gxp_peer            |
#          |  |              |
#          |  +--------------+----- child_peer
#          |
#          +---- parent_peer
#          +---- client_peer
#

class gxp_peer_base(ioman.process_base):
    """
    The gxp_peer_base class represents another gxp process
    this process connects to. It may be a child gxp process
    invoked by this process (it may in fact be an rsh-like
    process that remotely forks gxp), a parent gxp process
    that invoked this process (it may in fact be an rsh-like
    process that locally forked this process), or a command
    interpreter or any external process that sends gxp commands
    to this process.
    """
    STATE_IN_PROGRESS = 0               
    STATE_LIVE = 1
    def __init__(self, name, init_state, critical):
        ioman.process_base.__init__(self)
        # when state is in progress, it is a child process
        # that is not ready to accept commands. i.e. `explore'
        # is in progress
        self.state = init_state
        self.peer_name = name
        self.critical = critical
        self.wch = None                 # set by subclass
        
    def write_channel(self):
        return self.wch

    def set_write_channel(self, ch):
        self.wch = ch

class parent_peer(gxp_peer_base):
    """
    represent process that spawns me
    """
    def __init__(self, name, no_stdin):
        gxp_peer_base.__init__(self, name,
                               gxp_peer_base.STATE_LIVE, 1)
        # self.wch = None
        # now no_stdin is always zero
        assert no_stdin == 0
        if no_stdin == 0:
            pch0 = ioman.primitive_channel_fd(0, 1)
            ch0 = ioman.rchannel_process(pch0, self, None)
            ch0.set_expected([("M",)])
        pch1 = ioman.primitive_channel_fd(1, 1)
        pch2 = ioman.primitive_channel_fd(2, 1)
        ch1 = ioman.wchannel_process(pch1, self, None)
        ch2 = ioman.wchannel_process(pch2, self, None)
        self.set_write_channel(ch1)
        self.task = None

    def clear_critical(self):
        self.critical = 0

    def discard(self):
        # one day I tried to terminate inst_local.py once it has done
        # the job and connection is successfully upgraded.
        # need more investigations to make it work
        if 0:
            for ch in self.r_channels_rev.keys():
                ch.discard()
            for ch in self.w_channels_rev.keys():
                ch.discard()
        if 0:
            pid = os.fork()
            if pid > 0:
                os._exit(0)

    def is_garbage(self):
        return 0
    
class connect_peer(gxp_peer_base):
    """
    represent process that connects to me
    """
    def __init__(self, name, so):
        gxp_peer_base.__init__(self, name, # peer_name == ""
                               gxp_peer_base.STATE_LIVE, 0)
        pch = ioman.primitive_channel_socket(so, 0)
        ch_r = ioman.rchannel_process(pch, self, None)
        ch_r.set_expected([("M",)])
        ch_w = ioman.wchannel_process(pch, self, None)
        self.set_write_channel(ch_w)
        self.task = None

    def set_critical(self):
        self.critical = 1

    def discard(self):
        pass

    def is_garbage(self):
        return 0

class child_peer(ioman.child_process,gxp_peer_base):
    """
    represent a gxp process that is invoked by me.
    It is typically a result of `explore' commands.
    """
    upgrading_status_init = 0
    upgrading_status_in_progress = 1
    upgrading_status_succeeded = 2
    upgrading_status_failed = 3
    def __init__(self, cmd, pipe_desc, env, cwd, rlimits):
        gxp_peer_base.__init__(self, "", # peer_name = ""
                               gxp_peer_base.STATE_IN_PROGRESS, 0)
        ioman.child_process.__init__(self, cmd, pipe_desc, env, cwd, rlimits)
        self.target_label = None
        self.hostname = None
        self.task = None
        self.rid = None
        self.upgrading_channel_w = None
        self.upgrading_channel_r = None
        self.upgrading_status = child_peer.upgrading_status_init

class child_task_process(ioman.child_process):
    """
    This class represents a non-gxp child process. That is,
    processes invoked as a result of `e' commands etc.
    """
    def __init__(self, cmd, pipe_desc, env, cwd, rlimits):
        ioman.child_process.__init__(self, cmd, pipe_desc, env, cwd, rlimits)
        self.task = None
        self.rid = None

# -------------------------------------------------------------------
# 
# -------------------------------------------------------------------

class task_tree_node:
    """
    This class represents a node of a task tree.
    a task is conceptually a set of distributed processes.
    it is typically the result of executing an 'e' command.

    They form a tree along the tree of gxp processes.

    The tree is used to route msgs/commands from the command
    interpreter to processes, to route msgs from processes to
    the command interpreter, and to signal events (termination, etc.)
    to the command interpreter.

    Though it is called a tree, a single task node may in general
    have multiple parents.
    
    """
    def __init__(self, tid):
        self.tid = tid                  # name of the task (string)
        self.parent_peers = {}          # parents
        self.child_peers = {}           # child -> weight
        # processes are locally running processes constituing
        # the task
        self.processes = {}             # process -> 1
        # each process has `a relative process id', which 
        # identifies a particular process WITHIN a task.
        # this is specifieid by the command interpreter to specify
        # a particular process it wants to operate on.
        # thus we keep a lookup table from relative id to the process
        self.proc_by_rid = {}           # rid -> proc
        self.persist = 0                # 1 if it should never terminate
        self.weight = 1
        # note : self.processes give pid -> proc

    def show(self):
        return ("task_tree_node(%s, %d parents, %d children, %d procs, %d proc_by_rid)"
                % (self.tid, len(self.parent_peers), len(self.child_peers),
                   len(self.processes), len(self.proc_by_rid)))

    def forward_up(self, m, msg):
        """
        send msg m to all parents.
        """
        # msg = gxpm.unparse(m)           # FASTER ---------
        if dbg>=1:
            ioman.LOG(("sending up msg to %d parents\n"
                       % len(self.parent_peers)))
        for parent in self.parent_peers.keys():
            x = parent.write_msg(msg)
            if x == -1:
                # this parent has gone
                del self.parent_peers[parent]
                if dbg>=1:
                    ioman.LOG("up msg to parent lost %d bytes "
                              "%d parents left [%s ...]\n"
                              % (len(msg), len(self.parent_peers), msg[0:30]))
            else:
                if dbg>=2:
                    ioman.LOG("up msg to parent OK\n")
        # what we should do if we have no parents at all?

    def close_connection_to_parent(self, parent):
        if dbg>=2:
            ioman.LOG("close_connection_to_parent:\n")
        x = parent.write_eof()
        if dbg>=1 and x == -1:
            ioman.LOG("close_connection_to_parent: parent.write_eof() failed\n")

    def close_up(self):
        """
        If close_on_fin flag is set, this task node has been told
        to close channels to the parents upon termination.
        So we close them.
        """
        for parent,keep_connection in self.parent_peers.items():
            if keep_connection < gxpm.keep_connection_forever:
                self.close_connection_to_parent(parent)
                del self.parent_peers[parent]

    def is_dead(self):
        """
        This particular node is considered dead if it has no
        running child processes and child peers. used to detect
        termination of the whole task.
        """
        if self.persist: return 0
        if len(self.child_peers) > 0: return 0
        if len(self.processes) > 0: return 0
        return 1

# -------------------------------------------------------------------
# options to bring up gxp
# -------------------------------------------------------------------

class gxpd_opts(opt.cmd_opts):
    def __init__(self):
        #             (type, default)
        # types supported
        #   s : string
        #   i : int
        #   f : float
        #   l : list of strings
        #   None : flag
        opt.cmd_opts.__init__(self)
        # gupid of parent gxpd.py
        self.parent = ("s", "")
        # in which name it is explored
        self.target_label = ("s", "")
        # address to listen to
        self.listen = ("s", "unix:")
        # 1 if it is created with --create_session 1
        # self.created_explicitly = ("i", 0)
        # prefix of stdout/stderr/unix-socket
        self.name_prefix = ("s", "gxp-00000000")
        self.qlen = ("i", 1000)
        self.remove_self = (None, 0)
        self.root_gupid = ("s", "")
        self.continue_after_close = (None, 0)
        # short version
        self.p = "parent"
        self.l = "listen"


# -------------------------------------------------------------------
# 
# -------------------------------------------------------------------


class gxpd_environment:
    """
    Environment variables set for gxp daemons and inherited
    to subprocesses
    """
    def __init__(self, dict):
        self.dict = dict
        e = os.environ
        for k,v in dict.items():
            setattr(self, k, v)
            e[k] = v


class gxpd_profiler:
    def __init__(self):
        self.prof_file = None
        self.prof = None
        self.to_start = 0
        self.to_stop = 0

    def mark_start(self, file):
        import hotshot
        if self.prof is None:
            self.to_start = 1
            self.prof_file = file
            self.prof = hotshot.Profile(self.prof_file)
            return 0
        else:
            return -1

    def started(self):
        self.to_start = 0

    def mark_stop(self):
        if self.prof is not None:
            self.to_stop = 1
            return 0
        else:
            return -1

    def stopped(self):
        import hotshot.stats
        self.to_stop = 0
        self.prof.close()
        stats = hotshot.stats.load(self.prof_file)
        stats.strip_dirs()
        stats.sort_stats('time', 'calls')
        self.prof_file = None
        self.prof = None


# -------------------------------------------------------------------
# 
# -------------------------------------------------------------------

def Es(s):
    os.write(2, s)

def get_this_file_xxx():
    g = globals()
    file = None
    if __name__ == "__main__" and \
       len(sys.argv) > 0 and len(sys.argv[0]) > 0:
        # run from command line (python .../gxpd.py)
        file = sys.argv[0]
    elif g.has_key("__file__"):
        # appears this has been loaded as a module
        file = g["__file__"]
        # if it is .pyc file, get .py instead
        m = re.match("(.+)\.pyc$", file)
        if m: file = m.group(1) + ".py"
    if file is None:
        Es("cannot find the location of gxpd.py\n")
        return None
    #file = os.path.abspath(file)
    file = os.path.realpath(file)
    if os.access(file, os.F_OK) == 0:
        Es("source file %s is missing!\n" % file)
        return None
    elif os.access(file, os.R_OK) == 0:
        Es("cannot read source file %s!\n" % file)
        return None
    else:
        return file

class gxpd(ioman.ioman):
    """
    gxp daemon that basically has the following functions.
    (1) run a child process
    (2) manage all IOs (stdin/out/err, pipes from/to children,
    connections to sockets it is listening)
    """

    # taken from python 2.4 os module. copied here for portability
    # to older python versions
    EX_OK = 0
    EX_USAGE = 64
    EX_OSERR = 71
    EX_CONFIG = 78


    def init(self, opts):
        self.opts = opts
        # set of files that should be cleaned up on exit
        self.remove_on_exit = []
        self.quit = 0
        # addresses to accept connections from command interpreters
        self.listen_addrs = []
        # calc some identification information
        # and build globally unique process id
        self.hostname = self.gethostname()
        self.user_name = self.get_user_name()
        self.boot_time = self.get_boot_time()
        self.pid = os.getpid()
        self.gupid = self.get_gupid()
        self.short_gupid = self.get_short_gupid()
        # log filename
        ioman.set_log_filename("log-%s" % self.gupid)

        # hogehoge ----------------------
        self.my_addrs = ifconfig.get_my_addrs("") # ugly
        self.upgrade_listen_channel = None
        # hogehoge ----------------------

        # target label
        self.target_label = None
        # parent peer (set by setup_parent_peer)
        self.parent = None
        # live tasks
        self.tasks = {}
        # environment (gxpd_environment)
        self.gxpd_env = None
        # prof object
        self.profiler = gxpd_profiler()
        return 0

    def gethostname(self):
        # gxpd dies with error: AF_UNIX path too long
        # an incomplete workaround here.
        # truncate FQDN into shorter one
        h = socket.gethostname()
        m = re.match("(\.*[^\.]*)", h)
        assert m
        return m.group(1)

    def get_boot_time(self):
        return time.strftime("%Y-%m-%d-%H-%M-%S")

    def get_user_name(self):
        return os.environ.get("USER", "unknown")

    def get_gupid(self):
        """
        gupid = globally unique process id
        """
        h = self.hostname
        u = self.user_name
        t = self.boot_time
        pid = self.pid
        return "%s-%s-%s-%s" % (h, u, t, pid)

    def get_short_gupid(self):
        """
        short gupid used to name daemon socket file,
        because unix domain socket does not allow too long
        pathnames. It does not contain host/user information.
        user information is (most of the time) included 
        in the upper directory name (/tmp/gxp-USER-xxxxx/...).
        exclusion of hostname is a potential source of conflict,
        but since /tmp is not normally shared, it should be OK...
        """
        t = self.boot_time
        pid = self.pid
        return "%s-%s" % (t, pid)

    def get_gxp_tmp(self):
        suffix = os.environ.get("GXP_TMP_SUFFIX", "default")
        return os.path.join("/tmp", ("gxp-%s-%s" % (self.user_name, suffix)))

    def ensure_dir(self, directory):
        try:
            # os.makedirs(directory, 0777)
            os.makedirs(directory, 0700)
        except OSError,e:
            if e.args[0] == errno.EEXIST:
                pass
            else:
                raise
        os.chmod(directory, 0700)

    def parse_listen_arg(self, listen_arg, name_prefix): # created_explicitly
        """
        Syntax of listen_arg
             unix:path | inet:addr:port
        path, addr, port may be omitted

        Return (AF_TYPE,addr) where addr is whatever is
        passed to bind system call.
        (for inet sockets, it is (ip_addr,port). for unix domain
        sockets, it is a path name).
        e.g.,

        'unix:hoge' --> (AF_UNIX, 'hoge')
        'inet:123.456.78.9:50' --> (AF_INET, ('123.456.78.9', 50))

        reasonably handle default cases:

        'inet::' --> (AF_INET, ('', 0))

        
        """
        if listen_arg == "":
            return None,""              # no error, no listen
        fields = string.split(listen_arg, ":", 1)
        if len(fields) == 2:
            [ af_str, rest ] = fields
        elif len(fields) == 1:
            [ af_str ] = fields
            rest = ""
        else:
            bomb()
        af_str = string.upper(af_str)
        if af_str == "TCP" or af_str == "INET":
            # find the last ':' and split there
            i = string.rfind(rest, ":")
            if i == -1:
                addr = rest
                port = 0
            else:
                addr = rest[:i]
                port = int(rest[i+1:])  # xxxx
            return socket.AF_INET,(addr,port)
        elif af_str == "UNIX":
            path = rest
            if path == "":
                gxp_tmp = self.get_gxp_tmp()
                self.ensure_dir(gxp_tmp)
                if 0:
                    if created_explicitly:
                        prefix = "Gxpd"
                    else:
                        prefix = "gxpd"
                path = os.path.join(self.get_gxp_tmp(), 
                                    ("%s-daemon-%s" % (name_prefix, 
                                                       self.short_gupid)))
            return socket.AF_UNIX,path
        elif af_str == "NONE":
            return 0,None
        else:
            return None,None            # error
        

                                               # created_explicitly, 
    def setup_channel_listen(self, listen_arg, name_prefix, qlen):
        """
        Open a socket to listen on. af is an address family
        (normally socket.AF_INET), addr is an address to bind to
        (we accept anonymous name ("",0), and it is the typical),
        and qlen is the queue length (backlog).

        return 0 on success, -1 on error
        
        """
        # created_explicitly
        af,addr = self.parse_listen_arg(listen_arg, name_prefix)
        if addr is None:
            if af == 0:
                return 0
            else:
                assert af is None, af
                Es("%s : invalid listen addr '%s'\n" \
                       % (self.gupid, listen_arg))
                return -1
        s = ioman.mk_non_interruptible_socket(af, socket.SOCK_STREAM)
        s.bind(addr)
        if af == socket.AF_UNIX:
            os.chmod(addr, 0600)
            self.remove_on_exit.append(addr)
        s.listen(qlen)
        # blocking = 1
        ch = ioman.achannel(ioman.primitive_channel_socket(s, 1))
        self.add_rchannel(ch)
        self.listen_addrs.append(addr)
        if dbg>=2:
            ioman.LOG("listening on %s\n" % addr)
        return 0

    def redirect_stdout_stderr(self):
        # xp = open("/tmp/xxx", "wb")
        # os.dup2(xp.fileno(), 2)
        # if xp.fileno() > 2: xp.close()

        opts = self.opts
        if 0:
            if opts.created_explicitly:
                oprefix = "Gxpout"
                eprefix = "Gxperr"
            else:
                oprefix = "gxpout"
                eprefix = "gxperr"
        opath = os.path.join(self.get_gxp_tmp(),
                             ("%s-stdout-%s" % (opts.name_prefix, self.gupid)))
        epath = os.path.join(self.get_gxp_tmp(),
                             ("%s-stderr-%s" % (opts.name_prefix, self.gupid)))
        if dbg>=2:
            ioman.LOG("redirecting stdout to %s\n" % opath)
            ioman.LOG("redirecting stderr to %s\n" % epath)
        op = open(opath, "wb")
        os.chmod(opath, 0600)
        os.dup2(op.fileno(), 1)
        if op.fileno() > 1: op.close()
        ep = open(epath, "wb")
        os.chmod(epath, 0600)
        os.dup2(ep.fileno(), 2)
        if ep.fileno() > 2: ep.close()

    def setup_parent_peer(self, opts):
        """
        Set up a data structure to talk to my parent. It is
        not necessarily the process that really `forked' me.
        It is rather a `conceptual' parent process which may
        be a remote process that did something like
        'ssh <this_node> <this_program>.

        In any case, I am presumably connected to the conceptual
        parent via some file descriptors. For now, we assume file
        descriptors 0,1,2 are connected to the parent.

        """
        # not useful anymore
        # self.redirect_stdout_stderr(opts)
        # opts.no_stdin
        if dbg>=2:
            ioman.LOG(("setup parent peer (name = %s)\n"
                       % opts.parent))
        parent = parent_peer(opts.parent, 0)
        for ch in parent.r_channels_rev.keys():
            self.add_rchannel(ch)
        for ch in parent.w_channels_rev.keys():
            self.add_wchannel(ch)
        self.parent = parent
        return 0

    def upgrade_parent_peer(self, ch, ev):
        if ev.kind == ioman.ch_event.OK:
            if dbg>=2:
                ioman.LOG("got connection to upgrade socket\n")
            new_parent = connect_peer(self.parent.peer_name,
                                      ev.new_so)
            new_parent.set_critical()
            if fix_2007_12_25: self.parent.clear_critical()
            # self.parent.discard()
            # ioman.LOG("yattayo\n")
            self.parent = new_parent
            for ch in new_parent.r_channels_rev.keys():
                self.add_rchannel(ch)
            for ch in new_parent.w_channels_rev.keys():
                self.add_wchannel(ch)
            self.parent.write_msg("Connection upgrade OK\n")
            if dbg>=2:
                ioman.LOG("sent con upgrade ack to parent\n")
        elif ev.kind == ioman.ch_event.TIMEOUT:
            if dbg>=2:
                ioman.LOG("got timeout to upgrade socket\n")
            self.parent.write_msg("Connection upgrade TIMEOUT\n")
            if dbg>=2:
                ioman.LOG("sent failure notification to parent\n")
        elif ev.kind == ioman.ch_event.IO_ERROR:
            if dbg>=1:
                ioman.LOG("IO error on listen channel\n") 
            self.quit = 1
        else:
            assert 0,ev.kind

    # ------------- tasks and processes -------------

    def check_task_status(self, task):
        """
        Check if the task node is already `useless.'
        It is useless if it has no running local processes,
        and if it has no child task nodes. That is, no processes
        are running under the subtree rooted at task.

        If so, send an event (event_fin) upward to signal
        task death (if the root node becomes garbage, the task
        is considered finished).
        """
        if task.is_dead():
            if dbg>=2:
                ioman.LOG("tid %s is dead\n" % task.tid)
            # if so, send fin event
            m = gxpm.syn(self.gupid,
                         task.tid, gxpm.event_fin(task.weight))
            task.forward_up(m, gxpm.unparse(m))
            # close the channel to the parent if told to do so
            # (normally so at the root).
            task.close_up()
            # delete the task in my table
            del self.tasks[task.tid]
            self.send_event_invalidate_view(task.tid)

    def send_event_invalidate_view(self, tid):
        """
        send notifications to all connecting gxpc procs
        """
        parents = {}
        for task in self.tasks.values():
            if task.tid == tid: continue
            for p in task.parent_peers.keys():
                if isinstance(p, connect_peer) and not parents.has_key(p):
                    if dbg>=1:
                        ioman.LOG(("send notification to parent peer %s\n"
                                   % p.peer_name))
                    ev = gxpm.event_invalidate_view()
                    m = gxpm.up(self.gupid, tid, ev)
                    msg = gxpm.unparse(m)
                    x = p.write_msg(msg)
                    if x == -1:
                        # this parent has gone
                        del task.parent_peers[p]
                        if dbg>=1:
                            ioman.LOG("notification to parent lost\n")
                    else:
                        parents[p] = None
                        if dbg>=2:
                            ioman.LOG("notification to parent OK\n")
             
    def cleanup_process(self, p):
        # task p belongs to
        task = p.task
        pid = p.pid
        rid = p.rid
        if not task.processes.has_key(p.pid):
            if dbg>=2:
                ioman.LOG("pid = %s rid = %s already garbage\n" \
                          % (p.pid, p.rid))
            return
        if dbg>=2:
            ioman.LOG("pid = %s rid = %s garbage\n" \
                      % (p.pid, p.rid))
        # delete p from its group
        del task.processes[p.pid]
        # 2010 5/5 fixed memory leak bugs
        del task.proc_by_rid[rid]
        # let the client know the process is dead
        m = gxpm.up(self.gupid, task.tid,
                    gxpm.event_die("proc", rid,
                                   pid, p.term_status, p.rusage,
                                   p.time_start, p.time_end))
        task.forward_up(m, gxpm.unparse(m))
        # 2007 12/2 tau
        # fixed a descriptor-leak bug that does not close
        # stdin of the process
        p.discard()
        # delete the task it belongs to, if necessary
        self.check_task_status(task)
        # NEW handle cases where a child gxp died, and
        # some tasks use it
        if isinstance(p, gxp_peer_base):
            for t in self.tasks.values():
                if t.child_peers.has_key(p):
                    if fix_2007_12_25:
                        err_msg = ("%s : gxp process %s is dead\n" %
                                   (self.gupid, p.peer_name))
                        m = gxpm.up(self.gupid, task.tid,
                                    gxpm.event_info(None, err_msg))
                        t.forward_up(m, gxpm.unparse(m))
                        if dbg>=1: ioman.LOG(err_msg)
                    del t.child_peers[p]
                    self.check_task_status(t)

    def check_proc_status(self, p):
        """
        Check if the process is garbage. It is garbage if
        the gxp has recognized its termination and all the
        pipes from it to the gxp has been closed.

        If so, first send an event upwards notifying of processs
        termination. Delete the process from the task it belongs to.
        It may in turn mark the task as dead, which may trigger
        propagation of fin events upward.
        """
        # delete p if p becomes garbage
        if p.is_garbage():
            self.cleanup_process(p)
        else:
            if dbg>=2:
                ioman.LOG("pid = %s rid = %s still alive\n" \
                          % (p.pid, p.rid))
            
            

    # ------------- handling syn msgs -------------

    def handle_syn(self, ch, m):
        """
        syn msg is used to detect the termination of a task.
        To detect global terminatin of a task, we assign weights
        on task nodes. A task node that has received n msgs
        from above has weight n + 1. A parent node also
        remembers the number of times it has sent msgs to
        each child, plus one. 
        
        When a task node is deleted, it generates a syn msg
        that carries its weight. When it gets a syn msg signaling
        the deletion of a child node, it subtracts the weight
        carried on the syn msg from what it remembers as the
        weight of the child. If the weight becomes zero, the
        parent deletes the task from its table.

        This mechanism is necessary to avoid race conditions
        between downward msgs creating child processes under
        a task and upward msgs generated when a process is gone.
        """
        task = self.tasks[m.tid]
        task.child_peers[ch.proc] = task.child_peers[ch.proc] - m.event.weight
        assert task.child_peers[ch.proc] >= 0, \
               (task.child_peers[ch.proc], m.event.weight)
        if task.child_peers[ch.proc] == 0:
            del task.child_peers[ch.proc]
        self.check_task_status(task)
        
    # ------------- handling down msgs -------------

    def search_peer(self, peer_name):
        """
        return a neighboring gxp process matching peer_name.
        if not found return None.
        """
        for p in self.processes.values():
            if isinstance(p, gxp_peer_base) and \
                   p.state == gxp_peer_base.STATE_LIVE and \
                   re.match(peer_name, p.peer_name):
                return p
        return None

    def all_peers_except(self, procs):
        """
        return a list of all neighboring gxp processes except
        for those in procs.
        """
        P = []
        for p in self.processes.values():
            if isinstance(p, gxp_peer_base) and \
                   p.state == gxp_peer_base.STATE_LIVE and \
                   not procs.has_key(p):
                P.append(p)
        return P

    def register_task(self, tid, parent, target, persist, keep_connection):
        """
        Register a task named tid, with the specified parent.
        If a task of the given name (tid) does not exist, create one
        (common case). Otherwise it simply adds parent as
        (another) parent of it.

        target is the tree of target (gxpm.target_tree).
        persist is 1 if this task should never die.
        keep_connection is
        gxpm.keep_connection_never if the connection to the parent
        should be immediately closed.
        gxpm.keep_connection_until_fin if the connection to the
        parent should closed when all procs are finished.
        gxpm.keep_connection_never if the connection to the parent
        should never be closed.
        """
        if self.tasks.has_key(tid):
            if dbg>=2:
                ioman.LOG("tid %s exists\n" % tid)
            task = self.tasks[tid]
            task.weight = task.weight + 1 # ???
            # if this is an additional process to an existing task,
            # notify other gxps of them
        else:
            if dbg>=2:
                ioman.LOG("tid %s created\n" % tid)
            task = task_tree_node(tid)
            self.tasks[tid] = task
        # update persistent flag of the task
        if persist: task.persist = persist
        # add the sender as a new parent
        if task.parent_peers.has_key(parent):
            task.parent_peers[parent] = max(task.parent_peers[parent],
                                            keep_connection)
        else:
            task.parent_peers[parent] = keep_connection
        if task.parent_peers[parent] == gxpm.keep_connection_never:
            task.close_connection_to_parent(parent)
        return task

    def forward_down(self, task, parent, m, msg):
        """
        forward msg m down, along the tree specified with
        target tree in m.

        the m should look like:
           <down>
             <target> ... </target>
             <tid> ... </tid>
               ...
           </down>
        """
        # get destination
        # <target><tree ...>...</tree></target>
        root = m.target
        # <tree ...>...</tree>
        # root = target.children[0]
        if root.children is None:
            # m says this should be forwarded to all children.
            # m_ = m.copy()
            # delete "close" attribute, even originally present,
            # to prevent the child from disconnecting the connection
            # to this gxp upon termination.
            # m_.del_subtree("close")
            # msg_ = m_.to_str()
            if m.keep_connection < gxpm.keep_connection_forever:
                # non-roots should always keep connection to the parent
                m_keep_connection = m.keep_connection
                m.keep_connection = gxpm.keep_connection_forever
                msg_ = gxpm.unparse(m)
                m.keep_connection = m_keep_connection
            else:
                msg_ = msg
            # forward the msg down (to everybody except for
            # the parent)
            peers = self.all_peers_except({ parent : 1 })
            for peer in peers:
                # increment its weight by one, reflecting the
                # fact it has sent a msg to it.
                # FIX??: if this msg has been lost due to the crash
                # of the gxpd hosting the child node, then we can
                # never delete of the child?
                # Or it has been fixed??? (see check_proc_status)
                if not task.child_peers.has_key(peer):
                    task.child_peers[peer] = 0
                task.child_peers[peer] = task.child_peers[peer] + 1
                peer.write_msg(msg_)
        else:
            # m says this should be forwarded to some specified
            # children
            for t in root.children:
                child_name = t.name
                # look for the peer of the specified name
                peer = self.search_peer(child_name)
                if peer is None:
                    record = ("no children of name %s. perhaps it's dead\n" 
                              % child_name)
                    if dbg>=1: ioman.LOG(record)
                    err_msg = "%s : %s" % (self.gupid, record)
                    em = gxpm.up(self.gupid, task.tid,
                                 gxpm.event_info(None, err_msg))
                    task.forward_up(em, gxpm.unparse(em))
                elif peer is parent:
                    # never send a msg to the parent (why not?)
                    continue
                else:
                    # m_ = m.copy()
                    # delete "close" attribute to prevent the child
                    # from disconecting the channel to me
                    # m_.del_subtree("close")
                    # set the target of m to the subtree rooted at
                    # the destination peer. i.e. if the original
                    # taget is a -- b -- c
                    #             +-d -- e
                    # and we are about to forward m to b, the target
                    # of the msg is now b -- c
                    # m_.set_subtree("target", new_tgt)
                    # m_close,m_target = m.close,m.target
                    m_keep_connection,m_target = m.keep_connection,m.target
                    m.keep_connection,m.target = gxpm.keep_connection_forever,t
                    msg_ = gxpm.unparse(m)
                    m.keep_connection,m.target = m_keep_connection,m_target
                    if not task.child_peers.has_key(peer):
                        task.child_peers[peer] = 0
                    # increment weight by one (see above)
                    task.child_peers[peer] = task.child_peers[peer] + 1
                    peer.write_msg(msg_)

    # ------------- handlers for individual actions -------------


    #
    # action quit
    #
    def do_action_quit(self, task, parent, down_m, tgt, action):
        """<action name=quit></action>"""
        self.quit = 1

    def mk_pong_info(self, level):
        # children + children in progress
        target_label = self.target_label
        gupid = self.gupid = self.get_gupid()
        parent = self.parent.peer_name
        C = []
        I = []
        for p in self.processes.values():
            if isinstance(p, child_peer):
                if p.state == gxp_peer_base.STATE_LIVE:
                    C.append(p.peer_name)
                else:
                    assert p.state == gxp_peer_base.STATE_IN_PROGRESS
                    I.append(p.peer_name)
        pong = gxpm.event_info_pong(0, None, target_label,
                                    gupid, self.hostname, parent, C, I)
        if level == 0:
            pong.msg = ("%s\n" % self.target_label)
        else:
            pong.pid = ("%s" % self.pid)
            pong.user_name = self.user_name
            pong.boot_time = self.boot_time
            addrs = map(lambda x: ("  %s\n" % (x,)),
                        self.listen_addrs)
            pong.listen_addrs = string.join(addrs, "")
            # pong.tasks = string.join(self.tasks.keys(), " ")
            pong.tasks = map(lambda x: x.show(), self.tasks.values())
            pong.cwd = os.getcwd()
            env = map(lambda (var,val): ("  %s=%s\n" % (var, val)),
                      self.gxpd_env.dict.items())
            pong.env = string.join(env, "")
            channels = map(lambda c: c.fileno(),
                           self.get_all_channels())
            channels.sort()
            channels = map(lambda c: ("%s" % c), channels)
            pong.channels = string.join(channels, " ")
            procs = map(lambda (pid,p): \
                        ("  %s : %s\n" % (pid, p.cmd)),
                        self.processes.items())
            pong.procs = string.join(procs, "")

            child_peers = map(lambda x: ("  %s\n" % x), C)
            child_peers = string.join(child_peers, "")

            in_prog = map(lambda x: ("  %s\n" % x), I)
            child_peers_in_progress = string.join(in_prog, "")
            
            pong.msg = string.join([
                ("target_label: %s\n" % target_label),
                (" gupid: %s\n" % gupid),
                (" parent: %s\n" % parent),
                (" child_peers:\n%s" % child_peers),
                (" child_peers_in_prog:\n%s" \
                 % child_peers_in_progress),
                (" hostname: %s\n" % pong.hostname),
                (" pid: %s\n" % pong.pid),
                (" user_name: %s\n" % pong.user_name),
                (" boot_time: %s\n" % pong.boot_time),
                (" listen_addrs:\n%s" % pong.listen_addrs),
                (" tasks: %s\n" % pong.tasks),
                (" cwd: %s\n" % pong.cwd),
                (" env:\n%s" % pong.env),
                (" channels: %s\n" % pong.channels),
                (" procs:\n%s" % pong.procs) ], "")
        return pong
    
    def do_action_ping(self, task, parent, down_m, tgt, action):
        """
        Perform action in response to ping msg
        <action name=ping level=xxx></action>
        """
        if dbg>=2:
            ioman.LOG("received a ping\n")
        m = gxpm.up(self.gupid, task.tid,
                    self.mk_pong_info(action.level))
        task.forward_up(m, gxpm.unparse(m))

    def do_action_chdir(self, task, parent, down_m, tgt, action):
        """
        <action name=chdir><to>/tmp/hogehoge</to></action>
        """
        dir = action.to
        ex_dir = dir
        ok = 0
        # TODO: expand `this`
        try:
            ex_dir = os.path.expanduser(os.path.expandvars(dir))
            os.chdir(ex_dir)
            ok = 1
        except OSError,e:
            pass
        if ok:
            i = gxpm.event_info(0, "")
        else:
            msg = ("%s : cannot cd to %s %s\n" \
                   % (self.gupid, ex_dir, e.args))
            i = gxpm.event_info(1, msg)
        m = gxpm.up(self.gupid, task.tid, i)
        task.forward_up(m, gxpm.unparse(m))

    def do_action_export(self, task, parent, down_m, tgt, action):
        """
        <action name=expor><var>x</var><val>y</val></action>
        """
        var = action.var
        val = action.val
        ex_var = var                    # for now
        ex_val = val                    # for now
        ok = 0
        try:
            ex_var = os.path.expanduser(os.path.expandvars(var))
            ex_val = os.path.expanduser(os.path.expandvars(val))
            os.environ[ex_var] = ex_val
            ok = 1
        except OSError,e:
            pass
        if ok:
            i = gxpm.event_info(0, "")
        else:
            msg = ("%s : failed to set %s=%s %s\n" \
                   % (self.gupid, ex_var, ex_val, e.args))
            i = gxpm.event_info(1, msg)
        m = gxpm.up(self.gupid, task.tid, i)
        task.forward_up(m, gxpm.unparse(m))

    def mk_pipe_desc(self, pipes):
        """
        pipes ultimately come from gxpc.
        it is a list. the format of each element is:

        (pipe_type, parent_usage, child_usage)
          pipe_type    : one of 'pipe', 'sockpair', 'pty'
          parent_usage : list of (r/w, name, atomicity)
            r means parent will read it.
            w means parent will write to it.
            name is like 0, 1, ..
            atomicity is relevant only for 'r'.
            it is either line, ...

        ('sockpair', [('r', fd, atomicity)], [('w', fd)])
        """
        desc = []
        for pipe_con,parent_use,child_use in pipes:
            if pipe_con == "pty":
                pipe_con_ = ioman.pipe_constructor_pty
            elif pipe_con == "pipe":
                pipe_con_ = ioman.pipe_constructor_pipe
            elif pipe_con == "sockpair":
                pipe_con_ = ioman.pipe_constructor_sockpair
            else:
                msg = "%s : bad pipe_con (%s) ignored\n" % \
                      (self.gupid, pipe_con)
                m = gxpm.up(self.gupid, task.tid,
                            gxpm.event_info(None, msg))
                task.forward_up(m, gxpm.unparse(m))
                continue
            parent_use_ = []
            for mode,name,atom in parent_use:
                if mode == "r":
                    ch_con = ioman.rchannel_process
                elif mode == "w":
                    ch_con = ioman.wchannel_process
                else:
                    msg = "%s : bad mode (%s) ignored\n" % \
                          (self.gupid, mode)
                    m = gxpm.up(self.gupid, task.tid,
                                gxpm.event_info(None, msg))
                    task.forward_up(m, gxpm.unparse(m))
                parent_use_.append((mode,name,ch_con))
            desc.append((pipe_con_,parent_use_,child_use))
        return desc

    def set_pipe_atomicity(self, pipes, action_name, p):
        for pipe_con,parent_use,child_use in pipes:
            for mode,name,atom in parent_use:
                if mode == "r":
                    if action_name == "createpeer" and name == 1:
                        p.r_channels[name].set_expected([("M",)])
                    elif atom == "none":
                        # set filter for stderr (get anything)
                        p.r_channels[name].set_expected([("*",)])
                    elif atom == "eof":
                        p.r_channels[name].set_expected([])
                    else:                   # default is line
                        p.r_channels[name].set_expected(["\n"])

    def allocate_rid(self, task):
        # for now, limit the number of procs per task to 1M
        i = 0
        while i < 1000000:
            if not task.proc_by_rid.has_key(i):
                return i
            i = i + 1
        assert 0

    #
    # action create_proc or create_peer and helper
    #
    def action_create_proc_or_peer(self, task, tgt, action, action_name):
        """
        Create a regular child process ('e') or a process spawning
        a new gxpd on another node ('explore').

        task: the task the new process should belong to
        action: specifies what should be created
        actiion_name: either createproc or createpeer depending on
        it is a regular process or a new gxpd.
        <action name=...><cmd>...</cmd></action>
        """
        # not create proc with existing tid
        if action.rid is None:
            action.rid = self.allocate_rid(task)
        if action.rid in task.proc_by_rid:
            msg = ("%s : requested process (rid=%s) for task %s not "
                   "created because it already exists\n"
                   % (self.gupid, action.rid, task.tid))
            m = gxpm.up(self.gupid, task.tid, gxpm.event_info(1, msg))
            task.forward_up(m, gxpm.unparse(m))
            return
        if action_name == "createproc":
            # get everything from non-gxp processes
            process_class = child_task_process
        elif action_name == "createpeer":
            # get a properly formatted msg from gxp processes
            process_class = child_peer
        else:
            bomb()
        # say stdin/stdout/stderr should be connected to me
        # say also other stuff specified in the action should
        # be connected to me
        pipe_desc = self.mk_pipe_desc(action.pipes)
        # really fork a new process
        shcmd = [ "/bin/sh", "-c", action.cmd ]
        env = { "GXP_EXEC_IDX"  : "%s" % tgt.exec_idx,
                "GXP_NUM_EXECS" : "%s" % tgt.num_execs }
        if tgt.eenv is None:
            cwd = None
            env = None
        else:
            cwd = tgt.eenv.cwd
            env.update(tgt.eenv.env)
        # given by --dir options of e
        cwds = []
        if len(action.cwds) == 0:
            # no --dir option given. use session dir if given
            if cwd is not None:
                cwds.append(cwd)
        else:
            # --dir options are given
            for act_cwd in action.cwds:
                # expand it first
                act_cwd = os.path.expandvars(os.path.expanduser(act_cwd))
            # then join with it with cwd in session 
                if cwd is not None:
                    act_cwd = os.path.realpath(os.path.join(cwd, act_cwd))
                cwds.append(act_cwd)
        # given by --export option of e
        if action.env is not None:
            if env is None:
                env = action.env
            else:
                env.update(action.env)
        p,msg = self.spawn_generic(process_class, shcmd, pipe_desc, 
                                   env, cwds, action.rlimits)
        if p is None:
            m = gxpm.up(self.gupid, task.tid, gxpm.event_info(1, msg))
            task.forward_up(m, gxpm.unparse(m))
            return
        # set relative id <-> process
        p.rid = action.rid
        task.processes[p.pid] = p
        task.proc_by_rid[action.rid] = p
        p.task = task
        self.set_pipe_atomicity(action.pipes, action_name, p)
        if dbg>=1:
            ioman.LOG("process for task %s " \
                      "rid %s pid %s (%s) created\n" \
                      % (task.tid, action.rid, p.pid, action.cmd))

    def do_action_createproc(self, task, parent, down_m, tgt, action):
        """
        <action name=createproc>
         <rid>...</rid>
         <cmd>hostname</cmd>
         <pipes>
          <pipe><fd>3</fd><mode>r</mode></pipe>
         </pipes>
        </action>
        """
        self.action_create_proc_or_peer(task, tgt, action, "createproc")

    def do_action_createpeer(self, task, parent, down_m, tgt, action):
        """
        <action name=createpeer><cmd>...</cmd></action>
        """
        self.action_create_proc_or_peer(task, tgt, action, "createpeer")

    #
    # action feed
    #
    def do_action_feed(self, task, parent, down_m, tgt, action):
        """
        Feed a process with some string.

        task : the task the target process belongs to.
        action : specified what should be fed to which descriptor
        of which process.
        
        <action name=feed>
         <rid>1234</rid>     # relative id of the target process
         <fd>0</fd>          # target descriptor (0 is stdin)
         <payload>...</payload> # string that should be fed
        </action>
        """
        rid = action.rid
        fd = action.fd
        # non-existent rid or fd
        if rid is None:
            procs = task.proc_by_rid.values()
        elif task.proc_by_rid.has_key(rid):
            procs = [ task.proc_by_rid[rid] ]
        else:
            if dbg>=1:
                ioman.LOG("do_action_feed : "
                          "rid %s not in task %s\n" \
                          % (rid, task.tid))
            return
        if dbg>=2:
            ioman.LOG("do_action_feed : rid=%s fd=%s %d bytes %d procs\n"
                      % (rid, fd, len(action.payload), len(procs)))
        for proc in procs:
            if proc.w_channels.has_key(fd):
                # really send the stuff (or EOF)
                ch = proc.w_channels[fd]
                if action.payload == "":
                    if dbg>=2:
                        ioman.LOG("do_action_feed : ch.write_eof()\n")
                    ch.write_eof()
                else:
                    if dbg>=2:
                        ioman.LOG("do_action_feed : ch.write_stream(%d bytes)\n"
                                  % len(action.payload))
                    ch.write_stream(action.payload)
            else:
                if dbg>=1:
                    ioman.LOG("do_action_feed : fd %s not in "
                              "task %s rid %s's write channels\n" \
                              % (fd, task.tid, rid))

    #
    # action close
    #
    def do_action_close(self, task, parent, down_m, tgt, action):
        """
        close process output channel

        task : the task the target process belongs to.
        action : specified what should be fed to which descriptor
        of which process.
        
        <action name=close>
         <rid>1234</rid>     # relative id of the target process
         <fd>0</fd>          # target descriptor (0 is stdin)
        </action>
        """
        rid = action.rid
        fd = action.fd
        # non-existent rid or fd
        if rid is None:
            procs = task.proc_by_rid.values()
        elif task.proc_by_rid.has_key(rid):
            procs = [ task.proc_by_rid[rid] ]
        else:
            if dbg>=1:
                ioman.LOG("do_action_close : "
                          "rid %s not in task %s\n" \
                          % (rid, task.tid))
            return
        for proc in procs:
            if proc.r_channels.has_key(fd):
                # really send the stuff (or EOF)
                ch = proc.r_channels[fd]
                ch.discard()
            else:
                if dbg>=1:
                    ioman.LOG("do_action_feed : fd %s not in "
                              "task %s rid %s's write channels\n" \
                              % (fd, task.tid, rid))

    #
    # action sig and helpers
    #
    def get_signum(self, sig):
        """
        translate symbolic signal name (e.g., KILL) to the
        signal number (e.g., 9).
        if it is alreday an integer, use it as it is
        """
        if type(sig) is types.IntType: return sig
        sigsym = "SIG%s" % string.upper(sig)
        if hasattr(signal, sigsym):
            return getattr(signal, sigsym)
        else:
            try:
                return string.atoi(sig)
            except ValueError,e:
                return None

    def try_kill(self, pid, signum):
        """
        send signum to pid if the process exists
        """
        try:
            os.kill(pid, signum)
            return None
        except OSError,e:
            return "%s" % (e,)

    def try_killpg(self, pid, signum):
        """
        send signum to pid if the process exists
        """
        try:
            os.killpg(pid, signum)
            return None
        except OSError,e:
            return "%s" % (e,)

    def do_action_sig(self, task, parent, down_m, tgt, action):
        """
        Send signal to a process.
        
        <action name=sig>
         <rid>1</rid>      # target process
         <sig>15</sig>     # signal to send (symbolic or numeric)
        </action>

        
        """
        rid = action.rid
        sig = action.sig
        if rid is None:
            procs = task.proc_by_rid.values()
        elif task.proc_by_rid.has_key(rid):
            procs = [ task.proc_by_rid[rid] ]
        else:
            if dbg>=1:
                ioman.LOG("do_action_sig : "
                          "rid %r not in task %s\n" \
                          % (rid, task.tid))
            return
        if dbg>=1:
            ioman.LOG("do_action_sig : killing %s\n" % procs)
        for proc in procs:
            if 0 and not task.proc_by_rid.has_key(rid):
                if dbg>=1:
                    ioman.LOG("do_action_sig : "
                              "rid %s not in task %s\n" \
                              % (rid, task.tid))
                continue
            pid = proc.pid
            signum = self.get_signum(sig)
            if signum is None:
                msg = "%s : bad signal %s\n" % (self.gupid, sig)
                i = gxpm.event_info(1, msg)
            else:
                if dbg>=2:
                    ioman.LOG("KILL %d\n" % pid)
            
                # tau : 
                # From 2007 10/25, we send signal to the
                # process group (-pid). for processes created by e
                # commands (and alike), the child process is
                # /bin/sh, not the processes the user specified in
                # the command line. Now signals are delivered to all
                # subprocesses.
                e = self.try_killpg(pid, signum)
                if e is None:
                    if dbg>=2:
                        ioman.LOG("OK %d\n" % pid)
                    i = gxpm.event_info(0, "")
                else:
                    if dbg>=2:
                        ioman.LOG("NG %d %s\n" % (pid, e))
                    msg = ("%s : could not kill pid=%s tid=%s rid=%s %s\n"
                           % (self.gupid, pid, task.tid, rid, e))
                    i = gxpm.event_info(1, msg)
                m = gxpm.up(self.gupid, task.tid, i)
                task.forward_up(m, gxpm.unparse(m))

    def do_action_prof_start(self, task, parent, down_m, tgt, action):
        if self.profiler.mark_start(action.file) == 0:
            msg = ("%s : profiling started (%s)\n" \
                   % (self.gupid, action.file))
            i = gxpm.event_info(0, msg)
        else:
            msg = "%s : profile already started\n" % self.gupid
            i = gxpm.event_info(1, msg)
        m = gxpm.up(self.gupid, task.tid, i)
        task.forward_up(m, gxpm.unparse(m))

    def do_action_prof_stop(self, task, parent, down_m, tgt, action):
        if self.profiler.mark_stop() == 0:
            msg = "%s : profile result saved on %s\n" \
                  % (self.gupid, self.profiler.prof_file)
            i = gxpm.event_info(0, msg)
        else:
            msg = "%s : profile not started\n" % self.gupid
            i = gxpm.event_info(1, msg)
        m = gxpm.up(self.gupid, task.tid, i)
        task.forward_up(m, gxpm.unparse(m))

    def do_action_set_log_level(self, task, parent, down_m, tgt, action):
        global dbg
        dbg = action.level
        ioman.dbg = action.level
        i = gxpm.event_info(0, "")
        m = gxpm.up(self.gupid, task.tid, i)
        task.forward_up(m, gxpm.unparse(m))

    def do_action_set_log_base_time(self, task, parent, down_m, tgt, action):
        if dbg>=2:
            ioman.LOG("reset log base time\n")
        ioman.set_log_base_time()
        i = gxpm.event_info(0, "")
        m = gxpm.up(self.gupid, task.tid, i)
        task.forward_up(m, gxpm.unparse(m))

    def do_action_set_max_buf_len(self, task, parent, down_m, tgt, action):
        self.max_buf_len = action.max_buf_len
        i = gxpm.event_info(0, "")
        m = gxpm.up(self.gupid, task.tid, i)
        task.forward_up(m, gxpm.unparse(m))

    def do_action_trim(self, task, parent, down_m, tgt, action):
        """
        """
        root = down_m.target
        # make a dict of all child peers
        child_to_discard = {}
        for p in self.processes.values():
            if isinstance(p, gxp_peer_base) and p is not parent:
                child_to_discard[p.peer_name] = p
        for t in root.children:
            child_name = t.name
            if child_to_discard.has_key(child_name):
                del child_to_discard[child_name]
        if len(child_to_discard) > 0:
            trimmed = string.join(child_to_discard.keys(), " ")
            msg = ("%s : child %s will be trimmed\n" % \
                   (self.gupid, trimmed))
        else:
            msg = ""
        m = gxpm.up(self.gupid, task.tid, gxpm.event_info(0, msg))
        task.forward_up(m, gxpm.unparse(m))
        for name,peer in child_to_discard.items():
            peer.discard()

    #
    # action reclaim???
    #
    def do_action_reclaim_task(self, task, parent, down_m, tgt, action):
        """<action name=reclaim>
            <targettid>tid</targettid>
           </action>

        reclaim specified task id (targettid) unconditionally
        """
        for target_tid in action.target_tids:
            if self.tasks.has_key(target_tid):
                target_task = self.tasks[target_tid]
                del self.tasks[target_tid]
                msg = ("%s : task %s : %d procs %d child_peers\n" 
                       % (self.gupid, target_tid,
                          len(target_task.processes),
                          len(target_task.child_peers)))
                m = gxpm.up(self.gupid, task.tid, gxpm.event_info(None, msg))
                task.forward_up(m, gxpm.unparse(m))

    #
    # action synchronize
    #
    def xxx_do_action_synchronize(self, task, parent, down_m, tgt, action):
        """
        send synchronize message to all known parents
        """
        event = gxpm.event_synchronize(action.peer_tree, action.exec_tree)
        m = gxpm.up(self.gupid, task.tid, event)
        parent_peers = task.parent_peers.keys()
        for tid, _task in self.tasks.items():
            _parent_peers = _task.parent_peers.keys()
            if tid[:7] != 'explore': # not explore
                if len(_parent_peers) > 1 \
                    or (len(_parent_peers) == 1 \
                    and _parent_peers[0] != parent_peers[0]):
                    _task.forward_up(m, gxpm.unparse(m))

    #
    # dispatch action (search do_action_xxx method and call it)
    #
    def do_action(self, task, parent, down_m, tgt, action):
        aname = action.__class__.__name__
        mname = "do_%s" % aname
        if hasattr(self, mname):
            method = getattr(self, mname)
            method(task, parent, down_m, tgt, action)
        else:
            msg = ("%s : no action named %s (%s)\n" % \
                   (self.gupid, aname, gxpm.unparse(action)))
            m = gxpm.up(self.gupid, task.tid,
                        gxpm.event_info(None, msg))
            task.forward_up(m, gxpm.unparse(m))

    def do_guarded_commands_xxx(self, task, parent, down_m):
        """
        execute guarded commands in m
        """
        tgt = down_m.target
        if tgt.eflag:
            for clauses in down_m.gcmds:
                for clause in clauses:
                    if re.match(clause.on, self.gupid):
                        for action in clause.actions:
                            self.do_action(task, parent, down_m, tgt, action)
                        break
        self.check_task_status(task)

    def do_guarded_commands(self, task, parent, down_m):
        """
        execute guarded commands in m
        """
        tgt = down_m.target
        if tgt.eflag:
            for clauses in down_m.gcmds:
                actions = clauses.get(self.gupid)
                if actions is None:
                    actions = clauses.get(None)
                if actions:
                    for action in actions:
                        self.do_action(task, parent, down_m, tgt, action)
        self.check_task_status(task)

    # ------------- handle low level events -------------

    def handle_DEATHS(self, ch, ev):
        """
        handle an event in which some child processes have
        terminated
        """
        if dbg>=2:
            ioman.LOG("%d child processes died\n" \
                      % len(ev.dead_processes))
        for p in ev.dead_processes:
            if dbg>=2 or \
               (dbg>=1 and isinstance(p, child_peer) and \
                p.state == gxp_peer_base.STATE_LIVE):
                ioman.LOG("gxp process %s is dead\n" % p.pid)
            if p is None:
                # p is None if dead process is created by non-regular
                # ways (like popen). specifically this happens when
                # we run ifconfig
                pass
            else:
                self.check_proc_status(p)

    def handle_OUTPUT_proc(self, ch, ev):
        """
        check the result of write to a regular process.
        nothing useful other than reporting unusual events
        """
        proc = ch.proc
        pid = proc.pid
        rid = proc.rid
        cmd = proc.cmd
        task = proc.task
        ch_name = proc.w_channels_rev[ch]
        tid = task.tid
        if ev.kind == ioman.ch_event.OK:
            if dbg>=2:
                ioman.LOG(("written %d bytes to %s... "
                           "fd=%s tid=%s pid=%s tag=%s buf_len=%d\n" \
                           % (ev.written, cmd[0:20], ch_name, tid, pid,
                              ev.tag, ch.buf_len)))
        else:
            # something unusual happened (IO_ERROR, etc.)
            if dbg>=1:
                ioman.LOG(("%s when writing to to [%s...] "
                           "fd=%s tid=%s pid=%s tag=%s buf_len=%d\n" \
                           % (ev.kind_str(), cmd[0:20], ch_name, tid, pid,
                              ev.tag, ch.buf_len)))

    def handle_OUTPUT_gxp(self, ch, ev):
        """
        check the result of write to a gxp process
        """
        gupid = self.gupid
        proc = ch.proc
        peer_name = proc.peer_name
        task = proc.task
        ch_name = proc.w_channels_rev[ch]
        if ev.kind == ioman.ch_event.OK:
            if dbg>=2:
                ioman.LOG(("written %d bytes to %s fd=%s tag=%s buf_len=%d\n"
                           % (ev.written, peer_name, ch_name,
                              ev.tag, ch.buf_len)))
        else:
            if dbg>=1:
                ioman.LOG(("%s when writing to %s fd=%s buf_len=%d\n"
                           % (ev.kind_str(), peer_name, ch_name, ch.buf_len)))
            # task is None when it is the guy connecting to me
            if task is not None:
                tid = task.tid
                pid = proc.pid
                rid = proc.rid
                m = gxpm.up(gupid, tid,
                            gxpm.event_io("peer", ev.kind, rid, pid, ch_name,
                                          "", ev.err_msg))
                task.forward_up(m, gxpm.unparse(m))
                if fix_2007_12_25:
                    if proc.upgrading_status == child_peer.upgrading_status_succeeded:
                        proc.discard()
                self.check_proc_status(proc)
            if proc.critical: self.quit = 1

    def handle_OUTPUT(self, ch, ev):
        proc = ch.proc
        if isinstance(proc, gxp_peer_base):
            self.handle_OUTPUT_gxp(ch, ev)
        else:
            self.handle_OUTPUT_proc(ch, ev)

    def handle_OUTPUTx(self, ch, ev):
        """
        Handle an envent which a write operation has now completed.
        For now, quit when any error occurred.
        """
        if ev.kind == ioman.ch_event.OK:
            if dbg>=2:
                ioman.LOG("written %d bytes tag %s, buf_len %d\n" \
                          % (ev.written, ev.tag, ch.buf_len))
        else:
            if dbg>=1:
                ioman.LOG("%s on write channel\n" % ev.kind_str())

    def handle_LISTEN(self, ch, ev):
        """
        Handle an event in which somebody connected.
        It should be a peer. The primary rason for this method
        to be called is the command frontend connects to daemon
        to issue a command.
        """
        if ch is self.upgrade_listen_channel:
            self.upgrade_parent_peer(ch, ev)
        elif ev.kind == ioman.ch_event.OK:
            if dbg>=2:
                ioman.LOG("accepted connection\n")
            # create a placeholder representing the process that
            # connected to this process
            peer = connect_peer("", ev.new_so)
            for ch in peer.r_channels_rev.keys():
                self.add_rchannel(ch)
            for ch in peer.w_channels_rev.keys():
                self.add_wchannel(ch)
        elif ev.kind == ioman.ch_event.IO_ERROR:
            if dbg>=1:
                ioman.LOG("IO error on listen channel\n")
            self.quit = 1
        else:
            assert 0,ev.kind

    def handle_msg(self, ch, msg):
        if dbg>=2:
            ioman.LOG("handle_msg %d bytes [%s ...]\n" \
                      % (len(msg), msg[0:30]))
        m = gxpm.parse(msg)
        if isinstance(m, gxpm.up):
            task = self.tasks.get(m.tid)
            if task:
                task.forward_up(m, msg)
            else:
                # should not happen,
                # but debug them later
                ioman.LOG("handle_msg %d bytes [%s ...] to non-existing task %s\n" \
                          % (len(msg), msg[0:30], m.tid))
        elif isinstance(m, gxpm.syn):
            self.handle_syn(ch, m)
        elif isinstance(m, gxpm.down):
            task = self.register_task(m.tid, ch.proc, m.target,
                                      m.persist, m.keep_connection)
            self.forward_down(task, ch.proc, m, msg)
            self.do_guarded_commands(task, ch.proc, m)
        else:
            bomb()
            
    # 1. connection
    # 2. parent gxp
    # 3. child gxp in progress (stdout/stderr)
    # 4. child gxp live (stdout/stderr)
    # 5. child proc (stdout/stderr)

    def handle_INPUT_gxp_live(self, ch, ev):
        """
        got something from live gxp peer.
        """
        gupid = self.gupid
        proc = ch.proc
        peer_name = proc.peer_name
        ch_name = proc.r_channels_rev[ch]
        if ev.kind == ioman.ch_event.OK and ch.is_msg_mode():
            # msg. (down commands, up events, syn events)
            self.handle_msg(ch, ev.data)
        else:
            # something wrong. most likely the child died
            if dbg>=2 or \
               (dbg>=1 and self.quit == 0 and \
                isinstance(proc, child_peer)):
                ioman.LOG(("%s from live gxp [%s] "
                           "fd=%s data=%d bytes [%s ...]\n" % \
                           (ev.kind_str(), peer_name,
                            ch_name, len(ev.data), ev.err_msg)))
            task = proc.task
            if task is not None:
                tid = task.tid
                pid = proc.pid
                rid = proc.rid
                m = gxpm.up(gupid, tid,
                            gxpm.event_io("peer", ev.kind, rid,
                                          pid, ch_name, ev.data, ev.err_msg))
                task.forward_up(m, gxpm.unparse(m))
                if fix_2007_12_25:
                    if proc.upgrading_status == child_peer.upgrading_status_succeeded:
                        proc.discard()
                self.check_proc_status(proc)
            # see mondai
            if proc.critical: 
                if dbg>=2:
                    ioman.LOG(("this live gxp is critical (parent.name = %s)\n" 
                               % self.parent.peer_name))
                if self.opts.continue_after_close: # parent.peer_name == ""
                    self.redirect_stdout_stderr()
                else:
                    self.quit = 1

    def parse_addr_spec(self, addr_spec):
        [ proto, host, addr, port ] = string.split(addr_spec, ":")
        assert proto == "tcp", proto
        af = socket.AF_INET
        try:
            a = socket.gethostbyname(host)
        except socket.gaierror,e:
            a = addr
        port = int(port)
        # socket.AF_INET,"123.234.78.89",10230
        return af,a,port
    
    def upgrade_connection_to_child(self, proc, addr_spec):
        """
        addr_spec is like   tcp:133.11.12.13:10000
        
        a child has brought up. we try to directly connect to
        the child via TCP
        """
        if dbg>=2:
            ioman.LOG("upgrade_connection_to_child\n")
        if 0:
            [ proto, host, addr, port ] = string.split(addr_spec, ":")
            assert proto == "tcp", (proto, addr, port)
            port = int(port)
        else:
            af,addr,port = self.parse_addr_spec(addr_spec)
        so = ioman.mk_non_interruptible_socket(af, socket.SOCK_STREAM)
        pch = ioman.primitive_channel_socket(so, 0) # blocking=0
        if dbg>=2:
            ioman.LOG("issueing connection to %s:%s:%d\n" % (af, addr, port))
        r,e = pch.connect((addr, port))
        if r == -1 and e.args[0] != errno.EINPROGRESS:
            # or e.args[0] == errno.EAGAIN?
            if dbg>=2:
                ioman.LOG("upgrade immediately failed "
                          "with %s %s %s\n" % (r, e.args[0], e.args[1]))
            pch.close()
            return -1
        else:
            ch_r = ioman.rchannel_process(pch, proc, None)
            ch_r.set_expected([("M",)])
            ch_w = ioman.wchannel_process(pch, proc, None)
            # make sure they are set (for dbg purpose)
            assert proc.upgrading_channel_w is None
            assert proc.upgrading_channel_r is None
            assert proc.upgrading_status == child_peer.upgrading_status_init
            proc.upgrading_channel_w = ch_w
            proc.upgrading_channel_r = ch_r
            proc.upgrading_status = child_peer.upgrading_status_in_progress
            self.add_rchannel(ch_r)
            self.add_wchannel(ch_w)
            if dbg>=2:
                ioman.LOG("upgrade in progress\n")
            # so far OK. the rest of the things are handled by
            # handle_INPUT_gxp_stdout_in_progress
            return 0
        
    # ------------- handling connection upgrade msgs -------------

    def handle_connection_upgrade_ack(self, ch, upgrade):
        """
        this proc has tried to connect to a child, and the child
        has acknowledged. upgrade = 1 on success. in this case,
        ch is the new channel (on the TCP connection).
        now we replace the channel to it
        otherwise, ch is the old connection (SSH connection).
        """
        proc = ch.proc
        assert proc.upgrading_status == child_peer.upgrading_status_in_progress
        if upgrade:                     # OK, do it
            # in this case, ch is the new connection
            if dbg>=2:
                ioman.LOG("received connection upgrade OK\n")
            proc.set_write_channel(proc.upgrading_channel_w)
            proc.upgrading_status = child_peer.upgrading_status_succeeded
        else:
            # in this case, ch is the old connection
            proc.upgrading_status = child_peer.upgrading_status_failed
            if dbg>=2:
                ioman.LOG("received connection upgrade NG\n")

    def handle_INPUT_gxp_stdout_in_progress(self, ch, ev):
        """
        got something from stdout of a child gxp peer.
        in any event we know if a createpeer suceeeded or not.
        """
        gupid = self.gupid
        proc = ch.proc
        pid = proc.pid
        rid = proc.rid
        ch_name = proc.r_channels_rev[ch]
        peer_name = proc.peer_name      # None
        target_label = proc.target_label
        hostname = proc.hostname
        task = proc.task
        tid = task.tid
        status = "NG"                   # default is NG

        if dbg>=2:
            # or (dbg>=1 and status != "OK"):
            ioman.LOG("%s from stdout of in-progress gxp [%s] fd=%s"
                      " data=%d bytes [%s...] err_msg=[%s]\n" % \
                      (ev.kind_str(), peer_name,
                       ch_name, len(ev.data), ev.data[0:30], ev.err_msg))


        if ev.kind == ioman.ch_event.OK:
            # did we get the right brought up msg?
            # Brought up on gupid ap seq
            if 0:
                ma = re.match("Brought up on (.+) (.+) (.+)\n", ev.data)
            else:
                ma = re.match("Brought up on (.+) (.+) (.+) (.+) (.+)\n", ev.data)
            if ma:
                # yes, now we try to upgrade the connection to the
                # child. so, the state is in progress
                peer_name = ma.group(1)
                ap = ma.group(2)
                target_label = ma.group(3)
                hostname = ma.group(4)
                proc.peer_name = peer_name
                proc.target_label = target_label
                proc.hostname = hostname
                if enable_connection_upgrade:
                    assert ap != "None", ap
                    status = "PROGRESS"
                    # try to connect to the child
                    self.upgrade_connection_to_child(proc, ap)
                else:
                    assert ap == "None", ap
                    status = "OK"
                    proc.state = gxp_peer_base.STATE_LIVE
            else:
                # we got the response to connection upgrade.
                # TIMEOUT or OK. either case, the child is now
                # in LIVE state.
                mb = re.match("Connection upgrade (.+)\n", ev.data)
                if mb:
                    status = "OK"
                    proc.state = gxp_peer_base.STATE_LIVE
                    if mb.group(1) == "OK":
                        self.handle_connection_upgrade_ack(ch, 1)
                    else:
                        self.handle_connection_upgrade_ack(ch, 0)
        else:
            # Fixed 2007.9.28.
            # we got IO_ERROR or EOF from stdout.
            # there are two cases.
            # (1) it is from the old (SSH) connection
            # (2) it is from the upgrading channel (socket)
            # in the latter case, we should continue until we get
            # "Connection upgrade (TIMEOUT)\n" in the above.
            if ch is proc.upgrading_channel_r:
                status = "PROGRESS"
                
        # forward whatever I got

        if status == "NG":
            m = gxpm.up(gupid, tid,
                        gxpm.event_io("peer", ev.kind, rid,
                                      pid, ch_name, ev.data, ev.err_msg))
            task.forward_up(m, gxpm.unparse(m))
        # let the parent know of the status of the peer
        if status != "PROGRESS":
            m = gxpm.up(gupid, tid,
                        gxpm.event_peerstatus(peer_name, target_label, hostname,
                                              status, gupid, rid))
            task.forward_up(m, gxpm.unparse(m))
            self.check_proc_status(proc)
            self.send_event_invalidate_view(task.tid)

    def handle_INPUT_gxp_stderr_in_progress(self, ch, ev):
        gupid = self.gupid
        proc = ch.proc
        pid = proc.pid
        rid = proc.rid
        ch_name = proc.r_channels_rev[ch]
        peer_name = proc.peer_name      # None
        task = proc.task
        tid = task.tid
        if dbg>=2:
            #  or (dbg>=1 and ev.kind != ioman.ch_event.OK)
            ioman.LOG("%s from stderr of in-progress gxp [%s] "
                      "fd=%s data=%d bytes [%s...] err=[%s]\n" % \
                      (ev.kind_str(), peer_name,
                       ch_name, len(ev.data), ev.data[0:30], ev.err_msg))
        m = gxpm.up(gupid, tid,
                    gxpm.event_io("peer", ev.kind, rid,
                                  pid, ch_name, ev.data, ev.err_msg))
        task.forward_up(m, gxpm.unparse(m))
        self.check_proc_status(proc)

    def handle_INPUT_gxp(self, ch, ev):
        if ch.proc.state == gxp_peer_base.STATE_LIVE:
            self.handle_INPUT_gxp_live(ch, ev)
        else:
            assert ch.proc.state == gxp_peer_base.STATE_IN_PROGRESS
            if ch.is_msg_mode():
                self.handle_INPUT_gxp_stdout_in_progress(ch, ev)
            else:
                self.handle_INPUT_gxp_stderr_in_progress(ch, ev)
        
    def handle_INPUT_proc(self, ch, ev):
        gupid = self.gupid
        proc = ch.proc
        pid = proc.pid
        rid = proc.rid
        cmd = proc.cmd
        task = proc.task
        ch_name = proc.r_channels_rev[ch]
        tid = task.tid
        if dbg>=2:
            ioman.LOG("%s from proc [%s] "
                      "fd=%s data=%d bytes [%s ...]\n" % \
                      (ev.kind_str(), cmd,
                       ch_name, len(ev.data), ev.data[0:30]))
        m = gxpm.up(gupid, tid,
                    gxpm.event_io("proc", ev.kind, rid,
                                  pid, ch_name, ev.data, ev.err_msg))
        task.forward_up(m, gxpm.unparse(m))
        if ev.kind != ioman.ch_event.OK:
            self.check_proc_status(proc)

    def handle_INPUT(self, ch, ev):
        proc = ch.proc
        if isinstance(proc, gxp_peer_base):
            self.handle_INPUT_gxp(ch, ev)
        else:
            self.handle_INPUT_proc(ch, ev)

    # ------------- start up and stuff -------------

    def open_upgrade_listen_socket(self):
        """
        open a server socket to wait for connection upgrade.
        we set its timeout to 2.0, meaning that if we do not
        receive connection in 2.0 sec, we give up.
        """
        host = self.hostname
        if len(self.my_addrs) == 0:
            addr = "127.0.0.1"
        else:
            addr = self.my_addrs[0]
        s = ioman.mk_non_interruptible_socket(socket.AF_INET,
                                              socket.SOCK_STREAM)
        s.bind(("",0))
        s.listen(1)
        ch = ioman.achannel(ioman.primitive_channel_socket(s, 1))
        self.add_rchannel(ch)
        ch.set_timeout(2.0)
        _,port = s.getsockname()
        addr_spec = "tcp:%s:%s:%d" % (host, addr, port)
        self.upgrade_listen_channel = ch
        return addr_spec

    def say_hogehoge(self, parent):
        """
        say hogehoge msg when brought up
        """
        if enable_connection_upgrade:
            ap = self.open_upgrade_listen_socket()
        else:
            ap = None
        hogehoge = ("BEGIN_hogehoge %s %s %s %s END_hogehoge" 
                    % (self.gupid, ap, self.target_label, self.hostname))
        if dbg>=2:
            ioman.LOG("announced to parent %s\n" % hogehoge)
        parent.write_stream(hogehoge)

    def xxx_get_gxp_dir_xxx(self):
        # potentially dangerous
        # gxp_dir = os.environ.get("GXP_DIR", "")
        # if gxp_dir != "": return gxp_dir
        full_path_gxpd_py = get_this_file()
        if full_path_gxpd_py is None: return None
        # full_path_gxpd_py should be like a/gxp3/gxpd.py
        a,b = os.path.split(full_path_gxpd_py)
        if b != "gxpd.py":
            Es(("%s : could not derive GXP_DIR from %s\n"
                % (self.gupid, full_path_gxpd_py)))
            ioman.LOG(("%s : could not derive GXP_DIR from %s\n" 
                       % (self.gupid, full_path_gxpd_py)))
            return None
        return a

    def get_gxp_dir(self):
        gxp_dir,err = this_file.get_this_dir()
        if gxp_dir is None: 
            Es("%s\n" % err)
            ioman.LOG("%s\n" % err)
            return None
        return gxp_dir

    def push_path(self, orig, val):
        if orig == "":
            return val
        else:
            return "%s:%s" % (val, orig)

    def append_path(self, orig, val):
        if orig == "":
            return val
        else:
            return "%s:%s" % (orig, val)

    def get_gxpd_environment(self, root_gupid):
        gxp_dir = self.get_gxp_dir()
        if gxp_dir is None: return None
        if root_gupid == "": root_gupid = self.gupid
        if 1:
            path = os.environ.get("PATH", "")
            path = self.append_path(path, gxp_dir)
            path = self.append_path(path,
                                    os.path.join(gxp_dir, "gxpbin"))
            path = self.append_path(path,
                                    os.path.join(gxp_dir, "gxpmake"))
        if 1:
            pypath = os.environ.get("PYTHONPATH", "")
            pypath = self.append_path(pypath, gxp_dir)
            pypath = self.append_path(pypath,
                                      os.path.join(gxp_dir, "gxpbin"))
            pypath = self.append_path(pypath,
                                      os.path.join(gxp_dir, "gxpmake"))
        prefix,gxp_top = os.path.split(gxp_dir)
        # including these seem to make gxpc slower in
        # some environments
        return gxpd_environment({ "GXP_DIR"        : gxp_dir,
                                  "GXP_TOP"        : gxp_top,
                                  "GXP_HOSTNAME"   : self.hostname,
                                  "GXP_GUPID"      : self.gupid,
                                  "GXP_ROOT_GUPID" : root_gupid,
                                  "PATH"           : path,
                                  "PYTHONPATH"     : pypath,
                                  })

    def set_gxpd_environment(self, remove_self, root_gupid):
        env = self.get_gxpd_environment(root_gupid)
        if env is None:
            return -1
        else:
            if dbg>=2:
                ioman.LOG(("gxpd environment:\n%s\n" 
                           % env.__dict__))
            if (env is not None) and remove_self:
                dire = os.path.dirname(env.GXP_DIR)
                self.remove_on_exit.append(dire)
                if dbg>=2:
                    ioman.LOG(("added to remove on exit: %s\n" 
                               % dire))
            self.gxpd_env = env
            return 0

    def set_target_label(self, target_label):
        """
        set the name in which this gxpd was explored.
        it comes from explore -> inst_local.py -> gxpd.py.
        by default, it defaults to its hostname.
        this is normally used for the root gxpd
        """
        if target_label == "":
            self.target_label = self.hostname
        else:
            self.target_label = target_label
        if dbg>=2:
            ioman.LOG("set target_label to %s\n" % self.target_label)

    def main_no_cleanup(self, argv):
        if dbg>=2:
            ioman.LOG("argv = %s\n" % argv)
        opts = gxpd_opts()
        if opts.parse(argv[1:]) == -1: return gxpd.EX_USAGE
        if self.init(opts) == -1: return gxpd.EX_CONFIG
        self.set_target_label(opts.target_label)
        # open socket to receive requests
        if self.setup_channel_listen(opts.listen, opts.name_prefix, opts.qlen) == -1:
            return gxpd.EX_OSERR
        # set environment variables
        if self.set_gxpd_environment(opts.remove_self, opts.root_gupid) == -1:
            return gxpd.EX_OSERR
        self.setup_parent_peer(opts)

        # no longer necessary
        # if self.parent.peer_name != "" or 1 or 1 or 1: # see mondai
        self.say_hogehoge(self.parent)

        while self.quit == 0:
            ch,ev = self.process_an_event()
            if isinstance(ch, ioman.rchannel_wait_child):
                self.handle_DEATHS(ch, ev)
            elif isinstance(ch, ioman.achannel):
                self.handle_LISTEN(ch, ev)
            elif isinstance(ch, ioman.rchannel):
                self.handle_INPUT(ch, ev)
            elif isinstance(ch, ioman.wchannel):
                self.handle_OUTPUT(ch, ev)
            else:
                bomb()
            if self.profiler.to_start:
                self.profiler.prof.start()
                self.profiler.started()
            if self.profiler.to_stop:
                self.profiler.prof.stop()
                self.profiler.stopped()
        if dbg>=2:
            ioman.LOG("self.quit = 1. quitting ...\n")
        return 0

    def allowed_to_remove(self, allowed, f):
        """
        return 1 if f is under a directory listed in allowed.
        e.g., allowed = [ /tmp' ] and f = /tmp/hoge -> OK
        """
        for d in allowed:
            if os.path.commonprefix([ d, f ]) == d:
                return 1
        return 0

    def safe_remove(self, allowed, f):
        """
        remove f without raising exception
        """
        if not os.access(f, os.F_OK):
            if dbg>=2:
                ioman.LOG("file %s does not exist\n" % f)
        elif self.allowed_to_remove(allowed, f) == 0:
            if dbg>=2:
                ioman.LOG("not allowed to remove %s\n" % f)
        elif not os.path.isabs(f):
            if dbg>=2:
                ioman.LOG("never remove relative path %s\n" % f)
        else:
            if dbg>=2:
                ioman.LOG("remove %s\n" % f)
            try:
                if os.path.isdir(f):
                    os.rmdir(f)
                else:
                    os.remove(f)
            except EnvironmentError,e:
                if dbg>=1:
                    ioman.LOG("could not remove %s %s\n" \
                              % (f, e.args))
            

    def cleanup(self):
        """
        clean up files made by copying GXP_DIR
        """
        allowed =  [ "/tmp" ]
        if "HOME" in os.environ:
            gxp_tmp = os.path.join(os.environ["HOME"], ".gxp_tmp")
            allowed.append(gxp_tmp)
            allowed.append(os.path.realpath(gxp_tmp))

        for top in self.remove_on_exit:
            if self.allowed_to_remove(allowed, top) == 0:
                if dbg>=2:
                    ioman.LOG("not allowed to remove %s\n" % top)
            else:
                if os.path.isdir(top):
                    for dpath,dirs,files in os.walk(top, topdown=0):
                        for x in files + dirs:
                            p = os.path.join(dpath, x)
                            self.safe_remove(allowed, p)
                self.safe_remove(allowed, top)

    def get_exception_trace(self):
        import cStringIO,traceback
        type,value,trace = sys.exc_info()
        cio = cStringIO.StringIO()
        traceback.print_exc(trace, cio)
        return cio.getvalue()

    def main(self, argv):
        try:
            self.main_no_cleanup(argv)
        except Exception,e:
            ioman.LOG("gxpd.py terminated with an exception:\n")
            ioman.LOG("%s\n" % self.get_exception_trace())
        self.cleanup()

def main():
    return gxpd().main(sys.argv)

if __name__ == "__main__":
    main()

# $Log: gxpd.py,v $
# Revision 1.25  2013/10/25 12:00:53  ttaauu
# added higher-level APIs to ioman and a document for it
#
# Revision 1.24  2012/07/04 07:22:22  ttaauu
# shorten unix domain socket name (again), so it no longer includes user name
#
# Revision 1.23  2011/09/29 17:24:19  ttaauu
# 2011-09-30 Taura
#
# Revision 1.22  2010/12/15 08:48:12  ttaauu
# *** empty log message ***
#
# Revision 1.21  2010/09/08 04:08:22  ttaauu
# a new job scheduling framework (gxpc js). see ChangeLog 2010-09-08
#
# Revision 1.20  2010/05/25 18:13:58  ttaauu
# support --translate_dir src,dst1,dst2,... and associated changes. ChangeLog 2010-05-25
#
# Revision 1.19  2010/05/23 09:02:36  ttaauu
# small bug fix in work.db generation
#
# Revision 1.18  2010/05/20 14:56:56  ttaauu
# e supports --rlimit option. e.g., --rlimit rlimit_as:2g ChangeLog 2010-05-20
#
# Revision 1.17  2010/05/19 03:41:10  ttaauu
# gxpd/gxpc capture time at which processes started/ended at remote daemons. xmake now receives and displays them. xmake now never misses IO from jobs. ChangeLog 2010-05-19
#
# Revision 1.16  2010/05/11 08:02:35  ttaauu
# *** empty log message ***
#
# Revision 1.15  2010/05/09 04:55:28  ttaauu
# *** empty log message ***
#
# Revision 1.14  2010/03/05 05:27:08  ttaauu
# stop extending PYTHONPATH. see 2010-3-5 ChangeLog
#
# Revision 1.13  2010/01/31 05:31:28  ttaauu
# added mapreduce support
#
# Revision 1.12  2010/01/05 06:48:32  ttaauu
# fixed fixed a bug in gxpd.py that generates a too long unix domain socket pathnames
#
# Revision 1.11  2009/12/27 16:02:20  ttaauu
# fixed broken --create_daemon 1 option
#
# Revision 1.10  2009/09/17 18:47:53  ttaauu
# ioman.py,gxpm.py,gxpd.py,gxpc.py,xmake: changes to track rusage of children and show them in state.txt
#
# Revision 1.9  2009/09/06 20:05:46  ttaauu
# lots of changes to avoid creating many dirs under ~/.gxp_tmp of the root host
#
# Revision 1.8  2009/06/06 14:06:22  ttaauu
# added headers and logs
#
