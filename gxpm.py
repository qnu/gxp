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
# $Header: /cvsroot/gxp/gxp3/gxpm.py,v 1.11 2010/09/08 04:08:22 ttaauu Exp $
# $Name:  $
#

def import_safe_pickler():
    """
    import cPickle if it exists.
    otherwise resort to pickle.
    """
    import cPickle,pickle
    try:
        cPickle.dumps(None)
        return cPickle
    except:
        return pickle

pickler = import_safe_pickler()

def unparse(m):
    """
    msg -> string
    """
    return pickler.dumps(m)

def parse(s):
    """
    string -> msg
    """
    return pickler.loads(s)

class exec_env:
    """
    process execution environment
    """
    def __init__(self):
        self.cwd = None         # working dir (None : do not change it)
        self.env = {}           # environment variables
    def show(self):
        return ("cwd=%s, env=%s" % (self.cwd, self.env))

class target_tree:
    """
    a tree of daemons, describing which daemons should
    deliver this message
    """
    def __init__(self, name, hostname, target_label,
                 eflag, exec_idx, eenv, children):
        # gupid of the receiving daemon
        self.name = name
        # hostname of this daemon
        self.hostname = hostname
        # target label of it
        self.target_label = target_label
        # 1 if this daemon should deliver the msg
        self.eflag = eflag
        # sequential index of this daemon
        self.exec_idx = exec_idx
        # exec_env instance
        self.eenv = eenv
        # children daemons. a list of target_tree
        # or None to mean all children
        self.children = children
        # number of daemons that should deliver the message
        # or None if it is not known (because some
        # nodes have None as children)
        self.num_execs = None

    def count(self):
        """
        return # of nodes of the tree, whether eflag is on
        or not
        """
        if self.children is None: return None # unknown
        c = 1
        for ch in self.children:
            x = ch.count()
            if x is None: return None
            c = c + x
        return c

    def count_execs(self):
        """
        return # of nodes of the tree whose eflag are set
        """
        if self.children is None: return None # unknown
        c = self.eflag
        for ch in self.children:
            x = ch.count_execs()
            if x is None: return None
            c = c + x
        return c

    def show(self):
        """
        convert to string
        """
        if self.children is None:
            cs = None
        else:
            cs = map(lambda c: c.show(), self.children)
        if self.eenv is None:
            eenv_show = "None"
        else:
            eenv_show = self.eenv.show()
        return ("target_tree(%s, %s, %s, %s, %s, %s, %s)" \
                % (self.name, self.hostname, self.target_label,
                   self.eflag, self.exec_idx,
                   eenv_show, cs))

    def set_eflag(self, flag):
        """
        set eflag of all nodes to flag
        """
        self.eflag = flag
        if self.children is not None:
            for child in self.children:
                child.set_eflag(flag)

def merge_target_tree(tgt1, tgt2):
    """
    merge two target trees
    """
    eflag = tgt1.eflag or tgt2.eflag
    name_dictionary = {}
    children = tgt1.children + tgt2.children
    for child in children:
        if name_dictionary.has_key(child.name):
            # both trees have a child of the same name, 
            # so merge them recursively
            name_dictionary[child.name] = merge_target_tree(name_dictionary[child.name], child)
        else:
            name_dictionary[child.name] = child
    return target_tree(tgt1.name, tgt1.hostname, tgt1.target_label,
                       eflag, None, tgt1.eenv, name_dictionary.values())


class xxx_synchronize_message:
    def __init__(self, peer_tree=None, exec_tree=None):
        self.peer_tree = peer_tree
        self.exec_tree = exec_tree

#
# actions
# 
# an action describes an 'instruction' to a daemon node,
# such as "create a process of this command line",
# or "feed this msg to the process of this id"
#

class action:
    pass

class action_quit(action):
    """
    daemons that receive this action should quit
    """
    pass

class action_ping(action):
    """
    daemons that receive this action immediately respond
    with basic information about the daemon
    """
    def __init__(self, level):
        """
        level : specifies how detailed shoult the response be
        """
        self.level = level

class action_createproc(action):
    """
    receiving this action, the daemon should create
    a process with a specified command line (cmd),
    working dir (cwd), environment (env), relative id
    (rid), and open file descriptors (pipes).

    relative id is an id given to the process unique 
    in the task the process belongs to.
    
    for "pipes", see gxpc.py's add_down_pipe method.
    it is a list of a record that loooks like...

    """
    def __init__(self, rid, cwds, env, cmd, pipes, rlimits):
        self.rid = rid          # relative process id
        self.cwds = cwds        # list of dirs or None
        self.env = env
        self.cmd = cmd
        self.pipes = pipes
        self.rlimits = rlimits

class action_createpeer(action):
    """
    similar to action_createproce. the only difference
    is it should create a child daemon, so it should
    notify the parent when the daemon brought up.
    """
    def __init__(self, rid, cwds, env, cmd, pipes, rlimits):
        self.rid = rid
        self.cwds = cwds        # list of dirs or None
        self.env = env
        self.cmd = cmd
        self.pipes = pipes
        self.rlimits = rlimits

class action_feed(action):
    """
    an instruction to feed a string (payload)
    to file descriptor (fd) of a process whose
    relative id is rid.
    """
    def __init__(self, rid, fd, payload):
        self.rid = rid
        self.fd = fd
        self.payload = payload
    
class action_close(action):
    """
    an instruction to close file descriptor (fd) 
    of a process whose relative id is rid.
    """
    def __init__(self, rid, fd):
        self.rid = rid
        self.fd = fd
    
class action_sig(action):
    """
    an instruction to send a signal (sig)
    to a process whose relative id is rid.
    """
    def __init__(self, rid, sig):
        self.rid = rid
        self.sig = sig

class action_chdir(action):
    """
    an instruction to change its dir to TO.
    currently not used.
    """
    def __init__(self, to):
        self.to = to
    
class action_export(action):
    """
    an instruction to set its environment
    variable (var) to val.
    """
    def __init__(self, var, val):
        self.var = var
        self.val = val

class action_trim(action):
    """
    an instruction to trim its children that
    do not receive this msg.
    """
    def __init__(self):
        pass
    
class action_set_max_buf_len(action):
    """
    an instruction to set its maximum
    buffer length.
    """
    def __init__(self, max_buf_len):
        self.max_buf_len = max_buf_len
    
class action_prof_start(action):
    """
    an instruction to start profiling
    """
    def __init__(self, file):
        self.file = file
    
class action_prof_stop(action):
    """
    an instruction to stop profiling
    """
    pass

class action_set_log_level(action):
    """
    an instruction to set its loglevel
    """
    def __init__(self, level):
        self.level = level
    
class action_set_log_base_time(action):
    """
    an instruction to set its log base time
    """
    pass
    
class action_reclaim_task(action):
    def __init__(self, target_tids):
        self.target_tids = target_tids

# to synchronize gxpcs
class xxx_action_synchronize(action, xxx_synchronize_message):
    pass


#
# commands
#

#
# clause
#
# clause is a list of actions with a condition under which
# those actions should be executed.
#

class clausexxx:
    """
    an instruction that says "do those actions
    when your name (gupid) matches a regular expression ON"
    """
    def __init__ (self, on, actions):
        self.on = on            # regular exp of gupid
        self.actions = actions

#
# down msg
#
#

keep_connection_never = 0
keep_connection_until_fin = 1
keep_connection_forever = 2

class down:
    def __init__(self, target, tid, persist, keep_connection, gcmds):
        # target tree (target_tree instance)
        self.target = target
        # task id this msg talks about
        self.tid = tid
        # 1 if the task sholud persist even if its processes are all gone
        self.persist = persist
        # see above constants (0, 1, or 2).
        # specify what the root daemon does to the connection to the
        # client (gxpc.py) process.
        # never     : immediately close it
        # until_fin : keep it until tasks are gone. close it when 
        # tasks are gone
        # forever   : keep forever (never close from this side)
        self.keep_connection = keep_connection
        # list of list of clauses
        self.gcmds = gcmds

#
# event
#
# describes some events that occurred around the daemon,
# such as "a process finished", and "a process outputs
# this".  besides, it generally describes information
# from daemons to the client (gxpc.py).
#

class event:
    pass
    
class event_info(event):
    """
    low level messages such as error messages
    """
    def __init__(self, status, msg):
        """
        status : status of gxpd
        msg : whatever string a daemon wishes to deliver
        """
        self.status = status
        self.msg = msg

class event_info_pong(event_info):
    """
    response to ping action.
    """
    def __init__(self, status, msg,
                 targetlabel, peername, hostname,
                 parent, children, children_in_progress):
        event_info.__init__(self, status, msg)
        # target label of the daemon
        self.targetlabel = targetlabel
        # gupid
        self.peername = peername
        # hostname
        self.hostname = hostname
        # parent gupid
        self.parent = parent
        # children gupid
        self.children = children
        # children that are in progress
        self.children_in_progress = children_in_progress

class event_io(event):
    """
    an event indicating a process or a child gxp says something.
    """
    def __init__(self, src, kind, rid, pid, fd, payload, err_msg):
        # proc or peer
        self.src = src
        # OK, EOF, ERROR, TIMEOUT
        self.kind = kind
        # relative process ID within a task
        self.rid = rid
        # process id
        self.pid = pid
        # file descriptor (channel name)
        self.fd = fd
        # string that is output
        self.payload = payload
        # string indicating error msg
        self.err_msg = err_msg
        
class event_die(event):
    """
    an event indicating a process is dead.
    """
    def __init__(self, src, rid, pid, status, rusage, time_start, time_end):
        # src : proc or peer
        self.src = src
        # relative process ID within a task
        self.rid = rid
        # process id
        self.pid = pid
        # status (return value of waitpid)
        self.status = status
        # rusage of the process
        self.rusage = rusage
        # local time (via time.time()) at which the process was
        # started/finished
        self.time_start = time_start
        self.time_end = time_end

class event_peerstatus(event):
        
    """
    an event indicating a peer status (NG/OK) becomes available
    """
    def __init__(self, peername, target_label, hostname, status, parent_name, rid):
        # gupid of the child gxpd in question
        self.peername = peername
        # its target label
        self.target_label = target_label
        # its hostname
        self.hostname = hostname
        # OK, NG
        self.status = status
        self.parent_name = parent_name
        # relative id
        self.rid = rid

class event_fin(event):
    """
    an event indicating that no processes of the task
    are currently left under the sender's subtree.
    used to detect a task has finished.
    """
    def __init__(self, weight):
        self.weight = weight

class event_nopeersinprogress(event):
    """
    similar to event_fin, but indicates that no
    gxpd processes of the task are in progress 
    under the sender's subtree
    used to detect an explore has finished.
    """
    pass

# to synchronize gxpcs
class event_invalidate_view(event):
    def __init__(self):  # peer_tree, exec_tree
        pass

#
# an upward msg (from down to up)
#

class up:
    def __init__(self, gupid, tid, event):
        self.gupid = gupid
        self.tid = tid
        self.event = event

class syn:
    def __init__(self, gupid, tid, event):
        self.gupid = gupid
        self.tid = tid
        self.event = event

# $Log: gxpm.py,v $
# Revision 1.11  2010/09/08 04:08:22  ttaauu
# a new job scheduling framework (gxpc js). see ChangeLog 2010-09-08
#
# Revision 1.10  2010/05/25 18:13:58  ttaauu
# support --translate_dir src,dst1,dst2,... and associated changes. ChangeLog 2010-05-25
#
# Revision 1.9  2010/05/20 14:56:56  ttaauu
# e supports --rlimit option. e.g., --rlimit rlimit_as:2g ChangeLog 2010-05-20
#
# Revision 1.8  2010/05/19 03:41:10  ttaauu
# gxpd/gxpc capture time at which processes started/ended at remote daemons. xmake now receives and displays them. xmake now never misses IO from jobs. ChangeLog 2010-05-19
#
# Revision 1.7  2009/09/27 17:15:14  ttaauu
# added comment on gxpm.py
#
# Revision 1.6  2009/09/17 18:47:53  ttaauu
# ioman.py,gxpm.py,gxpd.py,gxpc.py,xmake: changes to track rusage of children and show them in state.txt
#
# Revision 1.5  2009/06/06 14:06:23  ttaauu
# added headers and logs
#
