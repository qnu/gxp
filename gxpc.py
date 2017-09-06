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
# $Header: /cvsroot/gxp/gxp3/gxpc.py,v 1.76 2012/07/04 15:32:53 ttaauu Exp $
# $Name:  $
#

def prompt_():
    s = os.environ.get("GXP_SESSION", "")
    if s == "":
        D = "/tmp/gxp-%s-%s" % (os.environ.get("USER", "unknown"),
                                os.environ.get("GXP_TMP_SUFFIX", "default"))
        for d in os.listdir(D):
            # "gxp-xxxxxxxx-session-..."
            if d[0:4] == "gxp-" and d[12:21] == "-session-":
                if s == "":
                    s = os.path.join(D, d)
                else:
                    os.write(1, "[?/?/?]\n")
                    return 1
    fp = open(s)
    os.write(1, fp.readline())
    return 0

def prompt():
    try:
        return prompt_()
    except:
        return 1
    
import os,sys
if len(sys.argv) == 2 and sys.argv[1] == "prompt":
    os._exit(prompt())
    
def import_safe_pickler():
    import cPickle,pickle
    try:
        cPickle.dumps(None)
        return cPickle
    except:
        return pickle

pickler = import_safe_pickler()
import cStringIO,errno,fcntl,glob,random,re
import select,signal,socket,stat # shlex
import string,time,threading,types,copy
import opt,gxpm,this_file
# ,gxpd

#
# gxp frontend (command interpreter) that talks to daemons
#

def Ws(s):
    sys.stdout.write(s)

def Es(s):
    sys.stderr.write(s)

def Ef():
    sys.stderr.flush()

class counter:
    def __init__(self, init):
        self.x = init
    def decrement(self):
        x = self.x
        self.x = x - 1
        return x
    def add(self, y):
        self.x += y

class peer_tree_node:
    def __init__(self):
        self.name = None                # peer_name. initially None
        self.hostname = None
        self.children = {}              # nid -> peer_tree_node
        self.cmd = None                 # cmd issued to get this peer
        self.target_label = None        # target label
        self.eenv = gxpm.exec_env()

    def show_rec(self, indent):
        spaces = " " * indent
        Ws(("%s%s (= %s %s)\n" 
            % (spaces, self.name, self.hostname, self.target_label)))
        for c in self.children.values():
            c.show_rec(indent + 1)

    def show(self):
        self.show_rec(0)

class login_method_configs:
    def __init__(self):
        # syntax of the following.
        # if the first character is non alphabetical, use
        # it as the separator (see ssh below, which uses :)
        # otherwise it uses whitespaces as the separator
        self.ssh         = ("ssh -o StrictHostKeyChecking=no "
                            "-o PreferredAuthentications=hostbased,publickey "
                            "-A %target% %cmd%")
        self.ssh_as      = ("ssh -o StrictHostKeyChecking=no "
                            "-o PreferredAuthentications=hostbased,publickey "
                            "-A -l %user% %target% %cmd%")

        self.rsh         = "rsh %target% %cmd%"
        self.rsh_as      = "rsh -l %user% %target% %cmd%"
        self.sh          = "sh -c %cmd%"
        self.qrsh        = "qrsh %cmd%"
        self.qrsh_host   = "qrsh -l hostname=%target% %cmd%"

        self.sge         = "qsub_wrap --sys sge %cmd%"
        self.sge_host    = "qsub_wrap --sys sge %cmd% -- -l hostname=%target%"
        
        self.torque      = "qsub_wrap --sys torque %cmd%"
        self.torque_n    = ("qsub_wrap --sys torque %cmd% "
                            "-- -l nodes=%nodes:-1%:ppn=%ppn:-1%")
        self.torque_host = ("qsub_wrap --sys torque %cmd% "
                            "-- -l nodes=1:%target%:ppn=%ppn:-1%")
        self.torque_psched = ("qsub_wrap --sys torque_psched %cmd% "
                              "-- --node %target% --lib %lib:-libtorque.so%")
        self.condor        = "qsub_wrap --sys condor %cmd%"

        self.nqs_hitachi = "qsub_wrap --sys nqs_hitachi %cmd%"
        self.nqs_fujitsu = "qsub_wrap --sys nqs_fujitsu %cmd%"
        # aliases
        self.hitachi     = "qsub_wrap --sys nqs_hitachi %cmd%"
        self.fujitsu     = "qsub_wrap --sys nqs_fujitsu %cmd%"

        self.n1ge        = "qsub_wrap --sys n1ge %cmd%"
        self.n1ge_host   = "qsub_wrap --sys n1ge %cmd% -l host=%target%"

        self.tsubame     = ("qsub_wrap --sys n1ge "
                            "--timeout %timeout:-100% %cmd% "
                            "--qsub n1ge --qstat qstat --qdel qdelete %cmd% "
                            "-- -q %q% -g %g% -mem %mem:-4.0% -rt %cpu:-30%")
        self.ha8000     = ("qsub_wrap --sys nqs_hitachi "
                           "--timeout %timeout:-100% --addr 10 %cmd% "
                           "-- -q %q% -N %nodes:-1% -J T%ppn:-1% "
                           "-lT %cpu:-5%:00 -lm %mem:-28%gb")
        self.hx600     = ("qsub_wrap --sys nqs_fujitsu "
                          "--timeout %timeout:-100% %cmd% "
                          "-- -q %q% -g %g% -lP %nodes:-1% -lp %ppn:-1% "
                          "-cp %cpu:-5%:00 -lm %mem:-28%gb "
                          "-nr -Pvn UNPack")
        self.tsubame2     = ("qsub_wrap --sys torque "
                            "--timeout %timeout:-100%  "
                            "--qsub t2sub --qstat t2stat --qdel t2del %cmd% "
                             "-- -q %q% -W group_list=%group_list% -l walltime=%walltime:-1:00:00% "
                             "-l select=%nodes:-1%:ncpus=%ncpus:-12%:mem=%mem:-52%gb "
                             "-l place=%place:-scatter%")
        self.fx10 = "qsub_wrap --sys pjsub %cmd%"
        self.reedbush = ("qsub_wrap --sys torque "
                         "--timeout %timeout:-100% "
                         "--qstat rbstat "
                         "--script_dir /lustre/gc64/c64000 "
                         "%cmd% "
                         "-- -q %q% "
                         "-l select=%nodes:-1%:ncpus=%ncpus:-36%:mpiprocs=%mpiprocs:-1%:ompthreads=%ompthreads:-1% "
                         "-W group_list=%group_list% "
                         "-l walltime=%walltime:-1:00:00% ")
        # kyoto university MPP (system A)
        # the script needs to invoke python by aprun python ...
        # the address to connect back to is 10.7.x.x, among 
        # other addrs like 10.5.x.x, 10.6.x.x, etc.
        # usage example: request 3 nodes from host xe-????
        #   gxpc use kyoto_mpp xe foo
        #   gxpc explore -a q=queue_name -a ug=user_group_name foo 3
        self.kyoto_mpp = [ "qsub_wrap", 
                           "--python", "aprun python", 
                           "--addr", "10.7",
                           "--sys", "lsf", "%cmd%",
                           "--",
                           "-q", "%q%", "-ug", "%ug%",
                           "-A", "p=%p:-1%:t=%t:-16%:c=%c:-16%:m=%m:-61440M%",
                           "-W", "%W:-24:00%" ]
        # kyoto university cluster (system B)
        # the address to connect back to is 10.5.x.x, among 
        # other addrs like 10.4.x.x, 10.6.x.x, etc.
        # usage example: request 3 nodes from host ap-???
        #   gxpc use kyoto_cluster ap foo
        #   gxpc explore -a q=queue_name -a ug=user_group_name foo 3
        self.kyoto_cluster = ("qsub_wrap --addr 10.5 "
                              "--sys lsf %cmd% "
                              "-- -q %q% -ug %ug% "
                              "-A p=%p:-1%:t=%t:-16%:c=%c:-16%:m=%m:-61440M% "
                              "-W %W:-24:00%")


class mask_patterns:
    def __init__(self, hostmask, gupidmask, targetmask, idxmask):
        self.hostmask = hostmask
        self.gupidmask = gupidmask
        self.targetmask = targetmask
        self.idxmask = idxmask

class session_state:
    """
    State of a gxp session.
    A session lasts longer than a single command.

    Regular command execution:

    1. specified exec tree is chosen and used to send msgs
    2. last_exec_tree is set to the chosen exec tree.
       last_term_status is cleared (empty dictionary)
    3. termination statuses are written into last_term_status
    4. when execution is done, last_exec_tree and last_term_status
       are examined and nodes that are marked successful are counted.
    5. update last_ok_count by this.

    smask/pushmask:

    1. make a new exec tree based on last_exec_tree and
       last_term_status.
    2. install the new tree as stack_exec_trees[0]
       smask will overwrite the old stack_exec_trees[0],
       whereas pushmask will not (push).

    rmask/explore:
    1. stack_exec_trees[0] will become the whole peer_tree
    2. update peer_tree_count
    3. last_ok_count = peer_tree_count

    popmask:

    1. delete stack_exec_trees[0]
    2. 

    
    """
    def __init__(self, filename):
        self.filename = filename
        self.peer_tree = None           # tree of all live peers
        self.stack_exec_trees = [ None ] # exec tree,exec count
        self.saved_exec_trees = {}      #
        # exec tree used for the last submission
        self.last_exec_tree = None
        self.last_term_status = None
        # of nodes in peer_tree
        self.peer_tree_count = None
        # of nodes with eflag=1 in stack_exec_trees[0]
        self.cur_exec_count = None      
        # of nodes with term_status=1 in last_exec_tree
        self.last_ok_count = None       
        # gupid -> peer_tree_node
        self.reached = None
        # dict of successfully reached targets
        self.successful_targets = {}
        # whatever is specified via edges commands
        self.edges = []
        # some explore parameters
        #
        # self.max_children_soft = 10
        # self.max_children_hard = 100
        self.default_explore_opts = None
        # specified targets
        # self.target_hosts = []
        # creatd flag is 1 if this was just created.
        # 0 if unpickled from disk (cleard upon save)
        self.created = 1
        # dirty is 0 if it is 100% sure that disk has the same image
        # OK to always pretend it is 1.
        self.dirty = 1
        # set to 1 when other tasks change the state of the tree
        self.invalid = 0
        self.init_random_generator()
        # e.g., [("ssh", ....), ("qrsh", ....)]
        self.login_methods = {}
        # default login methods
        lm = login_method_configs()
        for name,cmdline in lm.__dict__.items():
            if type(cmdline) is types.ListType:
                cmd = cmdline
            else:
                c = cmdline[0]
                if c not in string.letters:
                    cmd = string.split(cmdline[1:], c)
                else:
                    cmd = string.split(cmdline)
            self.login_methods[name] = cmd

    def show(self, level):
        Ws("%s\n" % self.filename)
        if level >= 1:
            self.peer_tree.show()
        if level >= 2:
            Ws("stack_exec_trees:\n")
            for ex,t in self.stack_exec_trees:
                Ws(" %s: %s\n" % (ex, t.show()))
            Ws("saved_exec_trees: %s\n" % self.saved_exec_trees)
            Ws("last_exec_tree: %s\n" % self.last_exec_tree.show())
            Ws("last_term_status: %s\n" % self.last_term_status)
            Ws("peer_tree_count: %s\n" % self.peer_tree_count)
            Ws("cur_exec_count: %s\n" % self.cur_exec_count)
            Ws("last_ok_count: %s\n" % self.last_ok_count)
            Ws("reached: %s\n" % self.reached)
            Ws("successful_targets: %s\n" % self.successful_targets)
            Ws("edges: %s\n" % self.edges)
            Ws("default_explore_opts: %s\n" % self.default_explore_opts)
            Ws("created: %s\n" % self.created)
            Ws("dirty: %s\n" % self.dirty)
            Ws("invalid: %s\n" % self.invalid)
            Ws("login_methods: %s\n" % self.login_methods)

    def init_random_generator(self):
        self.rg = random.Random()
        self.rg.seed(time.time() * os.getpid())
        
    def randint(self, a, b):
        return self.rg.randint(a, b)

    def gen_random_id(self):
        return self.randint(0, 10**8 - 1)

    def mk_pat_from_mask(self, name, negname, mask, negmask):
        if mask is not None:
            try:
                pat = re.compile(mask)
            except Exception,e:
                Es("gxpc: invalid %s '%s' %s\n" % (name, mask, e.args))
                return None
        elif negmask is not None:
            try:
                neg_pat = re.compile(negmask)
            except Exception,e:
                Es("gxpc: invalid %s '%s' %s\n" % (negname, e.args))
                return None
            pat = re.compile("(?!(%s))" % negmask)
        else:
            pat = re.compile(".")
        return pat

    def select_exec_tree(self, opts):
        """
        mask : name, number, or None (all)

        based on parameters given in the command line, select
        and/or make the target tree
        mask :         given by -m (--withmask)         0
        hostmask :     given by -h (--withhostmask)     .*
        hostnegmask :  given by -H (--withhostnegmask)  .*
        gupidmask :    given by -g (--withgupidmask)    .*
        gupidnegmask : given by -G (--withgupidnegmask) .*
        targetmask :    given by -g (--withtargetmask)    .*
        targetnegmask : given by -G (--withtargetnegmask) .*

        return ex,t
        where ex is exec_count and t is tree
        """

        # first choose the appropriate tree by mask
        # most of the time it is in stack_exec_trees[0]
        mask = opts.withmask
        if self.peer_tree is None:
            # special case
            t = gxpm.target_tree(".*", ".*", ".*", 1, 0, gxpm.exec_env(), None)
            ex = ""
        elif mask is None:
            ex,t = self.mk_whole_exec_tree(self.peer_tree)
        elif type(mask) is types.StringType:
            if self.saved_exec_trees.has_key(mask):
                ex,t = self.saved_exec_trees[mask]
            else:
                Es("gxpc: no exec tree entry named %s\n" % mask)
                return (None,None)        # NG
        elif type(mask) is types.IntType:
            if 0 <= mask < len(self.stack_exec_trees):
                ex,t = self.stack_exec_trees[mask]
            else:
                Es("gxpc: invalid mask value %d\n" % mask)
                return (None,None)        # NG
        # then filter hosts by -h, -H, -g, -G, -i, -I
        hostmask = self.mk_pat_from_mask("hostmask", "hostnegmask",
                                         opts.withhostmask, opts.withhostnegmask)
        if hostmask is None: return (None,None)
        gupidmask = self.mk_pat_from_mask("gupidmask", "gupidnegmask",
                                          opts.withgupidmask, opts.withgupidnegmask)
        if gupidmask is None: return (None,None)
        targetmask = self.mk_pat_from_mask("targetmask", "targetnegmask",
                                           opts.withtargetmask, opts.withtargetnegmask)
        if targetmask is None: return (None,None)
        idxmask = self.mk_pat_from_mask("idxmask", "idxnegmask",
                                        opts.withidxmask, opts.withidxnegmask)
        if idxmask is None: return (None,None)
        pats = mask_patterns(hostmask, gupidmask, targetmask, idxmask)
        # 2. really filter nodes
        ex_,t_ = self.mk_selected_exec_tree_rec2(t, pats)
        if t_ is not None:
            # set exec idx of nodes
            ex__,_ = self.set_exec_idx_rec(t_, 0, 0, ex_)
            assert (ex__ == ex_), (ex__, ex_)
        self.last_exec_tree = t_
        self.last_term_status = {}
        return (ex_,t_)

    def mk_selected_exec_tree_rec2(self, tgt, pats):
        """
        like mk_selected_exec_tree_rec, but select nodes whose names match pat
        """
        if tgt is None or tgt.name is None:
            return (0, None)
        elif tgt.children is None:
            # tgt.children means allchildren, treated specially
            # return (1, tgt)
            # ex = "" means exec_count is unknown
            return ("", tgt)
        else:
            T = []
            C = 0
            for child in tgt.children:
                c,t = self.mk_selected_exec_tree_rec2(child, pats)
                if t is not None:
                    T.append(t)
                    C = C + c
            if tgt.eflag and pats.hostmask.match(tgt.hostname) \
                    and pats.gupidmask.match(tgt.name) \
                    and pats.targetmask.match(tgt.target_label) \
                    and pats.idxmask.match("%d" % tgt.exec_idx):
                ef = 1
                C = C + 1
            else:
                ef = 0
            if C > 0:
                return (C, gxpm.target_tree(tgt.name, tgt.hostname, tgt.target_label,
                                            ef, None, tgt.eenv, T))
            else:
                return (0, None)

    def mk_selected_exec_tree_rec(self, tgt, sign, status):
        """
        tgt    : instance of gxpm.target_tree.
        sign   : 1 or 0
        status : dictionary gupid -> exit status (int)

        return a pair (0, None) or (c, tree), where tree
        is an instance of gxpm.target_tree. c is the number of nodes
        in tree whose status (i.e., status[n.tgt_name]) match sign.
        (if sign is 1, count nodes whose statuses are zero.
        if sign is 0, count nodes whose statuses are non-zero).

        tree is the one made by removing some nodes of tgt from
        leaves.  it removes node N if and only if N contains no nodes
        under it whose status (obtained by status dictionary) do not
        match sign.
        
        """
        if tgt is None or tgt.name is None:
            return (0, None)
        elif tgt.children is None:
            # tgt.children means allchildren, treated specially
            return ("", tgt)
        else:
            T = []
            C = 0
            for child in tgt.children:
                c,t = self.mk_selected_exec_tree_rec(child, sign,
                                                     status)
                if t is not None:
                    T.append(t)
                    C = C + c
            if tgt.eflag and \
                   ((sign and status.get(tgt.name, 1) == 0) or \
                    (sign == 0 and status.get(tgt.name, 1) != 0)):
                ef = 1
                C = C + 1
            else:
                ef = 0
            if C > 0:
                return (C, gxpm.target_tree(tgt.name, tgt.hostname, tgt.target_label,
                                            ef, None, tgt.eenv, T))
            else:
                return (0, None)

    def mk_whole_exec_tree(self, ptree):
        ex,tree = self.mk_whole_exec_tree_rec(ptree)
        if tree is not None:
            ex_,all = self.set_exec_idx_rec(tree, 0, 0, ex)
            assert (ex_ == ex), (ex_, ex)
        return ex,tree
    
    def mk_whole_exec_tree_rec(self, ptree):
        """
        ptree : instance of peer_tree
        """
        if ptree is None or ptree.name is None:
            return (0, None)
        else:
            T = []
            C = 0
            for child in ptree.children.values():
                c,t = self.mk_whole_exec_tree_rec(child)
                if t is not None:
                    T.append(t)
                    C = C + c
            return (C + 1,
                    gxpm.target_tree(ptree.name, ptree.hostname, ptree.target_label,
                                     1, None, ptree.eenv, T))

    def set_exec_idx_rec(self, tgt, exec_idx, all_idx, num_execs):
        """
        traverse target tree tgt and set exec_idx and num_execs of nodes.
        return exec_idx,all_idx
        where exec_idx is the number of nodes whose eflag is set
        all_idx is the number of nodes
        """
        assert (num_execs == "" or type(num_execs) is types.IntType), num_execs
        all_idx = all_idx + 1
        if tgt.eflag:
            tgt.num_execs = num_execs
            tgt.exec_idx = exec_idx
            exec_idx = exec_idx + 1
        # tgt.children means allchildren, treated specially
        if tgt.children is None:
            return "",""                # unknown,unknown
        else:
            for child in tgt.children:
                R = self.set_exec_idx_rec(child, exec_idx,
                                          all_idx, num_execs)
                exec_idx,all_idx = R
            return exec_idx,all_idx

    def set_selected_exec_tree(self, sign, push, name):
        """
        smask/pushmask:
        
        1. make a new exec tree based on last_exec_tree and
        last_term_status.
        2. install the new tree as stack_exec_trees[0]
        smask will overwrite the old stack_exec_trees[0],
        whereas pushmask will not (push).
        """
        tgt = self.last_exec_tree
        status = self.last_term_status
        ex,tree = self.mk_selected_exec_tree_rec(tgt, sign, status)
        if tree is not None:
            ex_,_ = self.set_exec_idx_rec(tree, 0, 0, ex)
            assert (ex_ == ex), (ex_, ex)
        if push:
            self.stack_exec_trees.insert(0, (ex, tree))
        else:
            self.stack_exec_trees[0] = (ex, tree)
        if name is not None:
            self.saved_exec_trees[name] = (ex, tree)
        # update cache of cur_exec_count
        self.cur_exec_count = ex

    def reset_exec_tree(self):
        """
        stack_exec_trees[0] will become the whole peer_tree
        """
        ex,tree = self.mk_whole_exec_tree(self.peer_tree)
        # 2008.7.9 is this what I should do?
        self.last_exec_tree = tree
        self.stack_exec_trees[0] = (ex,tree)
        self.last_ok_count = ex
        self.cur_exec_count = ex
        self.peer_tree_count = ex

    def mk_peer_tree(self, events):
        """
        analyze the events received as a result of a ping command
        and create the tree of live nodes
        """
        # gupid -> peer_tree_node
        reached = {}
        # target_label -> count
        successful_targets = {}

        # first create an empty node for each live node
        pongs = []
        for gupid,tid,ev in events:
            if isinstance(ev, gxpm.event_info_pong):
                pongs.append((gupid, ev))
        for gupid,pong in pongs:
            reached[gupid] = peer_tree_node()

        # fill their names and children. also find root
        roots = []
        for gupid,pong in pongs:
            assert isinstance(pong, gxpm.event_info_pong), pong
            # record attributes of the peer_tree_node for gupid
            target_label = pong.targetlabel
            t = reached[gupid]
            t.name = gupid
            t.hostname = pong.hostname
            t.target_label = target_label
            # record how many nodes we got for target_label
            s = successful_targets.get(target_label, 0)
            successful_targets[target_label] = s + 1
            # record children of gupid
            C = []
            for cname in pong.children:
                # add child
                if reached.has_key(cname):
                    id = self.gen_random_id()
                    t.children[id] = reached[cname]
            # no parent -> it is a root
            if pong.parent == "": roots.append(t)
        # obviously the root must be unique
        if len(roots) != 1:
            Es("gxpc: broken gxp daemon tree (deamons seem broken, roots=%s)\n" % roots)
            return None,None,None
        # check if parent and children are consistent
        for gupid,pong in pongs:
            # get gupid's parent
            if pong.parent == "": continue
            # check if gupid is a child of the parent
            p = reached[pong.parent]
            found = 0
            for c in p.children.values():
                if c.name == gupid:
                    found = 1
                    break
            assert found == 1
        return roots[0],reached,successful_targets

    def construct_peer_tree(self, events):
        R = self.mk_peer_tree(events)
        peer_tree,reached,successful_targets = R
        if peer_tree is None: return -1
        self.peer_tree = peer_tree
        self.reached = reached
        self.successful_targets = successful_targets
        self.reset_exec_tree()
        return 0

    def list_gupids_rec(self, tgt, gupids):
        if tgt is None: return gupids
        assert not gupids.has_key(tgt.name)
        gupids[tgt.name] = 1
        if tgt.children is not None:
            for child in tgt.children:
                self.list_gupids_rec(child, gupids)
        return gupids

    def trim_peer_tree_rec(self, peer_tree, reached,
                           successful_targets, target_gupids):
        if not target_gupids.has_key(peer_tree.name):
            return None
        reached[peer_tree.name] = peer_tree
        s = successful_targets.get(peer_tree.target_label, 0)
        successful_targets[peer_tree.target_label] = s + 1
        for id,child in peer_tree.children.items():
            if self.trim_peer_tree_rec(child, reached,
                                       successful_targets,
                                       target_gupids) is None:
                del peer_tree.children[id]
        return peer_tree
        
    def trim_peer_tree(self, tgt):
        target_gupids = self.list_gupids_rec(tgt, {})
        # trim all nodes that do not appear in targets
        reached = {}
        successful_targets = {}
        peer_tree = self.trim_peer_tree_rec(self.peer_tree,
                                            reached,
                                            successful_targets,
                                            target_gupids)
        self.peer_tree = peer_tree
        self.reached = reached
        self.successful_targets = successful_targets
        self.reset_exec_tree()


    def restore_exec_tree(self, name):
        if self.saved_exec_trees.has_key(name):
            ex,tree = self.saved_exec_trees[name]
            self.stack_exec_trees[0] = (ex,tree)
            self.cur_exec_count = ex
            return 0
        else:
            return -1
        
    def pop_exec_tree(self):
        if len(self.stack_exec_trees) > 1:
            self.stack_exec_trees.pop(0)
            ex,tree = self.stack_exec_trees[0]
            self.cur_exec_count = ex
            return 0                    # OK
        else:
            return -1                   # NG

    def update_last_ok_count(self):
        status = 0
        ok = 0
        for s in self.last_term_status.values():
            if s == 0: ok = ok + 1
            status = max(status, s)
        self.last_ok_count = ok
        return status

    def clear_dirty(self):
        self.dirty = 0

    def set_dirty(self):
        self.dirty = 1

    def save(self, verbosity):
        """
        save session state in a file
        """
        if verbosity >= 2:
            Es(("gxpc: save session created=%d dirty=%d invalid=%d %s\n"
                % (self.created, self.dirty, self.invalid, self.filename)))
        if self.created or self.dirty:
            self.created = 0
            self.invalid = 0
            self.clear_dirty()
            directory,base = os.path.split(self.filename)
            # why _?
            # otherwise it may be found by other processes as
            # session file and deleted as a garbage
            rand_base = "_%s-%d-%d" % (base, os.getpid(),
                                       self.gen_random_id())
            rand_file = os.path.join(directory, rand_base)
            fd = os.open(rand_file,
                         os.O_CREAT|os.O_WRONLY|os.O_TRUNC)
            wp = os.fdopen(fd, "wb")
            # wp = open(self.filename, "wb")
            wp.write("[%s/%s/%s]\n" % \
                     (self.last_ok_count,
                      self.cur_exec_count,
                      self.peer_tree_count))
            pickler.dump(self, wp)
            wp.close()
            os.chmod(rand_file, 0600)
            os.rename(rand_file, self.filename)
            if self.peer_tree is None:
                Es("gxpc: suggest gxpc ping\n")
        else:
            if verbosity >= 2:
                Es("gxpc: clean session not saved\n")


class e_cmd_opts(opt.cmd_opts):
    def __init__(self):
        #             (type, default)
        # types supported
        #   s : string
        #   i : int
        #   f : float
        #   l : list of strings
        #   None : flag
	opt.cmd_opts.__init__(self)
        self.pty      = (None, 0)
        self.up       = ("s*", [])
        self.down     = ("s*", [])
        self.updown   = ("s*", [])
        self.master   = ("s", None)
        # ------ global options that can also be given as e options.
        # default values are in interpreter_opts
        self.withall = (None, 0)
        self.withmask = ("s", None)
        self.withhostmask = ("s", None)
        self.withhostnegmask = ("s", None)
        self.withgupidmask = ("s", None)
        self.withgupidnegmask = ("s", None)
        self.withtargetmask = ("s", None)
        self.withtargetnegmask = ("s", None)
        self.withidxmask = ("s", None)
        self.withidxnegmask = ("s", None)
        self.timeout = ("f", None)
        self.notify_proc_exit = ("i", None)
        self.log_io = ("i", None)
        self.persist = ("i", None)
        self.keep_connection = ("i", None)
        self.tid = ("s", None)
        self.rid = ("s", None)
        # self.dir = ("s", None)
        self.dir = ("s*", [])
        # given as list of var=val, and converted
        # to dictionary in postcheck
        self.export = ("s*", [])
        self.rlimit = ("s*", [])
        # ------
        # self.join = (None, 0)
        # short options
        self.a = "withall"
        self.m = "withmask"
        self.h = "withhostmask"
        self.H = "withhostnegmask"
        self.g = "withgupidmask"
        self.G = "withgupidnegmask"
        self.t = "withtargetmask"
        self.T = "withtargetnegmask"
        self.i = "withidxmask"
        self.I = "withidxnegmask"

    def postcheck_up_or_down(self, arg, F, opt):
        fields = string.split(arg, ":", 1)
        if len(fields) == 1:
            # [ fd ] --> [ fd, fd ]
            fields.append(fields[0])
        [ fd0,fd1 ] = map(lambda x: self.safe_atoi(x, None), fields)
        if fd0 is None or fd1 is None:
            Es(("invalid argument for --%s (%s)."
                " It must be int or int:int (e.g., 3, 3:4)\n" 
                % (opt, arg)))
            return None
        if F.get(fd0) is None:
            F[fd0] = ("--up %s" % arg)
        else:
            Es("gxpc: %s and --up %s is incompatible\n" \
               % (F[fd0], arg))
            return None
        return (fd0, fd1)

    def postcheck_updown(self, arg, F):
        fields = string.split(arg, ":", 2)
        if len(fields) == 1:
            Es(("invalid argument for --updown (%s)."
                " It must be int:int or int:int:cmd "
                "(e.g., 3:4, or '3:4:grep hoge')\n" % arg))
            return None
        elif len(fields) == 2:
            fds = fields
            cmd = None
        else:
            fds = fields[:2]
            [ cmd ] = fields[2:]
        [ fd0,fd1 ] = map(lambda x: self.safe_atoi(x, None), fds)
        if fd0 is None or fd1 is None:
            Es(("invalid argument for --updown (%s)."
                " It must be int:int or int:int:cmd "
                "(e.g., 3:4, '3:4:grep hoge')\n" 
                % arg))
            return None

        if fd0 == fd1:
            Es("gxpc: --updown %s is invalid\n" % arg)
            return None
        for fd in [ fd0, fd1 ]:
            if F.get(fd) is None:
                F[fd] = ("--updown %s" % arg)
            else:
                Es("gxpc: %s and --updown %s is incompatible\n" \
                   % (F[fd], arg))
                return None
        return (fd0, fd1, cmd)
        
    def parse_export(self, export):
        """
        export : list of "var=val"
        """
        env = {}
        for varval in export:
            var_val = string.split(varval, "=", 1)
            if len(var_val) == 1:
                Es(("gxpc: invalid arg to --export (%s). "
                    "It should be var=val\n" % varval))
                return None
            [ var, val ] = var_val
            env[var] = val
        return env

    def postcheck(self):
        # check if updown is a list of int:int
        up = []
        down = []
        updown = []
        if self.pty:
            F = { 0 : "--pty", 1 : "--pty", 2 : "--pty" }
        else:
            F = { 0 : None, 1 : None, 2 : None }
        
        # parse --up
        for arg in self.up:
            x = self.postcheck_up_or_down(arg, F, "up")
            if x is None: return -1
            up.append(x)
        # parse --down
        for arg in self.down:
            x = self.postcheck_up_or_down(arg, F, "down")
            if x is None: return -1
            down.append(x)
        # parse --updown
        for arg in self.updown:
            x = self.postcheck_updown(arg, F)
            if x is None: return -1
            updown.append(x)

        if F[0] is None: down.insert(0, (0, 0))
        if F[2] is None: up.insert(0, (2, 2))
        if F[1] is None: up.insert(0, (1, 1))

        self.up = up
        self.down = down
        self.updown = updown
        # if --withmask is given, use it
        if self.withmask is not None:
            self.withmask = self.safe_atoi(self.withmask,
                                           self.withmask)
        if self.withall:
            self.withmask = None

        self.export = self.parse_export(self.export)
        if self.export is None: return -1
            
        return 0

class hosts_parser_base:
    def __init__(self):
        self.filename = ""
        self.cmd = ""
        self.line_count = 0
        
    def safe_atoi(self, x, defa):
        try:
            return string.atoi(x)
        except ValueError,e:
            return defa

    def parse_error(self):
        Es("%s:%d: parse error in line `%s'\n" % \
           (self.filename, self.line_count, self.line))
        return -1

    def parse_fp(self, fp, filename, cmd, flag):
        hosts = self.hosts.copy()
        self.filename = filename
        self.cmd = cmd
        self.line_count = 0
        eof = 0
        while 1:
            line = fp.readline()
            if line == "":
                eof = 1
                break
            self.line = line
            self.line_count = self.line_count + 1
            if line[0] == "#": continue
            r = self.process_line(line, hosts, flag)
            if r == 1: eof = 1
            if r != 0: break
        r = fp.close()
        if eof == 0: return -1          # parse error (NG)
        if r is not None and r != 0:
            Es("command %s exited abnormally (output ignored)\n" \
               % self.cmd)
            return -1
        self.hosts = hosts
        return 0                    # OK

    def parse_file(self, filename, flag, signal_error):
        fp = None
        try:
            fp = open(filename, "rb")
        except IOError,e:
            if signal_error:
                Es("gxpc: %s: %s\n" % (filename, e.args))
        if fp is not None:
            self.parse_fp(fp, filename, None, flag)

    def parse_pipe(self, cmd, flag):
        fp = os.popen(cmd)
        self.parse_fp(fp, None, cmd, flag)
    
    def parse_args(self, args):
        self.filename = "[cmdarg]"
        self.line_count = 0
        self.line = string.join(args, " ")
        self.process_list(args, self.hosts, 1)

    def parse(self, files, alias_files, cmds, args):
        self.hosts = {}
        for filename in files:
            self.parse_file(filename, 1, 1)
        for filename in alias_files:
            self.parse_file(filename, 0, 0)
        for cmd in cmds:
            self.parse_pipe(cmd, 1)
        self.parse_args(args)
        return self.hosts

class etc_hosts_parser(hosts_parser_base):
    """
    parse a file describing alias relationships
    """
    def process_line(self, line, hosts, flag):
        """
        line is like:
        
           123.456.78.9    hoge.bar.com   hoge

        """
        aliases = []
        for field in string.split(line):
            if field[0] == "#": break
            aliases.append(field)
        return self.process_list(aliases, hosts, flag)

    def process_list(self, args, hosts, flag):
        """
        args : a list of hostnames that appear in a single
        line (e.g., [ "123.456.78.9", "hoge.bar.com", "hoge" ]).
        we consider them to mean the same host.
        """
        L = []
        # list all aliases of all names in the line. i.e.,
        # L =  "123.456.78.9"'s aliases + 
        #      "hoge.bar.com"'s aliases + 
        #      "hoge"'s aliases 
        for a in args:
            for b in hosts.get(a, []):
                if b not in L: L.append(b)
        for a in args:
            if a not in L: L.append(a)
        # record the fact that L are aliases of a.
        for a in L:
            if flag or hosts.has_key(a):
                hosts[a] = L
        return 0

class targets_parser(hosts_parser_base):
    def process_line(self, line, hosts, flag):
        """
        parse a single line like: "istbs010 2"
        """
        line = string.strip(line)
        if line == "": return 1
        fields = string.split(line, None, 1)
        return self.process_list(fields, hosts, flag)
        
    def expand_number_notation_1(self, host):
        # look for istbs[[xxxx]]
        m = re.match("(?P<prefix>.*)\[\[(?P<set>.*)\]\](?P<suffix>.*)", host)
        if m is None: return [ host ]
        prefix = m.group("prefix")
        suffix = m.group("suffix")
        fields = re.split("(,|;|:)",  m.group("set"))
        S = {}
        sign = 1
        for f in fields:
            if f == ",":
                sign = 1
            elif f == ";" or f == ":":
                sign = -1
            else:
                # "10-20" or "10"
                m = re.match("(?P<a>\d+)((?P<m>-+)(?P<b>\d+))?", f)
                if m is None:
                    return None         # parse error
                else:
                    # "10-20" -> a = 10, b = 20
                    # "10"    -> a = 10, b = 10
                    m_str = m.group("m") # -, --, ...
                    a_str = m.group("a")
                    b_str = m.group("b")
                    if b_str is None: b_str = a_str
                    if m_str is None: m_str = "-"
                    a = string.atoi(a_str)
                    if len(m_str) > 1:
                        b = string.atoi(b_str) - 1
                    else:
                        b = string.atoi(b_str)
                    # a-b represents [a,b]. 
                    for x in range(a, b + 1):
                        x_str = "%d" % x
                        if b_str is not None and len(a_str) == len(b_str):
                            x_str = string.zfill(x_str, len(b_str))
                        if sign == 1:
                            S[x_str] = 1
                        else:
                            if S.has_key(x_str):
                                del S[x_str]
        H = []
        for x_str in S.keys():
            for y in self.expand_number_notation_1("%s%s%s" % (prefix, x_str, suffix)):
                H.append(y)
        return H

    def expand_number_notation(self, host):
        H = self.expand_number_notation_1(host)
        H.sort()
        return H

    def add_target_host(self, hosts, host, n):
        # hosts : dictionary hostname -> n
        # host : a hostname regexp that may contain number expansion notation.
        #        e.g., istbs[000-100] -> istbs000 istbs001 ... istbs100
        #        e.g., istbs[[000-100,100-200]] -> istbs000 istbs001 ... istbs100
        for h in self.expand_number_notation(host):
            hosts[h] = hosts.get(h, 0) + n

    def process_list(self, args, hosts, flag):
        """
        args : fields in a line e.g., [ "istbs000", "istbs010", "2" ]
        """
        cur_host = None
        for a in args:
            # is this a number?
            n = self.safe_atoi(a, None)
            if n is None:
                # no -> hostname
                if cur_host is not None:
                    self.add_target_host(hosts, cur_host, 1)
                    # hosts[cur_host] = 1
                cur_host = a
            else:
                # yes -> multiply the previous host by n
                if cur_host is None:
                    return self.parse_error()
                self.add_target_host(hosts, cur_host, n)
                # hosts[cur_host] = n
                cur_host = None
        if cur_host is not None:
            self.add_target_host(hosts, cur_host, 1)
            # hosts[cur_host] = 1
        return 0
        
    
class explore_cmd_opts(opt.cmd_opts):
    """
    gxpc explore
    --hostfile /etc/hosts
    --hostcmd 'ypcat hosts'
    --targetfile file
    --targetcmd 'hogehoge'
    args ...
    """

    def __init__(self):
        #             (type, default)
        # types supported
        #   s : string
        #   i : int
        #   f : float
        #   l : list of strings
        #   None : flag
        opt.cmd_opts.__init__(self)
        self.dry = (None, 0)
        self.default_hostfile = ("s", "/etc/hosts")
        self.aliasfile = ("s*", [ "/etc/hosts" ])
        self.hostfile = ("s*", [])
        self.hostcmd = ("s*", [])
        self.targetfile = ("s*", [])
        self.targetcmd = ("s*", [])
        self.subst_arg = ("s*", [])
        self.python = ("s*", [])

        self.verbosity = ("i", None) # default leave it to inst_local.py
        self.timeout = ("f", None)   # ditto
        self.install_timeout = ("f", None) # ditto
        self.target_prefix = ("s", None)
        self.children_soft_limit = ("i", 10)
        self.children_hard_limit = ("i", 40)
        self.min_wait = ("i", 5)
        self.wait_factor = ("f", 0.2)
        self.reset_default = (None, 0)
        self.set_default = (None, 0)
        self.show_settings = (None, 0)

        self.a = "subst_arg"
        self.h = "hostfile"
        self.t = "targetfile"

    def postcheck(self):
        if self.children_soft_limit < 2:
            Es("gxpc: children_soft_limit must be >= 2\n")
            return -1
        if self.children_hard_limit < 2:
            Es("gxpc: children_hard_limit must be >= 2\n")
            return -1
        S = []
        for a in self.subst_arg:
            var_val = string.split(a, "=", 1)
            if len(var_val) != 2:
                Es("gxpc: bad arg to --subst_arg/-a (%s). use --subst_arg X=Y or -a X=Y\n")
                return -1
            [ var, val ] = var_val
            S.append((string.strip(var), string.strip(val)))
        self.subst_arg = S
        return 0

    def import_defaults(self, default_opts):
        for x in self.__dict__.keys():
            if x == "specified_fields": continue
            if x == "reset_default": continue
            if x == "set_default": continue
            if x == "dry": continue
            if x == "args":
                if len(self.args) == 0 and default_opts is not None:
                    self.args = default_opts.args
            elif not self.specified_fields.has_key(x) and default_opts is not None:
                setattr(self, x, getattr(default_opts, x))
        
class use_cmd_opts(opt.cmd_opts):
    """
    use --delete number
    use --delete method from [to]
    use method from [to]

    """

    def __init__(self):
        #             (type, default)
        # types supported
        #   s : string
        #   i : int
        #   f : float
        #   l : list of strings
        #   None : flag
        opt.cmd_opts.__init__(self)
        self.delete = (None, 0)         # delete flag
        self.as__ = ("s", "")             # --as
        self.d = "delete"

class rsh_cmd_opts(opt.cmd_opts):
    """
    rsh [--full] [name]

    """
    def __init__(self):
        #             (type, default)
        # types supported
        #   s : string
        #   i : int
        #   f : float
        #   l : list of strings
        #   None : flag
        opt.cmd_opts.__init__(self)
        self.full = (None, 0)           # full flag

class interpreter_opts(opt.cmd_opts):
    def __init__(self):
        #             (type, default)
        # types supported
        #   s : string
        #   i : int
        #   f : float
        #   l : list of strings
        #   None : flag
        opt.cmd_opts.__init__(self)
        self.help = (None, 0)
        self.verbosity = ("i", 1)
        self.root_target_name = ("s", None)
        self.target_prefix = ("s", None)
        self.create_session = ("i", 0)
        self.session = ("s", None)
        self.create_daemon = ("i", 0)
        self.daemon = ("s", None)
        
        self.profile = ("s", None)
        self.buffer = (None, 0)
        self.atomicity = ("s", "line")
        self.withall = (None, 0)
        self.withmask = ("s", "0")
        self.withhostmask = ("s", None)  # matches everything
        self.withhostnegmask = ("s", None)  # matches everything
        self.withgupidmask = ("s", None)  # matches everything
        self.withgupidnegmask = ("s", None)  # matches everything
        self.withtargetmask = ("s", None)  # matches everything
        self.withtargetnegmask = ("s", None)  # matches everything
        self.withidxmask = ("s", None)
        self.withidxnegmask = ("s", None)
        self.timeout = ("f", None)
        self.notify_proc_exit = ("i", -1)
        self.log_io = ("i", -1)
        self.persist = ("i", 0)
        # 0 : never, 1 : until exit, 2 : forever
        self.keep_connection = ("i", gxpm.keep_connection_until_fin)
        self.save_session = ("i", 1)
        self.tid = ("s", None)
        # short options
        self.a = "withall"
        self.m = "withmask"
        self.h = "withhostmask"
        self.H = "withhostnegmask"
        self.g = "withgupidmask"
        self.G = "withgupidnegmask"
        self.t = "withtargetmask"
        self.T = "withtargetnegmask"
        self.i = "withidxmask"
        self.I = "withidxnegmask"

    def postcheck(self):
        self.withmask = self.safe_atoi(self.withmask, self.withmask)
        if self.withall: self.withmask = None
        return 0

class texinfo_formatter:
    def begin_format(self, m):
        self.block = "format"
        # return "@sp 1\n@b{%s}\n@format" % m.group()
        return "@vskip 5mm\n@b{%s}\n@format\n" % m.group()

    def begin_example(self, m):
        self.block = "example"
        # return "@sp 1\n@b{%s}\n@example" % m.group()
        return "@vskip 5mm\n@b{%s}\n@example\n" % m.group()

    def begin_table(self, m):
        self.block = "table"
        # return "@sp 1\n@b{%s}\n@table @code" % m.group()
        return "@vskip 5mm\n@b{%s}\n@table @code\n" % m.group()

    def begin_paragraph(self, m):
        # return "@sp 1\n@b{%s}" % m.group()
        return "@vskip 5mm\n@b{%s}\n\n" % m.group()

    def mk_command_name(self, m):
        x = m.group(2)
        y = "@t{%s}" % x
        return re.sub(x, y, m.group())

    def mk_var_name(self, m):
        x = m.group(2)
        y = "@var{%s}" % x
        return re.sub(x, y, m.group())

    def format(self, helps):
        self.block = None
        replace_table = [
            ("Examples:",       self.begin_example),
            ("Usage:",          self.begin_example),
            ("See Also:",       self.begin_example),
            ("Options:",        self.begin_table),
            ("Bugs:",           self.begin_paragraph),
            ("Description:",    self.begin_paragraph),
            ("/etc/hosts",      self.mk_command_name),
            ("gxpc",            self.mk_command_name),
            ("rsh",             self.mk_command_name),
            ("show_cmd",        self.mk_command_name),
            ("parameters",      self.mk_command_name),
            ("--[A-Za-z_]+",    self.mk_command_name),
            ("-[hmt]",          self.mk_command_name),
            # ("-",               self.mk_command_name),
            ("[A-Z][A-Z0-9_]*", self.mk_var_name)
            ]
        for group,help in helps:
            for cmd in group:
                item = (cmd, self.mk_command_name)
                replace_table.append(item)
        table = []
        for pat,repl in replace_table:
            p = ("(\s|\[|\(|=|`|:|;|,|\.|^)"
                 "(%s)"
                 "(\s|\]|\)|=|'|:|;|,|\.|$)") % pat
            table.append((re.compile(p), repl))

        for group,help in helps:
            Ws("@section %s\n" % string.join(group, ", "))
            for line in string.split(help, "\n"):
                for pat,repl in table:
                    while pat.search(line):
                        line = pat.sub(repl, line)
                if self.block == "example?":
                    if line[0:2] == "  ":
                        self.block = "example"
                        # Ws("@sp 1\n")
                        Ws("@vskip 5mm\n@example\n\n")
                    elif line[0:2] == "- ":
                        self.block = "itemize"
                        # Ws("@sp 1\n")
                        Ws("@vskip 5mm\n@itemize\n\n")
                    else:
                        self.block = None
                elif string.strip(line) == "":
                    if self.block is None:
                        self.block = "example?"
                    else:
                        Ws("\n@end %s\n@vskip 5mm\n" % self.block)
                        # Ws("@sp 1\n")
                        self.block = None
                if self.block == "table" and re.match("  @t{--", line):
                    Ws("@item ")
                    Ws(line)
                elif self.block == "itemize" and re.match("-", line):
                    Ws("@item ")
                    Ws(line[1:])
                else:
                    Ws(line)
                Ws("\n")
        if self.block is not None and self.block != "example?":
            Ws("\n@end %s\n" % self.block)

class gxpc_environment:
    """
    Environment variables set for gxp daemons and inherited
    to subprocesses
    """
    def __init__(self, dict):
        self.dict = dict
        for k,v in dict.items():
            setattr(self, k, v)

class explore_logger:
    def __init__(self, filename):
        self.start_t = time.time()
        self.wp = open(filename, "wb")
    def log(self, msg):
        t = time.time() - self.start_t
        self.wp.write("%.3f : %s\n" % (t, msg))
    def close(self):
        self.wp.close()

class cmd_interpreter:
    RET_NOT_RUN = -1
    RET_SIGINT = -2

    RECV_CONTINUE = 0
    RECV_QUIT = 1
    RECV_INTERRUPTED = 2
    RECV_TIMEOUT = 3
    
    def __init__(self):
        self.init_level = 0
        self.gupid = None
        # full path to the socket file to talk to the daemon
        self.daemon_addr = None
        # full path to the session file
        self.session_file = None
        self.session = None
        self.master_pids = []

    def init1(self):
        # defer heavier initializations until really needed
        # (see real_init)
        if self.init_level >= 1: return 0
        self.init_level = 1
        # determine session and daemon
        if self.ensure_gxp_tmp() == -1: return -1
        self.gxp_tmp = self.get_gxp_tmp()
        if self.find_or_create_session() == -1:
            return -1
        assert self.gupid is not None
        assert self.daemon_addr is not None
        assert self.session_file is not None
        return 0

    def init2(self):
        if self.init_level >= 2: return 0
        if self.init1() == -1: return -1
        self.init_level = 2
        # extend environment (is this the right place?)
        env = self.get_gxpc_environment()
        if env is not None:
            os.environ.update(env.dict)
        # Es("gxpc: sys.path = %s\n" % sys.path)
        # sys.path = sys.path[1:] + sys.path[:1]
        # load or make session
        if os.path.exists(self.session_file):
            if self.opts.verbosity>=2:
                Es("gxpc: session file %s found\n" % self.session_file)
            # there appears to be a previously saved session.
            # load it
            self.session = self.load_session(self.session_file, 1)
            if self.session is None:
                Es("gxpc: broken session file %s, "
                   "creating a new session\n" \
                   % (self.session_file))
            else:
                return 0 # OK
        else:
            if self.opts.verbosity>=2:
                Es(("gxpc: session file %s not found, creating a new session\n" 
                    % self.session_file))
        # no previous session. create one.
        self.session = session_state(self.session_file)
        if self.do_ping_cmd(["--quiet"]) == -1:
            return -1                   # NG
        else:
            return 0                    # OK

    def init3(self):
        if self.init_level >= 3: return 0
        if self.init2() == -1: return -1
        self.init_level = 3
        # constants
        self.h_temp = "HEADER len %20d prio %20d HEADER_END"
        self.h_len  = len(self.h_temp % (0,0))
        self.h_pat  = re.compile("HEADER len +(\d+) prio +(\d+) HEADER_END")
        self.t_temp = "TRAIL len %20d prio %20d sum %20d TRAIL_END"
        self.t_len  = len(self.t_temp % (0,0,0))
        self.t_pat  = re.compile("TRAIL len +(\d+) prio +(\d+) sum +(\d+) TRAIL_END")
        # socket and its lock
        self.so = None
        self.so_lock = threading.Lock()
        # clean up session file whose gxpd no longer exists
        # self.cleanup_old_session_file(self.opts.verbosity)

        # state of this particular run
        # termination status of child procs
        # self.term_status = {}
        self.exploring = {}             # hosts being explored
        self.failed_to_explore = []
        self.events = []                # list of upward events
        # specify which output should go which fp
        self.outmap = {}
        # specify which input should go which fd
        self.inmap = {}
        # setup default pipes 0 -> 0, 1 -> 1, 2 -> 2, etc.
        self.setup_default_pipes(self.opts)
        self.notify_proc_exit_fp = None
        self.log_io_fp = None
        
        return 0
        
    def load_session(self, filename, full):
        """
        format of session file
        s e r<newline>
        dump of session object
        """
        try:
            fp = open(filename, "rb")
        except OSError,e:
            Es("%s\n" % (e.args,))
            return None
        m = re.search("(\d+)/(\d+)/(\d+)", fp.readline())
        if m is None: return None
        (ok,exe,all) = map(lambda x: self.safe_atoi(x, None), m.group(1,2,3))
        if ok is None or exe is None or all is None:
            return None
        session = None
        if full:
            try:
                session = pickler.load(fp)
            except pickler.UnpicklingError,e:
                Es("%s\n" % (e.args,))
                fp.close()
                return None
            except AttributeError,e:
                Es("%s\n" % (e.args,))
                fp.close()
                return None
            # to play safe, we assume it is always dirty and clear
            # this only when it is worth doing so (do_count_cmd)
            session.set_dirty()
            session.init_random_generator()
        fp.close()
        return session

    def reload_session(self, filename, full):
        session = self.load_session(filename, full)
        session.last_term_status = self.session.last_term_status
        session.last_exec_tree = self.session.last_exec_tree
        session.cur_exec_count = self.session.cur_exec_count
        session.update_last_ok_count()
        return session

    def ensure_connect_locked(self):
        """
        ensure connection to gxpd.
        """
        if self.so is None:
            if self.opts.verbosity>=2:
                Es("gxpc: connecting to daemon\n")
            daemon_addr_path = os.path.join(self.gxp_tmp, self.daemon_addr)
            # so = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            for i in range(2):
                try:
                    so = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    so.connect(daemon_addr_path)
                    break
                except socket.error,e:
                    Es(("gxpc: warning failed to connect to gxpd (%s), retry %s\n"
                        % (daemon_addr_path, e.args)))
                    so.close()
                    time.sleep(1.0)
            if i > 0:
                Es("gxpc: OK, connected to gxpd\n")
            self.so = so

    def ensure_connect(self):
        self.so_lock.acquire()
        try:
            self.ensure_connect_locked()
        finally:
            self.so_lock.release()

    def disconnect_locked(self):
        if self.so is not None:
            self.so.close()
            self.so = None

    def disconnect(self):
        self.so_lock.acquire()
        try:
            self.disconnect_locked()
        finally:
            self.so_lock.release()
            

    # ---------- find_gxpd and helpers ----------

    def get_user_name(self):
        return os.environ.get("USER", "unknown")

    def get_gxp_tmp(self):
        suffix = os.environ.get("GXP_TMP_SUFFIX", "default")
        u = self.get_user_name()
        d = os.path.join("/tmp", ("gxp-%s-%s" % (u, suffix)))
        return d

    def ensure_gxp_tmp(self):
        d = self.get_gxp_tmp()
        try:
            os.mkdir(d)
            return 0
        except OSError,e:
            pass
        if os.path.isdir(d): return 0
        if os.path.exists(d):
            Es("gxpc: %s exists but is not a directory\n" % d)
            return -1
        Es("gxpc: could not create gxp tmp directory %s %s\n"
           % (d, e.args))
        return -1

    def proc_running(self, pid):
        """
        1 if pid is running
        """
        try:
            os.kill(pid, 0)
            return 1
        except OSError,e:
            return 0

    def safe_stat(self, file):
        try:
            return os.stat(file)
        except OSError,e:
            if e.args[0] == errno.ENOENT:
                return None
            else:
                raise

    def safe_remove(self, filename):
        if os.path.exists(filename):
            try:
                os.remove(filename)
            except OSError,e:
                Es("gxpc: %s %s\n" % (filename, e.args,))
        
    def safe_remove_if_empty(self, filename):
        if os.path.exists(filename) and os.path.getsize(filename) == 0:
            self.safe_remove(filename)
        
    def is_mine(self, file):
        uid = os.getuid()
        st = self.safe_stat(file)
        if st is None: return 0
        owner_uid = st[stat.ST_UID]
        if uid == owner_uid:
            return 1
        else:
            return 0

    def is_my_file(self, file):
        """
        1 if file is socket and the owner is the user
        """
        if self.is_mine(file) == 0: return 0
        st = self.safe_stat(file)
        if st is None: return 0
        mode = st[stat.ST_MODE]
        if stat.S_ISREG(mode):
            return 1
        else:
            return 0

    def is_my_socket(self, file):
        """
        1 if file is socket and the owner is the user
        """
        if self.is_mine(file) == 0: return 0
        st = self.safe_stat(file)
        mode = st[stat.ST_MODE]
        if stat.S_ISSOCK(mode):
            return 1
        else:
            return 0

    def get_gxp_dir(self):
        d,err = this_file.get_this_dir()
        if d is None:
            Es("%s : %s\n" % (self.gupid, err))
            return None
        else:
            return d

    def append_path(self, orig, val):
        if orig == "":
            return val
        else:
            return "%s:%s" % (orig, val)

    def push_path(self, orig, val):
        if orig == "":
            return val
        else:
            return "%s:%s" % (val, orig)

    def get_gxpc_environment(self):
        gxp_dir = self.get_gxp_dir()
        if gxp_dir is None: return None
        prefix,gxp_top = os.path.split(gxp_dir)
        gxpbin_dir = os.path.join(gxp_dir, "gxpbin")
        if 1:
            path = os.environ.get("PATH", "")
            path = self.append_path(path, gxp_dir)
            path = self.append_path(path, gxpbin_dir)
        if 1:
            pypath = os.environ.get("PYTHONPATH", "")
            pypath = self.append_path(pypath, gxp_dir)
            pypath = self.append_path(pypath, gxpbin_dir)
        # including these seem to make gxpc slower in
        # some environments
        return gxpc_environment({ "GXP_DIR"      : gxp_dir,
                                  "GXP_TOP"      : gxp_top,
                                  "GXP_GUPID"    : self.gupid,
                                  "PATH"         : path,
                                  # "PYTHONPATH"   : pypath,
                                   })
    # session management

    def mk_generic_session_file_pattern_regexp(self):
        return re.compile("/(?P<prefix>[Gg]xp-\d+)-session-(?P<gupid>.*-(?P<pid>\d+))-(?P<id>\d+)$")

    def mk_generic_daemon_addr_pattern_regexp(self):
        return re.compile("/(?P<prefix>[Gg]xp-\d+)-daemon-(?P<gupid>.*-(?P<pid>\d+))$")

    def parse_session_filename(self, session_file):
        """
        break session file name (like gxp-xxxxxx-session-monako-tau-..-pid-yyyyyy)
        into components (gxp-xxxxxx, monako-tau-...-pid, pid, yyyyyyyy)
        """
        p = self.mk_generic_session_file_pattern_regexp()
        m = p.search(session_file)
        if m is None: return None,None,None,None
        prefix,gupid,pid,sid = m.group("prefix", "gupid", "pid", "id")
        pid = self.safe_atoi(pid, None)
        if pid is None: return None,None,None,None
        sid = self.safe_atoi(sid, None)
        if sid is None: return None,None,None,None
        return prefix,gupid,pid,sid

    def parse_daemon_addr(self, daemon_addr):
        """
        break daemon addr name (like gxp-xxxxxx-daemon-monako-tau-..-pid)
        into components (gxp-xxxxxx, monako-tau-...-pid, pid)
        """
        p = self.mk_generic_daemon_addr_pattern_regexp()
        m = p.search(daemon_addr)
        if m is None: return None,None,None
        prefix,gupid,pid = m.group("prefix", "gupid", "pid")
        pid = self.safe_atoi(pid, None)
        if pid is None: return None,None,None
        return prefix,gupid,pid

    def mk_daemon_addr(self, prefix, gupid):
        base = "%s-daemon-%s" % (prefix, gupid)
        return os.path.join(self.gxp_tmp, base)

    def mk_stdout_file(self, prefix, gupid):
        base = "%s-stdout-%s" % (prefix, gupid)
        return os.path.join(self.gxp_tmp, base)

    def mk_stderr_file(self, prefix, gupid):
        base = "%s-stderr-%s" % (prefix, gupid)
        return os.path.join(self.gxp_tmp, base)

    def generate_session_filename_of_daemon_addr(self, daemon_addr):
        """
        given daemon_addr, generate a random session filename for it
        """
        # gxp-xxxxxxxx-daemon-... -> gxp-xxxxxxxx-session-... 
        prefix,gupid,_ = self.parse_daemon_addr(daemon_addr)
        assert prefix, deamon_addr
        session_prefix = ("%s-session-%s" % (prefix, gupid))
        if self.opts.create_session > 0 or self.opts.create_daemon > 0:
            # gxp-xxxxxxxx-session-... -> Gxp-xxxxxxxx-session-... 
            session_prefix = session_prefix.replace("gxp-", "Gxp-")
        id = random.randint(0, 99999999)
        base = "%s-%08d" % (session_prefix, id)
        return os.path.join(self.gxp_tmp, base)

    def find_session_files_of_daemon_addr(self, daemon_addr):
        """
        given gupid, return session files that may be associated
        with it.
        """
        assert (daemon_addr[0] == "/"), daemon_addr
        daemon_addr_base = os.path.basename(daemon_addr)
        prefix,gupid,_ = self.parse_daemon_addr(daemon_addr_base)
        session_pat = ("%s-session-%s-*" % (prefix, gupid))
        glob_pat = os.path.join(self.gxp_tmp, session_pat)
        return glob.glob(glob_pat)

    def generate_prefix(self):
        """
        generate the first two components of various files
        (daemon addr, session file, stdout, stderr file)
        gxp-xxxxxxxx or Gxp-xxxxxxxx
        """
        id = random.randint(0, 99999999)
        if self.opts.create_session > 0 or self.opts.create_daemon > 0:
            return "Gxp-%08d" % id
        else:
            return "gxp-%08d" % id

    def mk_specified_session_file_pattern_str(self):
        """
        return user-specified session pattern
        """
        # user-specified session pattern
        # 1. command line (--session)
        # 2. environment variable (GXP_SESSION)
        # 3. default is match always (.*)
        opts = self.opts
        if opts.session is not None:
            if opts.verbosity >= 2:
                Es("gxpc: --session %s given\n" % opts.session)
            return opts.session
        # check env
        env_session = os.environ.get("GXP_SESSION")
        if env_session is not None:
            if opts.verbosity >= 2:
                Es("gxpc: --session not given, use GXP_SESSION (%s)\n" 
                   % env_session)
            return env_session
        # default
        assert (opts.create_session <= 0), opts.create_session
        assert (opts.create_daemon <= 0), opts.create_daemon
        default_session_pat = os.path.join(self.gxp_tmp, "gxp-\d+-session-.*")
        if opts.verbosity >= 2:
            Es("gxpc: neither --session nor GXP_SESSION given, "
               "use default (%s)\n" % default_session_pat)
        return default_session_pat

    def mk_specified_session_file_pattern_regexp(self):
        """
        return a reg exp object to search for session files,
        depending on --session or GXP_SESSION
        """
        opts = self.opts
        session_pat_str = self.mk_specified_session_file_pattern_str()
        # compile user specified pattern
        if opts.verbosity >= 2:
            Es("gxpc: session pattern to search for is %s\n" % session_pat_str)
        try:
            return re.compile(session_pat_str)
        except ValueError,e:
            Es("gxpc: %s %s\n" % (session_pat_str, e.args))
            return None
        
    def mk_specified_daemon_addr_pattern_str(self):
        opts = self.opts
        if opts.daemon is not None:
            if opts.verbosity >= 2:
                Es("gxpc: --daemon %s given\n" % opts.daemon)
            return opts.daemon
        env_daemon = os.environ.get("GXP_DAEMON")
        if env_daemon is not None:
            if opts.verbosity >= 2:
                Es("gxpc: --daemon not given, use GXP_DAEMON (%s)\n" 
                   % env_daemon)
            return env_daemon
        assert (opts.create_daemon <= 0), opts.create_daemon 
        default_daemon_pat = os.path.join(self.gxp_tmp, "[Gg]xp-\d+-daemon-.*")
        if opts.verbosity >= 2:
            Es("gxpc: neither --daemon nor GXP_DAEMON given, "
               "use default (%s)\n" % default_daemon_pat)
        return default_daemon_pat

    def mk_specified_daemon_addr_pattern_regexp(self):
        """
        return a reg exp object to search for daemon addrs,
        depending on --daemon or GXP_DAEMON
        """
        opts = self.opts
        daemon_pat_str = self.mk_specified_daemon_addr_pattern_str()
        if opts.verbosity >= 2:
            Es("gxpc: daemon addr pattern to search for is %s\n" % daemon_pat_str)
        try:
            return re.compile(daemon_pat_str)
        except ValueError,e:
            Es("gxpc: wrong daemon address pattern %s %s\n" 
               % (daemon_pat_str, e.args))
            return None

    def daemon_running(self, daemon_addr, gupid, pid):
        """
        check if daemon gupid is really running.
        - process pid exists
        - its socket file 
        (/tmp/gxp-$USER-default/gxp-xxxxxxxx-daemon-*) exists
        """
        opts = self.opts
        if opts.verbosity >= 2:
            Es("gxpc: checking if daemon %s (pid=%d) is alive\n" 
               % (gupid, pid))
        if self.proc_running(pid) == 0:
            if opts.verbosity >= 2:
                Es("gxpc: no, process %d not alive\n" % pid)
            return -1           # dead and daemon_addr should be removed
        if opts.verbosity >= 2:
            Es("gxpc: yes, process %d is alive\n" % pid)
            Es("gxpc: checking if %s is a socket\n" % daemon_addr)
        if not self.is_my_socket(daemon_addr):
            if opts.verbosity >= 2:
                Es("gxpc: no, %s does not exist or is not a socket\n" 
                   % daemon_addr)
            return 0            # daemon_addr is not a socket
        if opts.verbosity >= 2:
            Es("gxpc: yes, %s is a socket\n" % daemon_addr)
        return 1

    def find_sessions(self):
        """
        Find session files matching the requested pattern.
        session file name is

          gxpsession-hostname-user-yyyy-mm-dd-hh-mm-ss-pid-random

        Along the way, we make sure the daemon is actually running
        by checking pid and its socket file. Always return a session
        whose daemon seems really alive.

        """
        opts = self.opts
        assert (opts.create_daemon <= 0), opts.create_daemon
        assert (opts.create_session <= 0), opts.create_session
        generic_pat = self.mk_generic_session_file_pattern_regexp()
        user_pat = self.mk_specified_session_file_pattern_regexp()
        daemon_pat = self.mk_specified_daemon_addr_pattern_regexp()
        # now search for files
        sessions = []
        if opts.verbosity >= 2:
            Es("gxpc: searching directory %s ...\n" % self.gxp_tmp)
        for session_base in os.listdir(self.gxp_tmp):
            session = os.path.join(self.gxp_tmp, session_base)
            # filter out those not matching against the user-specified pattern
            if opts.verbosity >= 2:
                Es("gxpc: matching %s to %s\n" % (session, user_pat.pattern))
            m = user_pat.search(session)
            if m is None: 
                if opts.verbosity >= 2:
                    Es("gxpc: %s does not match %s\n" 
                       % (session, user_pat.pattern))
                continue
            # match against the common pattern and get gupid/pid part
            if opts.verbosity >= 2:
                Es("gxpc: %s matches %s, extracting daemon_addr, gupid\n" 
                   % (session, user_pat.pattern))
            prefix,gupid,pid,sid = self.parse_session_filename(session)
            if prefix is None:
                Es("gxpc: %s does not match the generic pattern "
                   "of session files, ignored \n" % session)
                continue
            # match extacted daemon part to the specified daemon address
            daemon_addr = self.mk_daemon_addr(prefix, gupid)
            if opts.verbosity >= 2:
                Es("gxpc: checking daemon addr %s\n" % daemon_addr)
            if daemon_pat.search(daemon_addr) is None:
                if opts.verbosity >= 2:
                    Es("gxpc: %s does not match %s\n"
                       % (daemon_addr, daemon_pat.pattern))
                continue
            # check if daemon is really alive
            live = self.daemon_running(daemon_addr, gupid, pid)
            if live == -1:
                # session exists, but the process seems gone.
                # cleanup garbages
                self.cleanup_daemon_files(daemon_addr)
                self.cleanup_session_file(session)
                # Es("gxpc: cleanup session file %s\n" % session)
            elif live == 1:
                sessions.append((gupid, daemon_addr, session))
        return sessions
            
    def requested_session(self):
        ses = self.opts.session
        if ses: return ses
        ses = os.environ.get("GXP_SESSION")
        if ses: return ses
        return None

    def requested_daemon_addr(self):
        dmn = self.opts.daemon
        if dmn: return dmn
        dmn = os.environ.get("GXP_DAEMON")
        if dmn: return dmn
        return None

    def find_or_create_session(self):
        """
        Find or create a session to attach to.
        It is determined by several factors.
        (1) command line option (--session) specifying it
        (2) environment variable (GXP_SESSION) specifying it
        (3) command line option (--create_session and
            --create_daemon) specifying when to create a new session.
              -1 : never create
               0 : create in some cases (see below)
               1 : always create
        If create_session is not 1, first searches for file
                /tmp/gxp-$USER/gxpsession-*
        If create_session is 1, it behaves below as if no
        such files are found. 
        For each file, it checks if the name matches the pattern
        given by --session or GXP_SESSION.
        If mulitple files match -> error.
        If exactly one file matches -> OK, return it.
        If none matches -> it creates a new session only if
        neither --session nor GXP_SESSION are given. If one is given,
        it immediately reports an error saying found no matching
        session.

        To create a new session, we first try to find a running
        daemon according to the specified pattern (--daemon or
        GXP_DAEMON). If not found, whether or not we create a
        new daemon is determined by other factors (--create_daemon,
        GXP_DAEMON, --create_daemon).
                  
        """
        opts = self.opts
        if opts.verbosity >= 2:
            Es("gxpc: find or create session "
               "(create_daemon=%d, create_session=%d)\n" 
               % (opts.create_daemon, opts.create_session))
        if opts.create_daemon > 0 or opts.create_session > 0:
            # pretend that no session files are found
            sessions = []
        else:
            # look for specified /tmp/gxp-$USER/gxp-xxxxxxxx-session-* files
            sessions = self.find_sessions()
            # regexp error
            if sessions is None: return -1

        if len(sessions) > 1:
            # multiple sessions found --> always an error
            if opts.session is None:
                Es("gxpc: there are multiple sessions. "
                   "use environment variable GXP_SESSION or "
                   "option --session to specify one\n")
            else:
                Es("gxpc: there are multiple sessions that "
                   "matched the requested pattern (%s). "
                   % opts.session)
            # show all sessions that matched
            for gupid,daemon_addr,session_file in sessions:
                Es("%s\n" % session_file)
            return -1

        if len(sessions) == 1:
            # happy case. we found exactly one. 
            gupid,daemon_addr,session_file = sessions[0]
            self.gupid = gupid
            self.daemon_addr = daemon_addr
            self.session_file = session_file
            if opts.verbosity >= 2:
                Es("gxpc: OK, session found (%s)\n" \
                   % self.session_file)
            return 0

        ses = self.requested_session()
        if ses:
            # none found, but a pattern is specified either by
            # --session or GXP_SESSION -> NOT FOUND error
            Es("gxpc: no session matching the requested pattern (%s)\n" 
               % ses)
            return -1

        # none found, and 
        # neither --session nor GXP_SESSION specified
        # -> see if we should create one
        if opts.create_session < 0:
            # session not found, and command says dont create
            Es("gxpc: no session\n")
            return -1
        # --create_session >= 0 (you MAY create one)
        # --> create
        if opts.verbosity >= 2:
            Es("gxpc: no session found, try to create one\n")
        if self.find_or_create_daemon() == -1: return -1
        assert self.gupid is not None
        assert self.daemon_addr is not None
        if opts.verbosity >= 2:
            Es("gxpc: daemon found (%s), create session"
               " for it\n" % self.gupid)
        session_file = self.generate_session_filename_of_daemon_addr(self.daemon_addr)
        self.session_file = session_file
        if opts.verbosity >= 2:
            Es("gxpc: new session %s\n" % self.session_file)
        return 0

    def find_or_create_daemon(self):
        """
        Find or create a session to attach to.
        It is determined by several factors.
        (1) command line option (--daemon) specifying it
        (2) environment variable (GXP_DAEMON) specifying it
        (3) command line option (--create_daemon) specifying
        when to create a new daemon.
              -1 : never create
               0 : create in some cases (see below)
               1 : always create
        If create_daemon is not 1, first searches for file
                /tmp/gxp-$USER/gxpd-*
        If create_daemon is 1, it behaves below as if no
        such files are found. 
        For each file, it checks if the name matches the pattern
        given by --daemon or GXP_DAEMON.
        If mulitple files match -> error.
        If exactly one file matches -> OK, return it.
        If none matches -> it creates a new daemon only if
        neither --daemon nor GXP_DAEMON are given. If one is given,
        it immediately reports an error saying found no matching
        daemon.

        Upon success, we set gupid, daemon_addr fields.

        """
        opts = self.opts
        if opts.verbosity >= 2:
            Es("gxpc: find or create daemon (create_daemon=%d)\n"
               % opts.create_daemon)
        if opts.create_daemon > 0:
            # pretend as if no socket files are found
            addrs = []
        else:
            # search for specified /tmp/gxp-$USER/gxpd-* files
            addrs = self.find_daemon_addrs(None)
            # regexp error
            if addrs is None: return -1

        if len(addrs) > 1:
            # multiple daemon socket files found -> error
            Es("gxpc: there are multiple daemons that "
               "matched the requested pattern (%s). "
               "use environment variable GXP_DAEMON or "
               "option --daemon to specify one\n"
               % opts.daemon)
            return -1

        if len(addrs) == 1:
            # exactly one found -> OK
            gupid,daemon_addr = addrs[0]
            self.gupid = gupid
            self.daemon_addr = daemon_addr
            if opts.verbosity >= 2:
                Es("gxpc: OK, daemon found (%s)\n" % self.gupid)
            return 0

        dmn = self.requested_daemon_addr()
        if dmn:
            # none found, but a pattern is specified either by
            # --daemon or GXP_DAEMON -> NOT FOUND error
            Es("gxpc: no daemon matching the requested pattern (%s)\n" 
               % dmn)
            return -1

        # none found, and neither --daemon nor GXP_DAEMON specified,
        # -> see if we should create one
        if opts.create_daemon < 0:
            Es("gxpc: no daemon\n")
            return -1
        # --create_daemon >= 0 (you MAY create one)
        # -> create one
        if opts.verbosity >= 1:
            Es("gxpc: no daemon found, create one. "
               "'gxpc quit' to clean up. "
               "'gxpc help' to get help.\n")
        pat = self.really_create_daemon()
        if opts.verbosity >= 2:
            Es("gxpc: created, check it again\n")
        addrs = self.find_daemon_addrs(pat)
        if len(addrs) == 1:
            gupid,daemon_addr = addrs[0]
            self.gupid = gupid
            self.daemon_addr = daemon_addr
            if opts.verbosity >= 2:
                Es("gxpc: daemon successfully brought "
                   "up (%s)\n" % self.gupid)
            return 0
        else:
            assert (len(addrs) == 0), addrs
            Es("gxpc: failed to bring up daemon\n")
            return -1

    def find_daemon_addrs(self, pat):
        """
        find files that match pat under gxp_tmp dir
        """
        opts = self.opts
        generic_pat = self.mk_generic_daemon_addr_pattern_regexp()
        if pat is None:
            # no pattern is given. use one specified in
            # the command line or GXP_DAEMON
            daemon_pat = self.mk_specified_daemon_addr_pattern_regexp()
            if daemon_pat is None: return None
        else:
            daemon_pat = pat
        if opts.verbosity >= 2:
            Es("gxpc: searching directory %s ...\n" % self.gxp_tmp)
        addrs = []
        # daemon is actually a name of a unix-domain socket file
        for addr_base in os.listdir(self.gxp_tmp):
            addr = os.path.join(self.gxp_tmp, addr_base)
            # match against the user-specified or newly created pattern 
            if opts.verbosity >= 2:
                Es("gxpc: matching %s to %s\n" % (addr, daemon_pat.pattern))
            m = daemon_pat.search(addr)
            if m is None: 
                if opts.verbosity >= 2:
                    Es("gxpc: %s does not match %s\n" 
                       % (addr, daemon_pat.pattern))
                continue
            # match against the common pattern and get gupid/pid part
            if opts.verbosity >= 2:
                Es("gxpc: %s matches %s, extracting daemon_addr, gupid\n" 
                   % (addr, generic_pat.pattern))
            prefix,gupid,pid = self.parse_daemon_addr(addr)
            if prefix is None: 
                Es("gxpc: %s does not match the generic pattern "
                   "of daemon addrs, ignored \n" % addr)
                continue
            # here we know addr matches pattern
            live = self.daemon_running(addr, gupid, pid)
            if live == -1:      # deamon seems gone.
                self.cleanup_daemon_files(addr)
            elif live == 1:
                addrs.append((gupid, addr))
        return addrs
        
    def really_create_daemon(self):
        """
        really create daemon
        """
        prefix = self.generate_prefix()
        pid = os.fork()
        if pid == 0:
            gxp_dir = self.get_gxp_dir()
            inst_local_py = os.path.join(gxp_dir, "inst_local.py")
            os.setpgrp()
            # os.close(0)
            argv = [ sys.executable, inst_local_py,
                     "--dont_wait",
                     "--seq", "explore-root-gxpd",
                     "--rsh", "sh",
                     "--rsh", "-c",
                     "--rsh", "%(cmd)s" ]
            # target_prefix
            tpx = self.opts.target_prefix
            if tpx is not None:
                argv = argv + [  "--target_prefix", tpx ]
            # args passed to gxpd
            argv = argv + [ "--first_args_template",  "--remove_self",
                            "--first_args_template",  "--continue_after_close",
                            "--first_args_template",  "--name_prefix",
                            "--first_args_template",  prefix,
                            "--second_args_template", "--remove_self",
                            "--second_args_template", "--continue_after_close",
                            "--second_args_template", "--remove_self",
                            "--second_args_template", "--continue_after_close",
                            "--second_args_template", "--name_prefix",
                            "--second_args_template", prefix,
                            ]
            rtn = self.opts.root_target_name
            if rtn is not None:
                argv = argv + [ "--first_args_template",  "--target_label",
                                "--first_args_template",  rtn,
                                "--second_args_template", "--target_label",
                                "--second_args_template", rtn ]
            if self.opts.verbosity >= 2:
                Es("gxpc: execvp(%s, %s)\n" % (argv[0], argv))
            os.execvp(argv[0], argv)
        else:
            os.waitpid(pid, 0)
            base = "%s-daemon" % prefix
            return re.compile(os.path.join(self.gxp_tmp, base))
        
    def cleanup_daemon_files(self, daemon_addr):
        """
        cleanup files of apparantly dead daemons.
        - gxpd-[gupid] socket file
        - gxpout-[gupid] stdout
        - gxperr-[gupid] stderr
        """
        if self.opts.verbosity >= 2:
            Es("gxpc: clean up daemon files for %s\n" % daemon_addr)
        prefix,gupid,pid = self.parse_daemon_addr(daemon_addr)
        assert prefix, daemon_addr
        out_file = self.mk_stdout_file(prefix, gupid)
        err_file = self.mk_stderr_file(prefix, gupid)
        self.safe_remove(daemon_addr)
        self.safe_remove_if_empty(out_file)
        self.safe_remove_if_empty(err_file)

    def cleanup_session_file(self, session_file):
        if self.opts.verbosity >= 2:
            Es("gxpc: clean up session file %s\n" % session_file)
        self.safe_remove(session_file)

    # ------------- handle events from gxpd -------------

    def safe_write(self, fp, s):
        try:
            fp.write(s)
            if self.opts.buffer == 0: fp.flush()
        except IOError,e:
            if e.args[0] == errno.EPIPE:
                return -1
            else:
                raise
        return 0

    def safe_close(self, fp):
        try:
            # since python insists keeping stdout/err open
            if fp is not sys.stdout and fp is not sys.stderr:
                fp.close()
                # the following does not work and get a strange msg
                # close failed: [Errno 9] Bad file descriptor LATER
                # (not immediately). perhaps it is a msg from
                # python finalizer?
                # os.close(fp.fileno())
        except IOError,e:
            if e.args[0] == errno.EPIPE:
                return -1
            else:
                raise
        return 0

    def handle_event_info(self, gupid, tid, ev):
        # ask where info should go
        if self.opts.verbosity>=2:
            Es("gxpc: handle_event_info(%s, %s, ev.msg=%s)\n" \
               % (gupid, tid, ev.msg))
        fp,_ = self.outmap.get("info")
        if fp is None: return 0
        if ev.msg != "":
            if self.safe_write(fp, ev.msg) == -1:
                return -1
        return 0

    def handle_event_info_pong(self, gupid, tid, ev):
        return self.handle_event_info(gupid, tid, ev)

    def handle_event_io(self, gupid, tid, ev):
        if self.opts.verbosity>=2:
            Es("gxpc: handle_event_io(%s,%s,ev.src=%s,ev.kind=%s,ev.rid=%s,ev.pid=%s,ev.fd=%s,ev.payload=%s)\n" \
               % (gupid, tid, ev.src, ev.kind, ev.rid, ev.pid, ev.fd, ev.payload))
        # this must match constants in ioman.ch_event
        OK = 0                              # got OK data
        IO_ERROR = -1                       # got IO error
        TIMEOUT = -2                        # got timeout
        EOF = -3                            # got EOF
        if ev.fd is None:
            Es("gxpc: handle_event_io(%s,%s,ev.src=%s,ev.fd=%s,ev.kind=%s,ev.payload=%s,err_msg=%s)\n" \
               % (gupid, tid, ev.src, ev.fd, ev.kind, ev.payload, ev.err_msg))
        # fp,co = self.outmap[ev.fd]
        fp,co = self.outmap.get(ev.fd, (None, None))
        if 1 and ev.src == "proc" and self.log_io_fp:
            evs = gxpm.unparse(ev)
            self.safe_write(self.log_io_fp, "%9d %s" % (len(evs), evs))
        if ev.payload != "":
            if ev.src == "proc":
                # self.event_log.append((time.time(), ev.payload))
                # if fp is None: Es("what?\n")
                # Es("fp=%s, payload=%s\n" % (fp, ev.payload))
                if 0 and self.log_io_fp:
                    evs = gxpm.unparse(ev)
                    self.safe_write(self.log_io_fp, "%9d %s" % (len(evs), evs))

                if fp is not None and \
                       self.safe_write(fp, ev.payload) == -1:
                    # Es("got epipe!\n")
                    self.outmap[ev.fd] = None,None
                    tgt = self.session.last_exec_tree
                    rid = None          # all
                    self.send_action(tgt, tid, gxpm.action_close(rid, ev.fd),
                                     0, 0)
                    return -1
            else:                       # peer
                h = self.session.reached[gupid]
                if h.children.has_key(ev.rid):
                    tgt = h.children[ev.rid].target_label
                else:
                    tgt = ev.rid
                msg = ("%s heard from %s : %s" % (gupid, tgt,
                                                  ev.payload))
                if fp is not None and \
                   self.safe_write(fp, msg) == -1:
                    self.outmap[ev.fd] = None,None
                    return -1
        if ev.kind == EOF or ev.kind == IO_ERROR:
            if co and co.decrement() == 1 and self.opts.persist == 0:
                self.outmap[ev.fd] = None,None
                if fp is not None and self.safe_close(fp) == -1:
                    return -1
        return 0

    def handle_event_die(self, gupid, tid, ev):
        if self.opts.verbosity>=2:
            Es("gxpc: handle_event_die(%s, %s, ev.status=%s, ev.rusage=%s, ev.time_start=%s, ev.time_end=%s)\n" \
               % (gupid, tid, ev.status, ev.rusage, ev.time_start, ev.time_end))
        # Ws("shindayo\n")
        self.session.last_term_status[gupid] = int(ev.status)
        if self.notify_proc_exit_fp:
            # self.opts.notify_proc_exit > 0
            s = (gupid, tid, ev.src, ev.rid, ev.pid, ev.status, ev.rusage, ev.time_start, ev.time_end)
            self.safe_write(self.notify_proc_exit_fp, "%s\n" % (s,))
            # ("%s %s %s %s %s %s %s\n"
            # % (gupid, tid, ev.src, ev.rid, ev.pid, ev.status, ev.rusage))

    def handle_event_peerstatus(self, gupid, tid, ev):
        if self.opts.verbosity>=2:
            Es(("handle_event_peerstatus(%s,%s,%s,%s,"
                "ev.parent_name=%s,ev.rid=%s)\n" % 
                (ev.peername, ev.target_label, ev.hostname, 
                 ev.status, ev.parent_name, ev.rid)))
        session = self.session
        reached = session.reached
        successful_targets = session.successful_targets
        t = reached[ev.parent_name]
        if not t.children.has_key(ev.rid):
            Es("gxpc: warning: ignore late status from %s "
               "(status=%s, parent=%s)\n" % \
               (ev.peername, ev.status, ev.parent_name))
            return
        c = t.children[ev.rid]
        target_label = c.target_label
        # Ws("del %s\n" % c)
        del self.exploring[c]
        fp,_ = self.outmap.get("explore")
        assert fp is not None
        if ev.status == "OK":
            c.name = ev.peername
            c.hostname = ev.hostname
            assert c.target_label == ev.target_label, \
                (c.target_label == ev.target_label)
            reached[ev.peername] = c
            s = successful_targets.get(target_label, 0)
            successful_targets[target_label] = s + 1
            self.safe_write(fp, ("reached : %s (%s)\n" % 
                                 (target_label, ev.hostname)))
        else:
            self.safe_write(fp, ("failed  : %s (%s) <- %s\n" % 
                                 (target_label, ev.hostname, ev.parent_name)))
            assert ev.status == "NG", result
            del t.children[ev.rid]
            self.failed_to_explore.append(target_label)

    def handle_event_fin(self, gupid, tid, ev):
        pass

    def get_num_execs(self, exec_tree):
        if exec_tree.num_execs != None:
            return exec_tree.num_execs
        else:
            for child in exec_tree.children:
                num_execs = self.get_num_execs(child)
                if num_execs != None:
                    return num_execs
            return None

    def handle_event_invalidate_view(self, gupid, tid, ev):
        if self.opts.verbosity >= 2:
            Es("gxpc: received invalidate view\n")
        self.session.invalid = 1

    # ---------- send and recv stuff ----------

    def asend_locked(self, str):
        """
        assume connectin is established.
        send str to gxpd.
        """
        so = self.so
        if so is None: 
            if self.opts.verbosity>=2:
                Es("gxpc: couldn't send %d bytes to daemon because socket is already closed\n" % len(str))
            return -1
        if 1:
            msg = (self.h_temp % (len(str), 0)) + str + (self.t_temp % (len(str), 0, 0))
            so.send(msg)
        else:
            so.send(self.h_temp % (len(str), 0))
            so.send(str)
            so.send(self.t_temp % (len(str), 0, 0))
        if self.opts.verbosity>=2:
            Es("gxpc: sent %d bytes to daemon\n" % len(str))
        return 0

    def asend(self, str):
        self.so_lock.acquire()
        try:
            self.asend_locked(str)
        except socket.error,e:
            self.so_lock.release()
            if e.args[0] == errno.EPIPE \
               or e.args[0] == errno.ECONNRESET \
               or e.args[0] == errno.EBADF: # closed just before
                if self.opts.verbosity>=2:
                    Es("gxpc: warning: send to daemon got error %s\n" % (e.args,))
                return -1
            else:
                raise
        self.so_lock.release()
        return 0

    def time_limit_to_out(self, time_limit):
        if time_limit is None: return None
        to = time_limit - time.time()
        # we never set timeout to be less than 0.5.
        # this means if we keep receiving something in <0.5sec interval,
        # we never quit
        if to < 0.0: return 0.5
        return to
        
    def timeout_to_limit(self, timeout):
        if timeout is None: return None
        return timeout + time.time()
        
    def recv_msg_from_daemon(self, so):
        """
        receive a msg from daemon (assuming there is one ready).
        return None if it gets EOF.
        return string otherwise.
        """
        hd = so.recv(self.h_len)
        if hd == "": return None
        m = self.h_pat.match(hd)
        assert m is not None, hd
        sz = int(m.group(1))
        assert sz > 0, sz
        remain_sz = sz
        bodies = []
        while remain_sz > 0:
            body = so.recv(remain_sz)
            if body == "":
                Es("premature EOF\n")
                return None
            bodies.append(body)
            remain_sz = remain_sz - len(body)
        tr = so.recv(self.t_len)
        if len(tr) < self.t_len:
            Es("expected %d bytes, only got [%s]\n" % \
               (self.t_len, len(tr)))
            return None
        return string.join(bodies, "")
        
    def process_msg_from_daemon(self, so):
        """
        recv msg from the daemon and and process it
        return 0 if it gets EOF.
        return 1 otherwise.
        """
        msg = self.recv_msg_from_daemon(so)
        if msg is None:
            if self.opts.verbosity>=2:
                Es("gxpc: got EOF from daemon\n")
            # gxpd closed the connection to me. quit
            self.disconnect()
            return 0
        # Es("process_msg_from_daemon -> %d bytes\n" % len(msg))
        # parse msg to build a structure
        m = gxpm.parse(msg)
        assert (isinstance(m, gxpm.up) or isinstance(m, gxpm.syn)), m
        if self.opts.verbosity>=2:
            Es("gxpc: got %s\n" % m.event)
        # record it if interesting
        if self.events is not None:
            if self.opts.verbosity>=2:
                Es("gxpc: got event %s %s %s\n" 
                   % (m.gupid, m.tid, m.event))
            self.events.append((m.gupid, m.tid, m.event))
        # event type
        # Es("process_msg_from_daemon -> class = %s\n" %  m.event.__class__.__name__)
        h = "handle_%s" % m.event.__class__.__name__
        method = getattr(self, h)
        # call one of handle_xxx methods
        method(m.gupid, m.tid, m.event)
        return 1
        
    def process_data_from_fd(self, fd, tgt, tid):
        """
        get some data from an input file descriptor and
        process it.
        """
        max_read_gran = 8 * 1024
        target_fd = self.inmap[fd]
        payload = os.read(fd, max_read_gran)
        if self.opts.verbosity>=2:
            Es("gxpc: got %d bytes from fd=%d (target_fd=%d)\n" 
               % (len(payload), fd, target_fd))
        if payload == "": del self.inmap[fd]
        rid = None                  # all
        act = gxpm.action_feed(rid, target_fd, payload)
        x = self.send_action(tgt, tid, act, 0, 0)
        if x is None: return 0
        return 1

    def process_an_event(self, tgt, tid, time_limit):
        """
        receive an event. event is either receiving a message
        from the daemon it connects to or receiving data from
        one of its incoming file descriptors.  it is a result
        of merging recv_once and forwarder.
        """
        # assert tgt or (len(self.inmap) == 0)
        if self.so is None: 
            if self.opts.verbosity>=2:
                Es("gxpc: process_an_event: socket to deamon closed\n")
            return 0
        R_ = self.inmap.keys()
        R_.append(self.so)
        to = self.time_limit_to_out(time_limit)
        # Es("process_an_event %d input fds, socket is %s, timeout = %s\n"
        # % (len(self.inmap), self.so, to))
        if self.opts.verbosity>=2:
            Es("gxpc: process_an_event: wait for an event (timeout = %s)\n" % to)
        if to is None:
            R,_,_ = select.select(R_, [], [])
        else:
            R,_,_ = select.select(R_, [], [], to)
        for r in R:
            if r is self.so:
                self.process_msg_from_daemon(r)
            else:
                self.process_data_from_fd(r, tgt, tid)
        return 1

    def process_events_loop(self, tgt, tid, n_break_exploring, time_limit):
        # assert tgt or (len(self.inmap) == 0)
        try:
            while 1:
                if self.opts.verbosity>=2:
                    Es("gxpc: process_events_loop exploring=%d, n_break_exploring=%s, time_limit=%s\n"
                       % (len(self.exploring), n_break_exploring, time_limit))
                if len(self.exploring) <= n_break_exploring: 
                    return cmd_interpreter.RECV_CONTINUE
                if self.process_an_event(tgt, tid, time_limit) == 0: 
                    return cmd_interpreter.RECV_QUIT
                if (time_limit is not None) and (time.time() > time_limit):
                    return cmd_interpreter.RECV_TIMEOUT
        except KeyboardInterrupt,e:
            return cmd_interpreter.RECV_INTERRUPTED

    def send_action(self, tgt, tid, act, persist, keep_connection):
        """
        do action (act) on some nodes.
        
        return (term_status,out_list):

         term_status : dictionary of termination status
         out_list    : output list
        
        """
        if self.opts.verbosity>=2:
            Es("gxpc: send action %s to daemon persist=%d keep_connection=%d\n"
               % (act, persist, keep_connection))
        # clauses = [ gxpm.clause(".*", [ act ]) ]
        clauses = { None : [ act ] }
        gcmds = [ clauses ]
        peer_tree = self.session.peer_tree

        if tgt is None: return None
        if tid is None:
            tid = "t%s" % self.session.gen_random_id()
        self.ensure_connect()
        m = gxpm.down(tgt, tid, persist, keep_connection, gcmds)
        msg = gxpm.unparse(m)
        if self.asend(msg) == -1:
            return None
        else:
            return tid

    # 
    # ---------- helper functions for showing help ----------
    #

    def generic_help(self, cmd):
        Es("Try `gxpc help %s' for more information.\n" % cmd)
        
    #
    # help
    #
    def show_help_summary(self):
        p = re.compile("do_(.*)_cmd")
        cmds = []
        for a in dir(self):
            m = p.match(a)
            if m is not None:
                cmds.append(m.group(1))
        cmds.sort()
        Es((r"""Usage:
  gxpc [global_options] COMMAND options ...
COMMAND is one of:
  %s

Try
  gxpc help COMMAND
to get more help on a specific command.
""" % string.join(cmds, ",")))

    def show_help_command(self, cmd, full):
        cname = "do_%s_cmd" % cmd
        usage_name = "usage_%s_cmd" % cmd
        if hasattr(self, cname) and hasattr(self, usage_name):
            method = getattr(self, usage_name)
            Es(method(full))
        elif hasattr(self, cname):
            Es("gxpc: %s: command has no help\n" % cmd)
        else:
            Es("gxpc: %s: no such command\n" % cmd)
            self.show_help_summary()

    def search_cmd_group(self, cmd, groups):
        for group in groups:
            if cmd in group:
                return group
        return [ cmd ]
        
    def show_help_all_commands(self):
        p = re.compile("do_(.*)_cmd")
        command_groups = [
            [ "smask", "savemask", "pushmask" ],
            [ "e", "mw" ],
            [ "use", "edges" ],
            [ "prof_start", "prof_stop" ],
            [ "log_level", "log_base_time" ],
            ]
        cmds = []
        dirs = dir(self)
        dirs.sort()
        helps = []
        marked = {}
        for a in dirs:
            m = p.match(a)
            if m is not None:
                cmd = m.group(1)
                if marked.has_key(cmd): continue
                groups = self.search_cmd_group(cmd, command_groups)
                for c in groups: marked[c] = 1
                method = getattr(self, ("usage_%s_cmd" % cmd))
                helps.append((groups, method(1)))
        texinfo_formatter().format(helps)

    # 
    # ---------- commands (do_xxxx_cmd and usage_xxxx_cmd ) ----------
    #

    #
    # help
    #
    def usage_help_cmd(self, full):
        u = r"""Usage:
  gxpc help
  gxpc help COMMAND
"""

        if full:
            u = u + r"""
Description:
  Show summary of gxpc commands or a help on a specific COMMAND.
"""
        return u

    def do_help_cmd(self, args):
        if len(args) == 0:
            self.show_help_summary()
        else:
            self.show_help_command(args[0], 1)
        return 0
                
    #
    # help
    #
    def usage_version_cmd(self, full):
        u = r"""Usage:
  gxpc version
"""
        return u

    def get_version(self):
        gxp_dir = self.get_gxp_dir()
        if gxp_dir is None: return None
        release_number_file = os.path.join(gxp_dir, "RELEASE_NUMBER")
        if not os.path.exists(release_number_file):
            return None
        fp = open(release_number_file, "rb")
        version = fp.read().strip()
        fp.close()
        return version

    def do_version_cmd(self, args):
        version = self.get_version()
        Ws("GXP version %s\n" % version)
    #
    # makeman
    #
    def usage_makeman_cmd(self, full):
        u = r"""Usage:
  gxpc makeman
"""
        if full:
            u = u + r"""
Description:
  Generate command reference chapter of gxp manual.
"""
        return u

    def do_makeman_cmd(self, args):
        self.show_help_all_commands()
        return 0

    #
    # stat
    #
    def usage_stat_cmd(self, full):
        u = r"""Usage:
  gxpc stat [LEVEL]
"""
        if full:
            u = u + r"""
Description:
  Show all live gxp daemons in tree format. LEVEL is 0, 1, or 2 and
determines the detail level of the information shown.
"""
        return u
                    
    def do_stat_cmd(self, args):
        """
        stat
        show tree of peers
        """
        level = 1
        if len(args) > 0:
            level = self.safe_atoi(args[0], level)
        if self.init2() == -1: return cmd_interpreter.RET_NOT_RUN

        self.session.clear_dirty()
        if 1:
            self.session.show(level)
        else:
            Ws("%s\n" % self.session_file)
        # Ws("%s\n" % self.gupid)
            if level >= 1:
                self.session.peer_tree.show()
        return 0
                    
    #
    # edges
    #
    def usage_use_cmd(self, full):
        u = r"""Usage:
  gxpc use          [--as USER] RSH_NAME SRC [TARGET]
  gxpc use --delete [--as USER] RSH_NAME SRC [TARGET]
  gxpc use
  gxpc use --delete [idx]

  e.g.,
  gxpc use ssh your_hostname compute_node_prefix
"""
        if full:
            u = u + r"""
Description:

  Configure rsh-like commands used to login targets matching a
particular pattern from hosts matching a particular pattern. The
typical usage is `gxpc use RSH_NAME SRC TARGET', which says gxp can
use an rsh-like command RSH_NAME for SRC to login TARGET. gxpc
remembers these facts to decide which hosts should issue which
commands to login which hosts, when explore command is issued. See the
tutorial section of the manual.

Examples:
  gxpc use           ssh abc000.def.com pqr.xyz.ac.jp
  gxpc use           ssh abc000 pqr
  gxpc use           ssh abc
  gxpc use           rsh abc
  gxpc use --as taue ssh abc000 pqr
  gxpc use qrsh      abc
  gxpc use qrsh_host abc
  gxpc use sge       abc
  gxpc use torque    abc

The first line says that, if gxpc is told to login pqr.xyz.ac.jp by
explore command, hosts named abc000.def.com can use `ssh' method to do
so.  How it translates into the actual ssh command line can be shown
by `show_explore' command (try `gxpc help show_explore') and can be
configured by `rsh' command (try `gxpc help rsh').

SRC and TARGET are actually regular expressions, so the line like the
first one can often be written like the second one.  The first line
is equivalent to the second line as long as there is only one host
begining with abc000 and there is only one target beginning with pqr.
In general, the specification:

  gxpc use RSH_NAME SRC TARGET

is read: if gxpc is told to login a target matching regular
expession TARGET, a host matching regular expression SRC can use
RSH_NAME to do so.

Note that the effect of use command is NOT to specify which target
gxpc should login, but to specify HOW it can do so, if it is told
to. It is the role of explore command to specify which target hosts it
should login

If the TARGET argument is omitted as in the third line, it is
treated as if TARGET expression is SRC. That is, the third line
is equivalent to:

  gxpc use ssh abc abc

This is often useful to express that ssh login is possible
between hosts within a single cluster, which typically have a
common prefix in their host names. If the traditional rsh command
is allowed within a single cluster, the fourth line may be useful
too.

If --as user option is given, login is issued using an explicit user
name. The fifth line says when gxp attempts to login pqr from abc000,
the explicit user name `taue' should be given. You do not need this as
long as the underlying rsh-like command will complement it by a
configuration file. e.g., ssh will read ~/.ssh/config to complement
user name used to login a particular host.

qrsh_host uses command qrsh, with an explicit hostname argument
to login a particular host (i.e., qrsh -l hostname=...).  This is
useful in environments where direct ssh is discouraged or
disallowed and qrsh is preferred.

qrsh also uses qrsh, but without an explicit hostname. The host
is selected by the scheduler. Therefore it does not make sense to
try to speficify a particular hostname as TARGET.  Thus, the
effect of the line

  gxpc use qrsh abc

is if targets beginning with abc is given (upon explore command),
a host beginning with abc will issue qrsh, and get whichever host
is allocated by the scheduler.

See Also:
  explore rsh
"""
        return u

    def do_use_cmd(self, args):
        """
        use (see below)
        """
        if self.init2() == -1: return cmd_interpreter.RET_NOT_RUN
        session = self.session
        session.clear_dirty()
        opts = use_cmd_opts()
        if opts.parse(args) == -1:
            return cmd_interpreter.RET_NOT_RUN
        
        if len(opts.args) == 0:
            if opts.delete:
                # use --delete --> delete all edges
                Es("gxpc : deleting all use clauses\n")
                del session.edges[:]
                session.set_dirty()
            else:
                # use --> show all edges
                idx = 0
                for m,u,s,t in session.edges:
                    if u == "":
                        Ws("%s : use %s %s %s\n" % (idx, m,s,t))
                    else:
                        Ws("%s : use --as %s %s %s %s\n" % (idx, u,m,s,t))
                    idx = idx + 1
        elif len(opts.args) == 1:
            if opts.delete:
                # "use --delete xxx". xxx must be a number
                idx = opts.safe_atoi(opts.args[0], None)
                if idx is None:
                    return cmd_interpreter.RET_NOT_RUN
                elif not 0 <= idx < len(session.edges):
                    Es("gxpc use: --delete idx (%d) out of range (try 'gxpc use')\n" \
                       % idx)
                    return cmd_interpreter.RET_NOT_RUN
                else:
                    del session.edges[idx]
                    session.set_dirty()
            else:
                return cmd_interpreter.RET_NOT_RUN
        else:
            if len(opts.args) == 2:
                [ method, src ] = opts.args
                target = src
            elif len(opts.args) == 3:
                [ method, src, target ] = opts.args
            else:
                return cmd_interpreter.RET_NOT_RUN
            if opts.as__ != "": method = ("%s_as" % method)
            if opts.delete:
                item = (method, opts.as__, src, target)
                if item in session.edges:
                    session.edges.remove(item)
                    session.set_dirty()
                else:
                    Es("gxpc use: no such use clause '%s'\n" \
                       % string.join(item, " "))
                    return cmd_interpreter.RET_NOT_RUN
            else:
                if self.session.login_methods.has_key(method):
                    item = (method, opts.as__, src, target)
                    if item in session.edges:
                        if opts.as__ == "":
                            edge_str = "use %s %s %s" % (method,src,target)
                        else:
                            edge_str = "use --as %s %s %s %s\n" % (method,opts.as__, src,target)

                        Es("gxpc use: ignore duplicated use clause: %s\n" \
                           % edge_str)
                    else:
                        session.edges.insert(0, (method, opts.as__, src, target))
                        session.set_dirty()
                else:
                    Es("gxpc use: no rsh-like method called '%s.' "
                       "try 'gxpc rsh' to see available methods\n" % method)
                    return cmd_interpreter.RET_NOT_RUN
        return 0

    #
    # showmasks
    #
    def show_target_tree_rec(self, tree, indent, sio):
        if tree is None:
            return
        else:
            if tree.eflag:
                sio.write("%4d: " % tree.exec_idx)
            else:
                sio.write("%4s: " % "-")
            sio.write(" " * indent)
            sio.write(tree.name)
            if tree.children is None: sio.write(" *")
            sio.write("\n")
            for ch in tree.children:
                self.show_target_tree_rec(ch, indent + 1, sio)

    def show_target_tree(self, tree):
        sio = cStringIO.StringIO()
        sio.write(" idx: name (gupid)\n")
        self.show_target_tree_rec(tree, 0, sio)
        r = sio.getvalue()
        sio.close()
        return r

    def usage_showmasks_cmd(self, full):
        u = r"""Usage:
  gxpc showmasks [--level 0/1] [NAME]
  e.g.,
  gxpc showmasks
  gxpc showmasks --level 1
"""

        if full:
            u = u + r"""
Description:
  Show summary (--level 0) or detail (--level 1) of all execution
masks (if `name' is omitted) or a specified excution mask named
`name.' The name of the current default execution mask is `0'.

Examples:
  gxpc showmasks
  gxpc showmasks --level 0     # show summary of all exec masks
  gxpc showmasks --level 0 0   # show summary of the current mask
  gxpc showmasks --level 1 0   # show detail of current exec mask
  gxpc showmasks --level 1 all # show detail of exec mask named `all'

Without any argument as in the first line, a summary like the
following is shown.

   0 : 1
   1 : 66
   six : 6
   ten : 8
   half : 27

This says there are three execution masks, called `0', `1',
`six', `ten', and `half.' The right column indicates the number
of nodes that will execute a command when the mask is selected by
a --withmask (-m) option. `0' is the current, default execution
mask, used when --withmask (-m) option is not given. These
masks are created by savemask or pushmask command.

When option --level 1 is given, details about the execution mask
will be shown, like the following.

  six : 6
   idx: name (gupid)
     -: hongo-lucy-tau-2006-12-31-22-40-24-31387
     -:  hongo002-tau-2006-12-31-13-39-17-338
     0:   hongo010-tau-2006-12-31-22-25-58-20559
     -:  hongo006-tau-2006-12-31-22-27-19-11951
     1:   hongo020-tau-2006-12-31-22-34-06-11026
     -:  hongo001-tau-2006-12-31-22-34-38-15517
     2:   hongo030-tau-2006-12-31-22-44-07-4931
     -:  hongo007-tau-2006-12-31-22-30-54-21738
     3:   hongo040-tau-2006-12-31-22-47-55-32666
     -:  hongo004-tau-2008-12-31-22-42-33-31254
     4:   hongo060-tau-2006-12-31-22-42-59-3899
     -:  hongo005-tau-2006-12-31-22-38-30-1150
     5:   hongo050-tau-2006-12-31-22-40-55-18088

This is a subtree of the whole tree of gxp daemons.  Nodes that will
actually execute commands will have an index number (0, 1, ..., 5) on
the first column. Nodes marked with `-' will not execute the
command, but are part of the minimum subtree containing those six
nodes.

See Also:
  smask rmask savemask restoremask pushmask popmask
"""
        return u

    def do_showmasks_cmd(self, args):
        if self.init2() == -1: return cmd_interpreter.RET_NOT_RUN
        level = 0
        arg = None
        i = 0
        while i < len(args):
            if args[i] == "--level" and i + 1 < len(args):
                level = self.safe_atoi(args[i + 1], 0)
                i = i + 1
            else:
                arg = args[i]
            i = i + 1
            
        trees = []
        if arg is None:
            # show all masks
            for i in range(len(self.session.stack_exec_trees)):
                ex,tree = self.session.stack_exec_trees[i]
                trees.append((i, ex, tree))
            for i in self.session.saved_exec_trees.keys():
                ex,tree = self.session.saved_exec_trees[i]
                trees.append((i, ex, tree))
        else:
            # look for the specified mask and show it
            i = self.safe_atoi(arg, arg)
            if i == arg:                # string
                if self.session.saved_exec_trees.has_key(i):
                    ex,tree = self.session.saved_exec_trees[i]
                    trees.append((i, ex, tree))
                else:
                    Es("gxpc: No such exec mask %s\n" % i)
                    return cmd_interpreter.RET_NOT_RUN
            else:                       # int
                if 0 <= i < len(self.session.stack_exec_trees):
                    ex,tree = self.session.stack_exec_trees[i]
                    trees.append((i, ex, tree))
                else:
                    Es("gxpc: No such exec mask %s\n" % i)
                    return cmd_interpreter.RET_NOT_RUN
            
        for i,ex,tree in trees:
            Ws("%s : %s\n" % (i, ex))
            if level > 0:
                Ws(self.show_target_tree(tree))
        return 0
        
    #
    # smask or pushmask
    #
    def smask_like_cmd(self, cmd_name, args):
        """
        smask [-]
        pushmask [-]
        savemask [-] name

        smask/pushmask:
        
        1. make a new exec tree based on last_exec_tree and
        last_term_status.
        2. install the new tree as stack_exec_trees[0]
        smask will overwrite the old stack_exec_trees[0],
        whereas pushmask will not (push).

        """
        if self.init2() == -1: return cmd_interpreter.RET_NOT_RUN
        sign = 1
        name = None
        for arg in args:
            if arg == "-":
                sign = 0
            else:
                name = arg
        if cmd_name == "smask" or cmd_name == "savemask":
            push = 0
        else:
            assert cmd_name == "pushmask", cmd_name
            push = 1
        self.session.set_selected_exec_tree(sign, push, name)
        return 0

    def usage_smask_like(self, full):
        u = r"""Usage:
  gxpc smask    [-]
  gxpc savemask [-] NAME
  gxpc pushmask [-]
  e.g.,
  gxpc  e  'uname | grep Linux'
  gxpc  smask
"""
        if full:
            u = u + r"""
Description:
  All three commands have a common effect. That is to modify the
set of nodes that will execute subsequent commands.  When `-'
argument is not given, nodes that executed the last command and
succeeded are selected. With argument `-', nodes that executed
the last command and failed are set. The definition of sucess or
failure depends on commands, but in the case of `e' command, a
node is considered to succeed if the command exits with status
zero.

These commands can be used to efficiently choose the nodes
to execute subsequent commands by various criterion.

Examples:

1.

  gxpc  e  'uname | grep Linux'
  gxpc  smask

This will set the execution mask of Linux nodes.

2.

  gxpc  e  'which apt-get'
  gxpc  smask  -
  gxpc  e  hostname

This will set the execution mask of nodes that do not have apt-get
command, and the last command will show their hostnames.

To see the effect of these commands, it is advised to include the
gxp3 directory in your PATH, and add something like the following
in your shell (bash) prompt, which will show the number of nodes
that succeeded the last command, the number of nodes that is
currently selected, and the number all nodes.

  export PS1='\h:\W`which gxp_prompt 1> /dev/null && gxp_prompt`% '

With this, you can see the effect of these commands in shell
prompt.

  [66/66/66]% e 'uname | grep Linux'
  Linux
  Linux
  Linux
  Linux
  Linux
  Linux
  Linux
  Linux
  Linux
  Linux
  [10/66/66]% gxpc smask
  [10/10/66]% 

In addition to setting the execution mask, savemask saves the set
of selected nodes with the specified name. Sets of nodes hereby
saved can be later used for execution by giving --withmask (-m)
option. This is useful when your work needs several, typical set
of nodes to execute commands on. For example, you may save a
small number of nodes for test, all gateway nodes to compile
programs, all nodes within a particular cluster, and really all
nodes.

Command pushmask is similar to savemask, but the set is saved
onto a stack. The newly selected set of nodes are on the top of
the stack and named `0'.  Previously selected nodes are named by
the distance from the top.

See Also:
  showmasks rmask restoremask popmask
"""
        return u
            

    #
    # smask
    #
    def usage_smask_cmd(self, full):
        return self.usage_smask_like(full)
        
    def do_smask_cmd(self, args):
        """
        smask [-]
        """
        return self.smask_like_cmd("smask", args)

    #
    # pushmask
    #
    def usage_pushmask_cmd(self, full):
        return self.usage_smask_like(full)

    def do_pushmask_cmd(self, args):
        """
        pushmask
        """
        return self.smask_like_cmd("pushmask", args)

    #
    # savemask
    #
    def usage_savemask_cmd(self, full):
        return self.usage_smask_like(full)

    def do_savemask_cmd(self, args):
        """
        savemask
        """
        return self.smask_like_cmd("savemask", args)

    #
    # popmask
    #
    def usage_popmask_cmd(self, full):
        u = r"""Usage:
  gxpc popmask
"""
        if full:
            u = u + r"""
Description:
  Pop the set of nodes on the top of the stack. The next entry
that is used to be referred to by name `1' will now be the top
of the stack, and thus become the default set of nodes selected
for execution.

See Also:
  pushmask
"""
        return u

    def do_popmask_cmd(self, args):
        """
        popmask
        """
        if self.init2() == -1: return cmd_interpreter.RET_NOT_RUN
        if self.session.pop_exec_tree() == 0:
            return 0
        else:
            return 1

    #
    # restoremask
    #
    def usage_restoremask_cmd(self, full):
        return r"""Usage:
  gxpc restoremask NAME
"""

    def do_restoremask_cmd(self, args):
        if self.init2() == -1: return cmd_interpreter.RET_NOT_RUN
        if len(args) != 1:
            return cmd_interpreter.RET_NOT_RUN
        elif self.session.restore_exec_tree(args[0]) == 0:
            return 0
        else:
            return 1

    #
    # rmask
    #
    def usage_rmask_cmd(self, full):
        u = r"""Usage:
  gxpc rmask
"""
        if full:
            u = u + r"""
Description:
  Reset execution mask. Let all nodes execute subsequent commands.

See Also:
  showmasks smask savemask restoremask pushmask popmask
"""
        return u

    def do_rmask_cmd(self, args):
        """
        rmask
        rmask/explore:
        1. stack_exec_trees[0] will become the whole peer_tree
        2. update peer_tree_count
        """
        if self.init2() == -1: return cmd_interpreter.RET_NOT_RUN
        self.session.reset_exec_tree()
        return 0


    #
    # quit
    #
    def usage_quit_cmd(self, full):
        u = r"""Usage:
  gxpc quit [--session_only]
"""
        if full:
            u = u + r"""
Description:
  Quit gxp session. By default, all daemons will exit.
If --session_only is given, daemons keep running and only the 
session will cease.
"""
        return u
        
    def do_quit_cmd(self, args):
        """
        quit 
        """
        if self.init3() == -1: return cmd_interpreter.RET_NOT_RUN
        self.cleanup_session_file(self.session_file)
        if len(args) == 0 or args[0] != "--session_only":
            tgt = gxpm.target_tree(".*", ".*", ".*", 1, 0, gxpm.exec_env(), None)
            tid = self.send_action(tgt, None, gxpm.action_quit(),
                                   self.opts.persist,
                                   self.opts.keep_connection)
            assert tid is not None
            self.cleanup_daemon_files(self.daemon_addr)
            session_files = self.find_session_files_of_daemon_addr(self.daemon_addr)
            for session_file in session_files:
                self.cleanup_session_file(session_file)
        return 0
        
    #
    # ping and helper
    #
    #
    # ping-like commands (ping, cd, export, prof_start, prof_stop, loglevel)
    #

    def ping_like_cmd(self, action, quiet, trimmed):
        if self.init3() == -1: return cmd_interpreter.RET_NOT_RUN
        # --------- ugly warning begin
        if quiet: self.outmap["info"] = (None, None)
        # --------- ugly warning end
        ex,tgt = self.session.select_exec_tree(self.opts)
        if ex == -1: return cmd_interpreter.RET_NOT_RUN
        if trimmed and isinstance(tgt, gxpm.target_tree):
            tgt.set_eflag(1)
        got_sigint = 0
        if tgt is not None:             # some nodes to send
            tid = self.send_action(tgt, None, action, self.opts.persist,
                                   self.opts.keep_connection)
            assert tid is not None
            time_limit = self.timeout_to_limit(self.opts.timeout)
            # res = self.recv_loop_noex(None, time_limit)
            res = self.process_events_loop(tgt, tid, None, time_limit)
            assert res != cmd_interpreter.RECV_CONTINUE
            if res == cmd_interpreter.RECV_INTERRUPTED:
                got_sigint = 1
        for gupid,tid,ev in self.events:
            if isinstance(ev, gxpm.event_info) \
                   and ev.status is not None:
                self.session.last_term_status[gupid] = ev.status
        if self.session.peer_tree is None:
            if self.opts.verbosity>=2:
                Es("gxpc: construct peer tree\n")
            if self.session.construct_peer_tree(self.events) == -1:
                return -1
        elif trimmed:
            self.session.trim_peer_tree(tgt)
        status = self.session.update_last_ok_count()
        # --------- ugly warning begin
        if quiet: self.outmap["info"] = (sys.stdout, None)
        # --------- ugly warning end
        if got_sigint:
            return cmd_interpreter.RET_SIGINT
        else:
            return status

    def usage_ping_cmd(self, full):
        u = r"""Usage:
  gxpc ping [LEVEL]
"""
        if full:
            u = u + r"""
Description:
  Send a small message to the selected nodes and show some
information. The parameter LEVEL is 0 or 1. Default is 0.
This is useful to know the name and basic information about
all or some nodes. It is also useful to check the status (liveness)
of nodes and trim non-responding nodes.

See Also:
  trim smask
"""
        return u

    def safe_atoi(self, x, defa):
        try:
            return string.atoi(x)
        except ValueError,e:
            return defa

    def do_ping_cmd(self, args):
        """
        ping
        """
        quiet = 0
        level = 0
        for a in args:
            if a == "--quiet":
                quiet = 1
            else:
                level = self.safe_atoi(a, level)
        act = gxpm.action_ping(level)
        return self.ping_like_cmd(act, quiet, 0)

    #
    # trim
    #
    def usage_trim_cmd(self, full):
        u = r"""Usage:
  gxpc trim
"""
        if full:
            u = u + r"""
Description:
  Trim (release) some subtrees of gxp daemons. This is typically
used after a ping cmd followed by smask, to prune non-responding
(dead) daemons.  Specifically, trim command will be executed on
the selected nodes, and each such node will throw away a children
C if no nodes under the subtree rooted at C are selected for
execution of this trim command. This effectively prunes (trims)
the subtree from the tree of gxp daemons. For example,

  gxpc ping
  # If there are some non-responding daemons, this command will hang.
  # type <Ctrl-C> to quit.
  gxpc smask
  gxpc trim
"""
        return u

    def do_trim_cmd(self, args):
        act = gxpm.action_trim()
        return self.ping_like_cmd(act, 0, 1) # trim = 1

    def usage_set_max_buf_len_cmd(self, full):
        u = r"""Usage:
  gxpc set_max_buf_len N
"""
        if full:
            u = u + r"""
Description:
  Set maximum internal buffer size of gxp daemons in bytes.
  default is 10KB and any value below the default is ignored. If no argument
  is given, use the default value.
"""
        return u

    def do_set_max_buf_len_cmd(self, args):
        max_buf_len = 10000
        if len(args) > 0:
            max_buf_len = self.safe_atoi(args[0], None)
            if max_buf_len is None:
                return cmd_interpreter.RET_NOT_RUN
            if max_buf_len < 10000: max_buf_len = 10000
        act = gxpm.action_set_max_buf_len(max_buf_len)
        return self.ping_like_cmd(act, 0, 0)

    #
    #
    #
    def usage_prof_start_stop(self, full):
        u = r"""Usage:
  gxpc prof_start FILENAME
  gxpc prof_stop
"""
        if full:
            u = u + r"""
Description:
  Start/stop profiling of the selected nodes. Stats are saved to
the FILENAME.
"""
        return u

    def usage_prof_start_cmd(self, full):
        return self.usage_prof_start_stop(full)

    def do_prof_start_cmd(self, args):
        """
        prof_start
        """
        if len(args) != 1:
            return cmd_interpreter.RET_NOT_RUN
        else:
            act = gxpm.action_prof_start(args[0])
            return self.ping_like_cmd(act, 0, 0)

    #
    #
    #
    def usage_prof_stop_cmd(self, full):
        return self.usage_prof_start_stop(full)

    def do_prof_stop_cmd(self, args):
        """
        prof_stop
        """
        act = gxpm.action_prof_stop()
        return self.ping_like_cmd(act, 0, 0)

    #
    #
    #
    def usage_log_like(self, full):
        u = r"""Usage:
  gxpc log_level LEVEL
  gxpc log_base_time
"""
        if full:
            u = u + r"""
Description:
  Command log_level will set the log level of the selected nodes
to the specified LEVEL.  0 will write no logs. 2 will write many.
Command log_base_time will reset the time of the selected nodes
to zero.  Subsequent log entries will record the time relative to
this time.
"""
        return u

    def usage_log_level_cmd(self):
        return self.usage_log_like()

    def do_log_level_cmd(self, args):
        """
        log_level
        """
        level = 0
        if len(args) > 0:
            level = self.safe_atoi(args[0], level)
        act = gxpm.action_set_log_level(level)
        return self.ping_like_cmd(act, 0, 0)

    #
    #
    #
    def usage_log_base_time_cmd(self, full):
        return self.usage_log_like(full)

    def do_log_base_time_cmd(self, args):
        """
        log_base_time
        """
        act = gxpm.action_set_log_base_time()
        return self.ping_like_cmd(act, 0, 0)

    #
    #
    #
    def usage_reclaim_cmd(self, full):
        u = r"""Usage:
  gxpc reclaim tid
"""
        if full:
            u = u + r"""
Description:
  Command reclaim will reclaim task tid unconditionally.
"""
        return u

    def do_reclaim_cmd(self, args):
        """
        log_base_time
        """
        act = gxpm.action_reclaim_task(args)
        return self.ping_like_cmd(act, 0, 0)

    #
    # the 'e' command and helper
    #

    def usage_e_and_mw(self, full):
        u = r"""Usage:
  gxpc e  [OPTION ...] CMD
  gxpc mw [OPTION ...] CMD
  gxpc ep [OPTION ...] FILE
"""
        if full:
            u = u + r"""
Description:
  Execute the command on the selected nodes.

Options: (for mw only):
  --master 'command'
    equivalent to e --updown '3:4:command' ...
  if --master is not given, it is equivalent to e --updown 3:4 ...

Options: (for e, mw, and ep):
  --withmask,-m MASK
    execute on a set of nodes saved by savemask or pushmask
  --withhostmask,-h HOSTMASK
    execute on a set of nodes whose hostnames match regexp HOSTMASK
  --withhostnegmask,-H HOSTMASK
    execute on a set of nodes whose hostnames do not match regexp HOSTMASK
  --withgupidmask,-g HOSTMASK
    execute on a set of nodes whose gupid (shown by gxpc stat) 
    match regexp HOSTMASK
  --withgupidnegmask,-G HOSTMASK
    execute on a set of nodes whose gupid (shown by gxpc stat) 
    do not match regexp HOSTMASK
  --withtargetmask,-t HOSTMASK
    execute on a set of nodes whose target name (shown by gxpc stat) 
    match regexp HOSTMASK
  --withtargetnegmask,-T HOSTMASK
    execute on a set of nodes whose target name (shown by gxpc stat) 
    do not match regexp HOSTMASK
  --up FD0[:FD1]
    collect output from FD0 of CMD, and output them to FD1 of gxpc.
    if :FD1 is omitted, it is treated as if FD1 == FD0
  --down FD0[:FD1]
    broadcast input to FD0 of gxpc to FD1 of CMD.
    if :FD1 is omitted, it is treated as if FD1 == FD0
  --updown FD1:FD2[:MASTER]
    if :MASTER is omitted, collect output from FD1 of CMD,
    and broadcast them to FD2 of CMD.
    if :MASTER is given, run MASTER on the local host, collect
    output from FD1 of CMD, feed them to stdin of the MASTER.
    broadcast stdout of the MASTER to FD1 of CMD.
  --pty
    assign pseudo tty for stdin/stdout/stderr of CMD
  --rlimit rlimit_xxx:soft[:hard]
    apply setrlimit(rlimit_xxx, soft, hard)

By default,

- stdin of gxpc are broadcast to stdin of CMD
- stdout of CMD are output to stdout of gxpc
- stderr of CMD are output to stderr of gxpc

This is as if `--down 0 --up 1 --up 2' are specified.  In this
case, stdout/stderr are block-buffered by default.  You may need
to do setbuf in your program or flush stdout/err, to display
CMD's output without delay.  --pty overwrites this and turn them
to line-buffered (by default).  both stdout/err of CMD now goto
stdout of gxpc (they are merged).  CMD's stdout/err should appear
as soon as they are newlined.

See Also:
  smask savemask pushmask rmask restoremask popmask
"""
        return u
        
    def add_pty_pipes(self, atomicity, pipes, open_count):
        pipes.append(("pty",
                      [ ("w", 0, atomicity), ("r", 1, atomicity) ],
                      [ ("r", 0), ("w", 1), ("w", 2) ]))
        if open_count == "":
            co = None
        else:
            co = counter(open_count)
        self.outmap[1] = (sys.stdout, co)
        self.inmap[0] = 0

    def add_up_pipe(self, from_fd, to_fd,
                    atomicity, pipes, open_count):
        # pipes.append((fd, "w", atomicity))
        if self.check_fd_writable(to_fd) == 0:
            Es("gxpc: file descriptor %s is not open for write. "
               "perhaps missing '%s> file'\n" % (to_fd, to_fd))
            return -1
        fp = self.fdopen(to_fd, "wb", -1) # -1 = default buffer
        pipes.append(("pipe", # "sockpair",
                      [("r", from_fd, atomicity)], [("w", from_fd)]))
        if open_count == "":
            co = None
        else:
            co = counter(open_count)
        # assert not self.outmap.has_key(from_fd)
        self.outmap[from_fd] = (fp, co)
        return 0

    def add_down_pipe(self, from_fd, to_fd, pipes):
        """
        make an entry which says if this process gets
        something from 'from_fd', it should go to 'to_fd'
        of all running procs
        """
        pipes.append(("pipe", [("w", to_fd, None)], [("r", to_fd)]))
        # check if from_fd is readable
        if self.check_fd_readable(from_fd) == 0:
            Es("gxpc: file descriptor %s is not open for read. "
               "perhaps missing '%s< file'\n" % (from_fd, from_fd))
            return -1
        else:
            # assert not self.inmap.has_key(from_fd)
            self.inmap[from_fd] = to_fd
        return 0

    def setup_default_pipes(self, opts):
        self.outmap["info"] = (sys.stdout, None)
        self.outmap["explore"] = (sys.stdout, None)

    def check_fd_readable(self, fd):
        try:
            select.select([ fd ], [], [], 0.0)
        except select.error,e:
            if e.args[0] == errno.EBADF:
                return 0
            else:
                raise
        return 1

    def check_fd_writable(self, fd):
        try:
            select.select([], [ fd ], [], 0.0)
        except select.error,e:
            if e.args[0] == errno.EBADF:
                return 0
            else:
                raise
        return 1

    def fdopen(self, fd, mode, bufsz):
        if fd == 1:
            return sys.stdout
        elif fd == 2:
            return sys.stderr
        elif bufsz is None:
            return os.fdopen(fd, mode)
        else:
            return os.fdopen(fd, mode, bufsz)
        assert 0

    def fork_master(self, cmd):
        r0,w0 = os.pipe()
        r1,w1 = os.pipe()
        pid = os.fork()
        if pid == 0:
            # child
            os.dup2(r0, 0)
            os.dup2(w1, 1)
            for fd in [ r0, w0, r1, w1 ]: os.close(fd)
            os.execvp("/bin/sh", [ "/bin/sh", "-c", cmd ])
        else:
            os.close(r0)
            os.close(w1)
        return r1,w0,pid

    def setup_pipes(self, atom, pty, up, down, updown, nexecs):
        """
        n_execs : the number of processes that will be invoked
        """
        pipes = []
        if pty:
            self.add_pty_pipes(atom, pipes, nexecs)

        for fd0,fd1 in up:
            # option: --up fd
            # this entry says child proc's output from fd should
            # go to my fd
            if self.add_up_pipe(fd0, fd1, atom, pipes, nexecs) == -1:
                return None
        for fd0,fd1 in down:
            # option: --down fd
            # this says my input from fd should go to
            # child proc's fd
            if self.add_down_pipe(fd0, fd1, pipes) == -1:
                return None
        for fd0,fd1,cmd in updown:
            # option: --updown fd0:fd1
            # this says child proc's output from fd0 should
            # go back to child proc's fd1
            # output from fd0 of the child will arrive here as
            # a msg. arrange thing so that the main thread that 
            # got it will write its payload to one end of pipe
            # (w below) and the helper thread will get the payload
            # from the other end (r below) and write it back to fd1
            if cmd is None:
                r,w = os.pipe()
            else:
                r,w,pid = self.fork_master(cmd)
                self.master_pids.append(pid)
            if self.add_up_pipe(fd0, w, atom, pipes, nexecs) == -1:
                return None
            # the helper thread will obtain it from r
            # and will generate a packet to feed child proc's fd1
            if self.add_down_pipe(r, fd1, pipes) == -1:
                return None
        pipes.sort()
        return pipes
        
    def safe_wait(self):
        try:
            os.wait()
            return 0
        except KeyboardInterrupt,e:
            return -1

    def die_with_sigint(self):
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        os.kill(os.getpid(), signal.SIGINT)

    def set_fcntl_constants(self):
        ok = 0
        if fcntl.__dict__.has_key("F_SETFL"):
            self.F_GETFL = fcntl.F_GETFL
            self.F_SETFL = fcntl.F_SETFL
            self.F_GETFD = fcntl.F_GETFD
            self.F_SETFD = fcntl.F_SETFD
            self.FD_CLOEXEC = fcntl.FD_CLOEXEC
            ok = 1
        else:
            try:
                import FCNTL
                self.F_GETFL = FCNTL.F_GETFL
                self.F_SETFL = FCNTL.F_SETFL
                self.F_GETFD = FCNTL.F_GETFD
                self.F_SETFD = FCNTL.F_SETFD
                self.FD_CLOEXEC = FCNTL.FD_CLOEXEC
                ok = 1
            except ImportError:
                pass
        if ok == 0:
            LOG("This platform provides no ways to make "
                "fd non-blocking. abort\n")
            os._exit(1)

    def set_close_on_exec_fd(self, fd, close_on_exec):
        """
        make fd non blocking
        """
        self.set_fcntl_constants()
        if close_on_exec:
            fcntl.fcntl(fd, self.F_SETFD, self.FD_CLOEXEC)
        else:
            fcntl.fcntl(fd, self.F_SETFD, 0)

    def overwrite_defaults(self, opts):
        self.opts.withall = opts.withall
        if opts.withmask is not None:
            self.opts.withmask = opts.withmask
        if self.opts.withall:
            self.opts.withmask = None
        if opts.withhostmask is not None:
            self.opts.withhostmask = opts.withhostmask
        if opts.withhostnegmask is not None:
            self.opts.withhostnegmask = opts.withhostnegmask
        if opts.withgupidmask is not None:
            self.opts.withgupidmask = opts.withgupidmask
        if opts.withgupidnegmask is not None:
            self.opts.withgupidnegmask = opts.withgupidnegmask
        if opts.withtargetmask is not None:
            self.opts.withtargetmask = opts.withtargetmask
        if opts.withtargetnegmask is not None:
            self.opts.withtargetnegmask = opts.withtargetnegmask
        if opts.withidxmask is not None:
            self.opts.withidxmask = opts.withidxmask
        if opts.withidxnegmask is not None:
            self.opts.withidxnegmask = opts.withidxnegmask
        if opts.timeout is not None:
            self.opts.timeout = opts.timeout
        if opts.notify_proc_exit is not None:
            self.opts.notify_proc_exit = opts.notify_proc_exit
        if opts.log_io is not None:
            self.opts.log_io = opts.log_io
        if opts.persist is not None:
            self.opts.persist = opts.persist
        if opts.keep_connection is not None:
            self.opts.keep_connection = opts.keep_connection
        if opts.tid is not None:
            self.opts.tid = opts.tid

    def e_like_cmd(self, cname, args, transform, default_stdout, accum_events):
        """
        e hostname ...
        """
        if self.init3() == -1: return cmd_interpreter.RET_NOT_RUN
        # say we are not interested in accumulating events
        # since they may be potentially big
        if accum_events == 0: self.events = None
        # build shell command from args
        opts = e_cmd_opts()
        if opts.parse(args) == -1:
            return cmd_interpreter.RET_NOT_RUN
        # overwrite defaults
        self.overwrite_defaults(opts)

        if opts.notify_proc_exit > 0:
            self.notify_proc_exit_fp = os.fdopen(opts.notify_proc_exit, "wb")
        if opts.log_io > 0:
            self.log_io_fp = os.fdopen(opts.log_io, "wb")

        if cname == "mw":
            if opts.master is None:
                updown = opts.updown + [ (3, 4, None) ]
            else:
                updown = opts.updown + [ (3, 4, opts.master) ]
        else:
            # assert (cname == "e"), cname
            if opts.master is None:
                updown = opts.updown
            else:
                return cmd_interpreter.RET_NOT_RUN
        # send this action to the currently selected nodes
        ex,tgt = self.session.select_exec_tree(self.opts)
        if ex == -1: return cmd_interpreter.RET_NOT_RUN

        got_sigint = 0
        if tgt is not None:
            # Es("tgt = %s\n" % tgt.show())
            pipes = self.setup_pipes(self.opts.atomicity, opts.pty,
                                     opts.up, opts.down, updown, ex)
            if pipes is None: return cmd_interpreter.RET_NOT_RUN

            # -------------- ugly
            if default_stdout == 0:
                stdout,co = self.outmap[1]
                self.outmap[1] = (None, co)
            # -------------- ugly

            shcmd = string.join(opts.args, " ")
            if transform is not None:
                shcmd = transform(shcmd)
            act = gxpm.action_createproc(opts.rid, opts.dir,
                                         opts.export, shcmd, pipes, 
                                         opts.rlimit)
            tid = self.send_action(tgt, self.opts.tid, act,
                                   self.opts.persist,
                                   self.opts.keep_connection)
            assert tid is not None, (self.opts.withmask, tgt)

            time_limit = self.timeout_to_limit(self.opts.timeout)
            res = self.process_events_loop(tgt, tid, None, time_limit)
            assert res != cmd_interpreter.RECV_CONTINUE
            for sig in [ "INT", "TERM", "KILL" ]:
                if res == cmd_interpreter.RECV_QUIT: break
                if self.opts.verbosity>=2:
                    Es("gxpc: got interrupt\n")
                got_sigint = 1
                rid = None          # all
                Es("gxpc: sending %s signal\n" % sig)
                self.send_action(tgt, tid,
                                 gxpm.action_sig(rid, sig),
                                 self.opts.persist,
                                 self.opts.keep_connection)
                time_limit2 = self.timeout_to_limit(3.0)
                res = self.process_events_loop(tgt, tid, None, time_limit2)
            if res == cmd_interpreter.RECV_TIMEOUT:
                Es("gxpc: warning: timeout after interrupt (some processes may be left)\n")
            for i in range(len(self.master_pids)):
                if self.safe_wait() == -1: break
        status = self.session.update_last_ok_count()
        if got_sigint:
            return cmd_interpreter.RET_SIGINT
        else:
            return status

    def usage_e_cmd(self, full):
        return self.usage_e_and_mw(full)

    def do_e_cmd(self, args):
        return self.e_like_cmd("e", args, None, 1, 0)

    def usage_mw_cmd(self, full):
        return self.usage_e_and_mw(full)

    def do_mw_cmd(self, args):
        return self.e_like_cmd("mw", args, None, 1, 0)

    def usage_ep_cmd(self, full):
        return self.usage_e_and_mw(full)

    def do_ep_cmd(self, args):
        return self.e_like_cmd("ep", args, None, 1, 0)

    #
    # cd
    #
    def set_cwd_rec(self, tree, new_dirs):
        """
        tree is target tree
        """
        if tree is not None:
            if new_dirs.has_key(tree.name):
                nwd = new_dirs[tree.name]
                # Ws("%s : %s -> %s\n" % (tree.name, tree.eenv.cwd, nwd))
                tree.eenv.cwd = nwd
            for ch in tree.children:
                self.set_cwd_rec(ch, new_dirs)

    def usage_cd_cmd(self, full):
        u = r"""Usage:
  gxpc cd [OPTIONS ...] DIRECTORY
"""
        if full:
            u = u + r"""
Description:
  Set current directory of the selected nodes to DIRECTORY.
Subsequent commands will start at the specified directory.
Options are the same as those of `e' command.
"""
        return u

    def transform_to_cd(self, shcmd):
        return "cd %s > /dev/null && echo $PWD" % shcmd

    def do_cd_cmd(self, args):
        r = self.e_like_cmd("cd", args, self.transform_to_cd, 0, 1)
        outputs = {}
        for gupid,tid,ev in self.events:
            if isinstance(ev, gxpm.event_io) and ev.fd == 1:
                if not outputs.has_key(gupid):
                    outputs[gupid] = []
                outputs[gupid].append(ev.payload)
        # calc new dirs
        new_dirs = {}
        for gupid,out in outputs.items():
            if self.session.last_term_status.get(gupid, -1) == 0:
                nwd = string.strip(string.join(out, ""))
                new_dirs[gupid] = nwd
        # modify exec tree
        self.set_cwd_rec(self.session.last_exec_tree, new_dirs)
        return r

    #
    # export
    #
    def set_env_rec(self, tree, new_envs):
        if tree is not None:
            if new_envs.has_key(tree.name):
                var,val = new_envs[tree.name]
                # Ws("%s : %s := %s\n" % (tree.name, var, val))
                tree.eenv.env[var] = val
            for ch in tree.children:
                self.set_env_rec(ch, new_envs)

    def usage_export_cmd(self, full):
        u = r"""Usage:
  gxpc export VAR=VAL
"""
        if full:
            u = u + r"""
Description:
  Set environment variable VAR to VAL on the selected nodes.
Options are the same as those of `e' command.
"""
        return u

    def transform_to_export(self, shcmd):
        return "echo %s" % shcmd

    def do_export_cmd(self, args):
        r = self.e_like_cmd("export", args, self.transform_to_export, 0, 1)
        outputs = {}
        for gupid,tid,ev in self.events:
            if isinstance(ev, gxpm.event_io) and ev.fd == 1:
                if not outputs.has_key(gupid):
                    outputs[gupid] = []
                outputs[gupid].append(ev.payload)
        # calc new dirs
        new_envs = {}
        for gupid,out in outputs.items():
            if self.session.last_term_status.get(gupid, -1) == 0:
                var_val = string.strip(string.join(out, ""))
                var_val = string.split(var_val, "=", 1)
                if len(var_val) == 2:
                    [ var, val ] = var_val
                    new_envs[gupid] = (var, val)
        # modify exec tree
        self.set_env_rec(self.session.last_exec_tree, new_envs)
        return r
    
    #
    # target and helper
    #
    def mk_targets_to_explore(self, target_hosts, aliases):
        """
        From hosts listed in session.target_hosts and hosts
        already reached, make a list of targets to explore
        """
        to_explore = []
        reached = []
        marked_targets = []
        requested_counts = {}
        successful_targets = self.session.successful_targets
        for host,n in target_hosts:
            # the entry says we should reach host n times
            c = requested_counts.get(host, 0)
            r = 0
            for h_alias in aliases.get(host, [ host ]):
                r = r + successful_targets.get(h_alias, 0)
            a = min(c + n, r) - c
            s = c + n - r
            if a > 0:
                for i in range(a): reached.append(host)
                marked_targets.append(("R", a, host))
            if s > 0:
                for i in range(s): to_explore.append(host)
                marked_targets.append(("N", s, host))
            requested_counts[host] = c + n
        return marked_targets,to_explore,reached

    def extract_target_hosts(self, targets, aliases):
        T = []
        aliases = aliases.items()
        aliases.sort()
        targets = targets.items()
        targets.sort()
        marked = {}
        for t,n in targets:
            found = 0
            for h,h_aliases in aliases:
                if marked.has_key(h): continue
                if re.match(t, h):
                    found = 1
                    for a in h_aliases: marked[a] = 1
                    T.append((h, n))
                    break
            if found == 0: T.append((t, n))
        return T
        
    #
    # configure explore and helpers
    #

    #
    # show rsh
    #

    def usage_show_explore_cmd(self, full):
        u = r"""Usage:
  gxpc show_explore SRC TARGET
"""
        if full:
            u = u + r"""
Description:
  Show command used to explore TARGET from SRC.
"""
        return u
        
    def do_show_explore_cmd(self, args):
        if self.init3() == -1: return cmd_interpreter.RET_NOT_RUN
        opts = explore_cmd_opts()
        if opts.parse(args) == -1:
            return cmd_interpreter.RET_NOT_RUN
        # ----------- set default values for opts
        opts.import_defaults(self.session.default_explore_opts)

        # ----------- real work -----------
        if len(opts.args) < 2:
            return cmd_interpreter.RET_NOT_RUN
        name = opts.args[0]
        # treat as if the src node's target_label/hostname/gupid are 
        # all the same
        nid,tgt,cmd = self.mk_explore_cmd(name, name, name,
                                          opts.args[1], {}, opts)
        if cmd is None:
            Ws((r"""I don't know how to login %s from %s.
Use use command to specify how (e.g., 'gxpc use ssh %s %s').
""" \
                % (opts.args[1], opts.args[0], opts.args[0], opts.args[1])))
        else:
            Ws("%s\n" % cmd)
            if 1:
                Ws(r"""Set environment variable GXP_DIR to run the above command. e.g., 
  export GXP_DIR=%s
""" % os.environ["GXP_DIR"])
            if 0:
                Ws(r"""Set environment variables GXP_DIR/GXP_GUPID
to run the above command. e.g., 
  export GXP_DIR=%s
  export GXP_TOP=%s
  export GXP_GUPID=gupid
""" % (os.environ["GXP_DIR"], os.environ["GXP_TOP"]))
        return 0
        

    #
    # add_rsh
    #
    def usage_rsh_cmd(self, full):
        u = r"""Usage:
  gxpc rsh [OPTIONS]
  gxpc rsh [OPTIONS] rsh_name
  gxpc rsh [OPTIONS] rsh_name rsh_like_command_template
"""
        if full:
            u = u + r"""
Description:
  Show, add, or modify rsh-like command explore/use will recognize
  to the repertoire.
  'gxpc rsh' lists all configured rsh-like commands.
  'gxpc rsh rsh_name' shows the specified rsh-like command.
  'gxpc rsh rsh_name rsh_like_command_template' adds or modifies (if exist)
  the specified rsh-like command to use rsh_like_command_template.

  The rsh_name is used as the first parameter of 'use' command (e.g.,
  'gxpc use ssh src target' or 'gxpc use rsh <src> <target>').

  By default, the following rsh-like commands are builtin.

  ssh, ssh_as, rsh, rsh_as, sh, sge, torque, and pbs.

Options:
  --full
    when invoked as gxpc rsh --full (with no other args),
    show command lines of all available rsh-like commands.

Examples:
1.
  gxpc rsh ssh
  (output) ssh : ssh -o 'StrictHostKeyChecking no' ... -A %target% %cmd%

  This displays that an rsh-like command named 'ssh' is configured,
  and gxp understands that, to run a command a host via ssh, it should
  use a command:

         ssh -o 'StrictHostKeyChecking no' ... -A %target% %cmd%

  with %target% replaced by a target name (normally a
  hostname) and %cmd% by whatever commands it wants
  to execute on the target.
  
2.   
  gxpc rsh ssh ssh -i elsewhere/id_dsa -o 'StrictHostKeyChecking no' -A %target% %cmd%

  This instructs gxp to use command line:

     ssh -i elsewhere/id_dsa -o 'StrictHostKeyChecking no' -A %target% %cmd%

  when it uses ssh.  You can arbitrarily name a new rsh-like command. 
  For example, let's say on some hosts, ssh daemons listen on customized 
  ports (say 2000) and you need to connect to that port to login those 
  hosts, while connecting to the regular port to login others. Then you 
  first define a new rsh-like command.
  
3.
  gxpc rsh ssh2000 ssh -p 2000 -o 'StrictHostKeyChecking no' -A %target% %cmd%

  And you specicy ssh2000 label to login those hosts, using 'use' command. e.g.,

     use ssh2000 <src> <target>

See Also:
  use explore

"""
        return u
        
    def do_rsh_cmd(self, args):
        if self.init3() == -1: return cmd_interpreter.RET_NOT_RUN
        opts = rsh_cmd_opts()
        if opts.parse(args) == -1:
            return cmd_interpreter.RET_NOT_RUN
        if len(opts.args) == 0:
            # gxpc rsh -> show available methods
            if opts.full:
                A = self.session.login_methods.items()
                A.sort()
                for rsh_name,cmd in A:
                    Ws(("%s : %s\n"
                        % (rsh_name, self.show_login_method_cmdline(cmd))))
            else:
                A = self.session.login_methods.keys()
                A.sort()
                Ws("%s\n" % string.join(A, ", "))
            return 0
        elif len(opts.args) == 1:
            # gxpc rsh ssh -> show command lines
            rsh_name = opts.args[0]
            if not self.session.login_methods.has_key(rsh_name):
                Ws("No such rsh-like method %s\n" % rsh_name)
                return cmd_interpreter.RET_NOT_RUN
            cmd = self.session.login_methods[rsh_name]
            Ws(("%s : %s\n"
                % (rsh_name, self.show_login_method_cmdline(cmd))))
            return 0
        else:
            # gxpc rsh ssh ssh %target% %cmd%
            rsh_name = opts.args[0]
            cmd = opts.args[1:]
            self.session.login_methods[rsh_name] = cmd
            return 0

    #
    # explore and helpers
    #

    def minimum_subtree(self, ptree, C):
        """
        a minimum target tree rooted at t including all keys in C.
        """
        children = []
        for c in ptree.children.values():
            x = self.minimum_subtree(c, C)
            if x is not None: children.append(x)
        if C.has_key(ptree):                # t.name?
            exec_flag = 1
        else:
            exec_flag = 0
        if len(children) > 0:
            return gxpm.target_tree(ptree.name, ptree.hostname, ptree.target_label,
                                    exec_flag, 0, ptree.eenv, children)
        elif exec_flag == 1:
            return gxpm.target_tree(ptree.name, ptree.hostname,ptree.target_label,
                                    exec_flag, 0, ptree.eenv, [])
        else:
            return None

    def show_login_method_cmdline(self, cmd_and_args):
        A = []
        for a in cmd_and_args:
            if re.search("\s", a):
                A.append("'%s'" % a)
            else:
                A.append(a)
        return string.join(A, " ")
                

    def conv_login_method_cmdline(self, cmd_and_args):
        # cmd_and_args : list of strings
        # convert %p% -> %(p)s
        # convert %p:10% -> %(p)s
        replaced_args = []
        default_args = {}               # p, q, r
        pat = re.compile("%([^%]*)%")
        for a in cmd_and_args:
            # say a is like "T%J:-16%"
            a_ = []
            while 1:
                m = pat.search(a)
                if m is None: break
                p = m.group(1)          # p : "J"
                b = m.start(1) - 1      # position of "T%J%"
                r = m.end(1) + 1        # position that follows "T%J%"
                a_.append(a[:b])        # append "T"
                idx = string.find(p, ":-") # see if p is like J:-5
                if idx == -1:
                    p_default = None
                else:
                    p,p_default = p[:idx], p[idx+2:]
                if p_default is not None:
                    default_args[p] = p_default
                if p == "cmd":
                    a_.append("%%(cmd)s") # append "%%(cmd)s"
                else:
                    a_.append("%%(%s)s" % p) # append "%(J)s"
                a = a[r:]               # examine what follows "T%J%"
            a_.append(a)
            ra = string.join(a_, "")
            replaced_args.append(ra)
        return replaced_args, default_args

    def mk_installer_cmd(self, method_name, user, target, seq, opts):
        """
        return a long command line that logins host as user
        using rsh-like method method_name
        """
        session = self.session
        if not session.login_methods.has_key(method_name):
            return None
        # rsh_args is like a
        #   [ "ssh", "%(target)s", "%(cmd)s" ]
        rsh_args,default_args = self.conv_login_method_cmdline(session.login_methods[method_name])
        # mandatory args
        mand_args = ("--target_label %s --root_gupid %s --seq %s" 
                     % (target, self.gupid, seq))

        # optional args, passed if specified
        opt_args = []
        if opts.timeout is not None:
            opt_args.append("--hello_timeout %f" % opts.timeout)
        if opts.install_timeout is not None:
            opt_args.append("--install_timeout %f" % opts.install_timeout)
        if opts.target_prefix is not None:
            opt_args.append("--target_prefix %s" % opts.target_prefix)
            # explore option
        elif self.opts.target_prefix is not None:
            # global option
            opt_args.append("--target_prefix %s" % self.opts.target_prefix)
        if opts.verbosity is not None:
            opt_args.append("--dbg %d" % opts.verbosity)
        for python in opts.python:
            opt_args.append("--python %s" % python)
        opt_args = string.join(opt_args, " ")

        # build --rsh xxx --rsh xxx ...
        dic = {}
        dic.update(default_args)
        dic.update({ "user" : user, "target" : target })
        for var,val in opts.subst_arg:
            dic[var] = val
            
        A = []
        for a in rsh_args:
            try:
                subst_a = (a % dic)
            except KeyError,e:
                p = e.args[0]
                Es(("gxpc: a=%s parameter '%s' specified in rsh (try 'gxpc rsh %s') missing\n"
                    "in explore command line (give it by 'gxpc explore -a %s=X ...')\n"
                    % (a, p, method_name, p)))
                return None
            except ValueError,e:
                Es(("gxpc: parameter substitution of '%s' failed %s\n"
                    % (a, e.args)))
                return None
            A.append("--rsh '%s'" % subst_a)
        rsh_args = string.join(A, " ")
        # gxp_dir = os.environ["GXP_DIR"]
        return ("python ${GXP_DIR}/inst_local.py %s %s %s"
                % (rsh_args, mand_args, opt_args))
    
    def replace_tgt_pat(self, m, tgt_pat):
        # Es("replace_tgt_pat %s\n" % tgt_pat)
        p = tgt_pat
        m_groups = m.groups()
        m_groupdict = m.groupdict()
        for i in range(len(m_groups)):
            # replace %1% -> m.group(1)
            # we like to replace %1% with m.group(1)
            # here, m_groups[i] == m.group(i+1)
            j = i + 1
            # Es("applying %s -> %s\n" % (("%%%d%%" % j), m_groups[i]))
            p = string.replace(p, ("%%%d%%" % j), m_groups[i])
        for k,v in m_groupdict.items():
            # Es("applying %s -> %s\n" % (("%%%s%%" % k), v))
            p = string.replace(p, ("%%%s%%" % k), v)
        # Es("replace_tgt_pat -> %s\n" % p)
        return p
        
    def mk_explore_cmd_cache(self, src_target_label, src_hostname, src_gupid,
                             tgt, aliases, opts, ng_cache):
        if ng_cache.has_key((src_gupid,tgt)): return None,None,None
        nid,t,cmd = self.mk_explore_cmd(src_target_label, src_hostname,
                                        src_gupid, tgt, aliases, opts)
        if cmd is None: ng_cache[src_gupid,tgt] = None
        return nid,t,cmd

    def mk_explore_cmd(self, src_target_label, src_hostname, src_gupid,
                       tgt, aliases, opts):
        """
        make an explore command string for src to reach tgt
        """
        for method_name,user,src_pat,tgt_pat in self.session.edges:
            # check if src_pat is either "hostname=xxx", "gupid=xxx",
            # or "target=xxx"
            m = re.match("(target|hostname|gupid)=(.*)", src_pat)
            if m:
                src_pat = m.group(2)
                if m.group(1) == "target":
                    name_to_match = src_target_label
                elif m.group(1) == "hostname":
                    name_to_match = src_hostname
                else:
                    assert m.group(1) == "gupid"
                    name_to_match = src_gupid
            else:
                name_to_match = src_target_label
            m = re.match(src_pat, name_to_match)
            if m:
                found = 0
                for t in aliases.get(tgt, [ tgt ]):
                    tgt_pat = self.replace_tgt_pat(m, tgt_pat)
                    # Es("matching %s to %s\n" % (tgt_pat, t))
                    if re.match(tgt_pat, t):
                        found = 1
                        break
                if found == 0: continue
                nid = self.session.gen_random_id()
                seq = "explore-%03d-%s-%s" % (nid, src_gupid, t)
                cmd = self.mk_installer_cmd(method_name, user, t, seq, opts)
                if cmd is not None:
                    if opts.verbosity >= 1:
                        Ws("gxpc : %s %s -> %s\n" % (method_name, src_gupid, tgt))
                    if opts.verbosity >= 3:
                        Ws("gxpc : %s\n" % cmd)
                    return nid,t,cmd
        return None,None,None

    def mk_explore_cmds(self, to_explore, aliases,
                        ch_soft_lim, ch_hard_lim, opts, ng_cache):
        """
        Make a dictionary of src -> commands, which says
        src host should issue commands to get new nodes.
        Each item of commands is a triple
        (node_id,target_label,command).

        For each target in to_explore, it finds an appropriate
        src host in peer_tree_node, which (1) is specified
        by an edges command, and (2) currently has less that
        max_ch children.

        
        """
        if opts.verbosity >= 1:
            mk_explore_cmds_start = time.time()
            Ws("gxpc : finding explorable pairs\n")
        # src (peer_tree_node) -> targets
        C = {}
        session = self.session
        # q = candidate src hosts
        q = [ session.peer_tree ]
	# for each node, try to find nodes directly reachable from it
        n_ok = 0                        # OK pairs
        n_ng = 0                        # NG pairs
        n_ng_nodes = 0                  # NG nodes
        
        while len(q) > 0 and len(to_explore) > 0:
            # Es("len(q) = %d len(to_explore) = %d\n" % (len(q), len(to_explore)))
            
            h = q.pop(0)
            if h.name is None: continue # this guy still in progress
            # children become candidate srcs
            for c in h.children.values(): q.append(c)
            # no hope to reach ANY node from this guy
            if ng_cache.has_key(h.name):
                n_ng_nodes = n_ng_nodes + 1
                continue
            # okay, now we get some appropriate target nodes for h
            blocked = []
            # ensure each node can add at least one child
            # THIS turned out to be a BAD IDEA. it may potentially takes
            # too much time exploring many nodes one at a time
            # max_ch = min(ch_hard_lim, max(ch_soft_lim, len(h.children) + 1))
            max_ch = ch_soft_lim
            while len(h.children) < max_ch and len(to_explore) > 0:
                # get next target until h's children become too many
                tgt = to_explore.pop(0)
                # check if h is allowed to issue cmd to tgt
                nid,a_tgt,cmd = self.mk_explore_cmd_cache(h.target_label, h.hostname, h.name, tgt, aliases, opts, ng_cache)
                if cmd is None:
                    # no, h cannot directly reach tgt
                    n_ng = n_ng + 1
                    blocked.append(tgt)
                    continue
                n_ok = n_ok + 1
                # record the fact that h should reach tgt
                # along with node id
                if not C.has_key(h): C[h] = []
                C[h].append((nid, a_tgt, cmd))

                # extend peer_tree
                t = peer_tree_node()
                t.cmd = cmd
                t.target_label = a_tgt
                assert not h.children.has_key(nid)
                h.children[nid] = t
                # mark we are currently exploring the target
                self.exploring[t] = 1
                # Ws("exploring %s -> %s %s %s\n" % (h.name, tgt, nid, t))

            if len(to_explore) == 0: ng_cache[h.name] = None
            for x in blocked: to_explore.append(x)
        if opts.verbosity >= 1:
            mk_explore_cmds_stop = time.time()
            Ws(("gxpc : found %d explorable pairs "
                "(with %d NG pairs %d NG nodes) in %.3f sec\n"
                % (n_ok, n_ng, n_ng_nodes,
                   mk_explore_cmds_stop - mk_explore_cmds_start)))
        return C

    def send_explore_msg(self, tid, pipes, C):
        """
        C : src -> list of (node_id,target_label,command_to_reach_it)

        build an explore msg to do things to grab them as new peers
        """
        if len(C) == 0: return 0        # nothing to do at all
        target = self.minimum_subtree(self.session.peer_tree, C)
        assert target is not None, (self.session.peer_tree, C)
        # okay we build the msg
        # clauses = []
        clauses = {}
        for src,cmds in C.items():
            # build actions performed by this particular src host
            actions = []
            for nid,tgt,cmd in cmds:
                # cwd/env = None
                a = gxpm.action_createpeer(nid, [], {}, cmd, pipes, [])
                actions.append(a)
            # clauses.append(gxpm.clause(src.name, actions))
            clauses[src.name] = actions
        # calc the target of this particular msg (include the src
        # hosts)
        gcmds = [ clauses ]
        m = gxpm.down(target, tid, 0, gxpm.keep_connection_until_fin, gcmds)
        # really send it
        self.ensure_connect()
        self.asend(gxpm.unparse(m))
        return 1

    def explore_some(self, tid, pipes, to_explore, aliases,
                     ch_soft_lim, ch_hard_lim, opts, ng_cache):
        """
        build and send a single msg to explore some nodes
        """
        C = self.mk_explore_cmds(to_explore, aliases,
                                 ch_soft_lim, ch_hard_lim, opts, ng_cache)
        # Es("mk_explore_cmds -> %s\n" % C)
        r = self.send_explore_msg(tid, pipes, C)
        return r

    def usage_explore_cmd(self, full):
        u = r"""Usage:
  gxpc explore [OPTIONS] TARGET TARGET ...
"""
        if full:
            u = u + r"""
Description:
  Login target hosts specified by OPTIONS and TARGET.

Options:
  --dry
    dryrun. only show target hosts
  --hostfile,-h HOSTS_FILE
    give known hosts by file
  --hostcmd HOSTS_CMD
    give known hosts by command output
  --targetfile,-t TARGETS_FILE
    give target hosts by file
  --targetcmd TARGETS_CMD
    give target hosts by command output
  --timeout SECONDS
    specify the time to wait for a remote host's response
    until gxp considers it dead
  --children_soft_limit N (>= 2)
    control the shape of the explore tree. if this value is N, gxpc
    tries to keep the number of children of a single host no more than N,
    unless it is absolutely necessary to reach requested nodes.
  --children_hard_limit N
    control the shape of the explore tree. if this value is N, gxpc
    keeps the number of children of a single host no more than N, in any event.
  --target_prefix PATH
    specify the directory in remote hosts in which gxp files are installed.
    default is ~/.gxp_tmp, meaning a temporary directory like
    ~/.gxp_tmp/RANDOM_NAME/gxp3 will be created and all files are installed
    there. automatically created if it does not exist. use e.g., /tmp/YOUR_NAME
    or something if you have ridicuously slow home directory.
  --verbosity N (0 <= N <= 2)
    set verbosity level (the larger the more verbose)
  --set_default
    if you set this option, options specified in this explore becomes the default.
    for example, if you say --timeout 20.0 and --set_default, timeout is set to
    20.0 in subsequent explores, even if you do not specify --timeout.
  --reset_default
    reset the default values set by --set_default.
  --show_settings
    show effective explore options, considering those given by command line and
    those specified as default values.

Execution of an explore command will conceptually consist of the
following three steps.

(1) Known Hosts: Know names of existing hosts, either by
--hostfile, --hostcmd, or a default rule. These are called
'known hosts.' -h is an acronym of --hostfile.

(2) Targets: Extract login targets from known hosts. They are
extracted by regular expressions given either by --targetfile,
--targetcmd, or directly by command line arguments. -t is
an acronym of --targetfile.

(3) gxpc will attempt to login these targets according to the
rules specified by `use' commands.

Known hosts are specified by a file using --hostfile option, or
by output of a command using --hostcmd. Formats of the two are
common and very simple. In the simplest format, a single file
contains a single hostname. For example,

   hongo001
   hongo002
   hongo004
   hongo005
   hongo006
   hongo007
   hongo008

is a valid HOSTS_FILE. If you specify a command that outputs
a list of files in the above format, the effect is the same
as giving a file having the list by --hostfile. For example,

  --hostcmd 'for i in `seq 1 8` ; do printf "%03d\n" $i ; done'

has the same effect as giving the above file to --hostfile.

The format of a HOSTS_FILE is actually a so-called /etc/hosts
format, each line of which may contain several aliases of the
same host, as well as their IP address. gxpc simply regards them
as aliases of a single host, wihtout giving any significance to
which columns they are in. Anything after `#' on each line is a
comment and ignored. Lines not containning any name, such as
empty lines, are also ignored.  The above simple format is
obviously a special case of this.

It is sometimes convenient to specify /etc/hosts as an argument
to --hostfile or to specify `ypcat hosts' as an argument to
--hostcmd. As a matter of fact, if you do not specify any of
--hostfile, --hostcmd, --targetfile, and --targetcmd, it is
treated as if --hostfile /etc/hosts is given.

Login targets are specified by a file using --targetfile option,
--targetcmd option, or by directly listing targets in the command
line. Format of them are common and only slightly different from
HOSTS_FILE.  The format of the list of targets in the command
line is as follows.

   TARGET_REGEXP [N] TARGET_REGEXP [N] TARGET_REGEXP [N] ...

where N is an integer and TARGET_REGEXP is any string that cannot
be parsed as an integer. That is, it is a list of regular
expressions, each item of which may optionally be followed by an
integer. The integer indicates how many logins should occur to
the target matching TARGET_REGEXP. The following is a valid
command line.

  gxpc explore -h hosts_file hongo00

which says you want to target all hosts beginning with hongo00,
among all hosts listed in hosts_file.  If, for example, you have
specified by `use' command that the local host can login these
hosts by ssh, you will reach hosts whose names begin with
hongo00.  If you instead say

  gxpc explore -h hosts_file hongo00 2

you will get two processes on each of these hosts.

If you do not give any of --targetfile, --targetcmd, and command
line targets, it is treated as if a regular expression mathing
any string is given as the command line target. That is, all
known hosts are targets.

Format of targets_host is simply a list of lines each of which
is like the list of arguments just explained above. Thus, the
following is a valid TARGETS_FILE.

  hongo00 2
  chiba0
  istbs
  sheep

which says you want to get two processes on each host beginning
with hongo00 and one process on each host beginning with chiba0,
istbs, or sheep. Just to illustrate the syntax, the same thing
can be alternatively written with different arrangement into
lines.

  hongo00 2 chiba0
  istbs sheep

Similar to hosts_file, you may instead specify a command line
producing the output conforming to the format of TARGETS_FILE.

We have so far explained that target_regexp is matched against a
pool of known hosts to generate the actual list of targets.
There is an exception to this. If TARGET_REGEXP does not match
any host in the pool of known hosts, it is treated as if the
TARGET_REGEXP is itself a known host. Thus,

  gxpc explore hongo000 hongo001

will login hongo000 and hongo001, because neither hosts_file nor
hosts_cmd hosts are given so these expressions obviously won't
match any known host. Using this rule, you may have a file that
explicitly lists all hosts and solely use it to specify targets
without using separate HOSTS_FILE. For example, if you have a
long TARGETS_FILE called targets like:

  abc000
  abc001
    ...
  abc099
  def000
  def001
    ...
  def049
  pqr000
  pqr001
    ...
  pqr149

and say

  gxpc explore -t targets

you say you want to get these 300 targets using whatever methods
you specified by `use' commands.

Unlike HOSTS_FILE, an empty line in TARGETS_FILE is treated as if
it is the end of file. By inserting an empty line, you can easily
let gxpc ignore the rest of the file. This rule is sometimes
convenient when targeting a small number of hosts within a
TARGETS_FILE.

Here are some examples.

1.

  gxpc explore -h hosts_file chiba hongo

Hosts beginning with chiba or hongo in hosts_file 
become the targets.

2.

  gxpc explore -h hosts_file -t targets_file

Hosts matching any regular expression in targets_file become
the targets.

3.

  gxpc explore -h hosts_file

All hosts in hosts_file become the targets.  Equivalent to `gxpc
explore -h hosts_file .'  (`.' is a regular expression mathing
any non-empty string).

4.

  gxpc explore -t targets_file

All hosts in targetfile become the targets. This is simiar to the
previous case, but the file format is different.  Note that in
this case, strings in targets_file won't be matched against
anything, so they should be literal target names.
     
5.

  gxpc explore chiba000 chiba001 chiba002 chiba003

chiba000, chiba001, chiba002, and chiba003 become the targets.

6.

  gxpc explore chiba0

Equivalent to `gxpc explore -h /etc/hosts chiba0' which is hosts
beginning with chiba0 in /etc/hosts become the targets. Useful
when you use a single cluster and all necessary hosts are listed
in that file.
     
7.

  gxpc explore

Equivalent to `gxpc explore -h /etc/hosts' which is in turn
equivalent to `gxpc explore -h /etc/hosts .'  That is, all hosts
in /etc/hosts become the targets.  This will be rarely useful
because /etc/hosts typically includes hosts you don't want to
use.
"""
        return u
        
    def do_explore_cmd(self, args):
        """
        explore [file1 file2 ...]
        """
        t0 = time.time()
        if self.init3() == -1: return cmd_interpreter.RET_NOT_RUN
        opts = explore_cmd_opts()
        if opts.parse(args) == -1:
            return cmd_interpreter.RET_NOT_RUN

        if opts.reset_default:
            self.session.default_explore_opts = None

        # ----------- set default values for opts
        if self.session.default_explore_opts is not None:
            opts.import_defaults(self.session.default_explore_opts)

        if opts.set_default:
            self.session.default_explore_opts = opts.copy()

        # ----------- give some default values for hosts and targets
        if len(opts.hostfile) + len(opts.hostcmd) + \
           len(opts.targetfile) + len(opts.targetcmd) + \
           len(opts.args) == 0:
            # no targets nor hostfiles specified. default is to use /etc/hosts
            opts.hostfile.append(opts.default_hostfile)
        elif len(opts.targetfile) + \
           len(opts.targetcmd) + len(opts.args) == 0:
            # no targets specified. default is to use everything
            # in hostfile or hostcmd
            opts.args.append(".")

        if opts.show_settings:
            Ws("explore options: %s\n" % opts)

        # obtain candidate hosts
        hp = etc_hosts_parser()
        aliases = hp.parse(opts.hostfile, opts.aliasfile,
                           opts.hostcmd, [])
        # obtain targets
        tp = targets_parser()
        target_spec = tp.parse(opts.targetfile, [],
                               opts.targetcmd, opts.args)
        targets = self.extract_target_hosts(target_spec, aliases)

        X = self.mk_targets_to_explore(targets, aliases)
        marked_targets,to_explore,reached = X
        n_to_explore = len(to_explore)

        if opts.dry:
            Ws("%d Reached %d New\n\n" % \
               (len(reached), len(to_explore)))
            for m,n,t in marked_targets:
                Ws("%s %03s %s\n" % (m, n, t))
            return 0
        else:
            # real work begins
            tid = "explore%s" % self.session.gen_random_id()
            # , 0
            pipes = self.setup_pipes(self.opts.atomicity, 0, # pty
                                     [ (1,1),(2,2) ], [ (0,0) ], [], "")
            got_sigint = 0
            ch_soft_lim = opts.children_soft_limit
            ch_hard_lim = opts.children_hard_limit
            if ch_soft_lim > ch_hard_lim:
                ch_soft_lim = ch_hard_lim
            # here we repeat trying to get some new hosts. we have a tree
            # of hosts we have already reached, and try to get all specified
            # targets somehow.
            ng_cache = {}
            random.shuffle(to_explore)
            iteration = 0
            while 1:
                iteration = iteration + 1
                while ch_soft_lim <= ch_hard_lim and len(to_explore) > 0:
                    self.explore_some(tid, pipes, to_explore,
                                      aliases, ch_soft_lim, ch_hard_lim,
                                      opts, ng_cache)
                    if len(self.exploring) > 0: break
                    if ch_soft_lim == ch_hard_lim: break
                    # ch_soft_lim = int(ch_soft_lim * 1.1) + 1
                    ch_soft_lim = int(ch_soft_lim * 2.0) + 1
                    ch_soft_lim = min(ch_soft_lim, ch_hard_lim)
                # we tried twice and still no one exploring -> quit
                n_exploring = len(self.exploring)
                if n_exploring == 0: break
                break_value = max(0, min(n_exploring - opts.min_wait, 
                                         n_exploring - int(opts.wait_factor * n_exploring)))
                time_limit = self.timeout_to_limit(self.opts.timeout)
                if opts.verbosity >= 1:
                    wait_start = time.time()
                    Ws(("gxpc : waiting for %d outstanding logins (out of %d) "
                        "to be resolved (time_limit = %s)\n"
                        % (n_exploring - break_value, n_exploring, time_limit)))
                # res = self.recv_loop_noex(break_value, time_limit)
                res = self.process_events_loop(None, tid, break_value, time_limit)
                if opts.verbosity >= 1:
                    wait_stop = time.time()
                    Ws(("gxpc : waited for %.3f sec\n"
                        % (wait_stop - wait_start)))
                # well done or interrupted
                if res == cmd_interpreter.RECV_CONTINUE: continue
                if res == cmd_interpreter.RECV_INTERRUPTED:
                    got_sigint = 1
                break
            if len(to_explore) > 0:
                Es("%d unreachable targets:\n" % len(to_explore))
                Es(" Use `use' command to specify how.\n")
                if ch_soft_lim >= ch_hard_lim:
                    Es(" Or, consider specifying --children_hard_limit N to increase"
                       " the maximum number of children of a single host."
                       " e.g., explore --children_hard_limit 50 .... \n")
                to_explore.sort()
                for u in to_explore:
                    Es(" %s\n" % u)
            if len(self.failed_to_explore) > 0:
                failed = self.failed_to_explore[:]
                failed.sort()
                Es("%d failed logins:\n" % len(failed))
                for f in failed:
                    Es(" %s\n" % f)
            t1 = time.time()
            Ws(("gxpc : took %.3f sec to explore %d hosts\n" %
                (t1 - t0, n_to_explore)))
            self.session.reset_exec_tree()
            if got_sigint:
                return cmd_interpreter.RET_SIGINT
            else:
                return 0
    #
    def usage_make1_cmd(self, full):
        u = r"""Usage:
  gxpc make GNU-MAKE-ARGS [ -- GXPC-MAKE-ARGS ]
"""
        if full:
            u = u + r"""
Description:
  Parallel and distributed make.

Options:
"""
        return u

    def mk_with_args(self, opts):
        args = []
        if opts.withall: args.append("--withall")
        args.append("--withmask")
        args.append("%d" % opts.withmask)
        for x,y in [ ("--withhostmask", opts.withhostmask),
                     ("--withhostnegmask", opts.withhostnegmask),
                     ("--withgupidmask", opts.withgupidmask),
                     ("--withgupidnegmask", opts.withgupidnegmask),
                     ("--withtargetmask", opts.withtargetmask),
                     ("--withtargetnegmask", opts.withtargetnegmask),
                     ("--withidxmask", opts.withidxmask),
                     ("--withidxnegmask", opts.withidxnegmask) ]:
            if y is not None:
                args.append(x)
                args.append(y)
        return args

    def enter_interactive_shell(self, args):
        gxp_dir = self.get_gxp_dir()
        return os.system("bash --rcfile %s/misc/shell_settings/bash -i"
                         % gxp_dir)

    def usage_i_cmd(self, full):
        u = r"""Usage:
  gxpc i
"""
        if full:
            u = u + r"""
Description:
  Enter a new shell with a gxpc session.
"""
        return u

    def do_i_cmd(self, args):
        if self.init2() == -1: return cmd_interpreter.RET_NOT_RUN
        # make sure we have session files before jobs run
        self.session.save(self.opts.verbosity)
        # pass session name to make sure child processes attach to
        # the right session
        if not os.environ.has_key("GXP_SESSION"):
            os.environ["GXP_SESSION"] = self.session_file
        assert os.environ["GXP_SESSION"] == self.session_file, \
               (os.environ["GXP_SESSION"], self.session_file)
        self.enter_interactive_shell(args)
        self.do_quit_cmd(args)

    def make_makectl_cmd(self, args):
        if self.init2() == -1: return cmd_interpreter.RET_NOT_RUN
        # make sure we have session files before jobs run
        self.session.save(self.opts.verbosity)
        gxp_dir = os.environ["GXP_DIR"]
        xmake = os.environ.get("GXP_XMAKE", "xmake")
        make = os.path.join(gxp_dir, os.path.join("gxpbin", xmake))
        # pass session name to make sure child processes attach to
        # the right session
        if not os.environ.has_key("GXP_SESSION"):
            os.environ["GXP_SESSION"] = self.session_file
        assert os.environ["GXP_SESSION"] == self.session_file, \
               (os.environ["GXP_SESSION"], self.session_file)
        # add options passed to the first e
        with_args = [ "---" ] + self.mk_with_args(self.opts)
        os.execvp(make, [ make ] + args + with_args)

    def do_make1_cmd(self, args):
        self.make_makectl_cmd(args)

    def usage_makectl_cmd(self, full):
        u = r"""Usage:
  gxpc makectl (leave|leave_now|join)
"""
        if full:
            u = u + r"""
Description:
  GXP daemons who receive this command will leave, leave immediately,
or join the gxp make computation.
Options:
"""
        return u

    def do_makectl_cmd(self, args):
        # give --ctl option to xmake
        self.make_makectl_cmd([ "--", "--ctl" ] + args)

    def usage_mapred_cmd(self, full):
        u = r"""Usage:
  gxpc mapred GNU-MAKE-ARGS [ -- GXPC-MAKE-ARGS ]
"""
        if full:
            u = u + r"""
Description:
  This is a map-reduce framework built on top of GXP make.
You can run map-reduce without writing any Makefile by yourself.
You specify various options in GNU-MAKE-ARGS in the form of 
var=val.  A simple example:

  gxpc mapred -j input=big.txt output=out.txt \
    mapper=./my_mapper reducer=./my_reducer

You will find the '-n' option of make useful, since it tells you
which commands are going to be executed by this command line.

  gxpc mapred -n input=big.txt output=out.txt \
    mapper=./my_mapper reducer=./my_reducer

Options:
You will probably want to specify at least the following.
  input=<input filename>     (default: "input")
  output=<output filename>   (default: "output")
  mapper=<mapper command>    (default: "ex_word_count_mapper")
  reducer=<reducer command>  (default: "ex_word_count_reducer")

<input filename> and <output filename> are filenames.
<mapper command> is a command that reads anything from stdin and 
writes key-value pairs.
<reducer command> is a command that reads key-value pairs from stdin 
in sorted order and writes arbitrary final outputs.

You will frequently want to specify the following.
  n_mappers=<number of map tasks>       (default: 4)
  n_reducers=<number of reduce tasks>   (default: 2)
  reader=<reader command>               (default: "ex_line_reader")
  int_dir=<intermediate directory>      (default: "int_dir")
  keep_intermediates=y  will keep all intermediate files in int_dir
  small_step=y  will execute the entire computation in small steps
  dbg=y   equivalent to keep_intermediates=y small_step=y (useful for debugging)

More options are:
  partitioner=<partitioner command>  (default: "ex_partitioner")
  pre_reduce_sorter=<sort command>   (default: "sort")
  final_merger=<merge command>       (default: "cat")

pre_reduce_sorter is a command that runs before each reducer.
It takes in the command line mappers' output filenames and should
output to the stdout the sorted list of key-value pairs.
final_merger takes the filenames of all the reducers' output and
outputs the final result.

"""
        return u

    def do_mapred_make_cmd(self, args):
        # give --ctl option to xmake
        # ugly : init2 just to get gxp_dir ...
        if self.init2() == -1: return cmd_interpreter.RET_NOT_RUN
        gxp_dir = os.environ["GXP_DIR"]
        mapred_mk = os.path.join(gxp_dir, os.path.join("gxpmake", "gxp_make_mapred.mk"))
        self.make_makectl_cmd([ "-f", mapred_mk ] + args)

    def usage_pp_cmd(self, full):
        u = r"""Usage:
  gxpc pp GNU-MAKE-ARGS [ -- GXPC-MAKE-ARGS ]
"""
        if full:
            u = u + r"""
Description:
  This is a simple parameter parallel framework built on top of GXP make.
You can run a simple parameter-sweep type parallel applications without 
writing any Makefile by yourself.  You specify various options in GNU-MAKE-ARGS 
in the form of var=val.  A simple example:

  gxpc pp -j cmd='./my_cmd -a $(a) $(f) ' a="1 2 3 4" f="a.txt b.txt c.txt" parameters="a f"

This will execute "./my_cmd -a $(a) $(f)" for all the 4x3=12 combinations
of a and f. parameters= is mandatory.  For all names listed in parameters,
you need to specify at least one value.  You will find the '-n' option of 
make useful, since it tells you which commands are going to be executed by 
this command line.

  gxpc pp -n cmd='./my_cmd -a $(a) $(f) ' a="1 2 3" f="a.txt b.txt c.txt"

"""
        return u


    def do_pp_cmd(self, args):
        # ugly : init2 just to get gxp_dir ...
        if self.init2() == -1: return cmd_interpreter.RET_NOT_RUN
        gxp_dir = os.environ["GXP_DIR"]
        pp_mk = os.path.join(gxp_dir, os.path.join("gxpmake", "gxp_make_pp.mk"))
        self.make_makectl_cmd([ "-f", pp_mk ] + args)

    def do_vgxp_cmd(self, args):
        # ugly : init2 just to get gxp_dir ...
        if self.init2() == -1: return cmd_interpreter.RET_NOT_RUN
        gxp_dir = os.environ["GXP_DIR"]
        vgxp_launcher = os.path.join(gxp_dir, "vgxp/vgxp_launcher.pl")
        os.execvp(vgxp_launcher, [ vgxp_launcher ] + args)

    def usage_vgxp_cmd(self, args):
        u = r"""Usage:
  gxpc vgxp
"""
        return u

    def js_like_cmd(self, cname, args):
        if self.init2() == -1: return cmd_interpreter.RET_NOT_RUN
        gxp_dir = os.environ["GXP_DIR"]
        gxp_js = os.path.join(gxp_dir, "gxp_js.py")
        python = sys.executable
        os.execvp(python, [ python, gxp_js ] + self.argv[1:])

    def usage_js_like_cmd(self, cname, full):
        u = r"""Usage:
  gxpc %s [OPTION ...]
""" % cname
        return u

    def do_js_cmd(self, args):
        self.js_like_cmd("js", args)

    def usage_js_cmd(self, full):
        return self.usage_js_like_cmd("js", full)
    
    def do_make_cmd(self, args):
        self.js_like_cmd("make", args)

    def usage_make_cmd(self, full):
        return self.usage_js_like_cmd("make", full)
    
    def do_mapred_cmd(self, args):
        self.js_like_cmd("mapred", args)

    def usage_mapred_cmd(self, full):
        return self.usage_js_like_cmd("mapred", full)
    
    def do_p_cmd(self, args):
        self.js_like_cmd("p", args)

    def usage_p_cmd(self, full):
        return self.usage_js_like_cmd("p", full)
    
    def do_gnu_parallel_cmd(self, args):
        self.js_like_cmd("parallel", args)

    def usage_gnu_parallel_cmd(self, full):
        return self.usage_js_like_cmd("gnu_parallel", full)
    

    # ---------- dispatcher ----------

    def dispatch(self):
        argv = self.opts.args
        # argv is like [ "e", "hostname" ]
        if len(argv) == 0: argv = [ "stat", "0" ]
        cname = argv[0]
        c = "do_%s_cmd" % cname
        if hasattr(self, c):
            start_t = time.time()
            method = getattr(self, c)
            r = method(argv[1:])
            if r == cmd_interpreter.RET_NOT_RUN:
                self.show_help_command(cname, 0) # full == 0
                return r
            # ugly, but necessary to avoid saving
            # session file just deleted in do_quit_cmd
            if cname == "quit" or cname == "i": return r
            if self.session is not None:
                assert self.init_level >= 2
                if self.opts.save_session:
                    if self.session.invalid and self.session.created == 0:
                        if self.opts.verbosity >= 2:
                            Es(("gxpc: session invalid (created=%d, dirty=%d). reload %s\n"
                                % (self.session.created, self.session.dirty, self.session_file)))
                        self.session = self.reload_session(self.session_file, 1)
                    self.session.save(self.opts.verbosity)
                else:
                    if self.opts.verbosity >= 2:
                        Es("gxpc: do not save session\n")
            else:
                assert self.init_level == 1
            if r == cmd_interpreter.RET_SIGINT:
                self.die_with_sigint()
            else:
                return r
        else:
            Es("gxpc: %s: no such command\n" % argv[0])
            return cmd_interpreter.RET_NOT_RUN

    def init_and_dispatch(self):
        if self.init1() == -1: return cmd_interpreter.RET_NOT_RUN
        return self.dispatch()

    def profile_run(self, f, file):
        import hotshot, hotshot.stats
        prof = hotshot.Profile(file)
        r = prof.runcall(f)
        prof.close()

        stats = hotshot.stats.load(file)
        stats.strip_dirs()
        stats.sort_stats('time', 'calls')
        stats.print_stats()
        return r

    def main(self, argv):
        # parse command line args
        self.argv = argv
        opts = interpreter_opts()
        self.opts = opts
        if opts.parse(argv[1:]) == -1: return -1
        if opts.help:
            return self.do_help_cmd([])
        
        if opts.profile is None:
            return self.init_and_dispatch()
        else:
            return self.profile_run(self.init_and_dispatch,
                                    opts.profile)

if __name__ == "__main__":
    sys.exit(cmd_interpreter().main(sys.argv))
    
# $Log: gxpc.py,v $
# Revision 1.76  2012/07/04 15:32:53  ttaauu
# added kyoto_cluster and kyoto_mpp support
#
# Revision 1.75  2012/07/04 14:46:14  ttaauu
# added rsh kyoto_mpp and kyoto_cluster
#
# Revision 1.74  2012/04/03 13:00:29  ttaauu
# *** empty log message ***
#
# Revision 1.73  2011/09/29 17:24:19  ttaauu
# 2011-09-30 Taura
#
# Revision 1.72  2011/07/30 13:21:19  ttaauu
# *** empty log message ***
#
# Revision 1.71  2011/06/03 15:59:50  ttaauu
# *** empty log message ***
#
# Revision 1.70  2011/03/22 04:39:59  ttaauu
# *** empty log message ***
#
# Revision 1.69  2011/03/09 09:59:19  ttaauu
# add # aff: name=xxx option to make
#
# Revision 1.68  2011/02/20 05:30:28  ttaauu
# *** empty log message ***
#
# Revision 1.67  2011/01/12 16:06:55  ttaauu
# *** empty log message ***
#
# Revision 1.66  2011/01/11 14:19:14  ttaauu
# *** empty log message ***
#
# Revision 1.65  2011/01/11 13:58:52  ttaauu
# *** empty log message ***
#
# Revision 1.64  2010/09/08 12:30:49  ttaauu
# ChangeLog 2010-09-09
#
# Revision 1.63  2010/09/08 04:08:22  ttaauu
# a new job scheduling framework (gxpc js). see ChangeLog 2010-09-08
#
# Revision 1.62  2010/06/11 16:54:25  ttaauu
# fixed torque_host
#
# Revision 1.61  2010/05/25 18:13:58  ttaauu
# support --translate_dir src,dst1,dst2,... and associated changes. ChangeLog 2010-05-25
#
# Revision 1.60  2010/05/20 14:56:56  ttaauu
# e supports --rlimit option. e.g., --rlimit rlimit_as:2g ChangeLog 2010-05-20
#
# Revision 1.59  2010/05/19 03:41:10  ttaauu
# gxpd/gxpc capture time at which processes started/ended at remote daemons. xmake now receives and displays them. xmake now never misses IO from jobs. ChangeLog 2010-05-19
#
# Revision 1.58  2010/05/15 14:13:25  ttaauu
# added --target_prefix to global options. ChangeLog 2010-5-15
#
# Revision 1.57  2010/05/09 04:55:28  ttaauu
# *** empty log message ***
#
# Revision 1.56  2010/04/13 19:27:38  ttaauu
# *** empty log message ***
#
# Revision 1.55  2010/03/05 09:13:48  ttaauu
# added vgxp see ChangeLog 2010-3-5
#
# Revision 1.54  2010/03/05 05:27:08  ttaauu
# stop extending PYTHONPATH. see 2010-3-5 ChangeLog
#
# Revision 1.53  2010/02/10 07:27:18  ttaauu
# experimental gxpc i command
#
# Revision 1.52  2010/01/31 15:06:48  ttaauu
# n_maps/n_reduces -> n_mappers/n_reducers
#
# Revision 1.51  2010/01/31 13:21:34  ttaauu
# added help for mapred and pp
#
# Revision 1.50  2010/01/31 11:08:06  ttaauu
# added parameter parallel framework
#
# Revision 1.49  2010/01/31 05:31:28  ttaauu
# added mapreduce support
#
# Revision 1.48  2009/12/31 20:40:42  ttaauu
# *** empty log message ***
#
# Revision 1.47  2009/12/31 20:09:48  ttaauu
# *** empty log message ***
#
# Revision 1.46  2009/12/31 20:06:33  ttaauu
# *** empty log message ***
#
# Revision 1.45  2009/12/30 20:01:00  ttaauu
# *** empty log message ***
#
# Revision 1.44  2009/12/30 19:54:50  ttaauu
# *** empty log message ***
#
# Revision 1.43  2009/12/29 04:17:11  ttaauu
# fixed error when /tmp/gxp-user-default does not exist
#
# Revision 1.42  2009/12/28 07:27:26  ttaauu
# made the behavior of explore more intuitive (ChangeLog 2009-12-28)
#
# Revision 1.41  2009/12/27 16:02:20  ttaauu
# fixed broken --create_daemon 1 option
#
# Revision 1.40  2009/09/27 17:15:14  ttaauu
# added comment on gxpm.py
#
# Revision 1.39  2009/09/18 15:44:12  ttaauu
# record individual job output in state.html
#
# Revision 1.38  2009/09/17 18:47:53  ttaauu
# ioman.py,gxpm.py,gxpd.py,gxpc.py,xmake: changes to track rusage of children and show them in state.txt
#
# Revision 1.37  2009/09/11 09:17:33  ttaauu
# fixed an explore bug that embeds wrong gxp_dir directory in the installer command line
#
# Revision 1.36  2009/09/07 12:22:26  ttaauu
# *** empty log message ***
#
# Revision 1.35  2009/09/06 20:05:46  ttaauu
# lots of changes to avoid creating many dirs under ~/.gxp_tmp of the root host
#
# Revision 1.34  2009/08/04 13:16:39  ttaauu
# *** empty log message ***
#
# Revision 1.33  2009/06/18 16:45:27  ttaauu
# *** empty log message ***
#
# Revision 1.32  2009/06/17 23:50:36  ttaauu
# experimental condor support
#
# Revision 1.31  2009/06/06 13:53:26  ttaauu
# embed headers, revision numbers, and logs
#
