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
# $Header: /cvsroot/gxp/gxp3/inst_local.py,v 1.31 2011/09/29 17:24:19 ttaauu Exp $
# $Name:  $
#

import base64,glob,os,random,signal,socket,stat,string
import sys,time,types
import ioman,expectd,opt,this_file


"""
This file is script which, when invoked, copies a specified
set of files to a remote node and brings up a specified command
there. Here is how it basically works.

1. When this script is invoked, if a file $GXP_DIR/REMOTE_INSTALLED
   exists, it first tries to run an rsh-like command, to check if the
   remote system already has necessary things installed.

   ($GXP_DIR/REMOTE_INSTALLED does not exist in the gxp3 installation
   directory. It is created by the remote installation process, so
   the above check means we never use gxp3 installation directory that
   happens to be the same path in the remote host).

2. When it does, we are done.

3. Otherwise we go ahead and install the things. Here is how.

4. We run an rsh-like command to run a python (bootstrapping)
   script that installs things. This bootstrapping script is
   created locally and fed into the python's standard input
   (i.e., we run 'python -' and sends the script to its stdin).

"""
#
# List of space-separated strings that are used as python interpreters
# I never exprienced other than this.
#
default_python = "python"

#
# Overall, we first try to run
#   <python> <default_first_script> <default_first_args_template>
# to see if this immediately brings up remote gxpd.
# %(target_label)s and %(root_gupid)s are replaced by options given
# in the command line
#

default_first_script = "$GXP_DIR/gxpd.py"

default_first_args_template = [ "--listen", "none:",
                                "--parent", "$GXP_GUPID",
                                "--target_label", "%(target_label)s",
                                "--root_gupid", "%(root_gupid)s" ]

#
# An arbitrary string that is used to indicate that the remote gxpd
# successfully brought up
#

default_hello = "hogehoge"

#
# When remotely installing gxp, here is where.
# 

default_target_prefix = "~/.gxp_tmp"

#
# Source files that the installer brings to the remote host.
#

default_src_files = [ "$GXP_DIR",
                      "$GXP_DIR/gxpc",
                      "$GXP_DIR/gxpc.py",
                      "$GXP_DIR/ioman.py",
                      "$GXP_DIR/gxpd.py",
                      "$GXP_DIR/expectd.py",
                      "$GXP_DIR/ifconfig.py",
                      "$GXP_DIR/inst_local.py",
                      "$GXP_DIR/inst_remote_stub.py",
                      "$GXP_DIR/inst_remote.py",
                      "$GXP_DIR/gxpm.py",
                      "$GXP_DIR/opt.py",
                      "$GXP_DIR/this_file.py",
                      "$GXP_DIR/gxpbin",
                      "$GXP_DIR/gxpbin/bomb",
                      "$GXP_DIR/gxpbin/bcp",
                      "$GXP_DIR/gxpbin/conn",
                      "$GXP_DIR/gxpbin/gifconfig",
                      "$GXP_DIR/gxpbin/psfind",
                      "$GXP_DIR/gxpbin/nodefind",
                      "$GXP_DIR/gxpbin/nicer",
                      "$GXP_DIR/gxpbin/micer",
                      "$GXP_DIR/gxpbin/gxp_sched",
                      "$GXP_DIR/gxpbin/gxp_mom",
                      "$GXP_DIR/gxpbin/gxpm2.py",
                      "$GXP_DIR/gxpbin/ifconfig2.py",
                      "$GXP_DIR/gxpbin/mapred.py",
                      "$GXP_DIR/gxpbin/opt2.py",
                      "$GXP_DIR/gxpbin/xmake",
                      "$GXP_DIR/gxpbin/xmake.20100414",
                      "$GXP_DIR/gxpbin/xmake.mk",
                      "$GXP_DIR/gxpbin/mksh",
                      "$GXP_DIR/gxpbin/xmake2.mk",
                      "$GXP_DIR/gxpbin/mksh2",
                      "$GXP_DIR/gxpbin/worker_prof",
                      "$GXP_DIR/gxpbin/qsub_wrap",
                      "$GXP_DIR/gxpbin/qsub_wrap_client",
                      "$GXP_DIR/gxpbin/consub",
                      "$GXP_DIR/gxpbin/tmsub",
                      "$GXP_DIR/gxpbin/tmsub.rb",
                      "$GXP_DIR/gxpbin/su_cmd",
                      "$GXP_DIR/gxpbin/gmnt",
                      "$GXP_DIR/gxpbin/topology.py",
                      "$GXP_DIR/gxpbin/gio",
                      "$GXP_DIR/gxpbin/pfsb.py",
                      "$GXP_DIR/gxpmake",
                      "$GXP_DIR/gxpmake/ex_block_reader",
                      "$GXP_DIR/gxpmake/ex_count_reducer",
                      "$GXP_DIR/gxpmake/ex_line_reader",
                      "$GXP_DIR/gxpmake/ex_partitioner",
                      "$GXP_DIR/gxpmake/ex_exchanger",
                      "$GXP_DIR/gxpmake/ex_record_reader",
                      "$GXP_DIR/gxpmake/ex_word_count_mapper",
                      "$GXP_DIR/gxpmake/ex_xml_reader",
                      "$GXP_DIR/gxpmake/read_helper.py",
                      ]

#
# The script that will be invoked on the remote host to move everything
# to the remote host
#

default_inst_remote_file = "$GXP_DIR/inst_remote.py"
default_inst_remote_stub_file = "$GXP_DIR/inst_remote_stub.py"

#
# After the installation is done, we invoke
#  <python> <second_script> <second_args_template>
#

# default_second_script = "$GXP_DIR/gxpd.py"
default_second_script = "%(inst_dir)s/$GXP_TOP/gxpd.py"
default_second_args_template = [ "--remove_self" ] + default_first_args_template

#
# Default timeout values
#

default_hello_timeout = 86400.0
default_install_timeout = 1000.0

dbg = 0

# -------------------------------------------------------------------
# options
# -------------------------------------------------------------------

class inst_options(opt.cmd_opts):
    def __init__(self):
        opt.cmd_opts.__init__(self)
        # ---------------- 
        #    mandatory arguments that must be supplied each time
        # ---------------- 
        # target label of the gxpd that eventually starts
        self.target_label = ("s", None)
        # self.created_explicitly = ("i", 0)
        # gupid of the root gxpd
        self.root_gupid = ("s", None)
        # sequence number
        self.seq = ("s", None)
        # list of rsh-like programs to run commands remotely
        self.rsh = ("s*", [])

        # ---------------- 
        # optional arguments that can be omitted and have 
        # reasonable default values above
        # ---------------- 
        # (1) list of possible python paths 
        self.python = ("s*", []) # if empty, use [ default_python ]
        # (2) the command which is run to test if things have 
        # already been installed, so there is no need to install
        self.first_script = ("s", default_first_script)
        self.first_args_template = ("s*", None)
        # (3) a string the remote node is supposed to say when
        # things brought up
        self.hello = ("s", default_hello)
        # (4) the directory (on the remote node) to install things to
        self.target_prefix = ("s", default_target_prefix)
        # (5) source files to copy to the remote node
        self.src_file = ("s*", default_src_files)
        # (6) template from which installation (bootstrapping) script
        # is created
        self.inst_remote_file = ("s", default_inst_remote_file)
        self.inst_remote_stub_file = ("s", default_inst_remote_stub_file)
        # (7) script and arguments to eventually run after installation
        self.second_script = ("s", default_second_script)
        self.second_args_template = ("s*", None)
        # (8) control timeout and verbosity
        self.hello_timeout = ("f", default_hello_timeout)
        self.install_timeout = ("f", default_install_timeout)
        self.dont_wait = (None, 0)
        self.dbg = ("i", 0)

    def postcheck(self):
        if len(self.python) == 0:
            self.python.append(default_python)
        if self.first_args_template is None:
            self.first_args_template = default_first_args_template
        if self.second_args_template is None:
            self.second_args_template = default_second_args_template

# -----------
# installer
# -----------

class installer(expectd.expectd):

    def Em(self, m):
        self.stderr.write_stream(m)
        # self.stderr.write_msg(m)

    def Wm(self, m):
        # self.stdout.write_stream(m)
        self.stdout.write_msg(m)

    def find_ancestor(self, path, top_dirs):
        """
        check if any of the directories in top_dirs is an ancestor
        of path. 
        e.g., path="/a/b/c" and top_dirs = [ "/a", "/x", "/y/z" ],
        it returns "/a", "/b/c"
        """
        for top in top_dirs:
            if string.find(path, top) == 0:
                return top,path[len(top) + 1:]
        return None,None
        
    def expand(self, path, dic):
        """
        expand $VAR and ~ , and collapse ../ and ./ things
        """
        if dic: path = path % dic
        return os.path.normpath(os.path.expandvars(os.path.expanduser(path)))

    def expands(self, paths, dic):
        """
        template : list of strings. each string may contain %(key)s
        dic : dictionary mapping keys to strings.
        apply for each string the mapping
        """
        A = []
        for path in paths:
            A.append(self.expand(path, dic))
        return A

    def read_file(self, file):
        fp = open(file, "rb")
        r = fp.read()
        fp.close()
        return r

    def subst_cmd(self, cmd, rsh_template):
        S = []
        for t in rsh_template:
            S.append(t % { "cmd" : cmd })
        return S

    def remote_installed(self):
        """
        true if this process is running the automatically installed gxpd
        """
        gxp_dir = os.environ["GXP_DIR"]
        flag = os.path.join(gxp_dir, "REMOTE_INSTALLED")
        if dbg>=2: 
            ioman.LOG("checking remote flag %s/%s\n" % (gxp_dir, flag))
        if os.path.exists(flag):
            if dbg>=2: ioman.LOG("exists, remotely installed\n")
            return 1
        else:
            if dbg>=2: ioman.LOG("does not exit, locally installed\n")
            return 0

    def find_top_dirs(self, inst_files):
        """
        find "top directories" among inst_files (normally default_src_files).
        top directory is a directory whose parent is not in the list
        (inst_files).
        e.g., if inst_files are: [ "a", "a/b", "a/c", "d/x" ],
        top_directories are "a" and "d"
        
        """
        top_dirs = []
        for path in inst_files:
            path = self.expand(path, None)
            # path is like /home/tau/proj/gxp3/hoge
            ancestor,_ = self.find_ancestor(path, top_dirs)
            if ancestor is None:
                top_dirs.append(path)
        return top_dirs

    def mk_installed_data(self, inst_files):
        """
        inst_files : list of filenames to install (normally,
        default_src_files)

        convert it into a list of self-contained information sent to
        the remote host that it can generate all contents from.
        e.g., for regular files, this procdure converts it to
        (parent_dir, "REG", mode, base64_encoded_contensht).

        along the way it finds "top directories" among them. top directory
        is a directory whose parent is not in the list (inst_files).
        e.g., if inst_files are: [ "a", "a/b", "a/c", "d/x" ],
        top_directories are "a" and "d"
        
        """
        top_dirs = self.find_top_dirs(inst_files)
        inst_data = []
        inst_data_log = []
        warned_non_existing_path = 0
        for path in inst_files:
            path = self.expand(path, None)
            # path is like /home/tau/proj/gxp3/hoge
            ancestor,rel_path = self.find_ancestor(path, top_dirs)
            assert ancestor is not None, (path, top_dirs, inst_files)
            inst_path = os.path.join(os.path.basename(ancestor), rel_path)
            if not os.path.exists(path):
                if warned_non_existing_path == 0:
                    self.Em("inst_local.py: %s does not exist.  "
                            "!!! please update your gxp with cvs up -d !!!\n"
                            % path)
                    warned_non_existing_path = 1
                continue
            mode = os.stat(path)[0]
            # see what kind of file is it
            if stat.S_ISREG(mode):
                content = base64.encodestring(self.read_file(path))
                inst_data.append((inst_path,
                                  "REG", stat.S_IMODE(mode), content))
                inst_data_log.append((inst_path,
                                      "REG", stat.S_IMODE(mode), "..."))
            elif stat.S_ISDIR(mode):
                inst_data.append((inst_path, "DIR", stat.S_IMODE(mode), None))
                inst_data_log.append((inst_path, "DIR", stat.S_IMODE(mode), None))
            elif stat.S_ISFIFO(mode):
                self.Em("inst_local.py:mk_program: %s: "
                        "fifo (ignored)\n" % path)
            elif stat.S_ISLNK(mode):
                self.Em("inst_local.py:mk_program: %s: "
                        "symbolic link (ignored)\n" % path)
            elif stat.S_ISBLK(mode):
                self.Em("inst_local.py:mk_program: %s: "
                        "block device (ignored)\n" % path)
            elif stat.S_ISCHR(mode):
                self.Em("inst_local.py:mk_program: %s: "
                        "char device (ignored)\n" % path)
            elif stat.S_ISSOCK(mode):
                self.Em("inst_local.py:mk_program: %s: "
                        "socket (ignored)\n" % path)
            else:
                bomb()
        return inst_data,inst_data_log

    def mk_program(self, O, code):

        """
        Return a string of python program which, when invoked without
        argument, installs all inst_files 
        under a randomly created directory under target_prefix.
        For example, say target_prefix='target', 
        src_files=[ 'abc', 'def' ], it will create
        target/RANDOM_DIR/abc and target/RANDOM_DIR/def.
        When successful, the program will write <code> into standard
        out. The actual logic is taken from inst_remote_file.
        """
        # append main function to inst_remote_file (normally inst_remote.py)
        if self.remote_installed():
            inst_data,inst_data_LOG = None,None # perhaps we need no install
        else:
            inst_data,inst_data_LOG = self.mk_installed_data(O.src_file)
        # if dbg>=2: ioman.LOG("inst_data:\n%r\n" % inst_data)
        first_script = self.expand(O.first_script, None)
        first_args = self.expands(O.first_args_template, O.__dict__)
        second_script = self.expand(O.second_script, None)
        second_args = self.expands(O.second_args_template, O.__dict__)
        gxp_top = os.environ["GXP_TOP"]
        if dbg>=2:
            main_LOG = ("""
check_install_exec(python=%r, 
                   first_script=%r, 
                   first_args=%r,
                   second_script=%r, 
                   second_args=%r, 
                   target_prefix=%r,
                   gxp_top=%r, 
                   inst_data=%r, code=%r)
""" % (O.python, first_script, first_args, 
       second_script, second_args,
       O.target_prefix, gxp_top, inst_data_LOG, code))

        main = ("check_install_exec(%r, %r, %r, %r, %r, %r, %r, %r, %r)"
                % (O.python, first_script, first_args, 
                   second_script, second_args,
                   O.target_prefix, gxp_top, inst_data, code))
        inst_remote_stub = self.read_file(self.expand(O.inst_remote_stub_file,
                                                      None))
        inst_remote = self.read_file(self.expand(O.inst_remote_file, None))
        inst_remote_and_main = ("%s\n%s\n" % (inst_remote, main))
        if dbg>=2:
            inst_remote_and_main_LOG = ("%s\n%s\n" % (inst_remote, main_LOG))
        prog = ("%s%10d%s" % (inst_remote_stub,
                              len(inst_remote_and_main), inst_remote_and_main))
        if dbg>=2: 
            prog_LOG = ("%s%10d%s" % (inst_remote_stub,
                                      len(inst_remote_and_main), inst_remote_and_main_LOG))
            ioman.LOG(("string to feed cmd:\n-----\n%s\n-----\n" 
                       % prog_LOG))
	# wp = open("progprog", "wb")
	# wp.write(prog)
	# wp.close()
        return len(inst_remote_stub),prog

    def expect_hello(self, hello, timeout, forward_err):
        OK       = ioman.ch_event.OK
        begin_mark = "BEGIN_%s " % hello
        end_mark = " END_%s" % hello
        if dbg>=2: 
            # self.Em
            ioman.LOG("expect %s %s\n" % (begin_mark, timeout))
        s = self.expect([ begin_mark, ("TIMEOUT", timeout)], forward_err)
        if s != OK: return (s, None)
        if dbg>=2: 
            # self.Em
            ioman.LOG("expect %s %s\n" % (end_mark, 2.0))
        s = self.expect([ end_mark, ("TIMEOUT", 2.0)], forward_err)
        if s != OK: return (s, None)
        return (OK, self.ev.data[:-len(end_mark)])

    def mk_python_cmdline(self, pythons, stub_sz):
        """
        return a shell command string like:

        if type --path <python1> ; then
           <python1> -c "import os; exec(os.read(0, <stub_sz>);" ;
        elif type --path <python2> ; then
           <python2> -c "import os; exec(os.read(0, <stub_sz>);" ;
        elif ...
           ...
        fi

        Essentially, we search for a usable python interpreter and
        executes whichever is found first.
        the strange code given to the python with -c option reads
        stub_sz bytes from the standard input and then exec it.
        what is actually read there is inst_remote_stub.py. It is
        another simple program that reads the specified number of
        bytes from the standard input and then executes it. the
        difference is that it can wait for as many bytes as specified
        even if read prematurely returns (see inst_remote_stub.py).
        what is eventually executed is inst_remote.py
        """
        P = []
        for python in pythons:
            body = ('%s -c "import os; exec(os.read(0, %d));" '
                    % (python, stub_sz))
            if len(P) == 0:
                p = ('if type %s > /dev/null; then exec %s ;' % (python, body))
            else:
                p = ('elif type %s > /dev/null; then exec %s ;' % (python, body))
            P.append(p)
        if len(P) > 0: 
            p = (' else echo no python interpreter found "(%s)" 1>&2 ; fi'
                 % string.join(pythons, ","))
            P.append(p)
        return ("/bin/sh -c '%s'" % string.join(P, ""))

    def spawn_gxpd(self, O):
        """
        run ssh, sh, qsub_wrap or whatever to eventually
        spawn off new gxpd.py (locally or remotely).
        """
        code = "INSTALL%09d" % random.randint(0, 999999999)
        if dbg>=2: ioman.LOG("code = %s\n" % code)
        stub_sz,prog = self.mk_program(O, code)
        python_cmd = self.mk_python_cmdline(O.python, stub_sz)
        sub = self.subst_cmd(python_cmd, O.rsh)
        if dbg>=2: 
            ioman.LOG("cmd to exec:\n%s\n" % sub)
        self.spawn(sub)
        self.send(prog)
        return code

    def wait_gxpd_to_bring_up(self, O, code):
        """
        wait for gxpd to send a msg saying it has brought up
        return None if it appears to have failed.
        """
        OK       = ioman.ch_event.OK
        if dbg>=2: 
            ioman.LOG("Wait for gxpd to send code [%s]\n" % code)
        if self.expect([code, ("TIMEOUT", O.hello_timeout)], 1) != OK:
            if dbg>=1: 
                ioman.LOG("Install NG\n")
                self.Em("Install NG\n")
            return None
        if dbg>=2: 
            ioman.LOG("Got code [%s]\n" % self.ev.data)
            ioman.LOG("Wait for gxpd to send OK or WD\n")
        if self.expect([" OK\n", " WD\n",
                        ("TIMEOUT", O.hello_timeout)], 1) != OK:
            if dbg>=1: 
                self.Em("Install NG\n")
                ioman.LOG("Install NG\n")
            return None
        if self.ev.data[-4:] == " WD\n":
            if dbg>=2: 
                ioman.LOG("Got WD, send install data\n")
            inst_data,inst_data_LOG = self.mk_installed_data(O.src_file)
            inst_data_str = "%r" % inst_data
            inst_data_msg = "%10d%s" % (len(inst_data_str), inst_data_str)
            if dbg>=2: 
                ioman.LOG(("inst_data_msg:-----\n%s-----\n"
                           % inst_data_msg))
            self.send(inst_data_msg)
        elif self.ev.data[-4:] == " OK\n":
            if dbg>=2: 
                ioman.LOG("Got OK, no need to send install data\n")
        if dbg>=2: 
            ioman.LOG("Wait for gxpd to send hello\n")
        s,g = self.expect_hello(O.hello, O.hello_timeout, 1)
        if s == OK: return g
        if dbg>=1: 
            ioman.LOG("Bring up NG\n")
            self.Em("Bring up NG\n")
        return None

    def wait_gxpd_to_finish(self, O, code):
        g = self.wait_gxpd_to_bring_up(O, code)
        if g is not None:
            # say
            #  "Brought up on GUPID ACCESS_PORT TARGET_LABEL HOSTNAME\n"
            # e.g.,
            #  "Brought up on hongo100-tau-2008-07-06-14-40-00-3878 None hongo hongo100\n"
            # ioman.LOG("Brought up on %s %s\n" % (g, O.seq))
            self.Wm("Brought up on %s %s\n" % (g, O.seq))
        else:
            if dbg>=2:
                ioman.LOG("gxpd did not bring up, killing the process with SIGINT\n")
            self.kill_x(signal.SIGINT)
            try:
                time.sleep(2.0)
            except KeyboardInterrupt:
                pass
            if dbg>=2:
                ioman.LOG("gxpd did not bring up, killing the process with SIGKILL\n")
            self.kill_x(signal.SIGKILL)
        # self.Wm("WAIT suruyo---------n\n")
        self.wait(1)
        self.flush_outs()

    def show_argv(self, argv):
        for a in argv:
            self.Em("'%s' " % a)
        self.Em("\n")

    def set_inst_environment(self):
        if dbg>=2:
            ioman.LOG("setting environment\n")
        env = os.environ
        if "GXP_DIR" not in env or "GXP_TOP" not in env:
            if dbg>=2:
                ioman.LOG("GXP_DIR or GXP_TOP not in environment, find them\n")
            gxp_dir,err = this_file.get_this_dir()
            if gxp_dir is None: 
                self.Em("%s\n" % err)
                return -1
            prefix,gxp_top = os.path.split(gxp_dir)
            env["GXP_DIR"] = gxp_dir
            env["GXP_TOP"] = gxp_top
        if "GXP_GUPID" not in env:
            if dbg>=2:
                ioman.LOG("GXP_GUPID not in environment, set it to default\n")
            env["GXP_GUPID"] = "gupid"
        if dbg>=2:
            for v in [ "GXP_DIR", "GXP_TOP", "GXP_GUPID" ]:
                ioman.LOG("%s : %s\n" % (v, env[v]))
        

    def main(self, argv):
        global dbg
        if dbg>=2: self.show_argv(argv)
        O = inst_options()
        if O.parse(argv[1:]) == -1: return
        dbg = O.dbg
        ioman.set_log_filename("log-%s" % O.seq)
        self.set_inst_environment()
        # spawn gxpd 
        code = self.spawn_gxpd(O)
        if O.dont_wait:
            # do not wait for gxpd until its death
            self.wait_gxpd_to_bring_up(O, code)
        else:
            self.wait_gxpd_to_finish(O, code)

def main():
    installer().main(sys.argv)

if __name__ == "__main__":
    main()

# $Log: inst_local.py,v $
# Revision 1.31  2011/09/29 17:24:19  ttaauu
# 2011-09-30 Taura
#
# Revision 1.30  2010/12/17 08:28:35  ttaauu
# *** empty log message ***
#
# Revision 1.29  2010/12/15 06:33:25  ttaauu
# *** empty log message ***
#
# Revision 1.28  2010/04/13 19:27:38  ttaauu
# *** empty log message ***
#
# Revision 1.27  2010/03/09 16:00:39  ttaauu
# *** empty log message ***
#
# Revision 1.26  2010/03/05 05:27:08  ttaauu
# stop extending PYTHONPATH. see 2010-3-5 ChangeLog
#
# Revision 1.25  2010/03/02 12:45:27  ttaauu
# added conn command ChangeLog 2010-3-2
#
# Revision 1.24  2010/02/05 02:43:49  ttaauu
# fixed an unhandled execption when inst_local.py tries to pack non-existing files
#
# Revision 1.23  2010/01/31 05:31:28  ttaauu
# added mapreduce support
#
# Revision 1.22  2009/12/31 20:06:33  ttaauu
# *** empty log message ***
#
# Revision 1.21  2009/12/27 16:02:20  ttaauu
# fixed broken --create_daemon 1 option
#
# Revision 1.20  2009/09/29 10:06:12  ttaauu
# fixed a bug in inst_local.py with --verbosity 2
#
# Revision 1.19  2009/09/06 20:05:46  ttaauu
# lots of changes to avoid creating many dirs under ~/.gxp_tmp of the root host
#
# Revision 1.18  2009/06/17 23:50:36  ttaauu
# experimental condor support
#
# Revision 1.17  2009/06/06 14:13:12  ttaauu
# fixed bug around moving gxpbin/{opt.py,mount_all} to obsolete/
#
# Revision 1.16  2009/06/06 14:06:23  ttaauu
# added headers and logs
#
