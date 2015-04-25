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
# $Header: /cvsroot/gxp/gxp3/inst_remote.py,v 1.9 2009/09/06 20:05:46 ttaauu Exp $
# $Name:  $
#

#
# This is a program that is NOT supposed to run as is.
# inst_local.py concatenates a string that encodes contents
# to install, string in this file, and a string that invokes
# the main function of this program with appropriate arguments.
# The entire string is fed to the remote python interpreter.
#
# When invoked on the remote node, it creates a random directory
# under the specified directory (prefix), install everything (data),
# and then emits the specified msg (code) when it suceeds.

import base64,errno,os,random,sys,math,time

def safe_mkdir(directory, mode):
    """
    create directory with mode. be silent if exists.
    """
    try:
        if mode is None:
            os.mkdir(directory)
        else:
            os.mkdir(directory, mode)
    except OSError,e:
        if e.args[0] != errno.EEXIST: raise
    return 0

def safe_makedirs(directory, mode):
    """
    create directory and its ancestors as necessary
    """
    if os.path.exists(directory): 
        if os.path.isdir(directory): return 0
	os.write(2, "%s exists but not directory\n" % prefix)
    	return -1
    head,tail = os.path.split(directory)
    if head != directory:
        # this really talks about anything but root or ''
        if safe_makedirs(head, mode) == -1: return -1
        return safe_mkdir(directory, mode)

def mk_tmp_dir(root_directory, root_mode=None, mode=None):
    """
    create root_directory if not exist, and 
    make a unique temporary directory there.
    """
    if safe_makedirs(root_directory, root_mode) == -1:
        bomb("could not make tmp directory")
    for i in range(0, 10):
        t = int(math.floor(time.time() * 1000000.0) % 100000000.0)
        seq = random.randint(0, 999999)
        name = "gxp_%04d_%08d_%06d_" % (os.getpid(), t, seq)
        directory = os.path.join(root_directory, name)
        # create directory
        try:
            if mode is None:
                os.mkdir(directory)
            else:
                os.mkdir(directory, mode)
        except OSError,e:
            if e.args[0] == errno.EEXIST:
                continue
            else:
                raise
        return directory
    bomb("could not make tmp directory")

def rename_dir(root_directory, tmp_dir):
    """
    rename tmp_dir to another dir in root_directory.
    return the new name.
    """
    for i in range(0, 10):
        t = int(math.floor(time.time() * 1000000.0) % 100000000.0)
        seq = random.randint(0, 999999)
        name = "gxp_%04d_%08d_%06d" % (os.getpid(), t, seq)
        directory = os.path.join(root_directory, name)
        # create directory
        try:
            os.rename(tmp_dir, directory)
        except OSError,e:
            if e.args[0] == errno.EBUSY:
                continue
            else:
                raise
        return directory
    bomb("could not make tmp directory")
    

def install_file(path, type, mode, base64_content):
    """
    install file/directory/symlink etc. (according to type)
    with mode, as the name path. its content is given
    encoded in base64 format.
    """
    if type == "DIR":
        if mode is None:
            os.mkdir(path)
        else:
            os.mkdir(path, mode)
    elif type == "REG":
        flag = os.O_CREAT|os.O_WRONLY|os.O_TRUNC
        if mode is None:
            fd = os.open(path, flag)
        else:
            fd = os.open(path, flag, mode)
        if base64_content is not None:
            os.write(fd, base64.decodestring(base64_content))
        os.close(fd)
    elif type == "FIFO":
        if mode is None:
            os.mkfifo(path)
        else:
            os.mkfifo(path, mode)
    elif type == "LNK":
        os.symlink(path, content)
    else:
        bomb()

def install_files(prefix, gxp_top, data):
    """
    given a directory prefix (e.g., ~/.gxp_tmp), 
    make a temporary directory in it (e.g., ~/.gxp_tmp/gxp_hoge_1024),
    and install many files/directories/symlinks given as data under
    the temporary directory.
    data is a list of (path,type,mode,content) given to install_file,
    where each path is a relative path from the temporary directory.

    also create a flag file gxp3/REMOTE_INSTALLED to indicate this
    is a directory that was thus created automatically.
    """
    # expand ~ and $GXP_DIR etc.
    prefix = os.path.expandvars(os.path.expanduser(prefix))
    # make a temporary directory
    tmp_dir = mk_tmp_dir(prefix)
    flag = (("%s/REMOTE_INSTALLED" % gxp_top), 
            "REG", 0644, None)
    for rel_path,type,mode,base64_content in data + [ flag ]:
        path = os.path.join(tmp_dir, rel_path)
        install_file(path, type, mode, base64_content)
    # rename the temporary directory to something else
    return rename_dir(prefix, tmp_dir)

def find_py(pythons):
    for p in pythons:
        if os.system("type %s > /dev/null" % p) == 0:
            return p
    os.write(2, "inst_remote.py : fatal error : could not find python executable in %s !\n" % pythons)
    return None
    
def check_install_exec(python_cmds, first_script, first_args, 
                       second_script, second_args,
                       prefix, gxp_top, data, code):
    python_cmd = find_py(python_cmds)
    if python_cmd is None: os._exit(1)
    if data is None:
        if os.path.exists(first_script):
            # say installation finished
            os.write(1, ("%s OK\n" % code))
            # and exec
            # os.execvp(sys.executable, [ sys.executable, first_script ] + first_args)
            # changed because condor hides sys.executable
            os.execvp(python_cmd, [ python_cmd, first_script ] + first_args)
        else:
            # say I want data
            os.write(1, ("%s WD\n" % code))
            # wait for data
            data = eval(read_bytes(string.atoi(read_bytes(10))))
    # install the received data
    inst_dir = install_files(prefix, gxp_top, data)
    # say installation finished
    os.write(1, ("%s OK\n" % code))
    script = second_script % { "inst_dir" : inst_dir }
    # os.write(2, "executable = %s\n" % python_cmd)
    os.execvp(python_cmd, [ python_cmd, script ] + second_args)


# inst_local.py is supposed to append a string that invokes either
# install or check_install_exec function below. e.g.,
# install("~/.gxp_tmp", [ ("foo/bar", "REG", 0644, "..."),
#                      ("foo/xxx", "REG", 0644, "...") ],
#         "INSTALL1234")

# $Log: inst_remote.py,v $
# Revision 1.9  2009/09/06 20:05:46  ttaauu
# lots of changes to avoid creating many dirs under ~/.gxp_tmp of the root host
#
# Revision 1.8  2009/06/18 16:45:27  ttaauu
# *** empty log message ***
#
# Revision 1.7  2009/06/17 23:50:36  ttaauu
# experimental condor support
#
# Revision 1.6  2009/06/06 14:06:23  ttaauu
# added headers and logs
#
