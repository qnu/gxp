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
# $Header: /cvsroot/gxp/gxp3/expectd.py,v 1.7 2010/05/25 18:13:58 ttaauu Exp $
# $Name:  $
#

import ioman,sys

# -------------------------------------------------------------------
# expect daemon
# -------------------------------------------------------------------

class expect_error(Exception):
    pass

class expectd(ioman.ioman):
    """
    interact with a single process.

    """
    def __init__(self):
        ioman.ioman.__init__(self)
        self.set_std_channels()
        self.proc = None

    def spawn(self, cmd):
        """
        spawn a subprocess with command line cmd (list of strings).
        """
        if self.proc is not None:
            raise expect_error("Child process already spawned")
        pipe_desc = [ (ioman.pipe_constructor_pipe,
                       [("r", 1, ioman.rchannel_process)],
                       [("w", 1)]),
                      (ioman.pipe_constructor_pipe,
                       [("r", 2, ioman.rchannel_process)],
                       [("w", 2)]),
                      (ioman.pipe_constructor_pipe,
                       [("w", 0, ioman.wchannel_process)],
                       [("r", 0)]) ]
        proc,msg = self.spawn_generic(ioman.child_process,
                                      cmd, pipe_desc, {}, [], [])
        if proc is None: 
            raise expect_error("Failed to create child process %s\n" % msg)
        self.proc = proc
        return proc

    def kill(self):
        return self.proc.kill()

    def kill_x(self, sig):
        return self.proc.kill_x(sig)

    def send(self, msg):
        """
        send msg (string) to the standard input of the
        spawned subprocess
        """
        if self.proc is None:
            raise expect_error("No child process")
        self.proc.write_stream(msg)

    def send_eof(self):
        """
        send EOF to the standard input of the
        spawned subprocess
        """
        if self.proc is None:
            raise expect_error("No child process")
        self.proc.write_eof()

    def expect(self, ex_out, forward_err):
        if self.proc is None:
            raise expect_error("No child process")
        c_stdin = self.proc.w_channels[0]
        c_stdout = self.proc.r_channels[1]
        c_stderr = self.proc.r_channels[2]
        # we say :
        # wait for ex_out to come from the process's stdout
        expected_dict = { c_stdout : ex_out }
        # forward everything from process's stderr to my stderr
        # forward everything from my stdin to process's stdin
        if forward_err:
            forward_dict = { c_stderr : (self.stderr,0),
                             self.stdin : (c_stdin,1) }
        else:
            forward_dict = { self.stdin : (c_stdin,1) }
        return self.expect_(expected_dict, forward_dict, 0)

    def expect_(self, expected_dict, forward_dict,
                return_on_any_event):
        """
        expected_dict : channel -> pattern
        forwar_dict   : channel -> (channel_to_forward,forward_eof)
        return_on_any_event : 0 or 1

        wait for the spawned process to send something via its
        stdout/stderr, or something to come from this process's
        stdin.

        For channels in expected_dict, we wait for a particular
        pattern to come and quit when it comes. For channels
        in forward_dict, we simply wait for anything to come and
        forward everything to the channel_to_forward.
        forward also EOF when forward_eof flag is 1.
        
        """
        for ch,ex in expected_dict.items():
            # set pattern to expect
            ch.set_expected(ex)
        for ch in forward_dict.keys():
            # say we accept everything
            ch.set_expected([("*",)])
        event_kind = None
        while 1:
            self.ch,self.ev = self.process_an_event()
            ch,ev = self.ch,self.ev
            assert ch is not None
            if expected_dict.has_key(ch):
                event_kind = ev.kind
            elif forward_dict.has_key(ch):
                fch,forward_eof = forward_dict[ch]
                if len(ev.data) > 0:
                    # foward any data 
                    fch.write_stream(ev.data)
                if forward_eof and ev.kind == ioman.ch_event.EOF:
                    # foward eof if so specified
                    fch.write_eof()
                elif ev.kind == ioman.ch_event.IO_ERROR:
                    # got error. log it.
                    ioman.LOG("expected error: [%s]\n" % ev.err_msg)
            if return_on_any_event or event_kind is not None:
                # quit if we got the expected pattern or
                # told to quit always
                break
        for ch in expected_dict.keys():
            ch.set_expected([])
        for ch in forward_dict.keys():
            ch.set_expected([])
        return event_kind

    def wait(self, forward_err):
        """
        wait for the spawned process to die. meanwhile,
        we forward everything from its stdout/err to my stdout/err,
        and everything from my stdin to its stdin.
        """
        if self.proc is None:
            raise expect_error("No child process")
        c_stdin = self.proc.w_channels[0]
        c_stdout = self.proc.r_channels[1]
        c_stderr = self.proc.r_channels[2]
        if forward_err:
            forward_dict = { self.stdin : (c_stdin,1),
                             c_stdout : (self.stdout,0),
                             c_stderr : (self.stderr,0) }
        else:
            forward_dict = { self.stdin : (c_stdin,1),
                             c_stdout : (self.stdout,0) }
        while len(self.processes) > 0 or \
                  c_stdout.is_closed() == 0 or \
                  c_stderr.is_closed() == 0:
            self.expect_({}, forward_dict, 1)
        term_status = self.proc.term_status
        self.proc = None
        return term_status

    def set_std_channels(self):
        i = ioman.rchannel(ioman.primitive_channel_fd(0, 1))
        o = ioman.wchannel(ioman.primitive_channel_fd(1, 1))
        e = ioman.wchannel(ioman.primitive_channel_fd(2, 1))
        self.add_rchannel(i)
        self.add_wchannel(o)
        self.add_wchannel(e)
        self.stdin = i                  # read my stdin from i
        self.stdout = o                 # write to my stdout via o
        self.stderr = e                 # write to my stderr via e

    def flush_outs(self):
        # ioman.Es("Closing standard out/in\n")
        o = self.stdout
        e = self.stderr
        o.write_eof()
        e.write_eof()
        while o.is_closed() == 0 or e.is_closed() == 0:
            ch,ev = self.process_an_event()

def test1():
    e = expectd()
    e.spawn([ "ssh", "okubo000", "python", "fofo.py" ])
    if e.expect([], 0) == ioman.ch_event.EOF:
        sys.stdout.write("<%s>\n" % e.ev.data)
    else:
        bomb()
    e.wait(0)
    # e.flush_outs()

if __name__ == "__main__":
    test1()
    
# $Log: expectd.py,v $
# Revision 1.7  2010/05/25 18:13:58  ttaauu
# support --translate_dir src,dst1,dst2,... and associated changes. ChangeLog 2010-05-25
#
# Revision 1.6  2010/05/20 14:56:56  ttaauu
# e supports --rlimit option. e.g., --rlimit rlimit_as:2g ChangeLog 2010-05-20
#
# Revision 1.5  2009/06/06 14:06:22  ttaauu
# added headers and logs
#
