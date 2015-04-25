import sys
sys.path.append("../..")

import ioman

m = ioman.ioman()
n_procs = 2

#
# spawn two processes.  one prints "hello" every two sec,
# the other every three sec.
#

for i in range(n_procs):
    proc = m.make_child_proc(["python", "hello.py", "%f" % (i+2.0) ])
    print "proc = %s" % proc
    assert proc is not None
    for ch in proc.r_channels.values():
        # return an event as soon as '\n' is read
        ch.set_expected("\n")

dead = 0
eof = 0
while dead < n_procs or eof < n_procs * 2: # *2 because stdout and stderr
    print "*** dead = %d eof = %d ***" % (dead, eof)
    ch,ev = m.process_an_event()
    # we receive either output from the command
    # or the notification of the process's death
    print ch,ev
    if isinstance(ch, ioman.rchannel_wait_child):
        for p in ev.dead_processes:
            print ("process %s dead with status %d" 
                   % (p, p.term_status))
            dead += 1
    elif isinstance(ch, ioman.rchannel):
        print "read from process (kind=%d) [%s]" % (ev.kind, ev.data)
        if ev.kind == ioman.ch_event.EOF: 
            eof += 1
    else:
        assert 0
