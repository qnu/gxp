import sys
sys.path.append("../..")

import ioman

m = ioman.ioman()
proc = m.make_child_proc(["hostname"])
print "proc = %s" % proc
assert proc is not None

for ch in proc.r_channels.values():
    # return an event as soon as '\n' is read
    ch.set_expected("\n")

dead = 0
eof = 0
while dead < 1 or eof < 2:      # stdout and stderr
    ch,ev = m.process_an_event()
    # we receive either output from the command
    # or the notification of the process's death
    print ch,ev
    if isinstance(ch, ioman.rchannel_wait_child):
        assert len(ev.dead_processes) == 1
        p = ev.dead_processes[0]
        assert p is proc
        print ("process %s dead with status %d" 
               % (p, p.term_status))
        dead += 1
    elif isinstance(ch, ioman.rchannel):
        print "read from process (kind=%d) [%s]" % (ev.kind, ev.data)
        if ev.kind == ioman.ch_event.EOF: 
            eof += 1
