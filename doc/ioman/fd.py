import sys
sys.path.append("../..")

import ioman

m = ioman.ioman()
ch_stdin = m.add_read_fd(0)
ch_stdin.set_expected(["\n"])

while 1:
    ch,ev = m.process_an_event()
    print ch,ev
    assert ch is ch_stdin
    assert isinstance(ev, ioman.revent)
    print "read from stdin (kind=%d) [%s]" % (ev.kind, ev.data)
    if ev.kind == ioman.ch_event.EOF: break
    

