import socket,sys
sys.path.append("../..")
import ioman

m = ioman.ioman()

rch,wch = m.make_client_sock(socket.AF_INET, socket.SOCK_STREAM, ("localhost", 10000))
rch.set_expected(["\n"])
wch.write_stream("abc\n")

eof = 0
while eof < 1:
    ch,ev = m.process_an_event()
    print ch,ev
    if isinstance(ev, ioman.wevent):
        pass
    elif isinstance(ev, ioman.revent):
        print "got from server (kind=%d) [%s]" % (ev.kind, ev.data)
        if ev.kind == ioman.ch_event.EOF:
            eof += 1


