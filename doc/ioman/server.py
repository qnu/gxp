import socket,sys
sys.path.append("../..")

import ioman

m = ioman.ioman()

ch_sock = m.make_server_sock(socket.AF_INET, socket.SOCK_STREAM, ("",0), 1)
ip,port = ch_sock.getsockname()

print "server listening on %s, please connect to it (perhaps by 'nc localhost %d')" % (port, port)
# wait for connection to come

connected = 0
disconnected = 0
while connected == 0 or disconnected < connected:
    print "process_an_event"
    ch,ev = m.process_an_event()
    print ch,ev
    if isinstance(ev, ioman.aevent):
        print "got connection. add to watch list"
        rch,wch = m.add_sock(ev.new_so)
        rch.set_expected(["\n"])
        connected += 1
    elif isinstance(ev, ioman.revent):
        print "got from client (kind=%d) [%s]" % (ev.kind, ev.data)
        if ev.kind == ioman.ch_event.EOF:
            disconnected += 1
    else:
        assert 0,ev
        
