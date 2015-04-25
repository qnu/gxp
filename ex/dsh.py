#!/usr/bin/python
import os

def run_dsh(cmd, hosts):
    hosts_args = []
    for h in hosts:
        hosts_args.extend(["--host", h])
    pid = os.fork()
    if pid == 0:
        # child
        os.execvp("./dsh_rec.py",
                  [ "./dsh_rec.py" ] + hosts_args + cmd)
    else:
        os.wait()

def istbs():
    hosts = []
    idxs = range(1, 192)
    dead = [24,42,106]
    for d in dead:
        if d in idxs: idxs.remove(d)
    for i in idxs[0:200]:
        hosts.append("istbs%03d" % i)
    return hosts

# run_dsh(["for i in `seq 0 2`; do hostname; done"], istbs())
run_dsh(["hostname"], istbs())

