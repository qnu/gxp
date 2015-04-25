#!/usr/bin/python

import os,getopt,sys

def dsh_rec(cmd, hosts):
    if len(hosts) == 1:
        os.execvp(cmd[0], cmd)
    else:
        max_ch = 200
        groups = []
        for i in range(0, max_ch):
            a =       i * len(hosts) / max_ch
            b = (i + 1) * len(hosts) / max_ch
            group = hosts[a:b]
            if len(group) > 0:
                groups.append(group)
        W = []
        for group in groups:
            r,w = os.pipe()
            pid = os.fork()
            if pid == 0:
                os.close(w)
                os.dup2(r, 0)
                hosts_args = []
                for h in group:
                    hosts_args.extend(["--host", h])
                os.execvp("ssh",
                          ["ssh", "-x",
                           "-o", "StrictHostkeyChecking no",
                           group[0], "/home/taue/proj/gxp3/ex/dsh_rec.py"] \
                          + hosts_args + cmd)
            os.close(r)
            W.append(w)
        for _ in groups:
            os.wait()
        
def main():
    try:
        opts,args = getopt.getopt(sys.argv[1:], "", ["host="])
    except getopt.error:
        sys.exit(2)
    hosts = []
    for o,a in opts:
        if o == "--host":
            hosts.append(a)
    dsh_rec(args, hosts)

main()
