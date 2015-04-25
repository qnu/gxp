#!/usr/bin/python
import os,commands,sys,time

def Es(s):
    sys.stderr.write(s)

def my_sys(s):
    Es("%s\n" % s)
    return os.system(s)

def my_gso(s):
    Es("%s\n" % s)
    return commands.getstatusoutput(s)

def main():
    random_file = "random_file"
    seed = int((time.time() - int(time.time())) * 1000000)
    my_sys("./gen_random_file.py %s 1000000 10 5 1.0 1.0 %d"
           % (random_file, seed))
    n = 30
    F = []
    for i in range(n):
        F.append("frag/%s.%04d" % (random_file, i))
        my_sys("../simple_record_reader %s %d,%d > %s" % (random_file, i, n, F[-1]))
    my_sys("cat %s > %s.out" % (" ".join(F), random_file))
    if my_sys("diff %s %s.out" % (random_file, random_file)) == 0:
        # my_sys("rm -f %s" % " ".join(F))
        print "OK"
    else:
        print "NG"

if __name__ == "__main__":
    main()
