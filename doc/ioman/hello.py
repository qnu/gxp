
import sys,os,time

pid = os.getpid()
slp = float(sys.argv[1])
for i in range(10):
    sys.stdout.write("pid %d : hello %d\n" % (pid, i))
    sys.stdout.flush()
    time.sleep(slp)

