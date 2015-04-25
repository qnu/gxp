import os

while 1:
    h = os.read(0, 1000)
    os.write(1, "got %s" % h)
    if h == "": break
