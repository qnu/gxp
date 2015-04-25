import sys,os

# a simple wrapper that allows the client 
# in this directory (gxpbin) to import ifconfig
# (in the gxp3 directory), without having it
# in pythonpath.
# this become necessary when it turned out
# including many paths under home dir in
# pythonpath makes things very slow in 
# EC2 environment (and some others too).
# so I started enforcing rules that files
# in gxpbin must be contained within this directory.

gxp_dir = os.path.realpath("%s/../.." % __file__)
sys.path.append(gxp_dir)

from opt import *

