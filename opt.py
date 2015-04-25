# Copyright (c) 2008 by Kenjiro Taura. All rights reserved.
# Copyright (c) 2007 by Kenjiro Taura. All rights reserved.
# Copyright (c) 2006 by Kenjiro Taura. All rights reserved.
# Copyright (c) 2005 by Kenjiro Taura. All rights reserved.
#
# THIS MATERIAL IS PROVIDED AS IS, WITH ABSOLUTELY NO WARRANTY 
# EXPRESSED OR IMPLIED.  ANY USE IS AT YOUR OWN RISK.
# 
# Permission is hereby granted to use or copy this program
# for any purpose,  provided the above notices are retained on all 
# copies. Permission to modify the code and to distribute modified
# code is granted, provided the above notices are retained, and
# a notice that the code was modified is included with the above
# copyright notice.
#
# $Header: /cvsroot/gxp/gxp3/opt.py,v 1.5 2009/09/06 20:05:46 ttaauu Exp $
# $Name:  $
#

import copy,getopt,os,string,sys,types

class cmd_opts:
    """
    simple command line parser.
    usage:
      1. inherit this class (class xxx_cmd_opts(cmd_opts): ...)
      2. define __init__ function (see below for details).
      3. o = xxx_cmd_opts()
         if o.parse(args) == 0:
            OK
         else:
            ERROR

      4. if o.parse returns zero, then you have options in fields of o.

    __init__ method should define fields that correspond to options.
    e.g.,

    def __init__(self):
        self.xyz = ...
        self.abc = ...

    this says, this accepts options --xyz and --abc.

    type and default values of each option can be specified by the value.
    
        self.xyz = ("i", 10)

    says xyz should be an integer and its default value is 10. types
    supported are:
      
        s or s* : string
        i or i* : int
        f or f* : float
        None    : flag (no arg)

        for s, i, f, and None, if the same option occurs multiple times
        in the arg list, the last value overwrites all previous values.
        so, --xyz 1 --xyz 2 will have the same effect as --xyz 2.

        for s*, i*, f*, all values are put in a list.

        if you want to have the single character option of the same
        option (e.g., if -n is a synonym for --no_exec), you can say this
        by

        self.n = "no_exec"

    a complete example of __init__:

        self.abc = ("i", 0)
        self.pqr = ("f", 1.0)
        self.xyz = ("s", "hello")
        self.abcd = ("i*", [])
        self.pqrs = ("f*", [])
        self.xyzw = ("s*", [])
        self.hoge = (None, 0)
        self.a = "abc"
        self.p = "pqr"
        self.x = "xyz"

    if you need a postcheck for validity of arguments, define postcheck()
    so that it return zero on success and -1 otherwise.
    
    
    """
    def __init__(self):
        # (type, default)
        # types supported
        #   s : string
        #   i : int
        #   f : float
        #   l : list of strings
        #   None : flag
        self.specified_fields = {}
    
    def __str__(self):
        A = []
        for k,v in self.__dict__.items():
            if k == "specified_fields": continue
            if len(k) == 1: continue
            if type(v) is types.ListType:
                X = []
                for x in v:
                    X.append("--%s %s" % (k, x))
                A.append(string.join(X, " "))
            else:
                A.append("--%s %s" % (k, v))
        return string.join(A + self.args, " ")

    def parse(self, argv):
        short,long = self.get_options()
        try:
            opts,args = getopt.getopt(argv, short, long)
        except getopt.error,e:
            self.Es("%s\n" % e.args[0])
            return -1
        for opt,arg in opts:
            if self.setopt(opt, arg) == -1: return -1
        if self.finalize_opts() == -1: return -1
        self.args = args
        return 0

    def get_options(self):
        """
        return things passed getopt
        """
        long_opts = []
        short_opts = []
        for o in self.__dict__.keys():
            if o == "specified_fields":
                continue
            elif len(o) == 1:
                long_o = getattr(self, o) # p -> parent
                typ,default = getattr(self, long_o)
                if typ is None:         # no arg   (-h)
                    short_opts.append(o)
                else:                   # want arg (-f file)
                    short_opts.append("%s:" % o)
            else:
                if o[-2:] == "__": 
                    oo = o[:-2]
                else:
                    oo = o
                typ,default = getattr(self, o)
                if typ is None:         # no arg   (--help)
                    long_opts.append("%s" % oo)
                else:                   # want arg (--file file)
                    long_opts.append("%s=" % oo)
        return string.join(short_opts),long_opts
            
    def Es(self, s):
        os.write(2, s)

    def safe_atoi(self, s, defa):
        try:
            return string.atoi(s)
        except ValueError:
            return defa

    def safe_atof(self, s, defa):
        try:
            return string.atof(s)
        except ValueError:
            return defa

    def setopt(self, o, a):
        if len(o) == 2:                 # -h
            field = getattr(self, o[1:])
        else:                           # --help
            field = o[2:]
        if hasattr(self, field):
            typ,val = getattr(self, field)
        else:
            field = ("%s__" % field) # ugly as__
            typ,val = getattr(self, field)

        # type check
        if typ is None:
            x = 1
        elif typ[0] == "i":             # int option
            x = self.safe_atoi(a, None)
            if x is None:
                self.Es("invalid argument for %s (%s)\n" % (o, a))
                return -1
        elif typ[0] == "f":             # float option
            x = self.safe_atof(a, None)
            if x is None:
                self.Es("invalid argument for %s (%s)\n" % (o, a))
                return -1
        elif typ[0] == "s":
            x = a
        else:
            bomb()

        if typ is None or len(typ) == 1:
            setattr(self, field, (typ,x))
        else:
            assert len(typ) == 2, typ
            assert typ[1] == "*", typ
            if val is None: 
                val = []
                setattr(self, field, (typ,val))
            val.append(x)
        self.specified_fields[field] = 1
        return 0

    def finalize_opts(self):
        for o in self.__dict__.keys():
            if o == "specified_fields":
                continue
            elif len(o) > 1:              # strip things like 'h'
                typ,val = getattr(self, o)
                setattr(self, o, val)
        return self.postcheck()

    def postcheck(self):
        return 0                        # OK

    def copy(self):
        return copy.deepcopy(self)

# $Log: opt.py,v $
# Revision 1.5  2009/09/06 20:05:46  ttaauu
# lots of changes to avoid creating many dirs under ~/.gxp_tmp of the root host
#
# Revision 1.4  2009/06/06 14:06:23  ttaauu
# added headers and logs
#
