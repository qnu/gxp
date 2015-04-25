import re,os

def get_this_file():
    g = globals()
    file = None
    if __name__ == "__main__" and \
       len(sys.argv) > 0 and len(sys.argv[0]) > 0:
        # run from command line (python .../gxpd.py)
        file = sys.argv[0]
    elif g.has_key("__file__"):
        # appears this has been loaded as a module
        file = g["__file__"]
        # if it is .pyc file, get .py instead
        m = re.match("(.+)\.pyc$", file)
        if m: file = m.group(1) + ".py"
    if file is None:
        return None,"cannot find the location of this_file.py"
    #file = os.path.abspath(file)
    file = os.path.realpath(file)
    if os.access(file, os.F_OK) == 0:
        return None,("source file %s is missing!\n" % file)
    elif os.access(file, os.R_OK) == 0:
        return None,("cannot read source file %s!\n" % file)
    else:
        return file,None

def get_this_dir():
    f,err = get_this_file()
    if f is None:
        return None,err
    else:
        a,b = os.path.split(f)
        if b != "this_file.py":
            return None,("filename (%s) != this_file.py" % b)
        return a,None
