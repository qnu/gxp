import re,string,sys

def preprocess(src, wp):
    fp = open(src, "rb")
    while 1:
        line = fp.readline()
        if line == "": break
        m = re.match("#include +([^ ]+)", string.rstrip(line))
        if m:
            preprocess(m.group(1), wp)
        else:
            wp.write(line)
    fp.close()

def main():
    wp = open(sys.argv[2], "wb")
    preprocess(sys.argv[1], wp)
    wp.close()

if __name__ == "__main__":
    main()
