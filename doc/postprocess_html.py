import re,string,sys

def postprocess(src, wp):
    fp = open(src, "rb")
    while 1:
        line = fp.readline()
        if line == "": break
        m = re.match("<!-- HTML (.*) -->", string.rstrip(line))
        if m:
            wp.write("%s\n" % m.group(1))
        else:
            wp.write(line)
    fp.close()

def main():
    wp = open(sys.argv[2], "wb")
    postprocess(sys.argv[1], wp)
    wp.close()

if __name__ == "__main__":
    main()
