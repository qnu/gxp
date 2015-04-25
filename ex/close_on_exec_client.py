import socket,sys,string

def Ws(s):
    sys.stdout.write(s)
    sys.stdout.flush()

def main():
    port = string.atoi(sys.argv[1])
    so = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    so.connect(("localhost", port))
    a = so.recv(1)
    Ws("got %s\n" % a)

main()
