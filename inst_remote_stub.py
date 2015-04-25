import os,string
def read_bytes(n):
    A = []
    while n > 0:
        x = os.read(0, n)
        assert x != ""
        A.append(x)
        n = n - len(x)
    return string.join(A, "")

exec(read_bytes(string.atoi(read_bytes(10))))
