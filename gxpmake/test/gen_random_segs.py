#!/usr/bin/python

import os,random,stat,sys

def Ws(s):
    sys.stdout.write(s)

def gen_segments(seg_len_avg, seg_len_sigma, sz, seed):
    random.seed(seed)
    S = []
    a = 0
    while a < sz:
        seg_len = random.normalvariate(seg_len_avg, seg_len_sigma)
        seg_len = max(0, min(sz - a, int(seg_len + 0.5)))
        S.append((a, a + seg_len))
        a += seg_len
    assert (a == sz), a
    return S

def main():
    filename = "random_file"
    seg_len_avg = 30
    seg_len_sigma_frac = 1.0
    if len(sys.argv) > 1:
        filename = sys.argv[1]
    if len(sys.argv) > 2: seg_len_avg = int(sys.argv[2])
    if len(sys.argv) > 3: seg_len_sigma_frac = float(sys.argv[3])
    if len(sys.argv) > 4: seed = int(sys.argv[4])
    st = os.stat(filename)
    sz = st[stat.ST_SIZE]
    for x,y in gen_segments(seg_len_avg, 
                            int(seg_len_avg * seg_len_sigma_frac), sz, seed):
        Ws("%d,%d\n" % (x, y))
    sys.exit(0)

if __name__ == "__main__":
    main()
