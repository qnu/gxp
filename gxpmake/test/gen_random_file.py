#!/usr/bin/python
import string,sys,random

def gen_ramdom_file(wp, sz, word_len_avg, word_len_sigma, 
                    space_len_avg, space_len_sigma, seed):
    random.seed(seed)
    S = []
    l = 0
    while l < sz:
        wp.write(":::BEG::: ")
        word_len = random.normalvariate(word_len_avg, word_len_sigma)
        word_len = max(1, min(sz - l, int(word_len + 0.5)))
        l += word_len
        wp.write(random.choice(string.letters) * word_len)
        space_len = random.normalvariate(space_len_avg, space_len_sigma)
        space_len = max(0, min(sz - l, int(space_len + 0.5)))
        l += space_len
        if space_len > 0:
            wp.write(" " * (space_len - 1) + "\n")

def main():
    wp = sys.stdout
    sz = 1000000
    word_len_avg = 30
    space_len_avg = 10
    word_len_sigma_frac = 1.0
    space_len_sigma_frac = 1.0
    seed = 100
    if len(sys.argv) > 1 and sys.argv[1] != "-":
        wp = open(sys.argv[1], "wb") 
    if len(sys.argv) > 2: sz = int(sys.argv[2])
    if len(sys.argv) > 3: word_len_avg = int(sys.argv[3])
    if len(sys.argv) > 4: space_len_avg = int(sys.argv[4])
    if len(sys.argv) > 5: word_len_sigma_frac = float(sys.argv[5])
    if len(sys.argv) > 6: space_len_sigma_frac = float(sys.argv[6])
    if len(sys.argv) > 7: seed = int(sys.argv[7])
    gen_ramdom_file(wp, sz, word_len_avg, 
                    int(word_len_sigma_frac * word_len_avg), space_len_avg, 
                    int(space_len_sigma_frac * space_len_avg), seed)
    if wp is not sys.stdout: wp.close()
    sys.exit(0)

if __name__ == "__main__":
    main()

