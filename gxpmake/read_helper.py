#
# read_helper.py
#

import os,re,stat,sys
dbg=1

if not os.__dict__.has_key("SEEK_SET"):
    os.SEEK_SET = 0

def smart_parse_int(s):
    m = re.match("(\d+)([kKmMgGtTeE]?)", s)
    x = m.group(1)
    y = m.group(2)
    multiplier = 1
    if y in "kK":
        multiplier = 1024
    elif y in "mM":
        multiplier = 1024 * 1024
    elif y in "gG":
        multiplier = 1024 * 1024 * 1024
    elif y in "tT":
        multiplier = 1024 * 1024 * 1024 * 1024
    elif y in "eE":
        multiplier = 1024 * 1024 * 1024 * 1024 * 1024
    else:
        assert 0, s
    return x * multiplier

def find_pattern_fd(fd, start_offset, starter, terminator):
    """
    fd : file descriptor of the file to read data from
    start_offset : offset in the file to start reading at
    starter : pattern of the record start
    terminator : pattern of the record end

    return the offset at which starter starts
    """
    pattern = terminator + starter
    patlen = len(pattern)
    page_sz = 4096
    page_offset = start_offset - len(terminator)
    if page_offset < 0:
        page = terminator[0:-page_offset]
        file_offset = 0
    else:
        page = ""
        file_offset = page_offset
    os.lseek(fd, file_offset, os.SEEK_SET)
    while 1:
        #    +--------------+---------+
        #    |     page     |  delta  |
        #    +--------------+---------+
        # offset - len(terminator)
        delta = os.read(fd, page_sz)
        if delta == "": return page_offset + len(page)
        page = page + delta
        #             |idx
        #    +--------+--------------------------+
        #    |        |terminator+starter        |
        #    +--------+--------------------------+
        # offset - len(terminator)
        if dbg>=2: Es("page : (%s)\n" % page)
        idx = page.find(pattern)
        if dbg>=2: Es("idx : %d\n" % idx)
        if idx >= 0: return page_offset + idx + len(terminator)
        # leave the last len(pattern)-1 bytes
        if patlen == 1:
            new_page = ""
        else:
            new_page = page[-patlen+1:]
        if dbg>=2: 
            Es("page_offset : %d - %d + %d - %d\n" % 
               (page_offset, len(terminator), len(page), len(new_page)))
        page_offset = page_offset + len(page) - len(new_page)
        if dbg>=2: Es("page := (%s)\n" % new_page)
        page = new_page
    assert 0

def Es(s):
    sys.stderr.write(s)

def read_bytes_from_to(fd, begin, end, block_sz, wfd):
    """
    fd : file descriptor (int) to read data from
    begin/end : offsets 
    block_sz : size to read data at each read
    read fd from begin to end, each reading block_sz bytes
    and write data to wfd
    """
    i = os.lseek(fd, begin, os.SEEK_SET)
    while i < end:
        next_i = (i + block_sz) - (i + block_sz) % block_sz
        assert (next_i > i), (i, block_sz, next_i)
        next_i = min(next_i, end)
        if next_i == i: break
        block = os.read(fd, next_i - i)
        os.write(wfd, block)
        i = next_i
    os.close(fd)
    return 0

def read_records(filename, f, n, 
                 record_sz=0, starter="", terminator="", 
                 block_sz=1024*1024, wfd=1):
    """
    Read records in [f/n, (f+1)/n) portion of the filename.
    A record begins with "starter" and ends with "terminator".
    A record is in [f/n, (f+1)/n) portion iff its first
    byte is in the portion.
    """
    st = os.stat(filename)
    sz = st[stat.ST_SIZE]
    fd = os.open(filename, os.O_RDONLY)
    begin = (sz * f) / n
    end = (sz * (f + 1)) / n
    if record_sz != 0:
        begin_ = begin - (begin % record_sz)
        end_ = end - (end % record_sz)
    else:
        begin_ = find_pattern_fd(fd, begin, starter, terminator)
        end_ = find_pattern_fd(fd, end, starter, terminator)
    return read_bytes_from_to(fd, begin_, end_, block_sz, wfd)

