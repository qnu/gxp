#!/usr/bin/python

import cPickle,cStringIO
import errno,heapq,os,random,re,socket,stat,string,sys

dbg=0

if not os.__dict__.has_key("SEEK_SET"):
    os.SEEK_SET = 0
    os.SEEK_CUR = 1

def Ws(s):
    sys.stdout.write(s)

def Es(s):
    sys.stderr.write(s)

# ------------------------------------------------------------
# helper for mapper functions
# ------------------------------------------------------------

class record_stream:
    """
    read the specified part of a file, respecting record terminator.
    usage:

    rs = record_steram("big_file", 10000, 10000000, "\n")
    while 1:
      r = rs.read_next_block()
      if r == "": break
      ... r ... # r is guaranteed to be a complete single line

    """
    def __init__(self, input_file, begin_pos, end_pos, terminator, req_sz=1024*1024):
        self.input_file = input_file
        self.begin_pos = begin_pos
        self.end_pos = end_pos  # end_pos < 0 indidates up to EOF
        self.terminator = terminator
        self.req_sz = req_sz
        self.fd = None          # file descriptor to read input_file
        self.read_pos = None    # file position of the input_file
        self.cur_pos = None     # position of the record to return next
        self.block = ""         # current block on memory
        self.pos_in_block = 0 # the offset within block to get next record from
        self.eof = 0            # 1 if we have encounterd EOF from the underlying file

    def show_state(self):
        if self.block is None or len(self.block) <= 20:
            b = self.block
        else:
            b = self.block[:17] + "..."
        return ("begin_pos=%s end_pos=%s read_pos=%s cur_pos=%s"
                " pos_in_block=%s eof=%s block=[%s]"
                % (self.begin_pos, self.end_pos, self.read_pos, self.cur_pos, 
                   self.pos_in_block, self.eof, b))

    def open(self):
        assert (self.fd is None), self.fd
        self.fd = os.open(self.input_file, os.O_RDONLY)
        self.block = ""
        self.pos_in_block = 0
        self.eof = 0
        # read the first record. basically, 
        # - we seek to begin_pos
        # - if we are given a middle of the file (i.e., begin_pos > 0),
        #   try to read up until we encounter the first terminater and DISCARD it,
        #   because that record is continuing from the left of begin_pos, so
        #   it should be read by the mapper working on that interval.
        # however, we may given a begin_pos that immediately follows a
        # terminater, in which case we should not discard it.
        #           begin_pos
        #           |
        # terminator|....
        # in order to accommodate this case, too, we go a length of terminater
        # backward and try to read a record and discard it.
        p = max(0, self.begin_pos - len(self.terminator))
        os.lseek(self.fd, p, os.SEEK_SET)
        self.read_pos = os.lseek(self.fd, 0, os.SEEK_CUR)
        self.cur_pos = self.read_pos
        if self.begin_pos > 0:
            # discard the first record
            self.read_next_record()

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None

    def read_bytes(self, sz):
        """
        read exactly sz bytes or until EOF from input_file.
        return whatever is read
        """
        if dbg>=2: Es("  -> read_bytes: %s\n" % self.show_state())
        assert self.fd is not None
        chunks = []
        r = sz
        while r > 0:
            c = os.read(self.fd, r)
            if c == "": break
            chunks.append(c)
            r = r - len(c)
        x = string.join(chunks, "")
        if dbg>=2: Es("  <- read_bytes: [%s] %s\n" % (x, self.show_state()))
        return x


    def read_next_block(self):
        """
        read block from the underlying input_file
        and append it to block
        """
        if dbg>=2: Es(" -> read_next_block: %s\n" % self.show_state())
        assert self.fd is not None
        if self.eof: return
        # basically try to read req_sz bytes,
        # but try to align the end of request to
        # req_sz
        next_pos = self.read_pos + self.req_sz
        if next_pos % self.req_sz > 0:
            next_pos = next_pos + self.req_sz - next_pos % self.req_sz
        new_block = self.read_bytes(next_pos - self.read_pos)
        self.read_pos = self.read_pos + len(new_block)
        self.block = self.block[self.pos_in_block:] + new_block
        self.pos_in_block = 0
        # we requested to read up to next_pos, but we couldn't,
        # so we must have encountered EOF
        if self.read_pos < next_pos: self.eof = 1
        if dbg>=2: Es(" <- read_next_block: %s\n" % self.show_state())

    def seek_terminater(self):
        """
        seek pattern terminator from
        self.block[self.pos_in_block:]. if not found,
        return -1, otherwise, return the position within
        the block immediately after the terminater (start position
        of the next record)
        """
        if dbg>=2: Es(" -> seek_terminater: %s\n" % self.show_state())
        assert self.fd is not None
        idx = string.find(self.block, self.terminator, self.pos_in_block)
        if idx == -1:
            r = -1
        else:
            r = idx + len(self.terminator)
        if dbg>=2: Es(" <- seek_terminater: [%d]\n" % r)
        return r

    def read_next_record(self):
        if dbg>=2: Es("-> read_next_record: %s\n" % self.show_state())
        assert self.fd is not None
        if self.cur_pos >= self.end_pos >= 0 or self.block is None: 
            if dbg>=2: Es("<- read_next_record: [] %s\n" % self.show_state())
            return "",self.input_file,self.cur_pos
        while 1:
            idx = self.seek_terminater()
            if idx >= 0: 
                rec = self.block[self.pos_in_block:idx]
                pos = self.cur_pos
                self.cur_pos = self.cur_pos + len(rec)
                self.pos_in_block = idx
                if dbg>=2: 
                    Es("<- read_next_record: [%s] %s\n" 
                       % (rec, self.show_state()))
                return rec,self.input_file,pos
            elif self.eof:
                rec = self.block[self.pos_in_block:]
                pos = self.cur_pos
                self.cur_pos = self.cur_pos + len(rec)
                self.pos_in_block = None
                self.block = None
                if dbg>=2: 
                    Es("<- read_next_record: [%s] %s\n" 
                       % (rec, self.show_state()))
                return rec,self.input_file,pos
            self.read_next_block()

#
# crude load balancing
#

class load_balancer:
    def __init__(self, input_files, scheme, mapper_idx, n_mappers):
        self.input_files = input_files
        self.scheme = scheme    # load balancing (data partitioning) scheme
        self.mapper_idx = mapper_idx
        self.n_mappers = n_mappers

    def get_blocks(self):
        if self.scheme == "file":
            return self.file_partitioning()
        elif self.scheme == "none":
            return self.no_partitioning() # return all data
        elif self.scheme is None or self.scheme == "block":
            return self.block_partitioning()
        else:
            Es("invalid partitioning scheme %s\n" % self.scheme)
            return None

    def get_files_and_sizes(self):
        files_and_sizes = []
        for input_file in self.input_files:
            st = os.stat(input_file)
            sz = st[stat.ST_SIZE]
            files_and_sizes.append((input_file, sz))
        return files_and_sizes

    def no_partitioning(self):
        files = []
        for input_file,sz in self.get_files_and_sizes():
            st = os.stat(input_file)
            files.append((input_file, 0, sz))
        return files

    def block_partitioning(self):
        total_sz = 0
        file_and_range = []
        for input_file,sz in self.get_files_and_sizes():
            file_and_range.append((input_file, total_sz, total_sz + sz))
            total_sz = total_sz + sz
        start_pos = (total_sz * self.mapper_idx) / self.n_mappers
        end_pos = (total_sz * (self.mapper_idx + 1)) / self.n_mappers
        files = []
        for f,s,e in file_and_range:
            ss = max(s, start_pos)
            ee = min(e, end_pos)
            if ss < ee:
                files.append((f, ss - s, ee - s))
        return files

    def file_partitioning(self):
        file_and_range = []
        for input_file in self.input_files:
            file_and_range.append((input_file, 0, -1))
        n = len(file_and_range)
        b = (n * self.mapper_idx) / self.n_mappers
        e = (n * (self.mapper_idx + 1)) / self.n_mappers
        return file_and_range[b:e]

# ------------------------------------------------
# reduction object running along with mapper
# ------------------------------------------------

class reducer1:
    """
    a reducer object running inside map job.
    represents an intermediate key,value pairs 
    for a single reduce job.
    a primary interface is add(key, val), which receives
    a key-val pair and store it, occasionally flushing
    them into file.
    """
    def __init__(self, reducer_idx, unit_fun, reducer_factory):
        self.reducer_idx = reducer_idx
        self.unit_fun = unit_fun
        self.reducer_factory = reducer_factory
        if reducer_factory:
            self.D = reducer_factory()
        else:
            self.D = {}

    def add(self, key, val):
        if self.unit_fun:
            v = self.unit_fun(val)
        else:
            v = val
        if self.D.has_key(key):
            self.D[key] += v
            return 0
        else:
            self.D[key] = v
            return 1

    def __len__(self):
        return len(self.D)

    def write_segment_header(self, n_chunks, wp):
        """
        write a segment header 
        """
        header = "\nBSEGMENT %9d %9d\n" % (self.reducer_idx, n_chunks)
        assert (len(header) == 30), header
        wp.write(header)
        return len(header)

    def write_segment_trailer(self, wp):
        """
        write chunk to wp with header and trailer
        """
        trailer = "\nESEGMENT\n"
        assert (len(trailer) == 10), trailer
        wp.write(trailer)
        return len(trailer)

    def flush_chunk(self, items, wp):
        payload = cPickle.dumps(items)
        header = "\nBEGCHUNK %9d\n" % len(payload)
        assert (len(header) == 20), header
        wp.write(header)
        wp.write(payload)
        trailer = "\nENDCHUNK\n"
        assert (len(trailer) == 10), trailer
        wp.write(trailer)
        return len(header) + len(payload) + len(trailer)

    def flush_segment(self, wp, start_offset):
        """
        flush the buffered key,val pairs into file object wp,
        in sorted order. 
        they are split into chunks and together constitute a 
        segment.  we end up with writing something like this:

        BSEGMENT 3 7   # 3: reducer index 7: number of chunks
        BEGCHUNK 100   # the 1st chunk
          ...
        END_CHUNK
        BEGCHUNK 120   # the 2nd chunk
          ...
        END_CHUNK
          
          ... <more chunks> ...

        BEGCHUNK 113   # the 7th chunk
          ...
        END_CHUNK
        ESEGMENT
        """
        if len(self.D) == 0: return
        items = self.D.items()
        items.sort()
        # FIX. just for testing. need to examine the length of 
        # chunk more decently
        h = max(1, (len(items) + 1) / 2)
        n_chunks = (len(items) + h - 1) / h
        o0 = wp.tell()
        sz = 0
        sz = sz + self.write_segment_header(n_chunks, wp)
        for x in range(0, len(items), h):
            sz = sz + self.flush_chunk(items[x:x+h], wp)
        sz = sz + self.write_segment_trailer(wp)
        self.D.clear()
        o1 = wp.tell()
        assert (o1 - o0 == sz), (o0, o1, o1 - o0, sz)
        return n_chunks,sz

class reducers:
    """
    """
    def __init__(self, mapper_idx, n_reducers, 
                 output_file, idx_file, unit_fun, reduce_factory):
        self.mapper_idx = mapper_idx
        self.n_reducers = n_reducers
        self.output_file = output_file
        self.idx_file = idx_file
        self.unit_fun = unit_fun
        self.reduce_factory = reduce_factory
        self.wp = None
        self.offset = None      # current offset
        self.segment_offsets = {} # dest_idx -> list of segment offsets
        self.R = {}             # dest_idx -> reducer1
        # the amount of data currently held in memory
        self.on_mem_sz = 0
        # the maximum amount of data on memory
        self.max_on_mem_sz = (10 * 1024 * 1024) / n_reducers
        # self.max_on_mem_sz = 0

    def __len__(self):
        return self.on_mem_sz

    def ensure_dir(self, d):
        try:
            os.makedirs(d)
        except OSError,e:
            if e.args[0] != errno.EEXIST: raise
            
    def ensure_open(self):
        if self.wp is None:
            self.ensure_dir(os.path.dirname(self.output_file))
            self.wp = open(self.output_file, "wb")
            self.offset = 0

    def write_idx_file(self):
        """
        write an index file telling segment locations
        for each destination reducer

        (output_file, [ (dest, [ seg, seg, seg, ... ]),
                        (dest, [ seg, seg, seg, ... ]),
                           ...
                      ])
        """
        seg_offsets = self.segment_offsets.items()
        # sort them by destination index. not strictly necessary
        seg_offsets.sort()
        data = (self.output_file, seg_offsets)
        idx_wp = open(self.idx_file, "wb")
        idx_wp.write(cPickle.dumps(data))
        idx_wp.close()

    def close(self):
        self.flush_some(1)
        if self.wp:
            self.wp.close()
            self.wp = None
            self.write_idx_file()

    def partitioning_function(self, key):
        return hash(key)

    def flush_some(self, flush_all):
        n = max(len(self.R), 2) - 1
        # we like to maintain buffered
        # data after this procedure <= max_unflushed
        if flush_all:
            max_unflushed = 0
        else:
            max_unflushed = len(self) / 8
        min_flushed = len(self) - max_unflushed
        # chunks >= max_unflushed must be flushed
        threshold = (max_unflushed + n - 1) / n
        # keep track of how much flushed
        flushed = 0
        unflushed = 0
        for dest_idx,r in self.R.items():
            l = len(r)
            if l >= threshold:
                n_chunks,sz = r.flush_segment(self.wp, self.offset)
                if not self.segment_offsets.has_key(dest_idx):
                    self.segment_offsets[dest_idx] = []
                self.segment_offsets[dest_idx].append((self.offset, n_chunks))
                self.offset = self.offset + sz
                del self.R[dest_idx]
                flushed = flushed + l
            else:
                unflushed = unflushed + l
        assert flushed + unflushed == len(self), \
            (flushed, unflushed, len(self))
        assert (flushed >= min_flushed), \
            (flushed, min_flushed, unflushed, n_reducers, 
             len(self), threshold, flush_all)
        self.on_mem_sz = unflushed

    def add(self, key, val):
        """
        add (key, val) pair
        """
        reducer_idx = self.partitioning_function(key) % self.n_reducers
        if not self.R.has_key(reducer_idx):
            self.R[reducer_idx] = reducer1(reducer_idx, self.unit_fun, self.reduce_factory)
        self.on_mem_sz += self.R[reducer_idx].add(key, val)
        if self.on_mem_sz >= self.max_on_mem_sz:
            self.flush_some(0)

# 
# test procedures for readers
# 
# split the entire range [0, filesize) into many non-overlapping subranges.
# for each subrange [a, b), read that portion of the file.
# concatinate all results and compare it with the file's content
#

class test_record_stream_test:

    def read_from_to(filename, terminator, begin_i, end_i):
        """
        read file from begin_i to end_i
        """
        blocks = []
        rr = record_stream(filename, "\n", begin_i, end_i)
        rr.open()
        y = None
        while 1:
            x,_,_ = rr.read_next_record()
            if x == "": break
            if y is not None:
                assert (y[-1] == "\n"), y
            blocks.append(x)
            y = x
        rr.close()
        return string.join(blocks, "")

    def read_from_to_test_1(self, filename, terminator, seed, n, ans):
        st = os.stat(filename)
        sz = st[stat.ST_SIZE]
        O = [ 0, sz ]
        R = random.Random()
        R.seed(seed)
        for i in range(n):
            O.append(R.randint(0, sz))
        O.sort()
        S = []
        for i in range(len(O) - 1):
            s = read_from_to(filename, terminator, O[i], O[i+1])
            S.append(s)
        S = string.join(S, "")
        if ans == S:
            print "OK ", seed, n
            return 1
        else:
            print "NG ", seed, n, O
            return 0
        
    def read_from_to_test(self, filename, terminator):
        fp = open(filename, "rb")
        ans = fp.read()
        fp.close()
        n = 1
        while n < len(ans) * 4:
            for seed in range(10):
                if read_from_to_test_1(filename, terminator, seed, n, ans) == 0:
                    return
            n = n * 2

    def do_test(self):
        self.read_from_to_test("a", "\n")

# ------------------------------------------------
# read mapper output
# ------------------------------------------------

class segment_stream:
    def __init__(self, filename, segment_offset, n_chunks):
        """
        stream to read chunks in a single segment
        """
        # name of the file to read data from
        self.filename = filename
        # starting offset of the segment within the file
        # (where "\nBSEGMENT ..." header begins)
        self.segment_offset = segment_offset 
        # number of chunks in it
        self.n_chunks = n_chunks
        # offset at whcih the next chunk starts
        self.chunk_offset = None
        # the id of the current chunk (starting from zero)
        self.chunk_idx = None
        self.key = None
        self.val = None
        self.key_vals = []

    def read_segment_header(self, fp):
        """
        read the segment header from fp.
        note fp is shared by other segment streams, but
        we assume here it is already positioned at the
        right offset.
        """
        x = fp.read(30)
        assert (len(x) == 30), x
        m = re.match("\nBSEGMENT +(\d+) +(\d+)\n", x)
        assert m, x
        reducer_idx = int(m.group(1))
        n_chunks = int(m.group(2))
        return reducer_idx,n_chunks,len(x)

    def read_segment_trailer(self, fp):
        """
        write chunk to wp with header and trailer
        """
        x = fp.read(10)
        assert (x == "\nESEGMENT\n"), x
        return len(x)

    def read_chunk(self, fp):
        """
        write chunk to wp with header and trailer
        """
        h = fp.read(20)
        assert (len(h) == 20), h
        m = re.match("\nBEGCHUNK +(\d+)\n", h)
        assert m, h
        sz = int(m.group(1))
        x = fp.read(sz)
        assert len(x) == sz, (len(x), sz)
        t = fp.read(10)
        assert (t == "\nENDCHUNK\n"), t
        return cPickle.loads(x), (len(h) + sz + len(t))

    def show_segment_stream(self):
        """
        print the content of the segment for debugging
        """
        Ws("    filename=%s:\n" % self.filename)
        fp = open(self.filename, "rb")
        fp.seek(self.segment_offset)
        reducer_idx,n_chunks,header_sz = self.read_segment_header(fp)
        for i in range(n_chunks):
            chunk,sz = self.read_chunk(fp)
            Ws("      chunk[%d] data:\n" % i)
            # data should be a sorted list of (key,value)s
            for key,val in chunk:
                Ws("        %s %s\n" % (key, val))
        trailer_sz = self.read_segment_trailer(fp)
        fp.close()

    def read_next_chunk(self, fp):
        """
        read next chunk from file
        """
        # not necessary, but we call this only when this holds, so
        # just to make it clear
        assert (len(self.key_vals) == 0), self.key_vals
        if self.chunk_idx >= self.n_chunks: return 0 # EOF
        if self.chunk_offset is None:
            # this is the first time, we read from the beginning
            fp.seek(self.segment_offset)
            reducer_idx,n_chunks,header_sz = self.read_segment_header(fp)
            self.chunk_idx = 0
            if 1:
                pos = fp.tell()
                assert (pos == self.segment_offset + header_sz), \
                    (pos, self.segment_offset, header_sz)
            self.chunk_offset = self.segment_offset + header_sz
        else:
            fp.seek(self.chunk_offset)
        chunk,sz = self.read_chunk(fp)
        if 1:
            pos = fp.tell()
            assert (pos == self.chunk_offset + sz), \
                (pos, self.chunk_offset, sz)
        self.chunk_idx = self.chunk_idx + 1
        self.chunk_offset = self.chunk_offset + sz
        self.key_vals.extend(chunk)
        return 1

    def read_next_key_val(self, fp):
        if len(self.key_vals) == 0:
            if self.read_next_chunk(fp) == 0:
                return 0
        self.key,self.val = self.key_vals.pop(0)
        return 1

class segments_merging_stream:
    """
    merge segments up to some limit
    """
    def __init__(self, idx_files, reducer_idx):
        self.idx_files = idx_files
        self.reducer_idx = reducer_idx
        # list of (filename, list of (segment_offset, sz)),
        # computed in open
        self.indexes = []       # 
        self.streams = []       # heapq of (key, segment_stream)
        self.fps = {}           # filename -> fp

    def read_idx_files(self):
        """
        read index files generated by mappers
        (one index file per mapper, though this code does not
        assume anything about how they are generated)
        """
        indexes = []
        for idx_file in self.idx_files:
            fp = open(idx_file, "rb")
            x = fp.read()
            fp.close()
            file_dest_seg_offsets = cPickle.loads(x)
            # see reducer1:write_idx_file()
            # seg_desc is actually like:
            # (filename, [ (0,   [ seg, seg, seg, ...]),
            #              (1,   [ seg, seg, seg, ...]),
            #                       ...
            #              (R-1, [ seg, seg, seg, ...]) ])
            # each seg is:
            #   (offset,n_chunks)
            indexes.append(file_dest_seg_offsets)
        return indexes

    def show_segments_merging_stream(self):
        Ws("show_segments_merging_stream:\n")
        Ws("  indexes:\n")
        for filename,dests_segment_offsets in self.indexes:
            Ws("    filename=%s:\n" % filename)
            for dest_idx,segment_offsets in dests_segment_offsets:
                Ws("      segment for reducer %d:\n" % dest_idx)
                for offset,n_chunks in segment_offsets:
                    Ws("        offset=%d n_chunks=%d\n" % (offset, n_chunks))
        Ws("  contents:\n")
        for key,ss in self.streams:
            ss.show_segment_stream()

    def open(self):
        """
        create as many segment_streams as the number 
        of segments described in the index.
        also open the corresponding data file to 
        get the first chunk in.
        """
        # read index file describing segments and chunks
        self.indexes = self.read_idx_files()
        for filename,dests_segment_offsets in self.indexes:
            # dests_segment_offsets = list of (dest, [ (off,sz), (off,sz) .. ])
            # create a segment reader for each segment
            # (which is a list of chunks essentially)
            for dest_idx,segment_offsets in dests_segment_offsets:
                if dest_idx != self.reducer_idx: continue
                for segment_offset,n_chunks in segment_offsets:
                    ss = segment_stream(filename, segment_offset, n_chunks)
                    if not self.fps.has_key(filename):
                        # make sure we open the underlying file
                        fp = open(filename, "rb")
                        self.fps[filename] = fp
                    # get the first element of the stream and
                    # put it in the prio queue
                    if ss.read_next_key_val(self.fps[filename]):
                        self.streams.append((ss.key, ss))
        heapq.heapify(self.streams)

    def read_next_key_val(self):
        """
        return next key value pair.
        return either 0,None,None (indicating EOF)
        or 1,key,val
        """
        if len(self.streams) == 0: return 0,None,None
        # pick the stream that has the lowest key
        key,min_ss = heapq.heappop(self.streams)
        assert (key == min_ss.key), (key, min_ss.key)
        val = min_ss.val
        # let it move forward
        if min_ss.read_next_key_val(self.fps[min_ss.filename]):
            # the next element is there, so we push it again in prio-queue
            heapq.heappush(self.streams, (min_ss.key, min_ss))
        else:
            # this stream has reached EOF, so we do not push it again
            # FIX: should close fp when all streams reading it
            # are finished (need refcount)
            pass
        return 1,key,val

    def close(self):
        for filename,fp in self.fps.items():
            fp.close()
        self.fps.clear()

# 
# 
# 

def safe_int(x):
    if x is None: return None
    try:
        return int(x)
    except ValueError:
        return None

def need_define(x, v):
    X = "MAPRED_%s" % string.upper(v)
    if x is None and os.environ.has_key(X):
        Es("environment variable %s should be int\n" % X)
        raise
        return -1
    if x is None and not os.environ.has_key(X):
        Es("define environment variable %s\n" % X)
        raise
        return -1
    else:
        return 0

#
# two main entry points (map and reduce)
#

def set_default_opts(opts):
    def split_by_colon(s):
        if s is None: return None
        return string.split(s, ":")
    def split_by_input_files_delimiter(s):
        if s is None: return None
        return string.split(s, opts["input_files_delimiter"])
    env = os.environ
    for var,default,f in [ ("n_mappers", "1", safe_int),
                           ("n_reducers", "1", safe_int),
                           ("mapper_idx", None, safe_int),
                           ("reducer_idx", None, safe_int),
                           ("map_fun", None, None),
                           ("reduce_fun", None, None),
                           ("map_begin", None, None),
                           ("reduce_begin", None, None),
                           ("map_end", None, None),
                           ("reduce_end", None, None),
                           ("unit_fun", None, None),
                           ("reduce_factory", None, None),
                           ("input_files_delimiter", ":", None),
                           ("input_files", None, split_by_input_files_delimiter),
                           ("idx_files", None, split_by_colon),
                           ("record_terminator", "\n", None),
                           ("mapper_output_template", 
                            "_mapred/_mapper_output.%d", None),
                           ("mapper_idx_template", 
                            "_mapred/_mapper_idx.%d", None),
                           ("load_balancing_scheme", "block", None),
                           ("affinity", {}, None),
                           ("map_cmd", None, None),
                           ("reduce_cmd", None, None),
                           ("cmd", None, None),
                           ]:
        if not opts.has_key(var):
            env_var = "MAPRED_%s" % string.upper(var)
            if f is None:
                opts[var] = env.get(env_var, default)
            else:
                opts[var] = f(env.get(env_var, default))
    return 0
    

def become_mapper_(mapper_idx, map_fun, input_files, n_mappers, n_reducers,
                   map_begin, map_end, unit_fun, reduce_factory,
                   load_balancing_scheme, input_files_delimiter, record_terminator, 
                   mapper_output_template, mapper_idx_template):
    """
    become mapper
    
    (mandatory. must not be None)
    mapper_idx :
    map_fun : 
    input_files :
    n_mappers :
    n_reducers :

    (optional. may be None)
    map_begin :
    map_end :
    unit_fun :
    reduce_factory :
    load_balancing_scheme :
    input_files_delimiter :
    record_terminator :
    mapper_output_template : 
    mapper_idx_template :
    """
    if need_define(mapper_idx, "mapper_idx") == -1: return -1
    if need_define(map_fun, "map_fun") == -1: return -1
    if need_define(input_files, "input_files") == -1: return -1
    if need_define(n_mappers, "n_mappers") == -1: return -1
    if need_define(n_reducers, "n_reducers") == -1: return -1
    # input_files = string.split(input_files, input_files_delimiter)
    lb = load_balancer(input_files, load_balancing_scheme, mapper_idx, n_mappers)
    output_file = mapper_output_template % mapper_idx
    idx_file = mapper_idx_template % mapper_idx
    R = reducers(mapper_idx, n_reducers, output_file, idx_file, unit_fun, reduce_factory)
    R.ensure_open()
    if map_begin: map_begin(R)
    # FIXME: more flexible load balancing
    for input_file,begin_pos,end_pos in lb.get_blocks():
        reader = record_stream(input_file, begin_pos, end_pos, record_terminator)
        reader.open()
        while 1:
            line,_,pos = reader.read_next_record()
            if line == "": break
            if 0: Ws("LINE: [%s]\n" % line)
            map_fun(line, R, input_file, pos)
        reader.close()
    if map_end: map_end(R)
    R.close()
    return 0

def become_mapper(**opts):
    set_default_opts(opts)
    return become_mapper_(opts["mapper_idx"], opts["map_fun"], opts["input_files"], 
                          opts["n_mappers"], opts["n_reducers"],
                          opts["map_begin"], opts["map_end"], 
                          opts["unit_fun"], opts["reduce_factory"],
                          opts["load_balancing_scheme"], 
                          opts["input_files_delimiter"],
                          opts["record_terminator"], 
                          opts["mapper_output_template"], 
                          opts["mapper_idx_template"])

def become_reducer_(reducer_idx, reduce_fun, idx_files,
                    reduce_begin, reduce_end, reduce_factory):
    """
    become reducer
    (mandatory. must not be None)
    reducer_idx :
    reduce_fun :
    idx_files :

    (optional. may be None)
    reduce_begin :
    reduce_end :
    reduce_factory :
    """
    if need_define(reducer_idx, "reducer_idx") == -1: return -1
    if need_define(reduce_fun, "reduce_fun") == -1: return -1
    if need_define(idx_files, "idx_files") == -1: return -1
    if reduce_begin: reduce_begin()
    sms = segments_merging_stream(idx_files, reducer_idx)
    sms.open()
    if 0: sms.show_segments_merging_stream()
    not_eof,cur_key,cur_val = sms.read_next_key_val()
    while not_eof:
        if reduce_factory is None:
            R = {}
        else:
            R = reduce_factory()
        key = cur_key
        val = cur_val
        while not_eof and key == cur_key:
            if R.has_key(key):
                R[key] += val
            else:
                R[key] = val
            not_eof,key,val = sms.read_next_key_val()
        reduce_fun(cur_key, R[cur_key])
        cur_key = key
        cur_val = val
    sms.close()
    if reduce_end: reduce_end()
    return 0

def become_reducer(**opts):
    set_default_opts(opts)
    return become_reducer_(opts["reducer_idx"], opts["reduce_fun"], 
                           opts["idx_files"], opts["reduce_begin"], opts["reduce_end"], 
                           opts["reduce_factory"])


def become_job(**opts):
    """
    become mapper or reducer, depending on which of mapper_idx or reducer_idx
    is given.
    """
    set_default_opts(opts)
    if opts["mapper_idx"] is not None:
        return become_mapper(**opts)
    elif opts["reducer_idx"] is not None:
        return become_reducer(**opts)
    else:
        assert 0

def sched_local_(map_fun, reduce_fun, input_files, n_mappers, n_reducers,
                 map_begin, map_end, reduce_begin, reduce_end,
                 unit_fun, reduce_factory,
                 load_balancing_scheme, input_files_delimiter, record_terminator, 
                 mapper_output_template, mapper_idx_template):
    """
    do everything from mappers to reducers locally in this process
    """
    if n_mappers is None: n_mappers = 1
    if n_reducers is None: n_reducers = 1
    for mapper_idx in range(n_mappers):
        become_mapper_(mapper_idx, map_fun, input_files, n_mappers, n_reducers,
                       map_begin, map_end, unit_fun, reduce_factory,
                       load_balancing_scheme, input_files_delimiter, record_terminator, 
                       mapper_output_template, mapper_idx_template)
    mapper_outputs = []
    idx_files = []
    for i in range(n_mappers):
        mapper_outputs.append(mapper_output_template % i)
        idx_files.append(mapper_idx_template % i)
    for reducer_idx in range(n_reducers):
        become_reducer_(reducer_idx, reduce_fun, idx_files, 
                        reduce_begin, reduce_end, reduce_factory)
    return 0

def sched_local(**opts):
    set_default_opts(opts)
    return sched_local_(opts["map_fun"], opts["reduce_fun"], opts["input_files"], 
                        opts["n_mappers"], opts["n_reducers"],
                        opts["map_begin"], opts["map_end"], 
                        opts["reduce_begin"], opts["reduce_end"],
                        opts["unit_fun"], opts["reduce_factory"], 
                        opts["load_balancing_scheme"], 
                        opts["input_files_delimiter"], opts["record_terminator"], 
                        opts["mapper_output_template"], opts["mapper_idx_template"])

class map_reduce_scheduler:
    def __init__(self, addr, map_cmd, reduce_cmd, cmd,
                 input_files, input_files_delimiter, n_mappers, n_reducers, 
                 load_balancing_scheme, affinity):
        self.so = None
        self.addr = addr
        self.input_files = input_files
        self.input_files_delimiter = input_files_delimiter
        self.load_balancing_scheme = load_balancing_scheme
        if map_cmd is not None:
            self.map_cmd = map_cmd
        else:
            self.map_cmd = cmd
        if reduce_cmd is not None:
            self.reduce_cmd = reduce_cmd
        else:
            self.reduce_cmd = cmd
        self.out_template = "_mapred/out.%d"
        self.idx_template = "_mapred/idx.%d"
        self.n_mappers = n_mappers
        self.n_reducers = n_reducers
        self.affinity = affinity
        self.io = { 1 : cStringIO.StringIO(), 2 : cStringIO.StringIO() }

    def safe_int(self, x):
        try:
            return int(x)
        except:
            return None

    def get_msg(self):
        so = self.so
        sz = so.recv(10)
        assert (len(sz) == 10), sz
        sz = int(sz)
        content = so.recv(sz)
        assert (len(content) == sz), x
        [ t,rest ] = content.split(None, 1)
        if t == "io:":
            x = rest.split(None, 5)
            if len(x) == 5:
                [ work_idx,man_name,fd,eof,pay_sz ] = x
                payload = ""
            else:
                [ work_idx,man_name,fd,eof,pay_sz,payload ] = x
                payload = payload[:-1]
            assert int(pay_sz) == len(payload)
            return ("io", int(work_idx), man_name, (int(fd), payload, int(eof)))
        else:
            try:
                [ work_idx,man_name,exit_status,term_sig ] = rest.split(None, 3)
            except ValueError,e:
                Es("%s\n" % content)
                raise e
            if exit_status == "-":
                exit_status = None
            else:
                exit_status = int(exit_status)
            if term_sig == "-\n":
                term_sig = None
            else:
                term_sig = int(term_sig)
            return ("status", int(work_idx), man_name, (exit_status, term_sig))

    def get_notification(self):
        while 1:
            msg_type,work_idx,man_name,data = self.get_msg()
            if msg_type == "status":
                exit_status,term_sig = data
                return work_idx,man_name,exit_status,term_sig
            elif msg_type == "io":
                fd,payload,eof = data
                self.io[fd].write(payload)
            else:
                assert 0

    def send_task(self, cmd, where):
        # print "*** %s" % cmd
        if where is None:
            msg = "cmd: %s\n" % cmd
        else:
            msg = "aff: name=%s\ncmd: %s\n" % (where, cmd)
        # print msg
        self.so.send(msg)

    def sched(self):
        where = {}
        so = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        so.connect(self.addr)
        self.so = so
        inputs = string.join(self.input_files, self.input_files_delimiter)
        tmps = string.join([ (self.out_template % i) for i in range(self.n_mappers) ], ":")
        idxs = string.join([ (self.idx_template % i) for i in range(self.n_mappers) ], ":")
        map_cmd = ("MAPRED_JOB=1 "
                   "MAPRED_INPUT_FILES=%s MAPRED_LOAD_BALANCING_SCHEME=%s "
                   "MAPRED_MAPPER_IDX=%d MAPRED_N_MAPPERS=%d "
                   "MAPRED_MAPPER_OUTPUT_TEMPLATE=%s "
                   "MAPRED_MAPPER_IDX_TEMPLATE=%s "
                   + self.map_cmd)
        red_cmd = ("MAPRED_JOB=1 "
                   "MAPRED_INPUT_FILES=%s MAPRED_IDX_FILES=%s "
                   "MAPRED_REDUCER_IDX=%d "
                   + self.reduce_cmd)
        for i in range(self.n_mappers):
            self.send_task((map_cmd % (inputs, self.load_balancing_scheme, i, 
                                       self.n_mappers, self.out_template, 
                                       self.idx_template)),
                           self.affinity.get(("map", i)))
        status = 0
        for i in range(self.n_mappers):
            work_idx,man_name,exit_status,term_sig = self.get_notification()
            where[work_idx] = (man_name, exit_status, term_sig)
            # print "work_idx %s executed by %s" % (work_idx, man_name)
            status = max(status, exit_status)
        if status != 0:
            return status,self.io[1].getvalue(),self.io[2].getvalue()
        for i in range(self.n_reducers):
            self.send_task((red_cmd % (tmps, idxs, i)), self.affinity.get(("red", i)))
        for i in range(self.n_reducers):
            work_idx,man,exit_status,term_sig = self.get_notification()
            where[work_idx] = (man_name, exit_status, term_sig)
            status = max(status, exit_status)
        return status,self.io[1].getvalue(),self.io[2].getvalue(),where

def sched_gxp_(addr, map_cmd, reduce_cmd, cmd, 
               input_files, input_files_delimiter, n_mappers, n_reducers, 
               load_balancing_scheme, affinity):
    if cmd is None:
        if need_define(map_cmd, "map_cmd") == -1: return -1
        if need_define(reduce_cmd, "reduce_cmd") == -1: return -1
    if need_define(input_files, "input_files") == -1: return -1
    mrs = map_reduce_scheduler(addr, map_cmd, reduce_cmd, cmd, 
                               input_files, input_files_delimiter, n_mappers, n_reducers, 
                               load_balancing_scheme, affinity)
    return mrs.sched()
    
def sched_gxp(addr, **opts):
    set_default_opts(opts)
    return sched_gxp_(addr, 
                      opts["map_cmd"], opts["reduce_cmd"], opts["cmd"], 
                      opts["input_files"], opts["input_files_delimiter"], 
                      opts["n_mappers"], opts["n_reducers"], 
                      opts["load_balancing_scheme"], opts["affinity"])

#
# true entry point for running a map reduce job
#

def sched(**opts):
    addr = os.environ.get("GXP_JOBSCHED_WORK_SERVER_SOCK")
    if addr:
        return sched_gxp(addr, **opts)
    else:
        return sched_local(**opts)

