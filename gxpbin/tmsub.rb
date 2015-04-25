#!/usr/bin/env ruby

require "dl/import"
require "dl/struct"

module TMLIB
  extend DL::Importable

  TM_NULL_EVENT  =  0
  TM_ERROR_EVENT = -1
  
  ERR_MSGS = { 
    17000 => "TM_ESYSTEM",
    17001 => "TM_ENOEVENT",
    17002 => "TM_ENOTCONNECTED",
    17003 => "TM_EUNKNOWNCMD",
    17004 => "TM_ENOTIMPLEMENTED",
    17005 => "TM_EBADENVIRONMENT",
    17006 => "TM_ENOTFOUND",
    17007 => "TM_BADINIT"
  }

  def die(msg)
    abort("tmsub.rb: error: #{msg}")
  end

  def check_err(name, err)
    if (err != 0) 
      msg = ERR_MSGS.fetch(err, err)
      if msg == "TM_EBADENVIRONMENT"
        msg = "#{msg} (perhaps you are not in torque job)"
      end
      die("#{name} failed with #{msg}")
    end
  end

  def loadlib(libname)
    begin
      TMLIB.dlload libname
    rescue RuntimeError
      die("could not load #{libname}, (try 'gxpc explore -a lib=/full/path/to/libtorque.so' or gxpc export LD_LIBRARY_PATH=/where/libtorque/is/found")
    end
    TMLIB.typealias('tm_task_id', 'unsigned int')
    TMLIB.typealias('tm_node_id', 'int')
    TMLIB.typealias('tm_event_t', 'int')
    @tm_roots = TMLIB.struct ["tm_task_id tm_me",
                              "tm_task_id tm_parent",
                              "int tm_nnodes",
                              "int tm_ntasks",
                              "int tm_taskpoolid",
                              "tm_task_id * tm_tasklist"]
    TMLIB.extern "int tm_init(void*, tm_roots*)"
    TMLIB.extern "int tm_spawn(int, char**, char**, tm_node_id, tm_task_id*, tm_event_t*)"
    TMLIB.extern "int tm_poll(tm_event_t, tm_event_t*, int, int*)"

  end

  def mkenvp(env)
    # environ hash to C-style array
    envp = []
    env.each_pair { | var,val | envp.push("#{var}=#{val}") }
    envp.push(DL::PtrData.new(0))
    return envp
  end

  def init()
    # wrap tm_init
    roots = @tm_roots.malloc
    check_err("tm_init", TMLIB.tm_init(DL::PtrData.new(0), roots))
    return roots
  end

  def spawn(argv, env, where)
    # wrap tm_spawn
    amp_tid = DL.malloc(DL.sizeof("i"))
    amp_event = DL.malloc(DL.sizeof("i"))
    check_err("tm_spawn",
              TMLIB.tm_spawn(argv.size, argv, mkenvp(env), 
                             where, amp_tid, amp_event))
    return amp_event.to_a("I")[0],amp_tid.to_a("I")[0]
  end

  def poll()
    # wrap tm_poll
    _amp_event = DL.malloc(DL.sizeof("i"))
    _amp_err = DL.malloc(DL.sizeof("i"))
    check_err("tm_poll", 
              TMLIB.tm_poll(TM_NULL_EVENT, _amp_event, 1, _amp_err))
    _event = _amp_event.to_a("I")[0]
    _err = _amp_err.to_a("I")[0]
    check_err("tm_poll", _err)
    return _event,_err
  end

  def parse_opt(argv)
    # currently use primitive way of parsing args
    # --node x [ --lib file ] cmd

    # check if --node x is given (it must be)
    if argv.size < 3 or argv[0] != "--node"
      abort("usage:
  tmsub.rb --node NODE cmd arg ...
  NODE is a sequence of alphabets or underscores followed by a positive number (e.g., a23)")
    end
    # match to strings like "abc123", "xyz0", etc.
    m = /[A-za-z_]+0*([1-9]?[0-9]*)/.match(argv[1])
    if m.nil?
      die("arg to --node (#{argv[1]}) must be a sequence of alphabets or underscores followed by a positive number (e.g., a12, xyz123, a_b_1)")
    end
    if m[1] == "" 
      where = 0
    else
      where = Integer(m[1])
    end
    argv = argv[2..argv.size]

    # check if --lib file is given
    libname = "libtorque.so"
    if argv.size > 0 and argv[0] == "--lib"
      if argv.size > 1
        libname = argv[1]
      else
        die("arg to --lib must be given")
      end
      argv = argv[2..argv.size]
    end

    return libname,where,argv
  end

  def main(argv, env)
    # main driver
    # argv = ARGV, env = ENV
    # the first arg (arv[0]) must specify node id (0,1,...)
    libname,where,args = parse_opt(argv)

    loadlib(libname)
    roots = init()
    if where >= roots.tm_nnodes 
      die("request node id (#{where}) must be < number of nodes (#{roots.tm_nnodes})")
    end
    event,tid = spawn(args, env, where)
    while true
      _event,_err = poll()
      if (event == _event) then break end
    end
    puts "tmsub: child task #{tid} generated"
    return 0
  end
end

class TM_LIB; include TMLIB; end

def main()
  tmlib = TM_LIB.new
  tmlib.main(ARGV, ENV)
end

exit(main())

