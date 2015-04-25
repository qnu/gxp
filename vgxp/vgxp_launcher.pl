#!/usr/bin/env perl

my $script_dir;
BEGIN {
  my $rel_script_dir = `dirname $0`;
  chomp($rel_script_dir);
  chomp($script_dir = `(cd $rel_script_dir; pwd)`);
  unshift(@INC, $script_dir);
}

use strict;
use VGXP;
use POSIX ":sys_wait_h";
use Digest::MD5;
use warnings;
use IPC::Open3;
use Symbol;
use Getopt::Long;
use File::Basename;

my $pmaster_pid;
sub handle_sigint{
  kill 9, $pmaster_pid if $pmaster_pid;
}
$SIG{INT} = &handle_sigint;

#my $script_dir = File::Basename::dirname $0;
#die "Can't cd to $script_dir: $!\n" unless chdir $script_dir;

my $verbosity = 0;
sub dp($@){
  my $dlevel = shift;
  print STDERR @_ if($dlevel <= $verbosity);
}

sub subst_variable($$){
  my ($vars, $varname) = @_;
  if(defined $ENV{$varname}){
    sv($vars, $ENV{$varname});
  } elsif(defined $vars->{$varname}){
    sv($vars, $vars->{$varname});
  } else {
    dp(0, "Definition of $varname is not found!\n");
    "";
  }
}
sub sv($$){
  my ($vars, $str) = @_;
  $str =~ s/%([\w\d_]+)%/subst_variable($vars, $1)/eg;
  $str;
}

sub copy_or_apply_template($@){
  my ($vars, $apply_template, $tmpl, $target) = @_;
  open(my $rfh, $tmpl) || die("Cannot open $tmpl for read");
  local $/;
  my $content = <$rfh>;
  $content = sv($vars, $content) if $apply_template;
  close $rfh;
  my $content2 = "";
  open($rfh, $target) && do {
    $content2 = <$rfh>;
    close $rfh;
  };
  if($content eq $content2){
    dp(3, "IGNORE $target will not be modified.\n");
  } else {
    dp(3, sprintf("%s %s %s\n", ($apply_template ? "APPLY_TEMPLATE" : "COPY"), $tmpl, $target));
    my $tmpfile = "$target.$$";
    open(my $wfh, ">", $tmpfile) || die("Cannot open $tmpfile for write");
    print $wfh $content;
    close $wfh;
    rename $tmpfile, $target;
  }
}

sub read_vars($$){
  my ($vars, $file) = @_;
  my $rfh;
  open $rfh, $file;
  while(<$rfh>){
    chomp;
    if(/^\s*([\w\d_]+)\s*=\s*(\S.*)/){
      $vars->{$1} = sv($vars, $2);
    }
  }
  close $rfh;
}

my $vars = {};
my $hostname = `hostname -f`;
chomp $hostname;
$vars->{HOSTNAME} = $hostname;
$vars->{SCRIPT_DIR} = $script_dir;
read_vars($vars, "$script_dir/vgxpvars.txt");

## Parse args
my %opts = ();
Getopt::Long::Configure "gnu_getopt";
GetOptions(\%opts,
#           "d:w:m:i:h",
           "d|dir=s",
           "w|web=s",
           "m|master=s",
           "i|interval=i",
           "p|serverport=i",
           "h|help",
           "v|verbosity=i",
           "start",
           "stop",
    );
if($opts{h}){
  print STDERR << "EEE";
$0 [options]
  -d(--dir):       install dir
  -h(--help):      show help
  -i(--interval):  check interval (in seconds)
  -m(--master):    filename of master program
  -p(--serverport):port number of server
  --start:         start VGXP in background
  --stop:          stop VGXP
  -v(--verbosity): set verbosity(0..3)
  -w(--web):       base URL
EEE
  exit 1;
}
$vars->{INSTALL_DIR} = $opts{d} if $opts{d};
$vars->{CODEBASE} = $opts{w} if $opts{w};
$vars->{MASTER_PATH} = $opts{m} if $opts{m};
$vars->{PMASTER_CHECK_INTERVAL} = $opts{i} if $opts{i};
$vars->{VERBOSITY} = $opts{v} if $opts{v};
$vars->{SERVER_PORT} = $opts{p} if $opts{p};

##

$verbosity = sv($vars, "%VERBOSITY%");

my @remote_files = qw(CM.pm SSHSocket.pm agent.pl);
my @remote_files2 = qw(python pinfo.py);
my $portconf = sprintf "%s/%s", sv($vars, '%INSTALL_DIR%'), sv($vars, '%PORTCONF_FILENAME%');
my $pmaster = sv($vars, '%MASTER_PATH%');
my $check_interval = sv($vars, '%PMASTER_CHECK_INTERVAL%');
my $fname_notify = sv($vars, '%NOTIFY_FILENAME%');
my $install_dir = sv($vars, '%INSTALL_DIR%');
my $server_port = sv($vars, '%SERVER_PORT%');

$ENV{'SERVER_PORT'} = $server_port;

##

sub terminate_pmaster(){
  dp(0, "Stopping VGXP.\n");
  system("pkill -f $pmaster");
}

if($opts{stop}){
  terminate_pmaster;
  exit 0;
}

##

# Copy files

sub copy_files($){
  my ($vars) = @_;
  open(my $fh, "$script_dir/copyfiles.txt");
  while(<$fh>){
    next if /^\s*\#/;
    chomp;
    my @a = split;
    $a[1] = sv($vars, $a[1]);
    $a[2] = sv($vars, $a[2]);
    if(-d $a[2]){
      my $basename = File::Basename::basename($a[1]);
      $a[2] .= "/$basename";
    }
    copy_or_apply_template($vars, @a);
  }
}

sub my_mkdir($){
  my ($dir) = @_;
  my $parent = File::Basename::dirname $dir;
  die "$parent not found" if ! -d $parent;
  if(! -d $dir){
    dp(3, "MKDIR Creating directory $dir\n");
    system("mkdir $dir");
    die("Cannot make directory") if ! -d $dir;
  }
}

my_mkdir $install_dir;
copy_files($vars);
system(sv($vars, "chmod +x $install_dir/%PROXYCGI_FILENAME%"));

dp(0, "Files are installed in $install_dir\n");
dp(0, sprintf("URL to access VGXP is %s\n", sv($vars, '%CODEBASE%')));

# Launch VGXP

$ENV{PERL5LIB} = $script_dir;
my $log_dir = sv($vars, '%LOG_DIR%');
my_mkdir $log_dir;

while(1){
  &VGXP::gxp_check();
  #system("gxpc quit --session_only");
  my @cmd = ('gxpc', 'mw', #'-h', 'hongo-', #'(hongo-|tohoku|hiro-|kototoi-|kyoto-)',
             '--master', "$pmaster $portconf $log_dir", 'perl');#"$agent");
  my ($cmd_in, $cmd_out, $cmd_err) = (gensym, gensym, gensym);
  my $open3pid = open3($cmd_in, $cmd_out, $cmd_err, @cmd);
  #print STDERR "Open3: pid = $open3pid\n";
  for my $fname (@remote_files){
    open my $fh, "<", "$script_dir/$fname";
    while(<$fh>){
      print $cmd_in $_;
    }
    close $fh;
  }
  print $cmd_in "__DATA__\n", $remote_files2[0], $/;
  open my $fh, "<", sprintf("%s/%s", $script_dir, $remote_files2[1]);
  while(<$fh>){
    print $cmd_in $_;
  }
  close $fh;
  close $cmd_in;
  my $mw_err = <$cmd_err>;
  close $cmd_out;
  close $cmd_err;
  $mw_err =~ s/^(\d+)$/$pmaster_pid = $1;""/egm;
  $mw_err =~ s/^(.*)$/dp(1, "ERR: $1\n") if length($1)/egm;
  dp(1, "PMASTER_PID is $pmaster_pid\n");
  wait;
  if($opts{start}){
    dp(0, "VGXP is started in background.\n");
    exit 0;
  }
  while(1){
    sleep($check_interval);
    if(-f $fname_notify){
      unlink $fname_notify;
      dp(1, scalar(localtime), " Notify\n");
      kill "INT", $pmaster_pid;
      #system("pkill -f $pmaster");
    }elsif(kill(0,$pmaster_pid)){
      next;
    }else{
      dp(1, "Dead child ", scalar(localtime), $/);
    }
    $pmaster_pid = 0;
    sleep(3);
    last;
  }
}
