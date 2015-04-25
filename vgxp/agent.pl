#!/usr/bin/env perl

package main;

my $timeout = 60;

use strict;
use warnings;
use POSIX;
use IO::Socket;
use Fcntl;
use FileHandle;
use Time::HiRes qw(gettimeofday tv_interval);
use warnings;
use IPC::Open3;
use Symbol;

require CM unless scalar keys %CM::;
require SSHSocket unless scalar keys %SSHSocket::;

open(STDOUT, ">/dev/null"); # preserve fd=1
open(FDUP, ">>&=", 3);
FDUP->autoflush(1);

exit if fork;

my $exec_idx = $ENV{GXP_EXEC_IDX};
$exec_idx = "none" if ! exists $ENV{GXP_EXEC_IDX};
my $err_fname = "agent_${CM::hostname}_${exec_idx}.err";
my $log_local = 0;

sub log_err($){
  if($log_local){
    open(my $e, ">>$err_fname");
    print $e $_[0];
    close $e;
  }
}

my ($errfh, $olderr);
pipe($errfh, $olderr);    # this assigns !2 to $errfh
open(STDERR, ">&", $olderr); # this assigns 2 to STDERR
close($olderr);
STDERR->autoflush(1);

my $start_time = time;

my ($cmd, $cmd_data);
{$cmd = <DATA>; local $/; $cmd_data = <DATA>;}
unless($cmd){
  my $pinfo_py = "pinfo.py";
  #$cmd = "python $script_dir/$pinfo_py";
  $cmd = "python $pinfo_py";
}

my %hosts = ();
my $root = {};
$root->{_parent} = $root;
# parent state
#  0: just started
#  1: parse tree finished
#  2: parse ports finished
#  3: normal
my $parent_state = {
  state => 0,
  depth => 0,
  node => $root,
  data => "",
};

my %children = ();
# child state
#  0: before establish connection
#  1: connection established
#  2: normal

my $cm = CM->new();

sub err_func($){
  $cm->send_err($_[0]);
}

sub direct_connectable($$) {
  my ($a, $b) = @_;
  return 0;
  # return substr($a,0,5) eq substr($b,0,5);
}

sub parse_input($) {
  my $obuf = $_[0];
  $parent_state->{data} .= $obuf;
  for (split(/\n/,$obuf)){
    my ($op, $r) = split(/\s+/,$_,2);
    $cm->send_err("Invalid line: $_") if ! defined $op;
    if ( $op eq "T" ) {
      my ($level, $host, $gupid) = split(/\s+/,$r);
      ++ $parent_state->{depth};
      while ($parent_state->{depth} > $level) {
        -- $parent_state->{depth};
        $parent_state->{node} = $parent_state->{node}->{_parent};
      }
      $hosts{$gupid}->{_host} = $host;
      $hosts{$gupid}->{_gupid} = $gupid;
      $hosts{$gupid}->{_parent} = $parent_state->{node};
      $parent_state->{node}->{$gupid} = $hosts{$gupid};
      $parent_state->{node} = $hosts{$gupid};
    } elsif ( $op eq "P" ) {
      my ($gupid, $user, $port) = split(/\s+/,$r);
      $hosts{$gupid}->{_user} = $user;
      $hosts{$gupid}->{_port} = $port;
    } elsif ( $op eq "TT" ) {
      $parent_state->{state} = 1;
    } elsif ( $op eq "PP" ) {
      $parent_state->{state} = 2;
    } else {
      log_err "Invalid operator: $op\n";
      die "Invalid operator: $op\n";
    }
  }
}

sub connect_children ($) {
  my $cm = $_[0];
  my $gupid = $ENV{GXP_GUPID};
  my $node = $hosts{$gupid};
  for (keys %{$node}){
    next if /^_/;
    my $flag = 0;
    if ( defined $node->{$_}->{_port} ) {
      my $child = $node->{$_};
      my $cgupid = $child->{_gupid};
      #$cm->send_err(sprintf("%s => %s\@%s:%d(%s)", $gupid, $child->{_user}, $child->{_host}, $child->{_port}, $cgupid));
      my $sock;
      if ( direct_connectable($ENV{GXP_HOSTNAME}, $child->{_host}) ) {
        #$cm->send_err("Direct Connection: $ENV{GXP_HOSTNAME} => $child->{_host}");
        log_err("Direct Connection: $ENV{GXP_HOSTNAME} => $child->{_host}\n");
        $sock = IO::Socket::INET->new(PeerAddr => $child->{_host}, PeerPort => $child->{_port});
      } else {
        log_err("SSH Connection: $ENV{GXP_HOSTNAME} => $child->{_host}\n");
        $sock = SSHSocket->new(PeerAddr => $child->{_host}, PeerPort => $child->{_port}, User => $child->{_user}, ErrFunc => \&err_func);
      }
      if($sock){
        $children{$sock->fileno} = $child;
        $cm->add_client($sock, $cgupid);
        $child->{_sock} = $sock;
        #Time::HiRes::usleep(100000);
        $cm->send_data($sock->fileno,$parent_state->{data});
      }
    }
  }
}

sub exec_cmd() {
  my ($cmd_in, $cmd_out, $cmd_err) = (gensym, gensym, gensym);
  my $open3pid = open3($cmd_in, $cmd_out, $cmd_err, $cmd);
  if($cmd_data){
    print $cmd_in $cmd_data;
  }
  close $cmd_in;
  $cm->add_client($cmd_out,"_CMD");
  $cm->add_client($cmd_err,"_CMDERR");
}

# listen
my $server = IO::Socket::INET->new(LocalPort => 0, Listen => 10, ReuseAddr => 1)
    or do {
      print STDERR "EE: $!\n";
      log_err "Can't make server socket: $@\n";
      die "Can't make server socket: $@\n";
    };
$cm->add_server($server, $timeout);

$cm->register_accept_handler(\&accept_handler);
$cm->register_request_handler(\&request_handler);
$cm->register_close_handler(\&close_handler);
$cm->register_timeout_handler(\&timeout_handler);

printf FDUP "%s %s %d %d %d\n", $ENV{GXP_GUPID}, $ENV{USER}, $server->sockport(), $exec_idx, $ENV{GXP_NUM_EXECS};
close FDUP;

$cm->add_client($errfh, "_ERR");

exec_cmd();

# my @cnt = split(//,"0000000000");
# my $last_cnt = 0;

my %h = ();
my $pending_data = "";

my $last_time = -1;

if($log_local){
  $cm->send_err("Logfile will be written to $err_fname");
}

$cm->main_loop();

sub accept_handler {
  my ($m,$fileno) = @_;
  #my $name = $m->socket_name($fileno);
  $m->set_parent($fileno);
  #$m->send_err("ACCEPT $fileno");
  $m->remove_timeout($server->fileno);
}

sub request_handler {
  my ($sec, $usec) = gettimeofday;
  my $t = int(($sec % 100) * 10 + $usec * 10 / 1000000);
  return if $last_time == $t;
  $last_time = $t;
  my ($m,$q) = @_;
  my $obuf = "";
  my $obuf2 = "";
  my $obuf3 = "";
  while(scalar @{$q}){
    my ($fileno, $data) = @{shift @{$q}};
    my $name = $m->socket_name($fileno);
    #$m->send_err("DEQ: $fileno($name), $data");
    if ( defined $CM::parent && $fileno == $CM::parent ) {
      $obuf .= $data;
    } elsif ( exists $children{$fileno} ) {
      $obuf2 .= $data;
    } elsif ( $name eq "_CMD" ) {
      $obuf2 .= $data;
    } elsif ( $name eq "_ERR" ) {
      $obuf3 .= $data;
    } elsif ( $name eq "_CMDERR" ) {
      $obuf3 .= $data;
    } else {
      $m->send_err(":Invalid data from $fileno($name) is ignored.\nDATA=$data");
    }
  }
  if(length $obuf){
    if ( $parent_state->{state} < 2 ) { # parsing
      parse_input($obuf);
      if ( $parent_state->{state} == 2 ) {
        # TODO 非同期に接続するようにしたい
        connect_children($cm);
      }
    } else {
      for my $child (keys %children) {
        #$m->send_err(sprintf("send_data(%d(%s),len=%d)", $client, $m->socket_name($client), length $data));
        $m->send_data($child,$obuf);
      }
    }
  }
  if(length $obuf2){
    if(defined $CM::parent){
      if(defined $pending_data && length $pending_data){
        $m->send_data($CM::parent,$pending_data);
        $pending_data = undef;
      }
      $m->send_data($CM::parent,$obuf2);
    } else {
      $pending_data .= $obuf2;
      #my $l = length $obuf2;
      #$obuf2 =~ s/^(\S+)/$h{$1}=1/egm;
      #my $t = time % 10;
      #if($last_cnt != $t){
      #  if($last_cnt == 9){
      #    my $sum = 0;
      #    for (@cnt){ $sum += $_; };
      #    $m->send_err(sprintf("%s %d [%d] %.2f kbps ela=%d", $ENV{GXP_HOSTNAME}, $exec_idx, scalar keys %h, $sum / 10 * 8 / 1024, time - $start_time));
      #  }
      #  $cnt[$t] = 0;
      #  $last_cnt = $t;
      #}
      #$cnt[$t] += $l;
    }
  }
  if(length $obuf3){
    for (split(/\n/,$obuf3)) {
      $m->send_err($_);
    }
    if ( ! defined $CM::parent ) {
      log_err $obuf3;
    }
  }
}

sub close_handler {
  my ($m,$fileno) = @_;
  my $name = $m->socket_name($fileno);
  if ( defined $CM::parent && $fileno == $CM::parent ) {
    $m->set_parent(undef);
    log_err "Parent disconnected\n";
    exit;
    # if ( $exec_idx == 0 ) {
    #   exit;
    # }
  } elsif ( $name eq "_CMD" ) {
    sleep 1;
    $m->send_err("Restarting CMD");
    exec_cmd();
  } elsif ( exists $children{$fileno} ) {
    delete $children{$fileno}->{_sock};
    delete $children{$fileno};
  }
}

sub timeout_handler {
  my ($m,$fileno) = @_;
  $m->send_err("ACCEPT TIMEOUT $fileno");
  log_err "Connection from the parent is not established for $timeout seconds.\n";
  exit 1;
}

