#!/usr/bin/perl -w

use strict;
use CM;
use IO::Socket;
use VGXP;
use Time::HiRes qw(gettimeofday tv_interval);

my $protocol_version = 1.001;

my %hosts_available = ();

close STDOUT;

sub connect_agents($) {
  my ($log_dir) = @_;
  my $data = "";
  my %hosts = ();
  &VGXP::gxp_check();
  open(CMD,"gxpc stat|");
  $_ = <CMD>;
  while(<CMD>){
    # matches to " GUPID[host-user-date-pid] (= HOST TARGET)"
    my ($s,$d,$h,$t) = /^(\s*)(\S+)\s+\(=\s+(\S+)\s+(\S+)\)/;
    next if $d =~ /^None/;
    $s = length $s;
    $data .= "T $s $h $d\n";
    $hosts{$d} = $h;
    $hosts_available{$h} = 1;
  }
  close CMD;
  $data .= "TT\n";
  my $num_execs = -1;
  my ($i, $root_host, $root_port);
  while(<STDIN>){
    # print STDERR "QQ $_";
    my ($gupid, $user, $port, $idx, $n) = split(/\s+/,$_);
    if ( $num_execs == -1 ) {
      $num_execs = $n;
      $i = $n;
    }
    die "different NUM_EXECS ($n at $gupid) was found!" if $num_execs != $n;
    $data .= "P $gupid $user $port\n";
    if ( $idx == 0 ) {
      $root_host = $hosts{$gupid};
      $root_port = $port;
    }
    last if -- $i == 0;
  }
  my $pid = fork;
  if($pid){
    print STDERR "$pid\n";
    exit;
  }
  if(! -d $log_dir){
    print STDERR "Log directory ($log_dir) is not found!\n";
    exit 1;
  }
  my $log_filename = "$log_dir/pmaster.log";
  open(STDERR, ">>$log_filename") || die "Cannot open logfile($log_filename)";
  $data .= "PP\n";

  my $root = IO::Socket::INET->new(PeerHost => $root_host, PeerPort => $root_port, Proto => "tcp");

  print STDERR scalar(localtime), ": Connected to $root_host:$root_port\n";
  print STDERR scalar(localtime), " NUM_EXECS = $num_execs\n";

  print $root $data;
  $root->flush;
  #my %h = ();
  #my %h2 = ();
  #while(<$root>){
  #  my @line = split;
  #  $h{$line[0]} = 1;
  #  %h2 = () if $line[0] eq "shepherd";
  #  $h2{$line[0]} = 1;
  #  #printf STDERR "%s / %s\n", scalar(keys %h2), scalar(keys %h);
  #  if(scalar(keys %h2) == scalar(keys %h)){
  #    printf STDERR "%s: %d\n", scalar(localtime), scalar(keys %h);
  #  }
  #}
  $root;
}

my $fname_notify = "notify_pmaster";

my $port = $ENV{'SERVER_PORT'};
if ( !defined $port ) {
  #die ( "you must specify the port to be listened" );
  $port = 0;
}

my $server = IO::Socket::INET->new(LocalPort => $port, Listen => 10, ReuseAddr => 1)
    or do {
      system("touch $fname_notify");
      die "Can't make server socket: $@\n";
    };

#use Socket;
my $mysockaddr = getsockname($server);
my $myaddr;
($port, $myaddr) = sockaddr_in($mysockaddr);
#print STDERR "$port $myaddr\n";
my $portconf = "port.conf";
$portconf = $ARGV[0] if $ARGV[0] ne "";
open(F, ">$portconf");
print F "$port\n";
close F;

my $log_dir = "logs";
$log_dir = $ARGV[1] if $ARGV[1] ne "";

my %proc_table = ();
my %clients = ();
my %pending_clients = ();
my $hosts_string = "";

sub update_hosts_string () {
  my @h = ();
  for my $host (keys %hosts_available) {
    push(@h, $host) if ! exists $proc_table{$host};
  }
  my @s = ();
  for my $host (sort @h) {
    if ( $host =~ /^(\D*)(\d+)(\D*)$/ ) {
      my ($prefix, $num, $suffix) = ($1, $2, $3);
      if ( ( scalar(@s) > 0 ) &&
           ( $s[0]->[0] eq $prefix ) &&
           ( $s[0]->[1] eq $suffix ) &&
           ( $s[0]->[3] == $num - 1) ) {
        $s[0]->[3] = $num;
      }else{
        unshift(@s,[$prefix,$suffix,$num,$num]);
      }
    } else {
      unshift(@s,[$host]);
    }
  }
  $hosts_string = sprintf "(%d)", scalar keys %proc_table;
  $hosts_string .= join(",",map {if(scalar @$_ == 1){$_->[0]}elsif($_->[2] == $_->[3]){$_->[0] . $_->[2] . $_->[1]}else{sprintf "%s[%d-%d]%s",$_->[0],$_->[2],$_->[3],$_->[1]}} @s);
}

sub accept_handler (\%$) {
  my ($m,$fileno) = @_;
  my $name = $m->socket_name($fileno);
  #printf STDERR "ACCEPT $fileno $name\n";
  my $buf = "$protocol_version\n";
  $m->send_data($fileno,$buf);
  $pending_clients{$fileno} = 1;
}

my $last_time = -1;
sub request_handler (\%\@) {
  my ($sec, $usec) = gettimeofday;
  my $t = int(($sec % 100) * 10 + $usec * 10 / 1000000);
  return if $last_time == $t;
  $last_time = $t;
  my ($m,$q) = @_;
  my $obuf = "";
  my $update_hosts_flag = 0;
  while(scalar @{$q}){
    my ($fileno, $data) = @{shift @{$q}};
    my $name = $m->socket_name($fileno);
    if ( $name eq "ROOT" ) {
      if ( $data =~ /^ERR (.*)$/ ) {
        print STDERR "$1\n";
        next;
      }
      # broadcast to all clients
      $data =~ s/#[^#]*$/\n/;
      $obuf .= $data;
      # update process info
# chiba157 201 C 0.01 0.00 0.02 0.00 0 0 0 0 0 0 M 0.33 0.12 0.19 S state1 %0A N 0 0 D 0 0 0 0 P P 24415 python kamo 1 P 7624 python kamo2 1 P 16861 perl kamo 1
      my @d = split(/\s+/,$data);
      my $host = shift @d;
      my $tbl = $proc_table{$host};
      if(!defined $tbl){
        $update_hosts_flag = 1;
        $proc_table{$host} = {};
        $tbl = $proc_table{$host};
      }
      print STDERR "XXX $data\n" if !defined $host;
      my $interval = shift @d;
      while(@d){
        my $type = shift @d;
        last if $type eq 'P';
        if($type eq 'S'){
          my $state = shift @d;
          my $pgidinfo = shift @d;
          $tbl->{state} = "$state $pgidinfo";
        } elsif($type eq 'SS'){
          my $stateconfig = shift @d;
          $tbl->{stateconfig} = "$stateconfig";
        }
      }
      my @procs = @d;
      while(scalar @procs){
        my $p = shift @procs;
        if($p eq "P"){
          my $pid = shift @procs;
          my $cmd = shift @procs;
          my $uid = shift @procs;
          shift @procs; # CPU
          $tbl->{$pid} = "$cmd $uid";
        }elsif($p eq "C"){
          shift @procs; # pid
          shift @procs; # CPU
        }elsif($p eq "T"){
          my $pid = shift @procs;
          delete $tbl->{$pid};
        }else{
          die("Broken data: $data\n");
        }
      }
    } elsif ( defined $pending_clients{$fileno} ) {
      chomp($data);
      delete $pending_clients{$fileno};
      if($data =~ /V g3_20080304_0/){
        $clients{$fileno} = 1;
        my $buf;
        if( -f "actives" ){
          open(FILE,"actives");
          my $actives = join("",<FILE>);
          $buf = $actives . "\n";
        } else {
          $buf = join("", map {sprintf("%s %d\n", $_, (defined $proc_table{$_})?1:0)} (sort keys %hosts_available)) . "\n";
        }
        for my $host (keys %proc_table){
          for my $pid (keys %{$proc_table{$host}}){
            $buf .= "$host $pid " . $proc_table{$host}->{$pid} . "\n";
          }
        }
        $buf .= "\n";
        $m->send_data($fileno,$buf);
      }elsif($data =~ /^QUIT_PMASTER/){
        exit 0;
      } else {
        print STDERR "$name: incompatible client version=$data\n";
        $m->close_client($fileno, "V");
      }
    } else {
      # request from clients
      die("Not implemented yet.");
    }
  }
  if ( $update_hosts_flag ) {
    update_hosts_string();
    #printf STDERR "nodes: $hosts_string\n";
  }
  if(length $obuf){
    for my $client (keys %clients) {
      #printf STDERR "send_data(%d(%s),len=%d)\n", $client, $m->socket_name($client), length $data;
      $m->send_data($client,$obuf);
    }
  }
}

sub close_handler (\%$) {
  my ($m,$fileno) = @_;
  delete $clients{$fileno} if exists $clients{$fileno};
  delete $pending_clients{$fileno} if exists $pending_clients{$fileno};
}

my $cm = CM->new();
$cm->add_server($server);
my $root = connect_agents($log_dir);
$cm->add_client($root,"ROOT");
$cm->register_accept_handler(\&accept_handler);
$cm->register_request_handler(\&request_handler);
$cm->register_close_handler(\&close_handler);

$cm->main_loop();
