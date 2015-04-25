# -*- Perl -*-
package SSHSocket;

use strict;
use Socket;
use IO::Socket;
use IO::Select;
use IO::Handle;
use POSIX;
use IPC::Open2;
use Carp;
use POSIX ":sys_wait_h";

our @ISA = qw( IO::Socket::INET );
our $VERSION = '0.001';
our $DEBUG = 0;
our @login_cmd = ("ssh", "-o", "PasswordAuthentication no", "-o", "StrictHostKeyChecking no", "-A", "-l", "%User%", "-L", "%gwport%:localhost:%PeerPort%", "-T", "%PeerAddr%", "echo dummy; head -n 1");

our %pids = ();

sub die2 ($) {
  ErrFunc($_[0]);
  die $_[0];
}

sub nonblock ($) {
  my $socket = shift;
  my $flags;
  $flags = fcntl($socket, F_GETFL, 0)
      or die2 "Can't get flags for socket: $!\n";
  fcntl($socket, F_SETFL, $flags | O_NONBLOCK)
      or die2 "Can't make socket nonblocking: $!\n";
}

sub get_vacant_ports($;$){
  my ($self,$nports) = @_;
  $nports = 1 if ! defined $nports;
  my @socks = ();
  my @ports = ();
  while($nports){
    my $dummy = IO::Socket::INET->new(LocalPort => $self->{GatewayPort},
                                      Listen => 5)
        or do {
          croak "Can't make server socket: $@\n";
        };
    my $gwport = $dummy->sockport ();
    push(@socks, $dummy);
    push(@ports, $gwport);
    -- $nports;
  }
  for(@socks){
    $_->close();
  }
  if($DEBUG){
    ErrFunc(sprintf("Gateway ports: %s", join(",", @ports)));
  }
  return \@ports;
}

sub spawn_process {
  my (@l) = @_;
  my ($parent, $child);
  socketpair($child, $parent, AF_UNIX, SOCK_STREAM, PF_UNSPEC)
      or  die2 "socketpair: $!";
  $child->autoflush(1);
  $parent->autoflush(1);

  my $ssh_pid = fork();
  if ($ssh_pid){
    $parent->close();
    waitpid($ssh_pid,0);
    chomp($ssh_pid = <$child>);
  } else {
    die2("Cannot fork") if !defined $ssh_pid;
    $child->close();
    open(STDIN,  "<&", $parent)   || die2 "can't dup client to stdin";
    open(STDOUT, ">&", $parent)   || die2 "can't dup client to stdout";
    ## open(STDERR, ">&STDOUT") || die2 "can't dup stdout to stderr";
    $parent->close();
    my $ssh_pid2 = fork();
    exit if $ssh_pid2;
    die2("Cannot fork") if !defined $ssh_pid2;
    print "$$\n";
    exec(@l) or die2("Cannot exec");
  }
  [$child, $ssh_pid];
}

sub new {
  my $class = shift;
  my $self = {
    GatewayPort => 0,
    Concurrency => 2
  };
  my @pwent = getpwuid(getuid);
  $self->{User} = $pwent[0];
  while(scalar @_){
    my $key = shift;
    my $val = shift;
    $self->{$key} = $val;
  }
  my @args = ();
  while(my ($k, $v) = each %{$self}){
    next if $k eq "PeerAddr";
    next if $k eq "PeerPort";
    push(@args, $k, $v);
  }

  {
    no strict 'refs';
    *{"${class}::ErrFunc"} = $self->{ErrFunc};
  }

  # connect
  croak("PeerAddr is not defined") if !defined $self->{PeerAddr};
  croak("PeerPort is not defined") if !defined $self->{PeerPort};
  croak("User is not defined") if !defined $self->{User};

  # retry counter
  my $cnt = 0;
  my $timeout = 10;
retry:
  my $concurrency = $self->{Concurrency};
  my $ports = get_vacant_ports($self,$concurrency);
  my %commands = ();
  my $select = IO::Select->new();
  my $id = sprintf "READ(%s=>%s\@%s:%d)", $ENV{GXP_HOSTNAME}, $self->{User}, $self->{PeerAddr}, $self->{PeerPort};
  my ($child, $ssh_pid);
  for my $gwport (@$ports){
    $self->{gwport} = $gwport;
    my @l = map { my $a = $_; $a =~ s/%([^%]+)%/$self->{$1}/eg; $a } @login_cmd;
    print join(" ",@l), "\n" if($DEBUG || 1);
    ($child, $ssh_pid) = @{spawn_process(@l)};
    ErrFunc("PID = $ssh_pid") if $cnt;
    nonblock($child);
    $select->add($child);
    $commands{$gwport} = [$ssh_pid, $child];
  }
  my @fhs = $select->can_read($timeout);
  if(scalar @fhs){
    my $child = shift(@fhs);
    my $dummy = <$child>;
    chomp($dummy);
    if ( $dummy eq "" ) {
      # disconnected
      ErrFunc("W: $id is disconnected, retrying");
      for my $gwport (@$ports){
        $commands{$gwport}->[1]->close;
        #kill("TERM",$commands{$gwport}->[0]);
      }
      sleep 1;
      goto retry;
    } elsif ( $dummy ne "dummy" ) {
      ErrFunc("Unexpected response from ssh: $dummy");
      exit 1;
    }
    ErrFunc("OK: $id") if $cnt;
    for my $gwport (@$ports){
      if ($child == $commands{$gwport}->[1]) {
        $self->{gwport} = $gwport;
        $ssh_pid = $commands{$gwport}->[0];
      } else {
        $commands{$gwport}->[1]->close;
        #kill("TERM",$commands{$gwport}->[0]);
      }
    }
  }else{
    ++ $cnt;
    $timeout *= 2;
    my @pids = map {$commands{$_}->[0]} @$ports;
    ErrFunc(sprintf("W: $id retrying, kill %s", join(",",@pids)));
    for my $gwport (@$ports){
      $commands{$gwport}->[1]->close;
      #kill("TERM",$commands{$gwport}->[0]);
    }
    goto retry;
  }

  # ErrFunc(sprintf "%s: port=%d, pid=%d", $ENV{GXP_HOSTNAME}, $self->{gwport}, $ssh_pid);
  push ( @args, 
         Proto    => "tcp",
         PeerAddr => "localhost", 
         PeerPort => $self->{gwport});
  $self = $class->SUPER::new(@args);
  if(!$self){
    ErrFunc("Connection Failure: $!");
    kill "TERM", $ssh_pid;
    waitpid($ssh_pid,0);
    return undef;
  }

  $pids{$self} = $ssh_pid;

  # terminate dummy shell on the remote site
  print $child "\n";
  $self;
}

sub close {
  my $self = shift;
  my $ssh_pid = $pids{$self};
  delete $pids{$self};
  if($DEBUG){
    ErrFunc("SSH PID=$ssh_pid");
  }
  my $r = $self->SUPER::close(@_);
  waitpid($ssh_pid,0);
  $r;
}

1;
