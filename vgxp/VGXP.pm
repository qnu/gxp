# -*- Perl -*-
package VGXP;

use strict;
use FileHandle;
use IPC::Open2;

our $fname_nodes = "targets2";
our $fname_actives = "actives";
our %nodes;
our $SUFFIX = "default";

my $default_timeout = [10, 20];
my $fast_check = 0;

sub exec_command {
  my ($cmd, $do_gxp_check, $timeout, $sig, $input) = @_;
  $timeout = $default_timeout if ! defined $timeout;
  $sig = "TERM" if ! defined $sig;
  $input = "/dev/null" if ! defined $input;
  my $interrupted = 0;
  my $restart_count = 0;
  my @r = ();
  my $cmdstr = join(" ",@{$cmd});
 restart:
  eval {
    my $pid = 0;
    local $SIG{ALRM} = sub { kill($sig, $pid) if $pid; my $t = $timeout->[$restart_count]; print STDERR "timeout $t expired: $cmdstr\n"; ++ $interrupted; };
    alarm $timeout->[$restart_count];
    if($do_gxp_check){
      gxp_check();
      alarm $timeout->[$restart_count];
    }
    my($wtr, $rdr);
    eval {
      $pid = open2($rdr, $wtr, @{$cmd});
    };
    if ($@) {
      my $err = $@;
      die "fork($cmdstr) failed: $@";
    }
    my $ifile;
    open($ifile, $input);
    while(<$ifile>){
      print $wtr $_;
    }
    close($ifile);
    close($wtr);
    while (<$rdr>) {
      chomp($_);
      push(@r,$_);
    }
    close($rdr) || do {
      if ( $interrupted == $restart_count ) {
        printf "close failed: $cmdstr\n";
        warn "child process exited $?";
      }
    };
    alarm 0;
    waitpid $pid, 0;
  };
  if ( $interrupted != $restart_count ) {
    ++ $restart_count;
    if ( $restart_count < scalar @{$timeout} ) {
      print STDERR "cmd restart\n";
      goto restart;
    }
    print STDERR "cmd: Max restart count $restart_count exceeded.\n";
  }
  unshift(@r, ($interrupted == $restart_count));
  return \@r;
}

sub gxp_check {
  my $u = $ENV{'LOGNAME'};
  die("LOGNAME is undefined") if ! defined $u;
  while(1){
    delete $ENV{GXP_SESSION};
    my @sessions = </tmp/gxp-$u-$SUFFIX/gxp*session-*>;
    if(scalar(@sessions) > 1){
      # delete all sessions
      for(@sessions){
        system("gxpc --session $_ quit --session_only");
      }
    } elsif(scalar(@sessions) == 0) {
      system("gxpc >/dev/null 2>&1");
    } else {
      $sessions[0] =~ /(gxpsession*)$/;
      $ENV{GXP_SESSION} = $sessions[0];
      return;
    }
  }
}

sub gxp_env {
  gxp_check;
  print $ENV{GXP_SESSION}, "\n";
}

sub expand_hosts{
  my $h = $_[0];
  return ($h) if $h !~ /\[\[(\d+)-(\d+)\]\]/;
  my @r = ();
  for my $g ($1 .. $2){
    my $x = $h;
    $x =~ s/\[\[(\d+)-(\d+)\]\]/$g/;
    push(@r,expand_hosts($x));
  }
  @r;
}

sub read_hosts {
  # Initialize nodes table
  open(FILE, $fname_nodes) || die("Not found: $fname_nodes");
  while(<FILE>){
    chomp($_);
    s/^\s*//;
    s/\s*$//;
    if (!/^\#/) {
      for my $h (expand_hosts($_)){
        $nodes{$h} = 0;
      }
    }
  }
  close FILE;
}

sub ping_hosts {
  my $ping_timeout = [5, 5, 5, 5, 5];
  my $r = exec_command([qw|gxpc ping|],1,$ping_timeout,"INT");
  my $f = shift(@$r);
  open(FILE, $ENV{GXP_SESSION});
  my $abc = <FILE>;
  $abc =~ m#(\d+)/(\d+)/(\d+)#;
  $abc = [$1,$2,$3];
  my $isfailure = ($1 != $2);
  for (@$r){
    if ( /(no children|is dead)/ ) {
      $isfailure = 1;
    } else {
      $nodes{$_} = 1;
    }
  }
  return $isfailure;
}

sub update_actives ($) {
  my $failure_detected = $_[0];
  for my $h (keys %nodes){
    $nodes{$h} = 0;
  }
  my $isfailure = ping_hosts;
  if ( $isfailure ) {
    printf STDERR "%s: Failure detected ***         \n", scalar(localtime);
    VGXP::exec_command([qw|gxpc smask|],1);
    VGXP::exec_command([qw|gxpc trim|]);
  }
  my %actives = ();
  open(FILE,"$fname_actives") && do {
    while(<FILE>){
      my @a = split(/\s+/,$_);
      $actives{$a[0]} = int($a[1]);
    }
  };
  close FILE;
  # compare actives and nodes;
  my $changed = 0;
  if ( $fast_check ) {
    for my $h (keys %nodes){
      if(!defined($actives{$h}) || ($nodes{$h} ne $actives{$h})){
        $changed = 1;
        last;
      }
    }
    if(!$changed){
      for my $h (keys %actives){
        if(!defined($nodes{$h}) || ($nodes{$h} ne $actives{$h})){
          $changed = 1;
          last;
        }
      }
    }
  } else {
    my @added = ();
    my @removed = ();
    for my $h (keys %nodes){
      if(!defined($actives{$h}) || ($nodes{$h} > $actives{$h})){
        $changed = 1;
        push ( @added, $h );
      }
    }
    for my $h (keys %actives){
      if(!defined($nodes{$h}) || ($nodes{$h} < $actives{$h})){
        $changed = 1;
        push ( @removed, $h );
      }
    }
    for ( @added ) {
      print STDERR "Added: $_              \n";
    }
    for ( @removed ) {
      print STDERR "Removed: $_            \n";
    }
    if ( (@added > 0) || (@removed > 0) ) {
      printf STDERR "%s: Changed active nodes: +%d -%d\n", scalar localtime, scalar @added, scalar @removed;
    } else {
      printf STDERR "%s     \r", scalar localtime;
    }
  }
  return 0 if ! $changed;
  # write if changed
  open(FILE,">$fname_actives") || die("Cannot open: $fname_actives");
  for my $h (sort keys %nodes){
    print FILE "$h $nodes{$h}\n";
  }
  close(FILE);
  return 1;
}

1;
