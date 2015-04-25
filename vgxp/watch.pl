#!/usr/bin/perl -w

use strict;
use VGXP;

my $script_dir;
BEGIN {
  my $rel_script_dir = `dirname $0`;
  chomp($rel_script_dir);
  chomp($script_dir = `(cd $rel_script_dir; pwd)`);
  unshift(@INC, $script_dir);
  $ENV{'PERL5LIB'} = $script_dir;
}

system("mkdir -p logs");

my $fname_notify = "logs/notify_pmaster";
my $check_interval = 200;
my %nodes;
my $newnode_check_interval = 3;
my $max_log_count = 10;

sub process_trimlog {
  my ($l1, $l2) = @_;
  my %t1 = ();
  my %t2 = ();
  shift(@{$l1}); # the first element is 0 or 1 (is_interrupted)
  shift(@{$l2}); # the first element is 0 or 1 (is_interrupted)
  s/^(\S+)\s+: child (.*) will be trimmed/push(@{$t1{$1}},split(m|\s+|,$2))/eg for (@{$l1});
  s/^(\S+)\s+: child (.*) will be trimmed/push(@{$t2{$1}},split(m|\s+|,$2))/eg for (@{$l2});
  if(scalar keys %t2){
    print STDERR "Warning: some daemons could not trimmed by GXP3\n";
    for my $parent_id (keys %t2) {
      my ($parent_host, $parent_site, $parent_user) = ($parent_id =~ /^(([a-z]+).+)-(\D[^\-]*)-\d+/);
      for (@{$t2{$parent_id}}) {
        my ($host, $site, $user) = /^(([a-z]+).+)-(\D[^\-]*)-\d+/;
        my $cmd = "cat ./pkill.pl | gxpc e -h $parent_host perl -- /dev/stdin $host";
        print "  $cmd\n";
        system($cmd);
      }
    }
  }
}

VGXP::read_hosts();
VGXP::update_actives(1);

my $explore_timeout = 300; # 5 minutes
my $ping_count = 0;
my $log_count = 0;
while(1){
  my $f = VGXP::ping_hosts();
  $ping_count = ($ping_count + 1) % $newnode_check_interval;
  if($f || ($ping_count == 0)){
    if ( $f ) {
      printf STDERR "%s: Failure detected !!!            \n", scalar(localtime);
      # some nodes are down
      VGXP::exec_command([qw|gxpc smask|],1);
      my $trimlog = VGXP::exec_command([qw|gxpc trim|]);
      my $trimlog2 = VGXP::exec_command([qw|gxpc trim|]);
      process_trimlog($trimlog, $trimlog2);
    }
    # explore time
    my $cmd = "./explore >logs/explore_$log_count.log 2>&1";
    VGXP::exec_command(["sh","-c",$cmd],1,[$explore_timeout]);
    VGXP::exec_command(["pkill","-f","gxpc explore"],0);
    $log_count = ($log_count + 1) % $max_log_count;
    if ( VGXP::update_actives($f) ) {
      system("touch $fname_notify");
    }
  }
  sleep $check_interval;
}
