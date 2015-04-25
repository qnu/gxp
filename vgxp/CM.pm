# -*- Perl -*-
# Connection Manager
package CM;

# single-thread server
# TODO: Error handling
#   EINTR: ignore?
#   EPIPE: shutdown?

use strict;
use POSIX;
use IO::Socket;
use IO::Select;
use Socket;
use Fcntl;
use Time::HiRes qw(time gettimeofday tv_interval);

our $MAX_OUTBUF_SIZE = 30 * 1024 * 1024; # 30Mbytes
our $OUTBUF_SIZE_SOFT_LIMIT = 3 * 1024 * 1024; # 3Mbytes
our $counter = 0;
our $parent = undef;
our @errbuf = ();
our $hostname = $ENV{GXP_HOSTNAME} || `hostname -s`;
chomp $hostname;
$hostname =~ s/\..*$//;

sub new ($) {
  my $class = shift;
  my $self = {
    s => {},
    r => IO::Select->new(),
    w => IO::Select->new(),
    e => IO::Select->new(),
    q => []
  };
  bless $self => $class;
}

sub nonblock ($) {
  my $socket = shift;
  my $flags;
  $flags = fcntl($socket, F_GETFL, 0)
      or die "Can't get flags for socket: $!\n";
  fcntl($socket, F_SETFL, $flags | O_NONBLOCK)
      or die "Can't make socket nonblocking: $!\n";
}

sub set_parent (\%$) {
  my ($self, $p) = @_;
  $parent = $p;
  if(defined($parent) && @errbuf){
    while ( @errbuf ) {
      $self->send_data($parent, shift(@errbuf));
    }
  }
}

sub select_add_w (\%$) {
  my ($self, $fd) = @_;
  $self->{w}->add($fd);
  $self->{e}->add($fd);
}

sub select_remove_w (\%$) {
  my ($self, $fd) = @_;
  $self->{w}->remove($fd);
  $self->{e}->remove($fd) if ! $self->{r}->exists($fd);
}

sub select_add_r (\%$) {
  my ($self, $fd) = @_;
  $self->{r}->add($fd);
  $self->{e}->add($fd);
}

sub select_add (\%$) {
  my ($self, $fd) = @_;
  $self->{r}->add($fd);
  $self->{w}->add($fd);
  $self->{e}->add($fd);
}

sub select_remove (\%$) {
  my ($self, $fd) = @_;
  $self->{r}->remove($fd);
  $self->{w}->remove($fd);
  $self->{e}->remove($fd);
}

sub add_socket (\%$$;$$) {
  my ($self, $sock, $isserver, $name, $timeout) = @_;
  my $fileno = $sock->fileno;
  #$self->send_err("ADD SOCKET: $fileno $isserver $name");
  nonblock($sock);
  $self->select_add_r($fileno);
  $self->{s}->{$fileno} = {}; # initialize sockinfo
  my $sockinfo = $self->{s}->{$fileno};
  $sockinfo->{sock} = $sock;
  $sockinfo->{isserver} = $isserver if $isserver;
  $sockinfo->{name} = $name if $name;
  if($timeout){
    $sockinfo->{last_updated} = time;
    $sockinfo->{timeout} = $timeout;
  }
  $fileno;
}

sub remove_timeout(\%$){
  my ($self, $fileno) = @_;
  my $sockinfo = $self->{s}->{$fileno};
  delete $sockinfo->{last_updated};
  delete $sockinfo->{timeout};
}

sub add_server (\%$;$) {
  my ($self, $sock, $timeout) = @_;
  $self->add_socket($sock, 1, undef, $timeout);
}

sub add_client (\%$$) {
  my ($self, $sock, $name) = @_;
  $self->add_socket($sock, 0, $name);
}

sub register_close_handler (\%\&;$) {
  my ($self, $func, $hname) = @_;
  $hname = "DEFAULT" if ! defined $hname;
  $self->{close_handlers}->{$hname} = $func;
}

sub remove_close_handler (\%$) {
  my ($self, $hname) = @_;
  $hname = "DEFAULT" if ! defined $hname;
  delete $self->{close_handlers}->{$hname};
}

sub call_close_handlers (\%$) {
  my ($self, $fileno) = @_;
  while(my ($hname, $func) = each %{$self->{close_handlers}}){
    &{$func}($self,$fileno);
  }
}

sub register_timeout_handler (\%\&;$) {
  my ($self, $func, $hname) = @_;
  $hname = "DEFAULT" if ! defined $hname;
  $self->{timeout_handlers}->{$hname} = $func;
}

sub remove_timeout_handler (\%$) {
  my ($self, $hname) = @_;
  $hname = "DEFAULT" if ! defined $hname;
  delete $self->{timeout_handlers}->{$hname};
}

sub call_timeout_handlers (\%$) {
  my ($self, $fileno) = @_;
  while(my ($hname, $func) = each %{$self->{timeout_handlers}}){
    &{$func}($self,$fileno);
  }
}

sub close_client (\%$$) {
  my ($self, $fileno, $reason) = @_;
  my $sockinfo = $self->{s}->{$fileno};
  my $name = $sockinfo->{name};
  if(!defined $name){
    $name = "Unknown($fileno)";
  }
  my $sock = $sockinfo->{sock};
  delete $sockinfo->{$fileno};
  $self->send_err(sprintf("%s Disconnected: %s", $reason, $name));
  eval {
    $self->select_remove($fileno);
  };
  $self->call_close_handlers($fileno);
  eval {
    close $sock;
  };
}

sub socket_name (\%$) {
  my ($self,$fileno) = @_;
  $self->{s}->{$fileno}->{name};
}

sub send_data (\%$$;$) {
  my ($self,$fileno,$buf,$in_error) = @_;
  my $sockinfo = $self->{s}->{$fileno};
  die ("sockinfo is null") if ! defined $sockinfo;
  $sockinfo->{o} .= $buf;
  $self->select_add_w($fileno) if length ($sockinfo->{o});
  if(length($sockinfo->{o}) > $OUTBUF_SIZE_SOFT_LIMIT){
    if(0 && length($sockinfo->{o}) > $MAX_OUTBUF_SIZE){
      $self->close_client($fileno,"F");
    } elsif ( !$in_error ) {
      $self->send_err(sprintf("bufsize %d is too large",length($sockinfo->{o})),1);
    }
  }
}

sub send_err (\%$;$) {
  my ($self,$buf,$in_error) = @_;
  my $datestr = scalar localtime;
  my $str = "ERR $datestr $hostname: $buf\n";
  if ( !defined $parent ) {
    push(@errbuf, $str);
    return;
  }
  $self->send_data($parent, $str, $in_error);
}

sub register_accept_handler (\%\&;$) {
  my ($self, $func, $hname) = @_;
  $hname = "DEFAULT" if ! defined $hname;
  $self->{accept_handlers}->{$hname} = $func;
}

sub remove_accept_handler (\%$) {
  my ($self, $hname) = @_;
  $hname = "DEFAULT" if ! defined $hname;
  delete $self->{accept_handlers}->{$hname};
}

sub call_accept_handlers (\%$) {
  my ($self, $fileno) = @_;
  while(my ($hname, $func) = each %{$self->{accept_handlers}}){
    &{$func}($self,$fileno);
  }
}

sub register_request_handler (\%\&;$) {
  my ($self, $func, $hname) = @_;
  $hname = "DEFAULT" if ! defined $hname;
  $self->{request_handlers}->{$hname} = $func;
}

sub remove_request_handler (\%$) {
  my ($self, $hname) = @_;
  $hname = "DEFAULT" if ! defined $hname;
  delete $self->{request_handlers}->{$hname};
}

sub call_request_handlers (\%\@) {
  my ($self, $q) = @_;
  while(my ($hname, $func) = each %{$self->{request_handlers}}){
    &{$func}($self,$q);
  }
}

sub main_loop (\%) {
  my $self = shift;
  while (1) {
    #small wait
    #Time::HiRes::usleep(1000);

    my ($rv, $data);
    #$self->send_err("READ from: " . join(" ",$self->{r}->handles));
    my $timeout = undef;
    my $cur_time = time;
    for my $fileno (keys %{$self->{s}}) {
      my $sockinfo = $self->{s}->{$fileno};
      if($sockinfo->{timeout}){
        if(! defined $timeout ||
           ($timeout > $sockinfo->{last_updated} + $sockinfo->{timeout} - $cur_time)){
          $timeout = $sockinfo->{last_updated} + $sockinfo->{timeout} - $cur_time;
        }
      }
    }
    my ($rfds, $wfds, $efds);
    if(!defined $timeout || $timeout > 0){
      ($rfds, $wfds, $efds) = IO::Select::select($self->{r},$self->{w},$self->{e}, $timeout);
      $cur_time = time;
      #$self->send_err("WRITE OK: " . join(" ",@$wfds));
      #$self->send_err("READ  OK: " . join(" ",@$rfds));
    }
    if((defined $timeout && ($timeout <= 0)) ||
       ((0 == @{$rfds}) && (0 == @{$wfds}) && (0 == @{$efds}))){
      $self->send_err("Timeout $timeout expired");
      for my $fileno (keys %{$self->{s}}) {
        my $sockinfo = $self->{s}->{$fileno};
        if($sockinfo->{timeout}){
          if(0 >= $sockinfo->{last_updated} + $sockinfo->{timeout} - $cur_time){
            $self->call_timeout_handlers($fileno);
          }
        }
      }
      next;
    }
    my $close_flag = 0;
    for my $fileno ( @{$efds} ) {
      $self->close_client($fileno, "E");
    }
    next if scalar @{$efds};
    for my $fileno ( @{$wfds} ) {
      my $sockinfo = $self->{s}->{$fileno};
      my $name = $sockinfo->{name};
      my $sock = $sockinfo->{sock};
      if ( $sock->connected() ) {
        eval {
          #$rv = $sock->send($sockinfo->{o}, 0);
          $rv = $sock->syswrite($sockinfo->{o});
        };
        if ($@) {
          my $err = $@;
          chomp($err);
          $self->send_err("ERR! $err , rv = $rv");
          $rv = -1;
        }
      } else {
        $self->send_err("Client disconnected");
        $self->close_client($fileno,"W");
        $close_flag = 1;
        next;
      }
      unless(defined $rv){
        $self->send_err("I was told I could write, but I can't.");
        next;
      }
      if($rv > 0 ||
         #$rv == length $h->{_outbuffer} ||
         $! == POSIX::EWOULDBLOCK ){
        substr($sockinfo->{o}, 0, $rv) = '';
        if ( length($sockinfo->{o}) == 0 ) {
          $self->select_remove_w($fileno);
        }
      }else{
        $self->send_err(sprintf("ERR: fileno=%d, rv=%d, len(obuf)=%d, err=%s", $fileno, $rv, length $sockinfo->{o}, $!));
        $self->close_client($fileno,"W");
        next;
      }
    }
    next if $close_flag;
    for my $fileno ( @{$rfds} ) {
      my $sockinfo = $self->{s}->{$fileno};
      if($sockinfo->{timeout}){
        $sockinfo->{last_updated} = $cur_time;
      }
      if ( $sockinfo->{isserver} ) {
        my $sock = $sockinfo->{sock}->accept();
        my $name = sprintf "client%d", $counter ++;
        $self->add_client($sock,$name);
        my ($client_port,$client_iaddr) = unpack_sockaddr_in($sock->peername());
        my $client_name = inet_ntoa($client_iaddr) . ":$client_port";
        $self->send_err(sprintf("C Connected: %s", $client_name));
        $self->call_accept_handlers($sock->fileno);
      } else {
        my $sock = $sockinfo->{sock};
        $data = '';
        #$rv = $sock->recv($data, POSIX::BUFSIZ, 0);
        $rv = $sock->sysread($data, POSIX::BUFSIZ);
        unless (defined($rv) && length $data) {
          # probably EOF
          $self->close_client($fileno,"R");
          next;
        }
        if ( $data =~ s/(.*\n)// ) {
          push(@{$self->{q}}, [$fileno, ($sockinfo->{i} || "") . $1]);
          while($data =~ s/(.*\n)//){
            push(@{$self->{q}}, [$fileno, $1]);
          }
          $sockinfo->{i} = $data;
        } else {
          $sockinfo->{i} .= $data;
        }
      }
    }
    $self->call_request_handlers($self->{q}) if scalar @{$self->{q}};
  }
}

1;
