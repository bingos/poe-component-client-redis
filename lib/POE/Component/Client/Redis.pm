package POE::Component::Client::Redis;

#ABSTRACT: A POE Redis client

use strict;
use warnings;
use Socket qw[AF_INET SOCK_STREAM];
use POE qw[Filter::Redis Wheel::SocketFactory Wheel::ReadWrite];

sub spawn {
  my $package = shift;
  my %opts = @_;
  $opts{lc $_} = delete $opts{$_} for keys %opts;
  $opts{host} = 'localhost' unless $opts{host};
  $opts{port} = 6379 unless $opts{port};
  my $options = delete $opts{options};
  my $self = bless \%opts, $package;
  $self->{session_id} = POE::Session->create(
  object_states => [
     $self => { shutdown => '_shutdown', redis_cmd => '_redis_cmd' },
     $self => [qw(_start _redis_cmd _connect _sock_up _sock_fail _conn_input _conn_error _shutdown)],
  ],
  heap => $self,
  ( ref($options) eq 'HASH' ? ( options => $options ) : () ),
  )->ID();
  return $self;
}

sub connected {
  return $_[0]->{_wheel} ? 1 : 0;
}

sub session_id {
  return $_[0]->{session_id};
}

sub shutdown {
  my $self = shift;
  $poe_kernel->call( $self->{session_id}, '_shutdown' );
}

sub redis_cmd {
  my $self = shift;
  $poe_kernel->post( $self->{session_id}, '_redis_cmd', @_ );
}

sub _start {
  my ($kernel,$self) = @_[KERNEL,OBJECT];
  $self->{session_id} = $_[SESSION]->ID();
  if ( $self->{alias} ) {
     $kernel->alias_set( $self->{alias} );
  }
  else {
     $kernel->refcount_increment( $self->{session_id} => __PACKAGE__ );
  }
  $self->{_requests} = [ ];
  $self->{_refs} = { };
  $kernel->yield( '_connect' );
  return;
}

sub _shutdown {
  my ($kernel,$self) = @_[KERNEL,OBJECT];
  foreach my $id ( keys %{ $self->{_refs} } ) {
    $kernel->refcount_decrement( $id, __PACKAGE__ );
    delete $self->{_refs}->{ $id };
  }
  delete $self->{_refs};
  delete $self->{_requests};
  delete $self->{_factory};
  delete $self->{_wheel};
  $kernel->alarm_remove_all();
  $kernel->alias_remove( $_ ) for $kernel->alias_list();
  $kernel->refcount_decrement( $self->{session_id} => __PACKAGE__ ) unless $self->{alias};
  return;
}

sub _connect {
  my ($kernel,$self) = @_[KERNEL,OBJECT];
  $self->{_factory} = POE::Wheel::SocketFactory->new(
      SocketDomain => AF_INET,
      SocketType => SOCK_STREAM,
      SocketProtocol => 'tcp',
      RemoteAddress => $self->{host},
      RemotePort => $self->{port},
      SuccessEvent => '_sock_up',
      FailureEvent => '_sock_fail',
  );
  return;
}

sub _sock_fail {
  my ($kernel,$self) = @_[KERNEL,OBJECT];
  delete $self->{_factory};
  warn "Couldn\'t establish a connection: ", join(' ', @_[ARG0..ARG2]), "\n";
  $kernel->yield( '_shutdown' );
  return;
}

sub _sock_up {
  my ($kernel,$self,$socket,$peeraddr,$peerport) = @_[KERNEL,OBJECT,ARG0..ARG2];
  delete $self->{_factory};
    $self->{_wheel} = POE::Wheel::ReadWrite->new(
    Handle => $socket,
    Filter => POE::Filter::Redis->new(),
    InputEvent => '_conn_input',
    ErrorEvent => '_conn_error',
  );
  return;
}

sub _conn_input {
  my ($kernel,$self,$input) = @_[KERNEL,OBJECT,ARG0];
  my $job = shift @{ $self->{_requests } };
  my $id = $job->{id};
  my $event = $job->{event};
  $kernel->post( $id, $event, $input );
  $self->{_refs}->{ $id }--;
  unless ( $self->{_refs}->{ $id } ) {
    $kernel->refcount_decrement( $id, __PACKAGE__ );
    delete $self->{_refs}->{ $id };
  }
  return;
}

sub _conn_error {
  my ($kernel,$self) = @_[KERNEL,OBJECT];
  delete $self->{_socket};
  warn "Disconnected from redis server\n";
  $kernel->yield( '_shutdown' );
  return;
}

sub _redis_cmd {
  my ($kernel,$self,$sender) = @_[KERNEL,OBJECT,SENDER];
  my $event = $_[ARG0];
  return unless $event;
  my @cmd = @_[ARG1..$#_];
  return unless scalar @cmd;
  return unless $self->{_wheel};
  my $sender_id = $sender->ID;
  $kernel->refcount_increment( $sender_id, __PACKAGE__ )
    unless exists $self->{_refs}->{ $sender_id };
  $self->{_refs}->{ $sender_id }++;
  push @{ $self->{_requests} }, { id => $sender_id, job => \@cmd, event => $event };
  $self->{_wheel}->put( \@cmd );
  return;
}

qq[Redis and dat];

=pod

=head1 CONSTRUCTOR

=over

=item C<spawn>

=back

=head1 METHODS

=over

=item C<connected>

=item C<session_id>

=item C<shutdown>

=item C<redis_cmd>

=back

=cut
