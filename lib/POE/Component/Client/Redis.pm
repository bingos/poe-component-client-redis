package POE::Component::Client::Redis;

#ABSTRACT: A POE Redis client

use strict;
use warnings;
use POE qw[Filter::Redis];
use Test::POE::Client::TCP;

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
     $self => [qw(_start _redis_cmd _connect _shutdown),
               map { join('_','_redis',$_) } qw(socket_failed connected disconnected input),
     ],
  ],
  heap => $self,
  ( ref($options) eq 'HASH' ? ( options => $options ) : () ),
  )->ID();
  return $self;
}

sub connected {
  return $_[0]->{_redis}->server_info() ? 1 : 0;
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
  $self->{_redis}->shutdown();
  $kernel->alarm_remove_all();
  $kernel->alias_remove( $_ ) for $kernel->alias_list();
  $kernel->refcount_decrement( $self->{session_id} => __PACKAGE__ ) unless $self->{alias};
  return;
}

sub _connect {
  my ($kernel,$self) = @_[KERNEL,OBJECT];
  $self->{_redis} = Test::POE::Client::TCP->spawn(
    address => $self->{host},
    port    => $self->{port},
    filter  => POE::Filter::Redis->new(),
    prefix  => '_redis',
    autoconnect => 1,
  );
  return;
}

sub _redis_socket_failed {
  my ($kernel,$self) = @_[KERNEL,OBJECT];
  warn "Couldn\'t establish a connection: ", join(' ', @_[ARG0..ARG2]), "\n";
  $kernel->yield( '_shutdown' );
  return;
}

sub _redis_connected {
  my ($kernel,$self,$socket,$peeraddr,$peerport) = @_[KERNEL,OBJECT,ARG0..ARG2];
  return;
}

sub _redis_input {
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

sub _redis_disconnected {
  my ($kernel,$self) = @_[KERNEL,OBJECT];
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
  return unless $self->{_redis}->server_info();
  my $sender_id = $sender->ID;
  $kernel->refcount_increment( $sender_id, __PACKAGE__ )
    unless exists $self->{_refs}->{ $sender_id };
  $self->{_refs}->{ $sender_id }++;
  push @{ $self->{_requests} }, { id => $sender_id, job => \@cmd, event => $event };
  $self->{_redis}->send_to_server( [ \@cmd ] );
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
