use strict;
use warnings;
use Test::More qw[no_plan];
use POE qw[Component::Client::Redis];

{
  require IO::Socket::INET;
  my $sock = IO::Socket::INET->new( PeerAddr => 'localhost', PeerPort => 6379, Timeout => 10 )
     or do { ok('no redis server found'); exit 0; };
}

my $redis = POE::Component::Client::Redis->spawn();

POE::Session->create(
  package_states => [
    'main' => [ qw(_start _timer _what _stop) ],
  ]
);

$poe_kernel->run();
exit 0;

sub _start {
  $poe_kernel->delay( '_timer', 1 );
  return;
}

sub _timer {
  if ( $redis->connected ) {
    $redis->redis_cmd( '_what', 'INFO' );
    return;
  }
  $poe_kernel->delay( '_timer', 1 );
  return;
}

sub _stop {
  pass('Let my people go go');
  $redis->shutdown();
  return;
}

sub _what {
  my $resp = $_[ARG0];
  is( ref $resp, 'ARRAY', 'Got an array reference' );
  is( $resp->[0], '$', 'Correct type' );
  return;
}
