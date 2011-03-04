package POE::Filter::Redis;

#ABSTRACT: A POE Filter for the Redis protocol

use strict;
use warnings;
use Encode ();
use Carp qw[carp croak];
use base qw[POE::Filter];

sub new {
  my $package = shift;
  my %args = @_;
  $args{lc $_} = delete $args{$_} for keys %args;
  if ( my $encoding = $args{encoding}) {
    $args{encoding} = Encode::find_encoding( $encoding );
    unless ( ref $args{encoding} ) {
      carp qq{Encoding '$encoding' is invalid, ignoring};
      delete $args{encoding};
    }
  }
  $args{BUFFER} = '';
  return bless \%args, $package;
}

sub clone {
  my $self = shift;
  my $nself = { };
  $nself->{$_} = $self->{$_} for keys %{ $self };
  $nself->{BUFFER} = '';
  return bless $nself, ref $self;
}

sub get_one_start {
  my ($self, $stream) = @_;
  $self->{BUFFER} .= join '', @{ $stream };
}

sub get_one {
}

sub put {
  my ($self,$cmds) = @_;

  my @raw;
  foreach my $line ( @{ $cmds } ) {
    next unless ref $line eq 'ARRAY';
    my $cmd = shift @{ $line };
    push @raw, 
      join( "\x0D\x0A", 
            '*' . ( 1 + @{ $line } ),
            map { ('$' . length $_ => $_) }
              ( uc($cmd), map { $self->{encoding} && length($_)
                                ? $self->{encoding}->encode($_)
                                : $_ } @{ $line } ) );
  }
  \@raw;
}

qq[Redis Filter];

=pod

=cut
