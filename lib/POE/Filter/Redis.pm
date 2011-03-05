package POE::Filter::Redis;

#ABSTRACT: A POE Filter for the Redis protocol

use strict;
use warnings;
use Encode ();
use Carp qw[carp croak];
use base qw[POE::Filter];

# Bit mask.
use constant {
  PARSER_IDLE         => 0x01,
  PARSER_BETWEEN_BULK => 0x02,
  PARSER_IN_BULK      => 0x04,
};

use constant {
  SELF_BUFFER   => 0,
  SELF_ENCODING => 1,
  SELF_STATE    => 2,
  SELF_HAS      => 3,
  SELF_LENGTH   => 4,
  SELF_AWAITING => 5,
  SELF_TYPE     => 6,
};

sub new {
  my $package = shift;

  my %args = @_;

  if ( my $encoding = $args{encoding}) {
    $args{encoding} = Encode::find_encoding( $encoding );
    unless ( ref $args{encoding} ) {
      carp qq{Encoding '$encoding' is invalid, ignoring};
      delete $args{encoding};
    }
  }

  return bless [
    '',              # SELF_BUFFER
    $args{encoding}, # SELF_ENCODING
    PARSER_IDLE,     # SELF_STATE
    [ ],             # SELF_HAS
    0,               # SELF_LENGTH
    0,               # SELF_AWAITING
    undef,           # SELF_TYPE
  ], $package;
}

sub get_one_start {
  my ($self, $stream) = @_;
  $self->[SELF_BUFFER] .= join '', @{ $stream };
}

sub get_one {
  my $self = shift;

  return [ ] unless (
    length $self->[SELF_BUFFER] and $self->[SELF_BUFFER] =~ /\x0D\x0A/
  );

  # I expect it to be here mostly.
  if ($self->[SELF_STATE] & PARSER_IDLE) {

    # Single-line responses.  Remain in PARSER_IDLE state.
    return [ [ $1, $2 ] ] if $self->[SELF_BUFFER] =~ s/^([-+:])(.*?)\x0D\x0A//s;

    if ($self->[SELF_BUFFER] =~ s/^\*(-?\d+)\x0D\x0A//) {

      # Zero-item multibulk is an empty list.
      # Remain in the PARSER_IDLE state.
      return [ [ '*', ] ] if $1 == 0;

      # Negative item multibulk is an undef list.
      return [ [ '*', undef ] ] if $1 < 0;

      @$self[SELF_STATE, SELF_AWAITING, SELF_HAS, SELF_TYPE] = (
        PARSER_BETWEEN_BULK, $1, [], '*'
      );
    }
    elsif ($self->[SELF_BUFFER] =~ s/^\$(-?\d+)\x0D\x0A//) {

      # -1 length is undef.
      # Remain in the PARSER_IDLE state.
      return [ [ '$', undef ] ] if $1 < 0;

      @$self[SELF_STATE, SELF_AWAITING, SELF_LENGTH, SELF_HAS, SELF_TYPE] = (
        PARSER_IN_BULK, 1, $1 + 2, [], '$'
      );
    }
    else {
      # TODO - Recover somehow.
      die "illegal redis response:\n$self->[SELF_BUFFER]";
    }
  }

  while (1) {
    if ($self->[SELF_STATE] & PARSER_BETWEEN_BULK) {

      # Can't parse a bulk header?
      return [ ] unless $self->[SELF_BUFFER] =~ s/^\$(-?\d+)\x0D\x0A//;

      # -1 length is undef.
      if ($1 < 0) {
        if (push(@{$self->[SELF_HAS]}, undef) == $self->[SELF_AWAITING]) {
          $self->[SELF_STATE] = PARSER_IDLE;
          return [ [ $self->[SELF_TYPE], @{$self->[SELF_HAS]} ] ];
        }

        # Remain in PARSER_BETWEEN_BULK state.
        next;
      }

      # Got a bulk length.
      @$self[SELF_STATE, SELF_LENGTH] = (PARSER_IN_BULK, $1 + 2);

      # Fall through.
    }

    # TODO - Just for debugging..
    die "unexpected state $self->[SELF_STATE]" unless (
      $self->[SELF_STATE] & PARSER_IN_BULK
    );

    # Not enough data?
    return [ ] if length $self->[SELF_BUFFER] < $self->[SELF_LENGTH];

    # Got a bulk value.
    if (
      push(
        @{$self->[SELF_HAS]},
        substr(
          substr($self->[SELF_BUFFER], 0, $self->[SELF_LENGTH], ''),
          0, $self->[SELF_LENGTH] - 2
        )
      ) == $self->[SELF_AWAITING]
    ) {
      $self->[SELF_STATE] = PARSER_IDLE;
      return [ [ $self->[SELF_TYPE], @{$self->[SELF_HAS]} ] ];
    }

    # But... not enough of them.
    $self->[SELF_STATE] = PARSER_BETWEEN_BULK;
  }

  die "never gonna give you up, never gonna let you down";
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

# vim: ts=2 sw=2 expandtab
