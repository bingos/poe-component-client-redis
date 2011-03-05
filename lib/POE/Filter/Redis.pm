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

sub new {
  my $package = shift;
  my %args = @_;
  $args{lc $_} = delete $args{$_} for grep /[[:upper:]]/, keys %args;
  if ( my $encoding = $args{encoding}) {
    $args{encoding} = Encode::find_encoding( $encoding );
    unless ( ref $args{encoding} ) {
      carp qq{Encoding '$encoding' is invalid, ignoring};
      delete $args{encoding};
    }
  }
  @args{qw(BUFFER STATE)} = ('', PARSER_IDLE);
  return bless \%args, $package;
}

sub clone {
  my $self = shift;
  my $nself = { };
  $nself->{$_} = $self->{$_} for keys %{ $self };
  @$nself{qw(BUFFER STATE)} = ('', PARSER_IDLE);
  return bless $nself, ref $self;
}

sub get_one_start {
  my ($self, $stream) = @_;
  $self->{BUFFER} .= join '', @{ $stream };
}

sub get_one {
  my $self = shift;

  # I expect it to be here mostly.
  if ($self->{STATE} & PARSER_IDLE) {

    # Single-line responses.  Remain in PARSER_IDLE state.
    return [ [ $1, $2 ] ] if $self->{BUFFER} =~ s/^([-+:])(.*?)\x0D\x0A//s;

    if ($self->{BUFFER} =~ s/^\*(\d+)\x0D\x0A//) {

      # Zero-item multibulk is an empty list.
      # Remain in the PARSER_IDLE state.
      return [ [ '*', [] ] ] if $1 == 0;

      @$self{qw(STATE AWAITING HAS TYPE)} = (PARSER_BETWEEN_BULK, $1, [], '*');
    }
    elsif ($self->{BUFFER} =~ s/^\$(-?\d+)\x0D\x0A//) {

      # -1 length is undef.
      # Remain in the PARSER_IDLE state.
      return [ [ '$', undef ] ] if $1 == -1;;

      @$self{qw(STATE AWAITING LENGTH HAS TYPE)} = (
        PARSER_IN_BULK, 1, $1 + 2, [], '$'
      );
    }
    else {
      # TODO - Recover somehow.
      die "illgegal redis response:\n$self->{BUFFER}";
    }
  }

  while (1) {
    if ($self->{STATE} & PARSER_BETWEEN_BULK) {

      # Can't parse a bulk header?
      return [ ] unless $self->{BUFFER} =~ s/^\$(-?\d+)\x0D\x0A//;

      # -1 length is undef.
      if ($1 == -1) {
        if (push(@{$self->{HAS}}, undef) == $self->{AWAITING}) {
          $self->{STATE} = PARSER_IDLE;
          return [ @$self{qw(TYPE HAS)} ];
        }

        # Remain in PARSER_BETWEEN_BULK state.
        next;
      }

      # Got a bulk length.
      @$self{qw( STATE LENGTH )} = (PARSER_IN_BULK, $1 + 2);

      # Fall through.
    }

    # TODO - Just for debugging..
    die "unexpected state $self->{STATE}" unless (
      $self->{STATE} & PARSER_IN_BULK
    );

    # Not enough data?
    return [ ] if length $self->{BUFFER} < $self->{LENGTH};

    # Got a bulk value.
    if (
      push(
        @{$self->{HAS}},
        substr(
          substr($self->{BUFFER}, 0, $self->{LENGTH}, ''),
          0, $self->{LENGTH} - 2
        )
      ) == $self->{AWAITING}
    ) {
      $self->{STATE} = PARSER_IDLE;
      return [ @$self{qw(TYPE HAS)} ];
    }

    # But... not enough of them.
    $self->{STATE} = PARSER_BETWEEN_BULK;
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
