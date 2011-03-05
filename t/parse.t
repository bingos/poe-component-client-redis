#!perl

use warnings;
use strict;

use POE::Filter::Redis;
use YAML;
use Test::More tests => 26;

# Network Newline
my $nn = "\x0D\x0A";

my $f = POE::Filter::Redis->new();

{
	my $redis_stream = (
		"+OK${nn}" .               # Positive response.
		"-NOT OK THIS TIME${nn}" . # Negative response.
		":12345${nn}" .            # Single integer response.
		"\$-1${nn}" .              # Special! Undef!
		"\$5${nn}spang${nn}" .     # Single bulk reponse.
		"*0${nn}" .                # Empty list.
		"\$4${nn}narf${nn}" .      # Another single bulk reponse.
		"*-1${nn}"                 # Undef list.
	);

	my @redis_equiv = (
		[ '+', 'OK'               ],
		[ '-', 'NOT OK THIS TIME' ],
		[ ':', '12345'            ],
		[ '$', undef              ],
		[ '$', 'spang'            ],
		[ '*',                    ],
		[ '$', 'narf'             ],
		[ '*', undef              ],
	);

	my @result = @{ $f->get([ $redis_stream ]) };
	is_deeply(\@result, \@redis_equiv, 'redis stream parsed');
}

{
	my $redis_stream = "*3${nn}\$1${nn}a${nn}\$-1${nn}\$3${nn}def${nn}";

	my @redis_equiv = (
		([]) x (length($redis_stream) - 1),
		[ [ '*', 'a', undef, 'def' ] ],
	);

	foreach my $character ($redis_stream =~ m/(.)/sg) {
		$f->get_one_start([ $character ]);
		my @result = @{ $f->get_one([ $character ]) };
		is_deeply(\@result, shift @redis_equiv, "parsing stream incrementally");
	}
}
