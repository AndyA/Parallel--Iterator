#!/usr/bin/perl

use strict;
use warnings;
use lib 'lib';
use Parallel::Iterator qw( iterate_as_array );

print "Parent process is $$\n";

my @d = qw( Pink Frog Muddle );
@d = ( @d, @d ) for 1 .. 10;
my @g = iterate_as_array( sub { $_[1] x 2 }, \@d );
# print join("\n", @g), "\n";

END {
  print "END in $$\n";
}
