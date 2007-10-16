#!/usr/bin/perl

use strict;
use warnings;
use Inline 'C';
use Parallel::Iterator qw( iterate_as_array );

my @ar = qw( This Pork Bubble );
@ar = ( @ar, @ar ) for 1 .. 5;
my @got = iterate_as_array( sub { calc( $_[1] ) }, \@ar );
print join( ', ', @got ), "\n";

__END__    
__C__
int calc(char *str) {
    int sum = 0;
    int c;
    while (c = *str++) {
        sum = sum << 3 | c;
    }
    return sum;
}
