# $Id$
use strict;
use warnings;
use Test::More tests => 2;
use Parallel::Iterator qw( iterate_as_array );

for my $batch ( 97, 100 ) {
    my @in = ( 1 .. 1000 );
    my @want = map { $_ * 2 } @in;

    my @got = iterate_as_array(
        { batch => $batch },
        sub {
            my ( $id, $job ) = @_;
            return $job * 2;
        },
        \@in
    );

    is_deeply \@got, \@want, "processed OK";
}

1;
