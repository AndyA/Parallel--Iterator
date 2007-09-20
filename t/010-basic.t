use strict;
use warnings;
use Test::More tests => 4;
use Parallel::Workers qw( iterate );

sub array_iter {
    my @ar  = @_;
    my $pos = 0;
    return sub {
        return if $pos >= @ar;
        my @r = ( $pos, $ar[$pos] );
        $pos++;
        return @r;
    };
}

sub fill_array_from_iter {
    my $iter = shift;
    my @ar   = ();
    while ( my ( $pos, $value ) = $iter->() ) {
        $ar[$pos] = $value;
    }

    return @ar;
}

{
    my @ar   = ( 1, 2, 3, 4, 5 );
    my $iter = array_iter( @ar );
    my @got  = fill_array_from_iter( $iter );
    is_deeply \@got, \@ar, 'iterators';
}

for my $workers ( 1, 2, 10 ) {
    my $double_iter = iterate(
        { workers => $workers },
        sub {
            my ( $id, $job ) = @_;
            sleep 1;
            return $job * 2;
        },
        array_iter( 1, 2, 3, 4, 5 )
    );

    my @got = fill_array_from_iter( $double_iter );
    my @want = ( 2, 4, 6, 8, 10 );
    is_deeply \@got, \@want, "double, $workers workers";
}

