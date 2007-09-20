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
        die "Value for $pos is undef!\n" unless defined $value;
        $ar[$pos] = $value;
    }

    return @ar;
}

{
    my @ar   = ( 1 .. 5 );
    my $iter = array_iter( @ar );
    my @got  = fill_array_from_iter( $iter );
    is_deeply \@got, \@ar, 'iterators';
}

for my $workers ( 1, 2, 10 ) {
    my @nums        = ( 1 .. 100 );
    my @double      = map $_ * 2, @nums;
    my $done = 0;
    my $double_iter = iterate(
        { workers => $workers },
        sub {
            my ( $id, $job ) = @_;
            return $job * 2;
        },
        array_iter( @nums )
    );

    my @got = fill_array_from_iter( $double_iter );
    unless ( is_deeply \@got, \@double, "double, $workers workers" ) {
        use Data::Dumper;
        diag Dumper(
            {
                got    => \@got,
                wanted => \@double
            }
        );
    }
}

