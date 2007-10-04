# $Id$
use strict;
use warnings;
use Test::More;
use Parallel::Iterator qw( iterate_as_array );
use Time::HiRes qw( usleep );

my @spec = (
    {
        name    => 'default',
        options => {}
    },
    {
        name    => 'workers=0',
        options => { workers => 0 }
    },
    {
        name    => 'workers=1',
        options => { workers => 1 }
    },
    {
        name    => 'batch=13',
        options => { batch => 13 }
    },
    {
        name    => 'batch=20',
        options => { batch => 20 }
    },
    {
        name    => 'adaptive=1',
        options => { adaptive => 1 }
    },
    {
        name    => 'adaptive=1.234',
        options => { adaptive => 1.234 }
    },
    {
        name    => 'adaptive=2',
        options => { adaptive => 2 }
    },
    {
        name    => 'adaptive=[10,1,20]',
        options => { adaptive => [ 10, 1, 20 ] }
    }
);

plan tests => @spec * 1;

my $tot_sd = 0;

for my $spec ( @spec ) {
    my $name = $spec->{name};
    my @in   = ( 1 .. 50 );
    my @got  = iterate_as_array(
        $spec->{options},
        sub {
            usleep 100000;
            return $$;
        },
        \@in
    );

    my %by_pid = ();
    $by_pid{$_}++ for @got;

    diag "$name\n";
    diag sprintf( "%6d : %6d\n", $_, $by_pid{$_} )
      for sort { $a <=> $b } keys %by_pid;

    my $var = variance( values %by_pid );
    my $sd  = sqrt( $var );
    diag "SD: $sd\n";
    $tot_sd += $sd;

    ok 1, "$name";
}

diag "Average SD: ", $tot_sd / @spec, "\n";

sub variance {
    my @l    = @_;
    my $mean = 0;
    $mean += $_ for @l;
    $mean /= @l;
    my $var = 0;
    for ( @l ) {
        my $diff = $_ - $mean;
        $var += $diff * $diff;
    }
    return $var / @l;
}

1;
