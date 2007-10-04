#!/usr/bin/perl

use strict;
use warnings;
use lib 'lib';
use Parallel::Iterator qw( iterate );
use Time::HiRes qw( usleep );

$| = 1;

sub infinite {
    my $next = 1;
    return sub {
        return ( $next++ );
    };
}

my $iter = iterate(
    { workers => 14 },
    sub {
        usleep 250_000;
        return $$;
    },
    infinite()
);

my %per_pid = ();
my @pids    = ();
while ( my ( $id, $pid ) = $iter->() ) {
    if ( 1 == ++$per_pid{$pid} ) {
        @pids = sort { $a <=> $b } keys %per_pid;
        my $hdr = join( ' | ', map { sprintf( "%5d", $_ ) } @pids );
        my $rule = '=' x length $hdr;
        print "\n\n$rule\n$hdr\n$rule\n";
    }
    print "\r", join( ' | ', map { sprintf( "%5d", $per_pid{$_} ) } @pids );
}
