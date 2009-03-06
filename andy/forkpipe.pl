#!/usr/bin/perl

use strict;
use warnings;
use IO::Handle;

$| = 1;

my $rdr = IO::Handle->new;
my $wtr = IO::Handle->new;

pipe $rdr, $wtr or die "Can't create pipe ($!)\n";

if ( my $pid = fork ) {
  while ( <$rdr> ) {
    print;
  }
}
else {
  $wtr->autoflush( 1 );
  for ( 1 .. 10 ) {
    print $wtr "x$_\n";
    sleep 1;
  }
  close $wtr;
}
