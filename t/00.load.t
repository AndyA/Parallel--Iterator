use Test::More tests => 1;

BEGIN {
use_ok( 'Parallel::Workers' );
}

diag( "Testing Parallel::Workers $Parallel::Workers::VERSION" );
