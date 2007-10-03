# $Id: Iterator.pm 2666 2007-10-02 22:04:36Z andy $
use strict;
use warnings;
use IO::Handle;
use POSIX qw(:errno_h);
use Test::More tests => 1;
use Parallel::Iterator qw( iterate_as_array );

my $buffer_size = get_pipe_buffer_size();

# diag "I/O buffer size: $buffer_size\n";

{
    # Random data
    my $data = join '', map chr rand 256, (1 .. $buffer_size * 2);

    # Just in case someone decides to generate data by some other
    # means...
    die "Not enough data!" unless length $data > $buffer_size;

    my @input = (
        {
            type  => 'hash',
            value => $data,
        },
        [ 1, $data, 3 ],
        $data,
    );

    my @want = (
        {
            type  => 'hash',
            value => "$data!",
        },
        [ $data, $data ],
        $data . $data,
    );

    for ( 1 .. 4 ) {
        @input = ( @input, @input );
        @want  = ( @want,  @want );
    }

    my @got = iterate_as_array(
        { workers => 5 },
        sub {
            my ( $id, $job ) = @_;
            if ( ref $job ) {
                if ( 'HASH' eq ref $job ) {
                    $job->{value} .= '!';
                    return $job;
                }
                elsif ( 'ARRAY' eq ref $job ) {
                    return [ $data, $data ];
                }
            }
            else {
                return $job . $job;
            }
        },
        \@input
    );

    is_deeply \@got, \@want, "big data structure";
}

# Find out how much data we can write to a pipe...
sub get_pipe_buffer_size {
    my ( $in, $out ) = map IO::Handle->new, 1 .. 2;

    pipe $in, $out or die "Can't make pipe ($!)\n";

    defined $out->blocking( 0 ) or die "Can't turn off blocking ($!)\n";

    my $chunk = ' ' x ( 1024 * 4 );
    my $wrote = 0;

    CHUNK: while ( 1 ) {
        my $rc = $out->syswrite( $chunk, length $chunk );
        last CHUNK if !defined $rc && $! == EAGAIN;
        $wrote += $rc;
        last CHUNK if $rc != length $chunk;
    }

    close $_ for $in, $out;

    return $wrote;
}

