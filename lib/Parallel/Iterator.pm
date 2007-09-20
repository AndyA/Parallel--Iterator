package Parallel::Iterator;

use warnings;
use strict;
use Carp;
use Storable qw( store_fd fd_retrieve );
use IO::Handle;
use IO::Select;
use Config;

require 5.008;

our $VERSION = '0.2.0';
use base qw( Exporter );
our @EXPORT_OK = qw( iterate iterate_as_array iterate_as_hash );

my %DEFAULTS = ( workers => ( $Config{d_fork} ? 10 : 0 ) );

=head1 NAME

Parallel::Iterator - Simple parallel execution

=head1 VERSION

This document describes Parallel::Iterator version 0.2.0

=head1 SYNOPSIS

    use Parallel::Iterator qw( iterate );

    # A very expensive way to double 100 numbers...
    
    my @nums = ( 1 .. 100 );
    
    my $iter = iterate( sub {
        my ( $id, $job ) = @_;
        return $job * 2;
    }, \@nums );
    
    my @out = ();
    while ( my ( $index, $value ) = $iter->() ) {
        $out[$index] = $value;
    }
  
=head1 DESCRIPTION

The C<map> function applies a user supplied transformation function to
each element in a list, returning a new list containing the
transformed elements.

This module provides a 'parallel map'. Multiple worker processes are
forked so that many instances of the transformation function may be
executed simultaneously. 

For time consuming operations, particularly operations that spend most
of their time waiting for I/O, this is a big performance win. It also
provides a simple idiom to make effective use of multi CPU systems.

There is, however, a considerable overhead associated with forking, so
the example in the synopsis (doubling a list of numbers) is I<not> a
sensible use of this module.

=head2 Example

Imagine you have an array of URLs to fetch:

    my @urls = qw(
        http://google.com/
        http://hexten.net/
        http://search.cpan.org/
        ... and lots more ...
    );

Write a function that retrieves a URL and returns its contents or undef
if it can't be fetched:

    sub fetch {
        my $url = shift;
        my $resp = $ua->get($url);
        return unless $resp->is_success;
        return $resp->content;
    };

Now write a function to synthesize a special kind of iterator:

    sub list_iter {
        my @ar = @_;
        my $pos = 0;
        return sub {
            return if $pos >= @ar;
            my @r = ( $pos, $ar[$pos] );  # Note: returns ( index, value )
            $pos++;
            return @r;
        };
    }

The returned iterator will return each element of the list and then
undef. Actually it returns both the index I<and> the value of each
element in the array. Because multiple instances of the transformation
function execute in parallel the results won't necessarily come back in
order. The array index will later allow us to put completed items in the
correct place in an output array.

Get an iterator for the list of URLs:

    my $url_iter = list_iter( @urls );

Then get another iterator which will return the transformed results:

    my $page_iter = iterate( \&fetch, $url_iter );

Finally loop over the returned iterator storing results:

    my @out = ( );
    while ( my ( $index, $value ) = $page_iter->() ) {
        $out[$index] = $value;
    }

Behind the scenes your program forked into ten (by default) instances of
itself and executed the page requests in parallel.

=head2 Simpler interfaces

Having to construct an iterator is a pain so C<iterate> is smart enough
to do that for you. Instead of passing an iterator just pass a reference
to the array:

    my $page_iter = iterate( \&fetch, \@urls );

If you pass a hash reference the iterator you get back will return key,
value pairs:

    my $some_iter = iterate( \&fetch, \%some_hash );

If the returned iterator is inconvenient you can get back a hash or
array instead:

    my @done = iterate_as_array( \&fetch, @urls );

    my %done = iterate_as_hash( \&worker, %jobs );

=head2 Caveats

Process forking is expensive. Only use Parallel::Iterator in cases where:

=over

=item the worker waits for I/O

The case of fetching web pages is a good example of this. Fetching a
page with LWP::UserAgent may take as long as a few seconds but probably
consumes only a few milliseconds of processor time. Running many
requests in parallel is a huge win - but be kind to the server you're
talking to: don't launch a lot of parallel requests unless it's your
server or you know it can handle the load.

=item the worker is CPU intensive and you have multiple cores / CPUs

If the worker is doing an expensive calculation you can parallelise that
across multiple CPU cores. Benchmark first though. There's a
considerable overhead associated with Parallel::Iterator; unless your
calculations are time consuming that overhead will dwarf whatever time
they take.

=back

=head3 END blocks

Because the current process forks any END blocks will be executed once
for each child. If it's important that an END block execute only in the
parent use something like this to guard against multiple execution:

    my $pid = $$;       # in parent
    END {
        if ( $$ == pid ) {
            # Do END stuff in parent only
        }
    }

=head2 How It Works

The current process is forked once for each worker. Each forked child is
connected to the parent by a pair of pipes. The child's STDIN, STDOUT
and STDERR are unaffected.

Input values are serialised (using Storable) and passed to the workers.
Completed work items are serialised and returned.

=head1 INTERFACE 

=head2 C<< iterate( [ $options ], $trans, $iterator ) >>

Get an iterator that applies the supplied transformation function to
each value returned by the input iterator.

Instead of an iterator you may pass an array or hash reference and
C<iterate> will convert it internally into a suitable iterator.

If you are doing this you may with to investigate C<iterate_as_hash> and
C<iterate_as_array>.

=head3 Options

A reference to a hash of options may be supplied. The following options
are supported:

=over

=item C<workers>

The number of concurrent processes to launch. Set this to 0 to disable
forking. Defaults to 10 on systems that support fork and 0 (disable
forking) on those that do not.

=back

=cut

sub iterate {
    my %options = ( %DEFAULTS, %{ 'HASH' eq ref $_[0] ? shift : {} } );

    my $worker = shift;
    croak "Worker must be a coderef"
      unless 'CODE' eq ref $worker;

    my $iter = shift;
    if ( 'ARRAY' eq ref $iter ) {
        my @ar  = @$iter;
        my $pos = 0;
        $iter = sub {
            return if $pos >= @ar;
            my @r = ( $pos, $ar[$pos] );
            $pos++;
            return @r;
        };
    }
    elsif ( 'HASH' eq ref $iter ) {
        my %h = %$iter;
        my @k = keys %h;
        $iter = sub {
            return unless @k;
            my $k = shift @k;
            return ( $k, $h{$k} );
        };
    }
    elsif ( 'CODE' eq ref $iter ) {
        # do nothing
    }
    else {
        croak "Iterator must be a code, array or hash ref";
    }

    if ( $options{workers} > 0 && $DEFAULTS{workers} == 0 ) {
        warn "Fork not available, falling back to single process mode\n";
        $options{workers} = 0;
    }

    if ( $options{workers} == 0 ) {
        # Non-forking version
        return sub {
            if ( my @next = $iter->() ) {
                return ( $next[0], $worker->( @next ) );
            }
            else {
                return;
            }
        };
    }
    else {

        # TODO: If we kept track of how many outstanding tasks each worker
        # had we could load balance more effectively.

        my @workers      = ();
        my @result_queue = ();
        my $rdr_sel      = IO::Select->new;
        my $wtr_sel      = IO::Select->new;

        # Possibly modify the iterator here...

        return sub {
            LOOP: {
                # Make new workers
                if ( @workers < $options{workers} && ( my @next = $iter->() ) )
                {

                    my ( $my_rdr, $my_wtr, $child_rdr, $child_wtr )
                      = map IO::Handle->new, 1 .. 4;

                    pipe $child_rdr, $my_wtr
                      or croak "Can't open write pipe ($!)\n";

                    pipe $my_rdr, $child_wtr
                      or croak "Can't open read pipe ($!)\n";

                    $rdr_sel->add( $my_rdr );
                    $wtr_sel->add( $my_wtr );

                    if ( my $pid = fork ) {
                        # Parent
                        close $_ for $child_rdr, $child_wtr;

                        push @workers, $pid;
                        _put_obj( \@next, $my_wtr );
                    }
                    else {
                        # Child
                        close $_ for $my_rdr, $my_wtr;

                        # Worker loop
                        while ( defined( my $parcel = _get_obj( $child_rdr ) ) )
                        {
                            my $result = $worker->( @$parcel );
                            _put_obj( [ $parcel->[0], $result ], $child_wtr );
                        }

                        # End of stream
                        _put_obj( undef, $child_wtr );

                        close $_ for $child_rdr, $child_wtr;
                        exit;
                    }
                }

                return @{ shift @result_queue } if @result_queue;

                if ( $rdr_sel->count || $wtr_sel->count ) {
                    my ( $rdr, $wtr, $exc )
                      = IO::Select->select( $rdr_sel, $wtr_sel, undef );

                    # Anybody got completed work?
                    for my $r ( @$rdr ) {
                        if ( defined( my $results = _get_obj( $r ) ) ) {
                            push @result_queue, $results;
                        }
                        else {
                            $rdr_sel->remove( $r );
                            close $r;
                        }
                    }

                    # Anybody waiting for work?
                    for my $w ( @$wtr ) {
                        if ( my @next = $iter->() ) {
                            _put_obj( \@next, $w );
                        }
                        else {
                            _put_obj( undef, $w );
                            $wtr_sel->remove( $w );
                            close $w;
                        }
                    }
                    redo LOOP;
                }

                waitpid( $_, 0 ) for @workers;
                return;
            }
        };
    }
}

=head2 C<< iterate_as_array >>

As C<iterate> but instead of returning an iterator returns an array
containing the collected output from the iterator. In a scalar context
returns a reference to the same array.

For this to work properly the input iterator must return (index, value)
pairs. This allows the results to be placed in the correct slots in the
output array. The simplest way to do this is to pass an array reference
as the input iterator:

    my @output = iterate_as_array( \&some_handler, \@input );

=cut

sub iterate_as_array {
    my $iter = iterate( @_ );
    my @out  = ();
    while ( my ( $index, $value ) = $iter->() ) {
        $out[$index] = $value;
    }
    return wantarray ? @out : \@out;
}

=head2 C<< iterate_as_hash >>

As C<iterate> but instead of returning an iterator returns a hash
containing the collected output from the iterator. In a scalar context
returns a reference to the same hash.

For this to work properly the input iterator must return (key, value)
pairs. This allows the results to be placed in the correct slots in the
output hash. The simplest way to do this is to pass an hash reference as
the input iterator:

    my %output = iterate_as_hash( \&some_handler, \%input );

=cut

sub iterate_as_hash {
    my $iter = iterate( @_ );
    my %out  = ();
    while ( my ( $key, $value ) = $iter->() ) {
        $out{$key} = $value;
    }
    return wantarray ? %out : \%out;
}

sub _get_obj {
    my $fd = shift;
    my $r  = fd_retrieve $fd;
    return $r->[0];
}

sub _put_obj {
    my ( $obj, $fd ) = @_;
    store_fd [$obj], $fd;
    $fd->flush;
}

1;
__END__

=head1 CONFIGURATION AND ENVIRONMENT
  
Parallel::Iterator requires no configuration files or environment variables.

=head1 DEPENDENCIES

None.

=head1 INCOMPATIBILITIES

None reported.

=head1 BUGS AND LIMITATIONS

No bugs have been reported.

Please report any bugs or feature requests to
C<bug-parallel-workers@rt.cpan.org>, or through the web interface at
L<http://rt.cpan.org>.

=head1 AUTHOR

Andy Armstrong  C<< <andy@hexten.net> >>

=head1 LICENCE AND COPYRIGHT

Copyright (c) 2007, Andy Armstrong C<< <andy@hexten.net> >>. All rights reserved.

This module is free software; you can redistribute it and/or
modify it under the same terms as Perl itself. See L<perlartistic>.

=head1 DISCLAIMER OF WARRANTY

BECAUSE THIS SOFTWARE IS LICENSED FREE OF CHARGE, THERE IS NO WARRANTY
FOR THE SOFTWARE, TO THE EXTENT PERMITTED BY APPLICABLE LAW. EXCEPT WHEN
OTHERWISE STATED IN WRITING THE COPYRIGHT HOLDERS AND/OR OTHER PARTIES
PROVIDE THE SOFTWARE "AS IS" WITHOUT WARRANTY OF ANY KIND, EITHER
EXPRESSED OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE
ENTIRE RISK AS TO THE QUALITY AND PERFORMANCE OF THE SOFTWARE IS WITH
YOU. SHOULD THE SOFTWARE PROVE DEFECTIVE, YOU ASSUME THE COST OF ALL
NECESSARY SERVICING, REPAIR, OR CORRECTION.

IN NO EVENT UNLESS REQUIRED BY APPLICABLE LAW OR AGREED TO IN WRITING
WILL ANY COPYRIGHT HOLDER, OR ANY OTHER PARTY WHO MAY MODIFY AND/OR
REDISTRIBUTE THE SOFTWARE AS PERMITTED BY THE ABOVE LICENCE, BE
LIABLE TO YOU FOR DAMAGES, INCLUDING ANY GENERAL, SPECIAL, INCIDENTAL,
OR CONSEQUENTIAL DAMAGES ARISING OUT OF THE USE OR INABILITY TO USE
THE SOFTWARE (INCLUDING BUT NOT LIMITED TO LOSS OF DATA OR DATA BEING
RENDERED INACCURATE OR LOSSES SUSTAINED BY YOU OR THIRD PARTIES OR A
FAILURE OF THE SOFTWARE TO OPERATE WITH ANY OTHER SOFTWARE), EVEN IF
SUCH HOLDER OR OTHER PARTY HAS BEEN ADVISED OF THE POSSIBILITY OF
SUCH DAMAGES.
