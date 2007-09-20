package Parallel::Workers;

use warnings;
use strict;
use Carp;
use Storable qw( store_fd fd_retrieve );
use IO::Handle;
use IO::Select;

our $VERSION = '0.1.0';
use base qw( Exporter );
our @EXPORT_OK = qw( iterate );

=head1 NAME

Parallel::Workers - [One line description of module's purpose here]

=head1 VERSION

This document describes Parallel::Workers version 0.1.0

=head1 SYNOPSIS

    use Parallel::Workers;
  
=head1 DESCRIPTION

=head1 INTERFACE 

=over

=item iterate( [ $options ], $worker, $iterator )

=cut

my %DEFAULTS = ( workers => 10, );

sub iterate {
    my %options = ( %DEFAULTS, %{ 'HASH' eq ref $_[0] ? shift : {} } );

    my $worker = shift;
    croak "Worker must be a coderef"
      unless 'CODE' eq ref $worker;

    my $iter = shift;
    croak "Iterator must be a coderef"
      unless 'CODE' eq ref $iter;

    croak "Must have at least one worker"
      if $options{workers} < 1;

    my @workers      = ();
    my @result_queue = ();
    my $rdr_sel      = IO::Select->new;
    my $wtr_sel      = IO::Select->new;

    # Possibly modify the iterator here...

    return sub {
        LOOP: {
            # Make new workers
            if ( @workers < $options{workers} && ( my @next = $iter->() ) ) {

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

                    # my $job_id = undef;
                    #
                    # my $yield = do {
                    #     # Install yield function
                    #     no strict 'refs';
                    #     my $caller = caller;
                    #     *{ $caller . '::yield' } = sub {
                    #         my $obj = shift;
                    #         _put_obj( [ $job_id, $obj ], $child_wtr );
                    #     };
                    # };

                    my $done = 0;

                    # Worker loop
                    while ( defined( my $parcel = _get_obj( $child_rdr ) ) ) {
                        my $result = $worker->( @$parcel );
                        # $done++;
                        # warn "# $$ done $done\n";
                        _put_obj( [ $parcel->[0], $result ], $child_wtr );
                        # $yield->( $worker->( @$parcel ) );
                    }

                    # End of stream
                    _put_obj( undef, $child_wtr );

                    close $_ for $child_rdr, $child_wtr;
                    exit;
                }
            }

            return @{ shift @result_queue } if @result_queue;

            if ( $rdr_sel->count || $wtr_sel->count ) {
                # Got a full set of workers - just need to wait for them
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

=back

=head1 DIAGNOSTICS

=over

=item C<< Error message here, perhaps with %s placeholders >>

[Description of error here]

=back

=head1 CONFIGURATION AND ENVIRONMENT
  
Parallel::Workers requires no configuration files or environment variables.

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
