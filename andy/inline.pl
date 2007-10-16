#!/usr/bin/perl
use Inline C;
greet( 'Ingy' );
greet( 42 );
__END__    
__C__
void greet(char* name) {
    printf("Hello %s!\n", name);
}
