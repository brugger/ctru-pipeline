package CTRU::Pipeline::Log;
# 
# JobManagementSystem framework for running pipelines everywhere!
# 
# 
# Kim Brugger (02 Oct 2013), contact: kim.brugger@addenbrookes.nhs.net
use strict;
use warnings;
use Data::Dumper;


my %l2i = ('debug' => 1,
	   'info'  => 2,
	   'warn'  => 3,
	   'error' => 4,
	   'fatal' => 5);


my %i2l = ( 1 => 'debug',
	    2 => 'info', 
	    3 => 'warn', 
	    4 => 'error',
	    5 => 'fatal');



my $reporting_level = $l2i{ 'warn' };


# 
# 
# 
# Kim Brugger (02 Oct 2013)
sub level {
  my ( $self, $new_level ) = @_;

  $new_level = lc( $new_level );

  if ( ! $l2i{ $new_level } ) {
    warn("Unknown reporting level, should be one of: debug, info, warn, error or fatal\n");
    return;
  }
  
  $reporting_level = $l2i{ $new_level };

  info("Changed reporting level to: $new_level\n");

}



# 
# 
# 
# Kim Brugger (02 Oct 2013)
sub reporting_level {
  print "This module is currently reporting above: '$reporting_level' ($i2l{$reporting_level})\n";
  
}



# 
# 
# 
# Kim Brugger (02 Oct 2013)
sub debug {
  my ($self, $message) = @_;

  return if ( $reporting_level > $l2i{ 'debug'});

  chomp( $message );

  print "DEBUG :: $message \n";
  return "DEBUG :: $message \n";
}


# 
# 
# 
# Kim Brugger (02 Oct 2013)
sub info {
  my ($self, $message) = @_;

  return if ( $reporting_level > $l2i{ 'debug'});
  chomp( $message );

  print "INFO :: $message \n";
}


# 
# 
# 
# Kim Brugger (02 Oct 2013)
sub warn {
  my ($self, $message) = @_;

  return if ( $reporting_level > $l2i{ 'warn'});
  chomp( $message );

  print "WARN :: $message \n";
}


# 
# 
# 
# Kim Brugger (02 Oct 2013)
sub error {
  my ($self, $message) = @_;

  return if ( $reporting_level > $l2i{ 'error'});
  chomp( $message );

  print STDERR "ERRORs :: $message \n";
}


# 
# 
# 
# Kim Brugger (02 Oct 2013)
sub fatal {
  my ($self, $message) = @_;

  return if ( $reporting_level > $l2i{ 'fatal'});
  chomp( $message );

  print STDERR "FATALs :: $message \n";
}



1;
