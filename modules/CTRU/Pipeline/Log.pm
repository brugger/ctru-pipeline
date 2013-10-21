package CTRU::Pipeline::Log;
# 
# JobManagementSystem framework for running pipelines everywhere!
# 
# 
# Kim Brugger (02 Oct 2013), contact: kim.brugger@addenbrookes.nhs.net
use strict;
use warnings;
use Data::Dumper;
use Time::HiRes;
use POSIX 'strftime';


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

my $program;
my $rundir;
my $pid = $$;


BEGIN {

  $program = $0;
  $program =~ s/.*\///;
  $rundir      = `pwd`;
  chomp($rundir);

}

# 
# 
# 
# Kim Brugger (21 Oct 2013)
sub toString {
  my ( $input, $recursive ) = @_;

  $recursive ||= 0;
  return "" if ( ! $input );

  my $ref_type = ref $input;

  return $input if ( $ref_type eq "");

  if ( $ref_type eq "SCALAR" ) {
    return $$input;
  }
  elsif ( $ref_type eq "ARRAY") {
    return "[". join(", ", map { toString( $_ ,1) } @$input)."]" if ( $recursive );
    return  join(", ", map { toString( $_,1 ) } @$input);
  }
  elsif ( $ref_type eq "HASH") {
    return "{".join(", ", map { "$_:'". toString( $$input{$_},1 )."'" } sort keys %$input)."}" if ( $recursive );
    return join(", ", map { "$_:'". toString( $$input{$_},1 )."'" } sort keys %$input);
  }
  elsif ( $ref_type eq "REF") {
  }

  die "string, hash-, array or stringref expected";
}



# 
# 
# 
# Kim Brugger (21 Oct 2013)
sub meta_message {

  my $now = Time::HiRes::gettimeofday();
  my $timestamp = strftime('%d/%m/%y %H:%M:%S', localtime);

  return "$timestamp $program [$pid]";
  
}



# 
# 
# 
# Kim Brugger (02 Oct 2013)
sub level {
  my ( $self, $new_level ) = @_;

  $new_level = lc( $new_level );

  if ( ! $l2i{ $new_level } ) {
    warn($self,"Unknown reporting level, should be one of: debug, info, warn, error or fatal\n");
    return;
  }
  
  $reporting_level = $l2i{ $new_level };

  info($self, "Changed reporting level to: $new_level\n");

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

  $message = toString( $message );
  chomp( $message );

  print meta_message() . " DEBUG $message\n";
}


# 
# 
# 
# Kim Brugger (02 Oct 2013)
sub info {
  my ($self, $message) = @_;

  return if ( $reporting_level > $l2i{ 'info'});
  $message = toString( $message );
  chomp( $message );

  print meta_message() . " INFO $message\n";
}


# 
# 
# 
# Kim Brugger (02 Oct 2013)
sub warn {
  my ($self, $message) = @_;

  return if ( $reporting_level > $l2i{ 'warn'});
  $message = toString( $message );
  chomp( $message );

  print meta_message() . " WARN $message\n";
}


# 
# 
# 
# Kim Brugger (02 Oct 2013)
sub error {
  my ($self, $message) = @_;

  return if ( $reporting_level > $l2i{ 'error'});
  $message = toString( $message );
  chomp( $message );

  print STDERR meta_message() . " ERRORs $message\n";
}


# 
# 
# 
# Kim Brugger (02 Oct 2013)
sub fatal {
  my ($self, $message) = @_;

  return if ( $reporting_level > $l2i{ 'fatal'});
  $message = toString( $message );
  chomp( $message );

  print STDERR meta_message() . "FATALs $message\n";
}



1;
