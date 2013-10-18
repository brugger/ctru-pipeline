package CTRU::Log4Perl;
# 
# JobManagementSystem framework for running pipelines everywhere!
# 
# 
# Kim Brugger (02 Oct 2013), contact: kim.brugger@addenbrookes.nhs.net
use strict;
use warnings;
use Data::Dumper;
use Log::Log4perl qw(:easy);

our $logger;

BEGIN { 


# Initialize Logger
  my $log_conf = q(
   log4perl.rootLogger              = DEBUG, SCREEN
   log4perl.appender.SCREEN         = Log::Log4perl::Appender::Screen
   log4perl.appender.SCREEN.stderr  = 0
   log4perl.appender.SCREEN.layout  = Log::Log4perl::Layout::PatternLayout
   log4perl.appender.SCREEN.layout.ConversionPattern = %d %p %m %n
  );

#   log4perl.appender.LOG1.layout.ConversionPattern = %d %p %m %n

  Log::Log4perl::init(\$log_conf);
 
  $logger = Log::Log4perl->get_logger();
  $logger->level( $WARN );
}


my %l2i = ('debug' => 1,
	   'info'  => 2,
	   'warn'  => 3,
	   'error' => 4,
	   'fatal' => 5);


# 
# 
# 
# Kim Brugger (02 Oct 2013)
sub level {
  my ($self,  $new_level ) = @_;

  $new_level = lc( $new_level );

  if ( ! $l2i{ $new_level } ) {
    warn("Unknown reporting level, should be one of: debug, info, warn, error or fatal, not '$new_level'\n");
    return;
  }

  use Switch;

  switch( $new_level) {
    case 'debug' { $logger->level( $DEBUG ); }
    case 'info'  { $logger->level( $INFO );  }
    case 'warn'  { $logger->level( $WARN );  }
    case 'error' { $logger->level( $ERROR ); }
    case 'fatal' { $logger->level( $FATAL ); }
  }


}



# 
# 
# 
# Kim Brugger (02 Oct 2013)
sub debug {
  my ($self, $message) = @_;
  chomp( $message );
  $logger->debug($message);
}


# 
# 
# 
# Kim Brugger (02 Oct 2013)
sub info {
  my ($self, $message) = @_;
  chomp( $message );
  $logger->info($message);
}


# 
# 
# 
# Kim Brugger (02 Oct 2013)
sub warn {
  my ($self, $message) = @_;
  chomp( $message );
  $logger->warn($message);
}


# 
# 
# 
# Kim Brugger (02 Oct 2013)
sub error {
  my ($self, $message) = @_;
  chomp( $message );
  $logger->error($message);
}


# 
# 
# 
# Kim Brugger (02 Oct 2013)
sub fatal {
  my ($self, $message) = @_;
  chomp( $message );
  $logger->fatal($message);
}



1;
