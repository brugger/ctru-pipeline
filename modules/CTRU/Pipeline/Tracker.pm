package CTRU::Pipeline::Tracker;
# 
# Stores logging stats running state of jobs in a central database for easier tracking.
# 
# 
# Kim Brugger (02 Oct 2013), contact: kim.brugger@addenbrookes.nhs.net
use strict;
use warnings;


use EASIH::DB;

my $dbi;

# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub connect {
  my ($dbname, $dbhost, $db_user, $db_pass) = @_;
  $dbhost  ||= "mgsrv01";
  $db_user ||= 'easih_ro';

  $dbi = EASIH::DB::connect($dbname,$dbhost, $db_user, $db_pass);
  return $dbi;
}


# 
# 
# 
# Kim Brugger (29 May 2014)
sub connected {
  return 1 if ( defined $dbi);
  return 0;
}


# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub update_status {
  my ($name, $step, $status, $count) = @_;


  if ( ! $name || ! $step || ! $status || ! defined $count) { 
    print STDERR "update_status: missing variable: name: $name, step: $step, status:$status, count: $count \n";
    return -1;
  }

  
  my %call_hash = ( name => $name,
		    step => $step, 
		    status => $status, 
		    count => $count);

  return (EASIH::DB::replace($dbi, "status_tracking", \%call_hash));
}



1;
