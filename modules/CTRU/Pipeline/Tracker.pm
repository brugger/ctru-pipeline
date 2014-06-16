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



# 
# 
# 
# Kim Brugger (30 May 2014)
sub fetch_running_steps {

  my $q = "select DISTINCT name, step from status_tracking where count > 0 and (status = 'queuing' or status = 'running' or status ='unknow') order by name,step";

  my $sth  = EASIH::DB::prepare($dbi, $q);
  return EASIH::DB::fetch_array_hash( $dbi, $sth);
}



# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub update_progress {
  my ($name, $steps_done, $steps_total) = @_;


  if ( ! $name || ! defined $steps_done || ! defined $steps_total) { 
    print STDERR "update_status: missing variable: name: $name, steps_done: $steps_done, steps_total: $steps_total \n";
    return -1;
  }

  
  my %call_hash = ( name => $name,
		    steps_done => $steps_done, 
		    steps_total => $steps_total);

  return (EASIH::DB::replace($dbi, "progress_tracking", \%call_hash));
}



# 
# 
# 
# Kim Brugger (30 May 2014)
sub fetch_progresses {
  my ( $hour_limit ) = @_;

  my $q = "select * from progress_tracking";

  if ( $hour_limit ) {
    $q .= " WHERE time > ADDDATE(NOW(), INTERVAL - $hour_limit HOUR) ";
  }

  $q .= " order by time";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  return EASIH::DB::fetch_array_hash( $dbi, $sth);
}


1;
