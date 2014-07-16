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
  my ($run_name, $thread_name, $step, $step_nr, $done, $running, $queuing, $failed, $unknown) = @_;

  if ( ! $run_name || ! $step) { 
    print STDERR "update_status: missing variable: name: $run_name, \n";
    return -1;
  }

  
  my %call_hash = ( run_name => $run_name,
		    thread_name => $thread_name,		    
		    step => $step, 
		    step_nr => $step_nr,
		    done => $done,
		    running => $running,
		    queuing => $queuing,
		    failed => $failed,
		    unknown => $unknown);

  return (EASIH::DB::replace($dbi, "status_tracking", \%call_hash));
}



# 
# 
# 
# Kim Brugger (30 May 2014)
sub fetch_running_steps {

  my $q = "select DISTINCT run_name, thread_name, step from status_tracking where running >= 1 OR queuing >= 1 order by run_name,thread_name,step";

  my $sth  = EASIH::DB::prepare($dbi, $q);
  return EASIH::DB::fetch_array_hash( $dbi, $sth);
}



# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub update_progress {
  my ($run_name, $thread_name, $steps_done, $steps_total) = @_;


  if ( ! $run_name || ! defined $steps_done || ! defined $steps_total) { 
    print STDERR "update_status: missing variable: name: $run_name, steps_done: $steps_done, steps_total: $steps_total \n";
    return -1;
  }

  
  my %call_hash = ( run_name => $run_name,
		    thread_name => $thread_name, 
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



# 
# 
# 
# Kim Brugger (16 Jul 2014)
sub fetch_run_name_status {
  my ($run_name) = @_;

  my $q = "select * from status_tracking where run_name = ? order by step_nr";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  return EASIH::DB::fetch_array_hash( $dbi, $sth, $run_name);
  
}


# 
# 
# 
# Kim Brugger (16 Jul 2014)
sub fetch_thread_name_status {
  my ($thread_name) = @_;

  my $q = "select * from status_tracking where thread_name = ? order by step_nr";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  return EASIH::DB::fetch_array_hash( $dbi, $sth, $thread_name);
  
}


1;
