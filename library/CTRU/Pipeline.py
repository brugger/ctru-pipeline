#!/usr/bin/python
# 
# 
# 
# 
# Kim Brugger (06 Jun 2017), contact: kim@brugger.dk

import sys
import re

import time
import os
import pprint as pp


import  CTRU.Pipeline.Backend as Backend

VERSION        = "1.1"

last_save      =   0
save_interval  = 300
max_retry      =   3
jobs_submitted =   0

sleep_time     =   30
max_sleep_time =  300
sleep_start    =  sleep_time
sleep_increase =  30


sleep_time = 10
max_sleep_time = 10
current_logic_name = None
pre_jms_ids    = None
use_storing    =   1 # debugging purposes
max_jobs       =  -1 # to control that we do not flood Darwin, or if local, block the machine. -1 is no limit
argv = None  # the argv from main is fetched at load time, and a copy kept here so we can store it later
freeze_file = None

no_restart     =   0 # failed jobs that cannot be restarted. 
thread_no_restart = None
restarted_run  =   0

# default dummy backend/logger that will fail gracefully, and the class that every other backend
# should inherit from.
backend           = "CTRU::Pipeline::Backend"
logger           = "CTRU::Pipeline::Log"


thread_name_hash = {}

project_name = "CPipe" # What shows up in qstats
queue_name   = ""
project_id   = ""



thread_name_hash[ 0 ] = project_name

start_time = None
end_time   = None
delete_files = []

jms_hash = {}


retained_jobs = []
analysis_order = {}

job_counter      = 1 # This is for generating internal jms_id (JobManamentSystem_Id)

thread_counter   = 0 # This is for generating internal thread_id 
active_thread_id = 0

cwd      = "./"


#my $username = scalar getpwuid $<;


dependencies = {}

FINISHED    =    1
FAILED      =    2
RUNNING     =    3
QUEUEING    =    4
RESUBMITTED =    5
SUBMITTED   =    6
KILLED      =   99
UNKNOWN     =  100

s2status = { 1   :  "Finished",
             2   :  "Failed",
             3   :  "Running",
             4   :  "Queueing",
             5   :  "Resubmitted",
             6   :  "Submitted",
             100 :  "Unknown"}


analysis = {}
flow     = {}

start_steps = []


# 
# 
# 
# Kim Brugger (03 Sep 2015)
def successful():

    global no_restart

    # All jobs finished as expected!
    if ( not no_restart):
        return 1 

    return 0


# 
# 
# 
# Kim Brugger (03 Sep 2015)
def max_jobs( jobs = -1):

    global max_jobs
    max_jobs = jobs



# 
# 
# 
# Kim Brugger (03 Sep 2015)
def set_queue( new_queue=''):

    global queue_name
    queue_name = new_queue;


# 
# 
# 
# Kim Brugger (14 Jun 2012)
def next_step( prename="", name="", function_name="", cluster_param=None):

    if ( function_name is None or function_name == "" ):
        function_name = name


    analysis[ name ] = { 'function' : function_name, 
                         'cparam'   : cluster_param }


    if ( prename not in flow ):
        flow[ prename ] = []

    flow[ prename ].append( name )



# 
# Old wrapper function...
# 
# Kim Brugger (03 Sep 2015)
def add_step( prename="", name="", function_name="", cluster_param=None):
    next_step(prename, name, function_name, cluster_param_);


    
# 
# 
# 
# Kim Brugger (03 Sep 2015)
def start_step(name="", function_name="", cluster_param = None):

    if ( function_name is None or function_name == "" ):
        function_name = name


    analysis[ name ] = { 'function' : function_name, 
                         'cparam'   : cluster_param }


    start_steps.append( name) 




# 
# Old wrapper...
# 
# Kim Brugger (03 Sep 2015)
def add_start_step(name="", function_name="", cluster_param = None):
    start_step(name, function_name, cluster_param);


# 
# 
# 
# Kim Brugger (14 Jun 2012)
def merge_step(pre_name="", name="", function_name="", cluster_param=None):


    if ( function_name is None or function_name == "" ):
        function_name = name


    analysis[ name ] = { 'function' : function_name, 
                         'cparam'   : cluster_param,
                         'sync'     : 1}

    if ( prename not in flow ):
        flow[ prename ] = []

    flow[ prename ].append( name )



# 
# 
# 
# Kim Brugger (03 Sep 2015)
def add_merge_step(pre_name="", name="", function_name="", cluster_param=""):
    merge_step(pre_name, name, function_name, cluster_param)
  



# 
# 
# 
# Kim Brugger (03 Sep 2015)
def thread_merge_step(pre_name="", name="", function_name="", cluster_param=None):


    if ( function_name is None or function_name == "" ):
        function_name = name


    analysis[ name ] = { 'function' : function_name, 
                         'cparam'   : cluster_param,
                         'thread_sync'      : 1}

    if ( prename not in flow ):
        flow[ prename ] = []

    flow[ prename ].append( name )




# 
# 
# 
# Kim Brugger (03 Sep 2015)
def set_project_name( new_name = None):

    global project_name

    if ( new_name is not None):
        project_name = new_name

    print "---------------------------PROJECT NAME " + project_name

    # I am not sure about this thing. Should have used a better
    # identifier for the project
    global thread_name_hash
    thread_name_hash[ 0 ] = new_name

    
# Crap naming right now, will get it right later on. Need to dist
# between a cluster project and a run-name project.
#
#
# Kim Brugger (03 Sep 2015)
def set_project( new_name=None ):
    global project_id

    if ( new_name is not None):
        project_id = new_name


# 
# 
# 
# Kim Brugger (03 Sep 2015)
def database_tracking(dbname = "", dbhost="", dbuser="", dbpass=""):
    print "datebase_tracking not implemented for Python..."

    global database_tracking 
    database_tracking = 1
    return 1


#  die "Need to provide both database name, host, user and password  to be able database_tracking\n" 
#      if ( ! $dbhost || ! $dbname || !$dbuser || ! $dbpass);
#  require CTRU::Pipeline::Tracker;
#  CTRU::Pipeline::Tracker::connect(@_);


# 
# disable the store function
# 
# Kim Brugger (03 Sep 2015)
def no_store():

    global use_storing
    use_storing = 0



# 
# Need to make a new logging system...
# 
# Kim Brugger (03 Sep 2015)
def fail( messsage=""):

    print "Error:: " + message
    store_state()

    exit()

# 
# 
# 
# Kim Brugger (03 Sep 2015)
def backend( backend = 'Local'):

#    try:
#        import "ctru.". backend
#    else:
#        print backend . " backend is not suported!!! "
#        exit()

    pass

# 
# 
# 
# Kim Brugger (23 Apr 2010)
def sleep_time( new_sleep_time = 30):
    global sleep_time
    sleep_time = new_sleep_time


# 
# -1 is never or only on crashes
# 
# Kim Brugger (03 Sep 2015)
def save_interval( new_save_interval = "30"):
    global save_interval 
    save_interval = new_save_interval



# 
# 
# 
# Kim Brugger (03 Sep 2015)
def version():
    global VERSION
    return VERSION



# 
# Checks and see if the current state of the run should be stored
# The inverval of this happening is set with save_interval
#
# Kim Brugger (04 May 2010)
def check_n_store_state():

    if ( save_interval == -1):
        return
  
    global last_save
    global save_interval

    now = time.time()
    if ( now - last_save > save_interval):
        store_state()



# 
# 
# 
# Kim Brugger (03 Sep 2015)
def max_retry( new_max_retry = 0):

    if ( new_max_retry < 0):
        new_max_retry = 0

    global max_retry
    max_retry = new_max_retry

# 
# Setting the working directory, if different than the cwd
# 
# Kim Brugger (23 Apr 2010)
def cwd( new_cwd = None):

  if ( new_cwd is not None ):
      cwd = new_cwd



# 
# 
# 
# Kim Brugger (03 Sep 2015)
def new_thread_id( name = ""):

    global project_id

    if ( not name ):
        name = project_id
  


    global active_thread_id 
    global thread_counter
    global thread_name_hash

    thread_counter += 1
    active_thread_id = thread_counter
    
    thread_name_hash[ active_thread_id ] = name
    

    return thread_counter

# 
# 
# 
# Kim Brugger (03 Sep 2015)
def get_thread_id():
    return active_thread_id


# 
# 
# 
# Kim Brugger (03 Sep 2015)
def submit_system_job( cmd, output, limit, delete_file=0):
    submit_job(cmd, output, limit, delete_file, 1)


# 
# submit a single job if $system is then a single system call is doing the work!
# 
# Kim Brugger (22 Apr 2010)
def submit_job(cmd=None, output=None, limit=None, delete_file=None, system=None):

#  print "  my ($cmd, $output, $limit, $delete_file, $thread_id, $system) = @_;\n";

#  print "submit_job  - ACTIVE THREAD_ID: $active_thread_id\n";

  if (  cmd is None ):
      print "no command provided";
      exit()

  if ( len( retained_jobs ) and max_jobs > 0 and max_jobs > jobs_sumitted):

       retained_jobs.append( [ cmd, output, limit, system, current_logic_name])

       params = retained_jobs.pop()
       
#    $logger->debug("Queued/unqueued a job ( ". @retained_jobs . " jobs retained)\n");
       (cmd, output, current_logic_name)= list(params)
#    $logger->debug(" PARAMS :::     ($cmd, $output, $current_logic_name) \n");

  elif (max_jobs > 0 and max_jobs <= jobs_submitted ):
      
      retained_jobs.append( [ cmd, output, limit, system, current_logic_name])
      return


  global job_counter
  job_counter += 1

  jms_id = job_counter

  instance = {     'status'      : SUBMITTED,
		   'tracking'    : 1,
		   'command'     : cmd,
		   'output'      : output,
		   'limit'       : limit,
		   'logic_name'  : current_logic_name,
		   'pre_jms_ids' : pre_jms_ids,
		   'delete_file' : delete_file,
		   'thread_id'   : active_thread_id }


  if ( system is not None and system ):

#    eval { system "$cmd" };

    instance[ job_id ] = -1;

  #   if ( ! $@ ) {
  #     $$instance{ status } = $FINISHED;
  #   }
  #   else {
  #     $logger->debug("$@\n");
  #     $$instance{ status } = $FAILED;
  #   }
  # }
  else:

#    job_id = backend->submit_job( "cd $cwd;$cmd", limit);
    
    instance[ job_id ] = job_id;
 

  jms_hash[ jms_id ]  = instance
  jobs_submitted += 1  

#  $logger->debug( $jms_hash{ $jms_id } );

  # $logger->info( { 'type'       => "runtime_stats",
  #       	   'logic_name' => $jms_hash{ $jms_id }{ 'logic_name'}, 
  #       	   'job_id'     => $jms_hash{ $jms_id }{ 'job_id'    }, 
  #       	   "status"     => "STARTED",
  #       	   "command"    => $jms_hash{ $jms_id }{ 'command'   },
  #       	   "output"     => $jms_hash{ $jms_id }{ 'output'    },
  #       	   "limit"      => $jms_hash{ $jms_id }{ 'limit'     },
  #       	   "thread_id"  => $active_thread_id,
  #       	 });


  for pre_jms_id in list( pre_jms_ids ) :
      if pre_jms_id is not None:
          jms_hash[ pre_jms_id ][ post_jms_id ].append( jms_id );




# 
# 
# 
# Kim Brugger (26 Apr 2010)
def resubmit_job( jms_id ):

    

  instance   = jms_hash[ jms_id     ]
  logic_name = instance[ logic_name ];

  job_id = backend.submit_job( instance[ command ], instance[ limit ]);
  
  instance[ job_id   ]   = job_id
  instance[ status   ]   = RESUBMITTED;
  instance[ tracking ]   = 1
  jobs_submitted += 1      




# 
# 
# 
# Kim Brugger (24 Jun 2010)
def killall():

  for jms_id  in  fetch_jms_ids():
      status     = jms_hash[ jms_id ][ 'status' ];
    
      if ( status != FINISHED ):
          backend.kill(  jms_hash[ jms_id ][ job_id ])


# 
# 
# 
# Kim Brugger (05 Jul 2010)
def format_memory( memory = None) :

  if ( memory is None or memory == ""):
      return "N/A"
  else:
      memory = int( memory )

      if ( memory > 1000000000 ):
          return "%.2fGB" % ( memory / 1000000000.0);
      elif (memory  > 1000000 ):
          return "%.2fMB" % ( memory / 1000000.0 );
      elif (memory  > 1000 ):
          return "%.2fKB" % (memory / 1000.0);
  
  return memory



# 
# 
# 
# Kim Brugger (05 Jul 2010)
# def format_time( runtime):
#   my ( $runtime ) = @_;
  
#   return "N/A" if ( ! defined $runtime);

#   my $res;
#   my ($hour, $min, $sec) = (0,0,0);
#   $hour = int( $runtime / 3600);
#   $runtime -= 3600*$hour; 
#   $min = int( $runtime / 60);
#   $runtime -= 60*$min;
#   $sec = int( $runtime );
#   return sprintf("%02d:%02d:%02d", $hour, $min, $sec);
# }



# 
# 
# 
# Kim Brugger (24 Sep 2010)
sub freeze_file {
  my ($new_freeze_file) = @_;

  $freeze_file = $new_freeze_file if ( $new_freeze_file );

  # If already set return the name
  return $freeze_file if ( $freeze_file );
  
  # otherwise as default we revert to program_name.pid
  my $filename = $0;
  $filename =~ s/.*\///;

  $freeze_file = "$filename.$$";
  return $freeze_file;
}


# 
# 
# 
# Kim Brugger (05 Jul 2010)
sub get_timestamp {
  
  use POSIX 'strftime';
  my $time = strftime('%d/%m/%y %H.%M', localtime);


  return "[$time \@$host ".(freeze_file())."]\n" . "-"x30 . "\n";

}



# 
# 
# 
# Kim Brugger (13 Sep 2010)
sub fetch_jms_ids {
  my $active_only = shift || 0;

  my @jms_ids = sort {$a <=> $b } keys %jms_hash;

  if ( $active_only ) {
    my @active_jobs;
    foreach my $jms_id ( @jms_ids ) {
      push @active_jobs, $jms_id if ( $jms_hash{ $jms_id }{ tracking });
    }
    return @active_jobs;
  }

  return @jms_ids;
}


# 
# 
# 
# Kim Brugger (18 May 2010)
sub fetch_active_jms_ids {
  return fetch_jms_ids(1);
}


my $spin_count = 0;

# 
# 
# 
# Kim Brugger (27 Nov 2012)
sub spinner {
  
  

  my @spin = ('|', '/', '-','\\','|', '/','-','\\');
  my @spin2 = (">))'>",
	       "    >))'>",
	       "        >))'>",
	       "            >))'>",
	       "            <'((<",
	       "        <'((<",
	       "    <'((<",
	       "<'((<");
	      

  my @use_spin = @spin;
  @use_spin = @spin2;

  $spin_count++;
  return $use_spin[ int($spin_count % int( @use_spin))];
  
}



# 
# 
# 
# Kim Brugger (24 Jun 2010)
sub report_spinner {

  my %res = ();

  foreach my $jms_id ( fetch_jms_ids() ) {
    my $logic_name = $jms_hash{ $jms_id }{ logic_name};
    my $status     = $jms_hash{ $jms_id }{ status }; 
    $res{ $logic_name }{ $status }++;
    $res{ $logic_name }{ failed } += ($jms_hash{ $jms_id }{ failed } || 0);

    my $job_id     = $jms_hash{ $jms_id }{ job_id }; 
   
    if ( $job_id != -1 ) {
      my $memory = $backend->job_memory( $job_id ) || 0;
      $res{ $logic_name }{ memory } = $memory if ( !$res{ $logic_name }{ memory } || $res{ $logic_name }{ memory } < $memory);
      $res{ $logic_name }{ runtime } += $backend->job_runtime( $job_id ) || 0;
    }
  }

  return if ( keys %res == 0);

  my $report = get_timestamp(). "Run statistics:   ||  Runtime   ||  MaxMemory ||  D  R  Q  F  U\n";

  my @logic_names = sort {$analysis_order{ $a } <=> $analysis_order{ $b } } keys %res;
  
  for (my $i = 0; $i < @logic_names; $i++) {
    my $logic_name = $logic_names[$i];
    my $queue_stats;

    $queue_stats .= sprintf("%02d/%02d/",($res{ $logic_name }{ $FINISHED } || 0),($res{ $logic_name }{ $RUNNING  } || 0));
    my $sub_other = ($res{ $logic_name }{ $QUEUEING  } || 0);
    $sub_other += ($res{ $logic_name }{ $RESUBMITTED  } || 0);
    $sub_other += ($res{ $logic_name }{ $SUBMITTED  } || 0);
    $queue_stats .= sprintf("%02d/%02d/%02d",$sub_other, ($res{ $logic_name }{ failed  } || 0),($res{ $logic_name }{ $UNKNOWN  } || 0));

    my $spinner = "";
    if ( $i == @logic_names - 1) {
      $spinner = spinner();
    }

    $report .= sprintf("%-17s ||  %8s  || %10s || $queue_stats\t$spinner\n", $logic_name,
		       format_time($res{ $logic_name }{ runtime }), format_memory($res{ $logic_name }{ memory }));
  }

  return $report;
}



# 
# 
# 
# Kim Brugger (24 Jun 2010)
sub report2tracker {


  print "Project-name $project_name :: " . Dumper( \%thread_name_hash ) ;

    my %res = ();

    foreach my $jms_id ( fetch_jms_ids() ) {
      my $logic_name   = $jms_hash{ $jms_id }{ logic_name};
      my $status       = $jms_hash{ $jms_id }{ status }; 
      my $thread_id    = $jms_hash{ $jms_id }{ thread_id }; 
      $res{ $thread_id }{ $logic_name }{ $status }++;
      $res{ $thread_id }{ $logic_name }{ failed } += ($jms_hash{ $jms_id }{ failed } || 0 );

      my $job_id     = $jms_hash{ $jms_id }{ job_id }; 
      
      if ( $job_id != -1 ) {
	my $memory = $backend->job_memory( $job_id ) || 0;
	$res{ $thread_id }{ $logic_name }{ memory } = $memory if ( !$res{ $logic_name }{ memory } || $res{ $logic_name }{ memory } < $memory);
	$res{ $thread_id }{ $logic_name }{ runtime } += $backend->job_runtime( $job_id ) || 0;
      }
      
    }
    
    return if ( keys %res == 0);

    print Dumper( \%res );
    
#  die Dumper( \%analysis_order );

    my $total_steps = int(keys %analysis_order) - 1;
    
    foreach my $thread_id ( keys %res ) {
      my $finished_steps = 0;
      foreach my $logic_name ( keys %{$res{ $thread_id }} ) {

	my $sub_other = ($res{ $thread_id }{ $logic_name }{ $QUEUEING  } || 0);
	$sub_other += ($res{ $thread_id }{ $logic_name }{ $RESUBMITTED  } || 0);
	$sub_other += ($res{ $thread_id }{ $logic_name }{ $SUBMITTED  } || 0);
	
	$finished_steps++
	    if ( ! $res{ $thread_id }{ $logic_name }{ $RUNNING } &&
		 ! $res{ $thread_id }{ $logic_name }{ $UNKNOWN } &&
		 ! $sub_other);
	


	CTRU::Pipeline::Tracker::update_status( $project_name, $thread_name_hash{ $thread_id }, $logic_name, $analysis_order{ $logic_name },
						$res{ $thread_id }{ $logic_name }{ runtime   } || "NA",
						$res{ $thread_id }{ $logic_name }{ memory    } || "N/A",
						$res{ $thread_id }{ $logic_name }{ $FINISHED } || 0,
						$res{ $thread_id }{ $logic_name }{ $RUNNING }  || 0,
						$sub_other                                        || 0, # queuing
						$res{ $thread_id }{ $logic_name }{ $FAILED }   || 0,
						$res{ $thread_id }{ $logic_name }{ $UNKNOWN }  || 0); 

      }
      CTRU::Pipeline::Tracker::update_progress($project_name, $thread_name_hash{ $thread_id }, $finished_steps, $total_steps);
    }
      

      
#    printf("$project_name ----------->>> %.2f %% done\n", $finished_steps*100/$total_steps);

#    print "$finished_steps --> " . int(keys %analysis_order) . "\n";

    


  

}

# 
# 
# 
# Kim Brugger (24 Jun 2010)
sub report {

  my %res = ();

  foreach my $jms_id ( fetch_jms_ids() ) {
    my $logic_name = $jms_hash{ $jms_id }{ logic_name};
    my $status     = $jms_hash{ $jms_id }{ status }; 
    $res{ $logic_name }{ $status }++;
    $res{ $logic_name }{ failed } += ($jms_hash{ $jms_id }{ failed } || 0);

    my $job_id     = $jms_hash{ $jms_id }{ job_id }; 
   
    if ( $job_id != -1 ) {
      my $memory = $backend->job_memory( $job_id ) || 0;
      $res{ $logic_name }{ memory } = $memory if ( !$res{ $logic_name }{ memory } || $res{ $logic_name }{ memory } < $memory);
      $res{ $logic_name }{ runtime } += $backend->job_runtime( $job_id ) || 0;
    }
  }

  return if ( keys %res == 0);

  my $report = get_timestamp(). "Run statistics:   ||  Runtime   ||  MaxMemory ||  D  R  Q  F  U\n";

  foreach my $logic_name ( sort {$analysis_order{ $a } <=> $analysis_order{ $b } } keys %res ) {
    my $queue_stats;

    $queue_stats .= sprintf("%02d/%02d/",($res{ $logic_name }{ $FINISHED } || 0),($res{ $logic_name }{ $RUNNING  } || 0));
    my $sub_other = ($res{ $logic_name }{ $QUEUEING  } || 0);
    $sub_other += ($res{ $logic_name }{ $RESUBMITTED  } || 0);
    $sub_other += ($res{ $logic_name }{ $SUBMITTED  } || 0);
    $queue_stats .= sprintf("%02d/%02d/%02d",$sub_other, ($res{ $logic_name }{ failed  } || 0),($res{ $logic_name }{ $UNKNOWN  } || 0));

    $report .= sprintf("%-17s ||  %8s  || %10s || $queue_stats\n", $logic_name,
		       format_time($res{ $logic_name }{ runtime }), format_memory($res{ $logic_name }{ memory }));

  }

  return $report;
}





# 
# 
# 
# Kim Brugger (24 Jun 2010)
sub total_runtime {

  my $runtime = 0;
  
  foreach my $jms_id ( fetch_jms_ids() ) {
    my $job_id     = $jms_hash{ $jms_id }{ job_id }; 
   
    next if ( $job_id == -1 || !$job_id );

    $runtime += int($backend->job_runtime( $job_id )) if ($backend->job_runtime( $job_id ));
  }

  return sprintf("Total runtime: %8s\n", format_time( $runtime ));
}



# 
# 
# 
# Kim Brugger (04 Aug 2010)
sub real_runtime {
  return "" if ( ! $end_time || ! $start_time);
  return sprintf("Real runtime: %8s\n", format_time( $end_time - $start_time ));
}


# 
# 
# 
# Kim Brugger (24 Jun 2010)
sub full_report {

  my $report = get_timestamp();

  my %printed_logic_name = ();

  foreach my $jms_id ( sort { $analysis_order{ $jms_hash{ $a }{logic_name}} <=> $analysis_order{ $jms_hash{ $b }{logic_name}} } fetch_jms_ids() ) {   
    my $logic_name = $jms_hash{ $jms_id }{ logic_name};

    if ( ! $printed_logic_name{ $logic_name } ) {
      $report .= "\n| $logic_name\n";
      $report .=  "-="x10 . "-\n";
      $printed_logic_name{ $logic_name }++;
    }

    my $status     = $jms_hash{ $jms_id }{ status }; 
    my $job_id     = $jms_hash{ $jms_id }{ job_id }; 

    my %status2name = ( $FINISHED    => "Finished",
			$FAILED      => "Failed",
			$RUNNING     => "Running",
			$QUEUEING    => "Queueing",
			$RESUBMITTED => "Re-submitted",
			$SUBMITTED   => "Submitted",
			$KILLED      => "Killed",
			$UNKNOWN     => "Unknown" );
    
    $report .= sprintf("%3d/%-5d\t%12s\tfailures: %d\n", $jms_id, $job_id, $status2name{ $status }, $jms_hash{ $jms_id }{ failed } || 0);

    if ( $job_id != -1 ) {
      $report .= sprintf("Runtime: %s || Memory: %s\n", format_time($backend->job_runtime( $job_id )), format_memory($backend->job_memory( $job_id )));
    }
    if ( $jms_hash{ $jms_id }{ output } && ref ($jms_hash{ $jms_id }{ output }) eq 'ARRAY') {
      $report .= sprintf("cmd/output: %s --> %s\n", $jms_hash{ $jms_id }{ command }, join(",", @{$jms_hash{ $jms_id }{ output }}));
    }
    elsif ( $jms_hash{ $jms_id }{ output } && ref ($jms_hash{ $jms_id }{ output }) eq 'HASH') {
      $report .= sprintf("cmd/output: %s --> %s\n", $jms_hash{ $jms_id }{ command }, join(',',map { "$_=> $jms_hash{ $jms_id }{ output }{$_}"} keys %{$jms_hash{ $jms_id }{ output }}));
    }
    else {
      $report .= sprintf("cmd/output: %s --> %s\n", $jms_hash{ $jms_id }{ command }, ($jms_hash{ $jms_id }{ output } || ""));
    }
  }

  return $report;
}


# 
# 
# 
# Kim Brugger (05 Jul 2010)
sub mail_report {
  my ( $to, $subject, $extra) = @_;

  $subject = "$subject (Error)" if ( $no_restart );

  open(my $mail, " | mail -s '[easih-pipeline] $subject'  $to") || die "Could not open mail-pipe: $!\n";

  if ( $no_restart ) {
    print $mail "ERROR :: The pipeline was unsucessful with $no_restart jobs not being able to finish\n";
    print $mail "ERROR :: This might have interrupted the pipeline flow as well\n\n";
  }

  print $mail report() . "\n\n";
  print $mail total_runtime();
  print $mail real_runtime();

  print $mail "Running directory: $cwd, Freeze file: ".(freeze_file())."\n";
  print $mail "easih-pipeline version: " . version() . "\n";

  print $mail $extra. "\n\n";
  
  print $mail full_report(). "\n\n";
  close( $mail );


  mail_report('kb468@cam.ac.uk', $subject, $extra) if ( $to ne 'kb468@cam.ac.uk');


}



# 
# Wait for the jobs to terminate
# 
# Kim Brugger (22 Apr 2010)
sub check_jobs {

  foreach my $jms_id ( fetch_active_jms_ids ) {


    if ( ! defined $jms_hash{ $jms_id }{ job_id } ) {
      die "'$jms_id' ==> " . Dumper( $jms_hash{ $jms_id }) . "\n";
    }
 
    my $status;
    if ( $jms_hash{ $jms_id }{ job_id } == -1 ) {
      $status = $jms_hash{ $jms_id }{ status };
    }
    else {	
      $status = $backend->job_status( $jms_hash{ $jms_id}{ job_id } );
      $jms_hash{ $jms_id }{ status } = $status;
    }

    # this should be done with switch, but as we are not on perl 5.10+ this is how it is done...
    if ($status ==  $FINISHED  ) {
      $jobs_submitted--;

      $logger->debug( $jms_hash{ $jms_id });

      $logger->info( { 'type'       => "runtime_stats",
		       'logic_name' => $jms_hash{ $jms_id }{'logic_name'}, 
		       'job_id'     => $jms_hash{ $jms_id }{ 'job_id' }, 
		       "runtime"    => $backend->job_runtime( $jms_hash{ $jms_id }{ 'job_id' } ), 
		       "memory"     => $backend->job_memory( $jms_hash{ $jms_id }{ 'job_id' } ),
		       "status"     => "FINISHED",
		       "command"    => $jms_hash{ $jms_id }{ 'command' },
		       "output"     => $jms_hash{ $jms_id }{ 'output' },
		       "limit"      => $jms_hash{ $jms_id }{ 'limit' },
		     });

      # Job finished successfully, so delete any files tagged for deletion
      if ( $jms_hash{ $jms_id }{ 'delete_file' } ) {
	print "CLEAN UP :::: rm -f $jms_hash{ $jms_id }{ 'delete_file' } \n";
	system "rm -f $jms_hash{ $jms_id }{ 'delete_file' }";
      }

    }
    elsif ($status == $FAILED ) {
      $jobs_submitted--;
      $jms_hash{ $jms_id }{ failed }++;

      $logger->debug( $jms_hash{ $jms_id });
      $logger->info( { 'type'       => "runtime_stats",
		       'logic_name' => $jms_hash{ $jms_id }{'logic_name'}, 
		       'job_id'     => $jms_hash{ $jms_id }{ 'job_id' }, 
		       "runtime"    => $backend->job_runtime( $jms_hash{ $jms_id }{ 'job_id' } ), 
		       "memory"     => $backend->job_memory( $jms_hash{ $jms_id }{ 'job_id' } ),
		       "status"     => "FAILED"},
		       "command"    => $jms_hash{ $jms_id }{ 'command' },
		       "output"     => $jms_hash{ $jms_id }{ 'output' },
		       "limit"      => $jms_hash{ $jms_id }{ 'limit' },);

      if ( $jms_hash{ $jms_id }{ failed } < $max_retry ) {
	$logger->warn("Failed, resubmitting job\n");
	resubmit_job( $jms_id );
      }
      else { 
	$logger->warn("Cannot resubmit job ($jms_hash{ $jms_id }{ failed } < $max_retry)\n");
	$thread_no_restart{ $active_thread_id }++;
	$no_restart++;
      }
    }
    
  }

  return;
}



# 
# Hard ! resets the pipeline. If a analysis failed it is deleted and
# pushed back to the previous step in the pipeline. If that job
# resulted in multiple child processes then they and all their spawn
# is deleted.
#
# Did I mention this was a HARD reset?
#
# Kim Brugger (26 Apr 2010)
sub hard_reset {
  my ( $freezefile ) = @_;

  if ( ! $freezefile || ! -e $freezefile) {
    $logger->fail( "Cannot do a hard-reset without a freezefile\n");
    exit 1;
  }

  restore_state( $freezefile);

  # Update job statuses...
  check_jobs();

  # Only look at the jobs we are currently tracking
  foreach my $jms_id ( fetch_jms_ids() ) {

    next if (! $jms_hash{ $jms_id });
    next if ($jms_hash{ $jms_id }{ post_jms_id });
    # the analysis depends on a previous analysis, and can be rerun

    if ( $jms_hash{ $jms_id }{ status } == $FAILED  ||  
	 $jms_hash{ $jms_id }{ status } == $UNKNOWN ||  
	 $jms_hash{ $jms_id }{ status } == $KILLED) {

      my $pre_jms_ids = $jms_hash{ $jms_id }{ pre_jms_ids };
      
      foreach my $pre_jms_id ( @$pre_jms_ids ) {

	if ($pre_jms_id  && @{$jms_hash{ $pre_jms_id }{ post_jms_id }} > 1 ){
	  my @children = @{$jms_hash{ $pre_jms_id }{ post_jms_id }};
	  
	  while (my $child = shift @children ) {
	    push @children, @{$jms_hash{ $child }{ post_jms_id }} if ($jms_hash{ $child }{ post_jms_id });
	    delete $jms_hash{ $child };
	  }
	  
	}
	$logger->debug( "Resubmitted $pre_jms_id after hard reset downstream (due to $jms_id)\n");
	resubmit_job( $pre_jms_id );
      }
    }
    elsif (! $jms_hash{ $jms_id }{ post_jms_id }) {
      $logger->debug("Tracking $jms_id\n");
      $jms_hash{ $jms_id }{ tracking } = 1;
      next;
    }

  }

  $restarted_run = 1;
}

# soft-reset... 
# reset the failed states, so the pipeline can run again
# 
# Kim Brugger (26 Apr 2010)
sub reset {
  my ( $freezefile ) = @_;

  if ( ! $freezefile || ! -e $freezefile) {
    $logger->fail("Cannot do a reset without a freezefile\n");
    exit 1;
  }

  restore_state( $freezefile);

  # Update job statuses...
  check_jobs();

  # Only look at the jobs we are currently tracking
  foreach my $jms_id ( fetch_jms_ids() ) {

    next if ($jms_hash{ $jms_id }{ post_jms_id });
    # the analysis depends on a previous analysis, and can be rerun

    if ( $jms_hash{ $jms_id }{ status } == $FAILED  ||  
	 $jms_hash{ $jms_id }{ status } == $UNKNOWN || 
	 $jms_hash{ $jms_id }{ status } == $KILLED ) {

      $logger->debug("Resubmitted $jms_id\n");
      resubmit_job( $jms_id );
    }
    elsif (! $jms_hash{ $jms_id }{ post_jms_id }) {
      $logger->debug("Tracking $jms_id\n");
      #$jms_hash{ $jms_id }{ tracking } = 1;
      next;
    }
  }

  $restarted_run = 1;
}



# 
# 
# 
# Kim Brugger (22 Apr 2010)
sub tmp_file {
  my ($postfix, $keep_file) = @_;
  $postfix ||= "";
  $keep_file || 0;
  
  system "mkdir tmp" if ( ! -d './tmp');

  my ($tmp_fh, $tmp_file) = File::Temp::tempfile(DIR => "./tmp" );
  close ($tmp_fh);
  system "rm -f $tmp_file";

  push @delete_files, "$tmp_file$postfix" if (! $keep_file);

  return "$tmp_file$postfix";
}



# 
# 
# 
# Kim Brugger (17 May 2010)
sub next_analysis {
  my ( $logic_name ) = @_;
  
  my @res;

  my $next = $flow{ $logic_name} || undef;
  if ( ref ( $next ) eq "ARRAY" ) {
    @res = @$next;
  }
  elsif ( defined $next ) {
    push @res, $next;
  }
  
  return @res;
}




# 
# 
# 
# Kim Brugger (22 Apr 2010)
sub delete_tmp_files {

  system "rm -f @delete_files";
}





# 
# 
# 
# Kim Brugger (18 May 2010)
sub fetch_jobs {
  my ( @logic_names ) = @_;

  my @jobs;
  foreach my $jms_id ( fetch_jms_ids() ) {    
    push @jobs, $jms_id 
	if ( grep(/$jms_hash{ $jms_id }{ logic_name }/, @logic_names) && 
	     $active_thread_id == $jms_hash { $jms_id }{ thread_id });
  }

  return @jobs;
}


# 
# Set the pre analysis dependencies for each analysis.
# 
# Kim Brugger (05 Jul 2010)
sub set_analysis_dependencies {
  my ( $logic_name ) = @_;


  my @logic_names = next_analysis( $logic_name );

  foreach my $next_logic_name ( @logic_names) {
    push @{$dependencies{ $next_logic_name }}, $logic_name;
  }

  while ( $logic_name = shift @logic_names  ) {

    my @next_logic_names = next_analysis( $logic_name );
    next if ( ! @next_logic_names );
    push @logic_names, @next_logic_names if (@next_logic_names);
    foreach my $next_logic_name ( @next_logic_names) {
      push @{$dependencies{ $next_logic_name }}, $logic_name;
      push @{$dependencies{ $next_logic_name }}, @{$dependencies{ $logic_name }} if ($dependencies{ $logic_name });
    
    # make sure a logic_name only occurs once.
      my %saw;
      @{$dependencies{ $next_logic_name }} = grep(!$saw{$_}++, @{$dependencies{ $next_logic_name }});
    }
  }

}


# 
# Traverse the flow hash and stores the analysis order
# 
# Kim Brugger (09 Sep 2010)
sub set_analysis_order {
  my ( $logic_name ) = @_;
  
  $analysis_order{ $logic_name } = 1;
  my @logic_names = ( $logic_name );

  while ( $logic_name = shift @logic_names  ) {

    my @next_logic_names = next_analysis( $logic_name );
    

    foreach my $next_logic_name ( @next_logic_names ) {
      
      $analysis_order{ $next_logic_name } = $analysis_order{ $logic_name } + 1 
	  if (! $analysis_order{ $next_logic_name } || 
	      $analysis_order{ $next_logic_name } <= $analysis_order{ $logic_name } + 1);
    }
    push @logic_names, next_analysis( $logic_name );
  }


#  foreach my $key ( sort {$analysis_order{$a} <=> $analysis_order{$b}} keys %analysis_order ) {
#    printf("%03d --> $key\n", $analysis_order{ $key });
#  }
}




# 
# for print_flow, so we can fake dependencies...
# 
# Kim Brugger (05 Jul 2010)
sub waiting_for_analysis {
  my ($logic_name, @done_analyses) = @_;

  return 0 if ( ! $dependencies{ $logic_name });

  my %done;
  map { $done{ $_ }++ } @done_analyses;
  foreach my $dependency ( @{$dependencies{ $logic_name }} ) {
    if ( ! $done{ $dependency} ) {
      $logger->debug("$logic_name is waiting for $dependency\n");
      return 1;
    }
  }
  

  return 0;
}


# 
# 
# 
# Kim Brugger (05 Jul 2010)
sub depends_on_active_jobs {
  my ($logic_name) = @_;

  return 0 if ( ! $dependencies{ $logic_name });

  my %dependency;
  map { $dependency{ $_ }++ } @{$dependencies{ $logic_name }};
  
  foreach my $jms_id ( fetch_jms_ids() ) {
    next if (! $jms_hash{ $jms_id }{ tracking });
    
#    print "$jms_id --> $dependency{ $jms_hash{ $jms_id }{ logic_name }}\n";


    if ( $dependency{ $jms_hash{ $jms_id }{ logic_name }}) {
      return 1 if ( $active_thread_id && $jms_hash{ $jms_id }{ thread_id } == $active_thread_id);
      return 1 if ( $active_thread_id == 0); # Main thread id
    }
  }
  

  return 0;
}





# 
# Main loop that does all the work.
# 
# Kim Brugger (18 May 2010)
sub run {
  my (@start_logic_names) = @_;

  # $logger->info( { 'type'     => "pipeline_stats",
  # 		   'program'  => $0,
  # 		   'pid'      => $$,
  # 		   'cwd'      => $cwd,
  # 		   'status'   => "STARTED"});


  @start_logic_names = @start_steps if ( ! @start_logic_names );

  # Just to make sure that the script is setup as it should be
  # the overhead of doing this is close to null, and it saves 
  # time if things does not crash.
  validate_flow(@start_logic_names);


  $start_time = Time::HiRes::gettimeofday();

  foreach my $start_logic_name ( @start_logic_names ) {
    set_analysis_dependencies( $start_logic_name );
    set_analysis_order( $start_logic_name );
  }

  while (1) {

#    $active_thread_id = 0;
#    $project_name = $thread_name_hash{ $active_thread_id };

    my ($started, $queued, $running ) = (0,0,0);

    my @active_jobs = fetch_active_jms_ids();
       

#    print Dumper( \@active_jobs );
    
    # Not a restarted run, run everything from the start.
    if ( ! @active_jobs && ! $restarted_run ) {
      foreach my $start_logic_name ( @start_logic_names ) {
        run_analysis( $start_logic_name );
	$queued++;
      }
      # set this variable to null so we dont end here again. 
      # This could also be done with a flag, but for now here we are.
      @start_logic_names = ();
    }
    else {

      foreach my $jms_id ( @active_jobs ) {

	next if ( ! $jms_hash{ $jms_id }{ tracking });

        my $logic_name = $jms_hash{ $jms_id }{ logic_name };
	$active_thread_id = $jms_hash{ $jms_id }{ thread_id };
#	$project_name = $thread_name_hash{ $active_thread_id };

        if ( $jms_hash{ $jms_id }{ status } == $FINISHED ) {
	  
	  $jms_hash{ $jms_id }{ tracking } = 0;	  
          my @next_logic_names = next_analysis( $logic_name );

          # no more steps we can take, jump the the next job;
          if ( ! @next_logic_names ) {
            next;
          }

	  foreach my $next_logic_name ( @next_logic_names ) {

	    if ( $analysis{ $next_logic_name }{ sync }  || $analysis{ $next_logic_name }{ thread_sync }) { 

	      $active_thread_id = 0 if ( $analysis{ $next_logic_name }{ sync });

#	      print "SYNC ACTIVE THREAD ID :: $active_thread_id\n";

	      $DB::single = 1;

#	      next if (  $no_restart );
	      next if ( $thread_no_restart{ $active_thread_id } );
	      # we do not go further if new jobs has been started or is running.
	      next if ( @retained_jobs > 0 );

	      if (depends_on_active_jobs( $next_logic_name)) {
#		print "Depends on a job to finish @ $next_logic_name \n";
		next;
	      }
	      
	      my @depends_on;
	      foreach my $step ( keys %flow ) {
		foreach my $analysis ( @{$flow{ $step }} ) {
		  #print "$next_logic_name eq $analysis\n";
		  push @depends_on, $step if ( $next_logic_name eq $analysis );
		}
	      }
	      
	      # active_thread_id aware...
	      my @depend_jobs = fetch_jobs( @depends_on );
	      
#	      print "depends on:"  .Dumper( \@depends_on);
#	      print "depend jobs:" .Dumper( \@depend_jobs);
	      my $all_threads_done = 1;
	      foreach my $ljms_id ( @depend_jobs ) {
		if ( $jms_hash{ $ljms_id }{ status } != $FINISHED ) {
		  $all_threads_done = 0;
		  last;
		}
	      }
	      
	      if ( $all_threads_done ) {
		# collect inputs, and set their tracking to 0
		my @inputs; 
		my @jms_ids;
		
		#print "hash " . Dumper( \%jms_hash );

		foreach my $ljms_id ( @depend_jobs ) {
		  $jms_hash{ $ljms_id }{ tracking } = 0;
		  push @inputs, $jms_hash{ $ljms_id }{ output };
		  push @jms_ids, $ljms_id;
		}
		
		
		$logger->debug(" $jms_id :: $jms_hash{ $jms_id }{ logic_name }  --> $next_logic_name (synced !!!) $no_restart\n");
		#print "inputs: " . Dumper( \@inputs );

		run_analysis( $next_logic_name, \@jms_ids, \@inputs);
		$started++;
	      }
	      
	    }
	    # unsynced part of the pipeline, run the next job.
	    else {
	      $logger->debug(" $jms_id :: $jms_hash{ $jms_id }{ logic_name }  --> $next_logic_name  \n");
	      run_analysis( $next_logic_name, [$jms_id], $jms_hash{ $jms_id }{ output });
	      $started++;
	    }
	  }
	}
	elsif ( $jms_hash{ $jms_id }{ status } == $FAILED || $jms_hash{ $jms_id }{ status } == $KILLED ) {
	  $jms_hash{ $jms_id }{ tracking } = 0;
#	  $no_restart++;
	}
	elsif ( $jms_hash{ $jms_id }{ status } == $RUNNING ) {
          $queued++;
	  $running++;
	}
        else {
          $queued++;
        }
      }
    }


    while ( $max_jobs > 0 && $jobs_submitted < $max_jobs && @retained_jobs ) {
      my $params = shift @retained_jobs;
      submit_job(@$params);
      $started++;
    }


    check_n_store_state();
#    system('clear');
#    print report_spinner();
    print report();
    report2tracker() if ($database_tracking);

    last if ( ! $queued && ! $started && !@retained_jobs);

    # Increase sleeping if there are no jobs running. Should decrease the head node load when running a-ton-of-jobs (tm)
    $sleep_time += $sleep_increase
	if (  ! $running && $sleep_time < $max_sleep_time);

    $sleep_time = $sleep_start
	if (  $running );

#    print STDERR "SLEEP TIME: $sleep_time; 	if (  ! $running && $queued && ! $started && ($sleep_time < $max_sleep_time));\n";

    sleep ( $sleep_time );
    check_jobs();
  }

  print report();
  report2tracker() if ($database_tracking);
  print total_runtime();
  print real_runtime();

  if ( $no_restart ) {
    $logger->warn("The pipeline was unsucessful with $no_restart job(s) not being able to finish\n");
  }
  

  $logger->warn("Retaineded jobs: ". @retained_jobs . " (should be 0)\n") if ( @retained_jobs != 0);
  $end_time = Time::HiRes::gettimeofday();
  store_state();

  # $logger->debug( { 'type'     => "pipeline_stats",
  # 		   'program'  => $0,
  # 		   'pid'      => $$,
  # 		   'status'   => "FINISHED",
  # 		   'runtime'  => $end_time - $start_time });


  return( $no_restart );
}



# 
# 
# 
# Kim Brugger (18 May 2010)
sub run_analysis {
  my ( $logic_name, $pre_ids, @inputs) = @_;

#  $DB::single = 1;

  my $function = function_module($analysis{ $logic_name }{ function }, $logic_name);
	
  $current_logic_name = $logic_name;
  $pre_jms_ids        = $pre_ids || undef;

  {
    no strict 'refs';
    &$function(@inputs);
  }
}


# Nicked the following two functions from Module::Loaded as they are
# not in perl-core for 5.8.X and I needed a special version of them
sub is_loaded (*) { 
    my $pm      = shift;
    my $file    = __PACKAGE__->_pm_to_file( $pm ) or return;


    return 1 if (exists $INC{$file} || $pm eq 'main');
    
    return 0;
}

sub _pm_to_file {
    my $pkg = shift;
    my $pm  = shift or return;
    
    my $file = join '/', split '::', $pm;
    $file .= '.pm';
    
    return $file;
}    

# 
# 
# 
# Kim Brugger (27 Apr 2010)
sub function_module {
  my ($function, $logic_name) = @_;

  if ( ! $function ) {
    store_state();
    use Carp;
    
    Carp::confess "$logic_name does not point to a function\n";
  }

  
  
  my $module = 'main';
    
  ($module, $function) = ($1, $2) if ( $function =~ /(.*)::(\w+)/);
  die "ERROR :::: $module is not loaded!!!\n" if ( ! is_loaded( $module ));
  die "ERROR :::: $logic_name points to $module\:\:$function, but this function does not exist!\n" if ( ! $module->can( $function ) );

  return $module . "::" . $function;
}


# 
# 
# 
# Kim Brugger (23 Apr 2010)
sub print_flow {
  my (@start_logic_names) = @_;

  @start_logic_names = @start_steps if ( !@start_logic_names );


  if (! @start_logic_names) {
    $logger->fail("CTRU::Pipeline::print_flow not called with a logic_name\n");
    exit -1;
  }

  my @analyses;

  foreach my $start_logic_name ( @start_logic_names ) {
    set_analysis_dependencies( $start_logic_name );
  }

  my @logic_names = @start_logic_names;

  print "Starting with: @logic_names \n";
  print "--------------------------------------------------\n";

  
  while ( $current_logic_name = shift @logic_names ) {

    print "$current_logic_name queue: [@logic_names] \n";

    push @analyses, $current_logic_name;

    if ( ! $analysis{$current_logic_name} ) {
      $logger->fatal("No information on $current_logic_name in analysis");
      exit -1;
    }
    else {
      my $function = function_module($analysis{$current_logic_name}{ function }, $current_logic_name);
      print "$current_logic_name ==>  $function\n";
    }
      
    my @next_logic_names = next_analysis( $current_logic_name );

    if ( @next_logic_names ) {
      
      foreach my $next_logic_name ( @next_logic_names ) {
      
	if ($analysis{$next_logic_name}{ sync } ) {
	  print "$current_logic_name --> $next_logic_name (Synced!!)\n";
	}
	else {
	  print "$current_logic_name --> $next_logic_name\n";
	}

	if ( waiting_for_analysis($next_logic_name, @analyses)) {
#	  push @logic_names, $next_logic_name;
	}
	else {
	  push @logic_names, $next_logic_name;
	}
      }
    }
    print "--------------------------------------------------\n";
#    print "end of flow\n";
  }

  
}



# 
# 
# 
# Kim Brugger (23 Apr 2010)
sub validate_flow {
  my (@start_logic_names) = @_;

  die "CTRU::Pipeline::validate_flow not called with a logic_name\n" if (! @start_logic_names);

  my @analyses;

  foreach my $start_logic_name ( @start_logic_names ) {
    set_analysis_dependencies( $start_logic_name );
  }

  my @logic_names = @start_logic_names;

  while ( $current_logic_name = shift @logic_names ) {

    push @analyses, $current_logic_name;

    if ( ! $analysis{$current_logic_name} ) {
      $logger->fatal("No information on $current_logic_name in analysis\n");
      exit -1;
    }
    else {
      my $function = function_module($analysis{$current_logic_name}{ function }, $current_logic_name);
    }
      
    my @next_logic_names = next_analysis( $current_logic_name );

    if ( @next_logic_names ) {
      
      foreach my $next_logic_name ( @next_logic_names ) {
      
	if ($analysis{$next_logic_name}{ sync } ) {
	}
	else {
	}

	if ( waiting_for_analysis($next_logic_name, @analyses)) {
#	  push @logic_names, $next_logic_name;
	}
	else {
	  push @logic_names, $next_logic_name;
	}
      }
    }
#    print "end of flow\n";
  }

  $logger->debug("End of validate_run\n");
  
}



# 
# 
# 
# Kim Brugger (26 Apr 2010)
sub store_state {
  my ($filename ) = @_;

  return if ( ! $use_storing );

  $filename = freeze_file();
  
  $logger->info("CTRU::Pipeline :: Storing state in: '$filename'\n");

  my $blob = {delete_files       => \@delete_files,
	      jms_hash           => \%jms_hash,
	      save_interval      => $save_interval,
	      last_save          => $last_save,
	      save_interval      => $save_interval,
	      max_retry          => $max_retry,
	      sleep_time         => $sleep_time,
	      max_jobs           => $max_jobs,
	      backend            => $backend,
	      job_counter        => $job_counter,
	      start_time         => $start_time,
	      end_time           => $end_time,

	      # These are more post-run tracking...
	      cwd                => $cwd,
	      start_time         => $start_time,
	      username           => $username,
	      host               => $host,
	      project            => $project_name,
	      
	      stats              => $backend->stats,
	      
	      retained_jobs      => \@retained_jobs,
	      current_logic_name => $current_logic_name,

	      #main file variables.
	      argv               => \@argv,
	      analysis_order     => \%analysis_order,
	      dependencies       => \%dependencies,
              analysis           => \%analysis,
	      flow               => \%flow};

  $last_save = Time::HiRes::gettimeofday();

  return Storable::store($blob, $filename);
}



# 
# 
# 
# Kim Brugger (26 Apr 2010)
sub restore_state {
  my ( $filename ) = @_;

  
  $logger->debug("CTRU::Pipeline :: Re-storing state from: '$filename'\n");

  my $blob = Storable::retrieve( $filename);

  @delete_files       = @{$$blob{delete_files}};
  %jms_hash           = %{$$blob{jms_hash}};

  $save_interval      = $$blob{save_interval};

  $last_save          = $$blob{last_save};
  $save_interval      = $$blob{save_interval};
  $max_retry          = $$blob{max_retry};
  $sleep_time         = $$blob{sleep_time};
  $max_jobs           = $$blob{max_jobs};
  $job_counter        = $$blob{job_counter};
  
  $start_time         = $$blob{start_time};
  $end_time           = $$blob{end_time};
	      
  @retained_jobs      = @{$$blob{retained_jobs}};
  $current_logic_name = $$blob{current_logic_name};

  @main::ARGV         = @{$$blob{argv}};
  %analysis_order     = %{$$blob{analysis_order}};
  %dependencies       = %{$$blob{dependencies}} if ($$blob{dependencies});

  backend($$blob{backend}) if ( $$blob{backend});
  $backend->stats($$blob{stats});
  
  # Overwrite the argv array with the values just loaded...
  @argv = @main::ARGV;

}


sub catch_ctrl_c {
    $main::SIG{INT } = \&catch_ctrl_c;
    $main::SIG{KILL} = \&catch_ctrl_c;
#    $main::SIG{HUP } = \&catch_ctrl_c;
    fail("Caught a ctrl-c\n");
    store_state();
}


BEGIN {
  $SIG{INT} = \&catch_ctrl_c;
  $SIG{KILL} = \&catch_ctrl_c;

#  $SIG{HUP} = \&catch_ctrl_c;
  @argv = @main::ARGV;
}


1;



