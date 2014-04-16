package CTRU::Pipeline::Backend;

use CTRU::Pipeline::Backend::DetachedDummy;
use CTRU::Pipeline::Backend::Local;
use CTRU::Pipeline::Backend::Darwin;
#use CTRU::Pipeline::Backend::MPIexec;
use CTRU::Pipeline::Backend::SGE;



use strict;
use warnings;


# 
# 
# 
# Kim Brugger (18 May 2010)
sub submit_job {

  $CTRU::Pipeline::logger->fatal("submit_job is not implemented for this Backend\n");
  exit 1;


}

# 
# 
# 
# Kim Brugger (18 May 2010)
sub job_status {

  $CTRU::Pipeline::logger->info("pull_job is not implemented for this Backend\n");
  exit -1;
}


# 
# 
# 
# Kim Brugger (18 May 2010)
sub kill {

    $CTRU::Pipeline::logger->fatal("kill is not implemented for this Backend\n");
    exit -1;
 
}

sub job_runtime {
  $CTRU::Pipeline::logger->info("job_runtime has not been implemented for the backend you are using!\n");
  return 0;
  
}

sub job_memory {
  $CTRU::Pipeline::logger->info("job_memory has not been implemented for the backend you are using!\n");
  return 0;
}



sub stats {
  $CTRU::Pipeline::logger->info("stats has not been implemented for the backend you are using!\n");
  return 0;
}

# checks enviroment, eg qsub for SGE!
sub check {
  $CTRU::Pipeline::logger->info("check has not been implemented for the backend you are using!\n");
  return 1;
}

1;
