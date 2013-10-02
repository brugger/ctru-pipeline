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

  $CTRU::Pipeline::logger->fatal("pull_job is not implemented for this Backend\n");
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
    $CTRU::Pipeline::logger->warn("job_runtime has not been implemented for the backend you are using!\n");
  return 0;

}

sub job_memory {
  $CTRU::Pipeline::logger->warn("job_memory has not been implemented for the backend you are using!\n");
  return 0;
}



sub stats {
    $CTRU::Pipeline::logger->warn("stats has not been implemented for the backend you are using!\n");
  return 0;
}



1;
