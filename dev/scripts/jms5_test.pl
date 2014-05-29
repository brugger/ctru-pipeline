#!/usr/bin/perl 
# 
# Map/realign/indels/snps integrated pipeline
# 
# 
# Kim Brugger (27 Jul 2010), contact: kim.brugger@easih.ac.uk

use strict;
use warnings;
use Data::Dumper;

use Getopt::Std;

#use lib '/software/packages/ctru-pipeline/modules';
use lib '/home/kb468/projects/ctru-pipeline/modules';
use CTRU::Pipeline;
use CTRU::Pipeline::Misc;
#use CTRU::Pipeline::Samtools;
#use CTRU::Pipeline::Picard;

my $executer = "/software/packages/ctru-pipeline/dev/dummies/local.pl";


CTRU::Pipeline::start_step('A', 'multiple_starts');
CTRU::Pipeline::next_step('A', 'B', 'multiple');
CTRU::Pipeline::next_step('B', 'C', 'single');
CTRU::Pipeline::thread_merge_step('C', 'D', 'single');

CTRU::Pipeline::merge_step('D', 'E', 'single');


my %opts;
getopts('R:', \%opts);

#CTRU::Pipeline::no_store();
#CTRU::Pipeline::print_flow('fastq-split');

#CTRU::Pipeline::backend('Darwin');
CTRU::Pipeline::backend('Local');
#CTRU::Pipeline::backend('SGE');
CTRU::Pipeline::max_retry(3);

use lib '/software/packages/ctru-clinical/modules';
use CTRU::ComplexLog;
CTRU::Pipeline::logger('CTRU::ComplexLog');
$CTRU::Pipeline::logger->level('fatal');
CTRU::Pipeline::database_tracking('gemini_tracker', 'mgsrv01.medschl.cam.ac.uk', 'easih_admin', 'easih');

if ( $opts{R} ) {
  &CTRU::Pipeline::reset($opts{R});
#  &CTRU::Pipeline::hard_reset( $opts{R} );
#  &CTRU::Pipeline::reset();
  &CTRU::Pipeline::no_store();
  CTRU::Pipeline::run('A');
}
else {
  CTRU::Pipeline::run('A');
  CTRU::Pipeline::store_state();
}

$CTRU::Pipeline::logger->flush_queue();




# 
# 
# 
# Kim Brugger (20 May 2014)
sub multiple_starts {
  
  for(my $i=0;$i< 4; $i++ ) {
    CTRU::Pipeline::set_project_name("START-$i");
    my $thread_id = CTRU::Pipeline::new_thread_id();
    
    my $cmd = "$executer";
    my $tmp_file = 'tyt';
    CTRU::Pipeline::submit_job("$cmd ", $tmp_file);
  }

  
}


sub multiple {
  my ($input, $thread_id) = @_;

  my $cmd = "$executer";
  my $tmp_file = 'tyt';
  
  for ( my $i=0; $i< 3; $i++ ) {
    CTRU::Pipeline::submit_job("$cmd ", $tmp_file);
  }

}

sub multiple_fail {
  my ($input, $thread_id) = @_;

  my $cmd = "$executer";
  my $tmp_file = 'tyt';
  
  for ( my $i=0; $i< 3; $i++ ) {
    CTRU::Pipeline::submit_job("$cmd ", $tmp_file);
  }

  CTRU::Pipeline::submit_job("$cmd  ", $tmp_file);

}


sub single {
  my ($input, $thread_id) = @_;

  my $cmd = "$executer ";
  my $tmp_file = 'tyt';
  CTRU::Pipeline::submit_job("$cmd ", $tmp_file);
}

sub single_slow {
  my ($input, $thread_id) = @_;

  my $cmd = "$executer -S 60";
  my $tmp_file = 'tyt';
  CTRU::Pipeline::submit_job("$cmd ", $tmp_file);
}


# 
# 
# 
# Kim Brugger (09 Sep 2010)
sub single_fail {

  my ($input, $thread_id) = @_;

  my $cmd = "$executer ";
  my $tmp_file = 'tyt';
  CTRU::Pipeline::submit_job("$cmd -F", $tmp_file);
  
}




# 
# 
# 
# Kim Brugger (22 Apr 2010)
sub usage {
  
  print "Not the right usage, please look at the code\n";
  exit;

}
