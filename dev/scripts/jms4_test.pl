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

use lib '/software/packages/ctru-pipeline/modules';
use CTRU::Pipeline;
use CTRU::Pipeline::Misc;
#use CTRU::Pipeline::Samtools;
#use CTRU::Pipeline::Picard;

my $executer = "/software/packages/ctru-pipeline/dev/dummies/local.pl";


CTRU::Pipeline::add_start_step('A', 'single');
CTRU::Pipeline::add_step('A', 'B', 'single_slow');
CTRU::Pipeline::add_step('B', 'D', 'multiple');
CTRU::Pipeline::add_step('B', 'F', );

CTRU::Pipeline::add_step('A', 'C', 'single');
CTRU::Pipeline::add_step('C', 'E', 'multiple');
CTRU::Pipeline::add_step('C', 'F', 'single_slow');

CTRU::Pipeline::add_step('D', 'G', 'single');
CTRU::Pipeline::add_merge_step('E', 'G', 'single');
CTRU::Pipeline::add_merge_step('F', 'G', 'single');


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


sub multiple {
  my ($input) = @_;

  my $cmd = "$executer";
  my $tmp_file = 'tyt';
  
  for ( my $i=0; $i< 3; $i++ ) {
    CTRU::Pipeline::submit_job("$cmd ", $tmp_file);
  }

}

sub multiple_fail {
  my ($input) = @_;

  my $cmd = "$executer";
  my $tmp_file = 'tyt';
  
  for ( my $i=0; $i< 3; $i++ ) {
    CTRU::Pipeline::submit_job("$cmd ", $tmp_file);
  }

  CTRU::Pipeline::submit_job("$cmd  ", $tmp_file);

}


sub single {
  my ($input) = @_;

  my $cmd = "$executer ";
  my $tmp_file = 'tyt';
  CTRU::Pipeline::submit_job("$cmd ", $tmp_file);
}

sub single_slow {
  my ($input) = @_;

  my $cmd = "$executer -S 60";
  my $tmp_file = 'tyt';
  CTRU::Pipeline::submit_job("$cmd ", $tmp_file);
}


# 
# 
# 
# Kim Brugger (09 Sep 2010)
sub single_fail {

  my ($input) = @_;

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
