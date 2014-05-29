#!/usr/bin/perl 
# 
# 
# 
# 
# Kim Brugger (29 May 2014), contact: kim.brugger@easih.ac.uk

use strict;
use warnings;
use Data::Dumper;

use lib '/home/kb468/projects/ctru-pipeline/modules';

use CTRU::Pipeline::Tracker;

my $dbhost = 'mgsrv01';
my $dbname = 'gemini_tracker';

my $dbi = CTRU::Pipeline::Tracker::connect($dbname, $dbhost, "easih_admin", "easih");


for(my$i=0;$i< 100;$i++) {
  CTRU::Pipeline::Tracker::update_status("G000001", "bwa_mem", "running", "$i");
}
