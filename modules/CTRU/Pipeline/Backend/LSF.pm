package CTRU::Pipeline::Backend::LSF;

use strict;
use warnings;


use CTRU::Pipeline;
use base(qw(CTRU::Pipeline::Backend));



my %stats;

# 
# 
# 
# Kim Brugger (24 Jun 2010)
sub stats {
  my ($self, $new_stats ) = @_;
  %stats = %$new_stats if ( $new_stats );
  return \%stats;
}




# 
# 
# 
# Kim Brugger (18 May 2010)
sub submit_job {
  my ($self, $cmd, $limit) = @_;

  $limit = " $limit" if ( $limit && $limit ne "");
  $limit ||= "";

  my $farm_id = int(rand(1000000));

  my $out_file = "$CTRU::Pipeline::cwd/$CTRU::Pipeline::project_name.o$farm_id";
  my $err_file = "$CTRU::Pipeline::cwd/$CTRU::Pipeline::project_name.e$farm_id";

  my ($tmp_fh, $tmp_file) = File::Temp::tempfile(DIR => "./tmp" );
  close( $tmp_fh);
  system "rm $tmp_file";
  $tmp_file .= ".lsf";


  my $LSF_cmd = "bsub -o $out_file -e $err_file ";
  $LSF_cmd   .= " -J $CTRU::Pipeline::project_name "     if ( $CTRU::Pipeline::project_name );
  $LSF_cmd   .= " -P $CTRU::Pipeline::project_id " if ( $CTRU::Pipeline::project_id );
  $LSF_cmd   .= " $limit " if ( $limit );
  $LSF_cmd   .= " -q $CTRU::Pipeline::queue_name "   if ( $CTRU::Pipeline::queue_name ); 

  $LSF_cmd   .= " '$cmd' > $tmp_file ";


  print STDERR  "$LSF_cmd\n";
  system($LSF_cmd);
  my $status = $?;	

  die "Could not submit job: $!\n" if ( $status );
  
  my $job_id = -100;
    
  if ( -s $tmp_file ) { 
    open (my $tfile, $tmp_file) || die "Could not open '$tmp_file':$1\n";
    while(<$tfile>) {
      $job_id = $1 if ( /\<(\d+)\>/);
    }
    close ($tfile);
    $job_id =~ s/(\d+?)\..*/$1/;
  }
  
  system "rm $tmp_file" if ( $job_id != -100 );

  $stats{ $job_id }{ start } = Time::HiRes::gettimeofday;

  $stats{$job_id}{'stderr_file'} = "$err_file";
  $stats{$job_id}{'stdout_file'} = "$out_file";
  
  return $job_id;
}



# 
# 
# 
# Kim Brugger (18 May 2010)
sub job_status {
  my ($self, $job_id) = @_;


#  print "$job_id\n";

  return $CTRU::Pipeline::FAILED if ( $job_id == -100);

  my ( $user, $status, $queue);

  open (my $pipe, "bjobs $job_id | " ) || die "Could not open pipe: $!\n";
  while (<$pipe>) { 
    next if (/^JOBID/);
    chomp;
    (undef, $user, $status, $queue) = split(/\s+/);
  }
  close( $pipe );
#  print "JOB_ID = $job_id, status == $status \n";

  return $CTRU::Pipeline::UNKNOWN  if ( ! $status );
  return $CTRU::Pipeline::FAILED   if ( $status eq 'EXIT');
  return $CTRU::Pipeline::RUNNING  if ( $status eq 'RUN');
  return $CTRU::Pipeline::QUEUEING if ( $status eq 'PEND');

  if ( $status eq 'DONE' ) {

    my $bjobs_output = "";
    open ( my $pipe, "bjobs -l $job_id  |" ) || die "Could not open pipe: $!\n";
    while (<$pipe>) { 
      
      if ( /\d{2}:\d{2}:\d{2}/ ) {
	chomp;
	$_ = "\n $_";
      }
      if ( $_ =~ s/^\s{21}?//) {
	chomp;
	$_ .= "";
      }
      $bjobs_output .= $_;
    }

#    print "$bjobs_output\n";
      
    $stats{ $job_id }{ 'end' } = Time::HiRes::gettimeofday;



    my ($max_mem, $avg_mem, $threads, $run_time);
    if ( $bjobs_output =~ /MAX MEM:\s+(\d+) Mbytes/ ) {
      $stats{ $job_id }{ 'memory' }     = $1 * 1_000_000;
    }
    if ( $bjobs_output =~ /MAX MEM:\s+(\d+.\d+) Gbytes/ ) {
      $stats{ $job_id }{ 'memory' }     = $1 * 1_000_000_000;
    }
    
    if ( $bjobs_output =~ /The CPU time used is (\d+.\d+) seconds/ ) {
      $stats{ $job_id }{ 'runtime' } = $1;
    }

    

#    print "$job_id is finished -- and succeeded";
    if ( $bjobs_output =~ /Done successfully/ ) {      
      # Remove the darwin logfiles, as we succeeded and do not need them anymore...
      if ( $stats{ $job_id }{'stderr_file'} ) {
	system "rm -f $stats{ $job_id }{'stderr_file'}";
      }
      if ( $stats{ $job_id }{'stdout_file'} ) {
	system "rm -f $stats{ $job_id }{'stdout_file'}";
      }

#      print "successfully\n";
      return $CTRU::Pipeline::FINISHED 
    }
#    print "and failed\n";

    return $CTRU::Pipeline::FAILED;    
  }


  return $CTRU::Pipeline::UNKNOWN;
}




# 
# 
# 
# Kim Brugger (16 Apr 2014)
sub check {

  my $qsub = `which bsub`;
  chomp $qsub;

#  print STDERR "$qsub \n";
  
  return 1 if ( $qsub =~ /bsub/);
  return 0;
}




# 
# 
# 
# Kim Brugger (06 Jan 2011)
sub kill {
  my ($self, $job_id) = @_;
  system "qdel $job_id 2> /dev/null";
}


# 
# 
# 
# Kim Brugger (27 May 2010)
sub job_runtime {
  my ($self, $job_id ) = @_;

  return $stats{$job_id}{runtime};
}


# 
# 
# 
# Kim Brugger (27 May 2010)
sub job_memory {
  my ($self, $job_id ) = @_;

  
  my $mem_usage = $stats{$job_id}{memory };

  return undef if ( ! defined $mem_usage);

  return 0 if ( ! defined $mem_usage);

  if ( $mem_usage =~ /(\d+)kb/i) {
    $mem_usage = $1* 1000;
  }
  elsif ( $mem_usage =~ /(\d+)mb/i) {
    $mem_usage = $1* 1000000;
  }
  elsif ( $mem_usage =~ /(\d+)gb/i) {
    $mem_usage = $1* 1000000000;
  }
  $stats{$job_id}{memory} = $mem_usage;

  return $stats{$job_id}{memory};
}



1;
