package EASIH::DB;
# 
# General connect to databases module, as I seem to recreate this code constantly
# 
# 
# Kim Brugger (13 Jun 2011), contact: kim.brugger@easih.ac.uk

use strict;
use warnings;
use Data::Dumper;
use Time::HiRes;
use POSIX qw( strftime );

use DBI;


my ($DBname, $DBhost, $DBuser, $DBpass);


# for caching table column names and queries
my %table_columns;
my %sth_hash;


# 
# 
# 
# Kim Brugger (16 Feb 2012)
sub create_db {
  my ($dbname, $dbhost, $db_user, $db_pass) = @_;
  my $drh = DBI->install_driver("mysql");
  my $rc = $drh->func('createdb', $dbname, $dbhost, $db_user, $db_pass, 'admin');
}


# 
# 
# 
# Kim Brugger (16 Feb 2012)
sub drop_db {
  my ($dbname, $dbhost, $db_user, $db_pass) = @_;
  my $drh = DBI->install_driver("mysql");
  my $rc = $drh->func('dropdb', $dbname, $dbhost, $db_user, $db_pass, 'admin');
}



# 
# 
# 
# Kim Brugger (16 Feb 2012)
sub sql_file {
  my ($dbi, $infile) = @_;

  open( my $in, $infile) || die "Could not open '$infile': $!\n";
  my @statements = split(";", join("", <$in>));
  close( $in );
  
  foreach my $statement ( @statements ) {
    $statement =~ s/\s+//;
    next if ( ! $statement );
    $dbi->do( "$statement;" );
  }
}



# 
# 
# 
# Kim Brugger (06 Feb 2012)
sub connect {
  my ($dbname, $dbhost, $db_user, $db_pass) = @_;
  $dbhost  ||= "mgsrv01";
  $db_user ||= 'easih_ro';


  my $dbi = DBI->connect("DBI:mysql:$dbname:$dbhost", $db_user, $db_pass) || die "Could not connect to database: $DBI::errstr";
  $dbi->{mysql_auto_reconnect} = 1;
#  $dbi->{TraceLevel} = 1; ## for debugging
  ($DBname, $DBhost, $DBuser, $DBpass) = ($dbname, $dbhost, $db_user, $db_pass);

  return $dbi;
}



# 
# 
# 
# Kim Brugger (31 May 2012)
sub connect_psql {
  my ($dbname, $dbhost, $db_user, $db_pass) = @_;
  $dbhost  ||= "mgpc17";
  $db_user ||= 'easih_ro';

  my $dbi = DBI->connect("DBI:Pg:dbname=$dbname;host=$dbhost", $db_user) || die "Could not connect to database: $DBI::errstr";

#  $dbi->{TraceLevel} = 1; ## for debugging

  ($DBname, $DBhost, $DBuser, $DBpass) = ($dbname, $dbhost, $db_user, $db_pass);
  
  return $dbi;
}



# 
# 
# 
# Kim Brugger (13 Jun 2011)
sub check_connection {
  my ( $dbi ) = @_;

  print STDERR "Check connection == '" . ($dbi->ping) . "'\n";

  if ( $dbi->ping eq '' ) {
#    print STDERR "reconnect\n";
    $dbi = EASIH::DB::connect($DBname, $DBhost, $DBuser, $DBpass);
    %sth_hash = ();
    print STDERR "Check re-connection == '" . ($dbi->ping) . "'\n";
  }

  return $dbi;
}




# 
# 
# 
# Kim Brugger (06 Feb 2012)
sub prepare {
  my ($dbi, $sql) = @_;

  return $sth_hash{$sql} if ( $sth_hash{$sql} );

  my $sth = $dbi->prepare( $sql ) || die "Could not prepare '$sql':$DBI::errstr\n";
  $sth_hash{$sql} = $sth;

  return $sth;
}


# 
# 
# 
# Kim Brugger (07 Mar 2012)
sub do {
  my ($dbi, $sql, @params) = @_;


  my $sth = $sql if ( $sql->isa("DBI::st"));
  $sth = $dbi->prepare( $sql ) if ( !$sth );
  
  $sth->execute( @params ) || die "$DBI::errstr\n";
  $sth = undef; # somethimes this is retained, I dont know why. Reset it just to be safe
  return 1;
}

# 
# 
# 
# Kim Brugger (06 Feb 2012)
sub fetch_array_hash {
  my ($dbi, $sql, @params) = @_;

  my $sth = $sql if ( $sql->isa("DBI::st"));
  $sth = $dbi->prepare( $sql ) if ( !$sth );
  
  my @results;

  $sth->execute( @params );
 
  while (my $result = $sth->fetchrow_hashref() ) {
    push @results, $result;
  }

  $sth = undef;  # somethimes this is retained, I dont know why. Reset it just to be safe
  return @results if ( wantarray );
  return \@results;
}


# 
# 
# 
# Kim Brugger (06 Feb 2012)
sub fetch_array_array {
  my ($dbi, $sql, @params) = @_;

  my $sth = $sql if ( $sql->isa("DBI::st"));
  $sth = $dbi->prepare( $sql ) if ( !$sth );
  
  my @results;

  $sth->execute( @params );

  while (my @result_array = $sth->fetchrow_array() ) {
    push @results, \@result_array;
  }

  $sth = undef;  # somethimes this is retained, I dont know why. Reset it just to be safe
  return @results if ( wantarray );
  return \@results;
}


# 
# 
# 
# Kim Brugger (06 Feb 2012)
sub fetch_array {
  my ($dbi, $sql, @params) = @_;

  my $sth = $sql if ( $sql->isa("DBI::st"));
  $sth = $dbi->prepare( $sql ) if ( !$sth );

  $sth->execute( @params );
  
  my @results = $sth->fetchrow_array();

  $sth = undef;  # somethimes this is retained, I dont know why. Reset it just to be safe
  return @results if ( wantarray );
  return \@results;
}


# 
# 
# 
# Kim Brugger (06 Feb 2012)
sub fetch_hash {
  my ($dbi, $sql, @params) = @_;

  my $sth = $sql if ( $sql->isa("DBI::st"));
  $sth = $dbi->prepare( $sql ) if ( !$sth );
  
  $sth->execute( @params );

  my $result = $sth->fetchrow_hashref();

  $sth = undef;  # somethimes this is retained, I dont know why. Reset it just to be safe
  return %$result if ( defined $result && wantarray );
  return $result;
}




# 
# 
# 
# Kim Brugger (18 Feb 2012)
sub  get_column_names {
  my ( $dbi, $table ) = @_;

  return @{$table_columns{$table}} if ( $table_columns{$table});

  my $sth = prepare($dbi, "select * from $table where 1=0");
  my $res = $sth->execute(  );
  my @names = @{$sth->{NAME}};
  $table_columns{$table} = \@names;

  return @{$table_columns{$table}};
}


# 
# 
# 
# Kim Brugger (06 Feb 2012)
sub insert {
  my ($dbi, $table, $hash_ref)  = @_;

  my %columns;
  map { $columns{$_} = 1 } get_column_names( $dbi, $table);

  my (@keys, @params, @values);
  foreach my $key (keys %$hash_ref ) {
    
    if ( ! $columns{$key}) {
      print STDERR "Column name '$key' is not present in the '$table' table\n";
      return undef;
    }
    if ( $$hash_ref{ $key } ) {
      push @keys, "$key";
      push @params, "?";
      push @values, "$$hash_ref{ $key }";
    }
  }
  
  my $query = "INSERT INTO $table (" .join(",", @keys) .") VALUES (".join(",", @params).")";
  my $sth = prepare($dbi, $query);
  
  my $execute_value = $sth->execute(@values) || die $DBI::errstr;

  return $sth->{mysql_insertid} ||  -100;
}



# 
# 
# 
# Kim Brugger (29 May 2014)
sub replace {
  my ($dbi, $table, $hash_ref)  = @_;

  my %columns;
  map { $columns{$_} = 1 } get_column_names( $dbi, $table);

  my (@keys, @params, @values);
  foreach my $key (keys %$hash_ref ) {
    
    if ( ! $columns{$key}) {
      print STDERR "Column name '$key' is not present in the '$table' table\n";
      return undef;
    }
    if ( $$hash_ref{ $key } ) {
      push @keys, "$key";
      push @params, "?";
      push @values, "$$hash_ref{ $key }";
    }
  }

#  EASIH::DB::check_connection($dbi);
  
  my $query = "REPLACE INTO $table (" .join(",", @keys) .") VALUES (".join(",", @params).")";
  my $sth = prepare($dbi, $query);
  
#  print STDERR "Replace (connection):: ". $dbi->ping . "\n";
#  my $execute_value = $sth->execute(@values) || die $DBI::errstr;

  my $execute_value = _execute( $sth, \@values);

  return $sth->{mysql_insertid} ||  -100;
}


# 
# 
# 
# Kim Brugger (29 May 2014)
sub _execute {
  my($sth, $values) = @_;


  my $execute_value = $sth->execute(@$values);

  if ( !$execute_value ) {
    if ( $DBI::errstr !~ /Lost connection to MySQL server during query/) { 
      die $DBI::errstr;
    }
  }
}




sub update {
  my ($dbi, $table, $hash_ref, @condition_keys)  = @_;

  my %condition_keys;
  my @conditions;
  map { $condition_keys{ $_ } = 1;
  push @conditions, "$_='$$hash_ref{$_}'";} @condition_keys;

  my %columns;
  map { $columns{$_} = 1 } get_column_names( $dbi, $table);

  my $s = "UPDATE $table SET ";

  my @parts;
  # Build the rest of the sql here ...
  foreach my $key (keys %{$hash_ref}) {
    if ( ! $columns{$key}) {
      print "Column name '$key' is not present in the '$table' table\n";
      return undef;
    }
    # one should not meddle with the id's since it ruins the system
    next if ( $condition_keys{ $key });
    push @parts, "$key = '$$hash_ref{$key}'";
  }

  # collect and make sure we update the right table.
  $s .= join (', ', @parts) ." WHERE " . join(" AND ", @conditions);

  my $sth = $dbi->prepare($s);
  $sth->execute  || die $DBI::errstr;;

  return 1;
}



1;


