# file: load_qtl_experiments.pl
#
# purpose: QTL experiment data into chado
#
#          It is assumed that the .txt files have been verified.
#
# http://search.cpan.org/~capttofu/DBD-mysql-4.022/lib/DBD/mysql.pm
# http://search.cpan.org/~timb/DBI/DBI.pm
#
# history:
#  06/03/13


  use strict;
  use DBI;
  use Data::Dumper;
  use Encode;

  # load local util library
  use File::Spec::Functions qw(rel2abs);
  use File::Basename;
  use lib dirname(rel2abs($0));
  use CropLegumeBaseLoaderUtils;
  
  my $warn = <<EOS
    Usage:
      
    $0 data-dir
EOS
;
  if ($#ARGV < 0) {
    die $warn;
  }
  
  my $input_dir = @ARGV[0];
  my @filepaths = <$input_dir/*.txt>;
  my %files = map { s/$input_dir\/(.*)$/$1/; $_ => 1} @filepaths;
  
  # Used all over
  my ($table_file, $sql, $sth, $row, $count, @records, @fields, $cmd, $rv);
  my ($has_errors);

  my $dataset_name = 'qtl_experiments';

  # Holds experiments that are already in db; assume they should be updated
  my %existing_experiments;

  # Get connected
  my $dbh = connectToDB;

  # set default schema
  $sql = "SET SEARCH_PATH = chado";
  $sth = $dbh->prepare($sql);
  $sth->execute();

  # Use a transaction so that it can be rolled back if there are any errors
  eval {
    loadQTLexperiments($dbh);
    $dbh->commit;   # commit the changes if we get this far
  };
  if ($@) {
    print "\n\nTransaction aborted because $@\n\n";
    # now rollback to undo the incomplete changes
    # but do it in an eval{} as it may also fail
    eval { $dbh->rollback };
  }


  # ALL DONE
  $dbh->disconnect();
  print "\n\n";



################################################################################
####### Major functions                                                #########
################################################################################

sub loadQTLexperiments {
  my $dbh = $_[0];
  my ($fields, $sql, $sth, $row);
  
  print "Loading/verifying QTL_EXPERIMENT.txt...\n";
  
  $table_file = "$input_dir/QTL_EXPERIMENT.txt";
  @records = readFile($table_file);
  my $line_count = 0;
  my $skip_all = 0;  # skip all existing trait records
  foreach $fields (@records) {
    $line_count++;

    my $experiment_id;
    if ($experiment_id=experimentExists($dbh, $fields->{'name'})) {
      next if ($skip_all);
      print "$line_count: experiment ($fields->{'name'}) is already loaded.\nUpdate? (y/n/skipall/q)\n";
      my $userinput =  <STDIN>;
      chomp ($userinput);
      if ($userinput eq 'skipall') {
        $skip_all = 1;
        next;
      }
      elsif ($userinput eq 'n') {
        next;
      }
      elsif ($userinput eq 'q') {
        exit;
      }
      elsif ($userinput eq 'y') {
        $existing_experiments{$fields->{'name'}} = $experiment_id;
        cleanDependants($experiment_id);
      }
      else {
        print "unknown option ($userinput), skipping experiment\n";
        next;
      }
    }
    
    # name, title, description
    $experiment_id = setExperimentRec($dbh, $experiment_id, $fields);
    
    # indicate that this is a QTL project
    $sql = "
      INSERT INTO chado.projectprop
        (project_id, type_id, value)
      VALUES
        ($experiment_id,
         (SELECT cvterm_id FROM chado.cvterm 
          WHERE name='QTL experiment'
            AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='local')),
         '')";
    logSQL($dataset_name, $sql);
    doQuery($dbh, $sql);
    
    # attach geolocation to project record
    my $geolocation_id = getGeoLocation($dbh, $fields);
    my $nd_experiment_id = createExperiment($dbh, $geolocation_id, $fields);
    attachExperiment($dbh, $experiment_id, $nd_experiment_id, $fields);

    # publink_citation
    attachPublication($dbh, $experiment_id, $fields);
    
    # map_collection
    setMapCollection($dbh, $experiment_id, $fields);
    
    # comment
    if ($fields->{'comment'} ne '' && $fields->{'comment'} ne 'null'
          && $fields->{'comment'} ne 'NULL') {
      $sql = "
        INSERT INTO chado.projectprop
          (project_id, type_id, value)
        VALUES
          ($experiment_id,
           (SELECT cvterm_id FROM chado.cvterm 
            WHERE name='comments'
              AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='local')),
           '')";
      logSQL($dataset_name, $sql);
      doQuery($dbh, $sql);
    }#there is a comment

  }#each record
  
  print "Loaded $line_count QTL experiment records.\n\n";
}#loadQTLexperiments
  

################################################################################
################################################################################
################################################################################

sub attachExperiment {
  my ($dbh, $experiment_id, $nd_experiment_id, $fields) = @_;

  my $sql = "
     INSERT INTO chado.nd_experiment_project
       (project_id, nd_experiment_id)
     VALUES
       ($experiment_id, $nd_experiment_id)";
  logSQL($dataset_name, $sql);
  doQuery($dbh, $sql);
}#attachExperiment


sub attachPublication {
  my ($dbh, $experiment_id, $fields) = @_;
  my $sql = "
    INSERT INTO chado.project_pub
      (project_id, pub_id)
    VALUES
      ($experiment_id,
       (SELECT pub_id FROM chado.pub WHERE uniquename='$fields->{'publink_citation'}'))";
  logSQL($dataset_name, $sql);
  doQuery($dbh, $sql);
}#attachPublication


sub cleanDependants {
  my ($experiment_id) = @_;
  
  $sql = "DELETE FROM chado.projectprop WHERE project_id=$experiment_id";
  logSQL('', $sql);
  doQuery($dbh, $sql);
  
  $sql = "DELETE FROM chado.nd_experiment_project WHERE project_id=$experiment_id";
  logSQL('', $sql);
  doQuery($dbh, $sql);
  
  $sql = "DELETE FROM chado.project_pub WHERE project_id=$experiment_id";
  logSQL('', $sql);
  doQuery($dbh, $sql);
}#cleanDependants


sub createExperiment {
  my ($dbh, $geolocation_id, $fields) = @_;
  my ($sql, $sth, $row);
  
  # NOTE: because nd_experiment has no unique constraints, there is no way
  #       to clear out old records when changes are made. This will mean that
  #       unused nd_experiment records will build up over time.
  $sql = "
     INSERT INTO chado.nd_experiment
       (nd_geolocation_id, type_id)
     VALUES
       ($geolocation_id,
        (SELECT cvterm_id FROM chado.cvterm WHERE name='QTL experiment'))
     RETURNING nd_experiment_id";
  logSQL($dataset_name, $sql);
  $sth = doQuery($dbh, $sql);
  if ($row=$sth->fetchrow_hashref) {
    return $row->{'nd_experiment_id'};
  }
  
  return 0;
}#createExperiment


sub getGeoLocation {
  my ($dbh, $fields) = @_;
  my ($sql, $sth, $row);
  
  # nd_geolocation_id is a requied field, so there is (or will be) a 
  #   a record for a null, or unknown, geolocation.
  
  if ($fields->{'geolocation'} eq 'NULL') {
    $fields->{'geolocation'} = '';
  }
  
  $sql = "
   SELECT * FROM chado.nd_geolocation 
   WHERE description = '$fields->{'geolocation'}'";
  logSQL($dataset_name, $sql);
  $sth = doQuery($dbh, $sql);
  if ($row=$sth->fetchrow_hashref) {
    return $row->{'nd_geolocation_id'};
  }
  else {
    $sql = "
      INSERT INTO chado.nd_geolocation (description) 
      VALUES ('$fields->{'geolocation'}')
      RETURNING nd_geolocation_id";
    logSQL($dataset_name, $sql);
    $sth = doQuery($dbh, $sql);
    $row = $sth->fetchrow_hashref;
    return $row->{'nd_geolocation_id'};
  }

}#getGeoLocation


sub setMapCollection {
  my ($dbh, $experiment_id, $fields) = @_;
  if ($fields->{'map_collection'} && $fields->{'map_collection'} ne 'NULL') {
    my $sql = "
       INSERT INTO chado.projectprop
         (project_id, type_id, value, rank)
       VALUES
         ($experiment_id,
          (SELECT cvterm_id FROM chado.cvterm WHERE name='map collection'),
          '$fields->{'map_collection'}')";
    doQuery($dbh, $sql);
    logSQL($dataset_name, $sql);
  }#map_collection field is set
}#setMapCollection


sub setExperimentRec {
  my ($dbh, $experiment_id, $fields) = @_;
  my ($sql, $sth, $row);
  
  # Build description from title and description fields
  my $desc = '';
  if ($fields->{'title'} && $fields->{'title'} ne 'NULL') {
    $desc .= $fields->{'title'} . ' ';
  }
  if ($fields->{'description'} && $fields->{'description'} ne 'NULL') {
    $desc .= $fields->{'description'};
  }
  
  if ($experiment_id) {
    $sql = "
      UPDATE chado.project SET
        name='$fields->{'name'}',
        description='$desc'
      WHERE project_id=$experiment_id
      RETURNING project_id";
  }
  else {
    $sql = "
      INSERT INTO chado.project
        (name, description)
      VALUES
        ('$fields->{'name'}', '$desc')
      RETURNING project_id";
  }
  
  logSQL($dataset_name, $sql);
  $sth = doQuery($dbh, $sql);
  $row = $sth->fetchrow_hashref;
  
  $experiment_id = ($experiment_id) ? $experiment_id : $row->{'project_id'};
  return $experiment_id;
}#setExperimentRec


