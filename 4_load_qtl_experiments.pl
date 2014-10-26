# file: load_qtl_experiments.pl
#
# purpose: QTL experiment data into chado
#
#          It is assumed that the .txt files have been verified.
#
#          Tables: project, projectprop, project_pub, nd_geolocation, 
#                  nd_experiment_project
#
# http://search.cpan.org/~capttofu/DBD-mysql-4.022/lib/DBD/mysql.pm
# http://search.cpan.org/~timb/DBI/DBI.pm
#
# history:
#  06/03/13  eksc  created
#  08/20/14  eksc  modified for revised spreadsheets and better use of CVs; 
#                    working toward proper Tripal QTL module.


  use strict;
  use DBI;
  use Data::Dumper;
  use Encode;
  use feature 'unicode_strings';


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
  
  # get worksheet contants
  my %qei = getSSInfo('QTL_EXPERIMENTS');
  
  # Used all over
  my ($table_file, $sql, $sth, $row, $count, @records, @fields, $cmd, $rv);
  my ($has_errors);

  my $dataset_name = 'qtl_experiments';

  # holds experiments that are already in db; assume they should be updated
  my %existing_experiments;

  # get connected
  my $dbh = connectToDB;

  # set default schema
  $sql = "SET SEARCH_PATH = chado";
  $sth = $dbh->prepare($sql);
  $sth->execute();

  # use a transaction so that it can be rolled back if there are any errors
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
  
  print "Loading/verifying $qei{'worksheet'}.txt...\n";
  $table_file = "$input_dir/$qei{'worksheet'}.txt";
  @records = readFile($table_file);
  
  my ($skip, $skip_all, $update, $update_all);

  my $line_count = 0;
  foreach $fields (@records) {
    $line_count++;

    my $experiment_id;
    if ($experiment_id=experimentExists($dbh, $fields->{$qei{'name_fld'}})) {
      # qtl experiment exists
      next if ($skip_all);
      if ($update_all) {
          $existing_experiments{$fields->{$qei{'name_fld'}}} = $experiment_id;
          
          # remove dependent records; they will be re-inserted
          cleanDependants($experiment_id);
      }
      else {
        my $prompt = "$line_count: qtl experiment ($fields->{$qei{'name_fld'}}) ";
        ($skip, $skip_all, $update, $update_all) = checkUpdate($prompt);
        
        next if ($skip || $skip_all);
        
        if ($update || $update_all) {
          $existing_experiments{$fields->{$qei{'name_fld'}}} = $experiment_id;
          
          # remove dependent records; they will be re-inserted
          cleanDependants($experiment_id);
        }
      }#update_all not set
    }#map set exists
    
    # name, title, description
    $experiment_id = setExperimentRec($dbh, $experiment_id, $fields);
    if (!$experiment_id) {
      print "Unable to process record $line_count.\n\n";
      next;
    }
    
    # attach description
    attachDescription($dbh, $experiment_id, $fields);
    
    # attach geolocation to project record
    my $geolocation_id = getGeoLocation($dbh, $fields);
    my $nd_experiment_id = createExperiment($dbh, $geolocation_id, $fields);
    if (!$geolocation_id || !$nd_experiment_id) {
      print "Unable to process record $line_count.\n\n";
      next;
    }
    
    attachExperiment($dbh, $experiment_id, $nd_experiment_id, $fields);

    # publink_citation
    attachPublication($dbh, $experiment_id, $fields);
    
    # map_collection
    setMapCollection($dbh, $experiment_id, $fields);
    
    # comment
    my $comment = $dbh->quote($fields->{$qei{'comment_fld'}});
    if ($comment ne '' && $comment ne 'null' && $comment ne 'NULL') {
      $sql = "
        INSERT INTO chado.projectprop
          (project_id, type_id, value)
        VALUES
          ($experiment_id,
           (SELECT cvterm_id FROM chado.cvterm 
            WHERE name='Project Comment'
              AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='project_property')),
           ".$dbh->quote($comment).")";
      logSQL($dataset_name, $sql);
      doQuery($dbh, $sql);
    }#there is a comment
  }#each record
  
  print "Loaded $line_count QTL experiment records.\n\n";
}#loadQTLexperiments
  

################################################################################
################################################################################
################################################################################

sub attachDescription {
  my ($dbh, $experiment_id, $fields) = @_;
  
  my $desc = $fields->{$qei{'desc_fld'}};
  
  $sql = "
    INSERT INTO chado.projectprop
      (project_id, type_id, value)
    VALUES
      ($experiment_id,
       (SELECT cvterm_id FROM chado.cvterm 
        WHERE name='Project Description'
          AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='project_property')),
       ".$dbh->quote($desc).")";
  logSQL($dataset_name, $sql);
  doQuery($dbh, $sql);
}#attachDescription


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
  
  my $pub_id = getPubID($dbh, $fields->{$qei{'pub_fld'}});
  if ($pub_id == 0) {
    print "Fatal error: Unable to continue\n\n";
    exit;
  }
  
  my $sql = "
    INSERT INTO chado.project_pub
      (project_id, pub_id)
    VALUES
      ($experiment_id, $pub_id)";
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
        (SELECT cvterm_id FROM chado.cvterm 
         WHERE name='QTL Experiment'
               AND cv_id=(SELECT cv_id FROM cv WHERE name='nd_experiment_types')))
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
  
  my $geoloc = $fields->{$qei{'geoloc_fld'}};
  # make sure the description will fit (if not already loaded)
  if (length($geoloc) > 255) {
    print "Geolocation description is too long: [$geoloc]";
    return 0;
  }
  
  # nd_geolocation_id is a requied field, so there is (or will be) a 
  #   a record for a null, or unknown, geolocation.
  
  if ($geoloc eq 'NULL') {
    $geoloc = '';
  }
  
  $sql = "
   SELECT * FROM chado.nd_geolocation 
   WHERE description = '$geoloc'";
  logSQL($dataset_name, $sql);
  $sth = doQuery($dbh, $sql);
  if ($row=$sth->fetchrow_hashref) {
    return $row->{'nd_geolocation_id'};
  }
  else {
    $sql = "
      INSERT INTO chado.nd_geolocation (description) 
      VALUES ('$geoloc')
      RETURNING nd_geolocation_id";
    logSQL($dataset_name, $sql);
    $sth = doQuery($dbh, $sql);
    $row = $sth->fetchrow_hashref;
    return $row->{'nd_geolocation_id'};
  }

}#getGeoLocation


sub setMapCollection {
  my ($dbh, $experiment_id, $fields) = @_;
  
  my $mapname = $fields->{$qei{'map_fld'}};
  if ($mapname && $mapname ne 'NULL') {
    my $sql = "
       INSERT INTO chado.projectprop
         (project_id, type_id, value, rank)
       VALUES
         ($experiment_id,
          (SELECT cvterm_id FROM chado.cvterm 
           WHERE name='Project Map Collection'
                 AND cv_id = (SELECT cv_id FROM cv 
                              WHERE name='project_property')),
          '$mapname',
          0)";
    doQuery($dbh, $sql);
    logSQL($dataset_name, $sql);
  }#map_collection field is set
}#setMapCollection


sub setExperimentRec {
  my ($dbh, $experiment_id, $fields) = @_;
  my ($sql, $sth, $row);
  
  # use title for description field
  my $desc = $fields->{$qei{'title_fld'}};
  my $name = $fields->{$qei{'name_fld'}};
  
  if ($experiment_id) {
    $sql = "
      UPDATE chado.project SET
        name='$name',
        description=".$dbh->quote($desc)."
      WHERE project_id=$experiment_id
      RETURNING project_id";
  }
  else {
    $sql = "
      INSERT INTO chado.project
        (name, description)
      VALUES
        ('$name', ".$dbh->quote($desc).")
      RETURNING project_id";
  }
  
  logSQL($dataset_name, $sql);
  $sth = doQuery($dbh, $sql);
  if ($row = $sth->fetchrow_hashref) {
    $experiment_id = ($experiment_id) ? $experiment_id : $row->{'project_id'};

    # indicate that this is a QTL project
    $sql = "
      INSERT INTO chado.projectprop
        (project_id, type_id, value)
      VALUES
        ($experiment_id,
         (SELECT cvterm_id FROM chado.cvterm 
          WHERE name='QTL Experiment'
            AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='local')),
         '')";
    logSQL($dataset_name, $sql);
    doQuery($dbh, $sql);
    
    return $experiment_id;
  }
  
  return 0;
}#setExperimentRec


