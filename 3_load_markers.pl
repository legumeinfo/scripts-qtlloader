# file: load_markers.pl
#
# purpose: Load spreadsheet marker data into a chado database
#
#          It is assumed that the .txt files have been verified.
#
# http://search.cpan.org/~capttofu/DBD-mysql-4.022/lib/DBD/mysql.pm
# http://search.cpan.org/~timb/DBI/DBI.pm
#
# history:
#  05/28/13  eksc  created


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
  my ($has_errors, $line_count);

  my $dataset_name = 'markers';
    
  # Holds markers that are already in db; assume they should be updated
  my %existing_markers;

  # Get connected
  my $dbh = connectToDB;

  # set default schema
  $sql = "SET SEARCH_PATH = chado";
  $sth = $dbh->prepare($sql);
  $sth->execute();

  # Use a transaction so that it can be rolled back if there are any errors
  eval {
    loadMarkers($dbh);
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

sub loadMarkers {
  my $dbh = $_[0];
  my ($fields, $sql, $sth, $row);
  
######################################################################
print "\n\nTHIS SCRIPT NEEDS WORK!\n";
print "Placing markers on genetic maps is not yet implemented\n";
print "Continue? (y/n)";
my $userinput = <STDIN>;
chomp ($userinput);
if ($userinput ne 'y') {
  return;
}
######################################################################

  $table_file = "$input_dir/MARKERS.txt";
  print "Loading/verifying $table_file...\n";
  
  @records = readFile($table_file);
  $line_count = 0;
  my $skip_all = 0;  # skip all existing marker records
  my $update_all = 0; # update all existing marker records without asking
  
  print "\nLoading " . (scalar @records) . " markers...\n";
  
  foreach $fields (@records) {
    $line_count++;
    
    my $marker_id;
    my $uniq_marker_name = makeMarkerName('marker_name', $fields);
print "$line_count: handle marker $uniq_marker_name\n";
    
    if ($marker_id=markerExists($dbh, $uniq_marker_name)) {
      next if ($skip_all);
      if ($update_all) {
        $existing_markers{$uniq_marker_name} = $marker_id;
        cleanDependants($marker_id);
      }
      else {
        print "$line_count: marker ($fields->{'marker_name'}) is already loaded.\n";
        print "Update? (y/all/n/skipall/q)\n";
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
        elsif ($userinput eq 'all') {
          $update_all = 1;
          $existing_markers{$uniq_marker_name} = $marker_id;
          cleanDependants($marker_id);
        }
        elsif ($userinput eq 'y') {
          $existing_markers{$uniq_marker_name} = $marker_id;
          cleanDependants($marker_id);
        }
        else {
          print "unknown option ($userinput), skipping marker\n";
          next;
        }
      }
    }

    # sequence_source, sequence_id: dbxref
    my $primary_dbxref_id = setPrimaryDbxref($dbh, $fields);
    if (!$primary_dbxref_id) {
      $primary_dbxref_id = 'NULL';
    }

    # specieslink_abv, marker_name, source_sequence: feature
    $marker_id = setMarkerRec($dbh, $marker_id, $primary_dbxref_id, $fields);
    
    # sequence_source2, sequence_id2: dbxref attached via feature_dbxref
    setSecondaryDbxref($dbh, $marker_id, $fields);

    # assembly_ver, phys_chr, phys_start: featureloc
    my $start = ($fields->{'phys_start'} < $fields->{'phys_end'}) 
                  ? $fields->{'phys_start'} : $fields->{'phys_end'};
    my $end = ($fields->{'phys_start'} < $fields->{'phys_end'}) 
                  ? $fields->{'phys_end'} : $fields->{'phys_start'};
    if ($fields->{'phys_chr'} && $fields->{'phys_chr'} ne '' 
          && $fields->{'phys_chr'} ne 'NULL' 
          && $fields->{'phys_chr'} ne 'none') {
      my $chr_feature_id = getChromosomeID($dbh, $fields->{'phys_chr'}, 
                                           $fields->{'assembly_ver'});
      if ($chr_feature_id == 0) {
        $chr_feature_id = getScaffoldID($dbh, $fields->{'phys_chr'}, 
                                           $fields->{'assembly_ver'});
        if ($chr_feature_id == 0) {
          my $msg = "Unable to find chromosome/scaffold feature ";
          $msg = "$fields->{'phys_chr'}, version $fields->{'assembly_ver'}.";
          reportError($line_count, $msg);
        }
      }
      $sql = "
        INSERT INTO chado.featureloc
          (feature_id, srcfeature_id, fmin, fmax)
        VALUES
          ($marker_id, $chr_feature_id, $start, $end)";
      logSQL($dataset_name, $sql);
      doQuery($dbh, $sql);
    }#physical position exists
    
    # publink_citation: feature_pub
    if ($fields->{'publink_citation'} && $fields->{'publink_citation'} ne ''
          && $fields->{'publink_citation'} ne 'NULL'
          && $fields->{'publink_citation'} ne 'N/A') {
      $sql = "
        INSERT INTO chado.feature_pub
          (feature_id, pub_id)
        VALUES
          ($marker_id,
           (SELECT pub_id FROM chado.pub 
            WHERE uniquename='$fields->{'publink_citation'}'))";
      logSQL($dataset_name, $sql);
      $sth = doQuery($dbh, $sql);
      $sth->finish;
    }# attach publication to marker
    
    # primary_map, secondary_map(s), is_gene, marker_type, f_primer, 
    # r_primer, pub_linkage_group, frag_size, public, alt_citation, 
    # comments: featureprop
    setFeatureprop($dbh, 'assembly_ver', 'assembly version', 0, $fields);
    setFeatureprop($dbh, 'primary_map', 'on map', 1, $fields);
    setFeatureprop($dbh, 'secondary_map', 'on map', 2, $fields);
    setFeatureprop($dbh, 'is_gene', 'is gene', 0, $fields);
    setFeatureprop($dbh, 'marker_type', 'marker type', 0, $fields);
    setFeatureprop($dbh, 'f_primer', 'forward primer', 0, $fields);
    setFeatureprop($dbh, 'r_primer', 'reverse primer', 0, $fields);
    setFeatureprop($dbh, 'pub_linkage_group', 'linkage group name used in publication', 0, $fields);
    setFeatureprop($dbh, 'frag_size', 'PCR amplicon size', 0, $fields);
    setFeatureprop($dbh, 'public', 'public', 0, $fields);
    setFeatureprop($dbh, 'alt_citation', 'alt_citation', 0, $fields);
    setFeatureprop($dbh, 'comments', 'comments', 0, $fields);
  }#each record
  
  print "\n\nLoaded $line_count markers.\n\n";
}#loadMarkers


################################################################################
################################################################################
################################################################################

sub cleanDependants {
  my ($marker_id) = @_;
  
  # remove attached dbxrefs
  $sql = "DELETE FROM chado.feature_dbxref WHERE feature_id=$marker_id";
  logSQL('', $sql);
  doQuery($dbh, $sql);
  
  # remove coordinates
  $sql = "DELETE FROM chado.featureloc WHERE feature_id=$marker_id";
  logSQL('', $sql);
  doQuery($dbh, $sql);
  
  # remove associated pub(s)
  $sql = "DELETE FROM chado.feature_pub WHERE feature_id=$marker_id";
  logSQL('', $sql);
  doQuery($dbh, $sql);
  
  # remove all feature properties
  $sql = "DELETE FROM chado.featureprop WHERE feature_id=$marker_id";
  logSQL('', $sql);
  doQuery($dbh, $sql);
}#cleanDependants;


sub setFeatureprop {
  my ($dbh, $fieldname, $typename, $rank, $fields) = @_;
  my ($sql, $sth);
  
  if ($fields->{$fieldname} && $fields->{$fieldname} ne '' 
        && $fields->{$fieldname} ne 'NULL') {
    my $uniq_marker_name = makeMarkerName('marker_name', $fields);
    $sql = "
      INSERT INTO chado.featureprop
        (feature_id, type_id, value, rank)
      VALUES
        ((SELECT feature_id FROM chado.feature 
          WHERE uniquename='$uniq_marker_name'),
         (SELECT cvterm_id FROM chado.cvterm 
          WHERE name='$typename'
            AND cv_id = (SELECT cv_id FROM chado.cv WHERE name='local')),
         '$fields->{$fieldname}', $rank)";
    logSQL($dataset_name, $sql);
    doQuery($dbh, $sql);
  }#value for fieldname exists
}#setFeatureprop


sub setMarkerRec {
  my ($dbh, $marker_id, $primary_dbxref_id, $fields) = @_;
  
  my $uniq_marker_name = makeMarkerName('marker_name', $fields);
  my $organism_id = getOrganismID($dbh, $fields->{'specieslink_abv'}, $line_count);
  if ($existing_markers{$uniq_marker_name}) {
    $sql = "
      UPDATE chado.feature SET
        dbxref_id=$primary_dbxref_id,
        organism_id=$organism_id,
        name='$fields->{'marker_name'}',
        uniquename='$uniq_marker_name',
        residues='$fields->{'source_sequence'}'
      WHERE feature_id=$marker_id";
  }
  else {
    $sql = "
      INSERT INTO chado.feature
        (dbxref_id, organism_id, name, uniquename, residues, type_id)
      VALUES
        ($primary_dbxref_id, $organism_id, 
         '$fields->{'marker_name'}', 
         '$uniq_marker_name',
         '$fields->{'source_sequence'}',
         (SELECT cvterm_id FROM chado.cvterm 
          WHERE name='genetic_marker'
            AND cv_id = (SELECT cv_id FROM chado.cv WHERE name='sequence')))
       RETURNING feature_id";
  }
  
  logSQL($dataset_name, $sql);
  $sth = doQuery($dbh, $sql);
  if (!$marker_id) {
    $row = $sth->fetchrow_hashref;
    $marker_id = $row->{'feature_id'};
  }
  $sth->finish;
  
  return $marker_id;
}#setMarkerRec


sub setPrimaryDbxref {
  my ($dbh, $fields) = @_;
  my ($sql, $sth, $row);
  
  my $primary_dbxref_id = 0;
  if ($fields->{'sequence_source'} && $fields->{'sequence_source'} ne 'NULL'
        && $fields->{'sequence_id'} && $fields->{'sequence_id'} ne 'NULL') {
  
    # Only load if not already in db
    if (!($primary_dbxref_id = dbxrefExists($dbh, $fields->{'sequence_source'}, 
                                            $fields->{'sequence_id'}))) {
      $sql = "
        INSERT INTO chado.dbxref
          (db_id, accession)
        VALUES
          ((SELECT db_id FROM chado.db 
            WHERE name='$fields->{'sequence_source'}'),
           '$fields->{'sequence_id'}')
        RETURNING dbxref_id";
      logSQL($dataset_name, $sql);
      $sth = doQuery($dbh, $sql);
      $row = $sth->fetchrow_hashref;
      $primary_dbxref_id = $row->{'dbxref_id'};
      $sth->finish;
    }#dbxref doesn't already exist
  }#primary source fields are set
  
  return $primary_dbxref_id;
}#setPrimaryDbxref


sub setSecondaryDbxref {
  my ($dbh, $marker_id, $fields) = @_;
  my ($sql, $sth, $row);
  
  if ($fields->{'sequence_source2'} && $fields->{'sequence_source2'} ne 'NULL'
        && $fields->{'sequence_id2'} && $fields->{'sequence_id2'} ne 'NULL') {
    my $dbxref_id = dbxrefExists($dbh, $fields->{'sequence_source2'}, 
                                       $fields->{'sequence_id2'});
    if (!$dbxref_id) {
      $sql = "
        INSERT INTO chado.dbxref
          (db_id, accession)
        VALUES
          ((SELECT db_id FROM chado.db 
            WHERE name='$fields->{'sequence_source2'}'),
           '$fields->{'sequence_id2'}') 
        RETURNING dbxref_id";
      logSQL($dataset_name, $sql);
      $sth = doQuery($dbh, $sql);
      $row = $sth->fetchrow_hashref;
      $dbxref_id = $row->{'dbxref_id'};
      $sth->finish;
    }
    
    # connect dbxref record to feature record
    $sql = "
      INSERT INTO chado.feature_dbxref
        (feature_id, dbxref_id)
      VALUES
        ($marker_id, $dbxref_id)";
    logSQL($dataset_name, $sql);
    doQuery($dbh, $sql);
  }#secondary source fields are set
}#setSecondaryDbxref



