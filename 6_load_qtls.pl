# file: load_qtls.pl
#
# purpose: Load spreadsheet QTL data into chado
#
#          It is assumed that the .txt files have been verified.
#
# http://search.cpan.org/~capttofu/DBD-mysql-4.022/lib/DBD/mysql.pm
# http://search.cpan.org/~timb/DBI/DBI.pm
#
# history:
#  06/04/13  eksc  created
#  10/16/13  eksc  modified for new spreadsheet design


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

  # used globally:
  my $line_count;
  
  my $dataset_name = 'QTLs';
  
  # Holds QTLs that are already in db; assume they should be updated
  my %existing_qtls;

  # Get connected
  my $dbh = connectToDB;

  # set default schema
  my $sql = "SET SEARCH_PATH = chado";
  my $sth = $dbh->prepare($sql);
  $sth->execute();

  # Use a transaction so that it can be rolled back if there are any errors
  eval {
    loadQTLs($dbh);  
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
  print "\n\nScript completed\n\n";


################################################################################
####### Major functions                                                #########
################################################################################

sub loadQTLs {
  my $dbh = $_[0];
  my ($fields, $sql, $sth, $row);
  
  my $table_file = "$input_dir/QTL.txt";
  print "Loading/verifying $table_file...\n";
  
  my @records = readFile($table_file);
  $line_count = 0;
  my $skip_all = 0;  # skip all existing qtl records
  my $update_all = 0; # update all existing qtl records without asking
  foreach $fields (@records) {
    $line_count++;
    
    # create parent record: feature
    #   specieslink_name, qtl_symbol, qtl_identifer
    my $qtl_name    = makeQTLName($fields);
print "$line_count: $qtl_name\n";
    
    my $qtl_id;
    if ($qtl_id = getQTLid($dbh, $qtl_name)) {
      next if ($skip_all);
      if ($update_all) {
        cleanDependants($dbh, $qtl_id);
      }
      else {
        print "$line_count: qtl_symbol ($fields->{'qtl_symbol'}) is already loaded.\nUpdate? (y/n/skipall/all/q)\n";
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
          cleanDependants($dbh, $qtl_id);
        }
        elsif ($userinput eq 'y') {
          cleanDependants($dbh, $qtl_id);
        }
        else {
          print "unknown option ($userinput), skipping trait\n";
          next;
        }
      }#get user input
    }#qtl exits
    
    # Create/update QTL record
    $qtl_id = setQTLRecord($dbh, $qtl_id, $fields);
    
#print "  attach experiment\n";
    # link to study (project) via new table feature_project
    attachExperiment($dbh, $qtl_id, $fields);
         
#print "  attach experiment trait name\n";
    # trait_description
    loadFeatureprop($dbh, $qtl_id, 'expt_trait_name', 'experiment trait name', $fields);
    
#print "  attach experiment trait description\n";
    # trait_description
    loadFeatureprop($dbh, $qtl_id, 'expt_trait_description', 'experiment trait description', $fields);
    
#print "  attach trait unit\n";
    # trait_unit
    loadFeatureprop($dbh, $qtl_id, 'trait_unit', 'trait unit', $fields);

#print "  attach qtl symbol\n";
    # link to trait (qtl_symbol) via feature_cvterm and feature_cvtermprop
    attachTrait($dbh, $qtl_id, $fields);
    
#print "  attach qtl identifier\n";
    # qtl_identifier
    loadFeatureprop($dbh, $qtl_id, 'qtl_identifier', 'QTL identifier', $fields);
    
#print "  attach pub qtl symbol\n";
    # expt_qtl_symbol: set publication QTL symbol (if any) as synonym
    setSynonym($dbh, $qtl_id, 'expt_qtl_symbol', $fields);
    
#print "  attach favorable allele source\n";
    # favorable_allele_source
    attachFavorableAlleleSource($dbh, $qtl_id, $fields);

#print "  attach treatment\n";
    # treatment
    loadFeatureprop($dbh, $qtl_id, 'treatment', 'QTL study treatment', $fields);

#print "  attach analysis method\n";
    # analysis_method
    loadFeatureprop($dbh, $qtl_id, 'analysis_method', 'QTL analysis method', $fields);
    
#print "  attach measurements\n";
    # load measurements via analysisfeature
    loadMeasurement($dbh, $qtl_id, 'lod', 'LOD', $fields);
    loadMeasurement($dbh, $qtl_id, 'likelihood_ratio', 'likelihood ratio', $fields);
    loadMeasurement($dbh, $qtl_id, 'marker_r2', 'marker R2', $fields);
    loadMeasurement($dbh, $qtl_id, 'total_r2', 'total R2', $fields);
    loadMeasurement($dbh, $qtl_id, 'additivity', 'additivity', $fields);

#print "  attach pub lg\n";
    # publication_lg
    loadFeatureprop($dbh, $qtl_id, 'publication_lg', 'linkage group name used in publication', $fields);
    
#print "  attach lg\n";
# TODO: a linkage group should be a feature!
#    # lg (is this already in via the map and position?)
#    loadFeatureprop($dbh, $qtl_id, 'lg', 'linkage group', $fields);
    
#print "  attach map position\n";
    # set position (featurepos + featureposprop
    my $lg_mapname = makeLinkageMapName($fields);
    my $mapset = $fields->{'map_name'};
    insertGeneticCoordinates($dbh, $qtl_name, $mapset, $lg_mapname, $fields);

    # set interval calculation method
    loadFeatureprop($dbh, $qtl_id, 'interval_calc_method', 'interval calculation method', $fields);
    
#print "  attach markers\n";
    # link to markers (nearest, flanking) via feature_relationship
    attachMarker($dbh, $qtl_id, 'nearest_marker', 'nearest marker', $fields);
    attachMarker($dbh, $qtl_id, 'flanking_marker_low', 'flanking marker low', $fields);
    attachMarker($dbh, $qtl_id, 'flanking_marker_high', 'flanking marker high', $fields);

#print "  attach comment\n";
    # comment
    loadFeatureprop($dbh, $qtl_id, 'comment', 'comments', $fields);
  }#each record

  print "Loaded $line_count QTL records\n\n";
}#loadQTLs



################################################################################
################################################################################
################################################################################

sub setQTLRecord {
  my ($dbh, $qtl_id, $fields) = @_;
  my ($sql, $sth, $row);
  
  my $organism_id = getOrganismID($dbh, $fields->{'specieslink_abv'}, $line_count);
  my $qtl_name    = makeQTLName($fields);
  
  if ($qtl_id) {
    $sql = "
      UPDATE chado.feature
      SET 
        organism_id=$organism_id,
        name = '$qtl_name',
        uniquename = '$fields->{'specieslink_abv'}:$qtl_name',
        type_id = (SELECT cvterm_id FROM chado.cvterm 
                   WHERE name = 'QTL'
                         AND cv_id=(SELECT cv_id FROM chado.cv 
                                    WHERE name='sequence'))
      WHERE feature_id=$qtl_id";
     logSQL($dataset_name, "$line_count: $sql");
     $sth = doQuery($dbh, $sql);
  }
  else {
    $sql = "
      INSERT INTO chado.feature
        (organism_id, name, uniquename, type_id)
      VALUES
        ($organism_id,
         '$qtl_name',
         '$fields->{'specieslink_abv'}:$qtl_name',
         (SELECT cvterm_id FROM chado.cvterm 
          WHERE name = 'QTL'
            AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='sequence')))
      RETURNING feature_id";
     logSQL($dataset_name, "$line_count: $sql");
     $sth = doQuery($dbh, $sql);
     $row = $sth->fetchrow_hashref;
     $qtl_id = $row->{'feature_id'};
   }
   
   return $qtl_id;
}#setQTLRecord


sub cleanDependants {
  my ($dbh, $qtl_id) = @_;
  my ($sql, $sth);
  
  $sql = "DELETE FROM feature_project WHERE feature_id=$qtl_id";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);
  
  $sql = "DELETE FROM feature_stock WHERE feature_id=$qtl_id";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);

  $sql = "DELETE FROM feature_relationship WHERE subject_id=$qtl_id OR object_id=$qtl_id";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);

  $sql = "DELETE FROM feature_cvterm WHERE feature_id=$qtl_id";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);

  $sql = "DELETE FROM featurepos WHERE feature_id=$qtl_id";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);

  $sql = "DELETE FROM featureprop WHERE feature_id=$qtl_id";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);

  $sql = "DELETE FROM analysisfeature WHERE feature_id=$qtl_id";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);

  $sql = "DELETE FROM feature_synonym WHERE feature_id=$qtl_id";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);
}#cleanDependants


sub attachExperiment {
  my ($dbh, $feature_id, $fields) = @_;
  
#TODO: there may be more than one of these: increase rank if need be
    $sql = "
      INSERT INTO chado.feature_project
        (feature_id, project_id, rank)
      VALUES
        ($feature_id,
         (SELECT project_id FROM chado.project 
          WHERE name='$fields->{'qtl_experimentlink_name'}'),
         0)";
    logSQL($dataset_name, "$line_count: $sql");
    $sth = doQuery($dbh, $sql);
}#attachExperiment


sub attachFavorableAlleleSource {
  my ($dbh, $feature_id, $fields) = @_;
  my ($sql, $sth, $row);
       
  if (!$fields->{'favorable_allele_source'} 
        || $fields->{'favorable_allele_source'} eq '' 
        || $fields->{'favorable_allele_source'} eq 'NULL') {
    return;
  }
  
  $sql = "
     INSERT INTO chado.feature_stock
       (feature_id, stock_id, type_id)
     VALUES
       ($feature_id,
        (SELECT stock_id FROM chado.stock 
         WHERE name='$fields->{'favorable_allele_source'}'),
        (SELECT cvterm_id FROM chado.cvterm 
         WHERE name='favorable allele source'
           AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='local')))";
#print "$line_count: $sql\n";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);
}#attachFavorableAlleleSource


sub attachMarker {
  my ($dbh, $feature_id, $fieldname, $relationship, $fields) = @_;
  my ($sql, $sth, $row);
#print "attach marker in field '$fieldname'.\n" . Dumper($fields);

  if (!$fields->{$fieldname} || $fields->{$fieldname} eq '' 
        || $fields->{$fieldname} eq 'NULL') {
    return;
  }
  
  my $unique_marker_name = "$fields->{$fieldname}-$fields->{'specieslink_abv'}";
  
  # check for existing marker
  my $marker_id = 0;
  $sql = "SELECT feature_id FROM chado.feature WHERE uniquename='$unique_marker_name'";
#print "$sql\n";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);
  if (!($row=$sth->fetchrow_hashref)) {
=cut
    print "The marker $fields->{$fieldname} is not in the database. Should it be added? (y/n) ";
    my $ui =  <STDIN>;
    chomp ($ui);
=cut
my $ui = 'y';
    if (!($ui =~ /y.*/)) {
      # kill the whole process here
      print "\nQuitting process. No records will be inserted.\n";
      exit;
    }
    else {
      # insert a stub record for this marker
      my $organism_id = getOrganismID($dbh, $fields->{'specieslink_abv'}, $line_count);
      $sql = "
        INSERT INTO chado.feature
          (organism_id, name, uniquename, type_id)
        VALUES
          ($organism_id, '$unique_marker_name', '$unique_marker_name',
           (SELECT cvterm_id FROM chado.cvterm 
            WHERE name='genetic_marker'
              AND cv_id = (SELECT cv_id FROM chado.cv WHERE name='sequence')))
         RETURNING feature_id";
#print "$sql\n";
      logSQL($dataset_name, "line_count:$sql");
      $sth = doQuery($dbh, $sql);
      $row = $sth->fetchrow_hashref;
      $marker_id = $row->{'feature_id'};
    }
  }
  
  if ($marker_id > 0) {
    $sql = "
       INSERT INTO chado.feature_relationship
         (subject_id, type_id, object_id, value, rank)
       VALUES
         ($feature_id,
          (SELECT cvterm_id FROM chado.cvterm 
           WHERE name='$relationship' 
             AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='local')),
          $marker_id, '', 0)";
#print "$line_count: $sql\n";
    logSQL($dataset_name, "$line_count: $sql");
    $sth = doQuery($dbh, $sql);
  }
}#attachMarker


sub attachTrait {
  my ($dbh, $feature_id, $fields) = @_;
  my ($sql, $sth, $row);
  
  my $pub_id = getPubFromExperiment($dbh, $fields->{'qtl_experimentlink_name'});
  
  my $trait_id = getTrait($dbh, $fields->{'qtl_symbol'});
  if ($trait_id == 0) {
    # fatal error
    print "ERROR: unable to find QTL symbol '$fields->{'qtl_symbol'}'\n";
    exit;
  }
  
  $sql = "
    INSERT INTO chado.feature_cvterm
      (feature_id, cvterm_id, pub_id)
    VALUES
      ($feature_id, $trait_id, $pub_id)
    RETURNING feature_cvterm_id";
  logSQL($dataset_name, "$line_count: $sql");
#print "$line_count: $sql\n";
  $sth = doQuery($dbh, $sql);
  $row = $sth->fetchrow_hashref;
  my $feature_cvterm_id = $row->{'feature_cvterm_id'};
  
  $sql = "
    INSERT INTO chado.feature_cvtermprop
         (feature_cvterm_id, type_id, value, rank)
       VALUES
         ($feature_cvterm_id, 
          (SELECT cvterm_id FROM chado.cvterm 
           WHERE name='QTL symbol' 
             AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='local')),
          '', 
          0)";
#print "$line_count: $sql\n";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);
}#attachTrait


sub getPubFromExperiment {
  my ($dbh, $experiment) = @_;
  my ($sql, $sth, $row);
  
  if ($experiment && $experiment ne '' && $experiment ne 'NULL') {
    $sql = "
      SELECT P.pub_id
      FROM chado.pub P
        INNER JOIN chado.project_pub PP ON PP.pub_id=P.pub_id
        INNER JOIN chado.project E 
          ON E.project_id=PP.project_id AND E.name='$experiment'";
    logSQL($dataset_name, "$line_count: $sql");
    $sth = doQuery($dbh, $sql);
    if ($row = $sth->fetchrow_hashref) {
      return $row->{'pub_id'};
    }
    else {
      print "ERROR: unable to find publication associated with experiment [$experiment]\n";
      exit;
    }
  }
}#getPubFromExperiment


sub getSynonym {
  my ($dbh, $synonym) = @_;
  my ($sql, $sth, $row);
  $sql = "
    SELECT synonym_id FROM chado.synonym 
    WHERE name='$synonym' 
      AND type_id=(SELECT cvterm_id FROM chado.cvterm
                   WHERE name='symbol'
                     AND cv_id=(SELECT cv_id FROM chado.cv 
                                WHERE name='feature_property'))";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);
  if ($row = $sth->fetchrow_hashref) {
    return $row->{'synonym_id'};
  }
  else {
    return 0;
  }
}#getSynonym


sub insertGeneticCoordinates {
  my ($dbh, $qtl_name, $mapset, $lg_mapname, $fields) = @_;
  my ($sql, $sth, $row);
  
  return if (!$mapset || !$lg_mapname
              || (!$fields->{'left_end'} && !($fields->{'right_end'})));
  
  # Get id for mapset
  my $mapset_id = getMapSetID($dbh, $mapset);

  # Quit if no mapset
  if ($mapset_id == 0) {
    return;
  }
#print "insertGeneticCoordinates(): Got mapset id: $mapset_id\n";

  $sql = "
    INSERT INTO chado.featurepos
      (featuremap_id, feature_id, map_feature_id, mappos)
    VALUES
      ($mapset_id,
       (SELECT feature_id FROM chado.feature WHERE name='$qtl_name'),
       (SELECT feature_id FROM chado.feature WHERE name='$lg_mapname'),
       $fields->{'left_end'})
    RETURNING featurepos_id";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);
  $row = $sth->fetchrow_hashref;
  my $featurepos_id = $row->{'featurepos_id'};
  
  $sql = "
    INSERT INTO chado.featureposprop
      (featurepos_id, type_id, value, rank)
    VALUES
      ($featurepos_id,
       (SELECT cvterm_id FROM chado.cvterm 
        WHERE name='start coordinate'
          AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='local')),
       '', 1)";
#print "$line_count: $sql\n";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);
  
  $sql = "
    INSERT INTO chado.featurepos
      (featuremap_id, feature_id, map_feature_id, mappos)
    VALUES
      ($mapset_id,
       (SELECT feature_id FROM chado.feature WHERE name='$qtl_name'),
       (SELECT feature_id FROM chado.feature WHERE name='$lg_mapname'),
       $fields->{'right_end'})
    RETURNING featurepos_id";
#print "$line_count: $sql\n";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);
  $row = $sth->fetchrow_hashref;
  $featurepos_id = $row->{'featurepos_id'};

  $sql = "
    INSERT INTO chado.featureposprop
     (featurepos_id, type_id, value, rank)
    VALUES
     ($featurepos_id,
      (SELECT cvterm_id FROM chado.cvterm 
       WHERE name='stop coordinate'
         AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='local')),
      '', 1)";
#print "$line_count: $sql\n";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);
}#insertGeneticCoordinates


sub loadFeatureprop {
  my ($dbh, $feature_id, $fieldname, $propname, $fields) = @_;
#print "attach property in field '$fieldname' of type '$propname'.\n" . Dumper($fields);
  
  if (!$fields->{$fieldname} || $fields->{$fieldname} eq '' 
        || $fields->{$fieldname} eq 'NULL') {
    return;
  }
  
  $sql = "
    INSERT INTO chado.featureprop
     (feature_id, type_id, value, rank)
    VALUES
     ($feature_id,
      (SELECT cvterm_id FROM chado.cvterm 
       WHERE name='$propname'
         AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='local')),
      '$fields->{$fieldname}',
      0)";
#print "$line_count: $sql\n";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);
}#loadFeatureprop


sub loadMeasurement {
  my ($dbh, $feature_id, $fieldname, $analysis, $fields) = @_;
  my ($sql, $sth);
#print "attach analysis in field '$fieldname' of type $analysis.\n" . Dumper($fields);
  
  if (!$fields->{$fieldname} || $fields->{$fieldname} eq '' 
        || $fields->{$fieldname} eq 'NULL') {
    return;
  }
  
  $sql = "
       INSERT INTO chado.analysisfeature
         (feature_id, analysis_id, rawscore)
       VALUES
         ($feature_id,
          (SELECT analysis_id FROM chado.analysis WHERE name='$analysis'),
          $fields->{$fieldname})";
#print "$line_count: $sql\n";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);
}#loadMeasurement


sub setSynonym {
  my ($dbh, $feature_id, $fieldname, $fields) = @_;
  my ($sql, $sth, $row);

  if (!$fields->{$fieldname} || $fields->{$fieldname} eq '' 
        || $fields->{$fieldname} eq 'NULL') {
    return;
  }
  
  my $synonym_id = getSynonym($dbh, $fields->{$fieldname});
  if (!$synonym_id) {
    $sql = "
      INSERT INTO chado.synonym
       (name, synonym_sgml, type_id)
      VALUES
       ('$fields->{$fieldname}', '$fields->{$fieldname}',
        (SELECT cvterm_id FROM chado.cvterm
         WHERE name='symbol'
           AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='feature_property')))
      RETURNING synonym_id";
  #print "$line_count: $sql\n";
    logSQL($dataset_name, "$line_count: $sql");
    $sth = doQuery($dbh, $sql);
    $row = $sth->fetchrow_hashref;
    $synonym_id = $row->{'synonym_id'};
  }
  
  my $pub_id = getPubFromExperiment($dbh, $fields->{'qtl_experimentlink_name'});
  $sql = "
    INSERT INTO chado.feature_synonym 
      (synonym_id, feature_id, pub_id)
    VALUES
      ($synonym_id, $feature_id, $pub_id)";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);
}#setSynonym


