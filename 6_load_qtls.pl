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
#  08/26/14  eksc  modified for spreadsheet changes; better use of cvs.


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

  # hard-coded worksheet constants
  my $qtl_worksheet       = 'QTL';
  my $species_fld         = 'specieslink_abv';
  my $qtl_expt_fld        = 'qtl_experimentlink_name';
  my $expt_trait_name_fld = 'expt_trait_name';
  my $expt_trait_desc_fld = 'expt_trait_description';
  my $trait_unit_fld      = 'trait_unit';
  my $qtl_symbol_fld      = 'qtl_symbol';
  my $qtl_identifier      = 'qtl_identifier'
  my $expt_qtl_symbol_fld = 'expt_qtl_symbol'
  my $fav_allele_fld      = 'favorable_allele_source';
  my $treatment_fld       = 'treatment';
  my $method_fld          = 'analysis_method';
  my $lod_fld             = 'lod';
  my $like_ratio_fld      = 'likelihood_ratio';
  my $marker_r2_fld       = 'marker_r2';
  my $total_r2_fld        = 'total_r2';
  my $additivity_fld      = 'additivity';
  my $comment_fld         = 'comment';

  my $pos_worksheet       = 'MAP_POSITION';
  my $map_name_fld        = 'map_name';	
  my $pub_lg_fld          = 'publication_lg';
  my $lg_fld              = 'lg';
  my $left_end_fld        = 'left_end';
  my $right_end_fld       = 'right_end';
  my $QTL_peak_fld        = 'QTL_peak';
  my $int_calc_meth_fld   = 'interval_calc_method';
  my $nearest_mrkr_fld    = 'nearest_marker';
  my $flank_mrkr_low_fld  = 'flanking_marker_low';
  my $flank_mrkr_high_fld = 'flanking_marker_high';
  
  my $line_count;   # current line in file to load
  my %skip_QTLs;    # list of QTLs to skip (don't load map positions)
  
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
    loadQTLpos($dbh);
    
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
  
  my $table_file = "$input_dir/$worksheet";
  print "Loading/verifying $table_file...\n";
  
  my @records = readFile($table_file);
  
  my ($skip, $skip_all, $update, $update_all);

  $line_count = 0;
  foreach $fields (@records) {
    $line_count++;
    
    # create parent record: feature
    #   specieslink_name, qtl_symbol, qtl_identifer
    my $qtl_name    = makeQTLName($fields);
#print "$line_count: $qtl_name\n";
    
    my $qtl_id;
    if ($qtl_id = getQTLid($dbh, $qtl_name)) {
      if ($skip_all) {
        $skip_QTLs{$qtl_name} = 1;
        next;
      }
      if ($update_all) {
        cleanDependants($dbh, $qtl_id);
      }
      else {
        my $prompt =  "$line_count: qtl_symbol ($fields->{$qtl_symbol_fld}) is already loaded.";
        ($skip, $skip_all, $update, $update_all) = checkUpdate($prompt);
        
        if ($skip || $skip_all) {
          $skip_QTLs{$qtl_name} = 1;
          next;
        }
        
        if ($update || $update_all) {
          cleanDependants($dbh, $qtl_id);
        }
      }#get user input
    }#qtl exists
    
    # Create/update QTL record
    $qtl_id = setQTLRecord($dbh, $qtl_id, $fields);
    
#print "  attach experiment\n";
    # link to study (project) via new table feature_project
    attachExperiment($dbh, $qtl_id, $fields);
         
#print "  attach experiment trait name\n";
    # trait_name
    loadFeatureprop($dbh, $qtl_id, $expt_trait_name_fld, 
                    'Experiment Trait Name', 'feature_property', $fields);
    
#print "  attach experiment trait description\n";
    # trait_description
    loadFeatureprop($dbh, $qtl_id, $expt_trait_desc_fld, 
                    'Experiment Trait Description', 'feature_property', $fields);
    
#print "  attach trait unit\n";
    # trait_unit
    loadFeatureprop($dbh, $qtl_id, $trait_unit_fld, 
                    'Trait Unit', 'feature_property', $fields);

#print "  attach qtl symbol\n";
    # link to trait (qtl_symbol) via feature_cvterm and feature_cvtermprop
    attachTrait($dbh, $qtl_id, $fields);
    
#print "  attach qtl identifier\n";
    # qtl_identifier
    loadFeatureprop($dbh, $qtl_id, $qtl_identifier, 
                    'QTL Identifier', 'feature_property', $fields);
    
#print "  attach pub qtl symbol\n";
    # expt_qtl_symbol: set publication QTL symbol (if any) as synonym
    setSynonym($dbh, $qtl_id, $expt_qtl_symbol_fld, $fields);
    
#print "  attach favorable allele source\n";
    # favorable_allele_source
    attachFavorableAlleleSource($dbh, $qtl_id, $fields);

#print "  attach treatment\n";
    # treatment
    loadFeatureprop($dbh, $qtl_id, $treatment_fld, 
                    'QTL Study Treatment', 'feature_property', $fields);

#print "  attach analysis method\n";
    # analysis_method
    loadFeatureprop($dbh, $qtl_id, $method_fld, 
                    'QTL analysis method', 'feature_property', $fields);
    
#print "  attach measurements\n";
    # load measurements via analysisfeature
    loadMeasurement($dbh, $qtl_id, $lod_fld, 'LOD', $fields);
    loadMeasurement($dbh, $qtl_id, $like_ratio_fld, 'likelihood ratio', $fields);
    loadMeasurement($dbh, $qtl_id, $marker_r2_fld, 'marker R2', $fields);
    loadMeasurement($dbh, $qtl_id, $total_r2_fld, 'total R2', $fields);
    loadMeasurement($dbh, $qtl_id, $additivity_fld, 'additivity', $fields);

#print "  attach comment\n";
    # comment
    loadFeatureprop($dbh, $qtl_id, $comment_fld, 'comments', 
                    'feature_property', $fields);
  }#each record

  print "Loaded $line_count QTL records\n\n";
}#loadQTLs


sub loadQTLpos {
  my $dbh = $_[0];
  my ($fields, $sql, $sth, $row);
  
  my $table_file = "$input_dir/$worksheet";
  print "Loading/verifying $table_file...\n";
  
  my @records = readFile($table_file);
  
  my ($skip, $skip_all, $update, $update_all);

  $line_count = 0;
  foreach $fields (@records) {
    $line_count++;
    
    # create parent record: feature
    #   specieslink_name, qtl_symbol, qtl_identifer
    my $qtl_name    = makeQTLName($fields);
print "$line_count: $qtl_name\n";
    
    # check if this QTL should be skipped
    next if ($skip_QTLs{$qtl_name});
    
    my $qtl_id = getQTLid($dbh, $qtl_name));
    if (!$qtl_id) {
      # something is horribly wrong!
      print "ERROR: can't find QTL record for [$qtl_name]\n\n";
      exit;
    }


# NEW: QTL_peak

#print "  attach pub lg\n";
    # publication_lg
    loadFeatureprop($dbh, $qtl_id, $pub_lg_fld, 
                    'publication linkage group', 'feature_property', $fields);
    
#print "  attach lg\n";
# TODO: a linkage group should be a feature!
#    # lg (is this already in via the map and position?)
    loadFeatureprop($dbh, $qtl_id, 'lg', 
                    'linkage_group', 'sequence', $fields);
    
#print "  attach map position\n";
    # set position (featurepos + featureposprop
    my $lg_mapname = makeLinkageMapName($fields);
    my $mapset = $fields->{$map_name_fld};
    insertGeneticCoordinates($dbh, $qtl_name, $mapset, $lg_mapname, $fields);

    # set interval calculation method
    loadFeatureprop($dbh, $qtl_id, $int_calc_meth_fld, 
                    'Interval Calculation Method', 'feature_property', $fields);
    
#print "  attach markers\n";
    # link to markers (nearest, flanking) via feature_relationship
    attachMarker($dbh, $qtl_id, $nearest_mrkr_fld, 'Nearest Marker', $fields);
    attachMarker($dbh, $qtl_id, $flank_mrkr_low_fld, 'Flanking Marker Low', $fields);
    attachMarker($dbh, $qtl_id, $flank_mrkr_high_fld, 'Flanking Marker High', $fields);

    # change rank because there may already be a comment for this QTL
    loadFeatureprop($dbh, $qtl_id, $comment_fld, 'comments', 
                    'feature_property', $fields, 2);  # 2 = rank
}#loadQTLpos


################################################################################
################################################################################
################################################################################

sub setQTLRecord {
  my ($dbh, $qtl_id, $fields) = @_;
  my ($sql, $sth, $row);
  
  my $organism_id = getOrganismID($dbh, $fields->{$species_fld}, $line_count);
  my $qtl_name    = makeQTLName($fields);
  
  if ($qtl_id) {
    $sql = "
      UPDATE chado.feature
      SET 
        organism_id=$organism_id,
        name = '$qtl_name',
        uniquename = '$fields->{$species_fld}:$qtl_name',
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
         '$fields->{$species_fld}:$qtl_name',
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
          WHERE name='$fields->{$qtl_expt_fld}'),
         0)";
    logSQL($dataset_name, "$line_count: $sql");
    $sth = doQuery($dbh, $sql);
}#attachExperiment


sub attachFavorableAlleleSource {
  my ($dbh, $feature_id, $fields) = @_;
  my ($sql, $sth, $row);
       
  if (!$fields->{$fav_allele_fld} 
        || $fields->{$fav_allele_fld} eq '' 
        || $fields->{$fav_allele_fld} eq 'NULL') {
    return;
  }
  
  $sql = "
     INSERT INTO chado.feature_stock
       (feature_id, stock_id, type_id)
     VALUES
       ($feature_id,
        (SELECT stock_id FROM chado.stock 
         WHERE name='$fields->{$fav_allele_fld}'),
        (SELECT cvterm_id FROM chado.cvterm 
         WHERE name='Favorable Allele Source'
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
  
  my $unique_marker_name = "$fields->{$fieldname}-$fields->{$species_fld}";
  
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
      my $organism_id = getOrganismID($dbh, $fields->{$species_fld}, $line_count);
      $sql = "
        INSERT INTO chado.feature
          (organism_id, name, uniquename, type_id)
        VALUES
          ($organism_id, '$fields->{$fieldname}', '$unique_marker_name',
           (SELECT cvterm_id FROM chado.cvterm 
            WHERE name='genetic_marker'
              AND cv_id = (SELECT cv_id FROM chado.cv WHERE name='sequence')))
         RETURNING feature_id";
#print "$sql\n";
      logSQL($dataset_name, "line_count:$sql");
      $sth = doQuery($dbh, $sql);
      $row = $sth->fetchrow_hashref;
      $marker_id = $row->{'feature_id'};
#print "returned id $marker_id\n\n";
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
             AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='feature_relationship')),
          $marker_id, '', 0)";
#print "$line_count: $sql\n";
    logSQL($dataset_name, "$line_count: $sql");
    $sth = doQuery($dbh, $sql);
#print "returned $sth\n\n";
  }
}#attachMarker


sub attachTrait {
  my ($dbh, $feature_id, $fields) = @_;
  my ($sql, $sth, $row);
  
  my $pub_id = getPubFromExperiment($dbh, $fields->{$qtl_expt_fld});
  
  my $trait_id = getTrait($dbh, $fields->{$qtl_symbol_fld});
  if ($trait_id == 0) {
    # fatal error
    print "ERROR: unable to find QTL symbol '$fields->{$qtl_symbol_fld}'\n";
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
           WHERE name='QTL Symbol' 
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
              || (!$fields->{$left_end_fld} && !($fields->{$right_end_fld})));
  
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
       $fields->{$left_end_fld})
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
        WHERE name='start'
          AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='featurepos_property')),
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
       $fields->{$right_end_fld})
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
       WHERE name='stop'
         AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='featurepos_property')),
      '', 1)";
#print "$line_count: $sql\n";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);
}#insertGeneticCoordinates


sub loadFeatureprop {
  my ($dbh, $feature_id, $fieldname, $propname, $cv, $fields, $rank=1) = @_;
  
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
         AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='$cv')),
      '$fields->{$fieldname}',
      $rank)";
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
           AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='synonym_type')))
      RETURNING synonym_id";
#print "$line_count: $sql\n";
    logSQL($dataset_name, "$line_count: $sql");
    $sth = doQuery($dbh, $sql);
    $row = $sth->fetchrow_hashref;
    $synonym_id = $row->{'synonym_id'};
  }
  
  my $pub_id = getPubFromExperiment($dbh, $fields->{$qtl_expt_fld});
  $sql = "
    INSERT INTO chado.feature_synonym 
      (synonym_id, feature_id, pub_id)
    VALUES
      ($synonym_id, $feature_id, $pub_id)";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);
}#setSynonym


