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
  my %qi  = getSSInfo('QTL');
  my %mpi = getSSInfo('MAP_POSITIONS');

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
  
  my $table_file = "$input_dir/$qi{'worksheet'}.txt";
  print "Loading/verifying $table_file...\n";
  
  my @records = readFile($table_file);
  
  my ($skip, $skip_all, $update, $update_all);

  $line_count = 0;
  foreach $fields (@records) {
    $line_count++;
    
    # create full QTL name
    my $qtl_symbol     = $fields->{$qi{'qtl_symbol_fld'}};
    my $qtl_identifier = $fields->{$qi{'qtl_identifier_fld'}};
    my $qtl_name       = makeQTLName($qtl_symbol, $qtl_identifier);
print "\n$line_count: $qtl_name\n";

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
        my $prompt =  "$line_count: This QTL ($qtl_name) is already loaded.";
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
    loadFeatureprop($dbh, $qtl_id, $fields->{$qi{'expt_trait_name_fld'}}, 
                    'Experiment Trait Name', 'feature_property', $fields);
    
#print "  attach experiment trait description\n";
    # trait_description
    loadFeatureprop($dbh, $qtl_id, $fields->{$qi{'expt_trait_desc_fld'}}, 
                    'Experiment Trait Description', 'feature_property', $fields);
    
#print "  attach trait unit\n";
    # trait_unit
    loadFeatureprop($dbh, $qtl_id, $fields->{$qi{'trait_unit_fld'}}, 
                    'Trait Unit', 'feature_property', $fields);

#print "  attach qtl symbol\n";
    # link to trait (qtl_symbol) via feature_cvterm and feature_cvtermprop
    attachTrait($dbh, $qtl_id, $fields);
    
#print "  attach qtl identifier\n";
    # qtl_identifier
    loadFeatureprop($dbh, $qtl_id, $fields->{$qi{'qtl_identifier_fld'}}, 
                    'QTL Identifier', 'feature_property', $fields);
    
#print "  attach pub qtl symbol\n";
    # expt_qtl_symbol: set publication QTL symbol (if any) as synonym
    setSynonym($dbh, $qtl_id, $fields->{$qi{'expt_qtl_symbol_fld'}}, $fields);
    
#print "  attach favorable allele source\n";
    # favorable_allele_source
    attachFavorableAlleleSource($dbh, $qtl_id, $fields);

#print "  attach treatment\n";
    # treatment
    loadFeatureprop($dbh, $qtl_id, $fields->{$qi{'treatment_fld'}}, 
                    'QTL Study Treatment', 'feature_property', $fields);

#print "  attach analysis method\n";
    # analysis_method
    loadFeatureprop($dbh, $qtl_id, $fields->{$qi{'method_fld'}}, 
                    'QTL Analysis Method', 'feature_property', $fields);
    
#print "  attach markers\n";
    # link to markers (nearest, flanking) via feature_relationship
    attachMarker($dbh, $qtl_id, $fields->{$mpi{'nearest_mrkr_fld'}}, 
                 'Nearest Marker', $fields);
    attachMarker($dbh, $qtl_id, $fields->{$mpi{'flank_mrkr_low_fld'}}, 
                 'Flanking Marker Low', $fields);
    attachMarker($dbh, $qtl_id, $fields->{$mpi{'flank_mrkr_high_fld'}}, 
                 'Flanking Marker High', $fields);

#print "  attach measurements\n";
    # load measurements via analysisfeature
    loadMeasurement($dbh, $qtl_id, $fields->{$qi{'lod_fld'}}, 
                    'LOD', $fields);
    loadMeasurement($dbh, $qtl_id, $fields->{$qi{'like_ratio_fld'}}, 
                    'likelihood ratio', $fields);
    loadMeasurement($dbh, $qtl_id, $fields->{$qi{'marker_r2_fld'}}, 
                    'marker R2', $fields);
    loadMeasurement($dbh, $qtl_id, $fields->{$qi{'total_r2_fld'}}, 
                    'total R2', $fields);
    loadMeasurement($dbh, $qtl_id, $fields->{$qi{'additivity_fld'}}, 
                    'additivity', $fields);

#print "  attach comment\n";
    # comment
    loadFeatureprop($dbh, $qtl_id, $fields->{$qi{'comment_fld'}}, 
                    'comment', 'feature_property', $fields);
  }#each record

  print "Loaded $line_count QTL records\n\n";
}#loadQTLs


sub loadQTLpos {
  my $dbh = $_[0];
  my ($fields, $sql, $sth, $row);
  
  my $table_file = "$input_dir/$mpi{'worksheet'}.txt";
  print "Loading/verifying $table_file...\n";
  
  my @records = readFile($table_file);
  
  my ($skip, $skip_all, $update, $update_all);

  $line_count = 0;
  foreach $fields (@records) {
    $line_count++;
    
    # create parent record: feature
    #   specieslink_name, qtl_symbol, qtl_identifer
  my $qtl_symbol     = $fields->{$qi{'qtl_symbol_fld'}};
  my $qtl_identifier = $fields->{$qi{'qtl_identifier_fld'}};
  my $qtl_name       = makeQTLName($qtl_symbol, $qtl_identifier);
print "\n$line_count: $qtl_name\n";
    
    # check if this QTL should be skipped
    next if ($skip_QTLs{$qtl_name});
    
    my $qtl_id = getQTLid($dbh, $qtl_name);
    if (!$qtl_id) {
      # something is horribly wrong!
      print "ERROR: can't find QTL record for [$qtl_name]\n\n";
      exit;
    }


# NEW: QTL_peak

#print "  attach pub lg\n";
    # publication_lg
    loadFeatureprop($dbh, $qtl_id, $fields->{$mpi{'pub_lg_fld'}}, 
                    'Publication Linkage Group', 'feature_property', $fields);
    
#print "  attach lg\n";
#    # lg (is this already in via the map and position?)
#    loadFeatureprop($dbh, $qtl_id, $fields->{$mpi{'lg_fld'}}, 
#                    'linkage_group', 'sequence', $fields);
    
#print "  attach map position\n";
    # set position (featurepos + featureposprop
    my $ms_name  = $fields->{$mpi{'map_name_fld'}};
    my $lg         = $fields->{$mpi{'lg_fld'}};
    my $lg_mapname = makeLinkageMapName($ms_name, $lg);
    insertGeneticCoordinates($dbh, $qtl_id, $qtl_name, $ms_name, $lg_mapname, $fields);

    # change rank because there may already be a comment for this QTL
    loadFeatureprop($dbh, $qtl_id, $fields->{$mpi{'comment_fld'}}, 'comments', 
                    'feature_property', $fields, 2);  # 2 = rank
  }#each record
}#loadQTLpos


################################################################################
################################################################################
################################################################################

sub setQTLRecord {
  my ($dbh, $qtl_id, $fields) = @_;
  my ($sql, $sth, $row);
  
  my $species     = $fields->{$qi{'species_fld'}};
print "species: $species\n";
  my $organism_id = getOrganismID($dbh, $species, $line_count);
  my $qtl_symbol     = $fields->{$qi{'qtl_symbol_fld'}};
  my $qtl_identifier = $fields->{$qi{'qtl_identifier_fld'}};
  my $qtl_name       = makeQTLName($qtl_symbol, $qtl_identifier);
print "QTL name: $qtl_name\n";
  
  if ($qtl_id) {
    $sql = "
      UPDATE chado.feature
      SET 
        organism_id=$organism_id,
        name = '$qtl_name',
        uniquename = '$species:$qtl_name',
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
         '$species:$qtl_name',
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

  $sql = "DELETE FROM featureloc WHERE feature_id=$qtl_id";
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
  my ($sql, $sth);
  
  my $expt = $fields->{$qi{'qtl_expt_fld'}};
  $sql = "
    INSERT INTO chado.feature_project
      (feature_id, project_id, rank)
    VALUES
      ($feature_id,
       (SELECT project_id FROM chado.project 
        WHERE name='$expt'),
       0)";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);
}#attachExperiment


sub attachFavorableAlleleSource {
  my ($dbh, $feature_id, $fields) = @_;
  my ($sql, $sth, $row);
  
  my $fav_allele = $fields->{$qi{'fav_allele_fld'}};
#print "favorable allele source: $fav_allele\n";
  if (isNull($fav_allele)) {
    return;
  }
  
  # Make sure there is a stock record for the favorable allele source
  my $stock_id;
  $sql = "
     SELECT stock_id FROM chado.stock 
     WHERE name='$fav_allele'";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = $dbh->prepare($sql);
  $sth->execute();
  if ($row=$sth->fetchrow_hashref) {
    $stock_id = $row->{'stock_id'};
  }
  else {
    my $species = $fields->{$qi{'species_fld'}};
    my $organism_id = getOrganismID($dbh, $species);
    $sql = "
      INSERT INTO chado.stock
        (organism_id, name, uniquename, description, type_id)
      VALUES
        ($organism_id, '$fav_allele', '$species:$fav_allele', '',
         (SELECT cvterm_id FROM cvterm 
          WHERE name='Cultivar'
                AND cv_id=(SELECT cv_id FROM cv WHERE name='stock_type')))
      RETURNING stock_id";
    logSQL($dataset_name, "$line_count: $sql");
    $sth = $dbh->prepare($sql);
    $sth->execute();
    $row = $sth->fetchrow_hashref;
    $stock_id = $row->{'stock_id'};
  }
#print "got stock record: $stock_id\n";
  
  $sql = "
     INSERT INTO chado.feature_stock
       (feature_id, stock_id, type_id)
     VALUES
       ($feature_id, $stock_id,
        (SELECT cvterm_id FROM chado.cvterm 
         WHERE name='Favorable Allele Source'
           AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='local')))";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);
}#attachFavorableAlleleSource


sub attachMarker {
  my ($dbh, $feature_id, $markername, $relationship, $fields) = @_;
  my ($sql, $sth, $row);
print "attach marker $markername\n";
#print "attach marker $markername'.\n" . Dumper($fields);

  if (isNull($markername)) {
    return;
  }
  
  my $species = $fields->{$qi{'species_fld'}};
  my $unique_marker_name = "$markername-$species";
  
  # check for existing marker
  my $marker_id = 0;
  $sql = "SELECT feature_id FROM chado.feature WHERE uniquename='$unique_marker_name'";
#print "$sql\n";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);
  if ($row=$sth->fetchrow_hashref) {
    $marker_id = $row->{'feature_id'};
  }
  else {
    # insert a stub record for this marker
    my $organism_id = getOrganismID($dbh, $species, $line_count);
    $sql = "
      INSERT INTO chado.feature
        (organism_id, name, uniquename, type_id)
      VALUES
        ($organism_id, '$markername', '$unique_marker_name',
         (SELECT cvterm_id FROM chado.cvterm 
          WHERE name='genetic_marker'
            AND cv_id = (SELECT cv_id FROM chado.cv WHERE name='sequence')))
       RETURNING feature_id";
#print "$sql\n";
    logSQL($dataset_name, "line_count: $sql");
    $sth = doQuery($dbh, $sql);
    $row = $sth->fetchrow_hashref;
    $marker_id = $row->{'feature_id'};
  }
print "Got marker id: $marker_id\n";
  
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
    logSQL($dataset_name, "$line_count: $sql");
    $sth = doQuery($dbh, $sql);
  }
}#attachMarker


sub attachTrait {
  my ($dbh, $feature_id, $fields) = @_;
  my ($sql, $sth, $row);
  
  my $pub_id = getPubFromExperiment($dbh, $fields->{$qi{'qtl_expt_fld'}});
  
  my $trait_id = getTrait($dbh, $fields->{$qi{'qtl_symbol_fld'}});
  if ($trait_id == 0) {
    # fatal error
    print "ERROR: unable to find QTL symbol $fields->{$qi{'qtl_symbol_fld'}}\n";
    exit;
  }
  
  $sql = "
    INSERT INTO chado.feature_cvterm
      (feature_id, cvterm_id, pub_id)
    VALUES
      ($feature_id, $trait_id, $pub_id)
    RETURNING feature_cvterm_id";
  logSQL($dataset_name, "$line_count: $sql");
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
  my ($dbh, $qtl_id, $qtl_name, $mapset, $lg_mapname, $fields) = @_;
  my ($sql, $sth, $row);
  
  my $left_end  = $fields->{$mpi{'left_end_fld'}};
  my $right_end = $fields->{$mpi{'right_end_fld'}};
  return if (isNull($mapset) || isNull($lg_mapname) 
              || (!$left_end && !($right_end)));
  
  # In order to tie QTL to specific linkage groups (there may be more than one),
  #    it will be necessary to place the QTL directly on the linkage group 
  #    rather than indirectly through featurepos as was done when a QTL was
  #    placed on only one linkage group
  # This means the location will need to be divided by 100 to get the correct
  #    cM value (ugh).
  $left_end = int($left_end*100);
  $right_end = int($right_end*100);
  my $srcfeature_id = getFeatureID($dbh, $lg_mapname);
  
  # increase rank if need be
  $sql = "
    SELECT rank FROM chado.featureloc WHERE feature_id=$qtl_id";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = $dbh->prepare($sql);
  $sth->execute();
  my $rank = ($row=$sth->fetchrow_hashref) ? $row->{'rank'}+1 : 0;
  
  $sql = "
    INSERT INTO chado.featureloc
      (feature_id, srcfeature_id, fmin, fmax, rank)
    VALUES
      ($qtl_id, $srcfeature_id, $left_end, $right_end, $rank)
    RETURNING featureloc_id";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = $dbh->prepare($sql);
  $sth->execute();
  my $featureloc_id = ($row=$sth->fetchrow_hashref) ? $row->{'featureloc_id'} : 0;

  $sql = "
    INSERT INTO chado.featurelocprop
      (featureloc_id, type_id, value, rank)
    VALUES
      ($featureloc_id,
       (SELECT cvterm_id FROM cvterm 
        WHERE name='Interval Calculation Method'
              AND cv_id=(SELECT cv_id FROM cv WHERE name='feature_property')),
       '$fields->{$mpi{'int_calc_meth_fld'}}',
       0
      )";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = $dbh->prepare($sql);
  $sth->execute();
}#insertGeneticCoordinates


sub loadFeatureprop {
  my ($dbh, $feature_id, $prop, $propname, $cv, $fields, $rank) = @_;
  my($sql, $sth, $row);
  
  if (isNull($prop)) {
    return;
  }
  
  $prop = $dbh->quote($prop);
  
  if (!$rank) { 
    $rank = 0;
  }
  
  $sql = "
    SELECT rank FROM chado.featureprop
    WHERE feature_id=$feature_id
          AND type_id=(SELECT cvterm_id FROM chado.cvterm 
                       WHERE name='$propname'
                       AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='$cv'))";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = $dbh->prepare($sql);
  $sth->execute();
  if ($row=$sth->fetchrow_hashref) {
#print "Found rank $row->{'rank'} for $feature_id and $propname\n";
    $rank = $row->{'rank'} + 1;
#print "  rank is now $rank\n";
  }
    
  $sql = "
    INSERT INTO chado.featureprop
     (feature_id, type_id, value, rank)
    VALUES
     ($feature_id,
      (SELECT cvterm_id FROM chado.cvterm 
       WHERE name='$propname'
         AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='$cv')),
      $prop,
      $rank)";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);
}#loadFeatureprop


sub loadMeasurement {
  my ($dbh, $feature_id, $measure, $analysis, $fields) = @_;
  my ($sql, $sth);
#print "attach analysis in field '$fieldname' of type $analysis.\n" . Dumper($fields);
  
  if (isNull($measure)) {
    return;
  }
  
  $sql = "
       INSERT INTO chado.analysisfeature
         (feature_id, analysis_id, rawscore)
       VALUES
         ($feature_id,
          (SELECT analysis_id FROM chado.analysis WHERE name='$analysis'),
          $measure)";
#print "$line_count: $sql\n";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);
}#loadMeasurement


sub setSynonym {
  my ($dbh, $feature_id, $synonym, $fields) = @_;
  my ($sql, $sth, $row);

  if (isNull($synonym)) {
    return;
  }
  
  my $synonym_id = getSynonym($dbh, $synonym);
  if (!$synonym_id) {
    $sql = "
      INSERT INTO chado.synonym
       (name, synonym_sgml, type_id)
      VALUES
       ('$synonym', '$synonym',
        (SELECT cvterm_id FROM chado.cvterm
         WHERE name='Symbol'
           AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='synonym_type')))
      RETURNING synonym_id";
#print "$line_count: $sql\n";
    logSQL($dataset_name, "$line_count: $sql");
    $sth = doQuery($dbh, $sql);
    $row = $sth->fetchrow_hashref;
    $synonym_id = $row->{'synonym_id'};
  }
  
  my $pub_id = getPubFromExperiment($dbh, $fields->{$qi{'qtl_expt_fld'}});
  $sql = "
    INSERT INTO chado.feature_synonym 
      (synonym_id, feature_id, pub_id)
    VALUES
      ($synonym_id, $feature_id, $pub_id)";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);
}#setSynonym


