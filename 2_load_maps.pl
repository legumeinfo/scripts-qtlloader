## file: load_maps.pl
#
# purpose: Load spreadsheet map data into a chado database
#
#          It is assumed that the .txt files have been verified.
#
# http://search.cpan.org/~capttofu/DBD-mysql-4.022/lib/DBD/mysql.pm
# http://search.cpan.org/~timb/DBI/DBI.pm
#
# history:
#  05/22/13  eksc  created
#  11/30/13  eksc  revised for significant revisions of map worksheets
#  08/26/14  eksc  revised with better use of CVs


  use strict;
  use DBI;
  use Encode;
  use Data::Dumper;
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
  
  # Get spreadsheet constants
  my %mci = getSSInfo('MAP_COLLECTIONS');
  my %mi  = getSSInfo('MAPS');
  my %mpi  = getSSInfo('MARKER_POSITION');
  
  # Used all over
  my ($table_file, $sql, $sth, $row, $count, @records, @fields, $cmd, $rv);
  my ($has_errors, $line_count);

  my $dataset_name = 'maps';
  
  # Holds map sets that are already in db
  my %existing_map_sets;

  # Holds linkage maps that are already in db
  my %existing_lg_maps;
  
  # Holds LIS map set links (for CMap)
  my %lis_map_sets;

  # Get connected
  my $dbh = connectToDB;

  # set default schema
  $sql = "SET SEARCH_PATH = chado";
  $sth = $dbh->prepare($sql);
  $sth->execute();

  # Use a transaction so that it can be rolled back if there are any errors
  eval {
    loadMapCollection($dbh, $mci{'worksheet'});
    loadLinkageMaps($dbh, $mi{'worksheet'});
    loadMarkerPositions($dbh, $mpi{'worksheet'});
    
#    $dbh->commit;   # commit the changes if we get this far
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

sub loadMapCollection {
  my ($dbh, $filename) = @_;
  my (%fields, $sql, $sth);
  
  $table_file = "$input_dir/$filename.txt";
  print "\n\nLoading $table_file...\n";
  
  my ($skip, $skip_all, $update, $update_all);

  @records = readFile($table_file);
  $line_count = 0;
  foreach my $fields (@records) {
    $line_count++;
    
    # map name = mapping population uniquename
    # Note that the mapping population stock record and the map record itself 
    #     will have the same name.
    my $mapname = $fields->{$mi{'map_name_fld'}};

    if (my $map_set_id = mapSetExists($dbh, $mapname)) {
      # map set exists
      next if ($skip_all);
      if ($update_all) {
          $existing_map_sets{$mapname} = $map_set_id;
          
          # remove dependent records; they will be re-inserted
          clearMapSetDependencies($dbh, $map_set_id, $fields);
      }
      else {
        my $prompt = "$line_count: map ($fields->{$mi{'map_name_fld'}} = $map_set_id) ";
        ($skip, $skip_all, $update, $update_all) = checkUpdate($prompt);
        
        next if ($skip || $skip_all);
        
        if ($update || $update_all) {
          $existing_map_sets{$mapname} = $map_set_id;
          
          # remove dependent records; they will be re-inserted
          clearMapSetDependencies($dbh, $map_set_id, $fields);
        }
      }#update_all not set
    }#map set exists

    # create featuremap record (base record for map)
    my $map_id = setMapSetRec($dbh, $fields);  # featuremap_id

    my @publink_citations = split ';', $fields->{$mci{'pub_fld'}};
    
    my $map_name = $fields->{$mci{'map_name_fld'}};
    if ($map_name =~ /^.*_x_.*$/i) {
      # create mapping population stock record if needed
      confirmStockRecord($dbh, $map_name, 
                         'Mapping Population', $fields);
      
      # create parent stock records if needed
      if ($fields->{$mci{'parent1_fld'}} 
          && $fields->{$mci{'parent1_fld'}} ne '' 
          && $fields->{$mci{'parent1_fld'}} ne 'NULL') {
         confirmStockRecord($dbh, $fields->{$mci{'parent1_fld'}}, 
                            'Cultivar', $fields);
         connectParent($dbh, $fields->{$mci{'parent1_fld'}}, 'Parent1', $fields);
      }
      if ($fields->{$mci{'parent2_fld'}} 
          && $fields->{$mci{'parent2_fld'}} ne '' 
          && $fields->{$mci{'parent2_fld'}} ne 'NULL') {
        confirmStockRecord($dbh, $fields->{$mci{'parent2_fld'}}, 
                           'Cultivar', $fields);
        connectParent($dbh, $fields->{$mci{'parent2_fld'}}, 'Parent2', $fields);
      }
    
      # attach mapping population to publication
      foreach my $publink_citation (@publink_citations) {
        $publink_citation =~ s/^\s//;
        $publink_citation =~ s/\s+$//;
        my $pub_id = getPubID($dbh, $publink_citation);
        if ($pub_id) {
          $sql = "
            INSERT INTO chado.stock_pub
              (stock_id, pub_id)
            VALUES
              ((SELECT stock_id FROM chado.stock WHERE uniquename='$mapname'),
               $pub_id
              )";
          logSQL($dataset_name, "$sql");
          $sth = $dbh->prepare($sql);
          $sth->execute();
        }
      }
      
      # attach map collection (featuremap) to mapping population (stock)
      # map name = mapping population uniquename
      $sql = "
        INSERT INTO chado.featuremap_stock
          (featuremap_id, stock_id)
        VALUES
          ($map_id,
           (SELECT stock_id FROM chado.stock WHERE uniquename='$mapname'))";
      logSQL($dataset_name, $sql);
      $sth = $dbh->prepare($sql);
      $sth->execute();
    }#has mapping population
    
    # map_name, publication_map_name, pop_size, pop_type, analysis_method
    insertFeaturemapprop($dbh, $map_id, $mi{'map_name_fld'}, 'Display Map Name', $fields);
    insertFeaturemapprop($dbh, $map_id, $mci{'pub_map_name_fld'}, 'Publication Map Name', $fields);
    insertFeaturemapprop($dbh, $map_id, $mci{'pop_size_fld'}, 'Population Size', $fields);
    insertFeaturemapprop($dbh, $map_id, $mci{'pop_type_fld'}, 'Population Type', $fields);
    insertFeaturemapprop($dbh, $map_id, $mci{'a_method_fld'}, 'Methods', $fields);
    insertFeaturemapprop($dbh, $map_id, $mci{'comment_fld'}, 'Featuremap Comment', $fields);
          
    # attach map collection (featuremap) to publication
    foreach my $publink_citation (@publink_citations) {
      $sql = "
        INSERT INTO chado.featuremap_pub
          (featuremap_id, pub_id)
        VALUES
          ($map_id,
           (SELECT pub_id FROM chado.pub WHERE uniquename=?))";
      logSQL($dataset_name, "$sql\nWITH:\n$publink_citation");
      $sth = doQuery($dbh, $sql, ($publink_citation));
    }
    
    # make a dbxref record for LIS cmap link (for full mapset)
    makeMapsetDbxref($dbh, $map_id, $mci{'LIS_name_fld'}, $fields);
    
  }#each record
  
  print "Handled $line_count map collection records\n\n";
}#loadMapCollection


sub loadLinkageMaps {
  my ($dbh, $filename) = @_;
  my (%fields, $sql, $sth);
  
  print "\n\nLoading/verifying $filename.txt...\n";

  my ($skip, $skip_all, $update, $update_all);

  $table_file = "$input_dir/$filename.txt";
  if (!-e $table_file) {
    print "$filename.txt not found.\n";
    return;
  }
  
  @records = readFile($table_file);
  $line_count = 0;
  my $skip_all = 0;  # skip all existing consensus_map records
  my $update_all = 0; # update all existing qtl records without asking
  foreach my $fields (@records) {
    $line_count++;
    
    my $lg_mapname = makeLinkageMapName($fields->{$mi{'map_name_fld'}}, $fields->{$mi{'lg_fld'}});
    
    if (my $map_id = lgMapExists($dbh, $lg_mapname, $fields)) {
      next if ($skip_all);
      if ($update_all) {
          $existing_lg_maps{$lg_mapname} = $map_id;
          
          # remove dependent records; they will be re-inserted
          clearMapLGDependencies($dbh, $map_id);
      }
      else {
        my $prompt = "$line_count: linkage group map ($lg_mapname)";
        ($skip, $skip_all, $update, $update_all) = checkUpdate($prompt);
        
        next if ($skip || $skip_all);
        
        if ($update || $update_all) {
          $existing_lg_maps{$lg_mapname} = $map_id;
          
          # remove dependent records; they will be re-inserted
          clearMapLGDependencies($dbh, $map_id, $fields);
        }
      }#update_all not selected
    }#linkage map exists
    
    my $map_id = setLgMapRec($fields);  # feature_id

    my $mapset = $fields->{$mi{'map_name_fld'}};
    
    # set start and end coordinates
    setGeneticCoordinates($dbh, $map_id, $mapset, $lg_mapname, $fields);

    # make a dbxref record for LIS cmap link (for linkage group)
    makeLgDbxref($dbh, $map_id, $mi{'LIS_lg_fld'}, $fields);

  }#each record
  
  print "Handled $line_count map records\n\n";
}#loadLinkageMaps


sub loadMarkerPositions {
  my ($dbh, $filename) = @_;
  
  my ($fields, $sql, $sth, $row, $msg);

  $table_file = "$input_dir/$filename.txt";
  print "Loading/verifying $table_file...\n";
  
  @records = readFile($table_file);
  print "\nLoading " . (scalar @records) . " markers...\n";
  
  # build linkage groups from the markers and create/update lg records as needed.
  my %lgs = createLinkageGroups(@records);
  updateLinkageGroups($dbh, %lgs);
=cut  
  $line_count = 0;
  foreach $fields (@records) {
    $line_count++;
    
    # Try to detect QTLs and skip
    my $line = join ' ', $fields;
    if (lc($line) =~ /qtl/) {
      # guess that this is a QTL, not a marker
      $msg = "warning: This record appears to be a QTL, not a marker. It will not be loaded.";
      reportError($line_count, $msg);
      next;
    }
    
    my $marker_id;
    my $marker_name = $fields->{'marker_name'};
#print "$line_count: handle marker $marker_name\n";
    
    if ($marker_id=markerExists($dbh, $marker_name, $fields->{'specieslink_abv'})) {
      if ($skip_all) {
        next;
      }
      if ($update_all) {
#        cleanDependants($dbh, $marker_id);
      }
      else {
        my $prompt =  "$line_count: This marker ($marker_name)";
        ($skip, $skip_all, $update, $update_all) = checkUpdate($prompt);
        if ($skip || $skip_all) {
          next;
        }
        if ($update || $update_all) {
#          cleanDependants($dbh, $marker_id);
        }
      }
    }#marker exists
    
    # Place on linkage group
    placeMarkerOnLG($dbh, $marker_id, $fields);
    
    # CMap link
    setSecondaryDbxref($dbh, $marker_id, $mki{'cmap_acc_fld'}, 'LIS:cmap', $fields);
    
  }#each record
=cut
}#loadMarkerPositions
  





################################################################################
################################################################################
################################################################################
################################################################################

sub clearMapSetDependencies {
  my ($dbh, $map_set_id, $fields) = @_;
  my ($sql, $sth, $row);
  # $map_set_id is a featuremap_id
  
  # delete mapping population record
  my $stockname = $fields->{$mi{'map_name_fld'}};

  $sql = "DELETE FROM chado.stock_pub 
          WHERE stock_id IN (SELECT stock_id FROM stock
                             WHERE uniquename='$stockname')";
  logSQL('', $sql);
  doQuery($dbh, $sql); 

  # this will also delete dependancies, including featuremap_stock and 
  #    stock_relationship
  $sql = "DELETE FROM chado.stock WHERE uniquename='$stockname'";
  logSQL('', $sql);
  doQuery($dbh, $sql); 
  
  # clear featuremap properties
  $sql = "DELETE FROM chado.featuremapprop WHERE featuremap_id = $map_set_id";
  logSQL('', $sql);
  doQuery($dbh, $sql); # will also delete dependancies, eg, stock_pub
  
  # clear featuremap pub
  $sql = "DELETE FROM chado.featuremap_pub WHERE featuremap_id = $map_set_id";
  logSQL('', $sql);
  doQuery($dbh, $sql); # will also delete dependancies, eg, stock_pub

  # dbxrefs  
  $sql = "
    DELETE FROM chado.dbxref 
    WHERE dbxref_id IN 
      (SELECT dbxref_id FROM chado.featuremap_dbxref WHERE featuremap_id = $map_set_id)";
  logSQL('', $sql);
  doQuery($dbh, $sql);
  $sql = "
    DELETE FROM chado.featuremap_dbxref WHERE featuremap_id = $map_set_id";
  logSQL('', $sql);
  doQuery($dbh, $sql);
}#clearMapSetDependencies


sub clearMapLGDependencies {
  my ($dbh, $map_id) = @_;
  my ($sql, $sth, $row);
  # $map_id is a feature_id
  
  $sql = "DELETE FROM chado.featurepos WHERE feature_id = $map_id";
  logSQL('', $sql);
  doQuery($dbh, $sql);

  # dbxrefs  
  $sql = "
    DELETE FROM chado.dbxref 
    WHERE dbxref_id IN 
      (SELECT dbxref_id FROM chado.feature_dbxref WHERE feature_id = $map_id)";
  logSQL('', $sql);
  doQuery($dbh, $sql);
  $sql = "
    DELETE FROM chado.feature_dbxref WHERE feature_id = $map_id";
  logSQL('', $sql);
  doQuery($dbh, $sql);
  $sql = "DELETE FROM chado.featureprop WHERE feature_id = $map_id
          AND type_id =
          (SELECT cvterm_id FROM cvterm where name='Assigned Linkage Group')";
  logSQL('', $sql);
  doQuery($dbh, $sql);
}#clearMapLGDependencies


sub confirmStockRecord {
  my ($dbh, $stockname, $stock_type, $fields) = @_;
  my ($sql, $sth, $row);
  
  $sql = "
    SELECT * FROM chado.stock WHERE uniquename=?";
  logSQL($dataset_name, "$sql\WITH\n'$stockname'");
  $sth = doQuery($dbh, $sql, ($stockname));
  if (!$sth || !($row = $sth->fetchrow_hashref)) {
    my $organism_id = getOrganismID($dbh, $fields->{$mci{'species_fld'}}, $line_count);
    $sql = "
      INSERT INTO chado.stock 
        (organism_id, name, uniquename, type_id)
      VALUES
        ($organism_id, ?, '$stockname',
         (SELECT cvterm_id FROM chado.cvterm 
          WHERE name='$stock_type'
            AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='stock_type')))";
    logSQL($dataset_name, "$sql\nWITH\n'$stockname'");
    $sth = doQuery($dbh, $sql, ($stockname));
  }#create stock record
}#confirmStockRecord


sub connectParent {
  my ($dbh, $parent_stock, $parent_type, $fields) = @_;
  my ($sql, $sth, $row);

  my $mapping_stock = $fields->{$mci{'map_name_fld'}};
print "\n$parent_type: Got mapping stock: $mapping_stock\n";

  $sql = "
    SELECT * FROM chado.stock_relationship
    WHERE subject_id=(SELECT stock_id FROM chado.stock WHERE uniquename=?)
      AND object_id=(SELECT stock_id FROM chado.stock WHERE uniquename=?)
      AND type_id=(SELECT cvterm_id FROM chado.cvterm 
                   WHERE name='$parent_type'
                     AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='stock_relationship'))";
  logSQL($dataset_name, "$sql\nWITH\n'$parent_stock' and '$mapping_stock'");
  $sth = doQuery($dbh, $sql, ($parent_stock, $mapping_stock));
  if (!$sth || !($row = $sth->fetchrow_hashref)) {
    my $subj_stockname = $parent_stock;
    my $obj_stockname = $mapping_stock;
print "subject: $subj_stockname, object: $obj_stockname\n";
    $sql = "
         INSERT INTO chado.stock_relationship
           (subject_id, object_id, type_id, rank)
         VALUES
           ((SELECT stock_id FROM chado.stock WHERE uniquename='$subj_stockname'),
            (SELECT stock_id FROM chado.stock WHERE uniquename='$obj_stockname'),
            (SELECT cvterm_id FROM chado.cvterm 
             WHERE name='$parent_type'
               AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='stock_relationship')),
            1)";
    logSQL($dataset_name, $sql);
    $sth = $dbh->prepare($sql);
    $sth->execute();
  }#create population-parent relationship
}#connectParent


sub coordinatesExist {
  my ($dbh, $mapset_id, $feature_id, $mapname) = @_;
  my ($sql, $sth, $row);
  
  $sql = "
    SELECT * 
    FROM featurepos f 
      INNER JOIN featureposprop fp ON fp.featurepos_id=f.featurepos_id
    WHERE f.featuremap_id=$mapset_id
      AND feature_id= $feature_id
      AND map_feature_id= $feature_id
      AND fp.type_id=(SELECT cvterm_id FROM chado.cvterm 
                      WHERE name='start'
                        AND cv_id=(SELECT cv_id FROM chado.cv 
                                   WHERE name='featurepos_property'))";
  logSQL('', $sql);
  $sth = doQuery($dbh, $sql);
  if ($row=$sth->fetchrow_hashref) {
    return 1;
  }
  else {
    return 0;
  }
}#coordinatesExist


sub getLgCoord {
  my ($dbh, $lg, $coord) = @_;
  
  my $sql = "
    SELECT mappos FROM chado.featurepos fp
      INNER JOIN chado.featuremap m 
        ON m.featuremap_id = fp.featuremap_id
      INNER JOIN chado.featureposprop fpp 
        ON fpp.featurepos_id = fp.featurepos_id
    WHERE m.feature_id = 
          (SELECT feature_id FROM chado.feature 
           WHERE uniquename='$lg' 
                 AND type_id = (SELECT cvterm_id FROM cvterm 
                                WHERE name='genetic_marker' 
                                      AND cv_id = (SELECT cv_id FROM cv 
                                                  WHERE name='sequence')
          )
          AND fpp.type_id=(SELECT cvterm_id FROM chado.cvterm 
                  WHERE name='$coord'
                    AND cv_id=(SELECT cv_id FROM chado.cv 
                               WHERE name='featurepos_property'))";
   logSQL('', $sql);
print "$sql\n";
  my $sth = doQuery($dbh, $sql);
  if (my $row == $sth->fetchrow_hashref) {
    return $row->{'mappos'};
  }
  else {
    return 0;
  }
}#getLgCoord

         
sub insertFeaturemapprop {
  my ($dbh, $map_id, $fieldname, $proptype, $fields) = @_;
  my ($sql, $sth);
  
  if ($fields->{$fieldname}
        && $fields->{$fieldname} ne '' 
        && $fields->{$fieldname} ne 'NULL') {
    $sql = "
      INSERT INTO chado.featuremapprop
        (featuremap_id, type_id, value, rank)
      VALUES
        ($map_id,
         (SELECT cvterm_id FROM chado.cvterm 
          WHERE name='$proptype'
            AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='featuremap_property')),
         ?, 1)";
     logSQL($dataset_name, "$sql\nWITH\n'$fields->{$fieldname}'");
     $sth = doQuery($dbh, $sql, ($fields->{$fieldname}));
   }
}#insertFeaturemapprop


sub makeMapsetDbxref {
  my ($dbh, $map_id, $fieldname, $fields) = @_;
  # $map_id is a featuremap_id

  return if (!$fields->{$fieldname} || $fields->{$fieldname} eq 'NULL');
    
  # WARNING! THIS IS SPECIFIC TO LIS CMAP URLS!
  # "accession" here is the completion of db URL.
  my $accession = "?ref_map_accs=-1&ref_map_set_acc=" . $fields->{$fieldname};

  my $sql = "
    INSERT INTO dbxref
      (db_id, accession)
    VALUES
      ((SELECT db_id FROM db WHERE name='LIS:cmap'), ?)
    RETURNING dbxref_id";
  logSQL($dataset_name, "$sql\nWITH\n$accession");
  $sth = doQuery($dbh, $sql, ($accession));
  $row = $sth->fetchrow_hashref;
  
  my $dbxref_id = $row->{'dbxref_id'};
  
  $sql = "
    INSERT INTO featuremap_dbxref
      (featuremap_id, dbxref_id)
    VALUES
      ($map_id, $dbxref_id)";
  logSQL($dataset_name, $sql);
  $sth = $dbh->prepare($sql);
  $sth->execute();
  
  $lis_map_sets{$fields->{$mi{'map_name_fld'}}} = $fields->{$fieldname};
}#makeMapsetDbxref


sub makeLgDbxref {
  my ($dbh, $map_id, $fieldname, $fields) = @_;
  # $map_id is a feature_id

  return if (!$fields->{$fieldname}  || $fields->{$fieldname} eq 'NULL');
  
  # WARNING! THIS IS SPECIFIC TO LIS CMAP URLS!
  # "accession" here is the completion of db URL.
  my $lis_mapname = $lis_map_sets{$fields->{$mci{'map_name_fld'}}};
  my $accession = "?ref_map_set_acc=$lis_mapname;ref_map_accs=" . $fields->{$fieldname};
  print "create dbxref for $accession\n";

  my $sql = "
    INSERT INTO dbxref
      (db_id, accession)
    VALUES
      ((SELECT db_id FROM db WHERE name='LIS:cmap'), ?)
    RETURNING dbxref_id";
  logSQL($dataset_name, "$sql\nWITH\n$accession");
  $sth = doQuery($dbh, $sql, ($accession));
  $row = $sth->fetchrow_hashref;
  
  my $dbxref_id = $row->{'dbxref_id'};
  
  $sql = "
    INSERT INTO feature_dbxref
      (feature_id, dbxref_id)
    VALUES
      ($map_id, $dbxref_id)";
  logSQL($dataset_name, $sql);
  $sth = $dbh->prepare($sql);
  $sth->execute();
}#makeLgDbxref


sub lgMapExists {
  my ($dbh, $mapname, $fields) = @_;
  my ($sql, $sth, $row);
  $sql = "
    SELECT feature_id FROM chado.feature 
    WHERE name = '$mapname' 
          AND type_id=(SELECT cvterm_id FROM cvterm 
                       WHERE name='linkage_group' 
                             AND cv_id = (SELECT cv_id FROM cv WHERE name='sequence'))
    ";
  logSQL('', $sql);
  $sth = $dbh->prepare($sql);
  $sth->execute();
  if (($row = $sth->fetchrow_hashref)) {
    return $row->{'feature_id'};
  }
  else {
    return 0;
  }
}#lgMapExists


sub setGeneticCoordinates {
  my ($dbh, $feature_id, $mapset, $mapname, $fields) = @_;
  my ($sql, $sth);
  
  my $mapset_id = getMapSetID($dbh, $mapset);
  if (!$mapset_id) {
    print "ERROR: no mapset record for $mapset. Unable to continue.\n\n";
    exit;
  }

  # Insert map start position (start coordinate)
  if (isFieldSet($fields, $mi{'map_start_fld'})) {
    $sql = "
      INSERT INTO chado.featurepos
       (featuremap_id, feature_id, map_feature_id, mappos)
      VALUES
       ($mapset_id, $feature_id, $feature_id, $fields->{$mi{'map_start_fld'}})
      RETURNING featurepos_id";
    logSQL($dataset_name, $sql);
    $sth = $dbh->prepare($sql);
    $sth->execute();
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
    logSQL($dataset_name, $sql);
    $sth = $dbh->prepare($sql);
    $sth->execute();
 
    # insert map end position (stop coordinate)
    $sql = "
      INSERT INTO chado.featurepos
       (featuremap_id, feature_id, map_feature_id, mappos)
      VALUES
       ($mapset_id, $feature_id, $feature_id, $fields->{$mi{'map_end_fld'}})
      RETURNING featurepos_id";
    logSQL($dataset_name, $sql);
    $sth = $dbh->prepare($sql);
    $sth->execute();
    $row = $sth->fetchrow_hashref;
    my $featurepos_id = $row->{'featurepos_id'};
  
    $sql = "
      INSERT INTO chado.featureposprop
       (featurepos_id, type_id, value, rank)
      VALUES
       ($featurepos_id,
        (SELECT cvterm_id FROM chado.cvterm 
         WHERE name='stop'
           AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='featurepos_property')),
        '', 1)";
    logSQL($dataset_name, $sql);
    $sth = $dbh->prepare($sql);
    $sth->execute();
  }
}#setGeneticCoordinates

        
sub setLgMapRec {
  my ($fields) = @_;
  
  my $lg       = $fields->{$mi{'lg_fld'}};
  my $map_name = $fields->{$mi{'map_name_fld'}};
  my $mapname  = makeLinkageMapName($map_name, $lg);
  
  my $organism_id = getOrganismID($dbh, $fields->{$mi{'species_fld'}}, $line_count);
  
  # A consensus map, or linkage group map is a feature.
  #   Its proper name is map_name + linkage group.
  if ($existing_lg_maps{$mapname}) {
    $sql = "
      UPDATE chado.feature SET
        organism_id=$organism_id,
        name='$mapname',
        uniquename = '$mapname'
      WHERE feature_id = $existing_lg_maps{$mapname}
      RETURNING feature_id";
  }
  else {
    $sql = "
      INSERT INTO chado.feature
       (organism_id, name, uniquename, type_id)
      VALUES
       ($organism_id, '$mapname', '$mapname',
        (SELECT cvterm_id FROM chado.cvterm 
         WHERE name='linkage_group'
           AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='sequence')))
      RETURNING feature_id";
  }
  
  logSQL($dataset_name, $sql);
  $sth = $dbh->prepare($sql);
  $sth->execute();
  $row = $sth->fetchrow_hashref;
  my $map_id = $row->{'feature_id'};
  
  # Attach linkage group name as a property
  $sql = "
    INSERT INTO chado.featureprop
      (feature_id, type_id, value, rank)
    VALUES
      ($map_id,
       (SELECT cvterm_id FROM chado.cvterm
        WHERE name='Assigned Linkage Group'
          AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='feature_property')),
       '$lg',
       0)";
  logSQL($dataset_name, $sql);
  $sth = $dbh->prepare($sql);
  $sth->execute();
  
  $sth->finish;

  return $map_id;
}#setLgMapRec


sub setMapSetRec {
  my ($dbh, $fields) = @_;
  
  my $mapname      = $fields->{$mi{'map_name_fld'}};
  my $description  = $fields->{$mci{'description_fld'}};
  my $unit         = $fields->{$mci{'unit_fld'}};
  
  my $type_id = getCvtermID($dbh, $unit, 'tripal_featuremap');
  if (!$type_id) {
    print "Unable to continue.\n\n";
    exit;
  }
  
  if ($existing_map_sets{$mapname}) {
    $sql = "
      UPDATE chado.featuremap SET
        name='$mapname',
        description=?,
        unittype_id=$type_id
      WHERE name='$mapname'
      RETURNING featuremap_id";
  }
  else {
    $sql = "
      INSERT INTO chado.featuremap
        (name, description, unittype_id)
      VALUES
        ('$mapname', ?, $type_id)
      RETURNING featuremap_id";
  }
  
  logSQL($dataset_name, "$sql\nWITH\n$description");
  $sth = doQuery($dbh, $sql, ($description));
  $row = $sth->fetchrow_hashref;
  my $map_id = $row->{'featuremap_id'};
  $sth->finish;
  
  return $map_id;
}#setMapSetRec


sub updateLinkageGroups {
  my ($dbh, %lgs) = @_;
  my ($msg, $sql, $sth, $row);
  
  foreach my $lg (keys %lgs) {
#print "Handle lg $lg: " . Dumper($lgs{$lg});
    my $lg_id = getFeatureID($dbh, $lg);
    if ($lg_id) {
      # verify that lengths are the same
      my $start = getLgCoord($lg, 'start');
      my $end   = getLgCoord($lg, 'end');
print "Got coords $start, $end\n";
#leave exit in place until this code is completed and tested
exit;
      next;
    }

    my $organism_id = getOrganismID($dbh, $lgs{$lg}{'species'}, '');
    if (!$organism_id) {
      $msg = "ERROR: Unable to find a record for species '"
           . $lgs{$lg}{'species'} . "'.";
      reportError($line_count, $msg);
      next;
    }
    
    # check if linkage group already exists
    
    $sql = "
      INSERT INTO feature
        (organism_id, name, uniquename, type_id)
      VALUES
        ($organism_id, '$lg', '$lg',
         (SELECT cvterm_id FROM cvterm 
          WHERE name='linkage_group' 
                AND cv_id=(SELECT cv_id FROM cv WHERE name='sequence'))
        )";
    logSQL($dataset_name, $sql);
    doQuery($dbh, $sql);
=cut
#TODO    
#    setFeatureProp($dbh, $lg_id, $mki{'lg_fld'}, 
#                   'Assigned Linkage Group', 1, $fields);
  }#each lg
}#updateLinkageGroups




