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
  
  # Used all over
  my ($table_file, $sql, $sth, $row, $count, @records, @fields, $cmd, $rv);
  my ($has_errors, $line_count);

  my $dataset_name = 'maps';
  
  # Holds map sets that are already in db
  my %existing_map_sets;

  # Holds linkage maps that are already in db
  my %existing_lg_maps;
  
#TODO: should be more general
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
    loadMapCollection($dbh);
    loadMaps($dbh, $mi{'worksheet'});
    
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
  my $dbh = $_[0];
  my (%fields, $sql, $sth);
  
  $table_file = "$input_dir/$mci{'worksheet'}.txt";
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

    # create mapping population stock record if needed
    confirmStockRecord($dbh, $fields->{$mci{'map_name_fld'}}, 
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
    my @publink_citations = split ';', $fields->{$mci{'pub_fld'}};
print "publink_citations: " . Dumper(@publink_citations);
exit;

    my $pub_id = getPubID($dbh, $publink_citation);
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

    # create featuremap record (base record for map)
    my $map_id = setMapSetRec($dbh, $fields);  # featuremap_id

    # map_name, publication_map_name, pop_size, pop_type, analysis_method, and comment
    insertFeaturemapprop($dbh, $map_id, $mi{'map_name_fld'}, 'Display Map Name', $fields);
    insertFeaturemapprop($dbh, $map_id, $mci{'pub_map_name_fld'}, 'Publication Map Name', $fields);
    insertFeaturemapprop($dbh, $map_id, $mci{'pop_size_fld'}, 'Population Size', $fields);
    insertFeaturemapprop($dbh, $map_id, $mci{'pop_type_fld'}, 'Population Type', $fields);
    insertFeaturemapprop($dbh, $map_id, $mci{'a_method_fld'}, 'Methods', $fields);
    insertFeaturemapprop($dbh, $map_id, $mci{'comment_fld'}, 'Featuremap Comment', $fields);
          
    # attach map collection (featuremap) to publication
    $sql = "
      INSERT INTO chado.featuremap_pub
        (featuremap_id, pub_id)
      VALUES
        ($map_id,
         (SELECT pub_id FROM chado.pub WHERE uniquename=?))";
    logSQL($dataset_name, "$sql\nWITH:\n$publink_citation");
    $sth = $dbh->prepare($sql);
    $sth->execute($publink_citation);

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
    
    # make a dbxref record for LIS cmap link (for full mapset)
    makeMapsetDbxref($dbh, $map_id, $mci{'LIS_name_fld'}, $fields);
    
  }#each record
  
  print "Handled $line_count map collection records\n\n";
}#loadMapCollection


sub loadMaps {
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
    
    my $lg_mapname = makeLinkageMapName($fields);
    
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

    # Note that both the mapping population stock record and the map record  
    #     have the same name.
    my $mapset = $fields->{$mi{'map_name_fld'}};
    
    # set start and end coordinates
    setGeneticCoordinates($dbh, $map_id, $mapset, $lg_mapname, $fields);

    # make a dbxref record for LIS cmap link (for linkage group)
    makeLgDbxref($dbh, $map_id, $mi{'LIS_lg_fld'}, $fields);

  }#each record
  
  print "Handled $line_count map records\n\n";
}#loadMaps



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
}#clearMapLGDependencies


sub confirmStockRecord {
  my ($dbh, $stockname, $stock_type, $fields) = @_;
  my ($sql, $sth, $row);
  
  $sql = "
    SELECT * FROM chado.stock WHERE uniquename=?";
  logSQL($dataset_name, "$sql\nwith '$stockname'");
  $sth = $dbh->prepare($sql);
  $sth->execute($stockname);
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
    logSQL($dataset_name, "$sql\nWith '$stockname'");
    $sth = $dbh->prepare($sql);
    $sth->execute($stockname);
  }#create stock record
}#confirmStockRecord


sub connectParent {
  my ($dbh, $parent_stock, $parent_type, $fields) = @_;
  my ($sql, $sth, $row);

  my $mapping_stock = $fields->{$mci{'map_name_fld'}};
print "\nGot mapping stock: $mapping_stock\n";

  $sql = "
    SELECT * FROM chado.stock_relationship
    WHERE subject_id=(SELECT stock_id FROM chado.stock WHERE uniquename=?)
      AND object_id=(SELECT stock_id FROM chado.stock WHERE uniquename=?)
      AND type_id=(SELECT cvterm_id FROM chado.cvterm 
                   WHERE name='$parent_type'
                     AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='stock_relationship'))";
  logSQL($dataset_name, "$sql\nwith '$parent_stock' and '$mapping_stock'");
  $sth = $dbh->prepare($sql);
  $sth->execute($parent_stock, $mapping_stock);
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
                      WHERE name='start coordinate'
                        AND cv_id=(SELECT cv_id FROM chado.cv 
                                   WHERE name='local'))";
  logSQL('', $sql);
  $sth = doQuery($dbh, $sql);
  if ($row=$sth->fetchrow_hashref) {
    return 1;
  }
  else {
    return 0;
  }
}#coordinatesExist


sub insertFeaturemapprop {
  my ($dbh, $map_id, $fieldname, $proptype, $fields) = @_;
  my ($sql, $sth);
  
  if ($fields->{$fieldname}
        && $fields->{$fieldname} ne '' 
        && $fields->{$fieldname} ne 'NULL') {
    my $map_set_name = $fields->{$mi{'map_name_fld'}};
    
    $sql = "
      INSERT INTO chado.featuremapprop
        (featuremap_id, type_id, value, rank)
      VALUES
        ($map_id,
         (SELECT cvterm_id FROM chado.cvterm 
          WHERE name='$proptype'
            AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='featuremap_property')),
         ?, 1)";
     logSQL($dataset_name, "$sql\nwith '$fields->{$fieldname}'");
     $sth = $dbh->prepare($sql);
     $sth->execute($fields->{$fieldname});
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
  logSQL($dataset_name, "$sql\n with [$accession]");
  $sth = $dbh->prepare($sql);
  $sth->execute($accession);
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
  my $lis_mapname = $lis_map_sets{$fields->{$mi{'map_name_fld'}}};
  my $accession = "?ref_map_set_acc=$lis_mapname;ref_map_accs=" . $fields->{$fieldname};

  my $sql = "
    INSERT INTO dbxref
      (db_id, accession)
    VALUES
      ((SELECT db_id FROM db WHERE name='LIS:cmap'), ?)
    RETURNING dbxref_id";
  logSQL($dataset_name, "$sql\nwith [$accession]");
  $sth = $dbh->prepare($sql);
  $sth->execute($accession);
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
}#setGeneticCoordinates

        
sub setLgMapRec {
  my ($fields) = @_;
  
  my $mapname = makeLinkageMapName($fields);
  my $organism_id = getOrganismID($dbh, $fields->{$mi{'species_fld'}}, $line_count);
  
  # A consensus map, or linkage group map is a feature
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
  
  logSQL($dataset_name, $sql);
  $sth = $dbh->prepare($sql);
  $sth->execute(decode("iso-8859-1", $description));
  $row = $sth->fetchrow_hashref;
  my $map_id = $row->{'featuremap_id'};
  $sth->finish;
  
  return $map_id;
}#setMapSetRec
