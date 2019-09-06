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
  my %mpi = getSSInfo('MARKER_POSITION');

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

  # check for worksheets (script will exit on user request)
  my $load_map_collection  = checkWorksheet($input_dir, $mci{'worksheet'});
  my $load_linkage_maps    = checkWorksheet($input_dir, $mi{'worksheet'});
  my $load_marker_position = checkWorksheet($input_dir, $mpi{'worksheet'});

  # Get connected
  my $dbh = connectToDB;

  # set default schema
  $sql = "SET SEARCH_PATH = chado";
  $sth = $dbh->prepare($sql);
  $sth->execute();

  # Use a transaction so that it can be rolled back if there are any errors
  eval {
    if ($load_map_collection)  { loadMapCollection($dbh, $mci{'worksheet'});   }
    if ($load_linkage_maps)    { loadLinkageMaps($dbh, $mi{'worksheet'});      }
    if ($load_marker_position) { loadMarkerPositions($dbh, $mpi{'worksheet'}); }

#keep this commented-out until sure the script is working.    
    $dbh->commit;   # commit the changes if we get this far
  };
  if ($@) {
    print "\n\nTransaction aborted because $@\n\n";
    # now rollback to undo the incomplete changes
    # but do it in an eval{} as it may also fail
    eval { $dbh->rollback };
  }
  

# ALL DONE
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
      }
      else {
        my $prompt = "$line_count: map ($fields->{$mi{'map_name_fld'}} = $map_set_id) ";
        ($skip, $skip_all, $update, $update_all) = checkUpdate($prompt);
        
        next if ($skip || $skip_all);
        
        if ($update || $update_all) {
          $existing_map_sets{$mapname} = $map_set_id;
        }
      }#update_all not set
    }#map set exists

    # create featuremap record (base record for map)
    my $map_id = setMapSetRec($dbh, $fields);  # featuremap_id

    my @publink_citations = split ';', $fields->{$mci{'pub_fld'}};
    
    my $map_name = $fields->{$mci{'map_name_fld'}};
    if ($map_name =~ /^.*_x_.*$/i) {

      # create mapping population stock record if needed
      confirmStockRecord($dbh, $map_name, 'Mapping Population', $fields);
      
      # create parent stock records if needed
      if ($fields->{$mci{'parent1_fld'}} 
          && $fields->{$mci{'parent1_fld'}} ne '' 
          && $fields->{$mci{'parent1_fld'}} ne 'NULL') {
#TODO: add a stock type field, or guess from name?
         confirmStockRecord($dbh, $fields->{$mci{'parent1_fld'}}, 
                            'Cultivar', $fields);
         connectParent($dbh, $fields->{$mci{'parent1_fld'}}, 'Parent1', $fields);
      }
      if ($fields->{$mci{'parent2_fld'}} 
          && $fields->{$mci{'parent2_fld'}} ne '' 
          && $fields->{$mci{'parent2_fld'}} ne 'NULL') {
#TODO: add a stock type field, or guess from name?
        confirmStockRecord($dbh, $fields->{$mci{'parent2_fld'}}, 
                           'Cultivar', $fields);
        connectParent($dbh, $fields->{$mci{'parent2_fld'}}, 'Parent2', $fields);
      }
    
      # attach mapping population to publication
      if (!isNull($fields->{$mci{'pub_fld'}})) {
        foreach my $publink_citation (@publink_citations) {
          $publink_citation =~ s/^\s//;
          $publink_citation =~ s/\s+$//;
          my $pub_id = getPubID($dbh, $publink_citation);
          if ($pub_id) {
            attachStockPub($mapname, $pub_id);
          }#pub exists
        }#each pub
      }#pub is given
      
      # attach map collection (featuremap) to mapping population (stock)
      # map name = mapping population uniquename
      attachMapStock($map_id, $mapname);
    }#has mapping population
    
    # map_name, publication_map_name, pop_size, pop_type, analysis_method
    setFeaturemapprop($dbh, $map_id, $mi{'map_name_fld'}, 'Display Map Name', $fields);
    setFeaturemapprop($dbh, $map_id, $mci{'pub_map_name_fld'}, 'Publication Map Name', $fields);
    setFeaturemapprop($dbh, $map_id, $mci{'pop_size_fld'}, 'Population Size', $fields);
    setFeaturemapprop($dbh, $map_id, $mci{'pop_type_fld'}, 'Population Type', $fields);
    setFeaturemapprop($dbh, $map_id, $mci{'a_method_fld'}, 'Methods', $fields);
    setFeaturemapprop($dbh, $map_id, $mci{'download_url_fld'}, 'Download URL', $fields);
    setFeaturemapprop($dbh, $map_id, $mci{'comment_fld'}, 'Featuremap Comment', $fields);
          
    # attach map collection (featuremap) to publication
    if (!isNull($fields->{$mci{'pub_fld'}})) {
      attachMapPub($map_id, @publink_citations);
    }#pub is given
    
    if ($fields->{$mci{'LIS_name_fld'}} && $fields->{$mci{'LIS_name_fld'}} != 'NULL') {
      # WARNING! THIS IS SPECIFIC TO LIS CMAP URLS!
      # "accession" here is the completion of db URL.
      my $accession = "?ref_map_accs=-1&ref_map_set_acc=" . $fields->{$mci{'map_name_fld'}};
      makeFeaturemapDbxref($dbh, $map_id, $accession, 'LIS:cmap');
      
      # Need to keep map set name for later use
      $lis_map_sets{$fields->{$mci{'map_name_fld'}}} = $fields->{$mci{'map_name_fld'}};
    }
        
    # make dbxref for cmap-js
    if ($fields->{$mci{'download_url_fld'}}) {
      makeFeaturemapDbxref($dbh, $map_id, $fields->{$mci{'map_name_fld'}}, 'cmap-js');
    }
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
  my $skip_all = 0;  # skip all existing linkage map records
  my $update_all = 0; # update all existing linkage map records without asking
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
    # WARNING! THIS IS SPECIFIC TO LIS CMAP URLS!
    # "accession" here is the completion of db URL.
    if ($fields->{$mi{'LIS_lg_fld'}} && $fields->{$mi{'LIS_lg_fld'}} != 'NULL') {
print "ADD CMAP DBXREF\n";
      my $lis_mapname = $lis_map_sets{$fields->{$mi{'map_name_fld'}}};
print "map set accession: $lis_map_sets{$fields->{$mi{'map_name_fld'}}}\n";
print "map name: $fields->{$mi{'map_name_fld'}}\n";
print "all map set accessions:\n" . Dumper(%lis_map_sets) . "\n";
      my $accession = "?ref_map_set_acc=$lis_mapname;ref_map_accs=" . $lis_mapname;
print "create dbxref for $accession\n";
#      makeFeatureDbxref($dbh, $map_id, $accession, 'LIS:cmap');
    }
    
    # make a dbxref record for cmap-js link (for linkage group)
    if ($fields->{$mi{'map_name_fld'}}) {
      my $accession = $fields->{$mi{'map_name_fld'}} 
                    . ':'
                    . $fields->{$mi{'lg_fld'}};
      makeFeatureDbxref($dbh, $map_id, $accession, 'cmap-js');
    }
  }#each record
  
  print "Handled $line_count map records\n\n";
}#loadLinkageMaps


sub loadMarkerPositions {
  my ($dbh, $filename) = @_;
  
  my ($fields, $sql, $sth, $row, $msg);
  my ($skip_all, $change_all, $add_all, $quit);

  $table_file = "$input_dir/$filename.txt";
  print "Loading/verifying $table_file...\n";
  
  @records = readFile($table_file);
  return if ((scalar @records) == 0);
  print "\nLoading " . (scalar @records) . " markers...\n";
#print "All marker position records:\n" . Dumper(@records);
  
  if ((scalar @records) > 0) {
    # build linkage groups from the markers and create/update lg records as needed.
    my %lgs = createLinkageGroups(@records);
#print "\nMade new linkage groups:\n" . Dumper(%lgs);
    if ((scalar keys(%lgs)) > 0) {
      my $mapset = $records[0]->{$mpi{'map_name_fld'}};
      updateLinkageGroups($dbh, $mapset, %lgs);
    }
  }
  
  $line_count = 0;
  foreach $fields (@records) {
    $line_count++;
print ">>>>>>> $line_count:\n";# . Dumper($fields);
    
    # Try to detect QTLs and skip
    my @array;
    my @vals = map { $array[$_] = $fields->{$_} } keys %$fields;
    my $line = join ' ', @vals;
    if (lc($line) =~ /.*qtl.*/) {
      # guess that this is a QTL, not a marker
      $msg = "warning: This record appears to be a QTL, not a marker. It will not be loaded.";
      reportError($line_count, $msg);
      next;
    }
    
    my $species = $fields->{$mpi{'species_fld'}};
    my $marker_name = $fields->{'marker_identifier'};
    my $species_list = getMarkerSpecies($dbh, $marker_name);
#print "\nspecies list for $marker_name:\n" . Dumper($species_list);

    if ($species_list && scalar (keys %$species_list)
          && !$species_list->{$species} && !$change_all && !$add_all) {
      next if ($skip_all);
      if (!$change_all && !$add_all) {
        my $prompt = "$line_count: the marker $marker_name already exists ";
        $prompt .= "but is attached to " . join(', ', (keys %$species_list));
        $prompt .= " instead of $species. Choose an action: (skipall, changeall, addall, quit)";
        print "$prompt\n";
        my $userinput =  <STDIN>;
        chomp $userinput;
        if ($userinput eq 'skipall') {
          $skip_all = 1;
          next;
        }
        elsif ($userinput eq 'changeall') {
          $change_all = 1;
        }
        elsif ($userinput eq 'addall') {
          $add_all = 1;
        }
        elsif ($userinput eq 'quit') {
          exit;
        }
        else {
          print "don't recognize '$userinput', skipping.\n";
        }
      }#ask for user action
    }#potential conflict with existing marker

    my $marker_id;
    if ($change_all) {
      # change species to current assigned species
      $marker_id = updateMarker($dbh, $marker_name, $fields->{$mpi{'species_fld'}}, 1);
    }
    else {
      # add a record for this marker, if necessary
      $marker_id = updateMarker($dbh, $marker_name, $fields->{$mpi{'species_fld'}});
    }

    # Place on linkage group
    placeMarkerOnLG($dbh, $marker_id, $fields);
    
    # CMap link
    setFeatureDbxref($dbh, $marker_id, $mpi{'cmap_acc_fld'}, 'LIS:cmap', $fields);

    # Set marker type. This can be set in two different worksheets, but only 
    #   one is saved.
    setFeatureprop($dbh, $marker_id, $mpi{'marker_type_fld'}, 'Marker Type', 1, $fields);
    
    # map position comments are ranked 3 (which does limit a map postition 
    #    comment to 1 even though there maybe multiple positions for a marker)
    setFeatureprop($dbh, $marker_id, $mpi{'comment_fld'}, 'comment', 3, $fields);
#last if ($line_count > 1);
  }#each record
}#loadMarkerPositions
  





################################################################################
################################################################################
################################################################################
################################################################################


sub attachMapPub {
  my ($map_id, @publink_citations) = @_;
  my ($sql, $sth, $row);
  
  foreach my $publink_citation (@publink_citations) {
    $sql = "
      SELECT featuremap_pub_id FROM featuremap_pub
      WHERE featuremap_id=$map_id
            AND pub_id=(SELECT pub_id FROM chado.pub WHERE uniquename=?)";
    logSQL($dataset_name, "$sql\nWITH:\n$publink_citation");
    $sth = doQuery($dbh, $sql, ($publink_citation));
    if (!($row=$sth->fetchrow_hashref)) {
      $sql = "
        INSERT INTO chado.featuremap_pub
          (featuremap_id, pub_id)
        VALUES
          ($map_id,
           (SELECT pub_id FROM chado.pub WHERE uniquename=?))";
      logSQL($dataset_name, "$sql\nWITH:\n$publink_citation");
      $sth = doQuery($dbh, $sql, ($publink_citation));
    }
  }#each citation
}#attachMapPub


sub attachMapStock {
  my ($map_id, $stockname) = @_;
  my ($sql, $sth, $row);

  $sql = "
    SELECT featuremap_stock_id FROM chado.featuremap_stock
    WHERE featuremap_id=$map_id
          AND stock_id=(SELECT stock_id FROM chado.stock WHERE uniquename='$stockname')";
  logSQL($dataset_name, $sql);
  $sth = $dbh->prepare($sql);
  $sth->execute();
  if (($row=$sth->fetchrow_hashref)) {
    return $row->{'featuremap_stock_id'};
  }
  else {
    $sql = "
      INSERT INTO chado.featuremap_stock
        (featuremap_id, stock_id)
      VALUES
        ($map_id,
         (SELECT stock_id FROM chado.stock WHERE uniquename='$stockname'))
      RETURNING featuremap_stock_id";
    logSQL($dataset_name, $sql);
    $sth = $dbh->prepare($sql);
    $sth->execute();
    if (($row=$sth->fetchrow_hashref)) {
      return $row->{'featuremap_stock_id'};
    } 
  }
  
  return undef;   
}#attachMapStock


sub attachStockPub {
  my ($mapname, $pub_id) = @_;
  my ($sql, $sth, $row);
  
  $sql = "
    SELECT stock_pub_id FROM chado.stock_pub 
    WHERE stock_id=(SELECT stock_id FROM chado.stock WHERE uniquename='$mapname')
          AND pub_id=$pub_id";
  logSQL($dataset_name, "$sql\n");
  $sth = doQuery($dbh, $sql);
  if (($row=$sth->fetchrow_hashref)) {
    return $row->{'stock_pub_id'};
  }
  else {
    $sql = "
      INSERT INTO chado.stock_pub
        (stock_id, pub_id)
      VALUES
        ((SELECT stock_id FROM chado.stock WHERE uniquename='$mapname'),
         $pub_id
        )
      RETURNING stock_pub_id";
    logSQL($dataset_name, "$sql\n");
    $sth = doQuery($dbh, $sql);
    if ($row=$sth->fetchrow_hashref) {
      return $row->{'stock_pub_id'};
    }
  }
  
  return undef;
}#attachStockPub


sub changeLGcoord {
  my ($dbh, $lg_id, $mappos, $coord) = @_;
  my ($sql, $sth, $row);
  
  $sql= "
    SELECT fp.featurepos_id 
    FROM featurepos fp
      INNER JOIN featureposprop fpp ON fpp.featurepos_id=fp.featurepos_id
    WHERE fp.feature_id=$lg_id
          AND fpp.type_id = (SELECT cvterm_id FROM cvterm 
                             WHERE name='$coord' 
                                   AND cv_id = (SELECT cv_id FROM cv 
                                                WHERE name='featurepos_property'))";
  logSQL('', $sql);
  $sth = doQuery($dbh, $sql);
  if ($row = $sth->fetchrow_hashref) {
    $sql = "
      UPDATE featurepos
        SET mappos = $mappos
      WHERE featurepos_id=" . $row->{'featurepos_id'};
    logSQL('', $sql);
    doQuery($dbh, $sql);
  }
}#changeLGcoord



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
  my ($sql, $sth, $row, $msg);
  
  my $organism_id = getOrganismID($dbh, $fields->{$mci{'species_fld'}}, $line_count);
  my $stock_id    = getStockID($dbh, $stockname);
#print "Got organism id $organism_id for " . $fields->{$mci{'species_fld'}} . "<br>";  
#print "Got stock id: $stock_id\n";

  if ($stock_id) {
    $sql = "
      UPDATE chado.stock SET 
        organism_id=$organism_id,
        type_id=(SELECT cvterm_id FROM chado.cvterm 
                 WHERE name='$stock_type'
                   AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='stock_type'))
      WHERE stock_id=$stock_id";
    logSQL($dataset_name, "$sql\n");
    $sth = doQuery($dbh, $sql);
  }
  
  else {
print "Did not find stock record for [$stockname], will create it.\n";
    $sql = "
      INSERT INTO chado.stock 
        (organism_id, name, uniquename, type_id)
      VALUES
        ($organism_id, ?, '$stockname',
         (SELECT cvterm_id FROM chado.cvterm 
          WHERE name='$stock_type'
            AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='stock_type')))
      RETURNING stock_id";
    logSQL($dataset_name, "$sql\nWITH\n'$stockname'");
    $sth = doQuery($dbh, $sql, ($stockname));
    $row = $sth->fetchrow_hashref;
    $stock_id = $row->{'stock_id'};
  }#create stock record
  
  if ($stock_type eq 'Mapping Population' 
        && $fields->{$mci{'multispecies_fld'}}
        && $fields->{$mci{'multispecies_fld'}} ne 'NULL') {
    # append multiple species
    my $species_str = $fields->{$mci{'multispecies_fld'}};
    my @species_list = split ',', $species_str;
print "Species list: " . Dumper(@species_list) . "\n";

    # if any species already attached, get max rank
    $sql = "
        SELECT max(rank) FROM stock_organism
        WHERE stock_id=$stock_id";
    logSQL($dataset_name, "$sql\n");
    $sth = doQuery($dbh, $sql);
    my $rank = ($row=$sth->fetchrow_hashref) ? $row->{'max'}+1 : 1;

    foreach my $species (@species_list) {
      $species =~ s/^\s+//;
      $species =~ s/\s+$//;
      my $organism_id = getOrganismID($dbh, $species);
      if (!$organism_id) {
        $msg = "ERROR: this species, $species, is not in the organism table. ";
        $msg .= "Unable to continue.";
        die "$msg\n";
      }
      
      # Has this species already been attached?
      $sql = "
        SELECT stock_organism_id FROM stock_organism
        WHERE stock_id=$stock_id AND organism_id=$organism_id";
      logSQL($dataset_name, "$sql\n");
      $sth = doQuery($dbh, $sql);
      if ($row=$sth->fetchrow_hashref) {
        # nothing to do.
        next;
      }
      $sql = "
        INSERT INTO stock_organism
          (stock_id, organism_id, rank)
        VALUES
          ($stock_id, $organism_id, $rank)";
      logSQL($dataset_name, "$sql\n");
      doQuery($dbh, $sql);
      $rank++;
    }
  }
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
    WHERE fp.feature_id = 
          (SELECT feature_id FROM chado.feature 
           WHERE uniquename='$lg' 
                 AND type_id = (SELECT cvterm_id FROM cvterm 
                                WHERE name='linkage_group' 
                                      AND cv_id = (SELECT cv_id FROM cv 
                                                  WHERE name='sequence'))
          )
          AND fpp.type_id=(SELECT cvterm_id FROM chado.cvterm 
                           WHERE name='$coord'
                                 AND cv_id=(SELECT cv_id FROM chado.cv 
                                            WHERE name='featurepos_property'))";
  logSQL('', $sql);
  my $sth = doQuery($dbh, $sql);
  if (my $row=$sth->fetchrow_hashref) {
    return $row->{'mappos'};
  }
  else {
    return undef;
  }
}#getLgCoord


sub makeFeaturemapDbxref {
  my ($dbh, $map_id, $accession, $dbname) = @_;
  # $map_id is a featuremap_id

  return if (!$accession || $accession eq 'NULL');

  my ($sql, $sth, $row, $dbxref_id);

  $sql = "
    SELECT dbxref_id FROM chado.dbxref
    WHERE db_id=(SELECT db_id FROM db WHERE name='$dbname')
          AND accession=?";
  logSQL($dataset_name, "$sql\nWITH\n$accession\n");
  $sth = doQuery($dbh, $sql, ($accession));
  if (($row = $sth->fetchrow_hashref)) {
    $dbxref_id = $row->{'dbxref_id'};
  }
  else {
    $sql = "
      INSERT INTO dbxref
        (db_id, accession)
      VALUES
        ((SELECT db_id FROM db WHERE name='$dbname'), ?)
      RETURNING dbxref_id";
    logSQL($dataset_name, "$sql\nWITH\n$accession\n");
    $sth = doQuery($dbh, $sql, ($accession));
    $row = $sth->fetchrow_hashref;
    $dbxref_id = $row->{'dbxref_id'};
  }
  
  $sql = "
    SELECT featuremap_dbxref_id FROM featuremap_dbxref
    WHERE featuremap_id=$map_id AND dbxref_id=$dbxref_id";
  logSQL($dataset_name, "$sql");
  $sth = doQuery($dbh, $sql);
  if (!($row = $sth->fetchrow_hashref)) {
    $sql = "
      INSERT INTO featuremap_dbxref
        (featuremap_id, dbxref_id)
      VALUES
        ($map_id, $dbxref_id)";
    logSQL($dataset_name, $sql);
    $sth = $dbh->prepare($sql);
    $sth->execute();
  }
}#makeFeaturemapDbxref


sub makeFeatureDbxref {
  my ($dbh, $map_id, $accession, $dbname) = @_;
  # $map_id is a feature_id

  return if (!$accession  || $accession eq 'NULL');
  
  my $dbxref_id;
  my $sql = "
    SELECT dbxref_id FROM dbxref 
    WHERE accession='$accession'
          AND db_id=(SELECT db_id FROM db WHERE name='$dbname')";
  logSQL($dataset_name, "$sql");
  $sth = doQuery($dbh, $sql);
  if ($row=$sth->fetchrow_hashref) {
    $dbxref_id = $row->{'dbxref_id'};
  }
  else {
    $sql = "
      INSERT INTO dbxref
        (db_id, accession)
      VALUES
        ((SELECT db_id FROM db WHERE name=?), ?)
      RETURNING dbxref_id";
    logSQL($dataset_name, "$sql\nWITH\n$dbname, $accession");
    $sth = doQuery($dbh, $sql, ($dbname, $accession));
    $row = $sth->fetchrow_hashref;
    $dbxref_id = $row->{'dbxref_id'};
  }
print "dbxref id for $accession is $dbxref_id\n";
  
  $sql = "
    SELECT * FROM feature_dbxref
    WHERE feature_id=$map_id AND dbxref_id=$dbxref_id";
  logSQL($dataset_name, "$sql");
  $sth = doQuery($dbh, $sql);
  if (!($row= $sth->fetchrow_hashref)) {
    $sql = "
      INSERT INTO feature_dbxref
        (feature_id, dbxref_id)
      VALUES
        ($map_id, $dbxref_id)";
    logSQL($dataset_name, $sql);
    $sth = doQuery($dbh, $sql);;
  }
}#makeFeatureDbxref


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


sub placeMarkerOnLG {
  my ($dbh, $marker_id, $fields) = @_;
  my ($msg, $row, $sth, $sql);
  
#print "Check for map and linkage group data in this record:\n" . Dumper($fields);
  return if (!$fields->{$mpi{'map_name_fld'}} 
              || $fields->{$mpi{'map_name_fld'}} eq 'NULL');
  
  # Find map set record
  my $map_set_id = getMapSetID($dbh, $fields->{$mpi{'map_name_fld'}});
  if (!$map_set_id) {
    $msg = "ERROR: placeMarkerOnLG(): Unable to find record for map set " 
         . $fields->{$mpi{'map_name_fld'}};
    reportError($line_count, $msg);
    return;
  }
      
  # Find linkage group
  my $lg_name = makeLinkageMapName($fields->{$mpi{'map_name_fld'}}, 
                                   $fields->{$mpi{'lg_fld'}});
  my $lg_id = getFeatureID($dbh, $lg_name);
  if (!$lg_id) {
    $msg = "ERROR: placeMarkerOnLG(): Unable to find record for linkage group $lg_name.";
    reportError($line_count, $msg);
    return;
  }
  
  # Check for an existing featurepos record for this marker
  $sql = "
    SELECT featurepos_id FROM chado.featurepos
    WHERE featuremap_id=$map_set_id 
          AND feature_id=$marker_id 
          AND map_feature_id=$lg_id";
  logSQL($dataset_name, $sql);
  $sth = doQuery($dbh, $sql);
  if ($row=$sth->fetchrow_hashref) {
    $sql = "
      UPDATE chado.featurepos SET
        mappos = " . $fields->{$mpi{'position_fld'}} . "
      WHERE featurepos_id = " . $row->{'featurepos_id'};
  }
  else {
    $sql = "
      INSERT INTO chado.featurepos
        (featuremap_id, feature_id, map_feature_id, mappos)
      VALUES
        ($map_set_id, $marker_id, $lg_id, " . $fields->{$mpi{'position_fld'}} . ")";
  }
  logSQL($dataset_name, $sql);
  doQuery($dbh, $sql);
}#placeMarkerOnLG


sub setGeneticCoordinates {
  my ($dbh, $feature_id, $mapset, $mapname, $fields) = @_;
  my ($sql, $sth);
  
  my $mapset_id = getMapSetID($dbh, $mapset);
  if (!$mapset_id) {
    print "ERROR: no mapset record for $mapset. Unable to continue.\n\n";
    exit;
  }

  if (isFieldSet($fields, $mi{'map_start_fld'}, 1)) {
    # Insert map start position (start coordinate)
    setLgCoord($dbh, $mapset_id, $feature_id, $mapname, 'start', $fields->{$mi{'map_start_fld'}});
 
    # insert map end position (stop coordinate)
    setLgCoord($dbh, $mapset_id, $feature_id, $mapname, 'stop', $fields->{$mi{'map_end_fld'}});
  }
  else {
    print "warning: no map position provided for $mapname.\n";
  }
}#setGeneticCoordinates

        
sub setLgCoord {
  my ($dbh, $mapset_id, $lg_id, $lg_name, $coord, $mappos) = @_;
  
  my $sql = "
    SELECT fp.featurepos_id FROM chado.featurepos fp
      INNER JOIN chado.featuremap m 
        ON m.featuremap_id = fp.featuremap_id
      INNER JOIN chado.featureposprop fpp 
        ON fpp.featurepos_id = fp.featurepos_id
    WHERE fp.feature_id = $lg_id
          AND fpp.type_id=(SELECT cvterm_id FROM chado.cvterm 
                           WHERE name='$coord'
                                 AND cv_id=(SELECT cv_id FROM chado.cv 
                                            WHERE name='featurepos_property'))";
  logSQL('', $sql);
  my $sth = doQuery($dbh, $sql);
  if (my $row=$sth->fetchrow_hashref) {
    # Update position
    $sql = "
      UPDATE chado.featurepos SET
        mappos = $mappos
      WHERE featurepos_id = " . $row->{'featurepos_id'};
    logSQL($dataset_name, $sql);
    doQuery($dbh, $sql);
  }
  else {
    $sql = "
      INSERT INTO chado.featurepos
       (featuremap_id, feature_id, map_feature_id, mappos)
      VALUES
       ($mapset_id, $lg_id, $lg_id, $mappos)
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
          WHERE name='$coord'
            AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='featurepos_property')),
         '', 1)";
    logSQL($dataset_name, $sql);
    doQuery($dbh, $sql);
  }
}#setLgCoord


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


# Update linkage group if exists, create if not.
sub updateLinkageGroups {
  my ($dbh, $mapset, %lgs) = @_;
  my ($msg, $sql, $sth, $row);
  
  foreach my $lg (keys %lgs) {
    my $lg_id = getFeatureID($dbh, $lg);
    if ($lg_id) {
      # verify that lengths are the same
      my $start = getLgCoord($dbh, $lg, 'start');
      my $end   = getLgCoord($dbh, $lg, 'stop');
      if ($lgs{$lg}{'map_start'} < $start) {
        print "Warning: the calculated start for linkage group $lg ";
        print "(" . $lgs{$lg}{'map_start'} .") is less than ";
        print "what has already been set in the database ($start). ";
        print "Update? (y/n) ";
        my $userinput =  <STDIN>;
        chomp ($userinput);
        if (!($userinput =~ /^y.*/)) {
          next;
        }
        changeLGcoord($dbh, $lg_id, $lgs{$lg}{'map_start'}, 'start');
      }
      if ($lgs{$lg}{'map_end'} > $end) {
        print "Warning: the calculated end for linkage group $lg ";
        print "(" . $lgs{$lg}{'map_end'} .") is greater than ";
        print "is greater than what has already been set in the database ($end). ";
        print "Update? (y/n) ";
        my $userinput =  <STDIN>;
        chomp ($userinput);
        if (!($userinput =~ /^y.*/)) {
          next;
        }
        changeLGcoord($dbh, $lg_id, $lgs{$lg}{'map_end'}, 'stop');
      }
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
    if ($lg_id) {
      $sql = "
        UPDATE feature SET
          organism_id = $organism_id,
          name = '$lg',
          uniquename = '$lg',
          type_id = (SELECT cvterm_id FROM cvterm 
            WHERE name='linkage_group' 
                  AND cv_id=(SELECT cv_id FROM cv WHERE name='sequence'))
        WHERE feature_id = $lg_id";
    } 
    else {
      $sql = "
        INSERT INTO feature
          (organism_id, name, uniquename, type_id)
        VALUES
          ($organism_id, '$lg', '$lg',
           (SELECT cvterm_id FROM cvterm 
            WHERE name='linkage_group' 
                  AND cv_id=(SELECT cv_id FROM cv WHERE name='sequence'))
          )
        RETURNING feature_id";
    }
    
    logSQL($dataset_name, $sql);
    $sth = doQuery($dbh, $sql);
    $row = $sth->fetchrow_hashref;
    $lg_id = $row->{'feature_id'};

    # Set assigned linkage group (the short version, e.g. 'A01')
    
    my $lg_name = $lgs{$lg}{'lg'};

    # Check if this featureprop already exists
    $sql = "
      SELECT featureprop_id FROM chado.featureprop
      WHERE feature_id=$lg_id 
            AND type_id = (SELECT cvterm_id FROM chado.cvterm 
                           WHERE name='Assigned Linkage Group'
                                 AND cv_id = (SELECT cv_id FROM chado.cv 
                                              WHERE name='feature_property'))";
    logSQL($dataset_name, $sql);
    $sth = doQuery($dbh, $sql);
    if ($row=$sth->fetchrow_hashref) {
      $sql = "
        UPDATE chado.featureprop SET
          value='$lg_name'
        WHERE featureprop_id = " . $row->{'featureprop_id'};
    }
    else {
      $sql = "
        INSERT INTO chado.featureprop
          (feature_id, type_id, value, rank)
        VALUES
          ($lg_id,
           (SELECT cvterm_id FROM chado.cvterm 
            WHERE name='Assigned Linkage Group'
                  AND cv_id = (SELECT cv_id FROM chado.cv 
                               WHERE name='feature_property')),
           '$lg_name', 
           1)";
    }
    
    logSQL($dataset_name, $sql);
    doQuery($dbh, $sql);
    
    # Set coordinates
    my $mapset_id = getMapSetID($dbh, $mapset);
    setLgCoord($dbh, $mapset_id, $lg_id, $lg_name, 'start', $lgs{$lg}{'map_start'});
    setLgCoord($dbh, $mapset_id, $lg_id, $lg_name, 'stop', $lgs{$lg}{'map_end'});
  }#each lg
}#updateLinkageGroups



# Update marker if exists, create if not.
sub updateMarker {
  my ($dbh, $marker_name, $species, $changespecies) = @_;
  my ($sql, $sth, $row);
print "Check marker $marker_name, which is in species, $species. Will update species assignment if needed.\n";
  
  # Does this marker exist for the given species?
  my $marker_id = markerExists($dbh, $marker_name, $species);
  if ($marker_id) {
    return $marker_id;
  }
  else {
    my $organism_id = getOrganismID($dbh, $species);
print "Got organism id $organism_id for $species.\n";
    my $unique_marker_name = makeMarkerName($species, $marker_name);
    
    if ($changespecies) {
      # check if marker exists for a different species
      my @marker_list = getMarkerNameIDs($dbh, $marker_name);
      if (scalar @marker_list > 1) {
        print "\nWarning: there is more than one marker of this name. ";
        print "Don't know which one to fix. IDs = (";
        print (join ',', @marker_list) . ")\n";
        next;
      }
      elsif (scalar @marker_list == 0) {
        print "\nWarning: marker name not found!\n";
        next;
      }
      
      $sql = "
        UPDATE chado.feature
        SET
          organism_id=$organism_id
        WHERE
          feature_id = " . $marker_list[0] . "
        RETURNING feature_id";
    }
    else {
      $sql = "
        INSERT INTO chado.feature
          (organism_id, name, uniquename, type_id)
        VALUES
          ($organism_id,
           '$marker_name',
           '$unique_marker_name',
           (SELECT cvterm_id FROM cvterm 
            WHERE name='genetic_marker' 
                  AND cv_id = (SELECT cv_id FROM cv WHERE name='sequence'))
          )
        RETURNING feature_id";
    }
    logSQL($dataset_name, $sql);
    $sth = doQuery($dbh, $sql);
    $row = $sth->fetchrow_hashref;
    $marker_id = $row->{'feature_id'};
  }
  
  return $marker_id;
}#updateMarker
