# file: load_markers.pl
#
# purpose: Load spreadsheet marker data into a chado database.
#          
#          This loading script operates differently than the others because
#          marker data can come from multiple sources. When an existing marker
#          is found, it is not cleared and reloaded, but all fields and 
#          associated data are updated. Existing fields and associated data  
#          are not deleted if no data for them exists in the spreadsheet.
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
  
  
  #######################################################################################
  # IMPORTANT NOTE: as functionality is added, make sure it is reflected in 
  #                 the scripts 0_verifyWorksheets.pl, deletePubData.pl, 
  #                 and dumpSpreadsheet.pl.
  
  print "warning: this script has not been fully implemented and/or tested to handle:\n";
  print "  > physicial positions\n";
  print "  > updating existing records\n";
#  my $userinput =  <STDIN>;

  ########################################################################################
  
  
  my $input_dir = @ARGV[0];
  my @filepaths = <$input_dir/*.txt>;
  my %files = map { s/$input_dir\/(.*)$/$1/; $_ => 1} @filepaths;
  
  # get worksheet contants
  my %mki  = getSSInfo('MARKERS');
  my %mpi  = getSSInfo('MARKER_POSITION');
  my %msi  = getSSInfo('MARKER_SEQUENCE');

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
    loadMarkerSequence($dbh);
  
# commented-out until script is working properly  
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
  print "\n\n";



################################################################################
####### Major functions                                                #########
################################################################################
sub loadMarkers {
  my $dbh = $_[0];
  
  my ($fields, $sql, $sth, $row, $msg);
  my ($skip, $skip_all, $update, $update_all);

  $table_file = "$input_dir/MARKER.txt";
  print "Loading/verifying $table_file...\n";
  
  @records = readFile($table_file);
  print "\nLoading " . (scalar @records) . " markers...\n";
  
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
    my $species = $fields->{$mki{'species_fld'}};
    my $marker_name = $fields->{$mki{'marker_name_fld'}};
    my $unique_marker_name = makeMarkerName($species, $marker_name);
#print "$line_count: handle marker $marker_name\n";
    
    # likely to be duplicates that haven't been cleaned out of the spreadsheet
    if ($existing_markers{$marker_name}) {
# for expediency, just skip all of these for now.
#      print "$line_count: The marker $marker_name has already been "
#            . "loaded from this spreadsheet. Skip or update? (u/s/q)\n";
#      my $userinput =  <STDIN>;
#      chomp ($userinput);
#      if ($userinput eq 's') {
        next;
#     }
#     elsif ($userinput eq 'q') {
#       exit;
#     }
    }#already loaded from spreadsheet
    
    elsif ($marker_id=markerExists($dbh, $unique_marker_name, $species)) {
      
      # need this here (it already exists)
      $existing_markers{$marker_name} = $species;

      if ($skip_all) {
        next;
      }
      if (!$update_all) {
        my $prompt =  "$line_count: This marker ($marker_name)";
        ($skip, $skip_all, $update, $update_all) = checkUpdate($prompt);
        if ($skip || $skip_all) {
          next;
        }
      }
    }#marker already exists

    # Genbank_accession (if any)
    my $primary_dbxref_id = setPrimaryDbxref($dbh, $fields, 'genbank:nuccore');

    # specieslink_abv, marker_name, source_sequence: feature
    $marker_id = setMarkerRec($dbh, $marker_id, $primary_dbxref_id, $fields);
    
    # Synonyms
    attachSynonyms($dbh, $marker_id, $mki{'alt_name_fld'}, $fields);
    
    # assembly_name, phys_chr, phys_start: featureloc
    setPhysicalPosition($dbh, $marker_id, $fields);
    
    # marker_type and comment (marker comments are rank=1)
    setFeatureprop($dbh, $marker_id, $mki{'marker_type_fld'}, 'Marker Type', 1, $fields);
    setFeatureprop($dbh, $marker_id, $mki{'comment_fld'}, 'comment', 1, $fields);
    
    # add need this here (NOW it exists if it didn't already)
    $existing_markers{$marker_name} = $species;
  }#each record
  
  print "\n\nLoaded $line_count markers.\n\n";
}#loadMarkers


sub loadMarkerSequence {
  my $dbh = $_[0];
  
  my ($fields, $sql, $sth, $row, $msg);
  my ($skip, $skip_all, $update, $update_all);

  $table_file = "$input_dir/MARKER_SEQUENCE.txt";
  print "Loading/verifying $table_file...\n";
  
  @records = readFile($table_file);
  $line_count = 0;
  
  print "\nLoading " . (scalar @records) . " marker sequence records...\n";
  
  foreach $fields (@records) {
    $line_count++;
    
    my $marker_name = $fields->{$msi{'marker_name_fld'}};
#print "$line_count: handle sequence for $marker_name\n";

    # If this marker has not been loaded already, skip it
    if (!$existing_markers{$marker_name}) {
      print "skipping sequence data marker $marker_name because it doesn't exist\n";
      next;
    }#no marker record

    my $species = $existing_markers{$marker_name};
    my $unique_marker_name = makeMarkerName($species, $marker_name);
    my $marker_id = markerExists($dbh, $unique_marker_name, $species);
    
    setMarkerSequence($dbh, $marker_id, $fields);
    
    # sequence_type and comment (marker-sequence comments are rank=2)
    setFeatureprop($dbh, $marker_id, $msi{'sequence_type_fld'}, 'Sequence Type', 1, $fields);
    setFeatureprop($dbh, $marker_id, $msi{'comment_fld'}, 'comment', 2, $fields);

    # forward_primer, reverse_primer
    loadPrimers($dbh, $marker_id, $marker_name, $species, $fields);

  }#each record
}#loadMarkerSequence


################################################################################
################################################################################
################################################################################

sub attachSynonyms {
  my ($dbh, $marker_id, $fieldname, $fields) = @_;
  
  my ($msg, $sql, $sth, $row);
  
  if (isFieldSet($fields, $fieldname)) {
    my @synonyms = split /,/, $fields->{$fieldname};
    foreach my $syn (@synonyms) {
      my $synonym_id;
      if ($synonym_id=getSynonym($dbh, $syn, 'Marker Synonym')) {
        # this synonym already exists; see what it's attached to
        $sql = "
          SELECT name, feature_id FROM feature f
            INNER JOIN feature_synonym fs ON fs.feature_id=f.feature_id
            INNER JOIN synonym s ON s.synonym_id=fs.synonym_id
          WHERE s.synonym_id = $synonym_id
                AND s.type_id=(SELECT cvterm_id FROM cvterm 
                               WHERE name='marker_synonyn' 
                                     AND cv_id=(SELECT cv_id FROM cv 
                                                WHERE name='synonym_type'))";
        logSQL($dataset_name, $sql);
        $sth = doQuery($dbh, $sql);
        if (my $row=$sth->fetchrow_hashref) {
          $msg = "ERROR: The synonym, $syn, has already been used by feature ";
          $msg .= $row->{'name'} . "(id: " . $row->{'feature_id'} . ")";
          reportError($line_count, $msg);
          next;
        }
        
        # In the unlikely event that this synonym already exists but isn't 
        #   attached to a feature, ignore; the same synonym can exist if
        #   different types.
        
      }#duplicate synonym found
      
      $sql = "
        INSERT INTO synonym
          (name, type_id, synonym_sgml)
        VALUES
          ('$syn', 
           (SELECT cvterm_id FROM cvterm 
            WHERE name='Marker Synonym' 
                 AND cv_id=(SELECT cv_id FROM cv WHERE name='synonym_type')),
           '$syn')
        RETURNING synonym_id";
       logSQL($dataset_name, $sql);
       $sth = doQuery($dbh, $sql);
       $row = $sth->fetchrow_hashref;
       $synonym_id = $row->{'synonym_id'};
       
       $sql = "
         INSERT INTO feature_synonym
           (synonym_id, feature_id, pub_id)
         VALUES
           ($synonym_id, $marker_id,
            (SELECT pub_id FROM pub WHERE uniquename='null'))";
       logSQL($dataset_name, $sql);
       doQuery($dbh, $sql);
    }#each synonym
  }#synonym given
}#attachSynonyms


sub loadPrimers {
  my ($dbh, $marker_id, $marker_name, $species, $fields) = @_;
  my ($sequence, $seqlen, $msg, $row, $sth, $sql);
  
  loadPrimer($dbh, $marker_id, $marker_name, $species, 'forward_primer', 
             $msi{'forward_primer_seq_fld'}, $msi{'forward_primer_name_fld'}, 
             $fields);
  loadPrimer($dbh, $marker_id, $marker_name, $species, 'reverse_primer', 
             $msi{'reverse_primer_seq_fld'}, $msi{'reverse_primer_name_fld'}, 
             $fields);
}#loadPrimers


sub loadPrimer {
  my ($dbh, $marker_id, $marker_name, $species, $primer_type, 
      $seq_field, $name_field, $fields) = @_;
  
  my $organism_id = getOrganismID($dbh, $species, $line_count);
  
  if (isFieldSet($fields, $seq_field)) {
    my $sequence = $fields->{$seq_field};
    my $seqlen = length($sequence);
    
    my $primer_name;
    if (isFieldSet($fields, $name_field)) {
      $primer_name = $fields->{$name_field};
    }
    else {
      my $primer_suffix = ($primer_type eq 'forward_primer') ? 'fprimer' : 'rprimer';
      $primer_name = "$marker_name.$primer_suffix";
    }
    my $unique_primer_name = makeMarkerName($species, $primer_name);
    
    if (primerExists($organism_id, $unique_primer_name, $primer_type)) {
# TODO: if primer sequence needs to be changed, this will need to do an update
      print "The primer $primer_name has already been loaded. Skipping. \n";
      next;
    }
    
    $sql = "
      INSERT INTO feature
        (organism_id, name, uniquename, residues, seqlen, type_id)
      VALUES
        ($organism_id, 
         '$primer_name', 
         '$unique_primer_name', 
         '$sequence', 
         $seqlen,
         (SELECT cvterm_id FROM cvterm 
          WHERE name = '$primer_type' 
                AND cv_id = (SELECT cv_id FROM cv WHERE name='sequence'))
        )";
    logSQL($dataset_name, $sql);
    doQuery($dbh, $sql);
  }#primer provided in worksheet
}#loadPrimer


sub placeMarkerOnLG {
  my ($dbh, $marker_id, $fields) = @_;
  my ($msg, $row, $sth, $sql);
  
  # Find map set record
  my $map_set_id = getMapSetID($dbh, $fields->{$mki{'map_name_fld'}});
  if (!$map_set_id) {
    $msg = "ERROR: Unable to find record for map set " 
         . $fields->{$mki{'map_name_fld'}};
    reportError($line_count, $msg);
    return;
  }
      
  # Find linkage group
  my $lg_name = makeLinkageMapName($fields->{$mki{'map_name_fld'}}, 
                                   $fields->{$mki{'lg_fld'}});
  my $lg_id = getFeatureID($dbh, $lg_name);
  if (!$lg_id) {
    $msg = "ERROR: Unable to find record for linkage group $lg_name.";
    reportError($line_count, $msg);
    return;
  }
  
  $sql = "
    INSERT INTO featurepos
      (featuremap_id, feature_id, map_feature_id, mappos)
    VALUES
      ($map_set_id, $marker_id, $lg_id, $fields->{$mki{'position_fld'}})";
  logSQL($dataset_name, $sql);
  doQuery($dbh, $sql);
}#placeMarkerOnLG


sub primerExists {
  my ($organism_id, $uniquename, $type) = @_;
  my ($sql, $sth, $row);
  $sql = "
    SELECT feature_id FROM feature
    WHERE organism_id=$organism_id
          AND uniquename='$uniquename'
          AND type_id = (SELECT cvterm_id FROM cvterm 
                          WHERE name='$type' 
                                AND cv_id=(SELECT cv_id FROM cv 
                                           WHERE name='sequence'))";
  logSQL($dataset_name, $sql);
  $sth = doQuery($dbh, $sql);
  if ($row=$sth->fetchrow_hashref) {
    return $row->{'feature_id'};
  }
  else {
    return 0;
  }
}#primerExists


sub setFeatureprop {
  my ($dbh, $marker_id, $fieldname, $typename, $rank, $fields) = @_;
  my ($sql, $sth);
  
  if (isFieldSet($fields, $fieldname)) {
    my $value = $fields->{$fieldname};
    
    # Check if this featureprop type already exists
    $sql = "
      SELECT MAX(rank) FROM chado.featureprop
      WHERE feature_id=$marker_id 
            AND type_id = (SELECT cvterm_id FROM chado.cvterm 
                           WHERE name='$typename'
                             AND cv_id = (SELECT cv_id FROM chado.cv 
                                          WHERE name='feature_property'))
            AND value='$value'";
    logSQL($dataset_name, $sql);
    $sth = doQuery($dbh, $sql);
    if ($row=$sth->fetchrow_hashref) {
      # don't re-set or add a duplicate property
      return;
    }
    
    if ($rank == -1) {
      # Set the rank based on existing featureprop records
      $sql = "
        SELECT MAX(rank) AS rank FROM chado.featureprop
        WHERE feature_id=$marker_id 
              AND type_id = (SELECT cvterm_id FROM chado.cvterm 
                             WHERE name='$typename'
                               AND cv_id = (SELECT cv_id FROM chado.cv 
                                            WHERE name='feature_property'))";
      logSQL($dataset_name, $sql);
      $sth = doQuery($dbh, $sql);
      $rank =  ($row=$sth->fetchrow_hashref) ? $row->{'rank'}++ : 1;
    }#rank is caculated
    
    else {
      # If this featureprop already exists, change it
      $sql = "
        SELECT featureprop_id FROM chado.featureprop
        WHERE feature_id=$marker_id 
              AND type_id = (SELECT cvterm_id FROM chado.cvterm 
                             WHERE name='$typename'
                               AND cv_id = (SELECT cv_id FROM chado.cv 
                                            WHERE name='feature_property'))
              AND value='$value'";
      logSQL($dataset_name, $sql);
      $sth = doQuery($dbh, $sql);
      if ($row=$sth->fetchrow_hashref) {
        # This featureprop needs to be updated
        $sql = "
          UPDATE chado.featureprop SET
            value = '$value'
          WHERE featureprop_id = " . $row->{'featureprop_id'};
      }
      else {
        # This featureprop needs to be created
        $sql = "
          INSERT INTO chado.featureprop
            (feature_id, type_id, value, rank)
          VALUES
            ($marker_id,
             (SELECT cvterm_id FROM chado.cvterm 
              WHERE name='$typename'
                AND cv_id = (SELECT cv_id FROM chado.cv 
                    WHERE name='feature_property')),
             '$value', $rank)";
      }
    }#rank is fixed
    
    logSQL($dataset_name, $sql);
    doQuery($dbh, $sql);
  }#value for fieldname exists
}#setFeatureprop


sub setMarkerRec {
  my ($dbh, $marker_id, $primary_dbxref_id, $fields) = @_;
  my ($sql, $sth, $row);
  
  my $species = $fields->{$mki{'species_fld'}};
  my $marker_name = $fields->{$mki{'marker_name_fld'}};
  my $unique_marker_name = makeMarkerName($species, $marker_name);

  my $organism_id = getOrganismID($dbh, $fields->{$mki{'species_fld'}}, $line_count);
  
  if ($existing_markers{$marker_name}) {
    my $dbxref_clause = ($primary_dbxref_id) ? "dbxref_id = $primary_dbxref_id," : '';
    $sql = "
      UPDATE chado.feature SET
        $dbxref_clause
        organism_id=$organism_id,
        name='$marker_name',
        uniquename='$unique_marker_name'
      WHERE feature_id=$marker_id";
  }
  else {
    my $dbxref = ($primary_dbxref_id) ? $primary_dbxref_id : 'NULL';
    $sql = "
      INSERT INTO chado.feature
        (organism_id, name, uniquename, type_id, dbxref_id)
      VALUES
        ($organism_id, 
         '$marker_name', 
         '$unique_marker_name',
         (SELECT cvterm_id FROM chado.cvterm 
          WHERE name='genetic_marker'
            AND cv_id = (SELECT cv_id FROM chado.cv WHERE name='sequence')),
         $dbxref
        )
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


sub setMarkerSequence {
  my ($dbh, $marker_id, $fields) = @_;
  my ($sql, $sth);

  if (isFieldSet($fields, $msi{'marker_sequence_fld'})) {
    my $sequence = qw($fields->{$msi{'marker_sequence_fld'}});
    my $seqlen = length($sequence);
   
    $sql = "
      UPDATE chado.feature SET
        residues=$sequence,
        seqlen=$seqlen
      WHERE feature_id=$marker_id";
    logSQL($dataset_name, $sql);
    $sth = doQuery($dbh, $sql);
  }
}#setMarkerSequence


sub setPhysicalPosition {
  my ($dbh, $marker_id, $fields) = @_;
  my ($msg, $row, $sql, $sth);

# TODO: Don't delete existing data if nothing is set for these fields.
#       Warn if values change? (Better done in verify script?)
  
  if (isFieldSet($fields, $mki{'phys_ver_fld'})) {
    my $assembly_id = getAssemblyID($dbh, $fields->{$mki{'phys_ver_fld'}});
print "Not yet implemented";
exit;

    if (!$assembly_id) {
      return;
    }
    
    my $start = $fields->{$mki{'phys_start'}};
    my $end   = $fields->{$mki{'phys_start'}}; 
    my $chr   = $fields->{$mki{'phys_chr_fld'}};
    my $ver   = $fields->{$mki{'assembly_ver'}};
    my $chr_feature_id = getChromosomeID($dbh, $chr, $ver);
    if ($chr_feature_id == 0) {
      $chr_feature_id = getScaffoldID($dbh, $chr,  $ver);
      if ($chr_feature_id == 0) {
        $msg = "ERROR: Unable to find chromosome/scaffold feature $chr";
        $msg = "for assembly version $ver.";
        reportError($line_count, $msg);
      }
# TODO: featureloc record may already exist and neet updating
      $sql = "
        INSERT INTO chado.featureloc
          (feature_id, srcfeature_id, fmin, fmax)
        VALUES
          ($marker_id, $chr_feature_id, $start, $end)";
      logSQL($dataset_name, $sql);
      doQuery($dbh, $sql);
    }#physical chromosome found
  }#physical position information provided

}#setPhysicalPosition


sub setPrimaryDbxref {
  my ($dbh, $fields, $dbname) = @_;
  my ($sql, $sth, $row);
  
  my $primary_dbxref_id = 0;
  if (isFieldSet($fields, $mki{'Genbank_acc_fld'})) {
    my $acc = $fields->{$mki{'Genbank_acc_fld'}};
    # Only load if not already in db
    if (!($primary_dbxref_id = dbxrefExists($dbh, $dbname, $acc))) {
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
  my ($dbh, $marker_id, $fieldname, $dbname, $fields) = @_;
  my ($sql, $sth, $row);
  
  if (isFieldSet($fields, $fieldname)) {
    my $acc = $fields->{$fieldname};
    my $dbxref_id = dbxrefExists($dbh, $dbname, $acc);
    if (!$dbxref_id) {
      $sql = "
        INSERT INTO chado.dbxref
          (db_id, accession)
        VALUES
          ((SELECT db_id FROM chado.db 
            WHERE name='$dbname'),
           '$acc') 
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


sub setSequence {
  my ($dbh, $marker_id, $fieldname, $dbname, $fields) = @_;
  
  my $sequence = 'NULL';
  my $seqlen  = 'NULL';
  if (isFieldSet($fields, $mki{'sequence_fld'})) {
    $sequence = qw($fields->{$mki{'sequence_fld'}});
    $seqlen = length($sequence);
  }
  
  $sql = "
    UPDATE chado.feature SET
      residues=$sequence
      seqlen=$seqlen
    WHERE feature_id=$marker_id";
}#setSequence

