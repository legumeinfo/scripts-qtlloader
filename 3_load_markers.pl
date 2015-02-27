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
  
  
  #######################################################################################
  # IMPORTANT NOTE: as functionality is added, make sure it is reflected in 
  #                 cleanDependants(), and the scripts 0_verifyWorksheets.pl, 
  #                 deletePubData.pl, dumpSpreadsheet.pl.
  
  print "warning: this script has not been fully implemented and/or tested to handle:\n";
  print "  > physicial positions\n";
  print "  > primers\n";
  print "  > updating existing records\n";
#  my $userinput =  <STDIN>;

  ########################################################################################
  
  
  my $input_dir = @ARGV[0];
  my @filepaths = <$input_dir/*.txt>;
  my %files = map { s/$input_dir\/(.*)$/$1/; $_ => 1} @filepaths;
  
  # get worksheet contants
  my %mki  = getSSInfo('MARKERS');

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
  my ($skip, $skip_all, $update, $update_all);

  $table_file = "$input_dir/MARKERS.txt";
  print "Loading/verifying $table_file...\n";
  
  @records = readFile($table_file);
  
  # build linkage groups from the markers
  my %lgs = createLinkageGroups(@records);
  loadLGs($dbh, %lgs);

  $line_count = 0;
  
  print "\nLoading " . (scalar @records) . " markers...\n";
  
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
print "$line_count: handle marker $marker_name\n";
    
    if ($marker_id=markerExists($dbh, $marker_name, $fields->{'specieslink_abv'})) {
      if ($skip_all) {
        next;
      }
      if ($update_all) {
        cleanDependants($dbh, $marker_id);
      }
      else {
        my $prompt =  "$line_count: This marker ($marker_name)";
        ($skip, $skip_all, $update, $update_all) = checkUpdate($prompt);
        if ($skip || $skip_all) {
          next;
        }
        if ($update || $update_all) {
          cleanDependants($dbh, $marker_id);
        }
      }
    }#marker exists

    # Genbank_accession
    my $primary_dbxref_id = setPrimaryDbxref($dbh, $fields, 'genbank:nuccore');
    if (!$primary_dbxref_id) {
      $primary_dbxref_id = 'NULL';
    }

    # specieslink_abv, marker_name, source_sequence: feature
    $marker_id = setMarkerRec($dbh, $marker_id, $primary_dbxref_id, $fields);
    
    # CMap link
    setSecondaryDbxref($dbh, $marker_id, $mki{'cmap_acc_fld'}, 'LIS:cmap', $fields);
    
    # Synonyms
    attachSynonyms($dbh, $marker_id, $mki{'alt_name_fld'}, $fields);
    
    # Place on linkage group
    placeMarkerOnLG($dbh, $marker_id, $fields);
    
    # forward_primer, reverse_primer
    loadPrimers($dbh, $marker_id, $fields);

    # assembly_ver, phys_chr, phys_start: featureloc
    setPhysicalPosition($dbh, $marker_id, $fields);
    
    # marker_type and comment
    setFeatureprop($dbh, $marker_id, $mki{'marker_type_fld'}, 'Marker Type', 1, $fields);
    setFeatureprop($dbh, $marker_id, $mki{'comment_fld'}, 'comment', 1, $fields);

  }#each record
  
  print "\n\nLoaded $line_count markers.\n\n";
}#loadMarkers


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
          SELECT name FROM feature f
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
          $msg .= $row->{'name'};
        }
        else {
          $msg = "ERROR: The synonym, $syn, is already in use, but isn't attached ";
          $msg .= "to a feature. Please check."
        }
        reportError($line_count, $msg);
        next;
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


sub cleanDependants {
  my ($marker_id) = @_;
  
  # dbxrefs for GenBank and CMap including dbxref and feature_dbxref
  # synonyms, including synonym & feature_synonym
  # DON'T delete lgs?
  # featurepos
  # featureprops
  # primer features
  
}#cleanDependants;


sub loadLGs {
  my ($dbh, %lgs) = @_;
  my ($msg, $row, $sth, $sql);
  
  foreach my $lg (keys %lgs) {
#print "Handle lg $lg: " . Dumper($lgs{$lg});
    my $lg_id = getFeatureID($dbh, $lg);
    if ($lg_id) {
      # verify that lengths are the same
      next;
    }
    my $organism_id = getOrganismID($dbh, $lgs{$lg}{'species'}, '');
    if (!$organism_id) {
      $msg = "ERROR: Unable to find a record for species "
           . $lgs{$lg}{'species'};
      reportError($line_count, $msg);
      next;
    }
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
    
    setFeatureProp($dbh, $marker_id, $mki{'lg_fld'}, 
                   'Assigned Linkage Group', 1, $fields);
  }#each lg
}#loadLGs


sub loadPrimers {
  my ($dbh, $marker_id, $fields) = @_;
  my ($sequence, $seqlen, $msg, $row, $sth, $sql);
  
  my $organism_id = getOrganismID($dbh, $fields->{$mki{'species_fld'}}, $line_count);
  if (isFieldSet($fields, $mki{'fwd_primer_seq'})) {
    $sequence = $fields->{$mki{'fwd_primer_seq'}};
    $seqlen = length($sequence);
    $sql = "
      INSERT INTO feature
        (organism_id, name, uniquename, residues, seqlen, type_id)
      VALUES
        ($organism_id, '$sequence', '$sequence', '$sequence', $seqlen,
         SELECT cvterm_id FROM cvterm 
         WHERE name = 'forward_primer' 
               AND cv_id = (SELECT cv_id FROM cv WHERE name='sequence_id')";
    logSQL($dataset_name, $sql);
    doQuery($dbh, $sql);
  }#forward primer provided

  if (isFieldSet($fields, $mki{'bkwd_primer_seq'})) {
    $sequence = $fields->{$mki{'bkwd_primer_seq'}};
    $seqlen = length($sequence);
    $sql = "
      INSERT INTO feature
        (organism_id, name, uniquename, residues, seqlen, type_id)
      VALUES
        ($organism_id, '$sequence', '$sequence', '$sequence', $seqlen,
         SELECT cvterm_id FROM cvterm 
         WHERE name = 'reverse_primer' 
               AND cv_id = (SELECT cv_id FROM cv WHERE name='sequence_id')";
    logSQL($dataset_name, $sql);
    doQuery($dbh, $sql);
  }#backward primer provided
}#loadPrimers


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


sub setFeatureprop {
  my ($dbh, $marker_id, $fieldname, $typename, $rank, $fields) = @_;
  my ($sql, $sth);
  
  if (isFieldSet($fields, $fieldname)) {
    $sql = "
      INSERT INTO chado.featureprop
        (feature_id, type_id, value, rank)
      VALUES
        ($marker_id,
         (SELECT cvterm_id FROM chado.cvterm 
          WHERE name='$typename'
            AND cv_id = (SELECT cv_id FROM chado.cv WHERE name='feature_property')),
         '$fields->{$fieldname}', $rank)";
    logSQL($dataset_name, $sql);
    doQuery($dbh, $sql);
  }#value for fieldname exists
}#setFeatureprop


sub setMarkerRec {
  my ($dbh, $marker_id, $primary_dbxref_id, $fields) = @_;
  
  my $marker_name = $fields->{$mki{'marker_name_fld'}};
  my $organism_id = getOrganismID($dbh, $fields->{$mki{'species_fld'}}, $line_count);
  
  my $sequence = 'NULL';
  my $seqlen  = 'NULL';
  if (isFieldSet($fields, $mki{'sequence_fld'})) {
    $sequence = qw($fields->{$mki{'sequence_fld'}});
    $seqlen = length($sequence);
  }
  
  if ($existing_markers{$marker_name}) {
    $sql = "
      UPDATE chado.feature SET
        dbxref_id=$primary_dbxref_id,
        organism_id=$organism_id,
        name='$marker_name',
        uniquename='$marker_name',
        residues=$sequence
        seqlen=$seqlen
      WHERE feature_id=$marker_id";
  }
  else {
    $sql = "
      INSERT INTO chado.feature
        (dbxref_id, organism_id, name, uniquename, residues, seqlen, type_id)
      VALUES
        ($primary_dbxref_id, $organism_id, 
         '$marker_name', 
         '$marker_name',
         $sequence,
         $seqlen,
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


sub setPhysicalPosition {
  my ($dbh, $marker_id, $fields) = @_;
  my ($msg, $row, $sql, $sth);
  
  if (isFieldSet($fields, $mki{'phys_ver_fld'})) {
    my $assembly_id = getAssemblyID($dbh, $fields->{$mki{'phys_ver_fld'}});
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



