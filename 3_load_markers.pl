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
#  09/08/15  eksc  modified for pre-v2 worksheet design

# Rules:
#   primerx columns must start with 1 and be in numerical order
#   probex columns must start with 1 and be in numerical order
#   database/accession_database value must correspond to a db record
#   primer names must be unique (should marker and primer names be case-sensitive?)
#   check for duplicate names already loaded, using a near match rule too, need
#     to allow curator to mark one canonical.


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
  
  # get worksheet contants
  my %mki  = getSSInfo('MARKERS');

  # Used all over
  my ($table_file, $sql, $sth, $row, $count, @records, @fields, $cmd, $rv);
  my ($has_errors, $line_count);

  my $dataset_name = 'markers';
    
  # Get connected
  my $dbh = connectToDB;

  # set default schema
  $sql = "SET SEARCH_PATH = chado";
  $sth = $dbh->prepare($sql);
  $sth->execute();

  # Holds markers that are already in db; assume they should be updated if
  #   any of these appear in the worksheet.
  my %existing_markers = getExistingMarkers($dbh);
  
  # keeps track of markers already seen in worksheet.
  my %markers_in_ws;

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

  $sth->finish;
  # ALL DONE
  #$dbh->disconnect();
  print "\n\n";



################################################################################
####### Major functions                                                #########
################################################################################
sub loadMarkers {
  my $dbh = $_[0];
  
  my ($fields, $sql, $sth, $row, $msg);
  my ($change_all, $skip, $skip_all, $update, $update_all);

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
    my $marker_name = $fields->{$mki{'marker_identifier_fld'}};
    my $unique_marker_name = makeMarkerName($species, $marker_name);
    my $marker_citation = $fields->{$mki{'pub_fld'}};
print "\n$line_count: handle marker $marker_name as described by $marker_citation\n";
    
    if ($markers_in_ws{$marker_name}) {
      print "$line_count: The marker $marker_name has already been "
            . "loaded from this spreadsheet. Skip or update? (u/s/q)\n";
      my $userinput =  <STDIN>;
      chomp ($userinput);
      if ($userinput eq 's') {
        next;
      }
      elsif ($userinput eq 'q') {
        exit;
      }
    }#already loaded from spreadsheet
    
    elsif ($marker_id=$existing_markers{"$unique_marker_name:$species"}) {
print"found $unique_marker_name:$species - $marker_id\n";
  
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
            
      clearMarkerDependancies($dbh, $marker_id);
    }#marker already exists

    # specieslink_abv, marker_name, marker_sequence
    $marker_id = setMarkerRec($dbh, $marker_id, $fields);
    
    # database, database_accession
    my $primary_dbxref_id 
      = setFeatureDbxref($dbh, $marker_id, $mki{'db_accession_fld'}, 
                         $fields->{$mki{'database_fld'}}, $fields);
    
    # marker_citation
    linkPubFeature($dbh, $marker_id, $marker_citation);
    
    # alias, source_publication_marker_name
    if ($fields->{$mki{'alias_fld'}} ne $marker_name) {
      attachSynonyms($dbh, $marker_id, $mki{'alias_fld'}, $marker_citation, $fields);
    }
    if ($fields->{$mki{'pub_marker_name_fld'}} ne $marker_name) {
      attachSynonyms($dbh, $marker_id, $mki{'pub_marker_name_fld'}, $marker_citation, $fields);
    }
    
    # marker_type
    setMarkerType($dbh, $marker_id, $fields->{$mki{'marker_type_fld'}}, $marker_citation);
    
    # primer*_name, primer*_seq
    loadPrimers($dbh, $marker_id, $marker_name, $species, $fields);

    # SSR_repeat_motif, source_description, restriction_enzyme, product_length, 
    #   max_length, min_length, PCR_condition, sequence_name, marker_source, 
    #   SNP_alleles, SNP_five_prime_flanking_sequence, 
    #   SNP_three_prime_flanking_sequence, comment
#TODO: marker_stock needs to be species
    setFeatureprop($dbh, $marker_id, $mki{'src_descr_fld'},             'Source Description', 1, $fields);
    setFeatureprop($dbh, $marker_id, $mki{'repeat_motif_fld'},          'Repeat Motif', 1, $fields);
    setFeatureprop($dbh, $marker_id, $mki{'sequence_name_fld'},         'Sequence Name', 1, $fields);
    setFeatureprop($dbh, $marker_id, $mki{'restriction_enzyme_fld'},    'Restriction Enzyme', 1, $fields);
    setFeatureprop($dbh, $marker_id, $mki{'product_length_fld'},        'Product Length', 1, $fields);
    setFeatureprop($dbh, $marker_id, $mki{'max_length_fld'},            'Max Length', 1, $fields);
    setFeatureprop($dbh, $marker_id, $mki{'min_length_fld'},            'Min Length', 1, $fields);
    setFeatureprop($dbh, $marker_id, $mki{'PCR_condition_fld'},         'PCR Condition', 1, $fields);
    setFeatureprop($dbh, $marker_id, $mki{'SNP_alleles_fld'},           'SNP Alleles', 1, $fields);
    setFeatureprop($dbh, $marker_id, $mki{'SNP_5_prime_flank_seq_fld'}, 'SNP 5-prime Flanking Sequence', 1, $fields);
    setFeatureprop($dbh, $marker_id, $mki{'SNP_3_prime_flank_seq_fld'}, 'SNP 3-prime Flanking Sequence', 1, $fields);
    setFeatureprop($dbh, $marker_id, $mki{'comment_fld'},               'comment', 1, $fields);
    
    # assembly_name, phys_chr, phys_start: featureloc
    setPhysicalPosition($dbh, $marker_id, $fields);  
  }#each record
  
  print "\n\nLoaded $line_count markers.\n\n";
}#loadMarkers


################################################################################
################################################################################
################################################################################

sub attachSynonyms {
  my ($dbh, $marker_id, $fieldname, $marker_citation, $fields) = @_;
  
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
            (SELECT pub_id FROM pub WHERE uniquename='$marker_citation'))";
       logSQL($dataset_name, $sql);
       doQuery($dbh, $sql);
       $sth->finish;
    }#each synonym
  }#synonym given
}#attachSynonyms


sub clearMarkerDependancies {
  my ($dbh, $marker_id) = @_;
  
  my ($sql, $sth);
  
  # delete dbxrefs (e.g. CMap link)
  $sql = "
    DELETE FROM dbxref 
    WHERE dbxref_id IN (SELECT dbxref_id FROM feature_dbxref 
                        WHERE feature_id=$marker_id)";
  logSQL($dataset_name, $sql);
  $sth = doQuery($dbh, $sql);
  
  $sql = "
    DELETE FROM feature_dbxref WHERE feature_id=$marker_id";
  logSQL($dataset_name, $sql);
  $sth = doQuery($dbh, $sql);

  # delete pub link (but not pub)
  $sql = "
    DELETE FROM feature_pub WHERE feature_id=$marker_id";
  logSQL($dataset_name, $sql);
  $sth = doQuery($dbh, $sql);

  # delete synonyms
  $sql = "
    DELETE FROM synonym 
    WHERE synonym_id IN (SELECT synonym_id FROM feature_synonym 
                         WHERE feature_id=$marker_id)";
  logSQL($dataset_name, $sql);
  $sth = doQuery($dbh, $sql);

  # delete feature_cvterm (marker type)
  $sql = "
    DELETE FROM feature_cvterm WHERE feature_id=$marker_id";
  logSQL($dataset_name, $sql);
  $sth = doQuery($dbh, $sql);

  # delete featureprops
  $sql = "
    DELETE FROM featureprop WHERE feature_id=$marker_id";
  logSQL($dataset_name, $sql);
  $sth = doQuery($dbh, $sql);
    
  # delete featureloc (physical position)
  $sql = "
    DELETE FROM featureloc WHERE feature_id=$marker_id";
  logSQL($dataset_name, $sql);
  $sth = doQuery($dbh, $sql);
}#clearMarkerDependancies


sub getExistingMarkers {
  my $dbh = $_[0];
  
  my %markers;
  my $sql = "
    SELECT feature_id, name, d.accession AS mnemonic
    FROM feature f
      INNER JOIN organism o ON o.organism_id=f.organism_id 
      INNER JOIN chado.organism_dbxref od ON od.organism_id=o.organism_id 
      INNER JOIN chado.dbxref d on d.dbxref_id=od.dbxref_id 
    WHERE f.type_id=(SELECT cvterm_id FROM cvterm 
                   WHERE name='genetic_marker' AND cv_id=(SELECT cv_id FROM cv 
                                                          WHERE name='sequence'))
          AND d.db_id=(SELECT db_id FROM db WHERE name='uniprot:species')";
  logSQL($dataset_name, $sql);
  my $sth = doQuery($dbh, $sql);
  while (my $row=$sth->fetchrow_hashref) {
    my $key = $row->{'name'} . ":" . $row->{'mnemonic'};
    $markers{$key} = $row->{'feature_id'};
  }
  
  return %markers;
}#getExistingMarkers


sub linkPubFeature {
  my ($dbh, $marker_id, $marker_citation) = @_;
  
  $sql = "
  INSERT INTO chado.feature_pub 
    (feature_id, pub_id)
  VALUES
    ($marker_id,
    (SELECT pub_id FROM chado.pub WHERE uniquename = '$marker_citation'))";
  logSQL($dataset_name, $sql);
  doQuery($dbh, $sql);
}#linkPubFeature


sub loadPrimers {
  my ($dbh, $marker_id, $marker_name, $species, $fields) = @_;
  my ($sequence, $seqlen, $msg, $row, $sth, $sql);
  
  my $primer_num = 1;
  while ($fields->{"primer$primer_num" . "_name"} || $fields->{"primer$primer_num" . "_seq"}) {
    my $primer_name_fld = "primer$primer_num" . "_name";
    my $primer_seq_fld = "primer$primer_num" . "_seq";
    loadPrimer($dbh, $marker_id, $marker_name, $species, 'primer', $primer_num,
               $primer_name_fld, $primer_seq_fld, $fields);
    $primer_num++;
  }#each primer
}#loadPrimers


sub loadPrimer {
  my ($dbh, $marker_id, $marker_name, $species, $primer_type, $primer_num,
      $name_field, $seq_field, $fields) = @_;
  
  my $organism_id = getOrganismID($dbh, $species, $line_count);
  
  if (isFieldSet($fields, $seq_field)) {
    my $sequence = $fields->{$seq_field};
    my $seqlen = length($sequence);
    
    my $primer_name = (isFieldSet($fields, $name_field)) 
        ? $fields->{$name_field} : $marker_name;
    if (lc($marker_name) eq lc($fields->{$mki{'marker_identifier_fld'}})
          || lc($marker_name) eq lc($fields->{$mki{'pub_marker_name_fld'}})) {
      $primer_name = "$primer_name.p$primer_num";
    }
    my $unique_primer_name = makeMarkerName($species, $primer_name);
print "primer name is $primer_name ($unique_primer_name)\n";
    if (primerExists($organism_id, $unique_primer_name, $primer_type)) {
# TODO: if primer sequence needs to be changed, this will need to do an update
      print "The primer $primer_name has already been loaded. Skipping. \n";
      return;
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
        )
        RETURNING feature_id";
    logSQL($dataset_name, $sql);
    $sth = doQuery($dbh, $sql);
    $row = $sth->fetchrow_hashref;
    my $subject_id = $row->{'feature_id'};
        
    #linking the marker with its related features
    setFeatureRelationship($dbh, $marker_id, $subject_id, 'relationship'); 
  }#primer provided in worksheet
}#loadPrimer


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
  $sth->finish;
}#primerExists


# TODO: should be in GP lib
sub setFeatureRelationship {
  my ($dbh, $object_id, $subject_id, $relationship_type) = @_;
  $sql = "
        INSERT INTO feature_relationship
           (subject_id, object_id, type_id)
        VALUES
           ($subject_id, $object_id,
           (SELECT cvterm_id FROM cvterm
              WHERE name = '$relationship_type'
              AND cv_id = (SELECT cv_id FROM cv WHERE name = 'relationship'))
           )";
  logSQL($dataset_name, $sql);
  doQuery($dbh, $sql);
}# setFeatureRelationship
    
    
sub setMarkerRec {
  my ($dbh, $marker_id, $fields) = @_;
  my ($sql, $sth, $row);
  
  # TODO: The official data dictionary splits this into genus/species
  my $species = $fields->{$mki{'species_fld'}};
  
  my $marker_name = $fields->{$mki{'marker_identifier_fld'}};
  my $unique_marker_name = makeMarkerName($species, $marker_name);
  
  my $sequence = $fields->{$mki{'sequence_fld'}};
  my $seqlen = ($sequence && lc($sequence) ne 'null') ? length($sequence) : 0;

  my $organism_id = getOrganismID($dbh, $fields->{$mki{'species_fld'}}, $line_count);
  
  if ($marker_id) {
    $sql = "
      UPDATE chado.feature SET
        organism_id=$organism_id,
        name='$marker_name',
        uniquename='$unique_marker_name',
        residues='$sequence',
        seqlen=$seqlen
      WHERE feature_id=$marker_id";
  }
  else {
    $sql = "
      INSERT INTO chado.feature
        (organism_id, name, uniquename, type_id, residues, seqlen)
      VALUES
        ($organism_id, 
         '$marker_name', 
         '$unique_marker_name',
         (SELECT cvterm_id FROM chado.cvterm 
          WHERE name='genetic_marker'
            AND cv_id = (SELECT cv_id FROM chado.cv WHERE name='sequence')),
         '$sequence',
         $seqlen
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


sub setMarkerType {
  my ($dbh, $marker_id, $marker_type, $marker_citation) = @_;
  
  # Look for marker type in SO or 'local' ontology.
  my $sql = "
    SELECT cvterm_id FROM cvterm 
    WHERE name='$marker_type' 
          AND cv_id in (SELECT cv_id FROM cv WHERE name IN ('sequence', 'local'))";
  my $sth = doQuery($dbh, $sql);
  my $marker_type_id;
  if (my $row=$sth->fetchrow_hashref) {
    $marker_type_id = $row->{'cvterm_id'};
  }
  else {
    print "ERROR: unable to find marker type $marker_type in the SO\n";
    return 0;
  }
  
  # Attach as feature_cvterm record
  $sql = "
    INSERT INTO feature_cvterm
      (feature_id, cvterm_id, pub_id)
    VALUES
      ($marker_id, $marker_type_id,
       (SELECT pub_id FROM chado.pub WHERE uniquename = '$marker_citation'))";
  $sth = doQuery($dbh, $sql);
  
  return 1;
}#setMarkerType


sub setPhysicalPosition {
  my ($dbh, $marker_id, $fields) = @_;
  my ($msg, $row, $sql, $sth);

# TODO: Don't delete existing data if nothing is set for these fields.
#       Warn if values change? (Better done in verify script?)
  
  if (isFieldSet($fields, $mki{'phys_ver_fld'})) {
    my $assembly_id = getAssemblyID($dbh, $fields->{$mki{'phys_ver_fld'}});
print "assembly id is $assembly_id\n";
    if (!$assembly_id) {
      return;
    }
    
    my $start = $fields->{$mki{'phys_start_fld'}};
    my $end   = $fields->{$mki{'phys_end_fld'}}; 
    my $chr   = $fields->{$mki{'phys_chr_fld'}};
    my $ver   = $fields->{$mki{'phys_ver_fld'}};
    my $chr_feature_id = getChromosomeID($dbh, $chr, $ver);

    if ($chr_feature_id == 0) {
      $chr_feature_id = getScaffoldID($dbh, $chr, $ver);
      if ($chr_feature_id == 0) {
        $msg = "ERROR: Unable to find chromosome/scaffold feature $chr";
        $msg = "for assembly version $ver.";
        print "$msg\n";
        reportError($line_count, $msg);
      }
    }

    if ($chr_feature_id > 0) {
      $sql = "
        SELECT featureloc_id FROM chado.featureloc
        WHERE feature_id=$marker_id AND srcfeature_id=$chr_feature_id";
print "$sql\n";
      logSQL('', $sql);
      $sth = doQuery($dbh, $sql);
##print "sth: $sth\n";
#print "found " . $sth->rows . " rows\n";
#print "errors: " . $sth->errstr . "\n";
#$row=$sth->fetchrow_hashref;
#print "returns: " . Dumper($row);
#exit;
      if ($row=$sth->fetchrow_hashref) {
print "found existing record\n";
        my $featureloc_id = $row->{'featureloc_id'};
        $sql = "
          UPDATE chado.featureloc
          SET fmin=$start, fmax=$end
          WHERE featureloc_id=$featureloc_id";
        logSQL($dataset_name, $sql);
        doQuery($dbh, $sql);
      }
      else {
print "create new record\n";
        $sql = "
          INSERT INTO chado.featureloc
            (feature_id, srcfeature_id, fmin, fmax)
          VALUES
            ($marker_id, $chr_feature_id, $start, $end)";
        logSQL($dataset_name, $sql);
        doQuery($dbh, $sql);
      }
    }#physical chromosome found
  }#physical position information provided

}#setPhysicalPosition

