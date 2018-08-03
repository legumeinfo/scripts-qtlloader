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
  my %mgpi = getSSInfo('MARKER_GENOMIC_POSITION');

  # Used all over
  my ($table_file, $sql, $sth, $row, $count, @records, @fields, $cmd, $rv);
  my ($has_errors, $line_count);
  my ($add_all, $skip, $skip_all, $update, $update_all);

  my $dataset_name = 'markers';
    
  # check for worksheets (script will exit on user request)
  my $load_markers                  = checkWorksheet($input_dir, $mki{'worksheet'});
  my $load_marker_genomic_positions = checkWorksheet($input_dir, $mgpi{'worksheet'});

  # Get connected
  my $dbh = connectToDB;

  # set default schema
  $sql = "SET SEARCH_PATH = chado";
  $sth = $dbh->prepare($sql);
  $sth->execute();

  # Holds markers that are already in db; assume they should be updated if
  #   any of these appear in the worksheet.
  my %existing_markers = getExistingMarkers($dbh);
  
  # Holds marker physical positions already in database
  my %marker_phys_pos = getExistingPhysicalMarkerPositions($dbh);
#print "Existing marker postions:\n" . Dumper(%marker_phys_pos);
#exit;
  
  # keeps track of markers already seen in worksheet.
  my %markers_in_ws;

  # Use a transaction so that it can be rolled back if there are any errors
  eval {
    if ($load_markers)                  { loadMarkers($dbh); }
    if ($load_marker_genomic_positions) { loadMarkerGenomicPositions($dbh); }
    
    # SEE 2_load_maps.pl FOR MAKER POSITIONS
  
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
      $msg = "$line_count: warning: This record appears to be a QTL, not a marker. ";
      $msg .= "It will not be loaded.";
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
#print"found $unique_marker_name:$species - $marker_id\n";
  
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
    setMarkerType($dbh, $marker_id, $fields->{$mki{'marker_type_fld'}}, $marker_citation, $fields);
    
    # primer*_name, primer*_seq
    loadPrimers($dbh, $marker_id, $marker_name, $species, $fields);

    # SSR_repeat_motif, source_description, species_developed_in, restriction_enzyme, 
    #   product_length, max_length, min_length, PCR_condition, sequence_name, marker_source, 
    #   SNP_alleles, SNP_five_prime_flanking_sequence, 
    #   SNP_three_prime_flanking_sequence, comment
    setFeatureprop($dbh, $marker_id, $mki{'src_descr_fld'},             'Source Description', 1, $fields);
    setFeatureprop($dbh, $marker_id, $mki{'dev_species_fld'},           'Species Developed In', 1, $fields);
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
  }#each record
  
  print "\n\nLoaded $line_count markers.\n\n";
}#loadMarkers


sub loadMarkerGenomicPositions {
  my $dbh = $_[0];
  
  my ($fields, $sql, $sth, $row, $msg);

  $table_file = "$input_dir/MARKER_GENOMIC_POSITION.txt";
  print "Loading/verifying $table_file...\n";
  
  @records = readFile($table_file);
  print "\nLoading " . (scalar @records) . " marker genomic positions...\n";
  
  $line_count = 0;
  foreach $fields (@records) {
    $line_count++;
    print ">>>>>>> $line_count: " . $fields->{$mgpi{'marker_name_fld'}} . "\n"; # . Dumper($fields);

    my $marker_id = getMarkerID($dbh, $fields->{$mgpi{'marker_name_fld'}});
    if (!$marker_id) {
      print "warning: unable to find a marker record for " . $fields->{$mgpi{'marker_name_fld'}} . '. ';
      print "This record will be skipped.\n";
      next;
    }
    
    # Set physical position
    setPhysicalPosition($dbh, $marker_id, $fields);
#last if ($line_count > 150);    
  }#each record
  
  print "\n\nLoaded $line_count marker genomic positions.\n\n";
}#loadMarkerGenomicPositions


################################################################################
################################################################################
################################################################################

sub attachSynonyms {
  my ($dbh, $marker_id, $fieldname, $marker_citation, $fields) = @_;
  
  my ($msg, $sql, $sth, $row);
  
  if (isFieldSet($fields, $fieldname)) {
    my @synonyms = split /,/, $fields->{$fieldname};
    foreach my $syn (@synonyms) {
      # get/create synonym
      my $synonym_id;
      if ($synonym_id=getSynonym($dbh, $syn, 'Marker Synonym')) {
        # this synonym already exists; see what it's attached to
        $sql = "
          SELECT f.name, f.feature_id FROM feature f
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
        
      }#synonym exists
      
      else {
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
       }#make synonym
       
       $sql = "
         SELECT * FROM feature_synonym
         WHERE synonym_id=$synonym_id AND feature_id=$marker_id
               AND pub_id=(SELECT pub_id FROM pub 
                           WHERE uniquename='$marker_citation')";
       $sth = doQuery($dbh, $sql);
       if (!($row = $sth->fetchrow_hashref)) {
         $sql = "
           INSERT INTO feature_synonym
             (synonym_id, feature_id, pub_id)
           VALUES
             ($synonym_id, $marker_id,
              (SELECT pub_id FROM pub WHERE uniquename='$marker_citation'))";
         logSQL($dataset_name, $sql);
         doQuery($dbh, $sql);
         $sth->finish;
       }#attach synonym to feature
    }#each synonym
  }#synonym given
}#attachSynonyms


sub clearMarkerDependancies {
=cut eksc- don't do this; messes up updates
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
=cut
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


sub getExistingPhysicalMarkerPositions {
  my ($dbh) = @_;
  
  my %marker_phys_positions;
  
  my $sql = "
    SELECT fl.featureloc_id, m.feature_id AS marker_id, m.name AS marker, 
           c.feature_id AS chr_id, c.name AS chr, fl.fmin, fl.fmax, fl.rank 
    FROM featureloc fl
      INNER JOIN feature m ON m.feature_id=fl.feature_id
      INNER JOIN feature c ON c.feature_id=fl.srcfeature_id
      LEFT JOIN analysisfeature af ON af.feature_id=m.feature_id
      LEFT JOIN analysis a ON a.analysis_id=af.analysis_id
    WHERE fl.feature_id IN (SELECT feature_id FROM feature 
                            WHERE type_id=(SELECT cvterm_id FROM cvterm 
                                           WHERE name='genetic_marker' 
                                              AND cv_id=(SELECT cv_id FROM cv 
                                                         WHERE name='sequence')))";
  logSQL('', $sql);
  my $sth = doQuery($dbh, $sql);
  while (my $row=$sth->fetchrow_hashref) {
    my %value = (
      'featureloc_id' => $row->{'featureloc_id'},
      'marker'        => $row->{'marker'},
#      'ver'           => 
      'chr'           => $row->{'chr'},
      'fmin'          => $row->{'fmin'},
      'fmax'          => $row->{'fmax'},
      'rank'          => $row->{'rank'}
    );
    
    if (!$marker_phys_positions{$row->{'marker_id'}}) {
      $marker_phys_positions{$row->{'marker_id'}} = {};
    }
    if (!$marker_phys_positions{$row->{'marker_id'}}{$row->{'chr_id'}}) {
      $marker_phys_positions{$row->{'marker_id'}}{$row->{'chr_id'}} = [];
    }
    push @{$marker_phys_positions{$row->{'marker_id'}}{$row->{'chr_id'}}}, \%value;
  }
  
  return %marker_phys_positions;
}#getExistingPhysicalMarkerPositions


sub getMaxPositionRank {
  my ($marker_id) = @_;
  
  my $rank = -1;
  foreach my $chr_id (keys %{$marker_phys_pos{$marker_id}}) {
    foreach my $p (@{$marker_phys_pos{$marker_id}{$chr_id}}) {
      if ($p->{'rank'} > $rank) { $rank = $p->{'rank'} };
    }
  }#each chr position for this marker
  
  return $rank;
}#getMaxPositionRank


sub linkPubFeature {
  my ($dbh, $marker_id, $marker_citation) = @_;

  return if (!$marker_citation || $marker_citation eq 'NULL');
  
  my $feature_pub_id = 0;
  $sql = "
    SELECT feature_pub_id FROM feature_pub
    WHERE feature_id=$marker_id
          AND pub_id=(SELECT pub_id FROM chado.pub 
                      WHERE uniquename = '$marker_citation')";
  logSQL($dataset_name, $sql);
  if (($sth=doQuery($dbh, $sql)) && ($row=$sth->fetchrow_hashref)) {
    $feature_pub_id = $row->{'feature_pub_id'};
  }
  
  if (!$feature_pub_id) {
    $sql = "
    INSERT INTO chado.feature_pub 
      (feature_id, pub_id)
    VALUES
      ($marker_id,
      (SELECT pub_id FROM chado.pub WHERE uniquename = '$marker_citation'))";
    logSQL($dataset_name, $sql);
    doQuery($dbh, $sql);
  }
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
    # Append primer number if primer name is built from marker name
    if (lc($primer_name) eq lc($fields->{$mki{'marker_identifier_fld'}})
          || lc($primer_name) eq lc($fields->{$mki{'pub_marker_name_fld'}})) {
      $primer_name = "$primer_name.p$primer_num";
    }
    my $unique_primer_name = makeMarkerName($species, $primer_name);
print "primer name is $primer_name ($unique_primer_name)\n";
    if ((my $primer_id=primerExists($organism_id, $unique_primer_name, $primer_type))) {
      $sql = "
        UPDATE feature
          SET residues = '$sequence'
        WHERE feature_id=$primer_id";
      logSQL($dataset_name, $sql);
      $sth = doQuery($dbh, $sql);
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
print "Primer $primer_name has feature $subject_id\n";
        
    #linking the marker with its related features
    setFeatureRelationship($dbh, $marker_id, $subject_id, 'relationship'); 
  }#primer provided in worksheet
}#loadPrimer


sub markerHasPosition {
  my ($marker_id) = @_;
#print "check exiting position for marker id $marker_id:\n" . Dumper($marker_phys_pos{$marker_id});
  if ($marker_phys_pos{$marker_id} && defined($marker_phys_pos{$marker_id})) {
    return 1;
  }
  
  return undef;
}#markerHasPosition


sub newPositionAction {
  my ($marker_id, $marker, $chr_id, $chr, $start, $end) = @_;
  
  if ($add_all) { return 'add' };
  if ($update_all) { return 'update' }
  if ($skip_all) { return 'skip' }
  
#print "newPositionAction($marker_id, $marker, $chr_id, $chr, $start, $end)\n";
#print "Existing marker information:\n" . Dumper($marker_phys_pos{$marker_id});

  print "\nThe marker '$marker' already has one or more positions in the database.\n";
  print "Existing positions:\n";
  my %pos = %{$marker_phys_pos{$marker_id}};
#print "What is this???\n" . Dumper(%pos);
  foreach my $c (keys %pos) {
    foreach my $p (@{$marker_phys_pos{$marker_id}->{$c}}) {
      print '   ' . $p->{'chr'} . ': ' . $p->{'fmin'} . '-' . $p->{'fmax'} . "\n";
    }
  }
  print "New position:\n   $chr: $start - $end\n";
  print "Action: [u]pdate, [u]pdate [all], [a]add, [a]dd [all], [s]kip, [s]kip [all], [q]uit: ";
  my $userinput =  <STDIN>;
  chomp ($userinput);
  if ($userinput eq 'a') {
    return 'add';
  }
  elsif ($userinput eq 'aall') {
    $add_all = 1;
    return 'add';
  }
  elsif ($userinput eq 'u') {
    return 'update';
  }
  elsif ($userinput eq 'uall') {
    $update_all = 1;
    return 'update';
  }
  elsif  ($userinput eq 'sall') {
    $skip_all = 1;
    return 'skip';
  }
  else {
    exit;
  }
}#newPositionAction


sub positionLoaded {
  my ($marker_id, $ver, $chr_feature_id, $start, $end) = @_;
#print "positionLoaded(): version is $ver\n";
#exit;

  if (!$marker_phys_pos{$marker_id} || !$marker_phys_pos{$marker_id}{$chr_feature_id}) {
    return undef;
  }
  
  my $mkr_positions = $marker_phys_pos{$marker_id}{$chr_feature_id};
#print "marker $marker_id has these positions on $chr_feature_id:\n" . Dumper($mkr_positions);
  foreach my $p (@$mkr_positions) {
#print "one position:\n" . Dumper($p);
    if ($p->{'fmin'} == $start && $p->{'fmax'} == $end) {
      return 1;
    }
  } 
  
  return undef;
}#positionLoaded


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
    
    
sub setPhysicalPosition {
  my ($dbh, $marker_id, $fields) = @_;
  my ($msg, $row, $sql, $sth);

  if (isFieldSet($fields, $mgpi{'phys_ver_fld'})) {
    my $assembly_id = getAssemblyID($dbh, $fields->{$mgpi{'phys_ver_fld'}});
#print "Assembly analysis id for '" . $fields->{$mgpi{'phys_ver_fld'}} . "' is $assembly_id\n";
    if (!$assembly_id) {
      print "warning: unable to find analysis id for '" . $fields->{$mgpi{'phys_ver_fld'}} . "'\n";
      return;
    }
    
    my $marker = $fields->{$mgpi{'marker_name_fld'}};
    my $start  = $fields->{$mgpi{'phys_start_fld'}};
    my $end    = $fields->{$mgpi{'phys_end_fld'}}; 
    my $chr    = $fields->{$mgpi{'phys_chr_fld'}};
    my $ver    = $fields->{$mgpi{'phys_ver_fld'}};
    my $chr_feature_id = getChromosomeID($dbh, $chr, $ver);

    if ($chr_feature_id == 0) {
      $chr_feature_id = getScaffoldID($dbh, $chr, $ver);
      if ($chr_feature_id == 0) {
        $msg = "ERROR: Unable to find chromosome/scaffold feature $chr ";
        $msg .= "for assembly version $ver.";
        print "$msg\n";
        reportError($line_count, $msg);
exit;
        return;
      }
    }
#print "Chromosome feature id for $marker_id is $chr_feature_id\n";

    # decide what to do with this record; default is add
    my $action = 'add';
    
    # Is this position already loaded?
    if (positionLoaded($marker_id, $ver, $chr_feature_id, $start, $end)) {
print "This position is already in the database: $marker_id, $chr_feature_id, $start, $end\n";
      $action = 'skip';
    }
    elsif (markerHasPosition($marker_id)) {
      my $action = newPositionAction($marker_id, $marker, $chr_feature_id, $chr, $start, $end);
    }

    if ($action eq 'add') {
      my $rank = getMaxPositionRank($marker_id) + 1;
print "Add position for marker: ($marker_id, $chr_feature_id, $start, $end, $rank)\n";
      my $s = ($start<$end) ? $start : $end;
      my $e = ($start<$end) ? $end : $start;
      $sql = "
        INSERT INTO chado.featureloc
          (feature_id, srcfeature_id, fmin, fmax, rank)
        VALUES
          ($marker_id, $chr_feature_id, $s, $e, $rank)";
      logSQL($dataset_name, $sql);
      doQuery($dbh, $sql);
      
      # Add this position 
      if (!$marker_phys_pos{$marker_id}) {
        $marker_phys_pos{$marker_id} = {};
      }
      if (!$marker_phys_pos{$marker_id}{$chr_feature_id}) {
        $marker_phys_pos{$marker_id}{$chr_feature_id} = [];
      }
      my %value = (
        'featureloc_id' => 0,
        'marker'        => $marker,
#        'ver'           => $ver,
        'chr'           => $chr,
        'fmin'          => $start,
        'fmax'          => $end,
        'rank'          => $rank
      );
      push @{$marker_phys_pos{$marker_id}{$chr_feature_id}}, \%value;
    }
    elsif ($action eq 'update') {
print "\n\nupdate not implemented.\n\n";
exit;
    }
  }#physical position information provided

}#setPhysicalPosition


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
  my ($dbh, $marker_id, $marker_type, $marker_citation, $fields) = @_;
print "Set marker type to '$marker_type'\n";
  
  # Look for marker type in SO or 'local' ontology.
  my $sql = "
    SELECT t.cvterm_id FROM cvterm t 
      INNER JOIN cvtermsynonym ts ON ts.cvterm_id=t.cvterm_id 
    WHERE (t.name='$marker_type' OR ts.synonym='$marker_type')
          AND t.cv_id in (SELECT cv_id FROM cv WHERE name IN ('sequence', 'local'))";
  logSQL($dataset_name, $sql);
  my $sth = doQuery($dbh, $sql);
  my $marker_type_id;
  if (my $row=$sth->fetchrow_hashref) {
    $marker_type_id = $row->{'cvterm_id'};
  }
  else {
    print "warning: unable to find marker type $marker_type in the SO\n";
    return 0;
  }
print "Found marker type in the SO\n";
  
  # Attach as feature_cvterm record
  my $feature_cvterm_id = 0;
  $sql = "
    SELECT feature_cvterm_id FROM feature_cvterm
    WHERE feature_id=$marker_id AND cvterm_id=$marker_type_id
          AND pub_id=(SELECT pub_id FROM chado.pub WHERE uniquename = '$marker_citation')";
  logSQL($dataset_name, $sql);
  if (($sth=doQuery($dbh, $sql)) && ($row=$sth->fetchrow_hashref)) {
    $feature_cvterm_id = $row->{'feature_cvterm_id'};
  }
  if (!$feature_cvterm_id) {
    $sql = "
      INSERT INTO feature_cvterm
        (feature_id, cvterm_id, pub_id)
      VALUES
        ($marker_id, $marker_type_id,
         (SELECT pub_id FROM chado.pub WHERE uniquename = '$marker_citation'))";
    logSQL($dataset_name, $sql);
    $sth = doQuery($dbh, $sql);
  }
  
  # Here's (an often more accurate) marker type as a property
print "marker type prop set to " . $fields->{$mki{'src_marker_fld'}} . "\n";
  setFeatureprop($dbh, $marker_id, $mki{'src_marker_fld'}, 'Marker Type', 1, $fields);
 
  return 1;
}#setMarkerType

