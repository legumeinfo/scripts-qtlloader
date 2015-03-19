use strict;
use base 'Exporter';
use vars qw($VERSION @ISA @EXPORT @EXPORT_OK %EXPORT_TAGS);

package CropLegumeBaseLoaderUtils;

our $VERSION     = 1.00;
our @ISA         = qw(Exporter);
our @EXPORT      = (
                    qw(checkUpdate),
                    qw(connectToDB), 
                    qw(createLinkageGroups),
                    qw(dbxrefExists),
                    qw(doQuery), 
                    qw(experimentExists),
                    qw(getChromosomeID),
                    qw(getCvtermID), 
                    qw(getFeatureID),
                    qw(getMapSetID),
                    qw(getOBOName),
                    qw(getOBOTermID),
                    qw(getOrganismID),
                    qw(getOrganismMnemonic),
                    qw(getPubID),
                    qw(getQTLid),
                    qw(getScaffoldID),
                    qw(getSSInfo),
                    qw(getSynonym),
                    qw(getTrait),
                    qw(isFieldSet),
                    qw(isNull),
                    qw(logSQL),
                    qw(makeLinkageMapName),
                    qw(makeMappingPopulationName),
                    qw(makeMarkerName),
                    qw(makeQTLName),
                    qw(mapSetExists),
                    qw(markerExists),
                    qw(markerExistsOnOtherMap),
                    qw(publicationExists),
                    qw(qtlExists),
                    qw(readFile), 
                    qw(reportError),
                    qw(traitExists),
                    qw(uniqueID),
                   );
#our @EXPORT_OK   = ();
#our %EXPORT_TAGS = (DEFAULT => [qw(&testsub)]);


# Put connection variables in a separate file; 
#   connectToDB() defined here:
require 'db.pl';



sub testsub {
  print "this is a test\n";
}


sub getSSInfo {
  my $ss = $_[0];
  
  # PUB
  if ($ss eq 'PUBS') {
    return (
      'worksheet'    => 'PUB',
      'pub_fld'      => 'publink_citation',
      'species_fld'  => 'species',
      'ref_type_fld' => 'ref_type',
      'year_fld'     => 'year',
      'title_fld'    => 'title',
      'author_fld'   => 'authors',
      'journal_fld'  => 'journal',
      'volume_fld'   => 'volume',
      'issue_fld'    => 'issue',
      'page_fld'     => 'pages',
      'doi_fld'      => 'doi',
      'pmid_fld'     => 'pmid',
      'abstract_fld' => 'abstract',
      'keyword_fld'  => 'keywords',
      'url_fld'      => 'urls',
    );
  }
  
  elsif ($ss eq 'QTL_EXPERIMENTS') {
    return(
      'worksheet'   => 'QTL_EXPERIMENT',
      'species_fld' => 'specieslink_abv',
      'desc_fld'    => 'description',
      'geoloc_fld'  => 'geolocation',
      'map_fld'     => 'map_name',
      'name_fld'    => 'publink_citation_exp',
      'pub_fld'     => 'publink_citation',
      'title_fld'   => 'title',
      'comment_fld' => 'comment',
    );
  }

  # MAP_COLLECTIONS
  elsif ($ss eq 'MAP_COLLECTIONS') {
    return (
      'worksheet'        => 'MAP_COLLECTION',
      'species_fld'      => 'specieslink_abv',
      'pub_map_name_fld' => 'publication_map_name',
      'map_name_fld'     => 'map_name',
      'description_fld'  => 'description',
      'parent1_fld'      => 'parent1',
      'parent2_fld'      => 'parent2',
      'pop_size_fld'     => 'pop_size',
      'pop_type_fld'     => 'pop_type',
      'a_method_fld'     => 'analysis_method',
      'pub_fld'          => 'map_citation',
      'unit_fld'         => 'unit',
      'LIS_name_fld'     => 'LIS_mapset_name',
      'comment_fld'      => 'comment',
    );
  }
  elsif ($ss eq 'MAPS') {
    return (
      'worksheet'     => 'MAP',
      'species_fld'   => 'specieslink_abv',
      'map_name_fld'  => 'map_name',
      'lg_fld'        => 'lg',
      'map_start_fld' => 'map_start',
      'map_end_fld'   => 'map_end',
      'LIS_lg_fld'    => 'LIS_lg_map_name',
    );
  }
  elsif ($ss eq 'MARKERS') {
    return (
      'worksheet'           => 'MARKER',
      'species_fld'         => 'specieslink_abv',
      'marker_citation_fld' => 'marker_citation',
      'marker_name_fld'     => 'marker_name',
      'marker_synonym_fld'  => 'marker_synonym',
      'marker_type_fld'     => 'marker_type',
      'assembly_name_fld'   => 'assembly_name',
      'phys_chr_fld'        => 'phys_chr',
      'phys_start_fld'      => 'phys_start',
      'phys_end_fld'        => 'phys_end',
      'comment_fld'         => 'comment',
    );
  }
  elsif($ss eq 'MARKER_POSITION'){
    return(
      'worksheet'           => 'MARKER_POSITION',
      'species_fld'         => 'specieslink_abv',
      'marker_name_fld'     => 'marker_name',
      'cmap_acc_fld'        => 'Cmap_accession',
      'map_name_fld'        => 'map_name',
      'lg_fld'              => 'lg',
      'position_fld'        => 'position',
      'comment_fld'         => 'comment',
    );
  }
  elsif($ss eq 'MARKER_SEQUENCE'){
    return(
        'worksheet'               =>  'MARKER_SEQUENCE',
        'species_fld'             =>  'specieslink_abv',
        'marker_name_fld'         =>  'marker_name',
        'sequence_type_fld'       =>  'sequence_type',
        'genbank_acc_fld'         =>  'Genbank_accession',
        'sequence_name_fld'       =>  'sequence_name',
        'marker_sequence_fld'     =>  'marker_sequence',
        'forward_primer_name_fld' =>  'forward_primer_name',
        'reverse_primer_name_fld' =>  'reverse_primer_name',
        'forward_primer_seq_fld'  =>  'forward_primer_seq',
        'reverse_primer_seq_fld'  =>  'reverse_primer_seq',
        'comment_fld'             =>  'comment',        
    );
    }
  elsif ($ss eq 'TRAITS') {
    return (
      'worksheet'       => 'Traits',
      'qtl_symbol_fld'  => 'QTL_Symbol',
      'trait_name_fld'  => 'Trait_Name',
      'trait_class_fld' => 'Trait_Class',
      'onto_id_fld'     => 'Similar_Controlled_Vocab_Accessions',
      'onto_name_fld'   => 'Similar_Controlled_Vocabulary',
      'description_fld' => 'Description',
    );
  }
  elsif ($ss eq 'QTL') {
    return (
      'worksheet'           => 'QTL',
      'species_fld'         => 'specieslink_abv',
      'qtl_expt_fld'        => 'publink_citation_exp',
      'expt_trait_name_fld' => 'expt_trait_name',
      'expt_trait_desc_fld' => 'expt_trait_description',
      'trait_unit_fld'      => 'trait_unit',
      'qtl_symbol_fld'      => 'qtl_symbol',
      'qtl_identifier_fld'  => 'qtl_identifier',
      'expt_qtl_symbol_fld' => 'expt_qtl_symbol',
      'fav_allele_fld'      => 'favorable_allele_source',
      'treatment_fld'       => 'treatment',
      'method_fld'          => 'analysis_method',
      'lod_fld'             => 'lod',
      'like_ratio_fld'      => 'likelihood_ratio',
      'marker_r2_fld'       => 'marker_r2',
      'total_r2_fld'        => 'total_r2',
      'additivity_fld'      => 'additivity',
      'nearest_mrkr_fld'    => 'nearest_marker',
      'flank_mrkr_low_fld'  => 'flanking_marker_low',
      'flank_mrkr_high_fld' => 'flanking_marker_high',
      'comment_fld'         => 'comment',
    );
  }
  elsif ($ss eq 'MAP_POSITIONS') {
    return(
      'worksheet'           => 'MAP_POSITION',
      'qtl_symbol_fld'      => 'qtl_symbol',
      'qtl_identifier_fld'  => 'qtl_identifier',
      'map_name_fld'        => 'map_name',
      'pub_lg_fld'          => 'publication_lg',
      'lg_fld'              => 'lg',
      'left_end_fld'        => 'left_end',
      'right_end_fld'       => 'right_end',
      'QTL_peak_fld'        => 'QTL_peak',
      'int_calc_meth_fld'   => 'interval_calc_method',
    );
  }
}#getSSInfo


sub checkUpdate {
  my $prompt = $_[0];
  
  my ($skip, $skip_all, $update, $update_all);
  
  print "$prompt is already loaded.\nUpdate? (y/n/skipall/all/q)\n";
  my $userinput =  <STDIN>;
  chomp ($userinput);
  if ($userinput eq 'skipall') {
    $skip_all = 1;
  }
  elsif ($userinput eq 'n') {
    $skip = 1;
  }
  elsif ($userinput eq 'q') {
    exit;
  }
  elsif ($userinput eq 'all') {
    $update_all = 1;
  }
  elsif ($userinput eq 'y') {
    $update = 1;
  }
  else {
    print "unknown option ($userinput); skipping record\n";
    $skip = 1;
  }
 
 return ($skip, $skip_all, $update, $update_all);
}


sub createLinkageGroups {
  my @records = @_;
  
  # a hash describing the linkage groups represented by an array of marker 
  my %lgs;
  my %mpi = getSSInfo('MARKER_POSITION');
  
  foreach my $r (@records) {
    my $lg_map_name = makeLinkageMapName($r->{$mpi{'map_name_fld'}}, $r->{$mpi{'lg_fld'}});
    if (!$lgs{$lg_map_name}) {
      $lgs{$lg_map_name} = {'map_start' => 99999, 'map_end' => -99999};
    }
    if ($r->{$mpi{'position_fld'}} < $lgs{$lg_map_name}{'map_start'}) {
      $lgs{$lg_map_name}{'map_start'} = $r->{$mpi{'position_fld'}};
    }
    if ($r->{$mpi{'position_fld'}} > $lgs{$lg_map_name}{'map_end'}) {
      $lgs{$lg_map_name}{'map_end'} = $r->{$mpi{'position_fld'}};
    }
    $lgs{$lg_map_name}{'species'} = $r->{$mpi{'species_fld'}};
  }#each record
  
  return %lgs;
}#createLinkageGroups


sub dbxrefExists {
  my ($dbh, $db, $acc) = @_;
  my ($sql, $sth, $row);
 
  if ($db && $db ne '' && $acc && $acc ne '' && $acc ne 'NULL') {
    $sql = "
      SELECT dbxref_id 
      FROM chado.dbxref
      WHERE db_id=(SELECT db_id FROM chado.db WHERE name='$db')
        AND accession='$acc'";
    logSQL('', "$sql");
    $sth = doQuery($dbh, $sql);
    if ($row=$sth->fetchrow_hashref()) {
      return $row->{'dbxref_id'};
    }
  }
  
  return 0;
}#dbxrefExists


sub doQuery {
  use Data::Dumper;
  
  my ($dbh, $sql, @vals) = @_;
  if (!@vals) { @vals = (); }
  
  # Translate any UniCode characters in sql statment
  $sql = decode("iso-8859-1", $sql);
  
  # Translate any UniCode chararcters in values array
  if (@vals && (scalar @vals) > 0) {
    my @tr_vals = map{ decode("iso-8859-1", $_) } @vals;
    @vals = @tr_vals;
  }
  
  my $sth = $dbh->prepare($sql);
  $sth->execute(@vals);
  return $sth;
}#doQuery


sub experimentExists {
  my ($dbh, $experiment) = @_;
  my ($sql, $sth, $row);
  
  if ($experiment && $experiment ne 'NULL') {
    $sql = "SELECT project_id FROM chado.project WHERE name='$experiment'";
    logSQL('', $sql);
    $sth = doQuery($dbh, $sql);
    if ($row=$sth->fetchrow_hashref) {
      return $row->{'project_id'};
    }
  }
  
  return 0;
}#experimentExists


sub getCvtermID {
  my ($dbh, $term, $cv) = @_;
  my ($sql, $sth, $row);
  
  $sql = "
    SELECT cvterm_id FROM chado.cvterm 
    WHERE name='$term'
      AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='$cv')";
  logSQL('lib', $sql);
  $sth = doQuery($dbh, $sql);
  if ($row=$sth->fetchrow_hashref()) {
    return $row->{'cvterm_id'};
  }
  else {
    $term = lc($term);
    $sql = "
      SELECT cvterm_id FROM chado.cvterm 
      WHERE name='$term'
        AND cv_id=(SELECT cv_id FROM chado.cv WHERE LOWER(name)='$cv')";
    logSQL('lib', $sql);
    $sth = doQuery($dbh, $sql);
    if ($row=$sth->fetchrow_hashref()) {
      return $row->{'cvterm_id'};
    }
  }
  
  # search failed
  reportError("Unable to find term [$term] in controlled vocabulary [$cv]\n");
  return 0;
}#getCvtermID


sub getAssemblyID {
  my ($dbh, $assembly) = @_;
  my ($row, $sql, $sth);
  
  $sql = "
    SELECT analysis_id FROM analysis
    WHERE name='$assembly'";
  $sth = doQuery($dbh, $sql);
  if ($row=$sth->fetchrow_hashrec) {
    return $row-{'analysis_id'};
  }
  
  return 0;
}#getAssemblyID


sub getChromosomeID {
  my ($dbh, $chromosome, $version) = @_;
  my ($sql, $sth, $row);
  
  if (!$chromosome || $chromosome eq '' || $chromosome eq 'NULL' 
        || $chromosome eq 'none') {
    return 0;
  }
  
  $sql = "
    SELECT F.feature_id
    FROM chado.feature F
      INNER JOIN chado.featureprop FP
        ON FP.feature_id=F.feature_id
          AND FP.type_id = (SELECT cvterm_id FROM chado.cvterm 
                            WHERE name = 'assembly version'
                              AND cv_id=(SELECT cv_id FROM chado.cv 
                                         WHERE name='local'))
    WHERE F.type_id = (SELECT cvterm_id FROM chado.cvterm 
                       WHERE name='chromosome' 
                         AND cv_id = (SELECT cv_id FROM chado.cv 
                                       WHERE name='sequence')
                       )
      AND F.name='$chromosome' AND FP.value='$version'";
  logSQL('lib', $sql);
  $sth = doQuery($dbh, $sql);
  if ($row=$sth->fetchrow_hashref) {
    return $row->{'feature_id'};
  }
  else {
    return 0;
  }
}#getChromosomeID


#TODO: this should also include type and species because the unique constraint
#      on the table feature is (uniquename, type_id, organism_id.
sub getFeatureID {
  my ($dbh, $uniquename) = @_;
  my ($sql, $sth, $row);
  
  $sql = "
    SELECT feature_id FROM chado.feature
    WHERE uniquename='$uniquename'";
  logSQL('', "$sql");
  $sth = doQuery($dbh, $sql);
  if ($row=$sth->fetchrow_hashref) {
    return $row->{'feature_id'};
  }
  else {
    return 0;
  }
}#getFeatureID


sub getMapSetID {
  my ($dbh, $mapset) = @_;
  my ($sql, $sth, $row);
  
  $sql = "
    SELECT featuremap_id 
    FROM chado.featuremap
    WHERE name = '$mapset'";
  logSQL('', $sql);
  $sth = doQuery($dbh, $sql);
  if ($row=$sth->fetchrow_hashref) {
    return $row->{'featuremap_id'}
  }
  else {
    return 0;
  }
}#getMapSetID


sub getOBOName {
  my ($dbh, $dbxref_id) = @_;
  my ($sql, $sth, $row);
  
  my $obo_name;
  
  $sql = "SELECT name FROM cvterm WHERE dbxref_id=$dbxref_id";
  logSQL('', "$sql");
  $sth = doQuery($dbh, $sql);
  if (!($row=$sth->fetchrow_hashref)) {
    # pretty unlikely, but just in case...
    print "ERROR: unable to find cvterm record for $dbxref_id\n";
  }
  else {
    $obo_name = $row->{'name'};
  }
  
  return $obo_name;
}#getOBOName


sub getOBOTermID {
  my ($dbh, $term) = @_;
  my ($sql, $sth, $row);
  
  my ($cvterm_id, $dbxref_id);
  
  if (!($term =~ /^(.*?):(.*)/)) {
    print "ERROR: unknown OBO term: $term\n";
  }
  else {
    my $ontology  = $1;
    my $accession = $2;
    
    $dbxref_id = dbxrefExists($dbh, $ontology, $accession);
    if (!$dbxref_id && $ontology eq 'SOY') {
      # some special handling may be required
      $dbxref_id = dbxrefExists($dbh, 'soybean_whole_plant_growth_stage', $accession);
    }
    if (!$dbxref_id && $ontology eq 'SOY') {
      $dbxref_id = dbxrefExists($dbh, 'soybean_development', $accession);
    }
    if (!$dbxref_id && $ontology eq 'SOY') {
      $dbxref_id = dbxrefExists($dbh, 'soybean_structure', $accession);
    }
    if (!$dbxref_id && $ontology eq 'SOY') {
      $dbxref_id = dbxrefExists($dbh, 'soybean_trait', $accession);
    }
    
    if (!$dbxref_id) {
      print "ERROR: unable to find OBO term $term\n";
    }
    else {
      $sql = "SELECT cvterm_id FROM cvterm WHERE dbxref_id=$dbxref_id";
      logSQL('', "$sql");
      $sth = doQuery($dbh, $sql);
      if (!($row=$sth->fetchrow_hashref)) {
        print "ERROR: unable to find matching cvterm record for $dbxref_id = $term\n";
      }
      else {
        $cvterm_id = $row->{'cvterm_id'};
      }
    }#found dbxref record
  }#looks like an OBO term
  
  return $cvterm_id;
}#getOBOTermID


sub getOrganismID {
  my ($dbh, $mnemonic, $line_count) = @_;
  my ($sql, $sth, $row);
  
  $sql = "
    SELECT O.organism_id 
    FROM chado.organism O 
      INNER JOIN chado.organism_dbxref OD ON OD.organism_id=O.organism_id 
      INNER JOIN chado.dbxref D on D.dbxref_id=OD.dbxref_id 
    WHERE D.accession='$mnemonic'";
  logSQL('', "$sql");
  $sth = doQuery($dbh, $sql);
  if ($row=$sth->fetchrow_hashref) {
    return $row->{'organism_id'};
  }
  else {
    reportError($line_count, "unknown organism: [$mnemonic]");
    return 0;
  }
}#getOrganismID


sub getOrganismMnemonic {
  my ($dbh, $mnemonic, $line_count) = @_;
  my ($sql, $sth, $row);
  
  $sql = "
    SELECT O.organism_id 
    FROM chado.organism O 
      INNER JOIN chado.organism_dbxref OD ON OD.organism_id=O.organism_id 
      INNER JOIN chado.dbxref D on D.dbxref_id=OD.dbxref_id 
    WHERE D.accession='$mnemonic'";
  logSQL('', "$sql");
  $sth = doQuery($dbh, $sql);
  if ($row=$sth->fetchrow_hashref) {
    return $row->{'organism_id'};
  }
  else {
    reportError($line_count, "unknown organism: [$mnemonic]");
    return 0;
  }
}#getOrganismMnemonic


sub getPubID {
  my ($dbh, $citation) = @_;
  my ($sql, $sth, $row);
  
  $sql = "SELECT pub_id FROM chado.pub WHERE uniquename=?";
  logSQL('', "$sql");
  $sth = doQuery($dbh, $sql, ($citation));
  if ($row=$sth->fetchrow_hashref) {
    return $row->{'pub_id'};
  }
  else {
    reportError("unknown publication: [$citation]");
    return 0;
  }
}#getPubID


sub getQTLid {
  my ($dbh, $qtl) = @_;
  my ($sql, $sth, $row);
  
  if (!$qtl || $qtl eq '' || $qtl eq 'NULL' || $qtl eq 'none') {
    return 0;
  }
  
  $sql = "
    SELECT F.feature_id
    FROM chado.feature F
    WHERE F.name='$qtl' 
      AND F.type_id=(SELECT cvterm_id FROM chado.cvterm 
                     WHERE name = 'QTL'
                           AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='sequence'))";
  logSQL('lib', $sql);
  $sth = doQuery($dbh, $sql);
  if ($row=$sth->fetchrow_hashref) {
    return $row->{'feature_id'};
  }
  else {
    return 0;
  }
}#getQTLid


sub getScaffoldID {
  my ($dbh, $scaffold, $version) = @_;
  my ($sql, $sth, $row);
  
  if (!$scaffold || $scaffold eq '' || $scaffold eq 'NULL' 
        || $scaffold eq 'none') {
    return 0;
  }
  
#  $sql = "
#    SELECT F.feature_id
#    FROM chado.feature F
#      INNER JOIN chado.featureprop FP
#        ON FP.feature_id=F.feature_id
#          AND FP.type_id = (SELECT cvterm_id FROM chado.cvterm WHERE name = 'assembly version')
#    WHERE F.name='$scaffold' AND FP.value='$version'";
  $sql = "
    SELECT F.feature_id
    FROM chado.feature F
      INNER JOIN chado.featureprop FP
        ON FP.feature_id=F.feature_id
          AND FP.type_id = (SELECT cvterm_id FROM chado.cvterm 
                            WHERE name = 'assembly version'
                              AND cv_id=(SELECT cv_id FROM chado.cv 
                                         WHERE name='local'))
    WHERE F.type_id = (SELECT cvterm_id FROM chado.cvterm 
                       WHERE name='scaffold' 
                         AND cv_id = (SELECT cv_id FROM chado.cv 
                                       WHERE name='local')
                       )
      AND F.name='$scaffold' AND FP.value='$version'";
  logSQL('lib', $sql);
  $sth = doQuery($dbh, $sql);
  if ($row=$sth->fetchrow_hashref) {
    return $row->{'feature_id'};
  }
  else {
    return 0;
  }
}#getScaffoldID


sub getSynonym {
  my ($dbh, $synonym, $type) = @_;
  my ($sql, $sth, $row);
  $sql = "
    SELECT synonym_id FROM chado.synonym 
    WHERE name='$synonym' 
      AND type_id=(SELECT cvterm_id FROM chado.cvterm
                   WHERE name='$type'
                     AND cv_id=(SELECT cv_id FROM chado.cv 
                                WHERE name='synonym_type'))";
  logSQL('', "$sql");
  $sth = doQuery($dbh, $sql);
  if ($row = $sth->fetchrow_hashref) {
    return $row->{'synonym_id'};
  }
  else {
    return 0;
  }
}#getSynonym


sub getTrait {
  my ($dbh, $trait) = @_;
  
  my $trait_id = getCvtermID($dbh, $trait, 'LegumeInfo:traits');
  if (!$trait_id) {
    $trait_id = getCvtermID($dbh, $trait, 'SOY');
  }
  if (!$trait_id) {
    $trait_id = getCvtermID($dbh, $trait, 'soybean_whole_plant_growth_stage');
  }
  if (!$trait_id) {
    $trait_id = getCvtermID($dbh, $trait, 'soybean_development');
  }
  if (!$trait_id) {
    $trait_id = getCvtermID($dbh, $trait, 'soybean_structure');
  }
  if (!$trait_id) {
    $trait_id = getCvtermID($dbh, $trait, 'soybean_trait');
  }
  
  return $trait_id;
}#getTrait


sub isFieldSet {
  my ($fields, $fld, $allow_zero) = @_;
  if ((!$allow_zero && !$fields->{$fld}) 
        || $fields->{$fld} eq '' 
        || $fields->{$fld} eq 'null' 
        || $fields->{$fld} eq 'NULL') {
    return 0;
  }
  else {
    return 1;
  }
}#isFieldSet


sub isNull {
  my $value = $_[0];
  return (!$value || $value eq '' || lc($value) eq 'null');
  
}#isNull


sub logSQL {
  my ($datatype, $sql) = @_;  # $datatype now ignored
  my ($sec, $min, $hours, $mday, $month, $year) = localtime;
  $min = $min%10*20;   # new log every 20 minutes
  my $date = "" . (1900+$year) . "-" . ($month+1) . "-$mday:$hours:$min";
  open SQL, ">>log.$date.sql";
  print SQL "$0:\n$sql\n";
  close SQL;
}#logSQL


sub makeLinkageMapName {
  my ($map_name, $lg) = @_;
  
  my $lg_map_name = 0;
  if ($map_name && $map_name ne 'NULL' && $lg && $lg ne 'NULL') {
    $lg_map_name = "$map_name-$lg";
  }
  
  return $lg_map_name;
}#makeLinkageMapName


sub makeMappingPopulationName {
  my ($fields) = @_;
  
  my $map_set_name = "$fields->{'map_name'} - $fields->{'pop_type'}";
  if ($fields->{'pop_size'} && $fields->{'pop_size'} ne 'NULL') {
    $map_set_name .= ", pop size=$fields->{'pop_size'}";
  }
  if ($fields->{'publink_citation'} && $fields->{'publink_citation'} ne 'NULL') {
    $map_set_name .= " ($fields->{'publink_citation'})";
  }
  
  return $map_set_name;
}#makeMappingPopulationName


sub makeMarkerName {
  my ($species, $marker) = @_;
# Don't need to append species mnemonic because the unique constraint in the
#   feature table is (uniquename, type_id, organism_id).
# Also, what species name to use may not be clear if marker was developed in
#   an interspecific cross.
#  my $uniq_marker_name = "$species.$marker";
  my $uniq_marker_name = "$marker";
  return $uniq_marker_name;
}#makeMarkerName


sub makeQTLName {
  my ($symbol, $id) = @_;
  
  my $qtl_name = "$symbol $id";
  
  return $qtl_name;
}#makeQTLName


sub mapSetExists {
  my ($dbh, $mapname) = @_;
  my ($sql, $sth, $row);
  
  $sql = "SELECT featuremap_id FROM chado.featuremap WHERE name = '$mapname'";
  logSQL('', $sql);
  $sth = doQuery($dbh, $sql);
  if (($row = $sth->fetchrow_hashref)) {
    return $row->{'featuremap_id'};
  }
  else {
    return 0;
  }
}#mapSetExists


sub markerExists {
  my ($dbh, $marker, $species) = @_;
  my ($sql, $sth, $row);

  my ($sql, $sth, $row);
  $sql = "
    SELECT f.feature_id
    FROM chado.feature f
    WHERE uniquename='$marker'
          AND organism_id = (SELECT O.organism_id 
                             FROM chado.organism O 
                               INNER JOIN chado.organism_dbxref OD ON OD.organism_id=O.organism_id 
                               INNER JOIN chado.dbxref D on D.dbxref_id=OD.dbxref_id 
                             WHERE D.accession='$species'
                            )
          AND type_id = (SELECT cvterm_id FROM chado.cvterm 
                         WHERE name='genetic_marker' 
                               AND cv_id=(SELECT cv_id FROM chado.cv 
                                          WHERE name='sequence')
                        )";
                        
  logSQL('', $sql);
#print "$sql\n";
  $sth = doQuery($dbh, $sql);
  if (($row = $sth->fetchrow_hashref)) {
#print "marker exists: " . $row->{'feature_id'} . "\n";
    return $row->{'feature_id'};
  }
  
  return 0;
}#makerExists


sub markerExistsOnOtherMap {
  my ($dbh, $marker_name, $mapname) = @_;
  my ($sql, $sth, $row);

  my ($sql, $sth, $row);
  my @matches;
  
  $sql = "
    SELECT f.name 
    FROM chado.feature f
      INNER JOIN chado.featurepos lg ON lg.feature_id=f.feature_id
      INNER JOIN chado.featuremap map ON map.featuremap_id=lg.featuremap_id
    WHERE uniquename='$marker_name' 
      AND type_id = (SELECT cvterm_id FROM chado.cvterm 
                     WHERE name='genetic_marker' 
                       AND cv_id=(SELECT cv_id FROM chado.cv 
                                  WHERE name='sequence')
                     )
      AND map.name != '$mapname'";
  logSQL('', $sql);
  $sth = doQuery($dbh, $sql);
  while (($row = $sth->fetchrow_hashref)) {
    push @matches, $row-{'name'};
  }
  
  my $ret = ((scalar @matches) > 0) ? join ', ', @matches : 0;
  return $ret;
}#markerExistsOnOtherMap


sub publicationExists {
  my ($dbh, $citation) = @_;
  my ($sql, $sth, $row);

  $citation = decode("iso-8859-1", $citation);
  $sql = "SELECT pub_id FROM chado.pub WHERE uniquename='$citation'";
  logSQL('', "$sql");
  $sth = doQuery($dbh, $sql);
  if ($row=$sth->fetchrow_hashref) {
    return $row->{'pub_id'};
  }
  
  return 0;
}#publicationExists


sub qtlExists {
  my ($dbh, $qtl_name) = @_;
  my ($sql, $sth, $row);
  
  if ($qtl_name && $qtl_name ne 'NULL') {  
    $sql = "
        SELECT feature_id FROM chado.feature
        WHERE uniquename = '$qtl_name'
          AND type_id = (SELECT cvterm_id FROM chado.cvterm 
                         WHERE name = 'QTL'
                           AND cv_id=(SELECT cv_id 
                                      FROM chado.cv 
                                      WHERE name='sequence'))";
    logSQL('', $sql);
    $sth = doQuery($dbh, $sql);
    if ($row=$sth->fetchrow_hashref) {
      return $row->{'feature_id'};
    }
  }

  return 0;
}#qtlExists


sub readFile {
  use Encode;

  my $table_file = $_[0];

  # execute perl one-liner to fix line endings
  my $cmd = "perl -pi -e 's/(?:\\015\\012?|\\012)/\\n/g' $table_file";
  `$cmd`;

  open IN, "<:utf8", $table_file
      or die "\n\nUnable to open $table_file: $!\n\n";
  my @records = <IN>;
  close IN;

  my @hash_records;
  my (@cols, @field_names, $field_count);
  
  my $header_rows = 0;
  do {
#print $header_rows . ':' . $records[$header_rows] . "\n\n";
    next if ($records[$header_rows] =~ /^##/);  # A double-# marks comments at the head of a worksheet
    chomp $records[$header_rows];chomp $records[$header_rows];
    if ($records[$header_rows] =~ /^#/ && (scalar @field_names) == 0) {
      # First column that starts with #, so must be the header row
      $records[$header_rows] =~ s/#//;
      @cols = split "\t", $records[$header_rows];
      foreach my $col (@cols) {
        next if $col eq 'NULL';
        next if $col =~/TEMP/;
        $col =~ s/^\s+//;
        $col =~ s/\s+$//;
        push @field_names, $col;
      }

      $field_count = (scalar @field_names);
    }#heading row
    $header_rows++;
  } while ((scalar @field_names) == 0);
#print "Got header rows:\n" . Dumper(@field_names);
  
  for (my $i=$header_rows; $i<(scalar @records); $i++) {
    chomp $records[$i];
    next if ($records[$i] =~ /^#/);
    
    my @fields = split "\t", $records[$i];
    next if _allNULL(@fields);
    next if ((scalar @fields) == 0);
    if ((scalar @fields) < $field_count) {
      my $msg = "Insufficient columns. Expected $field_count, found " . (scalar @fields);
      reportError($i, $msg);
    }
    my %hash_record;
    for (my $j=0; $j<(scalar @field_names); $j++) {
      $fields[$j] =~ s/^"//;
      $fields[$j] =~ s/"$//;
      $hash_record{$field_names[$j]} = encode("utf8", $fields[$j]);
      # convert to Perl string format
#      $hash_record{$field_names[$j]} = decode("iso-8859-1", $fields[$j]);
#      $hash_record{$field_names[$j]} = $fields[$j];
    }
    if (!$hash_record{'unique_id'}) {
      $hash_record{'unique_id'} = uniqueID(5);
    }
    
    push @hash_records, {%hash_record};
  }

  print "File had " . (scalar @records) . " lines,";
  print " " . (scalar @hash_records) . " records were read successfully.\n";
  
  return @hash_records;
}#readFile


sub reportError {
  my ($line_count, $msg) = @_;
  if (!$line_count) {
    print "$msg\n";
  }
  else {
    print "  $line_count: $msg\n";
  }
}#reportError


sub traitExists {
  my ($dbh, $traitname) = @_;
  my ($sql, $sth, $row);
  
  if ($traitname && $traitname ne 'NULL') {
    $sql = "
      SELECT cvterm_id FROM chado.cvterm
      WHERE cv_id = (SELECT cv_id FROM chado.cv 
                     WHERE name='LegumeInfo:traits')
        AND name='$traitname'";
    logSQL('', $sql);
    $sth = doQuery($dbh, $sql);
    if ($row=$sth->fetchrow_hashref) {
      return $row->{'cvterm_id'};
    }
  }
  
  return 0;
}#traitExists


sub uniqueID {
  my $len = $_[0];
  my @a = map { chr } (48..57, 65..90, 97..122); 
  my $uniq;
  $uniq .=  $a[rand(@a)] for 1..$len;
  return $uniq;
}#uniqueID



################################################################################
####                          Internal functions                           #####
################################################################################

sub _allNULL {
  my $all_null = 1;
  foreach my $f (@_) {
    my $t = $f;
    $t =~ s/\s//;
    if ($t ne '' && $t ne 'NULL' && $t ne 'null') { 
      $all_null = 0; 
    }
  }
  return $all_null;
}#_allNULL


1;
