use strict;
use base 'Exporter';
use vars qw($VERSION @ISA @EXPORT @EXPORT_OK %EXPORT_TAGS);

package CropLegumeBaseLoaderUtils;

our $VERSION     = 1.00;
our @ISA         = qw(Exporter);
our @EXPORT      = (
                    qw(connectToDB), 
                    qw(dbxrefExists),
                    qw(doQuery), 
                    qw(experimentExists),
                    qw(getChromosomeID),
                    qw(getCvterm), 
                    qw(getMapSetID),
                    qw(getOBOTerm),
                    qw(getOrganismID),
                    qw(getOrganismMnemonic),
                    qw(getQTLid),
                    qw(getScaffoldID),
                    qw(getTrait), 
                    qw(logSQL),
                    qw(makeLinkageMapName),
                    qw(makeMappingPopulationName),
                    qw(makeMarkerName),
                    qw(makeQTLName),
                    qw(mapSetExists),
                    qw(markerExists),
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

=cut defined in db.pl above
sub connectToDB {

  # $g_connect_str, $g_user, and $g_pass defined in db.pl
  my $dbh = DBI->connect($g_connect_str, $g_user, $g_pass);

  $dbh->{AutoCommit} = 0;  # enable transactions, if possible
  $dbh->{RaiseError} = 1;

  return $dbh;
}#connectToDB
=cut


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
#print "start with: $sql\n";
  $sql = decode("iso-8859-1", $sql);
#print "end with: $sql\n";
  
  # Translate any UniCode chararcters in values array
  if (@vals && (scalar @vals) > 0) {
#print "start with:\n" . Dumper(@vals);
    my @tr_vals = map{ decode("iso-8859-1", $_) } @vals;
#print "translate:\n" . Dumper(@tr_vals);
    @vals = @tr_vals;
  }
  
  my $sth = $dbh->prepare($sql);
#print "end with:\n" . Dumper(@vals);
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


sub getCvterm {
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
  
  return 0;
}#getCvterm


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


sub getOBOTerm {
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
}#getOBOTerm


sub getOrganismID {
  my ($dbh, $mnemonic, $line_count) = @_;
  my ($sql, $sth, $row);
  
  $sql = "
    SELECT O.organism_id 
    FROM chado.organism O 
      INNER JOIN chado.organism_dbxref OD ON OD.organism_id=O.organism_id 
      INNER JOIN chado.dbxref D on D.dbxref_id=OD.dbxref_id 
    WHERE D.accession='$mnemonic'";
  $sth = $dbh->prepare($sql);
  $sth->execute();
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
  $sth = $dbh->prepare($sql);
  $sth->execute();
  if ($row=$sth->fetchrow_hashref) {
    return $row->{'organism_id'};
  }
  else {
    reportError($line_count, "unknown organism: [$mnemonic]");
    return 0;
  }
}#getOrganismMnemonic


sub getScaffoldID {
  my ($dbh, $scaffold, $version) = @_;
  my ($sql, $sth, $row);
  
  if (!$scaffold || $scaffold eq '' || $scaffold eq 'NULL' 
        || $scaffold eq 'none') {
    return 0;
  }
  
  $sql = "
    SELECT F.feature_id
    FROM chado.feature F
      INNER JOIN chado.featureprop FP
        ON FP.feature_id=F.feature_id
          AND FP.type_id = (SELECT cvterm_id FROM chado.cvterm WHERE name = 'assembly version')
    WHERE F.name='$scaffold' AND FP.value='$version'";
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


sub getTrait {
  my ($dbh, $trait) = @_;
  
  my $trait_id = getCvterm($dbh, $trait, 'LegumeInfo:traits');
  if (!$trait_id) {
    $trait_id = getCvterm($dbh, $trait, 'SOY');
  }
  if (!$trait_id) {
    $trait_id = getCvterm($dbh, $trait, 'soybean_whole_plant_growth_stage');
  }
  if (!$trait_id) {
    $trait_id = getCvterm($dbh, $trait, 'soybean_development');
  }
  if (!$trait_id) {
    $trait_id = getCvterm($dbh, $trait, 'soybean_structure');
  }
  if (!$trait_id) {
    $trait_id = getCvterm($dbh, $trait, 'soybean_trait');
  }
  
  return $trait_id;
}#getTrait


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
  my ($fields) = @_;
  
  my $lg_map_name = 0;
  if ($fields->{'map_name'} && $fields->{'map_name'} ne 'NULL'
        && $fields->{'lg'} && $fields->{'lg'} ne 'NULL') {
    $lg_map_name = "$fields->{'map_name'}-$fields->{'lg'}";
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
  my ($markerfield, $fields) = @_;
  my $uniq_marker_name = "$fields->{$markerfield}-$fields->{'specieslink_abv'}";
  return $uniq_marker_name;
}#makeMarkerName


sub makeQTLName {
  my ($fields) = @_;
  
  my $qtl_name = "$fields->{'qtl_symbol'} $fields->{'qtl_identifier'}";
  
  return $qtl_name;
}#makeQTLName


sub mapSetExists {
  my ($dbh, $mapname) = @_;
  my ($sql, $sth, $row);
  
  $sql = "SELECT featuremap_id FROM chado.featuremap WHERE name = '$mapname'";
  logSQL('', $sql);
  $sth = $dbh->prepare($sql);
  $sth->execute();
  if (($row = $sth->fetchrow_hashref)) {
    return $row->{'featuremap_id'};
  }
  else {
    return 0;
  }
}#mapSetExists


sub markerExists {
  my ($dbh, $marker) = @_;
  my ($sql, $sth, $row);

  my ($sql, $sth, $row);
  $sql = "
    SELECT * 
    FROM chado.feature
    WHERE uniquename='$marker' 
      AND type_id = (SELECT cvterm_id FROM chado.cvterm 
                     WHERE name='genetic_marker' 
                       AND cv_id=(SELECT cv_id FROM chado.cv 
                                  WHERE name='sequence')
                     )";
  logSQL('', $sql);
  $sth = doQuery($dbh, $sql);
  if (($row = $sth->fetchrow_hashref)) {
    return $row->{'feature_id'};
  }
  
  return 0;
}#makerExists


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
    next if ($records[$header_rows] =~ /^##/);
    chomp $records[$header_rows];chomp $records[$header_rows];
    if ($records[$header_rows] =~ /^#/ && (scalar @field_names) == 0) {
      $records[$header_rows] =~ s/#//;
      @cols = split "\t", $records[$header_rows];
      foreach my $col (@cols) {
        next if $col eq 'NULL';
        next if $col =~/TEMP/;
        push @field_names, $col;
      }

      $field_count = (scalar @field_names);
    }#heading row
    $header_rows++;
  } while ((scalar @field_names) == 0);
  
  for (my $i=$header_rows; $i<(scalar @records); $i++) {
    chomp $records[$i];
    next if ($records[$i] =~ /^#/);
    
    my @fields = split "\t", $records[$i];
    next if _allNULL(@fields);
    next if ((scalar @fields) == 0);
    if ((scalar @fields) < $field_count) {
      my $msg = "Insufficient columns. Expected $field_count, found " . (scalar @fields);
      reportError($i, $msg);
      next;
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
  print "  $line_count: $msg\n";
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
  foreach my $t (@_) {
    if ($t ne '' && $t ne 'NULL') { $all_null = 0; }
  }
  return $all_null;
}#_allNULL


1;
