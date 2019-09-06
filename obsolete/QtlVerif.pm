package QtlVerif;

use strict;
use warnings;
use CropLegumeBaseLoaderUtils;

my %qtls;
# Get spreadsheet constants
my %qi  = getSSInfo('QTL');
my %mpi = getSSInfo('MAP_POSITIONS');

sub qtlVerif {
    my ($input_dir,$dbh,$berkeley_dbh) = @_;
    my @filepaths = <$input_dir/*.txt>;
    my %files     = map { s/$input_dir\/(.*)$/$1/; $_ => 1} @filepaths;
    my ($status,$value);
    
    # Make sure we've got all the qtl table file
    my $qtlfile = $qi{'worksheet'} . '.txt';
    my $mpfile  = $mpi{'worksheet'} . '.txt';
    if (!$files{$qtlfile} || !$files{$mpfile}) {
      $msg = "\nOne or more QTL tables are missing.\n";
      $msg .= "$qtlfile and $mpfile are required.\n\n";
      reportError('', $msg);
      exit;
    }
  
    # QTL.txt:
    # 1. experiment name must exist in spreadsheet or db
    # 2. trait name (qtl_symbol) must exist in db or spreadsheet
    # 3. map set (map_collection) must exist in db or spreadsheet
    # 4. linkage map (consensus_map+lg) must exist in db or spreadsheet
    # 5. nearest_marker, flanking markers must exist in db or spreadsheet (warning only)
    # 6. species name must exist
    
    $wsfile = "$input_dir/$qtlfile";
    print "\nReading qtl records from $wsfile\n";
    @records = readFile($wsfile);
    
    $has_errors = 0;
    $line_count = 0;
    
    foreach my $fields (@records) {
      $line_count++;

      my $qtl_name = $fields->{$qi{'species_fld'}} . '.'
                   . makeQTLName($fields->{$qi{'qtl_symbol_fld'}}, 
                                 $fields->{$qi{'qtl_identifier_fld'}});        
      my $qtl = makeQTLName($fields->{$qi{'qtl_symbol_fld'}},
                            $fields->{$qi{'qtl_identifier_fld'}});
      
      if ($qtls{$qtl_name}) {
        $has_errors++;
        $msg = "ERROR: QTL ($qtl_name) already exists in spreadsheet.";
        reportError($line_count, $msg);
      }
      elsif (qtlExists($dbh, $qtl_name)) {
        $has_warnings++;
        $msg = "warning: QTL ($qtl_name) already exists in database.";
        reportError($line_count, $msg);
        $sql = "
                SELECT p.uniquename FROM pub p
                where p.pub_id =
                              (SELECT fc.pub_id FROM feature_cvterm fc
                                 WHERE fc.feature_id =
                                                   (SELECT f.feature_id f FROM feature f
                                                    WHERE f.uniquename = '$qtl_name'))";
        my $pub_name = $dbh->selectrow_array($sql);
        my @citation = split(' exp',$fields->{qi{'qtl_expt_fld'}});
        if ($pub_name ne $citation[0]) {
          $has_errors++;
          $msg = "ERROR: This QTL ($qtl) has already been used for publication $pub_name";
          reportError($line_count,$msg);
        }
      }
      
      my $expt = $fields->{$qi{'qtl_expt_fld'}};
      $status = $berkeley_dbh->db_get($expt,$value);
      if ($status && !experimentExists($dbh, $expt)) {
        $has_errors++;
        $msg = "ERROR: experiment name '$expt' does not exist ";
        $msg .= "in spreadsheet or database.";
        reportError($line_count, $msg);
      }
      
      my $qtl_symbol = $fields->{$qi{'qtl_symbol_fld'}};
      $status = $berkeley_dbh->db_get($qtl_symbol,$value);
      if ($status) {
        if (!getTrait($dbh, $qtl_symbol)) {
          $has_errors++;
          $msg = "ERROR: QTL symbol ($qtl_symbol) is not defined in ";
          $msg .= "the spreadsheet or database.";
          reportError($line_count, $msg);
        }
      }
      
      my $uniq_marker_name;
      
      my $species = $fields->{$qi{'species_fld'}};
      my $nearest_marker = $fields->{$qi{'nearest_mrkr_fld'}};
      if ($nearest_marker ne '' && lc($nearest_marker) ne 'null') {
        $uniq_marker_name = makeMarkerName($fields->{$qi{'species_fld'}}, 
                                           $nearest_marker);
        $status = $berkeley_dbh->db_get($nearest_marker,$value);
        if ($status && !markerExists($dbh, $uniq_marker_name, $species)) {
          $has_warnings++;
          $msg = "warning: nearest marker ($nearest_marker) for species ";
          $msg .= "$species does not exist in spreadsheet or database. ";
          $msg .= "A stub record will be created, or the species can ";
          $msg .= "be changed at load time.";
          reportError($line_count, $msg);
        }
      }
      
      my $flanking_marker_low = $fields->{$qi{'flank_mrkr_low_fld'}};
      if ($flanking_marker_low ne '' && lc($flanking_marker_low) ne 'null') {
        $uniq_marker_name = makeMarkerName($fields->{'specieslink_abv'}, 
                                           $fields->{'flanking_marker_low'});
        $status = $berkeley_dbh->db_get($flanking_marker_low,$value);
        if ($status && !markerExists($dbh, $uniq_marker_name, $species)) {
          $has_warnings++;
          $msg = "warning: flanking marker low ($flanking_marker_low) ";
          $msg .= "does not exist in spreadsheet or database. ";
          $msg .= "A stub record will be created.";
          reportError($line_count, $msg);
        }
      }
      
      my $flanking_marker_high = $fields->{$qi{'flank_mrkr_high_fld'}};
      if ($flanking_marker_high ne '' && lc($flanking_marker_high) ne 'null') {
        $uniq_marker_name = makeMarkerName($fields->{'specieslink_abv'},
                                           $fields->{'flanking_marker_high'});
        $status = $berkeley_dbh->db_get($flanking_marker_high,$value);
        if ($status && !markerExists($dbh, $uniq_marker_name, $species)) {
          $has_warnings++;
          $msg = "warning: flanking marker high ($flanking_marker_high) ";
          $msg .= "does not exist in spreadsheet or database. ";
          $msg .= "A stub record will be created.";
          reportError($line_count, $msg);
        }
      }
      
      $species = $fields->{$qi{'species_fld'}};
      if (!getOrganismID($dbh, $species)) {
        $has_errors++;
        $msg = "ERROR: species name ($species) doesn't exist";
        reportError($line_count, $msg);
      }
      
      $qtls{$qtl} = 1;
      $qtls{$qtl_name} = 1;
    }#each record
  
    $qtl_errors = $has_errors;
    $qtl_warnings = $has_warnings;
    if ($has_errors) {
      print "\n\nThe QTL table has $qtl_errors errors and $qtl_warnings warnings.\n\n";
    }
    
    # MAP_POSITION.txt:
    # 1. QTL name must match an existing one in the spreadsheet
    # 2. map set (map_collection) must exist in db or spreadsheet
    # 3. left_end should be < right_end
    
    $wsfile = "$input_dir/$mpfile";
    print "\nReading map position records from $wsfile\n";
    @records = readFile($wsfile);
print "finished reading file\n";
    $has_errors   = 0;
    $line_count   = 0;
    foreach my $fields (@records) {
      $line_count++;

      my $qtl_name = makeQTLName($fields->{$mpi{'qtl_symbol_fld'}},
                                 $fields->{$mpi{'qtl_identifier_fld'}});
      
      my $ms_name = $fields->{$mpi{'map_name_fld'}};
      my $lg      = $fields->{$mpi{'lg_fld'}};
      my $mapname = makeLinkageMapName($ms_name, $lg);
      if (!$mapname || $mapname eq '' || lc($mapname) eq 'null') {
        $has_errors++;
        $msg = "ERROR: map name is missing from record.";
        reportError($line_count, $msg);
      }
      else {
        $status = $berkeley_dbh->db_get($mapname,$value);
        if ($status) {
          if (!linkageMapExists($dbh,$mapname)) {
            $has_warnings++;
            $msg = "Warning: linkage map '$mapname' is not defined in the ";
            $msg .= "spreadsheet or database.";
            reportError($line_count, $msg);
          }
        }
      }
      if (!$qtls{$qtl_name}) {
        $has_errors++;
        $msg = "ERROR: This QTL ($qtl_name) does not appear in the QTL SpreadSheet.";
        reportError($line_count, $msg);
      }
      
      my $left_end  = $fields->{$mpi{'left_end_fld'}};
      my $right_end = $fields->{$mpi{'right_end_fld'}};
      if ($left_end eq '' || lc($left_end) eq 'null'
            || $right_end eq '' || lc($right_end) eq 'null') {
        $has_errors++;
        $msg = "ERROR: missing left and/or right end coordinates for QTL ($qtl_name).";
        reportError($line_count, $msg);
      }
      elsif ($left_end > $right_end) {
        $has_errors++;
        $msg = "ERROR: left coordinate ($left_end) is larger than right coordinate ($right_end) for QTL ($qtl_name).";
        reportError($line_count, $msg);
      }
    }#each record
    
  if ($has_errors) {
    print "\n\nThe Map Position table has $has_errors errors and $has_warnings warnings.\n\n";
  }

  print "\n\n\nSpreadsheet verification is completed.\n";
  if ($qtl_errors) {
    print "There were $qtl_errors errors shown above in the QTL worksheet.\n";
  }
  print "There were $has_warnings warnings that should be checked.\n\n\n";
  
}

sub linkageMapExists {
  my ($dbh,$mapname) = @_;
  $sql = "
  SELECT feature_id FROM chado.feature
  WHERE uniquename = '$mapname'";
  logSQL('', $sql);
  $sth = doQuery($dbh,$sql);
  if ($row=$sth->fetchrow_hashref) {
      return $row->{'feature_id'};
  }
}#linkageMapExists


1;