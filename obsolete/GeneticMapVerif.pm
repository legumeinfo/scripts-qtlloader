package GeneticMapVerif;

use strict;
use warnings;
use BerkeleyDB;
#use diagnostics;
use Encode;
#use Data::Dumper;
use Path::Class;
use PubVerif;
use CropLegumeBaseLoaderUtils;
#use Exporter qw(import);
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(geneticMapVerif);
#use vars qw(%citations);
  
    my %mapsets;
    my %linkagemaps;
    my %marker_position;
       
    # Get spreadsheet constants
    my %mci = getSSInfo('MAP_COLLECTIONS');
    my %mi  = getSSInfo('MAPS');
    my %mpi = getSSInfo('MARKER_POSITION');

    sub geneticMapVerif {
      
    my ($input_dir,$dbh,$berkeley_dbh) = @_;
    my @filepaths = <$input_dir/*.txt>;
    my %files     = map { s/$input_dir\/(.*)$/$1/; $_ => 1} @filepaths;
    my $dir = dir("$input_dir");
    my $file = $dir->file("MAP.txt");
    
    $has_warnings = 0;
    $line_count   = 0;
    $has_errors   = 0;

    # Make sure we've got all the map table files: 
    #   MAP_COLLECTIONS required, MAPS optional
    my $mcfile = $mci{'worksheet'} . '.txt';
    my $mfile  = $mi{'worksheet'} . '.txt';
    my $mpfile = $mpi{'worksheet'}.'.txt';
    
    if (!$files{$mcfile}) {
      $has_errors++;
      $msg = "\nOne or more required map tables is missing.\n";
      $msg .= "$mcfile is required .\n\n";
      reportError('', $msg);
      exit;
    }
    if (!$files{$mfile}) {
      $has_warnings++;
      $msg = "\nwarning: $mfile is missing but optional.\n";
      reportError('', $msg);
    }
    if (!$files{$mpfile}) {
      $has_warnings++;
      $msg = "\nwarning: MARKER_POSITION sheet is missing but optional";
      reportError('', $msg);
    }
    
    
    # map_collection.txt:
    # 1. citations must exist
    # 2. species name must exist
    # 3. map unit must be set and exist
    # 4. map name must not be duplicated in this spreadsheet
    
    $wsfile = "$input_dir/$mcfile";
    print "\nReading map collection records from $wsfile\n";
    @records = readFile($wsfile);
    foreach my $fields (@records) {
      $line_count++;
      
      # check citation
      my @publink_citations = split ';', $fields->{$mci{'pub_fld'}};
      foreach my $publink_citation (@publink_citations) {
        $publink_citation =~ s/^\s//;
        $publink_citation =~ s/\s+$//;
        if (!$publink_citation || $publink_citation eq ''
              || $publink_citation eq 'NULL') {
          $has_errors++;
          $msg = "ERROR: citation is missing";
          reportError($line_count, $msg);
        }
        my $value;
        my $enc_citation = encode("UTF-8", $publink_citation);
        my $status = $berkeley_dbh->db_get($enc_citation,$value);
        if ($status && !publicationExists($dbh, $publink_citation)) {
          $has_errors++;
          $msg = "ERROR: citation ($publink_citation) doesn't match any ";
          $msg .= " citations in spreadsheet or database.";
          reportError($line_count, $msg);
        }
      }
      
      # check species
      my $species = $fields->{$mci{'species_fld'}};
      if (!getOrganismID($dbh, $species)) {
        $has_errors++;
        $msg = "ERROR: species name ($species) doesn't exist";
        reportError($line_count, $msg);
      }
      
      # check map unit
      my $unit = $fields->{$mci{'unit_fld'}};
      if (!unitExists($dbh, $unit)) {
        $has_errors++;
        $msg = "ERROR: map unit [$unit] is not set or doesn't exist in the ";
        $msg .= "featuremap_unit controlled vocabulary.";
        reportError($line_count, $msg);
      }
      
      # check map name
      my $mapname = $fields->{$mci{'map_name_fld'}};
      if ($mapsets{$mapname}) {
        $has_errors++;
        $msg = "ERROR: map collection name ($mapname) already exists in spreadsheet";
        reportError($line_count, $msg);
      }
      elsif (mapSetExists($dbh, $mapname)) {
        $has_warnings++;
        $msg = "warning: This map collection name ($mapname)";
        $msg .= " is already in the database and will be updated.";
        reportError($line_count, $msg);
      }
      
      $mapsets{$mapname} = 1;
    }#each record
    
    if ($has_errors) {
      print "\n\nThe map collection table $mcfile has $has_errors errors.\n\n";
      exit;
    }
    
    sub unitExists {
      my ($dbh, $unit) = @_;
      if ($unit) {
        my $sql = "
        SELECT cvterm_id FROM cvterm
        WHERE name='$unit'
        AND cv_id =
                    (SELECT cv_id FROM chado.cv WHERE name='featuremap_units')";
        logSQL('', "$line_count:$sql");
        my $sth = $dbh->prepare($sql);
        $sth->execute();
        if (my $row=$sth->fetchrow_hashref) {
          return 1;
        }
      }
      return 0;
    }#unitExists

    
    # MAPs.txt:
    # 1. map name must be unique in db and spreadsheet
    # 2. must be a map set record
    # 3. start and end coordinates must be specified
    # 4. start <= end
    # 5. species name must exist
    
    $wsfile = "$input_dir/$mfile";
    if (-e $wsfile) {
      print "\nReading map records from $wsfile\n";
      @records = readFile($wsfile);
        
      $has_errors = 0;
      $line_count = 0;
      foreach my $fields (@records) {
        $line_count++;
    
        my $ms_name = $fields->{$mi{'map_name_fld'}};
        my $lg      = $fields->{$mi{'lg_fld'}};
        my $mapname = makeLinkageMapName($ms_name, $lg);
        next if (linkageMapExists($dbh,$mapname));
            
        # check for unique name
        if ($linkagemaps{$mapname}) {
          $has_errors++;
          $msg = "ERROR: linkage map name ($mapname) is not unique within spreadsheet";
          reportError($line_count, $msg);
        }
          
        # make sure there is an associated map collection record
        if (!$mapsets{$ms_name} && !mapSetExists($dbh, $ms_name)) {
          $has_errors++;
          $msg = "ERROR: Map set record (map_collection) for ";
          $msg .= "$ms_name does not exist in spreadsheet or database.";
          reportError($line_count, $msg);
        }
        
        mapPositionCheck($fields->{$mi{'map_start_fld'}}, $fields->{$mi{'map_end_fld'}});
        
        # species must exist
        my $species = $fields->{$mi{'species_fld'}};
        if (!getOrganismID($dbh, $species)) {
          $has_errors++;
          $msg = "ERROR: species name ($species) doesn't exist";
          reportError($line_count, $msg);
        }  
        $linkagemaps{$mapname} = 1;
        $berkeley_dbh->db_put($mapname, 1); 
      }#each record
    
      if ($has_errors) {
        print "\n\nThe map table has $has_errors errors. Unable to continue.\n\n";
        exit;
      }
    }

#############   Subroutines required for MAP check   ####################

    sub linkageMapExists {
      my ($dbh,$lg_mapname) = @_;
      if ($linkagemaps{$lg_mapname}) {
        return 1;
      }
      else {
        my ($sql, $sth, $row);
        $sql = "SELECT * FROM chado.feature WHERE name='$lg_mapname'";
        logSQL('', "$line_count:$sql");
        $sth = doQuery($dbh, $sql);
        if ($row=$sth->fetchrow_hashref) {
          return 1;
        }
      }
      return 0;
    }#linkageMapExists
    
    sub mapPositionCheck{
      my ($map_start, $map_end) = @_;
      #make sure that if either of start or end is set, the other must be set
      if ($map_start eq '' || lc($map_start) eq 'null' ||
          $map_end eq '' || lc($map_end) eq 'null') {
        
        if (!_allNULL($map_start, $map_end)) {
          $has_errors++;
          $msg = "ERROR: start and end positions, either both must be set";
          $msg.= " or both must be null";
          reportError($line_count, $msg);
        } #inner if
        
      }#outer if
      # make sure that always end position is greater than start position
      elsif ($map_end < $map_start) {
        $has_errors++;
        reportError($line_count, "ERROR: map end is < map start");
      }#elsif
    }#mapPositionCheck
    
################################################################################
    
    # marker_position.txt
    $wsfile = "$input_dir/MARKER_POSITION.txt";
    print "\nReading records from $wsfile\n";
    @records = readFile($wsfile);
    $line_count = 0;

    $has_errors   = 0;
    $has_warnings = 0;
    $line_count   = 0;
    
    foreach my $fields(@records) {
      $line_count++;
      
      # convenience:
      my $species = $fields->{$mpi{'species_fld'}};
      my $marker_name = $fields->{$mpi{'marker_name_fld'}};
      my $alt_marker_name = $fields->{$mpi{'alt_marker_name_fld'}};
      my $mapname_marker = $fields->{$mpi{'map_name_fld'}};
      my $lg = $fields->{$mpi{'lg_fld'}};
      my $position = $fields->{$mpi{'position_fld'}};
      
      #error: species field must exist
      if (!isFieldSet($fields, $mpi{'species_fld'})) {
        $has_errors++;
        $msg = "ERROR: specieslink abbrevation is missing";
        reportError($line_count,$msg);
      }
      #error: organism record must exist
      if (!getOrganismID($dbh, $fields->{$mpi{'species_fld'}}, $line_count)) {
        $has_errors++;
        $msg = "ERROR: The organism " . $fields->{$mpi{'species_fld'}}
             . " does not exist in the database.";
             reportError($line_count, $msg);
      }
      #error: marker_name must exist
      if (!isFieldSet($fields, $mpi{'marker_name_fld'})) {
        $has_errors++;
        $msg = "ERROR: marker name is missing";
        reportError($line_count, $msg);
      }
      elsif ($marker_position{$marker_name}) {
        #checking uniqueness of the marker name in MARKER_POSITION sheet
        $has_errors++;
        $msg = "ERROR: This marker ($marker_name) already exists";
        $msg.= " in the spreadsheet.";
        reportError($line_count, $msg);
      }
      
      #alt_marker name must be present
      if (!isFieldSet($fields, $mpi{'alt_marker_name_fld'})) {
        $has_errors++;
        $msg = "ERROR: alt_marker_name is missing";
        reportError($line_count, $msg);
      }      
      elsif ($marker_position{$alt_marker_name}) {  #marker synonym must be unique
        $has_errors++;
        $msg = "ERROR: This alternate marker name ($alt_marker_name) already exists in the spreadsheet";
        reportError($line_count++, $msg);
      }
      else {
        $marker_position{$alt_marker_name} = 1;
      }   
      
      markerCheck($dbh, $marker_name, $species); #do all checks on marker_name
      
      #error: position field must exist  
      if (!isFieldSet($fields, $mpi{'position_fld'}) && $position ne '0') {
        $has_errors++;
        $msg = "ERROR: genetic position is missing";
        reportError($line_count, $msg);
      }
      #error: map name must exist
      if (!isFieldSet($fields, $mpi{'map_name_fld'})) {
        $has_errors++;
        $msg = "ERROR: map name is missing";
        reportError($line_count, $msg);
      }
      else {
        #error: map collection must exist in spreadsheet or database
        if (!$mapsets{$mapname_marker} && !mapSetExists($dbh, $mapname_marker)) {
          $has_errors++;
          $msg = "ERROR: The map set $mapname_marker does not exist in the spreadsheet"
              . " or database.";
          reportError($line_count, $msg);
        }
      }
      # about lg
      #error: linkage group(lg) must exist
      if (!isFieldSet($fields, $mpi{'lg_fld'})) {
        $has_errors++;
        $msg = "ERROR: linkage group is missing for $marker_name";
        reportError($line_count, $msg);
      }
      else {
        positionSheetCheck($lg,$position,$marker_name,$file); #check the position in Spreadsheet.
      }
        
      positionDBCheck($dbh,$lg,$position,$mapname_marker); #check the position in DB.
      
      ### about lg ends here
            
      $marker_position{$marker_name} = 1;
    }#foreach - marker_position
    
    ### verification of marker_position is finished here. except about cmap_accession.
    
    if ($has_errors || $has_warnings) {
      $msg = "\n\nThe Marker Position table has $has_errors error(s)";
      $msg.= " and $has_warnings warning(s). Unable to continue..\n\n";
      print $msg;
      exit;
    }     
  }
    
#######    All Subroutines required for MARKER_POSITION check starts here   ######
##########****************************************************************########
  sub markerCheck {
        my ($dbh, $marker_name, $species) = @_;
        my $accession;
        if (markerExists($dbh, $marker_name, $mpi{'species_fld'})) {
        #checking if the marker is already existing in the database
        $has_warnings++;
        $msg = "Warning: This marker_name ($marker_name)"
             . " has already been loaded"
             . " and will be updated.";
        reportError($line_count, $msg);
        }
        $sql = "SELECT f.organism_id FROM feature f
                WHERE f.name = '$marker_name'";
        logSQL('', $sql);
        $sth = doQuery($dbh,$sql);
        while(my @org_id=$sth->fetchrow_array) {
          $sql = "
          SELECT dx.accession FROM dbxref dx
          WHERE dx.dbxref_id IN
                            (SELECT od.dbxref_id FROM organism_dbxref od
                             WHERE od.organism_id = $org_id[0])
           AND dx.db_id =
                         (SELECT d.db_id FROM db d
                          WHERE d.name = 'uniprot:species')";
           $accession = $dbh->selectrow_array($sql);
           if ($accession ne $species) {
            $has_warnings++;
            $msg = "Warning: The marker ($marker_name) is already associated with";
            $msg.= " different species ($accession) in the Database";
            reportError($line_count, $msg);
           } 
        }
      }#markerCheck
        
      sub positionSheetCheck {
        my ($lg, $position, $marker_name,$file) = @_;
        my @map_row;
        open(my $file_handle, "<", $file) || die "Failed to open the file:\n";
        while (<$file_handle>) {
          if ($_=~ m/^#/) {
            next;
          }
          else {
            @map_row = split('\t', $_);
            if ($map_row[2] eq $lg) {
              if ($position < $map_row[3] || $position > $map_row[4] ) {
                $has_errors++;
                $msg = "ERROR: The marker ($marker_name) is out of bounds";
                $msg.= " on the linkage group ($lg) with position $position";
                reportError($line_count, $msg);
              }#end of if-condition for lg check
              
            }#end of if-condition for position check
            
          }#end of else, if not starting with'#'
          
        }#end of while
        
      }#positionSheetCheck
      
      sub positionDBCheck {
        my ($dbh,$lg,$position,$mapname_marker,$marker_name) = @_;
        my $lg_map_name = makeLinkageMapName($mapname_marker,$lg);
        my $lg_id = lgExists($dbh, $lg_map_name);
        if ($lg_id!=0) {
          my $min = checkLG($dbh,$lg_id,'start');
          my $max = checkLG($dbh,$lg_id,'stop');
          if ($position < $min || $position > $max) {
            $has_errors++;
            $msg = "ERROR: The marker ($marker_name) is out of bounds on the linkage group ($lg)";
            $msg.= " with position $position";
            reportError($line_count,$msg);
          }
        }
      }#positionDBCheck
    
      sub lgExists {
        my ($dbh, $lg_map_name) = @_;
        my ($sql, $sth, $row);
        if ($lg_map_name && $lg_map_name ne 'NULL') {
          $sql = "SELECT feature_id FROM feature WHERE uniquename='$lg_map_name'";
          logSQL('', $sql);
          $sth = doQuery($dbh, $sql);
          if ($row=$sth->fetchrow_hashref) {
            return $row->{'feature_id'};
          }
        }
        return 0;
      }#lgExists
      
      sub checkLG {
        my ($dbh,$lg_id,$coord) = @_;
        # earlier query: "select mappos from featurepos where feature_id = $lg_id"; 
        $sql = "
        SELECT fp.mappos FROM chado.featurepos fp
           INNER JOIN chado.featuremap m
              ON m.featuremap_id = fp.featuremap_id
           INNER JOIN chado.featureposprop fpp
              ON fpp.featurepos_id = fp.featurepos_id
        WHERE fp.feature_id = $lg_id
                 AND fpp.type_id=(SELECT cvterm_id FROM chado.cvterm 
                                   WHERE name='$coord'
                                         AND cv_id=(SELECT cv_id FROM chado.cv 
                                                    WHERE name='featurepos_property'))";
        logSQL('',$sql);
        $sth = doQuery($dbh, $sql);
        return $row->{'mappos'};
      }#checkLG
#################### All Subroutines for MARKER_POSITION check ends here  ###################
####################*****************************************************####################

1;
    
