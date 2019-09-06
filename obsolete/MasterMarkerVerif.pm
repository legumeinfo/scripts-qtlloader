package MasterMarkerVerif;

use strict;
use warnings;
use CropLegumeBaseLoaderUtils;
   
my %markers;
my %hash_of_markers;
my %marker_sequence;
#Get spreadsheet constants
my %mki = getSSInfo('MARKERS');
my %msi = getSSInfo('MARKER_SEQUENCE');

sub masterMarkerVerif {
    my ($input_dir,$dbh,$berkeley_dbh) = @_;
    my @filepaths = <$input_dir/*.txt>;
    my %files     = map { s/$input_dir\/(.*)$/$1/; $_ => 1} @filepaths;
    
    # Make sure we've got all the marker table files
    my $mkfile = $mki{'worksheet'}.'.txt';
    my $msfile = $msi{'worksheet'}.'.txt';
    if (!$files{$mkfile} && !$files{$msfile}) {
      $msg = "\nOne or more required tables related to markers is missing\n";
      $msg .= "Both MARKERS.txt and MARKER_SEQUENCE.txt are required.\n\n";
      reportError('', $msg);
      exit;
    }
      
    ###############      PREVIOUS COMMENTS BY ETHY    #####################################
    # marker.txt:
    # error: marker_type, marker_name, species, map_name, linkage_group, postion required
    # error: marker name must be unique within map collection (check 
    #      spreadsheet and database)
    # warning: if marker name appears in another map collection, curator 
    #      must verify that it is the same marker or give it a different name
    # error: map collection must exist in spreadsheet or database
    # warning: linkage groups already exist but there are markers that
    #   exceed the linkage groups min and max.
    # error: organism record must exist
    # error: if given, physical chromosome record must exist
    # warning: no QTL marker types will be loaded as markers
    # REVISIT: error: verify that alt_marker_names aren't already used
    # warning: check if primers already exist. If so, issue warning and
    #   report what features they are attached to.
    # error: if physical position indicated, assembly version, chromosome,
    #   start and stop positions are all provided
    # error: if physical position indicated, analysis record for assembly version exists
    # error: if physical position indicated start < end.
    
    
    #########################################################################################
    $wsfile = "$input_dir/MARKER.txt";
    print "\nReading records from $wsfile...\n";
    @records = readFile($wsfile);
    $has_errors   = 0;
    $has_warnings = 0;
    $line_count   = 0;
    foreach my $fields (@records) {
      $line_count++;
      #convenience:
      my $specieslink_abv = $fields->{$mki{'species_fld'}};
      my $marker_citation = $fields->{$mki{'marker_citation_fld'}};
      my $marker_name     = $fields->{$mki{'marker_name_fld'}};
      #my $marker_synonym  = $fields->{$mki{'marker_synonym_fld'}};
      my $marker_type     = $fields->{$mki{'marker_type_fld'}};
      my $assembly_name   = $fields->{$mki{'assembly_name_fld'}};
      my $phys_chr        = $fields->{$mki{'phys_chr_fld'}};
      my $phys_start      = $fields->{$mki{'phys_start_fld'}};
      my $phys_end        = $fields->{$mki{'phys_end_fld'}};
      my $accession       = $fields->{$mki{'accession_fld'}};
      my $accession_src   = $fields->{$mki{'accession_src_fld'}};
      # variables $accession, $accession_source, $SNP_pos are yet to be confirmed
      
      #species field must be set
      if(!isFieldSet($fields, $mki{'species_fld'})) {
        $has_errors++;
        $msg = "ERROR: specieslink abbrevation is missing";
        reportError($line_count,$msg);
      }
      
      #organism record must exist
      if (!getOrganismID($dbh, $specieslink_abv, $line_count)) {
        $has_errors++;
        $msg = "ERROR: The organism " . $specieslink_abv 
              ." does not exist in the database.";
        reportError($line_count, $msg);
      }
      
      # about marker_citation
      if (!isFieldSet($fields, $mki{'marker_citation_fld'})) {
        $has_errors++;
        $msg = "ERROR: marker_citation is missing";
        reportError($line_count, $msg);
      }
      elsif(!publicationExists($dbh, $marker_citation)) {
        $has_errors++;
        $msg = "FATAL ERROR: The marker citation $marker_citation"
              ." does not exist in the database";
        reportError($line_count, $msg);
      }
      
      #marker_name must exist
      if (!isFieldSet($fields, $mki{'marker_name_fld'})) {
        $has_errors++;
        $msg = "ERROR: marker name is missing";
        reportError($line_count, $msg);
      }
      else{
        my $marker_id = markerExists($dbh, $marker_name, $specieslink_abv);
        if (not defined $marker_id) {
          $has_warnings++;
          $msg = "Warning: This marker ($marker_name) has a different species names in db.";
          $msg.= " (or) marker may not be existing in db";
          reportError($line_count, $msg);
        }
        else {
        #Marker Name cannot be associated with same species and different publications    
            my $pub_id = feature_pubExists($dbh, $marker_id);
            if (defined $pub_id) {
                if ($pub_id != getPubID($dbh, $marker_citation)) {
                    $has_errors++;
                    $msg = "ERROR: The marker ($marker_name) with same species";
                    $msg.= "is associated with a different publication in the database.";
                    reportError($line_count,$msg);
                } 
            } 
        }
        #marker_name must be unique within MARKER sheet
        if ($markers{$marker_name}) {
            #checking if all fields are matching
            if ($hash_of_markers{$marker_name}{$specieslink_abv} && $hash_of_markers{$marker_name}{$marker_citation}
                && $hash_of_markers{$marker_name}{$marker_type}
                && $hash_of_markers{$marker_name}{$assembly_name} && $hash_of_markers{$marker_name}{$phys_chr}
                && $hash_of_markers{$marker_name}{$phys_start} && $hash_of_markers{$marker_name}{$phys_end}) {
                
                $has_warnings++;
                $msg = "Warning: The record at ($line_count) is a Duplicate record.";
                $msg.= " already exists in the spreadhseet\n\n";
                print $msg;
            }
            else {
                $has_errors++;
                $msg = "ERROR: Marker name ($marker_name) is already";
                $msg.= " existing with different details.";
                reportError($line_count,$msg);
            }
        }
      else {
        $hash_of_markers{$marker_name}{$specieslink_abv} = 1;
        $hash_of_markers{$marker_name}{$marker_citation} = 1;
        $markers{$marker_name}                           = 1;
        $berkeley_dbh->db_put($marker_name, 1);
        #$hash_of_markers{$marker_name}{$marker_synonym}  = 1;
        $hash_of_markers{$marker_name}{$marker_type}     = 1;
        $hash_of_markers{$marker_name}{$assembly_name}   = 1;
        $hash_of_markers{$marker_name}{$phys_chr}        = 1;
        $hash_of_markers{$marker_name}{$phys_start}      = 1;
        $hash_of_markers{$marker_name}{$phys_end}        = 1;
      }
    }
      
      #marker_type must exist
      if (!isFieldSet($fields, $mki{'marker_type_fld'})) {
        $has_errors++;
        $msg = "ERROR: marker_type is missing";
        reportError($line_count, $msg);
      }
      else{
        if ($marker_type eq 'SNP' && !isFieldSet($fields, $mki{'snp_pos_fld'})) {
            $has_errors++;
            $msg = "ERROR: SNP_pos cannot be NULL when the marker type is 'SNP'";
            reportError($line_count, $msg);
        }
        
      }
      
      #assembly_name, phys_chr, phys_start, phys_end.
      #If atleast one is set, all must be set
      if (!_allNULL($assembly_name, $phys_chr, $phys_start, $phys_end)) {
        checkeachNULL($fields, $mki{'assembly_name_fld'}, $mki{'phys_chr_fld'},
                      $mki{'phys_start_fld'}, $mki{'phys_end_fld'});
      }
      
      if (isFieldSet($fields, $mki{'accession_fld'}) && !isFieldSet($fields, $mki{'accession_src_fld'})) {
        $has_errors++;
        $msg = "ERROR: accession_source cannot be Null/empty when accession is not Null";
        reportError($line_count, $msg);
      }
            
    }#foreach - markers
    
    if ($has_errors || $has_warnings) {
      $msg = "\n\nThe master marker sheet has $has_errors error(s) and $has_warnings warning(s).";
      $msg.= " Unable to continue.\n\n";
      print $msg;
      exit;
    }
    
  ## Verification of MARKER SEQUENCE sheet
  
    $wsfile = "$input_dir/MARKER_SEQUENCE.txt";
    print "\nReading records from $wsfile\n";
    @records = readFile($wsfile);
    $line_count = 0;
    foreach my $fields(@records) {
      $line_count++;
      # convenience:
      my $specieslink_abv     = $fields->{$msi{'species_fld'}};
      my $marker_name         = $fields->{$msi{'marker_name_fld'}};
      my $sequence_type       = $fields->{$msi{'sequence_type_fld'}};
      my $accession           = $fields->{$msi{'genbank_acc_fld'}};
      my $sequence_name       = $fields->{$msi{'sequence_name_fld'}};
      my $marker_sequence     = $fields->{$msi{'marker_sequence_fld'}};
      my $forward_primer_name = $fields->{$msi{'forward_primer_name_fld'}};
      my $reverse_primer_name = $fields->{$msi{'reverse_primer_name_fld'}};
      my $forward_primer_seq  = $fields->{$msi{'forward_primer_seq_fld'}};
      my $reverse_primer_seq  = $fields->{$msi{'reverse_primer_seq_fld'}};
      
      #my %$msi{'forward_primer_name_fld'};
    
      #error: species field must exist
      if (!isFieldSet($fields, $msi{'species_fld'})) {
        $has_errors++;
        $msg = "ERROR: specieslink abbrevation is missing";
        reportError($line_count,$msg);
      }
    
      #error: organism record must exist
      if (!getOrganismID($dbh, $fields->{$msi{'species_fld'}}, $line_count)) {
        $has_errors++;
        $msg = "ERROR: The organism " . $fields->{$msi{'species_fld'}}
              ." does not exist in the database.";
        reportError($line_count, $msg);
      }
    
      #marker_name check: marker_name must exist
      if (!isFieldSet($fields, $msi{'marker_name_fld'})) {
        $has_errors++;
        $msg = "ERROR: marker name is missing";
        reportError($line_count, $msg);
      }
      elsif ($marker_sequence{$marker_name}) { #Marker name must be unique
        $has_errors++;
        $msg = "ERROR: This marker name ($marker_name) already exists in the spreadsheet\n";
        reportError($line_count, $msg);
      }
      else {
        $marker_sequence{$marker_name} = 1;
        if (!$markers{$marker_name}) { #Marker Name must exist in the MARKERS sheet.
          $has_errors++;
          $msg = "ERROR: The marker name ($marker_name) doesn't exist in the master marker sheet\n";
          reportError($line_count, $msg);
        }
      }
    
      # just calling this routine to check
      checkSequenceType($sequence_type, $accession, $sequence_name, $marker_sequence);
    
      #just calling this routine to check the 4 columns forward_primer_name, reverse_primer_name,
      #forward_primer_seq, reverse_primer_seq
      checkPrimer($forward_primer_name,$reverse_primer_name,$forward_primer_seq,$reverse_primer_seq);
    
      #error: either of genbank_accession or marker_sequence must be null
      if (_allNULL($accession, $marker_sequence)) {
        $has_errors++;
        $msg = "ERROR: Either 'Genbank_accession' or 'marker_sequence' must be NULL";
        reportError($line_count++, $msg);
      }

    } #foreach - marker_sequence
    
    if ($has_errors) {
      $msg = "\n\nThe marker sequence sheet has $has_errors errors. ";
      $msg.= " Unable to continue.\n\n";
      print $msg;
      exit;
    }  
}#masterMarkerVerif

##########      All Subroutines required for master marker verification starts here     ##########

    sub checkeachNULL() {
      my ( @fld_array ) = @_; #argument elemets are copied to an array
      my $counter = 1; #to traverse through array elements which are fld values
      my $fields = $fld_array[0];
      while ( $counter < scalar @fld_array ) {
        my $fld = $fld_array[$counter];
        $fields->{$fld} =~ s/^\s+|\s+$//g; #trim leading and trailing white spaces
        if ( !$fields->{$fld} || $fields->{$fld} eq ''
            || lc($fields->{$fld}) eq 'null' ) {
          $has_errors++;
          $msg = "ERROR: '$fld' is missing. Among (assembly_name), (phys_chr),";
          $msg.= " (phys_start) and (phys_end), Either ALL should be null";
          $msg.=" or NONE should be null";
          reportError($line_count, $msg);
        }#if
        $counter++;  
      }#while
    }#checkeachNULL
        
    sub checkSequenceType() {
      my ($seq_type, $genbank_acc, $seq_name, $marker_seq) = @_;
      if (!_allNULL($genbank_acc, $seq_name, $marker_seq)) {
        # check if seq_type is filled. Throw error if null
        if ($seq_type eq '' || $seq_type eq 'null' || $seq_type eq 'NULL') {
          $has_errors++;
          $msg = "ERROR: sequence type must be filled";
          $msg.= ".Sequence_type can't be null when atleast one of genbank_accession,";
          $msg.= " sequence_name and marker_sequence are not null";
          reportError($line_count++, $msg);
        }
      }
    }#checkSequenceType
    
    #checking forward_primer and reverse_primer
    sub checkPrimer() {
      my ($forward_primer_name,$reverse_primer_name,$forward_primer_seq,$reverse_primer_seq) = @_;
      if($marker_sequence{$forward_primer_name}) {
        $has_errors++;
        $msg = "ERROR: The forward primer name ($forward_primer_name) already";
        $msg.= "exists in the spreadsheet.\n";
        reportError($line_count, $msg);
      }
      else {
        $marker_sequence{$forward_primer_name} = 1;
      }
      if($marker_sequence{$reverse_primer_name}) {
        $has_errors++;
        $msg = "ERROR: The reverse primer name ($reverse_primer_name) already";
        $msg.= "exists in the spreadsheet.\n";
        reportError($line_count, $msg);
      }
      else {
        $marker_sequence{$reverse_primer_name} = 1;
      }
      if($marker_sequence{$forward_primer_seq}) {
        $has_errors++;
        $msg = "ERROR: The forward primer sequence ($forward_primer_seq) already";
        $msg.= "exists in the spreadsheet.\n";
        reportError($line_count, $msg);
      }
      else {
        $marker_sequence{$forward_primer_seq} = 1;
      }
      if($marker_sequence{$reverse_primer_seq}) {
        $has_errors++;
        $msg = "ERROR: The reverse primer sequence ($reverse_primer_seq) already";
        $msg.= "exists in the spreadsheet.\n";
        reportError($line_count, $msg);
      }
      else {
        $marker_sequence{$reverse_primer_seq} = 1;
      }
    }#checkPrimer
    
    sub feature_pubExists {
        my ($dbh, $marker_id) = @_;
        $sql = "SELECT pub_id FROM feature_pub WHERE feature_id = $marker_id";
        logSQL('', $sql);
        $sth = doQuery($dbh, $sql);
        return $row->{'pub_id'};
    }#feature_pubExists         


########################################################################################################

1;
    
