# file: verifyWorksheets.pl
#
# purpose: check spreadsheet worksheets for errors and data integrity
#
# http://search.cpan.org/~capttofu/DBD-mysql-4.022/lib/DBD/mysql.pm
# http://search.cpan.org/~timb/DBI/DBI.pm
# http://perldoc.perl.org/Encode.html
#
# history:
#  05/09/13  eksc  created
#  09/05/14  eksc  modified for latest spreadsheet revisions


  use strict;
  use DBI;
  use Data::Dumper;
  use Getopt::Std;
  use Encode;
  use feature 'unicode_strings';

  # Load local util library
  use File::Spec::Functions qw(rel2abs);
  use File::Basename;
  use Path::Class;
  use lib dirname(rel2abs($0));
  use CropLegumeBaseLoaderUtils;
  
  my $warn = <<EOS
    Usage:
      
    $0 [opts] data-dir
      -p check publication files
      -e check experiment file
      -g check genetic map files
      -m check marker file
      -t check trait files
      -q check QTL file
      -a check all files
      
EOS
;
  if ($#ARGV < 1) {
    die $warn;
  }
  

  # What data sets need verifying?
  my ($do_pubs, $do_experiments, $do_genetic_maps, $do_markers, $do_traits, 
      $do_qtls);
  my %cmd_opts = ();
  getopts("pegmtqa", \%cmd_opts);
  if (defined($cmd_opts{'p'}) || defined($cmd_opts{'a'})) {$do_pubs         = 1;}
  if (defined($cmd_opts{'e'}) || defined($cmd_opts{'a'})) {$do_experiments  = 1;}
  if (defined($cmd_opts{'g'}) || defined($cmd_opts{'a'})) {$do_genetic_maps = 1;}
  if (defined($cmd_opts{'m'}) || defined($cmd_opts{'a'})) {$do_markers      = 1;}
  if (defined($cmd_opts{'t'}) || defined($cmd_opts{'a'})) {$do_traits       = 1;}
  if (defined($cmd_opts{'q'}) || defined($cmd_opts{'a'})) {$do_qtls         = 1;}
  
  my $input_dir = @ARGV[0];
  my @filepaths = <$input_dir/*.txt>;
  my %files = map { s/$input_dir\/(.*)$/$1/; $_ => 1} @filepaths;
  
  # Used all over
  my ($has_errors, $has_warnings, $qtl_errors, $qtl_warnings, $line_count, 
      $msg, $wsfile, $sql, $sth, $row, %fields, @records, @fields, $cmd, 
      $rv);
  
  
  # Track data warnings for entire data set:
  $has_warnings = 0;
  
  # Get connected to db
  my $dbh = connectToDB;

  # set default schema
  $sql = "SET SEARCH_PATH = chado";
  $sth = $dbh->prepare($sql);
  $sth->execute();
  

################################################################################
####                       PUBLICATION WORKSHEETS                          #####
################################################################################

  my %citations;
  if ($do_pubs) {
    
    # Get spreadsheet constants
    my %pi = getSSInfo('PUBS');

    # Make sure we've got all the pub table files
    my $pubsfile = $pi{'worksheet'} . '.txt';
    if (!$files{$pubsfile}) {
      $msg = "\nPublication table is missing.\n";
      $msg .= "$pubsfile is required.\n\n";
      reportError('', $msg);
      exit;
    }


    # pubs.txt:
    # 1. ref_type must exist and be in cvterm table
    # 2. citation can't already be in pub table or spread
    # 3. all citations must be unique in spreadsheet
    # 4. PMID, if present, should be a number
    # 5. DOI, if present, should look like a DOI
    # 6. author list is required
    $wsfile = "$input_dir/$pubsfile";
    print "\nReading publication records from $wsfile\n";
    @records = readFile($wsfile);

    $has_errors = 0;
    $line_count = 0;
    foreach my $fields (@records) {
      $line_count++;
      my $publink_citation = $fields->{$pi{'pub_fld'}};
      if ($publink_citation && $publink_citation ne '' 
            && $publink_citation ne 'NULL') {
        # check for required field ref_type
        my $ref_type = $fields->{$pi{'ref_type_fld'}};
        if ($ref_type ne '' && lc($ref_type) ne 'null' 
              && !getCvtermID($dbh, $ref_type, 'pub_type')) {
          $has_errors++;
          $msg = "ERROR: missing ref_type cvterm: $ref_type";
          reportError($line_count, $msg);
        }
        
        # make sure citation is unique
        my $enc_citation = encode("UTF-8", $publink_citation);
        if ($citations{$enc_citation}) {
          $has_errors++;
          $msg = "ERROR: citation has already been used in this spreadsheet: $publink_citation";
          reportError($line_count, $msg);
        }
        elsif (publicationExists($dbh, $publink_citation)) {
          $has_warnings++;
          $msg = "warning: citation already exists: ($publink_citation)";
          reportError($line_count, $msg);
        }
        else {
          $citations{$enc_citation} = 1;
        }
        
        # make sure PMID is a number
        my $pmid = $fields->{$pi{'pmid_fld'}};
        if ($pmid && lc($pmid) ne 'null') {
          if ($pmid == 0) {  # will be true if pmid is not a number
            $has_errors++;
            $msg = "ERROR: value given for PMID ($pmid) is not a number.";
            reportError($line_count, $msg);
          }
        }
        
        # verify DOI
        my $doi = $fields->{$pi{'doi_fld'}};
        if ($doi && lc($doi) ne 'null') {
          # All DOI numbers begin with a 10 and contain a prefix and a suffix 
          #   separated by a slash. The prefix is a unique number of four or more 
          #   digits assigned to organizations; the suffix is assigned by the 
          #   publisher.
          if (!($doi =~ /10\..*?\/.*/)) {
            $has_errors++;
            $msg = "ERROR: value given for DOI ($doi) doesn't ";
            $msg .= "look like a DOI identifier (10.<prefix>/<suffix>).\n";
            reportError($line_count, $msg);
          }
        }
        
        # verify author list
        my $authors = $fields->{$pi{'author_fld'}};
        if (!$authors || $authors eq '' || lc($authors) eq 'null') {
          $has_errors++;
          $msg = "ERROR: missing author list";
          reportError($line_count, $msg);
        }
      }
    }#each pub record
    
    if ($has_errors) {
      $msg = "The publication table has $has_errors errors. Unable to continue.\n\n";
      reportError('', $msg);
      exit;
    }
  }#do_pubs
  
  
################################################################################
####                      QTL EXPERIMENT WORKSHEET                         #####
################################################################################

  my %experiments;
  if ($do_experiments) {
    
    # get worksheet contants
    my %qei = getSSInfo('QTL_EXPERIMENTS');

    # Make sure we've got the experiment table file
    my $expfile = $qei{'worksheet'} . '.txt';
    if (!$files{$expfile}) {
      $msg = "\nthe experiment table is missing.\n";
      $msg .= "$expfile is required.\n\n";
      reportError('', $msg);
      exit;
    }
  
    # qtl_experiment.txt:
    # 1. citation must exist
    # 2. experiment name must be unique (in db and spreadsheet)
    # 3. species name must exist
    
    $wsfile = "$input_dir/$expfile";
    print "\nReading Experiment records from $wsfile\n";
    @records = readFile($wsfile);
    
    $has_errors = 0;
    $line_count = 0;
    foreach my $fields (@records) {
      $line_count++;
      
      my $publink_citation = $fields->{$qei{'pub_fld'}};
 
      if (!$publink_citation || $publink_citation eq ''
            || $publink_citation eq 'NULL') {
        $has_errors++;
        $msg = "ERROR: citation is missing";
        reportError($line_count, $msg);
      }
      else {
        my $enc_citation = encode("UTF-8", $publink_citation);
        if (!$citations{$enc_citation}
              && !publicationExists($dbh, $publink_citation)) {
          $has_errors++;
          $msg = "ERROR: citation ($publink_citation) doesn't match ";
          $msg .= "any citations in spreadsheet or database.";
          reportError($line_count, $msg);
        }
      }
      
      my $name = $fields->{$qei{'name_fld'}};
      my $enc_name = encode("UTF-8", $name);
      if ($experiments{$enc_name}) {
        $has_errors++;
        $msg = "ERROR: experiment name ($name) is not unique ";
        $msg .= "within spreadsheet";
        reportError($line_count, $msg);
      }
      elsif (experimentExists($dbh, $name)) {
        $has_warnings++;
        $msg = "warning: experiment name ($name) already used in ";
        $msg .= "database";
        reportError($line_count, $msg);
      }
      $experiments{$enc_name} = 1;
      
      my $species = $fields->{$qei{'species_fld'}};
      if (!getOrganismID($dbh, $species)) {
        $has_errors++;
        $msg = "ERROR: species name ($species) doesn't exist";
        reportError($line_count, $msg);
      }

      my $map_name = $fields->{$qei{'map_fld'}};
      if ($map_name eq '') {
        $has_errors++;
        $msg = "ERROR: map collection name not specified.";
        reportError($line_count, $msg);
      }

      my $geoloc = $fields->{$qei{'geoloc_field'}};
      if (length($fields->{'geolocation'}) > 255) {
        $has_errors++;
        $msg = "Geolocation description is too long: [$geoloc]";
        reportError($line_count, $msg);
      }
    }#each record
  
    if ($has_errors) {
      print "\n\nThe QTL experiment table has $has_errors errors. ";
      print "Unable to continue.\n\n";
      exit;
    }

  }#do experiments
  
  
################################################################################
####                           MARKER WORKSHEETS                           #####
################################################################################

  my %markers;
  my %hash_of_markers;
  my %marker_sequence;
  if ($do_markers) {
    
    #Get spreadsheet constants
    my %mki = getSSInfo('MARKERS');
    my %msi = getSSInfo('MARKER_SEQUENCE');
    
    $has_errors   = 0;
    $has_warnings = 0;
    $line_count   = 0;
    
    my $mkfile = $mki{'worksheet'}.'.txt';
    my $msfile = $msi{'worksheet'}.'.txt';
        
    # Make sure we've got all the marker table files
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
    $line_count = 0;
    foreach my $fields (@records) {
      $line_count++;
      #convenience:
      my $specieslink_abv = $fields->{$mki{'species_fld'}};
      my $marker_citation = $fields->{$mki{'marker_citation_fld'}};
      my $marker_name     = $fields->{$mki{'marker_name_fld'}};
      my $marker_synonym  = $fields->{$mki{'marker_synonym_fld'}};
      my $marker_type     = $fields->{$mki{'marker_type_fld'}};
      my $assembly_name   = $fields->{$mki{'assembly_name_fld'}};
      my $phys_chr        = $fields->{$mki{'phys_chr_fld'}};
      my $phys_start      = $fields->{$mki{'phys_start_fld'}};
      my $phys_end        = $fields->{$mki{'phys_end_fld'}};
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
      #marker_name must be unique within MARKER sheet
      elsif ($markers{$marker_name}) {
        $msg = "ERROR: The marker name ($marker_name) at line ($line_count)";
        $msg.= " already exists in the spreadsheet";
        reportError($line_count, $msg);
        #checking if all fields are matching
        if ($hash_of_markers{$marker_name}{$specieslink_abv} && $hash_of_markers{$marker_name}{$marker_citation}
          && $hash_of_markers{$marker_name}{$marker_synonym} && $hash_of_markers{$marker_name}{$marker_type}
          && $hash_of_markers{$marker_name}{$assembly_name} && $hash_of_markers{$marker_name}{$phys_chr}
          && $hash_of_markers{$marker_name}{$phys_start} && $hash_of_markers{$marker_name}{$phys_end}) {
          
          $has_errors++;
          $msg = "ERROR: This is a Duplicate record since all fields are repeated\n\n";
          print $msg;
        }
        else {
          $has_warnings++;
          $msg = "Warning: But other record details are different.";
          $msg.= " Please consider to review which one to retain\n\n";
          print $msg;
        }
      }
      else {
        $hash_of_markers{$marker_name}{$specieslink_abv} = 1;
        $hash_of_markers{$marker_name}{$marker_citation} = 1;
        $markers{$marker_name}                           = 1;
        $hash_of_markers{$marker_name}{$marker_synonym}  = 1;
        $hash_of_markers{$marker_name}{$marker_type}     = 1;
        $hash_of_markers{$marker_name}{$assembly_name}   = 1;
        $hash_of_markers{$marker_name}{$phys_chr}        = 1;
        $hash_of_markers{$marker_name}{$phys_start}      = 1;
        $hash_of_markers{$marker_name}{$phys_end}        = 1;
      }
      
      #marker synonym must be present
      if (!isFieldSet($fields, $mki{'marker_synonym_fld'})) {
        $has_errors++;
        $msg = "ERROR: marker_synonymn is missing";
        reportError($line_count, $msg);
      }      
      elsif ($markers{$marker_synonym}) {  #marker synonym must be unique
        $has_errors++;
        $msg = "ERROR: This marker synonym ($marker_synonym) already exists in the spreadsheet";
        reportError($line_count++, $msg);
      }
      else {
        $markers{$marker_synonym} = 1;
      }   
      
      #marker_type must exist
      if (!isFieldSet($fields, $mki{'marker_type_fld'})) {
        $has_errors++;
        $msg = "ERROR: marker_type is missing";
        reportError($line_count, $msg);
      }
      
      #assembly_name, phys_chr, phys_start, phys_end.
      #If atleast one is set, all must be set
      if (!_allNULL($assembly_name, $phys_chr, $phys_start, $phys_end)) {
        checkeachNULL($fields, $mki{'assembly_name_fld'}, $mki{'phys_chr_fld'},
                      $mki{'phys_start_fld'}, $mki{'phys_end_fld'});
      }   
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
          
    }#foreach - markers
    
    if ($has_errors || $has_warnings) {
      $msg = "\n\nThe master marker sheet has $has_errors error(s) and $has_warnings warning(s).";
      $msg.= " Unable to continue.\n\n";
      print $msg;
      exit;
    }
    
  ## Verification of MARKER SEQUENCE sheet
  #marker_sequence.txt
  
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
    
    } #foreach - marker_sequence
    
    if ($has_errors) {
      $msg = "\n\nThe marker sequence sheet has $has_errors errors. ";
      $msg.= " Unable to continue.\n\n";
      print $msg;
      exit;
    }
    
  }#do_markers
  
################################################################################
####                            MAP WORKSHEETS                             #####
################################################################################

  my %mapsets;
  my %linkagemaps;
  my %marker_position;
  if ($do_genetic_maps) {
    
    # Get spreadsheet constants
    my %mci = getSSInfo('MAP_COLLECTIONS');
    my %mi  = getSSInfo('MAPS');
    my %mpi = getSSInfo('MARKER_POSITION');

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
      $msg = "\nwarning: MAP_POSITION sheet is missing but optional";
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
        my $enc_citation = encode("UTF-8", $publink_citation);
        if (!$citations{$enc_citation}
              && !publicationExists($dbh, $publink_citation)) {
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
        $msg = "warning: this map collection name ($mapname)";
        $msg .= " is already in the database and will be updated.";
        reportError($line_count, $msg);
      }
      
      $mapsets{$mapname} = 1;
    }#each record
    
    if ($has_errors) {
      print "\n\nThe map collection table $mcfile has $has_errors errors.\n\n";
      exit;
    }
    
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
        next if (linkageMapExists($mapname));
            
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
               
        sub mapPositionCheck(){
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
        
        # species must exist
        my $species = $fields->{$mi{'species_fld'}};
        if (!getOrganismID($dbh, $species)) {
          $has_errors++;
          $msg = "ERROR: species name ($species) doesn't exist";
          reportError($line_count, $msg);
        }
          
        $linkagemaps{$mapname} = 1;
      }#each record
    
      if ($has_errors) {
        print "\n\nThe map table has $has_errors errors. Unable to continue.\n\n";
        exit;
      }
    }
    
   
    # marker_position.txt

    $wsfile = "$input_dir/MARKER_POSITION.txt";
    print "\nReading records from $wsfile\n";
    my @map_row;
    my $dir = dir("$input_dir");
    my $file = $dir->file("MAP.txt");
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
      if (!$marker_name) {
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
      elsif (markerExists($dbh, $marker_name, $mpi{'species_fld'})) {
        #checking if the marker is already existing in the database
        $has_warnings++;
        $msg = "warning: this marker_name ($marker_name)"
             . " has already been loaded"
             . " and will be updated.";
        reportError($line_count, $msg);
      }
      #error: position field must exist  
      if (!$position && $position ne '0') {
        $has_errors++;
        $msg = "ERROR: genetic position is missing";
        reportError($line_count, $msg);
      }
      #error: map name must exist
      if (!$mapname_marker) {
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
      ## about lg
      #error: linkage group(lg) must exist
      if (!$lg) {
        $has_errors++;
        $msg = "ERROR: linkage group is missing for $marker_name";
        reportError($line_count, $msg);
      }
      else {
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
                $msg = "ERROR: The linkage group ($lg) is out of bounds with the position $position";
                reportError($line_count, $msg);
              }#end of if-condition for lg check
              
            }#end of if-condition for position check
            
          }#end of else, if not starting with'#'
          
        }#end of while
        
      }#end of else, when lg is set
      
      my $lg_map_name = makeLinkageMapName($mapname_marker,$lg);
      my $lg_id = lgExists($dbh, $lg_map_name);
      if ($lg_id) {
        print " The linkage group ($lg) already exists in the database.\n";
        if(!checkLG($dbh,$position)) {
          $has_errors++;
          $msg = "ERROR: The linkage group position is out of bounds";
          $msg.= "with the position $position";
          reportError($line_count++, $msg);
         }   
      }
    
      sub lgExists() {
        my ($dbh, $lg_map_name) = @_;
        my ($sql, $sth, $row);
        if ($lg_map_name && $lg_map_name ne 'NULL') {
          $sql = "select feature_id from feature where uniquename='$lg_map_name'";
          logSQL('', $sql);
          $sth = doQuery($dbh, $sql);
          if ($row=$sth->fetchrow_hashref) {
            return $row->{'feature_id'};
          }
        }
        return 0;
      }#lgExists
      
      sub checkLG() {
        my ($dbh,$position) = @_;
        my ($sql, $sth, $row);
        my ($min, $max);
        my $count=0;
        $sql="select mappos from featurepos where feature_id = '$lg_id'";
        logSQL('',$sql);
        $sth = doQuery($dbh, $sql);
        while(my @lg_row=$sth->fetchrow_array) {
          $count++;
          if($count==1){ $min = $lg_row[0]; }
          else{ $max = $lg_row[0]; }
        }
        if ($position > $max || $position < $min) {
          return 0;
        }
        else { return 1; }
      }#checkLG
      
      ### about lg ends here
      ### verification of marker_position is finished here. except about cmap_accession.
      $marker_position{$marker_name} = 1;
    }#foreach - marker_position
    
    if ($has_errors) {
      print "\n\nThe marker position table has $has_errors errors. Unable to continue.\n\n";
      exit;
    }
    
  }#do genetic maps



  
################################################################################
####                           TRAIT WORKSHEETS                            #####
################################################################################

  my %traits;
  if ($do_traits) {
    
    # Get spreadsheet constants
    my %ti = getSSInfo('TRAITS');

    # Make sure we've got all the map table files
    my $traitfile = $ti{'worksheet'} . '.txt';
    if (!$files{$traitfile}) {
      $msg = "\nTrait table is missing.\n";
      $msg .= "$traitfile is required.\n\n";
      reportError('', $msg);
      exit;
    }
  
    # obs_trait.txt:
    # 1. ontology term must be valid if provided
    
    $wsfile = "$input_dir/$traitfile";
    print "\nReading trait records from $wsfile\n";
    @records = readFile($wsfile);
    
    $has_errors = 0;
    $line_count = 0;
    foreach my $fields (@records) {
      $line_count++;
      
      # See if trait already exists
#      my $trait_name = $fields->{$ti{'trait_name_fld'}};
#print "\ntrait name: $trait_name\n";
#      if (traitExists($dbh, $trait_name)) {
      my $qtl_symbol = $fields->{$ti{'qtl_symbol_fld'}};
#print "\ntrait name: $trait_name\n";
      if (traitExists($dbh, $qtl_symbol)) {
        $has_warnings++;
        $msg = "warning: trait name ($qtl_symbol) already exists in database.";
        reportError($line_count, $msg);
      }

      # See if obo term exists
      my $onto_id = $fields->{$ti{'onto_id_fld'}};
#print "ontology id: $onto_id\n";
      if ($onto_id && $onto_id ne '' && lc($onto_id) ne 'null') {
        if (!($onto_id =~ /^(.*?):(.*)/)) {
          $has_errors++;
          $msg = "ERROR: invalid OBO term format: [$onto_id]";
          reportError($msg);
        }
        else {
          my $db        = $1;
          my $accession = $2;
          my $dbxref_id;
          if (!($dbxref_id = dbxrefExists($dbh, $db, $accession))) {
            # non-fatal error (must be fixed, but don't hold up remaining process)
            $has_warnings++;
            $msg = "warning: Invalid or deleted OBO term: $onto_id";
            reportError($msg);
          }
          
          # Check if name in spread sheet matches name in db
          my $ss_obo_name = $fields->{$ti{'onto_name_fld'}};
#print "OBO name in ss: $ss_obo_name\n";
          if ($ss_obo_name) {
            my $obo_name = getOBOName($dbh, $dbxref_id);
            if (lc($obo_name) ne lc($ss_obo_name)) {
              $has_warnings++;
              $msg = "warning: OBO name in spreadsheet ($ss_obo_name) ";
              $msg .= "doesn't match offical name ($obo_name).";
              reportError($msg);
            }
          }
        }#onto term exists
      }#onto term provided in ss
      
      $traits{$qtl_symbol} = 1;
    }#each record
  
    if ($has_errors) {
      print "\n\nThe trait table has $has_errors errors.\n\n";
    }
  
  
    # parent_trait.txt:
    # 1. If there is a citation, it must exist in spreadsheet
    # 2. if ontology term provided it must exist
    # 3. Warn if parent stock is not defined in spread sheet or db
# TODO: THIS TABLE IS NOT YET BEING LOADED (06/25/13)
=cut
    $wsfile = "$input_dir/PARENT_TRAITS.txt";
    print "\nReading parent trait records from $wsfile\n";
    @records = readFile($wsfile);
    
    $has_errors = 0;
    $line_count = 0;
    foreach my $fields (@records) {
      $line_count++;
      my $publink_citation = $fields->{'publink_citation'};
      my $enc_citation = encode("UTF-8", $publink_citation);
print "\nCheck for [$publink_citation] in\n" . Dumper(%citations) . "\n\n";
      if ($publink_citation eq '' || $publink_citation eq 'NULL') {
        $has_warnings++;
        $msg = "warning: citation is missing";
        reportError($line_count, $msg);
      }
      elsif (!$citations{$enc_citation}
                && !publicationExists($dbh, $publink_citation)) {
        $has_errors++;
        $msg = "ERROR: citation ($publink_citation) doesn't match any ";
        $msg .= "citation in spreadsheet or database.";
        reportError($line_count, $msg);
      }

      if ($fields->{'ontology_number'} =~ /^(.*?):(.*)/) {
        my $db = $1;
        my $accession = $2;
        if (!dbxrefExists($dbh, $db, $accession)) {
          # non-fatal error (must be fixed, but don't hold up remaining process)
          $has_warnings++;
          $msg = "warning: invalid or deleted OBO term: $fields->{'ontology_number'}";
          reportError($msg);
        }
      }
# TODO: check for stock in db or spreadsheet; warn if missing
    }#each record
  
    if ($has_errors) {
      print "\n\nThe parent trait table has $has_errors errors.\n\n";
    }
=cut

  }#do traits
  

################################################################################
####                            QTL WORKSHEET                              #####
################################################################################

  my %qtls;
  if ($do_qtls) {
    
    # Get spreadsheet constants
    my %qi  = getSSInfo('QTL');
    my %mpi = getSSInfo('MAP_POSITIONS');

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
      if ($qtls{$qtl_name}) {
        $has_errors++;
        $msg = "ERROR: QTL ($qtl_name) already exists in spreadsheet.";
        reportError($line_count, $msg);
      }
      elsif (qtlExists($dbh, $qtl_name)) {
        $has_warnings++;
        $msg = "warning: QTL ($qtl_name) already exists in database.";
        reportError($line_count, $msg);
      }
      
      my $expt = $fields->{$qi{'qtl_expt_fld'}};
      if (!$experiments{$expt} && !experimentExists($dbh, $expt)) {
        $has_errors++;
        $msg = "ERROR: experiment name '$expt' does not exist ";
        $msg .= "in spreadsheet or database.";
        reportError($line_count, $msg);
      }
      
      my $qtl_symbol = $fields->{$qi{'qtl_symbol_fld'}};
      if (!$traits{$qtl_symbol}) {
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
        if (!$markers{$nearest_marker} 
                && !markerExists($dbh, $uniq_marker_name, $species)) {
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
        if (!$markers{$flanking_marker_low}
              && !markerExists($dbh, $uniq_marker_name)) {
          $has_warnings++;
          $msg = "warning: flanking marker low ($flanking_marker_low) ";
          $msg .= "does not exist in spreadsheet or database. ";
          $msg .= "A stub record will be created.";
          reportError($line_count, $msg);
        }
      }
      
      my $flanking_marker_high = $fields->{$qi{'flanking_marker_high'}};
      if ($flanking_marker_high ne '' && lc($flanking_marker_high) ne 'null') {
        $uniq_marker_name = makeMarkerName($fields->{'specieslink_abv'},
                                           $fields->{'flanking_marker_high'});
        if (!$markers{$flanking_marker_high}
              && !markerExists($dbh, $uniq_marker_name)) {
          $has_warnings++;
          $msg = "warning: flanking marker high ($flanking_marker_high) ";
          $msg .= "does not exist in spreadsheet or database. ";
          $msg .= "A stub record will be created.";
          reportError($line_count, $msg);
        }
      }
      
      my $species = $fields->{$qi{'species_fld'}};
      if (!getOrganismID($dbh, $species)) {
        $has_errors++;
        $msg = "ERROR: species name ($species) doesn't exist";
        reportError($line_count, $msg);
      }
      
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
        if (!$linkagemaps{$mapname}) {
          if (!linkageMapExists($mapname)) {
            $has_warnings++;
            $msg = "Warning: linkage map '$mapname' is not defined in the ";
            $msg .= "spreadsheet or database.";
            reportError($line_count, $msg);
          }
        }
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
      
      $line_count++;
    }#each record
    
  if ($has_errors) {
    print "\n\nThe Map Position table has $has_errors errors and $has_warnings warnings.\n\n";
  }

  print "\n\n\nSpreadsheet verification is completed.\n";
  if ($qtl_errors) {
    print "There were $qtl_errors errors shown above in the QTL worksheet.\n";
  }
  print "There were $has_warnings warnings that should be checked.\n\n\n";
  }#check QTL tables


################################################################################
################################################################################
################################################################################
################################################################################



sub consensusMapExists {
  my $mapname = $_[0];
  if ($mapname ne '') {
    $sql = "SELECT featuremap_id FROM chado.featuremap WHERE name='$mapname'";
    logSQL('', "$line_count:$sql");
    $sth = doQuery($dbh, $sql);
    if ($row=$sth->fetchrow_hashref()) {
      return 1;
    }
  }
  
  return 0;
}#consensusMapExists


sub dbExists {
  my $dbname = lc($_[0]);
  if ($dbname ne '') {
    $sql = "SELECT db_id FROM chado.db WHERE LOWER(name)='$dbname'";
    logSQL('', "$line_count:$sql");
    $sth = doQuery($dbh, $sql);
    if ($row=$sth->fetchrow_hashref()) {
      return 1;
    }
  }
  
  return 0;
}#dbExists


sub linkageMapExists {
  my ($lg_mapname) = @_;
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


sub unitExists {
  my ($dbh, $unit) = @_;
  
  if ($unit) {
    my $sql = "
      SELECT cvterm_id FROM cvterm 
             WHERE name='$unit' 
                   AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='featuremap_units')";
    logSQL('', "$line_count:$sql");
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    if (my $row=$sth->fetchrow_hashref) {
      return 1;
    }
  }
  
  return 0;
}#unitExists


sub scaffoldExists {
  my $chromosome = $_[0];
  my ($sql, $sth, $row);

  if (!$chromosome || $chromosome eq '' || $chromosome eq 'NULL' 
        || $chromosome eq 'none') {
    return 1;
  }
  
  $sql = "
    SELECT * FROM chado.feature
    WHERE name='$chromosome' 
      AND type_id=(SELECT cvterm_id FROM chado.cvterm 
                   WHERE name='scaffold'
                     AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='local'))";
   logSQL('', "$line_count:$sql");
   $sth = doQuery($dbh, $sql);
   if ($row=$sth->fetchrow_hashref) {
     return 1;
   }

   return 0;
}#scaffoldExists

