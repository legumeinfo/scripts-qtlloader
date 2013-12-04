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


  use strict;
  use DBI;
  use Data::Dumper;
  use Getopt::Std;
  use Encode;

  # Load local util library
  use File::Spec::Functions qw(rel2abs);
  use File::Basename;
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
  my ($has_errors, $line_count, $msg, $wsfile, $sql, $sth, $row, %fields, 
      @records, @fields, $cmd, $rv);
  
  # Track data warnings for entire data set:
  my $has_warnings = 0;
  
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
    
    # Make sure we've got all the pub table files
    if (!$files{'PUBS.txt'} || !$files{'PUB_AUTHORS.txt'} 
          || !$files{'PUB_KEYWORDS.txt'} || !$files{'PUB_URLS.txt'}) {
      $msg = "\nOne or more publication tables is missing.\n";
      $msg .= "PUBS.txt, PUB_AUTHORS.txt, PUB_KEYWORDS.txt and PUB_URLS.txt ";
      $msg .= "are required.\n\n";
      reportError('', $msg);
      exit;
    }


    # pubs.txt:
    # 1. ref_type must be in cvterm table
    # 2. citation can't already be in pub table or spread
    # 3. all citations must be unique in spreadsheet
    # 4. PMID, if present, should be a number
    # 5. DOI, if present, should look like a DOI
    
    $wsfile = "$input_dir/PUBS.txt";
    print "\nReading publication records from $wsfile\n";
    @records = readFile($wsfile);
  
    $has_errors = 0;
    $line_count = 0;
    foreach my $fields (@records) {
      $line_count++;
      if ($fields->{'publink_citation'} && $fields->{'publink_citation'} ne ''
            && $fields->{'publink_citation'} ne 'NULL') {
        # check for required field ref_type
        if ($fields->{'ref_type'} ne '' && lc($fields->{'ref_type'}) ne 'null' 
              && !getCvterm($dbh, $fields->{'ref_type'}, 'local')) {
          $has_errors++;
          $msg = "ERROR: missing ref_type cvterm: $fields->{'ref_type'}";
          reportError($line_count, $msg);
        }
        
        # make sure citation is unique
        if ($citations{$fields->{'publink_citation'}}) {
          $has_errors++;
          $msg = "ERROR: citation has already been used in this spreadsheet: $fields->{'citation'}";
          reportError($line_count, $msg);
        }
        elsif (publicationExists($dbh, $fields->{'publink_citation'})) {
          $has_warnings++;
          $msg = "warning: citation already exists: ($fields->{'publink_citation'})";
          reportError($line_count, $msg);
        }
        else {
          $citations{$fields->{'publink_citation'}} = 1;
        }
        
        # make sure PMID is a number
        if ($fields->{'pmid'} && lc($fields->{'pmid'}) ne 'null') {
          if ($fields->{'pmid'} == 0) {  # will be true if pmid is not a number
            $has_errors++;
            $msg = "ERROR: value given for PMID ($fields->{'pmid'}) is not a number.";
            reportError($line_count, $msg);
          }
        }
        
        # verify DOI
        if ($fields->{'doi'} && lc($fields->{'doi'}) ne 'null') {
          # All DOI numbers begin with a 10 and contain a prefix and a suffix 
          #   separated by a slash. The prefix is a unique number of four or more 
          #   digits assigned to organizations; the suffix is assigned by the 
          #   publisher.
          if (!($fields->{'doi'} =~ /10\..*?\/.*/)) {
            $has_errors++;
            $msg = "ERROR: value given for DOI ($fields->{'doi'}) doesn't ";
            $msg .= "look like a DOI identifier (10.<prefix>/<suffix>).\n";
            reportError($line_count, $msg);
          }
        }
      }
    }#each pub record
    
    if ($has_errors) {
      $msg = "The publication table has $has_errors errors. Unable to continue.\n\n";
      reportError('', $msg);
      exit;
    }

    # pub_authors.txt:
    # 1. citation must match one in pubs.txt
    # 2. cite order must be given
    # 3. author name must be given
  
    $wsfile = "$input_dir/PUB_AUTHORS.txt";
    print "\nReading publication author records from $wsfile\n";
    @records = readFile($wsfile);
    
    $has_errors = 0;
    $line_count = 0;
    foreach my $fields (@records) {
      $line_count++;
      if (!($fields->{'cite_order'} =~/\d+/)) {
        $has_errors++;
        $msg = "ERROR: missing citation order";
        reportError($line_count, $msg);
      }
      if ($fields->{'author'} eq '' || $fields->{'author'} eq 'NULL') {
        $has_errors++;
        $msg = "ERROR: no author name given";
        reportError($line_count, $msg);
      }
      if (!$fields->{'publink_citation'} || $fields->{'publink_citation'} eq ''
            || $fields->{'publink_citation'} eq 'NULL') {
        $has_errors++;
        $msg = "ERROR: citation is missing";
        reportError($line_count, $msg);
      }
      if (!$citations{$fields->{'publink_citation'}}
            && !publicationExists($dbh, $fields->{'publink_citation'})) {
        $has_errors++;
        $msg = "ERROR: citation ($fields->{'publink_citation'}) doesn't match any ";
        $msg .= "citations is spreadsheet or database";
        reportError($line_count, $msg);
      }
    }#each record
    
    if ($has_errors) {
      print "\n\nThe publication author table has $has_errors errors.\n\n";
    }
  
  
    # pub_url.txt:
    # 1. citation must match one in pubs.txt
    # 2. URL must be given
  
    $wsfile = "$input_dir/PUB_URLS.txt";
    print "\nReading publication URL records from $wsfile\n";
    @records = readFile($wsfile);
    
    $has_errors = 0;
    $line_count = 0;
    foreach my $fields (@records) {
      $line_count++;
      if ($fields->{'url'} eq '' || $fields->{'url'} eq 'NULL') {
#not an error
#        $has_errors++;
#        $msg = "missing URL";
#        reportError($line_count, $msg);
      }
      if (!$fields->{'publink_citation'} || $fields->{'publink_citation'} eq ''
            || $fields->{'publink_citation'} eq 'NULL') {
        $has_errors++;
        $msg = "ERROR: citation is missing";
        reportError($line_count, $msg);
      }
      if (!$citations{$fields->{'publink_citation'}} 
            && !publicationExists($dbh, $fields->{'publink_citation'})) {
        $has_errors++;
        $msg = "ERROR: citation ($fields->{'publink_citation'}) doesn't match any ";
        $msg .= "citations in spreadsheet or database.";
        reportError($line_count, $msg);
      }
    }#each record
    
    if ($has_errors) {
      print "\n\nThe publication URL table has $has_errors errors.\n\n";
    }
  
  
    # pub_keyword.txt:
    # 1. citation must match one in pubs.txt
    # 2. keywords must be given
  
    $wsfile = "$input_dir/PUB_KEYWORDS.txt";
    print "\nReading publication keyword records from $wsfile\n";
    @records = readFile($wsfile);
    
    $has_errors = 0;
    $line_count = 0;
    foreach my $fields (@records) {
      $line_count++;
      if ($fields->{'keyword'} eq '' || $fields->{'keyword'} eq 'NULL') {
        $has_errors++;
        $msg = "ERROR: missing keywords";
        reportError($line_count, $msg);
      }
      if (!$fields->{'publink_citation'} || $fields->{'publink_citation'} eq ''
            || $fields->{'publink_citation'} eq 'NULL') {
        $has_errors++;
        $msg = "ERROR: citation is missing";
        reportError($line_count, $msg);
      }
      if (!$citations{$fields->{'publink_citation'}}
            && !publicationExists($dbh, $fields->{'publink_citation'})) {
        $has_errors++;
        $msg = "ERROR: citation ($fields->{'publink_citation'}) doesn't match any ";
        $msg .= "citations in spreadsheet or database";
        reportError($line_count, $msg);
      }
    }#each record
    
    if ($has_errors) {
      print "\n\nThe publication keyword table has $has_errors errors.\n\n";
    }

  }#do publication tables
  
  
################################################################################
####                      QTL EXPERIMENT WORKSHEET                         #####
################################################################################

  my %experiments;
  if ($do_experiments) {
    
    # Make sure we've got the experiment table file
    if (!$files{'QTL_EXPERIMENT.txt'}) {
      $msg = "\nthe experiment table is missing.\n";
      $msg .= "QTL_EXPERIMENT.txt is required.\n\n";
      reportError('', $msg);
      exit;
    }
  
    # qtl_experiment.txt:
    # 1. citation must exist
    # 2. experiment name must be unique (in db and spreadsheet)
    # 3. species name must exist
    
    $wsfile = "$input_dir/QTL_EXPERIMENT.txt";
    print "\nReading Experiment records from $wsfile\n";
    @records = readFile($wsfile);
    
    $has_errors = 0;
    $line_count = 0;
    foreach my $fields (@records) {
      $line_count++;
      if (!$fields->{'publink_citation'} || $fields->{'publink_citation'} eq ''
            || $fields->{'publink_citation'} eq 'NULL') {
        $has_errors++;
        $msg = "ERROR: citation is missing";
        reportError($line_count, $msg);
      }
      if ($citations{$fields->{'publink_citation'}}
            && !publicationExists($dbh, $fields->{'publink_citation'})) {
        $has_errors++;
        $msg = "ERROR: citation ($fields->{'publink_citation'}) doesn't match ";
        $msg .= "any citations is spreadsheet or database.";
        reportError($line_count, $msg);
      }
      
      if ($experiments{$fields->{'name'}}) {
        $has_errors++;
        $msg = "ERROR: experiment name ($fields->{'name'}) is not unique ";
        $msg .= "within spreadsheet";
        reportError($line_count, $msg);
      }
      elsif (experimentExists($dbh, $fields->{'name'})) {
        $has_warnings++;
        $msg = "warning: experiment name ($fields->{'name'}) already used in ";
        $msg .= "database";
        reportError($line_count, $msg);
      }
      $experiments{$fields->{'name'}} = 1;
      
      if (!getOrganismID($dbh, $fields->{'specieslink_abv'})) {
        $has_errors++;
        $msg = "ERROR: species name ($fields->{'specieslink_abv'}) doesn't exist";
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
####                            MAP WORKSHEETS                             #####
################################################################################

  my %mapsets;
  my %linkagemaps;
  if ($do_genetic_maps) {
    
    # Make sure we've got all the map table files
    if (!$files{'MAP_COLLECTIONS.txt'}) {
      $msg = "\nOne or more required map tables is missing.\n";
      $msg .= "MAP_COLLECTION.txt is required and CONSENSUS_MAP.txt ";
      $msg .= "and/or EXPERIMENTAL_MAP are optional.\n\n";
      reportError('', $msg);
      exit;
    }
  
    # map_collection.txt:
    # 1. citation must exist
    # 2. species name must exist
    # 3. map unit must be set and exist
    # 4. map name must not be duplicated in this spreadsheet
    
    $wsfile = "$input_dir/MAP_COLLECTIONS.txt";
    print "\nReading map collection records from $wsfile\n";
    @records = readFile($wsfile);
    
    $has_errors = 0;
    $line_count = 0;
    foreach my $fields (@records) {
      $line_count++;
      
      # check citation
      if (!$fields->{'publink_citation'} || $fields->{'publink_citation'} eq ''
            || $fields->{'publink_citation'} eq 'NULL') {
        $has_errors++;
        $msg = "ERROR: citation is missing";
        reportError($line_count, $msg);
      }
      if (!$citations{$fields->{'publink_citation'}}
            && !publicationExists($dbh, $fields->{'publink_citation'})) {
        $has_errors++;
        $msg = "ERROR: citation ($fields->{'publink_citation'}) doesn't match any ";
        $msg .= " citations in spreadsheet or database.";
        reportError($line_count, $msg);
      }
      
      # check species
      if (!getOrganismID($dbh, $fields->{'specieslink_abv'})) {
        $has_errors++;
        $msg = "ERROR: species name ($fields->{'specieslink_abv'}) doesn't exist";
        reportError($line_count, $msg);
      }
      
      # check map unit
      my $unit = $fields->{'unit'};
      if (!unitExists($dbh, $unit)) {
        $has_errors++;
        $msg = "ERROR: map unit [$unit] is not set or doesn't exist in the ";
        $msg .= "featuremap_unit controlled vocabulary.";
        reportError($line_count, $msg);
      }
      
      # check map name
      my $mapname = $fields->{'map_name'};
      
      if ($mapsets{$mapname}) {
        $has_errors++;
        $msg = "ERROR: map collection name ($mapname) ";
        $msg .= "already exists in spreadsheet";
        reportError($line_count, $msg);
      }
      elsif (mapSetExists($dbh, $mapname)) {
        $has_warnings++;
        $msg = "warning: this map collection name ($mapname)";
        $msg .= "is already in the database and will be updated.";
        reportError($line_count, $msg);
      }
      
      $mapsets{$mapname} = 1;
    }#each record
  
    if ($has_errors) {
      print "\n\nThe map collection table has $has_errors errors.\n\n";
    }
  
    if ($files{'CONSENSUS_MAP.txt'}) {
      # consensus_map.txt:
      # 1. map name must be unique in db and spreadsheet
      # 2. must be a map set record
      # 3. start and end coordinates must be specified
      # 4. species name must exist
      
      $wsfile = "$input_dir/CONSENSUS_MAP.txt";
      print "\nReading consensus map records from $wsfile\n";
      @records = readFile($wsfile);
      
      $has_errors = 0;
      $line_count = 0;
      foreach my $fields (@records) {
        $line_count++;

        my $mapname = makeLinkageMapName($fields);
        next if (linkageMapExists($mapname));
        
        # check for unique name
        if ($linkagemaps{$mapname}) {
          $has_errors++;
          $msg = "ERROR: linkage map name ($mapname) is not unique within spreadsheet";
          reportError($line_count, $msg);
        }
        elsif (consensusMapExists($mapname)) {
          $has_warnings++;
          $msg = "warning: linkage map name ($mapname) already exists in database";
          reportError($line_count, $msg);
        }
        
        # make sure there is an associated map collection record
        if (!$mapsets{$fields->{'map_name'}}
              && !mapSetExists($dbh, $fields->{'map_name'})) {
          $has_errors++;
          $msg = "ERROR: Map set record (map_collection) for ";
          $msg .= "$fields->{'map_name'} does not ";
          $msg .= "exist in spreadsheet or database.";
          reportError($line_count, $msg);
        }
        
        # map_start and map_end must be set
        if ($fields->{'map_start'} ne '0' || $fields->{'map_start'} eq '' 
              || $fields->{'map_start'} eq 'NULL') {
          reportError($line_count, "map_start is missing");
        }
        if (!$fields->{'map_end'} || $fields->{'map_end'} eq '' 
              || $fields->{'map_end'} eq 'NULL') {
          reportError($line_count, "map_end is missing");
        }
        
        if (!getOrganismID($dbh, $fields->{'specieslink_abv'})) {
          $has_errors++;
          $msg = "ERROR: species name ($fields->{'specieslink_abv'}) doesn't exist";
          reportError($line_count, $msg);
        }
        
        $linkagemaps{$mapname} = 1;
      }#each record
  
      if ($has_errors) {
        print "\n\nThe consensus map table has $has_errors errors. Unable to continue.\n\n";
        exit;
      }
    }#consensus map file exists
    
    if ($files{'EXPERIMENT_MAP.txt'}) {
      # consensus_map.txt:
      # 1. map name must be unique in db and spreadsheet
      # 2. must be a map set record
      # 3. start and end coordinates must be specified
      # 4. species name must exist
      
      $wsfile = "$input_dir/EXPERIMENT_MAP.txt";
      print "\nReading experiment map records from $wsfile\n";
      @records = readFile($wsfile);
      
      $has_errors = 0;
      $line_count = 0;
      foreach my $fields (@records) {
        $line_count++;
    
        my $mapname = makeLinkageMapName($fields);
        next if (linkageMapExists($mapname));
        
        # check for unique name
        if ($linkagemaps{$mapname}) {
          $has_errors++;
          $msg = "ERROR: linkage map name ($mapname) is not unique within spreadsheet";
          reportError($line_count, $msg);
        }
        elsif (consensusMapExists($mapname)) {
          $has_warnings++;
          $msg = "warning: linkage map name ($mapname) already exists in database";
          reportError($line_count, $msg);
        }
        
        # make sure there is an associated map collection record
        if (!$mapsets{$fields->{'map_name'}}
              && !mapSetExists($dbh, $fields->{'map_name'})) {
          $has_errors++;
          $msg = "ERROR: Map set record (map_collection) for ";
          $msg .= "$fields->{'map_name'} does not ";
          $msg .= "exist in spreadsheet or database.";
          reportError($line_count, $msg);
        }
        
        # map_start and map_end must be set
        if ($fields->{'map_start'} ne '0' || $fields->{'map_start'} eq '' 
              || $fields->{'map_start'} eq 'NULL') {
          reportError($line_count, "map_start is missing");
        }
        if (!$fields->{'map_end'} || $fields->{'map_end'} eq '' 
              || $fields->{'map_end'} eq 'NULL') {
          reportError($line_count, "map_end is missing");
        }
        
        if (!getOrganismID($dbh, $fields->{'specieslink_abv'})) {
          $has_errors++;
          $msg = "ERROR: species name ($fields->{'specieslink_abv'}) doesn't exist";
          reportError($line_count, $msg);
        }
        
        $linkagemaps{$mapname} = 1;
      }#each record
  
      if ($has_errors) {
        print "\n\nThe experiment map table has $has_errors errors. Unable to continue.\n\n";
        exit;
      }
    }#experiment map file exists
  }#do genetic maps


################################################################################
####                           MARKER WORKSHEETS                           #####
################################################################################

  my %markers;
  if ($do_markers) {
    
    # Make sure we've got all the marker table files
    if (!$files{'MARKERS.txt'}) {
      $msg = "\nOne or more map tables is missing.\n";
      $msg .= "MARKERS.txt is required.\n\n";
      reportError('', $msg);
      exit;
    }
  
    # marker.txt:
    # 1. marker name must be unique in spreadsheet and data table
    # 2. chromosome must match an existing chromosome
    # 3. sequence_source must be in db table
    # 4. publication, if indicated, must exist in db or spreadsheet
    
    $wsfile = "$input_dir/MARKERS.txt";
    print "\nReading marker records from $wsfile\n";
    @records = readFile($wsfile);
    
    $has_errors = 0;
    $line_count = 0;
    foreach my $fields (@records) {
      $line_count++;
      
      if ($markers{$fields->{'marker_name'}}) {
        $has_errors++;
        $msg = "ERROR: This marker ($fields->{'marker_name'}) already exists ";
        $msg .= "in the spreadsheet.";
        reportError($line_count, $msg);
      }
      my $uniq_marker_name = makeMarkerName('marker_name', $fields);
      if (markerExists($dbh, $uniq_marker_name)) {
        $has_warnings++;
        $msg = "warning: This unique marker name ($uniq_marker_name) already exists ";
        $msg .= "in the database.";
        reportError($line_count, $msg);
      }
      
      if (!getChromosomeID($dbh, $fields->{'phys_chr'}, $fields->{'assembly_ver'})
            && !scaffoldExists($fields->{'phys_chr'})) {
        $has_errors++;
        $msg = "ERROR: Chromosome doesn't exist ($fields->{'phys_chr'}, ";
        $msg .= "v $fields->{'assembly_ver'})";
        reportError($line_count, $msg);
      }
      
      if ($fields->{'sequence_source'} && $fields->{'sequence_source'} ne ''
            && $fields->{'sequence_source'} ne 'NULL'
            && !dbExists($fields->{'sequence_source'})) {
        $has_errors++;
        $msg = "ERROR: Sequence source ($fields->{'sequence_source'}) ";
        $msg .= "is not in the database.";
        reportError($line_count, $msg);
      }
      
      if ($fields->{'publink_citation'} && $fields->{'publink_citation'} ne ''
            && $fields->{'publink_citation'} ne 'NULL'
            && $fields->{'publink_citation'} ne 'N/A') {
        if (!$citations{$fields->{'publink_citation'}} 
              && !publicationExists($dbh, $fields->{'publink_citation'})) {
#print "citation ($fields->{'publink_citation'}) not in spreadsheet:\n" . Dumper(%citations);
          $has_errors++;
          $msg = "ERROR: Publication is not in spreadsheet or database ";
          $msg .= "($fields->{'publink_citation'})";
          reportError($line_count, $msg);
        }
      }#publication indicated
      
      $markers{$fields->{'marker_name'}} = 1;
    }#each record

    if ($has_errors) {
      print "\n\nThe marker table has $has_errors errors. Unable to continue.\n\n";
      exit;
    }
  }#do markers

  
################################################################################
####                           TRAIT WORKSHEETS                            #####
################################################################################

  my %traits;
  if ($do_traits) {
    
    # Make sure we've got all the map table files
    if (!$files{'OBS_TRAITS.txt'} || !$files{'PARENT_TRAITS.txt'}) {
      $msg = "\nOne or more trait tables is missing.\n";
      $msg .= "OBS_TRAITS.txt and PARENT_TRAITS.txt are required.\n\n";
      reportError('', $msg);
      exit;
    }
  
    # obs_trait.txt:
    # 1. ontology term must be valid if provided
    
    $wsfile = "$input_dir/OBS_TRAITS.txt";
    print "\nReading observed trait records from $wsfile\n";
    @records = readFile($wsfile);
    
    $has_errors = 0;
    $line_count = 0;
    foreach my $fields (@records) {
      $line_count++;

#eksc this is not an error (same trait may have multiple synonyms in worksheet)
#      if ($traits{$fields->{'qtl_symbol'}}) {
#        $has_errors++;
#        $msg = "ERROR: trait name ($fields->{'qtl_symbol'}) ";
#        $msg .= "already exists in spreadsheet.";
#        reportError($line_count, $msg);
#      }
#      elsif (traitExists($dbh, $fields->{'qtl_symbol'})) {
      if (traitExists($dbh, $fields->{'qtl_symbol'})) {
        $has_warnings++;
        $msg = "warning: trait name ($fields->{'qtl_symbol'}) ";
        $msg .= "already exists in database.";
        reportError($line_count, $msg);
      }

# not tracking experiments for traits       
#      if (!$experiments{$fields->{'qtl_experimentlink_name'}}
#              && !experimentExists($dbh, $fields->{'qtl_experimentlink_name'})) {
#        $has_errors++;
#        $msg = "ERROR: experiment name '$fields->{'qtl_experimentlink_name'}' ";
#        $msg .= "does not exist in spreadsheet or database.";
#        reportError($line_count, $msg);
#      }

      if ($fields->{'ontology_number'} =~ /^(.*?):(.*)/) {
        my $db = $1;
        my $accession = $2;
        if (!dbxrefExists($dbh, $db, $accession)) {
          # non-fatal error (must be fixed, but don't hold up remaining process)
          $has_warnings++;
          $msg = "warning: Invalid or deleted OBO term: $fields->{'ontology_numer'}";
          reportError($msg);
        }
      }
      $traits{$fields->{'qtl_symbol'}} = 1;
    }#each record
  
    if ($has_errors) {
      print "\n\nThe observed trait table has $has_errors errors.\n\n";
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
      if ($fields->{'publink_citation'} eq '' 
            || $fields->{'publink_citation'} eq 'NULL') {
        $has_warnings++;
        $msg = "warning: citation is missing";
        reportError($line_count, $msg);
      }
      elsif (!$citations{$fields->{'publink_citation'}}
                && !publicationExists($dbh, $fields->{'publink_citation'})) {
        $has_errors++;
        $msg = "ERROR: citation ($fields->{'publink_citation'}) doesn't match any ";
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
    
    # Make sure we've got all the qtl table file
    if (!$files{'QTL.txt'}) {
      $msg = "\nthe qtl table is missing.\n";
      $msg .= "QTL.txt is required.\n\n";
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
    
    $wsfile = "$input_dir/QTL.txt";
    print "\nReading qtl records from $wsfile\n";
    @records = readFile($wsfile);
    
    $has_errors = 0;
    $line_count = 0;
    foreach my $fields (@records) {
      $line_count++;
#print Dumper($fields);
#exit;
      my $qtl_name    = "$fields->{'qtl_symbol'}+$fields->{'qtl_identifier'}";
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
      
      if (!$experiments{$fields->{'qtl_experimentlink_name'}}
              && !experimentExists($dbh, $fields->{'qtl_experimentlink_name'})) {
        $has_errors++;
        $msg = "ERROR: experiment name ";
        $msg .= "'$fields->{'qtl_experimentlink_name'}' does not exist ";
        $msg .= "in spreadsheet or database.";
        reportError($line_count, $msg);
      }
      
      if (!$traits{$fields->{'qtl_symbol'}}) {
        if (!getTrait($dbh, $fields->{'qtl_symbol'})) {
          $has_errors++;
          $msg = "ERROR: QTL symbol ($fields->{'qtl_symbol'}) is not defined in ";
          $msg .= "the spreadsheet or database.";
          reportError($line_count, $msg);
        }
      }
      
      if (my $mapname = makeLinkageMapName($fields)) {
        if (!$linkagemaps{$mapname}) {
          if (!linkageMapExists($mapname)) {
            $has_warnings++;
            $msg = "Warning: linkage map '$mapname' is not defined in the ";
            $msg .= "spreadsheet or database.";
            reportError($line_count, $msg);
          }
        }
      }
      
      my $uniq_marker_name;
      
      if ($fields->{'nearest_marker'} ne '' 
            && $fields->{'nearest_marker'} ne 'NULL') {
        $uniq_marker_name = makeMarkerName('nearest_marker', $fields);
        if (!$markers{$fields->{'nearest_marker'}} 
                && !markerExists($dbh, $uniq_marker_name)) {
          $has_warnings++;
          $msg = "warning: nearest marker ($fields->{'nearest_marker'}) ";
          $msg .= "does not exist in spreadsheet or database. ";
          $msg .= "A stub record will be created.";
          reportError($line_count, $msg);
        }
      }
      
      if ($fields->{'flanking_marker_low'} ne '' 
            && $fields->{'flanking_marker_low'} ne 'NULL') {
        $uniq_marker_name = makeMarkerName('flanking_marker_low', $fields);
        if (!$markers{$fields->{'flanking_marker_low'}}
              && !markerExists($dbh, $uniq_marker_name)) {
          $has_warnings++;
          $msg = "warning: flanking marker low ($fields->{'flanking_marker_low'}) ";
          $msg .= "does not exist in spreadsheet or database. ";
          $msg .= "A stub record will be created.";
          reportError($line_count, $msg);
        }
      }
      
      if ($fields->{'flanking_marker_high'} ne '' 
            && $fields->{'flanking_marker_high'} ne 'NULL') {
        $uniq_marker_name = makeMarkerName('flanking_marker_high', $fields);
        if (!$markers{$fields->{'flanking_marker_high'}}
              && !markerExists($dbh, $uniq_marker_name)) {
          $has_warnings++;
          $msg = "warning: flanking marker high ($fields->{'flanking_marker_high'}) ";
          $msg .= "does not exist in spreadsheet or database. ";
          $msg .= "A stub record will be created.";
          reportError($line_count, $msg);
        }
      }
      
      if (!getOrganismID($dbh, $fields->{'specieslink_abv'})) {
        $has_errors++;
        $msg = "ERROR: species name ($fields->{'specieslink_abv'}) doesn't exist";
        reportError($line_count, $msg);
      }
      
      $qtls{$qtl_name} = 1;
    }#each record
  
    if ($has_errors) {
      print "\n\nThe qtl table has $has_errors errors and $has_warnings warnings.\n\n";
    }
  }
  
  print "\n\n\nSpreadsheet verification is completed.\n";
  print "There were $has_warnings warnings that should be checked.\n\n\n";



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
  my ($mapname) = @_;
  if ($linkagemaps{$mapname}) {
    return 1;
  }
  else {
    my ($sql, $sth, $row);
    $sql = "SELECT * FROM chado.feature WHERE name='$mapname'";
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



