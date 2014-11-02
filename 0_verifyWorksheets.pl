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
        if ($citations{publink_citation}) {
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
          $citations{$publink_citation} = 1;
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
#print "\ncitation: $publink_citation\n";
 
      if (!$publink_citation || $publink_citation eq ''
            || $publink_citation eq 'NULL') {
        $has_errors++;
        $msg = "ERROR: citation is missing";
        reportError($line_count, $msg);
      }
      if ($citations{$publink_citation}
            && !publicationExists($dbh, $publink_citation)) {
        $has_errors++;
        $msg = "ERROR: citation ($publink_citation) doesn't match ";
        $msg .= "any citations in spreadsheet or database.";
        reportError($line_count, $msg);
      }
      
      my $name = $fields->{$qei{'name_fld'}};
#print "experiment name: $name\n";
      if ($experiments{$name}) {
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
      $experiments{$name} = 1;
      
      my $species = $fields->{$qei{'species_fld'}};
#print "species: $species\n";
      if (!getOrganismID($dbh, $species)) {
        $has_errors++;
        $msg = "ERROR: species name ($species) doesn't exist";
        reportError($line_count, $msg);
      }

      my $map_name = $fields->{$qei{'map_fld'}};
#print "map name: $map_name\n";
      if ($map_name eq '') {
        $has_errors++;
        $msg = "ERROR: map collection name not specified.";
        reportError($line_count, $msg);
      }

      my $geoloc = $fields->{$qei{'geoloc_field'}};
#print "geolocation: $geoloc\n";
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
####                            MAP WORKSHEETS                             #####
################################################################################

  my %mapsets;
  my %linkagemaps;
  if ($do_genetic_maps) {
    
    # Get spreadsheet constants
    my %mci = getSSInfo('MAP_COLLECTIONS');
    my %mi  = getSSInfo('MAPS');
    
    # Make sure we've got all the map table files
    my $mcfile = $mci{'worksheet'} . '.txt';
    my $mfile  = $mi{'worksheet'} . '.txt';
    if (!$files{$mcfile} || !$files{$mfile}) {
      $msg = "\nOne or more required map tables is missing.\n";
      $msg .= "$mcfile and $mfile are required .\n\n";
      reportError('', $msg);
      exit;
    }
  
    # map_collection.txt:
    # 1. citations must exist
    # 2. species name must exist
    # 3. map unit must be set and exist
    # 4. map name must not be duplicated in this spreadsheet
    
    $wsfile = "$input_dir/$mcfile";
    print "\nReading map collection records from $wsfile\n";
    @records = readFile($wsfile);
    
    $has_errors = 0;
    $line_count = 0;
    foreach my $fields (@records) {
      $line_count++;
      
      # check citation
      my @publink_citations = split ';', $fields->{$mci{'pub_fld'}};
print "Publication(s):\n" . Dumper(@publink_citations);
      foreach my $publink_citation (@publink_citations) {
        $publink_citation =~ s/^\s//;
        $publink_citation =~ s/\s+$//;
        if (!$publink_citation || $publink_citation eq ''
              || $publink_citation eq 'NULL') {
          $has_errors++;
          $msg = "ERROR: citation is missing";
          reportError($line_count, $msg);
        }
        if (!$citations{$publink_citation}
              && !publicationExists($dbh, $publink_citation)) {
          $has_errors++;
          $msg = "ERROR: citation ($publink_citation) doesn't match any ";
          $msg .= " citations in spreadsheet or database.";
          reportError($line_count, $msg);
        }
      }
      
      # check species
      my $species = $fields->{$mci{'species_fld'}};
print "species: $species\n";
      if (!getOrganismID($dbh, $species)) {
        $has_errors++;
        $msg = "ERROR: species name ($species) doesn't exist";
        reportError($line_count, $msg);
      }
      
      # check map unit
      my $unit = $fields->{$mci{'unit_fld'}};
print "unit: $unit\n";
      if (!unitExists($dbh, $unit)) {
        $has_errors++;
        $msg = "ERROR: map unit [$unit] is not set or doesn't exist in the ";
        $msg .= "featuremap_unit controlled vocabulary.";
        reportError($line_count, $msg);
      }
      
      # check map name
      my $mapname = $fields->{$mci{'map_name_fld'}};
print "map name: $mapname\n";
      if ($mapsets{$mapname}) {
        $has_errors++;
        $msg = "ERROR: map collection name ($mapname) already exists in spreadsheet";
        reportError($line_count, $msg);
      }
      elsif (mapSetExists($dbh, $mapname)) {
        $has_warnings++;
        $msg = "WARNING: this map collection name ($mapname)";
        $msg .= "is already in the database and will be updated.";
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
    print "\nReading map records from $wsfile\n";
    @records = readFile($wsfile);
      
    $has_errors = 0;
    $line_count = 0;
    foreach my $fields (@records) {
      $line_count++;

      my $ms_name = $fields->{$mi{'map_name_fld'}};
      my $lg      = $fields->{$mi{'lg_fld'}};
      my $mapname = makeLinkageMapName($ms_name, $lg);
print "\nlinkage map name: $mapname ($ms_name, $lg)\n";
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
        
      # map_start and map_end must be set
      my $map_start = $fields->{$mi{'map_start_fld'}};
      my $map_end   = $fields->{$mi{'map_end_fld'}};
      if ($map_start eq '' || lc($map_start) eq 'null') {
        $has_errors++;
        reportError($line_count, "ERROR: map_start is missing");
      }
      if (!$map_end || $map_end eq '' || lc($map_end) eq 'null') {
        $has_errors++;
        reportError($line_count, "ERROR: map_end is missing");
      }
      if ($map_end < $map_start) {
        $has_errors++;
        reportError($line_count, "ERROR: map end is < map start");
      }
      
      # species must exist
      my $species = $fields->{$mi{'species_fld'}};
print "species: $species\n";
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
    
  }#do genetic maps


################################################################################
####                           MARKER WORKSHEETS                           #####
################################################################################

  my %markers;
  if ($do_markers) {
    
    # Make sure we've got all the marker table files
    if (!$files{'MARKERS.txt'}) {
      $msg = "\nThe markers tables is missing.\n";
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
      my $uniq_marker_name = makeMarkerName($fields->{'specieslink_abv'}, 
                                            $fields->{'marker_name'});
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
#print Dumper($fields);
      $line_count++;

      my $qtl_name = makeQTLname($fields->{$qi{'qtl_symbol_fld'}}, 
                                 $fields->{$qi{'qtl_identifier_fld'}});
print "\nQTL name: $qtl_name\n";
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
print "experiment: $expt\n";
      if (!$experiments{$expt} && !experimentExists($dbh, $expt)) {
        $has_errors++;
        $msg = "ERROR: experiment name '$expt' does not exist ";
        $msg .= "in spreadsheet or database.";
        reportError($line_count, $msg);
      }
      
      my $qtl_symbol = $fields->{$qi{'qtl_symbol_fld'}};
print "QTL symbol: $qtl_symbol\n";
      if (!$traits{$qtl_symbol}) {
        if (!getTrait($dbh, $qtl_symbol)) {
          $has_errors++;
          $msg = "ERROR: QTL symbol ($qtl_symbol) is not defined in ";
          $msg .= "the spreadsheet or database.";
          reportError($line_count, $msg);
        }
      }
      
      my $uniq_marker_name;
      
      my $nearest_marker = $fields->{$qi{'nearest_mrkr_fld'}};
print "Nearest marker: $nearest_marker\n";
      if ($nearest_marker ne '' && lc($nearest_marker) ne 'null') {
        $uniq_marker_name = makeMarkerName($fields->{'specieslink_abv'}, 
                                           $fields->{'nearest_marker'});
        if (!$markers{$nearest_marker} 
                && !markerExists($dbh, $uniq_marker_name)) {
          $has_warnings++;
          $msg = "warning: nearest marker ($nearest_marker) ";
          $msg .= "does not exist in spreadsheet or database. ";
          $msg .= "A stub record will be created.";
          reportError($line_count, $msg);
        }
      }
      
      my $flanking_marker_low = $fields->{$qi{'flank_mrkr_low_fld'}};
print "Flanking marker low: $flanking_marker_low\n";
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
print "Flanking marker high: $flanking_marker_high\n";
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
print "species: $species\n";
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
      print "\n\nThe QTL table has $has_errors errors and $has_warnings warnings.\n\n";
    }
    
    # MAP_POSITION.txt:
    # 1. QTL name must match an existing one in the spreadsheet
    # 2. map set (map_collection) must exist in db or spreadsheet
    # 3. left_end should be < right_end
    
    $wsfile = "$input_dir/$mpfile";
    print "\nReading map position records from $wsfile\n";
    @records = readFile($wsfile);
    
    $has_errors = 0;
    $line_count = 0;
    foreach my $fields (@records) {
      $line_count++;

      my $qtl_name = makeQTLname($fields->{$mpi{'qtl_symbol_fld'}},
                                 $fields->{$mpi{'qtl_identifier_fld'}});
print "\nQTL name: $qtl_name\n";
      
      my $ms_name = $fields->{$mpi{'map_name_fld'}};
      my $lg      = $fields->{$mpi{'lg_fld'}};
      my $mapname = makeLinkageMapName($ms_name, $lg);
print "linkage map name: $mapname\n";
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
print "QTL coordinates: $left_end - $right_end\n";
      if ($left_end eq '' || lc($left_end) eq 'null'
            || $right_end eq '' || lc($right_end) eq 'null') {
        $has_errors++;
        $msg = "ERROR: missing left and/or right end coordinates for QTL.";
        reportError($line_count, $msg);
      }
      elsif ($left_end > $right_end) {
        $has_errors++;
        $msg = "ERROR: right coordinate is larger than left coordinate for QTL.";
        reportError($line_count, $msg);
      }
      
      $line_count++;
    }#each record
  }#check QTL tables
  
  if ($has_errors) {
    print "\n\nThe Map Position table has $has_errors errors and $has_warnings warnings.\n\n";
  }

  print "\n\n\nSpreadsheet verification is completed.\n";
  if ($qtl_errors) {
    print "There were $qtl_errors shown above in the QTL worksheet.\n";
  }
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


sub makeQTLname {
  my ($symbol, $id) = @_;
  my $qtl_name = "$symbol+$id";
  return $qtl_name;
}#makeQTLname


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



