package TraitVerif;
use CropLegumeBaseLoaderUtils;

my %traits;
# Get spreadsheet constants
my %ti = getSSInfo('TRAITS');

sub traitVerif {
    
    my ($input_dir,$dbh,$berkeley_dbh) = @_;
    my @filepaths = <$input_dir/*.txt>;
    my %files     = map { s/$input_dir\/(.*)$/$1/; $_ => 1} @filepaths;
    
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
      $berkeley_dbh->db_put($qtl_symbol, 1);
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

}#traitVerif

1;