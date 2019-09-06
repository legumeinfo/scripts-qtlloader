package PubVerif;

use strict;
use warnings;
use Encode;
use BerkeleyDB;
use HashDB;
use Data::Dumper;
use CropLegumeBaseLoaderUtils;
   
use Exporter qw(import);
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(pubVerif);
#our @EXPORT = qw($hashref);
#use vars qw(@ISA @EXPORT);

my %citations;
# Get spreadsheet constants
my %pi = getSSInfo('PUBS');


sub pubVerif {  
  
  my ($input_dir,$dbh,$berkeley_dbh) = @_;
  my @filepaths = <$input_dir/*.txt>;
  my %files     = map { s/$input_dir\/(.*)$/$1/; $_ => 1} @filepaths;
  
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
          $berkeley_dbh->db_put($enc_citation, 1); 
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
#    while( my( $key, $value ) = each %citations ){
#    print "PubVerifKey:$key -> PubverifValue: $value\n";
#}
}#pubVerif


#sub export_hash {
#my $hashref = \%citations;
#  if (!keys %citations) {
#    print "Inside export_hash:Empty";
#    }
#  while( my( $key, $value ) = each %citations ){
#    print "Key:$key -> Value: $value\n";
#}
#  return %citations;
#}# export_hash
  

1;