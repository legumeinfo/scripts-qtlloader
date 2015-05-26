package QtlExpVerif;

use strict;
use warnings;
#use diagnostics;
use Encode;
#use PubVerif;
use CropLegumeBaseLoaderUtils;

my %experiments;

# get worksheet contants
my %qei = getSSInfo('QTL_EXPERIMENTS');

sub qtlExpVerif {
    my ($input_dir,$dbh,$berkeley_dbh) = @_;
    my @filepaths = <$input_dir/*.txt>;
    my %files     = map { s/$input_dir\/(.*)$/$1/; $_ => 1} @filepaths;

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
        my $value;
        my $enc_citation = encode("UTF-8", $publink_citation);
        my $status = $berkeley_dbh->db_get($enc_citation,$value);
        if ($status && !publicationExists($dbh, $publink_citation)) {
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
      $berkeley_dbh->db_put($enc_name, 1);
      
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

      my $geoloc = $fields->{$qei{'geoloc_fld'}};
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
}

1;

  