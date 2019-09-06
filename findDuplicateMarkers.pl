# file: findDuplicateMarkers.pl
#
# purpose: attempt to identify markers with highly similar names. Curator will
#          determine which are actual duplicates.
#
# history:
#   09/09/15  eksc  created

  use strict;
  use DBI;
  use Data::Dumper;
  use Encode;

  # load local util library
  use File::Spec::Functions qw(rel2abs);
  use File::Basename;
  use lib dirname(rel2abs($0));
  use CropLegumeBaseLoaderUtils;
  
  my $markerfile = $ARGV[0];
  
  # Get connected
  my $dbh = connectToDB;

  my (%existing_markers, @marker_names, @sorted_names, %duplicate_names);
  
  print "Get existing markers...\n";
  %existing_markers = getExistingMarkers($dbh);
#print "Existing markers:\n" . Dumper(%existing_markers);
  @marker_names = (keys %existing_markers);
  
  if ($markerfile) {
    print "Will also include the markers in $markerfile\n";
    # Compare against names in this file
    open MARKER, "<$markerfile" or die "\nUnable to open $markerfile: $!\n\n";
    while (<MARKER>) {
      chomp;chomp;
      s/^\s+//;
      push @marker_names, $_;
    }
    close MARKER;
  }
  
  @sorted_names = sort {lc($a) cmp lc($b)} @marker_names;
#print Dumper(@sorted_names);

  my $test_name = '';
  foreach my $name (@sorted_names) {
    if (lc($name) ne lc($test_name) && !closeMatch($name, $test_name)) {
      $test_name = $name;
    }
    else {
      if ((!$existing_markers{$name} || !$existing_markers{$test_name})
            || ($existing_markers{$name} && $existing_markers{$test_name}
                && $existing_markers{$name} != $existing_markers{$test_name})
         ) {
        # don't record as duplicate if the same record
        if (!$duplicate_names{$test_name}) {
          $duplicate_names{$test_name} = ();
        }
        push @{$duplicate_names{$test_name}}, $name;
      }
    }
  }
#print Dumper(%existing_markers);
  
  print "\n\nPotential duplicate markers:\n";
  print "marker\tpub\tmap\t[potential_dup\tpotential_dup_pub\tpotential_dup_map]+\n";
  foreach my $name (keys %duplicate_names) {
    my @dups = @{$duplicate_names{$name}};
    
    my @marker_info;
    if ($existing_markers{$name}) {
      @marker_info = (
        $name, 
        $existing_markers{$name}->{'citation'}, 
        $existing_markers{$name}->{'map'},
      );
    }
    else {
      @marker_info = ($name.'[not-in-db]', 'n/a', 'n/a');
    }
    print (join "\t", @marker_info);
    print "\t";
      
    foreach my $dup (@dups) {
      if ($existing_markers{$dup}) {
        @marker_info = (
          $dup, 
          $existing_markers{$dup}->{'citation'}, 
          $existing_markers{$dup}->{'map'},
        );
      }
      else {
        @marker_info = ($dup.'[not-in-db]', 'n/a', 'n/a');
      }
      print (join "\t", @marker_info);
      print "\t";
    }#each duplicate
    print "\n";
  }#each marker with potential duplicates
  

##########################################################################################  

sub closeMatch {
  my ($name, $test_name) = @_;
  
  # look for 0-padded numbers
  my $name_unpad = lc($name);
  $name_unpad =~ s/(.*)0(\d)(.*)/$1$2$3/;
  my $test_unpad = lc($test_name);
  $test_unpad =~ s/(.*)0(\d)(.*)/$1$2$3/;
#print "$name -> $name_unpad, $test_name -> $test_unpad\n";
  if (lc($name) eq $test_unpad || $name_unpad eq lc($test_name)) {
    return 1;
  }
  
  return 0;
}#closeMatch


# If this script has a future, this function needs to be removed from 
#    3_load_markers.pl too and be moved to GP lib.
sub getExistingMarkers {
  my $dbh = $_[0];
  
  my %markers;
  my $sql = "
    SELECT f.feature_id, f.name AS marker, o.common_name, p.uniquename AS citation, m.name AS map 
    FROM feature f
      LEFT JOIN feature_pub fp ON fp.feature_id=f.feature_id
      LEFT JOIN pub p ON p.pub_id=fp.pub_id
      LEFT JOIN featurepos mp ON mp.feature_id=f.feature_id
      LEFT JOIN featuremap m ON m.featuremap_id=mp.featuremap_id
      LEFT JOIN organism o ON o.organism_id=f.organism_id
    WHERE f.type_id=(SELECT cvterm_id FROM cvterm 
                   WHERE name='genetic_marker' AND cv_id=(SELECT cv_id FROM cv 
                                                          WHERE name='sequence'))";
  logSQL('', $sql);
  my $sth = doQuery($dbh, $sql);
  while (my $row=$sth->fetchrow_hashref) {
    if ($markers{$row->{'marker'}}) {
      # already found this one
      push @{$duplicate_names{$test_name}}, $row->{'marker'};
    }
    else {
      $markers{$row->{'marker'}} = $row;
    }
  }
  
  return %markers;
}#getExistingMarkers

    
