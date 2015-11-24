# file: setCanonicaMarkers.pl
#
# purpose: given a file of marker names or ids, set one to be canonical and link the 
#          others to it via a 'instance_of' relationship.
#          
#          file format:
#          canonical-name{tab}alt-name[{tab}alt_name]*
#          or
#          canonical-id{tab}alt-id[{tab}alt_id]*
#
# history:
#  09/09/15  eksc  created

  use strict;
  use DBI;
  use Getopt::Std;
  use Encode;
  use Getopt::Std;

  # load local util library
  use File::Spec::Functions qw(rel2abs);
  use File::Basename;
  use lib dirname(rel2abs($0));
  use CropLegumeBaseLoaderUtils;
  
  my $warn = <<EOS
    Usage:
      $0 [opts] data-dir
        -n = use names [default]
        -i = use ids
EOS
;
      
  my $use_names = 1;
  my %cmd_opts = ();
  getopts("ni", \%cmd_opts);
  if (defined($cmd_opts{'i'})) {$use_names = 0;}

  if ($#ARGV < 0) {
    die $warn;
  }

  # Get connected
  my $dbh = connectToDB;

  eval {
    if ($use_names) {
      setCanonicalMarkersByName($dbh);
    }
    else {
      setCanonicalMarkersByID($dbh);
    }
  
    $dbh->commit;   # commit the changes if we get this far
  };
  if ($@) {
    print "\n\nTransaction aborted because $@\n\n";
    # now rollback to undo the incomplete changes
    # but do it in an eval{} as it may also fail
    eval { $dbh->rollback };
  }

  # ALL DONE
  print "\n\n";


  
###############################################################################

sub setCanonicalMarkersByName {
  my $dbh = $_[0];
  
  my $markerlistfile = @ARGV[0];
  open IN, "<$markerlistfile" or die "\n\nunable to open $markerlistfile: $!\n\n";
  while (<IN>) {
    chomp;chomp;
    my @markers = split "\t";

    if ((my $canonical_marker_id=getMarkerID($dbh, $markers[0])) > 0) {
print "\n Canonical marker " . $markers[0] . " is $canonical_marker_id\n";
      setCanonical($dbh, $canonical_marker_id);
      for (my $i=1; $i<=$#markers; $i++) {
print $markers[$i] . " is not canonical\n";
        if ((my $marker_id = getMarkerID($dbh, $markers[$i])) > 0) {
          unsetCanonical($dbh, $marker_id);
          linkToCanonicalMarker($dbh, $canonical_marker_id, $marker_id);
        }
      }#each linked marker
    }
  }#each row in marker list
  close IN;
}#setCanonicalMarkersByName


sub setCanonicalMarkersByID {
  my $dbh = $_[0];
  
  my $markerlistfile = @ARGV[0];
  open IN, "<$markerlistfile" or die "\n\nunable to open $markerlistfile: $!\n\n";
  while (<IN>) {
    chomp;chomp;
    my @markers = split "\t";

    my $canonical_marker_id = $markers[0];
print "\n Canonical marker $canonical_marker_id\n";
    setCanonical($dbh, $canonical_marker_id);
    for (my $i=1; $i<=$#markers; $i++) {
print $markers[$i] . " is not canonical\n";
      unsetCanonical($dbh, $markers[$i]);
      linkToCanonicalMarker($dbh, $canonical_marker_id, $markers[$i]);
    }#each linked marker
  }#each row in marker list
  close IN;
}#setCanonicalMarkersByID


sub getMarkerID {
  my ($dbh, $uniquename) = @_;
  
  my $sql = "
    SELECT feature_id FROM feature 
    WHERE name='$uniquename'
          AND type_id=(SELECT cvterm_id FROM cvterm 
                       WHERE name='genetic_marker' 
                             AND cv_id=(SELECT cv_id FROM cv 
                                        WHERE name='sequence'))";
  logSQL('', $sql);
  my $sth = doQuery($dbh, $sql);
  if (my $row=$sth->fetchrow_hashref) {
    return $row->{'feature_id'};
  }
  else {
    return 0;
  }
}#getMarkerID


sub linkToCanonicalMarker {
  my ($dbh, $canonical_marker_id, $marker_id) = @_;
  
  # relationship: $marker_id (subj) instance_of $canonical_marker_id (obj)
  
  # make sure a link isn't already set
  my $sql = "
    SELECT feature_relationship_id, object_id FROM feature_relationship 
    WHERE subject_id=$marker_id
          AND type_id=(SELECT cvterm_id FROM cvterm 
                       WHERE name='instance_of' 
                             AND cv_id=(SELECT cv_id FROM cv 
                                        WHERE name='relationship'))";
  logSQL('', $sql);
#print "$sql\n";
  my $sth = doQuery($dbh, $sql);
  if (my $row=$sth->fetchrow_hashref) {
    print "$marker_id an instance_of with " . $row->{'object_id'} . "\n";
    if ($row->{'object_id'} == $canonical_marker_id) {
      print "$marker_id is already an instance_of $canonical_marker_id\n";
      # nothing to do
      return;
    }
    else {
      # marker is linked to the wrong canonical marker; remove link
      print "$marker_id is linked to a different canonical marker: "
            . $row->{'object_id'} . "\n";
      $sql = "
        DELETE FROM feature_relationship 
        WHERE feature_relationship_id=" . $row->{'feature_relationship_id'};
      logSQL('', $sql);
      $sth = doQuery($dbh, $sql);
    }
  }
  
  # link marker to canonical marker
  $sql = "
    INSERT INTO feature_relationship
      (subject_id, object_id, type_id)
    VALUES
      ($marker_id, $canonical_marker_id,
       (SELECT cvterm_id FROM cvterm 
        WHERE name='instance_of' 
              AND cv_id=(SELECT cv_id FROM cv 
                         WHERE name='relationship')))";
  logSQL('', $sql);
  $sth = doQuery($dbh, $sql);
}#linkToCanonicalMarker


sub setCanonical {
  my ($dbh, $canonical_marker_id) = @_;
  
  # Make sure this feature isn't already marked canonical
  my $sql = "
    SELECT value FROM featureprop 
    WHERE feature_id=$canonical_marker_id
          AND type_id=(SELECT cvterm_id FROM cvterm 
                       WHERE name='Canonical Marker' 
                             AND cv_id=(SELECT cv_id FROM cv 
                                        WHERE name='local'))";
  logSQL('', $sql);
  my $sth = doQuery($dbh, $sql);
  if (my $row=$sth->fetchrow_hashref) {
    # nothing to do
    return;
  }
  else {
    $sql = "
      INSERT INTO featureprop
        (feature_id, type_id, value, rank)
      VALUES
        ($canonical_marker_id,
         (SELECT cvterm_id FROM cvterm 
          WHERE name='Canonical Marker' 
                AND cv_id=(SELECT cv_id FROM cv WHERE name='local')),
         '',
         1)";
    logSQL('', $sql);
    $sth = doQuery($dbh, $sql);
  }    
}#setCanonical

sub unsetCanonical {
  my ($dbh, $marker_id) = @_;
  
  # Make sure this feature isn't already marked canonical
  my $sql = "
    SELECT value FROM featureprop 
    WHERE feature_id=$marker_id
          AND type_id=(SELECT cvterm_id FROM cvterm 
                       WHERE name='Canonical Marker' 
                             AND cv_id=(SELECT cv_id FROM cv 
                                        WHERE name='local'))";
  logSQL('', $sql);
  my $sth = doQuery($dbh, $sql);
  if (my $row=$sth->fetchrow_hashref) {
    $sql = "
      DELETE FROM featureprop
      WHERE feature_id=$marker_id
            AND type_id=(SELECT cvterm_id FROM cvterm 
                         WHERE name='Canonical Marker' 
                               AND cv_id=(SELECT cv_id FROM cv 
                                          WHERE name='local'))";
    logSQL('', $sql);
    $sth = doQuery($dbh, $sql);
  }    
}#setCanonical
