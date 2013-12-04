# file: loadCV.pl
#
# purpose: Load (insert/update) CV terms for PeanutBase, LegumeInfo from
#          a tab-delineated file.
#
# http://search.cpan.org/~capttofu/DBD-mysql-4.022/lib/DBD/mysql.pm
# http://search.cpan.org/~timb/DBI/DBI.pm
#
# history:
#  11/27/13  eksc  created


  use strict;
  use DBI;
  use Data::Dumper;
  use Encode;

  # load local util library
  use File::Spec::Functions qw(rel2abs);
  use File::Basename;
  use lib dirname(rel2abs($0));
  use CropLegumeBaseLoaderUtils;
  
  my $warn = <<EOS
    Usage:
      
      $0 cvterm-file
      
EOS
;
  if ($#ARGV < 0) {
    die $warn;
  }
  
  my $cvfile = @ARGV[0];
  
  # Used all over
  my ($has_errors);
  
  # Get connected
  my $dbh = connectToDB;
  
  # Set default schema
  my $sql = "SET SEARCH_PATH = chado";
  my $sth = $dbh->prepare($sql);
  $sth->execute();

  # Use a transaction so that it can be rolled back if there are any errors
  eval {
    loadCVterms($dbh);
    $dbh->commit;   # commit the changes if we get this far
  };
  if ($@) {
    print "\n\nTransaction aborted because $@\n\n";
    # now rollback to undo the incomplete changes
    # but do it in an eval{} as it may also fail
    eval { $dbh->rollback };
  }


# ALL DONE
$dbh->disconnect();
print "\n\nScript completed\n\n";



################################################################################
####### Major functions                                                #########
################################################################################

sub loadCVterms {
  my $dbh = $_[0];
  my (@records, %fields, $sql, $sth);

  print "Loading $cvfile...\n";
  
  @records = readFile($cvfile);
  my $line_count = 0;
  my $skip_all = 0;  # skip all existing trait records
  foreach my $fields (@records) {
    $line_count++;

    # title, volume, issue, year, pages, citation, ref_type
    setCVterm($dbh, $fields);
  }#each record
  
  print "Loaded $line_count cvterms\n\n";
}#loadPublications



################################################################################
################################################################################
################################################################################
################################################################################

sub setCVterm {
  my ($dbh, $fields) = @_;
  
  # first, insert/update dbxref record
  setdbxref($dbh, $fields);
  
  my $sql = "
      SELECT cvterm_id FROM cvterm 
      WHERE name='$fields->{'term'}'
            AND cv_id=(SELECT cv_id FROM cv WHERE name='$fields->{'cv'}')";
print "$sql\n";
  my $sth = doQuery($dbh, $sql);
  if (!(my $row = $sth->fetchrow_hashref)) {
    # insert
    $sql = "
      INSERT INTO cvterm 
        (cv_id, name, definition, dbxref_id)
      VALUES
        ((SELECT cv_id FROM cv WHERE name='$fields->{'cv'}'),
         '$fields->{'term'}',
         '$fields->{'description'}',
         (SELECT dbxref_id FROM dbxref 
          WHERE accession='$fields->{'term'}' 
                AND db_id = (SELECT db_id FROM db WHERE name='$fields->{'db'}')))";
print "$sql\n";
    $sth = doQuery($dbh, $sql);
  }
  else {
    # note: can only update description
    print "The cvterm $fields->{'term'} already exists; updating description.\n";
    $sql = "
      UPDATE cvterm 
      SET definition='$fields->{'description'}'
      WHERE cvterm_id=$row->{'cvterm_id'}";
print "$sql\n";
    $sth = doQuery($dbh, $sql);
  }
}#setCVterm

sub setdbxref {
  my ($dbh, $fields) = @_;
  
  my $sql = "
      SELECT dbxref_id FROM dbxref 
      WHERE accession='$fields->{'term'}'";
print "$sql\n";
  my $sth = doQuery($dbh, $sql);
  if (!(my $row = $sth->fetchrow_hashref)) {
    # insert
    $sql = "
      INSERT INTO dbxref 
        (db_id, accession, description)
      VALUES
        ((SELECT db_id FROM db WHERE name='$fields->{'db'}'),
         '$fields->{'term'}',
         '$fields->{'description'}')";
print "$sql\n";
    $sth = doQuery($dbh, $sql);
  }
  else {
    # note: can only update description
    $sql = "
      UPDATE dbxref 
      SET description='$fields->{'description'}'
      WHERE dbxref_id=$row->{'dbxref_id'}";
print "$sql\n";
    $sth = doQuery($dbh, $sql);
  }
}#setdbxref

