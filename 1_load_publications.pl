# file: load_publications.pl
#
# purpose: Load spreadsheet publication data into a chado database
#
#          It is assumed that the .txt files have been checked and verified.
#
# http://search.cpan.org/~capttofu/DBD-mysql-4.022/lib/DBD/mysql.pm
# http://search.cpan.org/~timb/DBI/DBI.pm
#
# history:
#  05/16/13  eksc  created from load_xbase.pl
#  06/26/13  eksc  modified to handle updating records


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
      
    $0 data-dir
EOS
;
  if ($#ARGV < 0) {
    die $warn;
  }
  
  my $input_dir = @ARGV[0];
  my @filepaths = <$input_dir/*.txt>;
  my %files = map { s/$input_dir\/(.*)$/$1/; $_ => 1} @filepaths;
  
  # Used all over
  my ($table_file, $sql, $sth, $row, $count, @records, @fields, $cmd, $rv);
  my ($has_errors);
  
  my $dataset_name = 'publication';
  
  # Holds publications that are already in db; assume they should be updated
  my %existing_citations;

  # Get connected
  my $dbh = connectToDB;
  
  # Set default schema
  $sql = "SET SEARCH_PATH = chado";
  $sth = $dbh->prepare($sql);
  $sth->execute();

  # Use a transaction so that it can be rolled back if there are any errors
  eval {
    loadPublications($dbh);
    loadAuthors($dbh);
    loadURLs($dbh);
    loadKeywords($dbh);
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

sub loadPublications {
  my $dbh = $_[0];
  my (%fields, $sql, $sth);

  $table_file = "$input_dir/PUBS.txt";
  print "Loading $table_file...\n";
  
  @records = readFile($table_file);
  my $line_count = 0;
  my $skip_all = 0;  # skip all existing trait records
  foreach my $fields (@records) {
    $line_count++;
    
    if (my $pub_id = publicationExists($dbh, $fields->{'publink_citation'})) {
      next if ($skip_all);
      print "$line_count: publication ($fields->{'publink_citation'}) is already loaded.\nUpdate? (y/n/q)\n";
      my $userinput =  <STDIN>;
      chomp ($userinput);
      if ($userinput eq 'n') {
        next;
      }
      elsif ($userinput eq 'q') {
        exit;
      }
      elsif ($userinput eq 'y') {
        $existing_citations{$fields->{'publink_citation'}} = $pub_id;
      
        # remove dependent records; they will be re-inserted
        clearDependencies($dbh, $pub_id);
      }
      else {
        print "unknown option ($userinput), skipping publication\n";
        next;
      }
    }
    
    # title, volume, issue, year, pages, citation, ref_type
    setPubRec($dbh, $fields);

    # doi, isbn, pmid
    set_dbxref($dbh, 'DOI', 'doi', $fields);
    set_dbxref($dbh, 'PMID', 'pmid', $fields);
# pub table contains ISBNs for the journal, which creates duplicate entries. 
# ISBNs aren't too useful anyway...
#    set_dbxref($dbh, 'ISBN', 'isbn', $fields);
    
    # abstract
    setAbstract($dbh, $fields);
  }#each record
  
  print "Loaded $line_count pub records\n\n";
}#loadPublications


sub loadAuthors {
  my $dbh = $_[0];
  my (%fields, $sql, $sth);
  
  $table_file = "$input_dir/PUB_AUTHORS.txt";
  print "Loading $table_file...\n";
  
  @records = readFile($table_file);
  my $line_count = 0;
  foreach my $fields (@records) {
    $line_count++;
    
    # split author into last, first
    my ($last, $first) = split ",", $fields->{'author'};
    $sql = "
      INSERT INTO chado.pubauthor
        (pub_id, rank, surname, givennames)
      VALUES
        ((SELECT pub_id FROM chado.pub WHERE uniquename=?),
         $fields->{'cite_order'}, ?, ?)";
    logSQL($dataset_name, $sql);
    doQuery($dbh, $sql, ($fields->{'publink_citation'}, $last, $first));
  }#each record
  
  print "\n\nLoaded $line_count pub author records\n\n";
}#loadAuthors


sub loadURLs {
  my $dbh = $_[0];
  my (%fields, $sql, $sth);
  
  print "Loading PUB_URLS.txt...\n";
  
  $table_file = "$input_dir/PUB_URLS.txt";
  @records = readFile($table_file);
  my $line_count = 0;
  foreach my $fields (@records) {
    $line_count++;
    
    $sql = "
      INSERT INTO chado.pubprop
       (pub_id, type_id, value, rank)
      VALUES
       ((SELECT pub_id FROM chado.pub WHERE uniquename=?),
        (SELECT cvterm_id FROM chado.cvterm 
         WHERE name='publication URL'
           AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='local')),
        ?, 0)";
    logSQL($dataset_name, $sql);
    doQuery($dbh, $sql, ($fields->{'publink_citation'}, $fields->{'url'}));
  }#each record
  
  print "Loaded $line_count pub URL records\n\n";
}#loadURLs


sub loadKeywords {
  my $dbh = $_[0];
  my (%fields, $sql, $sth);
  
  # Likely multiple keywords per publication
  my %keyword_count;
  
  $table_file = "$input_dir/PUB_KEYWORDS.txt";
  print "Loading $table_file...\n";
  
  @records = readFile($table_file);
  my $line_count = 0;
  foreach my $fields (@records) {
    $line_count++;
    
    $keyword_count{$fields->{'publink_citation'}}++;
    $sql = "
      INSERT INTO chado.pubprop
       (pub_id, type_id, value, rank)
      VALUES
       ((SELECT pub_id FROM chado.pub WHERE uniquename=?),
        (SELECT cvterm_id FROM chado.cvterm 
         WHERE name='publication keyword'
           AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='local')),
        ?, $keyword_count{$fields->{'publink_citation'}})";
    logSQL($dataset_name, $sql);
    doQuery($dbh, $sql, ($fields->{'publink_citation'}, $fields->{'keyword'}));
  }#each record
  
  print "Loaded $line_count pub keyword records\n\n";
}#loadKeywords



################################################################################
################################################################################
################################################################################
################################################################################

sub clearDependencies {
  my ($dbh, $pub_id) = @_;
  
  # delete dbxref records and links to them.
  $sql = "SELECT dbxref_id FROM chado.pub_dbxref WHERE pub_id=$pub_id";
  logSQL('', $sql);
  $sth = doQuery($dbh, $sql);
  while ($row=$sth->fetchrow_hashref) {
    # NOTE: this will also delete dependant pub_dbxref record
    $sql = "DELETE FROM chado.dbxref WHERE dbxref_id=$row->{'dbxref_id'}";
    logSQL('', $sql);
    doQuery($dbh, $sql);
  }#each dependant dbxref
  
  # delete publication properties
  $sql = "DELETE FROM chado.pubprop WHERE pub_id=$pub_id";
  logSQL($sql);
  doQuery($dbh, $sql);
  
  # delete dependand pubauthor records
  $sql = "DELETE FROM chado.pubauthor WHERE pub_id=$pub_id";
  logSQL($sql);
  doQuery($dbh, $sql);
}#clearDependencies


sub setAbstract {
  my ($dbh, $fields) = @_;
  
  if ($fields->{'abstract'} && $fields->{'abstract'} ne '' 
        && $fields->{'abstract'} ne 'NULL') {
    $sql = "
     INSERT INTO chado.pubprop
       (pub_id, type_id, value, rank)
     VALUES
       ((SELECT pub_id FROM chado.pub WHERE uniquename=?),
        (SELECT cvterm_id FROM chado.cvterm 
         WHERE name='abstract'
           AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='tripal_pub')),
        ?, 0)";
    logSQL($dataset_name, $sql);
    doQuery($dbh, $sql, 
           ($fields->{'publink_citation'}, $fields->{'abstract'}));
  }
}#setAbstract


sub set_dbxref {
  my ($dbh, $name, $key, $fields) = @_;
  
  if ($fields->{$key} && $fields->{$key} ne '' && $fields->{$key} ne 'NULL') {
    my $sql = "
      INSERT INTO chado.dbxref
       (db_id, accession)
      VALUES
       ((SELECT db_id FROM chado.db WHERE name='$name'),
        '$fields->{$key}')";
    logSQL($dataset_name, $sql);
    doQuery($dbh, $sql);
    
    $sql = "
      INSERT INTO chado.pub_dbxref
       (pub_id, dbxref_id)
      VALUES
       ((SELECT pub_id FROM chado.pub WHERE uniquename='$fields->{'publink_citation'}'),
        (SELECT dbxref_id FROM chado.dbxref WHERE accession='$fields->{$key}'))";
    logSQL($dataset_name, $sql);
    doQuery($dbh, $sql);
  }
}#set_dbxref


sub setPubRec {
  my ($dbh, $fields) = @_;
  
  if ($existing_citations{$fields->{'publink_citation'}}) {
    $sql = "
      UPDATE chado.pub SET
        title=?, series_name=?, volume='$fields->{'volume'}',
        issue='$fields->{'issue'}', pyear='$fields->{'year'}',
        pages='$fields->{'pages'}', uniquename=?,
        type_id=(SELECT cvterm_id FROM chado.cvterm 
                 WHERE name=LOWER('$fields->{'ref_type'}')
                   AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='local'))
      WHERE pub_id=$existing_citations{$fields->{'publink_citation'}}";
  }
  else {
    $sql = "
     INSERT INTO chado.pub 
       (title, series_name, volume, issue, pyear, pages, uniquename, type_id)
      VALUES
         (?, ?, '$fields->{'volume'}', '$fields->{'issue'}', '$fields->{'year'}', 
          '$fields->{'pages'}', ?,
          (SELECT cvterm_id FROM chado.cvterm 
           WHERE name=LOWER('$fields->{'ref_type'}')
             AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='local'))
         )";
  }
  
  logSQL($dataset_name, 
         "$sql\nwith $fields->{'title'}, $fields->{'series_book_title'}, $fields->{'publink_citation'}");
  doQuery($dbh, $sql, 
          ($fields->{'title'}, 
           $fields->{'series_book_title'}, 
           $fields->{'publink_citation'}));
}#setPubRec

