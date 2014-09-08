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
  
  # Get spreadsheet constants
  my %pi = getSSInfo('PUBS');

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
    
# no longer separate worksheets
#    loadAuthors($dbh);
#    loadURLs($dbh);
#    loadKeywords($dbh);
    
#    $dbh->commit;   # commit the changes if we get this far
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

  $table_file = "$input_dir/$pi{'worksheet'}.txt";
  print "Loading $table_file...\n";
  
  my ($skip, $skip_all, $update, $update_all);

  @records = readFile($table_file);
  my $line_count = 0;
  foreach my $fields (@records) {
    $line_count++;

    my $pub_id;
    if ($pub_id = publicationExists($dbh, $fields->{$pi{'pub_fld'}})) {
      # publication exists
      next if ($skip_all);
      if ($update_all) {
          $existing_citations{$fields->{$pi{'pub_fld'}}} = $pub_id;
          
          # remove dependent records; they will be re-inserted
          clearDependencies($dbh, $pub_id);
      }
      else {
        my $prompt = "$line_count: publication ($fields->{$pi{'pub_fld'}}) ";
        ($skip, $skip_all, $update, $update_all) = checkUpdate($prompt);
        
        next if ($skip || $skip_all);
        
        if ($update || $update_all) {
          $existing_citations{$fields->{$pi{'pub_fld'}}} = $pub_id;
          
          # remove dependent records; they will be re-inserted
          clearDependencies($dbh, $pub_id);
        }
      }#update_all not set
    }#publication exists

    # title, volume, issue, year, pages, citation, ref_type
    setPubRec($dbh, $fields);

    # doi, pmid
    set_dbxref($dbh, 'DOI', $pi{'doi_fld'}, $fields);
    set_dbxref($dbh, 'PMID', $pi{'pmid_fld'}, $fields);
    
    # citation
    setCitation($dbh, $fields);
    
    # abstract
    setAbstract($dbh, $fields);
    
    # authors
    setAuthors($dbh, $fields);
    
    # keywords
    setKeywords($dbh, $fields);
    
    # URLs
    setURLs($dbh, $fields);
  }#each record
  
  print "Loaded $line_count pub records\n\n";
}#loadPublications


=cut not loading authors from separate worksheet
sub loadAuthors {
  my $dbh = $_[0];
  my (%fields, $sql, $sth);
  
  $table_file = "$input_dir/PUB_AUTHORS.txt";
  print "Loading $table_file...\n";
  
  @records = readFile($table_file);
  my @authors;
  my $publink_citation = '';
  my $line_count = 0;
  foreach my $fields (@records) {
    if ($publink_citation ne $fields->{'publink_citation'}) {
      # moved to new (or first) publication
      if ($publink_citation ne '') {
        # Also (bleech) save in a comma-separate list as a prop
        $sql = "
          INSERT INTO chado.pubprop
           (pub_id, type_id, value, rank)
          VALUES
           ((SELECT pub_id FROM chado.pub WHERE uniquename=?),
            (SELECT cvterm_id FROM chado.cvterm 
             WHERE name='Authors'
               AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='tripal_pub')),
            ?, 0)";
        logSQL($dataset_name, 
               "$sql\nWITH:\n  uniquename: $publink_citation\n  authors: " . (join ',', @authors));
        doQuery($dbh, $sql, ($publink_citation, (join ', ', @authors)));
      }
      @authors = ();
      $publink_citation = $fields->{'publink_citation'};
    }
        
    $publink_citation = $fields->{'publink_citation'};
    $line_count++;
    
    # split author into last, first
    my ($last, $first) = split ",", $fields->{'author'};
    $sql = "
      INSERT INTO chado.pubauthor
        (pub_id, rank, surname, givennames)
      VALUES
        ((SELECT pub_id FROM chado.pub WHERE uniquename=?),
         $fields->{'cite_order'}, ?, ?)";
    logSQL($dataset_name, 
           "$sql\nWITH:\n  uniquename: " . $fields->{'publink_citation'} . "\n  surname: $last\n  givennames: $first");
    doQuery($dbh, $sql, ($fields->{'publink_citation'}, $last, $first));
    
    push @authors, $fields->{'author'};
  }#each record
  
  print "\n\nLoaded $line_count pub author records\n\n";
}#loadAuthors
=cut


=cut not loading URLs from separate worksheet
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
         WHERE name='URL'
           AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='tripal_pub')),
        ?, 0)";
    logSQL($dataset_name, 
           "$sql\nWITH:\n  uniquename: " . $fields->{'publink_citation'} . "\n  url: " . $fields->{'url'});
    doQuery($dbh, $sql, ($fields->{'publink_citation'}, $fields->{'url'}));
  }#each record
  
  print "Loaded $line_count pub URL records\n\n";
}#loadURLs
=cut


=cut not loading keywords from separate spreadsheet
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
         WHERE name='Keywords'
           AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='tripal_pub')),
        ?, $keyword_count{$fields->{'publink_citation'}})";
    logSQL($dataset_name, 
           "$sql\nWITH:\n  uniquename: " . $fields->{'publink_citation'} . "\n  value: " . $fields->{'keyword'});
    doQuery($dbh, $sql, ($fields->{'publink_citation'}, $fields->{'keyword'}));
  }#each record
  
  print "Loaded $line_count pub keyword records\n\n";
}#loadKeywords
=cut



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
  
  # just for good measure...
  $sql = "DELETE FROM chado.pub_dbxref WHERE pub_id=$pub_id";
  logSQL('', $sql);
  doQuery($dbh, $sql);
  
  # delete publication properties
  $sql = "DELETE FROM chado.pubprop WHERE pub_id=$pub_id";
  logSQL('', $sql);
  doQuery($dbh, $sql);
  
  # delete dependant pubauthor records
  $sql = "DELETE FROM chado.pubauthor WHERE pub_id=$pub_id";
  logSQL('', $sql);
  doQuery($dbh, $sql);
}#clearDependencies


sub setAbstract {
  my ($dbh, $fields) = @_;
  
  if ($fields->{$pi{'abstract_fld'}} && $fields->{$pi{'abstract_fld'}} ne '' 
        && $fields->{$pi{'abstract_fld'}} ne 'NULL') {
    $sql = "
     INSERT INTO chado.pubprop
       (pub_id, type_id, value, rank)
     VALUES
       ((SELECT pub_id FROM chado.pub WHERE uniquename=?),
        (SELECT cvterm_id FROM chado.cvterm 
         WHERE name='Abstract'
           AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='tripal_pub')),
        ?, 0)";
    logSQL($dataset_name, $sql);
    doQuery($dbh, $sql, 
            ($fields->{$pi{'pub_fld'}}, $fields->{$pi{'abstract_fld'}}));
  }
}#setAbstract


sub setAuthors {
  my ($dbh, $fields) = @_;

  my $author_list = $fields->{$pi{'authorfld'}};
  my @authors = split /,/, $fields->{$pi{'author_fld'}};
  my $publink_citation = $fields->{$pi{'pub_fld'}};
  
  # (bleech) save in a comma-separate list as a prop
  $sql = "
    INSERT INTO chado.pubprop
     (pub_id, type_id, value, rank)
    VALUES
     ((SELECT pub_id FROM chado.pub WHERE uniquename=?),
      (SELECT cvterm_id FROM chado.cvterm 
       WHERE name='Authors'
         AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='tripal_pub')),
      ?, 0)";
  logSQL($dataset_name, 
         "$sql\nWITH:\n  uniquename: $publink_citation\n  authors: $author_list");
  doQuery($dbh, $sql, ($publink_citation, $author_list));
  
  # (the right way) also store each author in the pubauthor table
  foreach my $author (@authors) {
    # split author into last, first
    my ($last, $first) = split " ", $author;
    $sql = "
      INSERT INTO chado.pubauthor
        (pub_id, rank, surname, givennames)
      VALUES
        ((SELECT pub_id FROM chado.pub WHERE uniquename=?),
         $fields->{'cite_order'}, ?, ?)";
    logSQL($dataset_name, 
           "$sql\nWITH:\n  uniquename: $publink_citation\n  surname: $last\n  givennames: $first");
    doQuery($dbh, $sql, ($publink_citation, $last, $first));
  }#each record
}#setAuthors


sub setCitation {
  my ($dbh, $fields) = @_;
  
  my $publink_citation = $fields->{$pi{'pub_fld'}};
  
  if ($publink_citation && $publink_citation ne '' 
        && $publink_citation ne 'NULL') {
    my $citation = $publink_citation;
    $citation =~ s/\w$//; 
#print "citation will be [$citation]\n";
    $sql = "
     INSERT INTO chado.pubprop
       (pub_id, type_id, value, rank)
     VALUES
       ((SELECT pub_id FROM chado.pub WHERE uniquename=?),
        (SELECT cvterm_id FROM chado.cvterm 
         WHERE name='Citation'
           AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='tripal_pub')),
        ?, 0)";
    logSQL($dataset_name, 
           "$sql\nWITH:\n  uniquename: $publink_citation\n  citation: $citation");
    doQuery($dbh, $sql, ($publink_citation, $citation));
  }
}#setCitation


sub set_dbxref {
  my ($dbh, $name, $key, $fields) = @_;
  
  if ($fields->{$key} && $fields->{$key} ne '' && $fields->{$key} ne 'NULL'
        && $fields->{$key} ne 'none' && $fields->{$key} ne 'n/a') {
    my $sql = "
      INSERT INTO chado.dbxref
       (db_id, accession)
      VALUES
       ((SELECT db_id FROM chado.db WHERE name='$name'),
        '$fields->{$key}')";
    logSQL($dataset_name, 
           "$sql\nWITH:  accession: " . $fields->{$key});
    doQuery($dbh, $sql);
    
    $sql = "
      INSERT INTO chado.pub_dbxref
       (pub_id, dbxref_id)
      VALUES
       ((SELECT pub_id FROM chado.pub WHERE uniquename='$fields->{'publink_citation'}'),
        (SELECT dbxref_id FROM chado.dbxref WHERE accession='$fields->{$key}'))";
    logSQL($dataset_name, 
           "$sql\nWITH:  uniquename: " . $fields->{'publink_citation'} . "\n  accession: " . $fields->{$key});
    doQuery($dbh, $sql);
  }
}#set_dbxref


sub setKeywords {
  my ($dbh, $fields) = @_;
  
  my $publink_citation = $fields->{$pi{'pub_fld'}};
  
  my @keywords = split ",", $fields->{$pi{'keyword_fld'}};
  my $keyword_count = 0;
  foreach my $keyword (@keywords) {    
    $keyword_count++;
    $sql = "
      INSERT INTO chado.pubprop
       (pub_id, type_id, value, rank)
      VALUES
       ((SELECT pub_id FROM chado.pub WHERE uniquename=?),
        (SELECT cvterm_id FROM chado.cvterm 
         WHERE name='Keywords'
           AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='tripal_pub')),
        ?, $keyword_count)";
    logSQL($dataset_name, 
           "$sql\nWITH:\n  uniquename: $publink_citation\n  value: $keyword");
    doQuery($dbh, $sql, ($publink_citation, $keyword));
  }#each record
}#setKeywords


sub setPubRec {
  my ($dbh, $fields) = @_;
  
  my $publink_citation = $fields->{$pi{'pub_fld'}};

  if ($existing_citations{$publink_citation}) {
    $sql = "
      UPDATE chado.pub SET
        title=?, series_name=?, volume='$fields->{$pi{'volume_fld'}}',
        issue='$fields->{$pi{'issue_fld'}}', pyear='$fields->{$pi{'year_fld'}}',
        pages='$fields->{$pi{'page_fld'}}', uniquename=?,
        type_id=(SELECT cvterm_id FROM chado.cvterm 
                 WHERE LOWER(name)=LOWER('$fields->{$pi{'ref_type_fld'}}')
                   AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='tripal_pub'))
      WHERE pub_id=$existing_citations{$publink_citation}";
  }
  else {
    $sql = "
     INSERT INTO chado.pub 
       (title, series_name, volume, issue, pyear, pages, uniquename, type_id)
      VALUES
         (?, ?, '$fields->{$pi{'volume_fld'}}', '$fields->{$pi{'issue_fld'}}', 
          '$fields->{$pi{'year_fld'}}', '$fields->{$pi{'page_fld'}}', ?,
          (SELECT cvterm_id FROM chado.cvterm 
           WHERE LOWER(name)=LOWER('$fields->{$pi{'ref_type_fld'}}')
             AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='tripal_pub'))
         )";
  }
  
  logSQL($dataset_name, 
         "$sql\nWITH:\n  title: $fields->{$pi{'title_fld'}}\n  journal: $fields->{$pi{'journal_fld'}}\n  uniquename: $publink_citation\n\n");
  doQuery($dbh, $sql, 
          ($fields->{$pi{'title_fld'}}, 
           $fields->{$pi{'journal_fld'}}, 
           $publink_citation));
}#setPubRec


sub setURLs {
  my ($dbh, $fields) = @_;

  my $publink_citation = $fields->{$pi{'pub_fld'}};
  my @URLs = split ",", $fields->{$pi{'url_fld'}};
  my $URL_count = 0;
  foreach my $URL (@URLs) {    
    $URL_count++;
    
    $sql = "
      INSERT INTO chado.pubprop
       (pub_id, type_id, value, rank)
      VALUES
       ((SELECT pub_id FROM chado.pub WHERE uniquename=?),
        (SELECT cvterm_id FROM chado.cvterm 
         WHERE name='URL'
           AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='tripal_pub')),
        ?, 0)";
    logSQL($dataset_name, 
           "$sql\nWITH:\n  uniquename: $publink_citation\n  url: $URL");
    doQuery($dbh, $sql, ($publink_citation, $URL));
  }#each record
}#setURLs
