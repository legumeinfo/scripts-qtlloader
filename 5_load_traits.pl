# file: load_traits.pl
#
# purpose: Load spreadsheet map and QTL data into chado
#
#          It is assumed that the .txt files have been verified.
#
# IMPORTANT NOTE: this script assumes the data file has ^ALL^ of the
#                 the relevant information about this trait name.
#
#                 IF ADDITIONAL INFORMATION IS ATTACHED TO A TRAIT
#                 VIA OTHER MEANS, THAT INFORMATION WILL BE LOST IF
#                 THAT TRAIT IS THEN UPDATED VIA THIS SCRIPT!
#
# ALSO IMPORTANT: parent_trait worksheet/table is not yet mapped to chado
#                 and is therefore not loaded.
#
# Documentation:
#  http://search.cpan.org/~capttofu/DBD-mysql-4.022/lib/DBD/mysql.pm
#  http://search.cpan.org/~timb/DBI/DBI.pm
#
# history:
#  06/03/13  eksc  created
#  06/27/13  eksc  added update capability
#  10/16/13  eksc  modified for spreadsheet changes

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
  my ($has_errors, $line_count);

  my $dataset_name = 'traits';
  
  # Holds traits that are already in db; assume they should be updated
  my %existing_traits;

  # Get connected
  my $dbh = connectToDB;

  # set default schema
  $sql = "SET SEARCH_PATH = chado";
  $sth = $dbh->prepare($sql);
  $sth->execute();

  # Use a transaction so that it can be rolled back if there are any errors
  eval {
    loadTraits($dbh);
#not yet properly mapped to chado schema
#    loadParentTraits($dbh);
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


sub loadTraits {
  my $dbh = $_[0];
  my ($fields, $sql, $sth);
  
  $table_file = "$input_dir/OBS_TRAITS.txt";
  print "\nLoading/verifying $table_file...\n";  
  
  @records = readFile($table_file);
  $line_count = 0;
  my $skip_all = 0;  # skip all existing trait records
  my $update_all = 0; # update all existing trait records without asking
  foreach $fields (@records) {
    $line_count++;
    
print "$line_count: '$fields->{'qtl_symbol'}'\n";
    my $trait_id;
    if ($trait_id = getTraitRecord($dbh, 'qtl_symbol', $fields)) {
      next if ($skip_all);
      if ($update_all) {
        cleanDependants($trait_id);
      }
      else {
        print "$line_count: qtl_symbol ($fields->{'qtl_symbol'}) is already loaded.\n";
        print "Update? (y/all/n/skipall/q)\n";
        my $userinput =  <STDIN>;
        chomp ($userinput);
        if ($userinput eq 'skipall') {
          $skip_all = 1;
          next;
        }
        elsif ($userinput eq 'n') {
          next;
        }
        elsif ($userinput eq 'q') {
          exit;
        }
        elsif ($userinput eq 'all') {
          $update_all = 1;
          cleanDependants($trait_id);
        }
        elsif ($userinput eq 'y') {
          cleanDependants($trait_id);
        }
        else {
          print "unknown option ($userinput), skipping trait\n";
          next;
        }
      }
    }

    # insert a parent record for this obs_trait (qtl_symbol)
    $trait_id = setTraitRecord($dbh, $trait_id, 'qtl_symbol', $fields);
        
    # trait_name [changed from qtl_name]
    setTermRelationship($dbh, $trait_id, 'trait_name', 'has trait name', $fields);
    
    # alt names: cvtermsynonym (comma separated, if any)
    setAltNames($dbh, $trait_id, $fields);
    
    # trait_class and trait_unit: cvterm_relationship
    setTermRelationship($dbh, $trait_id, 'trait_class', 'has trait class', $fields);

    # OBO term
    setOBOTerm($dbh, $trait_id, $fields);
    
    # comment
    setCvtermprop($dbh, $trait_id, 'comment', 'comments', $fields);
   }#each record

  print "Loaded $line_count obs. trait records\n\n";
}#loadTraits
  
  
sub loadParentTraits {
  my ($fields, $sql, $sth) = @_;

=cut (not yet correctly mapped to chado schema)
  
  $table_file = "$input_dir/PARENT_TRAITS.txt";
  print "Loading/verifying $table_file...\n";
  
  @records = readFile($table_file);
  $line_count = 0;
  foreach $fields (@records) {
    $line_count++;
    
#   4b. load parent traits
#     4b.i trait name (cvterm)
#     4b.ii publication, comment (cvtermprop)
      # see 4a.ii
#     4b.iii parent (stock + stock_cvterm)
#     4b.iv verify that name links to a QTL symbol?
      # see 4a.iii
#     4b.v alt names (cvtermsynonym)
      # see 4a.v
   }#each record

  print "Loaded $line_count parent trait records\n\n";
=cut
}#loadParentTraits


################################################################################
################################################################################
################################################################################

sub attachDbxref {
  my ($dbh, $dbxref_id, $cvterm_id) = @_;
  my ($sql, $sth);
  $sql = "
    INSERT INTO chado.cvterm_dbxref
      (cvterm_id, dbxref_id)
    VALUES
      ($cvterm_id, $dbxref_id)";
    logSQL($dataset_name, "$line_count: $sql");
    doQuery($dbh, $sql);
}#attachDbxref


sub cleanDependants {
  my ($trait_id) = @_;
  my ($sql, $sth, $row);
  
  my $sql = "DELETE FROM chado.cvterm_dbxref WHERE cvterm_id=$trait_id";
  logSQL('', $sql);
  doQuery($dbh, $sql);
  
  my $sql = "DELETE FROM chado.cvterm_relationship WHERE subject_id=$trait_id";
  logSQL('', $sql);
  doQuery($dbh, $sql);
  
  my $sql = "DELETE FROM chado.cvtermprop WHERE cvterm_id=$trait_id";
  logSQL('', $sql);
  doQuery($dbh, $sql);
  
  my $sql = "DELETE FROM chado.cvtermsynonym WHERE cvterm_id=$trait_id";
  logSQL('', $sql);
  doQuery($dbh, $sql);
}#cleanDependants


sub getDbxref {
  my ($dbh, $fieldname, $fields) = @_;
  my ($sql, $sth, $row);
  
  my $term = $fields->{$fieldname};
  if ($term && $term ne '' && $term ne 'NULL') {
    if ($term =~ /(\w+)\:(\d+)/) {
      my $cv = $1;
      my $acc = $2;
      $sql = "
        SELECT dbxref_id FROM chado.dbxref 
        WHERE db_id = (SELECT db_id FROM chado.db WHERE name='$cv')
              AND accession = '$acc'
      ";
    }
    else {
      $sql = "
        SELECT dbxref_id FROM chado.dbxref 
        WHERE db_id = (SELECT db_id FROM chado.db WHERE name='LegumeInfo')
              AND accession = '$fields->{$fieldname}'
      ";
    }
    logSQL($dataset_name, "$line_count: $sql");
    $sth = doQuery($dbh, $sql);
    if ($row = $sth->fetchrow_hashref) {
      return $row->{'dbxref_id'}
    }
  }
  return 0;
}#getDbxref
  
  
sub getTraitRecord {
  my ($dbh, $fieldname, $fields) = @_;
  my ($sql, $sth, $row);
  
  $sql = "
    SELECT cvterm_id FROM chado.cvterm
    WHERE cv_id = (SELECT cv_id FROM chado.cv WHERE name='LegumeInfo:traits')
      AND name='$fields->{$fieldname}'";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);
  if ($row=$sth->fetchrow_hashref) {
    return $row->{'cvterm_id'};
  }
  
  return 0;
}#getTraitRecord


sub setAltNames {
  my ($dbh, $trait_id, $fields) = @_;
  my ($sql, $sth);
  
  if (!$fields->{'alt_names'} || $fields->{'alt_names'} eq '' 
        || $fields->{'alt_names'} eq 'NULL') {
    return;
  }
  
  my @names = split ',', $fields->{'alt_names'};
  foreach my $name (@names) {
    $sql = "
      INSERT INTO chado.cvtermsynonym
        (cvterm_id, synonym)
      VALUES
        ((SELECT cvterm_id FROM chado.cvterm WHERE name='$fields->{'qtl_symbol'}'),
         '$name')";
  }#each name
}#setAltNames


sub setCvtermprop {
  my ($dbh, $trait_id, $fieldname, $proptype, $fields) = @_;
  my ($sql, $sth, $row);
  
  # Check if there are already properties of this type
  my $rank = 0;
  $sql = "
    SELECT MAX(rank) FROM chado.cvtermprop 
    WHERE cvterm_id = $trait_id
        AND type_id = (SELECT cvterm_id FROM chado.cvterm 
                     WHERE name='$proptype'
                       AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='local'))";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);
  
  # If property exists, increase rank
  if ($row=$sth->fetchrow_hashref) {
    $rank = $row->{'rank'} + 1;
  }
  
  $sql = "
    INSERT INTO chado.cvtermprop 
      (cvterm_id, type_id, value, rank)
    VALUES
      ($trait_id,
       (SELECT cvterm_id FROM chado.cvterm WHERE name='$proptype'),
       '$fields->{$fieldname}', 
       $rank)";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);
}#setCvtermprop


sub setDbxref {
  my ($dbh, $fieldname, $fields) = @_;
  my ($sql, $sth, $row);
  
  my $dbxref_id = 0;
  
  my $term = $fields->{$fieldname};
  if ($term && $term ne '' && $term ne 'NULL') {
    $dbxref_id = getDbxref($dbh, $fieldname, $fields);
    if (!$dbxref_id) {
      $sql = "
        INSERT INTO chado.dbxref
          (db_id, accession)
        VALUES
          ((SELECT db_id FROM chado.db WHERE name='LegumeInfo'),
           '$fields->{$fieldname}')
        RETURNING dbxref_id";
      logSQL($dataset_name, "$line_count: $sql");
      $sth = doQuery($dbh, $sql);
      if ($row = $sth->fetchrow_hashref) {
        $dbxref_id = $row->{'dbxref_id'}
      }
    }#insert new dbxref record
  }#there is a term to search/insert
  
  return $dbxref_id;
}#setDbxref


sub setOBOTerm {
  my ($dbh, $trait_id, $fields) = @_;
  
  my $term = $fields->{'controlled_vocab_accessions'};
#print "set term $term\n";
  my $object_id = getOBOTerm($dbh, $term);
  if ($object_id) {
    $sql = "
      INSERT INTO chado.cvterm_relationship
        (subject_id, type_id, object_id)
      VALUES
        ($trait_id,
         (SELECT cvterm_id FROM chado.cvterm 
          WHERE name='has OBO term'
            AND cv_id = (SELECT cv_id FROM chado.cv WHERE name='local')),
         $object_id)";
    logSQL($dataset_name, "$line_count: $sql");
    $sth = doQuery($dbh, $sql);
  }
}#setOBOTerm


sub setTermRelationship {
  my ($dbh, $trait_id, $fieldname, $relationship, $fields) = @_;
  my ($sql, $sth, $row);

  if ($fields->{$fieldname} && $fields->{$fieldname} ne '' 
        && $fields->{$fieldname} ne 'NULL') {
    # insert related term if need be
    $sql = " SELECT cvterm_id FROM chado.cvterm WHERE name='$fields->{$fieldname}'";
    logSQL($dataset_name, "$line_count: $sql");
    $sth = doQuery($dbh, $sql);
    if (!($row=$sth->fetchrow_hashref)) {
      my $dbxref_id = setDbxref($dbh, $fieldname, $fields);
      $sql = "
        INSERT INTO chado.cvterm
          (cv_id, name, dbxref_id)
        VALUES
          ((SELECT cv_id FROM chado.cv WHERE name='LegumeInfo:traits'),
           '$fields->{$fieldname}',
           $dbxref_id)
        RETURNING cvterm_id";
      logSQL($dataset_name, "$line_count: $sql");
      $sth = doQuery($dbh, $sql);
      $row = $sth->fetchrow_hashref;
    }
    my $object_id = $row->{'cvterm_id'};
    
    $sql = "
      INSERT INTO chado.cvterm_relationship
        (subject_id, type_id, object_id)
      VALUES
        ($trait_id,
         (SELECT cvterm_id FROM chado.cvterm 
          WHERE name='$relationship'
            AND cv_id = (SELECT cv_id FROM chado.cv WHERE name='local')),
         $object_id)";
    logSQL($dataset_name, "$line_count: $sql");
    doQuery($dbh, $sql);
  }#value for property exists
}#setTermRelationship


sub setTraitRecord {
  my ($dbh, $trait_id, $fieldname, $fields) = @_;
  my ($sql, $sth, $row);
  
  # create dbxref
  my $dbxref_id = setDbxref($dbh, $fieldname, $fields);
  
  if ($trait_id) {
    $sql = "
      UPDATE chado.cvterm SET
        name='$fields->{$fieldname}',
        dbxref_id=$dbxref_id
      WHERE cvterm_id=$trait_id
      RETURNING cvterm_id";
  }
  else {
    $sql = "
      INSERT INTO chado.cvterm
        (cv_id, name, dbxref_id)
      VALUES
        ((SELECT cv_id FROM chado.cv WHERE name='LegumeInfo:traits'),
         '$fields->{$fieldname}', $dbxref_id)
      RETURNING cvterm_id";
  }
  
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);
  $row = $sth->fetchrow_hashref;
  
  return $row->{'cvterm_id'};
}#setTraitRecord

