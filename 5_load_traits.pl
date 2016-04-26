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
#  09/05/14  eksc  modified for current all-traits spreadsheet

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
  
  # get worksheet contants
  my %ti = getSSInfo('TRAITS');

  # Used all over
  my ($table_file, $sql, $sth, $row, $count, @records, @fields, $cmd, $rv);
  my ($has_errors, $line_count);

  my $dataset_name = 'traits';
  
  # Holds traits that are already in db
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
  print "\n\nScript completed\n\n";



################################################################################
####### Major functions                                                #########
################################################################################


sub loadTraits {
  my $dbh = $_[0];
  my ($fields, $sql, $sth);
  
  $table_file = "$input_dir/$ti{'worksheet'}.txt";
  print "\nLoading/verifying $table_file...\n";  
  
  my ($skip, $skip_all, $update, $update_all);

  @records = readFile($table_file);
  $line_count    = 0;
  foreach $fields (@records) {
    $line_count++;
    
    my $qtl_symbol = $fields->{$ti{'qtl_symbol_fld'}};
    next if (!$qtl_symbol || $qtl_symbol eq '' || lc($qtl_symbol) eq 'null');
print "\nQTL symbol: $qtl_symbol\n";

    my $trait_id;
    if ($trait_id = getTraitRecord($dbh, $qtl_symbol)) {
print "  exists\n";
      # trait exists
      next if ($skip_all);
      
      if ($update_all) {
          cleanDependants($trait_id);
      }
      else {
        my $prompt = "$line_count: QTL symbol ($qtl_symbol} = $trait_id) ";
        ($skip, $skip_all, $update, $update_all) = checkUpdate($prompt);
        
        next if ($skip || $skip_all);
        
        if ($update || $update_all) {
          $existing_traits{$qtl_symbol} = $trait_id;
          
          # remove dependent records; they will be re-inserted
          cleanDependants($trait_id);
        }
      }#update_all not set
    }#trait exists

    # insert a parent record for this QTL Symbol
    $trait_id = setTraitRecord($dbh, $trait_id, 
                               $fields->{$ti{'qtl_symbol_fld'}}, 
                               $fields->{$ti{'description_fld'}});
print "  trait ID: $trait_id\n";
    
    # trait_name 
    setTermRelationship($dbh, $trait_id, $fields->{$ti{'trait_name_fld'}}, 
                        'Has Trait Name', 'contains', $fields);
    
    # trait_class and trait_unit: cvterm_relationship
    setTermRelationship($dbh, $trait_id, $fields->{$ti{'trait_class_fld'}}, 
                        'Has Trait Class', 'is_a', $fields);

    # OBO term
    setOBOTerm($dbh, $trait_id, $fields);
  }#each record

  print "Loaded $line_count obs. trait records\n\n";
}#loadTraits
  
  
=cut #(not yet correctly mapped to chado schema)
sub loadParentTraits {
  my ($fields, $sql, $sth) = @_;

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
}#loadParentTraits
=cut


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
  
  my $sql = "DELETE FROM chado.cvterm_dbxref WHERE cvterm_id=$trait_id";
  logSQL('', $sql);
  doQuery($dbh, $sql);
}#cleanDependants


sub getDbxref {
  my ($dbh, $term) = @_;
  my ($sql, $sth, $row);
  
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
        WHERE db_id = (SELECT db_id FROM chado.db WHERE name='LegumeInfo:traits')
              AND accession = '$term'
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
  my ($dbh, $trait_name) = @_;
  my ($sql, $sth, $row);
  
  $sql = "
    SELECT cvterm_id FROM chado.cvterm
    WHERE cv_id = (SELECT cv_id FROM chado.cv WHERE name='LegumeInfo:traits')
      AND name='$trait_name'";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);
  if ($row=$sth->fetchrow_hashref) {
    return $row->{'cvterm_id'};
  }
  
  return 0;
}#getTraitRecord


sub setCvtermprop {
  my ($dbh, $trait_id, $prop, $proptype) = @_;
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
       '$prop', 
       $rank)";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);
}#setCvtermprop


sub setDbxref {
  my ($dbh, $term) = @_;
  my ($sql, $sth, $row);
  
  my $dbxref_id = 0;
  
  if ($term && $term ne '' && $term ne 'NULL') {
    $dbxref_id = getDbxref($dbh, $term);
    if (!$dbxref_id) {
      $term = $dbh->quote($term);
      $sql = "
        INSERT INTO chado.dbxref
          (db_id, accession)
        VALUES
          ((SELECT db_id FROM chado.db WHERE name='LegumeInfo:traits'),
           $term)
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
  
  my $term = $fields->{$ti{'onto_id_fld'}};
  
  return if (!$term || $term eq '' || lc($term) eq 'null');
  
  $term =~ /.*?:(\d+)/;
  my $acc = $1;
  my $name = $fields->{$ti{'onto_name_fld'}};
#print "set term $term: accession: $acc, name=$name\n";

  # check for existence
  my $dbxref_id = getDbxref($dbh, $term);
  if ($dbxref_id) {
    $sql = "
      INSERT INTO chado.cvterm_dbxref
        (cvterm_id, dbxref_id)
      VALUES
        ($trait_id, $dbxref_id)";
    logSQL($dataset_name, "$line_count: $sql");
    $sth = doQuery($dbh, $sql);
  }
}#setOBOTerm


sub setTermRelationship {
  my ($dbh, $trait_id, $term, $relationship, $path_type, $fields) = @_;
  my ($sql, $sth, $row);

  if ($term && $term ne '' && lc($term) ne 'null') {
    # Does this related term exist?
    $sql = "
      SELECT cvterm_id FROM chado.cvterm 
      WHERE name='$term' 
            AND cv_id = (SELECT cv_id FROM chado.cv 
                         WHERE name='LegumeInfo:traits')";
    logSQL($dataset_name, "$line_count: $sql");
    $sth = doQuery($dbh, $sql);
    
    if (!($row=$sth->fetchrow_hashref)) {
      # Term doesn't exist, create it
      my $dbxref_id = setDbxref($dbh, $term);
      $sql = "
        INSERT INTO chado.cvterm
          (cv_id, name, dbxref_id)
        VALUES
          ((SELECT cv_id FROM chado.cv WHERE name='LegumeInfo:traits'),
           '$term',
           $dbxref_id)
        RETURNING cvterm_id";
      logSQL($dataset_name, "$line_count: $sql");
      $sth = doQuery($dbh, $sql);
      $row = $sth->fetchrow_hashref;
    }#insert cvterm
    my $object_id = $row->{'cvterm_id'};
    
    # Indicate relationship between term and trait
    $sql = "
      SELECT cvterm_relationship_id FROM chado.cvterm_relationship
      WHERE subject_id=$trait_id
            AND type_id=(SELECT cvterm_id FROM chado.cvterm 
                         WHERE name='$relationship'
                               AND cv_id = (SELECT cv_id FROM chado.cv 
                                            WHERE name='local'))
            AND object_id=$object_id";
    logSQL($dataset_name, "$line_count: $sql");
    my $cr_sth = doQuery($dbh, $sql);
    if (!(my $cr_row=$cr_sth->fetchrow_hashref)) {
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
    }#insert cvterm_relationship
    
    # Indicate path between terms
    $sql = "
      SELECT cvtermpath_id FROM chado.cvtermpath
      WHERE type_id=(SELECT cvterm_id FROM chado.cvterm 
                     WHERE name='$path_type' 
                           AND cv_id=(SELECT cv_id FROM cv 
                                      WHERE name='relationship'))
             AND subject_id=$trait_id
             AND object_id=$object_id
             AND cv_id = (SELECT cv_id FROM chado.cv WHERE name='LegumeInfo:traits')";
    logSQL($dataset_name, "$line_count: $sql");
    my $cp_sth = doQuery($dbh, $sql);
    if (!(my $cp_row=$cp_sth->fetchrow_hashref)) {
      $sql = "
        INSERT INTO chado.cvtermpath
          (type_id, subject_id, object_id, cv_id, pathdistance)
        VALUES
          ((SELECT cvterm_id FROM chado.cvterm 
            WHERE name='$path_type' 
                  AND cv_id=(SELECT cv_id FROM cv 
                             WHERE name='relationship')),
           $trait_id,
           $object_id,
           (SELECT cv_id FROM chado.cv WHERE name='LegumeInfo:traits'),
           1)";
      logSQL($dataset_name, "$line_count: $sql");
      doQuery($dbh, $sql);
    }#insert cvtermpath
     
  }#value for property exists
}#setTermRelationship


sub setTraitRecord {
  my ($dbh, $trait_id, $name, $definition) = @_;
  my ($sql, $sth, $row);
  
  # create dbxref
  my $dbxref_id = setDbxref($dbh, $name);
  
  $name       = $dbh->quote($name);
  $definition = $dbh->quote($definition);

  if ($trait_id) {
    $sql = "
      UPDATE chado.cvterm SET
        name=$name,
        definition=$definition,
        dbxref_id=$dbxref_id
      WHERE cvterm_id=$trait_id
      RETURNING cvterm_id";
  }
  else {
    $sql = "
      INSERT INTO chado.cvterm
        (cv_id, name, definition, dbxref_id)
      VALUES
        ((SELECT cv_id FROM chado.cv WHERE name='LegumeInfo:traits'),
         $name, $definition, $dbxref_id)
      RETURNING cvterm_id";
  }
  
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);
  $row = $sth->fetchrow_hashref;
  
  return $row->{'cvterm_id'};
}#setTraitRecord

