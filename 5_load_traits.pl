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
  
  use Getopt::Std;
  
  # load local util library
  use File::Spec::Functions qw(rel2abs);
  use File::Basename;
  use lib dirname(rel2abs($0));
  use CropLegumeBaseLoaderUtils;
  
  my $warn = <<EOS
    Usage:
      
    $0 [opts] data-dir
    -w [optional] worksheet name
EOS
;
  my $worksheet = 'Traits';
  my %cmd_opts = ();
  getopts("w:", \%cmd_opts);
  if (defined($cmd_opts{'w'})) { $worksheet = $cmd_opts{'w'}; } 

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
  
  # Get connected
  my $dbh = connectToDB;

  # set default schema
  $sql = "SET SEARCH_PATH = chado";
  $sth = $dbh->prepare($sql);
  $sth->execute();

  # Holds traits that are already in db
  my %existing_traits = getExistingTraits($dbh);
#foreach my $key (keys %existing_traits) {
#  print "$key = " . $existing_traits{$key} . "\n";
#}
#exit;

  # Use a transaction so that it can be rolled back if there are any errors
  eval {
    loadTraits($dbh, $worksheet);

    $dbh->commit;   # commit the changes if we get this far
  };
  if ($@) {
    print "\n\nTransaction aborted because $@\n\n";
    # now rollback to undo the incomplete changes
    # but do it in an eval{} as it may also fail
    eval { $dbh->rollback };
  }

  # Check for traits in db but not in spread sheet
  print "\n\nChecking for orphaned traits...\n";
  my $orphaned_traits = 0;
  foreach my $trait (keys %existing_traits) {
    if ($existing_traits{$trait} ne 'found') {
      print "ORPHANED: " . $existing_traits{$trait} . " $trait\n";
      $orphaned_traits++;
    }
  }#each existing trait in db
  if ($orphaned_traits == 0) {
    print "None found.\n\n";
  }

  # ALL DONE
  print "\n\nScript completed\n\n";



################################################################################
####### Major functions                                                #########
################################################################################


sub loadTraits {
  my ($dbh, , $worksheet) = @_;
  my ($fields, $sql, $sth);
  
  $table_file = "$input_dir/$worksheet.txt";
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

      # trait exists
      next if ($skip_all);
      
      if ($update_all) {
        # mark this one as found in spreadsheet
        $existing_traits{$qtl_symbol} = 'found';
        
        # Get set to update
        cleanDependants($trait_id);
      }
      else {
        my $prompt = "$line_count: QTL symbol ($qtl_symbol) = $trait_id) ";
        ($skip, $skip_all, $update, $update_all) = checkUpdate($prompt);
        
        next if ($skip || $skip_all);
        
        if ($update || $update_all) {
          # mark this one as found in spreadsheet
          $existing_traits{$qtl_symbol} = 'found';
          
          # remove dependent records; they will be re-inserted
          cleanDependants($trait_id);
        }
      }#update_all not set
    }#trait exists

    # insert a parent record for this QTL Symbol
    $trait_id = setTraitRecord($dbh, $trait_id, 
                               $fields->{$ti{'qtl_symbol_fld'}}, 
                               $fields->{$ti{'description_fld'}});
#print "  trait ID: $trait_id\n";
    
    # trait_name 
    # subject, object, relationship, pathtype (for cvtermpath)
    setTermRelationship($dbh, $fields->{$ti{'trait_class_fld'}}, $fields->{$ti{'qtl_symbol_fld'}}, 
                        'Has Trait Name', 'contains', $fields);
    
    # trait_class and trait_unit: cvterm_relationship
    setTermRelationship($dbh, $fields->{$ti{'qtl_symbol_fld'}}, $fields->{$ti{'trait_class_fld'}}, 
                        'Has Trait Class', 'is_a', $fields);

    # OBO term
    setOBOTerms($dbh, $trait_id, $fields);
  }#each record

  print "Loaded $line_count obs. trait records\n\n";
}#loadTraits
  


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
  
  my $sql = "DELETE FROM chado.cvterm_relationship WHERE object_id=$trait_id";
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
      
      # verify that ontology exists
      $sql = "SELECT db_id FROM db WHERE name='$cv'";
      logSQL('', "$line_count: $sql");
      $sth = doQuery($dbh, $sql);
      if (!($row = $sth->fetchrow_hashref)) {
        print "\nERROR: no ontology loaded for $cv. Quitting.\n\n";
        exit;
      }
      $sql = "
        SELECT dbxref_id FROM chado.dbxref 
        WHERE db_id = " . $row->{'db_id'} . "
              AND accession = '$acc'";
    }
    else {
      # Assume this is a LegumeInfo trait
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
  

sub getExistingTraits {
  my $dbh = $_[0];
  my ($sql, $sth, $row);
  
  my %traits;
  $sql = "
    SELECT t.name, cvterm_id FROM cvterm t
      INNER JOIN dbxref dx ON dx.dbxref_id=t.dbxref_id
      INNER JOIN db d ON d.db_id=dx.db_id
    WHERE d.name='LegumeInfo:traits'
          AND t.cvterm_id NOT IN (
            SELECT t.cvterm_id 
            FROM cvterm t
              INNER JOIN dbxref dx ON dx.dbxref_id=t.dbxref_id
              INNER JOIN db d ON d.db_id=dx.db_id
              LEFT JOIN cvterm_relationship cr 
                ON cr.subject_id=t.cvterm_id
              LEFT JOIN cvterm ty on ty.cvterm_id=cr.type_id
            WHERE d.name='LegumeInfo:traits'
                  AND ty.name='Has Trait Name')";
  $sth = doQuery($dbh, $sql);
  while ($row=$sth->fetchrow_hashref) {
    $traits{$row->{'name'}} = $row->{'cvterm_id'};
  }
  
  return %traits;
}#getExistingTraits


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


sub setOBOTerms {
  my ($dbh, $trait_id, $fields) = @_;
  # wait on this one: 'IBP_Accession_GN'
  my @OBO_cols = ('Soybase_Accession', 'TO_Accession');
  
  foreach my $OBO_col (@OBO_cols) {
    my $term = $fields->{$OBO_col};
  
    return if (!$term || $term eq '' || lc($term) eq 'null');
  
    $term =~ /(.*?):(\d+)/;
    my $dbname = $1;
    my $acc = $2;
print "set term $term: accession: $acc, db: $dbname\n";

    # check for existence
    my $dbxref_id = getDbxref($dbh, $term);
    if ($dbxref_id) {
      # OBO term exists, attach it to this trait
      $sql = "
        INSERT INTO chado.cvterm_dbxref
          (cvterm_id, dbxref_id)
        VALUES
          ($trait_id, $dbxref_id)";
      logSQL($dataset_name, "$line_count: $sql");
      $sth = doQuery($dbh, $sql);
    }
  }#each OBO column
}#setOBOTerms


sub setTermRelationship {
  my ($dbh, $subject, $object, $relationship, $path_type, $fields) = @_;
  my ($sql, $sth, $row);
  
  if (!$subject || $subject eq '' || lc($subject) eq 'null'
        || !$object || $object eq '' || lc($object) eq 'null') {
    # Nothing to do
    print "Warning: missing subject or object: [$subject], [$object]\n";
    return;
  }
  
  # Get subject term id
print "Get id for subject $subject\n";
  my $subject_id;
  $sql = "
    SELECT cvterm_id FROM chado.cvterm
    WHERE name='$subject'
          AND cv_id = (SELECT cv_id FROM chado.cv 
                         WHERE name='LegumeInfo:traits')";
  logSQL($dataset_name, "$line_count: $sql");
  $sth = doQuery($dbh, $sql);
  if ($row=$sth->fetchrow_hashref) {
print "row: $row\n";
    $subject_id = $row->{'cvterm_id'};
  }
  else {
print "no term record for $subject\n";
    if ($path_type eq 'contains') {
      # this is a missing trait class; add it
      my $dbxref_id = setDbxref($dbh, $subject);
print "Got dbxref $dbxref_id\n";
      if (!$dbxref_id) {
        print "\nERROR: unable to find or make dbxref record for $subject\n\n";
        exit;
      }
      
      $sql = "
        INSERT INTO chado.cvterm
          (cv_id, name, dbxref_id)
        VALUES
          ((SELECT cv_id FROM chado.cv WHERE name='LegumeInfo:traits'),
           '$subject',
           $dbxref_id)
        RETURNING cvterm_id";
      logSQL($dataset_name, "$line_count: $sql");
      $sth = doQuery($dbh, $sql);
      $row = $sth->fetchrow_hashref;
      $subject_id = $row->{'cvterm_id'};
print "term record inserted\n";
    }
    else {
      print "\nERROR: missing subject term: $subject. Unable to continue.\n\n";
      exit;
    }
  }#get subject id
  
  # Get object term id
  my $object_id;
  if ($object && $object ne '' && lc($object) ne 'null') {
    # Does this related term exist?
    $sql = "
      SELECT cvterm_id FROM chado.cvterm 
      WHERE name='$object' 
            AND cv_id = (SELECT cv_id FROM chado.cv 
                         WHERE name='LegumeInfo:traits')";
    logSQL($dataset_name, "$line_count: $sql");
    $sth = doQuery($dbh, $sql);
    if ($row=$sth->fetchrow_hashref) {
      $object_id = $row->{'cvterm_id'};
    }
    else {
      if ($path_type eq 'is_a') {
        # Term doesn't exist, create it (this should be a trait class)
        my $dbxref_id = setDbxref($dbh, $object);
        $sql = "
          INSERT INTO chado.cvterm
            (cv_id, name, dbxref_id)
          VALUES
            ((SELECT cv_id FROM chado.cv WHERE name='LegumeInfo:traits'),
             '$object',
             $dbxref_id)
          RETURNING cvterm_id";
        logSQL($dataset_name, "$line_count: $sql");
        $sth = doQuery($dbh, $sql);
        $row = $sth->fetchrow_hashref;
        $object_id = $row->{'cvterm_id'};
      }
      else {
        print "\nERROR: missing subject term: $subject. Unable to continue.\n\n";
        exit;
      }
    }#get object id
     
    # Indicate relationship between subject and object
    $sql = "
      SELECT cvterm_relationship_id FROM chado.cvterm_relationship
      WHERE subject_id=$subject_id
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
          ($subject_id,
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
             AND subject_id=$subject_id
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
           $subject_id,
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

