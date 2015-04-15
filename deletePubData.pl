####                                                            ####
##  DATE: JANUARY 20 2015                                         ##
##  AUTHOR: VIJAY SHANKER KARINGULA                               ##
##  PURPOSE: THIS CODE DELETES PUBLICATION DATA FROM DATABASE FOR
##           ANY GIVEN CITATION NAME (or) PUBLICATION ID          ##
####                                                            ####


#!usr/bin/perl

  use strict;
  use warnings;
  use DBI;
  use Data::Dumper;   # a very handy simple debugging tool

  # Load local util library
  use File::Spec::Functions qw(rel2abs);
  use File::Basename;
  use lib dirname(rel2abs($0));
  use CropLegumeBaseLoaderUtils;
  
  my $citation;
  my $warn =<<EOS
  Usage:
  
  perl deletePubData.pl "<citation name>"
     (OR)
  perl deletePubData.pl "<publication_id>" (Quotations are optional for publication_id)
  
  Below is an example scenario--
  
  perl deletePubData.pl "Galeano, Fernandez et al., 2011b"
                  (OR)
  perl deletePubData.pl "90"
  
EOS
;

  if ((scalar @ARGV) < 1) {
    print "You haven't given a citation name or Publication ID.".
    " Please enter a valid Citation name or Publication ID.\n";
    die $warn;
  }
  else{
    $citation = $ARGV[0];
    chomp $citation;
  }

  #Connecting to the database
  my $driver = "Pg";
  my $database = "drupal";
  my $dsn = "DBI:$driver:dbname=$database";
  my $userid = '';
  my $password = '';
  my $dbh=DBI->connect($dsn,$userid,$password,{RaiseError=>1, PrintError=>0, ShowErrorStatement=>1, AutoCommit=>0}); #AutoCommit=0 enables the transactions

  my $sql = "SET SEARCH_PATH=chado";# Setting to Chado Schema
  my $sth = $dbh->prepare($sql);
  $sth->execute();

  my @row;
  my $pub_id;
  
  #Getting the publication id of the citation.
  if ($citation =~ /^[+-]?\d+$/) { #Checking if the given input is a numeric value
    $sql = "SELECT pub_id FROM pub WHERE pub_id ='$citation'";
    $pub_id = $dbh->selectrow_array($sql);
    logSQL('deletePub', "$sql");
  }
  else{
    $sql = "SELECT pub_id FROM pub WHERE uniquename = '$citation'";
    $pub_id = $dbh->selectrow_array();
    logSQL('deletePub', "$sql");
  }
  if (!$pub_id) {
    print "\nERROR: This citation or pub ID does not exist. ";
    print "Please enter a valid citation name or publication ID.\n\n";
    $dbh->disconnect();
    die $warn;
  }
  print "Publication ID: $pub_id\n";
    
  #### DELETING QTL DATA ####

  #Getting all QTL feature_ids for the publication.
  $sql = "
    SELECT DISTINCT(q.feature_id) FROM feature q
      INNER JOIN feature_project fp ON fp.feature_id=q.feature_id
      INNER JOIN project p ON p.project_id=fp.project_id
      INNER JOIN project_pub pp ON pp.project_id=p.project_id
      INNER JOIN pub ON pub.pub_id=pp.pub_id
    WHERE pub.pub_id = $pub_id
      AND q.type_id = (SELECT cvterm_id FROM cvterm WHERE name = 'QTL')";
  logSQL('deletePub', "$sql");
  $sth = $dbh->prepare($sql);
  my $rv = $sth->execute();
  if ($rv<0) {
    print $DBI::errstr;
  }
  print ("\nFound " . $sth->rows . " QTL records for this publication.\n\n");
  
  #For each QTL id, Loop through the following Delete Statements
  while (my $row = $sth->fetchrow_hashref()){
  my $qtl_id = $row->{'feature_id'}; # Storing feature_id as $qtl_id every time
  print "Deleting qtl $qtl_id\n";
    
  #Transaction Begins
  eval {
#eksc: Odd problems accessing these tables from the script. Since the records in
#      in this table will be deleted when the referenced feature and project
#      records are deleted, can leave out this statement.
#      $dbh->do("DELETE FROM feature_project WHERE feature_id='$qtl_id'");
#      
      $sql = "DELETE FROM chado.feature_stock WHERE feature_id ='$qtl_id'";
      logSQL('deletePub', "$sql");
      $dbh->do($sql);
      
      $sql = "DELETE FROM chado.feature_relationship 
                WHERE subject_id ='$qtl_id' OR object_id ='$qtl_id'";
      logSQL('deletePub', "$sql");
      $dbh->do($sql);
      
      $sql = "DELETE FROM chado.feature_cvterm WHERE feature_id ='$qtl_id'";
      logSQL('deletePub', "$sql");
      $dbh->do($sql);

      $sql = "DELETE FROM chado.featurepos WHERE feature_id ='$qtl_id'";
      logSQL('deletePub', "$sql");
      $dbh->do($sql);
      
      $sql = "DELETE FROM chado.featureprop WHERE feature_id ='$qtl_id'";
      logSQL('deletePub', "$sql");
      $dbh->do($sql);
      
      $sql = "DELETE FROM chado.featureloc WHERE feature_id ='$qtl_id'";
      logSQL('deletePub', "$sql");
      $dbh->do($sql);
      
      $sql = "DELETE FROM chado.analysisfeature WHERE feature_id ='$qtl_id'";
      logSQL('deletePub', "$sql");
      $dbh->do($sql);
      
      $sql = "
        DELETE FROM chado.synonym
        WHERE synonym_id IN
           (SELECT synonym_id FROM feature_synonym 
            WHERE feature_id ='$qtl_id')";
      logSQL('deletePub', "$sql");
      $dbh->do($sql);
      
      $sql = "DELETE FROM chado.feature WHERE feature_id ='$qtl_id'";
      logSQL('deletePub', "$sql");
      $dbh->do($sql);
      
      $sql ="DELETE FROM chado.dbxref x
             USING feature_dbxref fx
             WHERE fx.dbxref_id = x.dbxref_id
                   AND fx.feature_id = '$qtl_id'
                   AND x.db_id IN (SELECT db_id FROM chado.db 
                                   WHERE name = 'LIS:cmap')";
      logSQL('deletePub', "$sql");
      $dbh->do($sql);
    };# Transaction Ends

    #Error Handling & Rolling Back entire transaction if any error in the above SQL statements
    if ($@){
      local $dbh->{RaiseError} = 0;
      print "Transaction aborted:$@";
      $dbh->rollback();
    }
  }#each QTL id
  
  

  ####  DELETING MAP SET DATA  ####
  
  #Getting all featuremap_ids for the given pub_id of Citation
  $sql=
	"SELECT m.featuremap_id FROM chado.featuremap m
	 INNER JOIN chado.featuremap_pub mp ON mp.featuremap_id = m.featuremap_id
	 WHERE mp.pub_id = '$pub_id'";
  logSQL('deletePub', "$sql");
  $sth=$dbh->prepare($sql);
  $sth->execute();
  
  #For each featuremap_id, Loop through the following Delete Statements
  while(my $row=$sth->fetchrow_hashref()){
    my $map_set_id=$row->{'featuremap_id'}; #Storing featuremap_id as $map_set_id every time
    print "\nDeleting data for map set $map_set_id\n";
    
    # Keeping a count of records in the Stock table corresponding to the above featuremap_id(map_set_id)
    my $stock_count=$dbh->selectrow_array("
      SELECT COUNT(*) FROM chado.stock s 
        INNER JOIN chado.featuremap_stock fs ON fs.stock_id = s.stock_id 
      WHERE fs.featuremap_id = '$map_set_id'");
    print "  There are $stock_count stock record(s) associated with this map set.\n";
  
    $sql = "
       SELECT COUNT(*) FROM chado.feature l, chado.featurepos lp 
         WHERE lp.feature_id = l.feature_id 
               AND lp.featuremap_id = '$map_set_id'
               AND l.type_id = (SELECT cvterm_id FROM cvterm 
                                WHERE name = 'linkage_group' 
                                      AND cv_id = (SELECT cv_id FROM cv 
                                                   WHERE name = 'sequence')
                                )";
    logSQL('deletePub', "$sql");
    my $lg_count = $dbh->selectrow_array($sql);
    print "Deleting $lg_count linkage groups\n";
    
    #Transaction Begins
    eval{
        $sql=
        "DELETE FROM chado.featureprop
         WHERE feature_id IN
	                    (SELECT l.feature_id FROM chado.feature l
                             INNER JOIN featurepos lp ON lp.feature_id = l.feature_id
                             AND lp.featuremap_id = '$map_set_id'
                             AND l.type_id=
                                          (SELECT cvterm_id FROM chado.cvterm
                                           WHERE name ='linkage_group'
                                           AND cv_id =
                                                      (SELECT cv_id FROM chado.cv WHERE name = 'sequence')))";
        print "Deleting Assigned Linkage Groups(feature_property)";					      
        logSQL('deletePub', "$sql");
	      $dbh->do($sql);
        $sql = 
        "DELETE FROM chado.feature l 
         USING featurepos lp 
         WHERE lp.feature_id = l.feature_id 
               AND lp.featuremap_id = '$map_set_id'
               AND l.type_id = (SELECT cvterm_id FROM chado.cvterm 
                                WHERE name = 'linkage_group' 
                                      AND cv_id = (SELECT cv_id FROM chado.cv 
                                                   WHERE name = 'sequence')
                                )";
        logSQL('deletePub', "$sql");
        $dbh->do($sql);
        
        $sql = "
          DELETE FROM chado.dbxref 
          WHERE dbxref_id IN(SELECT dbxref_id 
                             FROM chado.featuremap_dbxref 
                             WHERE featuremap_id = '$map_set_id')";
        logSQL('deletePub', "$sql");
        $dbh->do($sql);
        
        if ($stock_count==1) {
          $sql = "
            DELETE from chado.stock s USING featuremap_stock fs
            WHERE fs.stock_id = s.stock_id 
                  AND fs.featuremap_id = '$map_set_id'";
          logSQL('deletePub', "$sql");
          $dbh->do($sql);
      }
      
      $sql = "DELETE FROM chado.featuremap_dbxref where featuremap_id = '$map_set_id'";
      logSQL('deletePub', "$sql");
      $dbh->do($sql);
      
      $sql = "DELETE FROM chado.featurepos where featuremap_id = '$map_set_id'";
      logSQL('deletePub', "$sql");
      $dbh->do($sql);  
      
      $sql = "DELETE FROM chado.featuremap WHERE featuremap_id = $map_set_id";
      logSQL('deletePub', "$sql");
      $dbh->do($sql);
    }; #Transaction Ends
    
    #Error Handling & Rolling Back entire transaction if any error in the above SQL statements
    if ($@){
      local $dbh->{RaiseError} = 0;
      print "Transaction aborted:$@";
      $dbh->rollback();
    }
  }#each featuremap record

  ####  DELETING EXPERIMENT DATA  ####
  
  #Getting all project_ids for the given pub_id of Citation
  $sql="SELECT p.project_id FROM chado.project p 
      INNER JOIN chado.project_pub pp 
      ON pp.project_id = p.project_id 
      WHERE pp.pub_id = '$pub_id'";
  logSQL('deletePub', "$sql");
  $sth = $dbh->prepare($sql);
  $sth->execute();
  
  #For each project_id, Loop through the following Delete Statements   
  while(my $row=$sth->fetchrow_hashref()){
        my $experiment_id = $row->{'project_id'}; #Storing project_id as $experiment_id every time
        print "Delete experiment $experiment_id\n";
	
        #Transaction Begins
        eval{
          $sql = "DELETE from chado.projectprop where project_id = '$experiment_id'";
          logSQL('deletePub', "$sql");
          $dbh->do($sql);
          $sql = "DELETE from chado.nd_experiment_project where project_id = '$experiment_id'";
          logSQL('deletePub', "$sql");
          $dbh->do($sql);
          $sql = "DELETE from chado.project_pub where project_id = '$experiment_id'";
          logSQL('deletePub', "$sql");
          $dbh->do($sql);
          $sql = "DELETE FROM chado.project WHERE project_id = $experiment_id";
          logSQL('deletePub', "$sql");
          $dbh->do($sql);
        }; #Transaction Ends
	
        #Error Handling & Rolling Back entire transaction if any error in the above SQL statements
        if ($@){
          local $dbh->{RaiseError} = 0;
          print "Transaction aborted:$@";
          $dbh->rollback();
        }
  }

  ####  DELETING PUBLICATION DATA  ####
  
  #Transaction Begins
  eval{
    $sql = "
      DELETE FROM dbxref 
      WHERE dbxref_id IN
		    (SELECT dbxref_id from chado.pub_dbxref WHERE pub_id = '$pub_id')";
		logSQL('deletePub', "$sql");
	  $dbh->do($sql);
	  
		$sql = "DELETE from chado.pub_dbxref where pub_id = '$pub_id'";
		logSQL('deletePub', "$sql");
    $dbh->do($sql);
    
    $sql = "DELETE from chado.pubprop where pub_id = '$pub_id'";
    logSQL('deletePub', "$sql");
    $dbh->do($sql);
    
    $sql = "DELETE from chado.pubauthor where pub_id = '$pub_id'";
    logSQL('deletePub', "$sql");
    $dbh->do($sql);
    
    $sql = "DELETE FROM chado.pub WHERE pub_id = $pub_id";
    logSQL('deletePub', "$sql");
    $dbh->do($sql);
  }; #Transaction Ends
  
  #Error Handling & Rolling Back entire transaction if any error in the above SQL statements
  if ($@){
    local $dbh->{RaiseError} = 0;
    print "Transaction aborted:$@";
    $dbh->rollback();
  }
  
  $sql="DELETE FROM chado.dbxref x WHERE x.dbxref_id NOT IN
         (SELECT fx.dbxref_id FROM chado.feature_dbxref fx
         INNER JOIN chado.feature_project fpr ON fpr.feature_id = fx.feature_id
         INNER JOIN chado.project pr ON pr.project_id = fpr.project_id
         INNER JOIN chado.project_pub pp ON pp.project_id = pr.project_id
         INNER JOIN chado.pub p ON p.pub_id = pp.pub_id WHERE p.pub_id = '$pub_id' )
        AND
        x.dbxref_id NOT IN
          (SELECT fd.dbxref_id FROM chado.featuremap_dbxref fd
          INNER JOIN chado.featuremap_pub fb ON fb.featuremap_id = fd.featuremap_id
          WHERE pub_id = '$pub_id')
        AND
        x.db_id = (SELECT d.db_id FROM chado.db d WHERE d.name = 'LIS:cmap');";

  logSQL('deletePub', "$sql");
  $sth=$dbh->prepare($sql);
  $sth->execute();

  $sth->finish();
  $dbh->commit;
  $dbh->disconnect();

############################################################################################################################################

#############  CHANGES    ################

## Date: 22 January 2015 -- Changed the input method. Now the script accepts both pub_id or citation_name as command line argument

## Date: 26 January 2015 --
##                       -- Changed the variable name '$dbxref_count' to '$stock_count'
##                       -- Indentation through TAB is changed to indentation through SPACE
##                       -- added a sql statement to delete a record from 'pub' table (DELETION OF PUBLICATION DATA)
##                       -- added a sql statement to delete a record from project table (DELETION OF EXPERIMENT DATA)

## Date: 28 January 2015 --
##                       -- added a sql statement to delete records from dbxref table which are linked to LIS:cmap

## Date: 2 February 2015 --
##                       -- added an sql delete statement to delete Assigned linkage Groups(feature_property) records from featureprop table (DELETION OF MAP_SET_DATA)


##############################################################################################################################################
