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

  
  my $citation;
  my $warn =<<EOS
  >>Usage:
  
  perl deletePubData.pl "<citation name>"
     (OR)
  perl deletePubData.pl "<publication_id>" (Quotations are optional for publication_id)
  
  Below is an example scenario--
  
  perl deletePubData.pl "Galeano, Fernandez et al., 2011b"
                  (OR)
  perl deletePubData.pl "90"
  
EOS
;

  if (not defined $ARGV[0]) {
    print "You haven't given any Citation name or Publication Id".
    ". Please enter the valid Citation name or Publication Id\n";
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
    $pub_id = $dbh->selectrow_array("SELECT pub_id FROM pub WHERE pub_id ='$citation'");
  }
  else{
    $pub_id = $dbh->selectrow_array("SELECT pub_id FROM pub WHERE uniquename = '$citation'");
  }
  if (not defined $pub_id) {
    print "Please enter the valid Citation Name or Publication Id:\n";
    $dbh->disconnect();
    die $warn;
  }
  print "Publication Id:$pub_id\n";
    
  #### DELETING QTL DATA ####

  #Getting all QTL feature_ids for the publication.
  $sql="SELECT DISTINCT(q.feature_id) FROM feature q
    INNER JOIN featureloc ql ON ql.feature_id = q.feature_id
    INNER JOIN feature l ON l.feature_id = ql.srcfeature_id
    INNER JOIN featurepos fp ON fp.feature_id = l.feature_id
    INNER JOIN featuremap m ON m.featuremap_id = fp.featuremap_id
    INNER JOIN featuremap_pub mp ON mp.featuremap_id = fp.featuremap_id
    WHERE mp.pub_id = '$pub_id'
    AND q.type_id = (SELECT cvterm_id FROM cvterm WHERE name = 'QTL')";
  print "$sql\n";
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
    eval{
#eksc: Odd problems accessing this table from the script. Since the records in
#      in this table will be deleted when the referenced feature and project
#      records are deleted, can leave out this statement.
#      $dbh->do("DELETE FROM feature_project WHERE feature_id='$qtl_id'");
      
      $dbh->do("DELETE FROM feature_stock WHERE feature_id ='$qtl_id'");
      
      $dbh->do("DELETE FROM feature_relationship 
                WHERE subject_id ='$qtl_id' OR object_id ='$qtl_id'");
      
      $dbh->do("DELETE FROM feature_cvterm WHERE feature_id ='$qtl_id'");

      $dbh->do("DELETE FROM featurepos WHERE feature_id ='$qtl_id'");
      
      $dbh->do("DELETE FROM featureprop WHERE feature_id ='$qtl_id'");
      
      $dbh->do("DELETE FROM featureloc WHERE feature_id ='$qtl_id'");
      
      $dbh->do("DELETE FROM analysisfeature WHERE feature_id ='$qtl_id'");
      
      $dbh->do("DELETE FROM synonym
         WHERE synonym_id IN
           (SELECT synonym_id FROM feature_synonym WHERE feature_id ='$qtl_id')");
      
      $dbh->do("DELETE FROM feature WHERE feature_id ='$qtl_id'");
      
      $sql ="DELETE FROM dbxref x
             USING feature_dbxref fx
             WHERE fx.dbxref_id = x.dbxref_id
                   AND fx.feature_id = '$qtl_id'
                   AND x.db_id IN (SELECT db_id FROM db 
                                   WHERE name = 'LIS:cmap')";
      print "$sql\n";
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
	"SELECT m.featuremap_id FROM featuremap m
	 INNER JOIN featuremap_pub mp ON mp.featuremap_id = m.featuremap_id
	 WHERE mp.pub_id = '$pub_id'";
  print "$sql\n";
  $sth=$dbh->prepare($sql);
  $sth->execute();
  
  #For each featuremap_id, Loop through the following Delete Statements
  while(my $row=$sth->fetchrow_hashref()){
    my $map_set_id=$row->{'featuremap_id'}; #Storing featuremap_id as $map_set_id every time
    print "\nDeleting data for map set $map_set_id\n";
    
    # Keeping a count of records in the Stock table corresponding to the above featuremap_id(map_set_id)
    my $stock_count=$dbh->selectrow_array("
      SELECT COUNT(*) FROM stock s 
        INNER JOIN featuremap_stock fs ON fs.stock_id = s.stock_id 
      WHERE fs.featuremap_id = '$map_set_id'");
    print "  There are $stock_count stock record(s) associated with this map set.\n";
  
    $sql = "
       SELECT COUNT(*) FROM feature l, featurepos lp 
         WHERE lp.feature_id = l.feature_id 
               AND lp.featuremap_id = '$map_set_id'
               AND l.type_id = (SELECT cvterm_id FROM cvterm 
                                WHERE name = 'linkage_group' 
                                      AND cv_id = (SELECT cv_id FROM cv 
                                                   WHERE name = 'sequence')
                                )";
    print "$sql\n";
    my $lg_count = $dbh->selectrow_array($sql);
    print "Deleting $lg_count linkage groups\n";
    
    #Transaction Begins
    eval{
        $sql=
        "DELETE FROM featureprop
         WHERE feature_id IN
	                    (SELECT l.feature_id FROM feature l
                             INNER JOIN featurepos lp ON lp.feature_id = l.feature_id
                             AND lp.featuremap_id = '$map_set_id'
                             AND l.type_id=
                                          (SELECT cvterm_id FROM cvterm
                                           WHERE name ='linkage_group'
                                           AND cv_id =
                                                      (SELECT cv_id FROM cv WHERE name = 'sequence')))";
        print "Deleting Assigned Linkage Groups(feature_property)";					      
        print "$sql\n";
	      $dbh->do($sql);
        $sql = 
        "DELETE FROM feature l 
         USING featurepos lp 
         WHERE lp.feature_id = l.feature_id 
               AND lp.featuremap_id = '$map_set_id'
               AND l.type_id = (SELECT cvterm_id FROM cvterm 
                                WHERE name = 'linkage_group' 
                                      AND cv_id = (SELECT cv_id FROM cv 
                                                   WHERE name = 'sequence')
                                )";
        print "$sql\n";
        $dbh->do($sql);
        
        $dbh->do("DELETE FROM dbxref 
                  WHERE dbxref_id IN(SELECT dbxref_id 
                                     FROM featuremap_dbxref 
                                     WHERE featuremap_id = '$map_set_id')");
        
        if($stock_count==1){
        $dbh->do("DELETE from stock s USING featuremap_stock fs
                  WHERE fs.stock_id = s.stock_id 
                        AND fs.featuremap_id = '$map_set_id'");
      }
      
      $dbh->do("DELETE FROM featuremap_dbxref where featuremap_id = '$map_set_id'");
      
      $dbh->do("DELETE FROM featurepos where featuremap_id = '$map_set_id'");  
      
      $sql = "DELETE FROM featuremap WHERE featuremap_id = $map_set_id";
      print "$sql\n";
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
  $sql="SELECT p.project_id FROM project p 
      INNER JOIN project_pub pp 
      ON pp.project_id = p.project_id 
      WHERE pp.pub_id = '$pub_id'";
  print "$sql\n";
  $sth = $dbh->prepare($sql);
  $sth->execute();
  
  #For each project_id, Loop through the following Delete Statements   
  while(my $row=$sth->fetchrow_hashref()){
        my $experiment_id = $row->{'project_id'}; #Storing project_id as $experiment_id every time
        print "Delete experiment $experiment_id\n";
	
        #Transaction Begins
        eval{
                $dbh->do("DELETE from projectprop where project_id = '$experiment_id'");
                $dbh->do("DELETE from nd_experiment_project where project_id = '$experiment_id'");
                $dbh->do("DELETE from project_pub where project_id = '$experiment_id'");
                $sql = "DELETE FROM project WHERE project_id = $experiment_id";
                print "$sql\n";
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
	  $dbh->do("DELETE FROM dbxref WHERE dbxref_id IN
		            (SELECT dbxref_id from pub_dbxref WHERE pub_id = '$pub_id')");
    $dbh->do("DELETE from pub_dbxref where pub_id = '$pub_id'");
    $dbh->do("DELETE from pubprop where pub_id = '$pub_id'");
    $dbh->do("DELETE from pubauthor where pub_id = '$pub_id'");
    $sql = "DELETE FROM pub WHERE pub_id = $pub_id";
    print "$sql\n";
    $dbh->do($sql);
  }; #Transaction Ends
  
  #Error Handling & Rolling Back entire transaction if any error in the above SQL statements
  if ($@){
    local $dbh->{RaiseError} = 0;
    print "Transaction aborted:$@";
    $dbh->rollback();
  }
  
  $sql="DELETE FROM dbxref x WHERE x.dbxref_id NOT IN
         (SELECT fx.dbxref_id FROM feature_dbxref fx
         INNER JOIN feature_project fpr ON fpr.feature_id = fx.feature_id
         INNER JOIN project pr ON pr.project_id = fpr.project_id
         INNER JOIN project_pub pp ON pp.project_id = pr.project_id
         INNER JOIN pub p ON p.pub_id = pp.pub_id WHERE p.pub_id = '$pub_id' )
        AND
        x.dbxref_id NOT IN
          (SELECT fd.dbxref_id FROM featuremap_dbxref fd
          INNER JOIN featuremap_pub fb ON fb.featuremap_id = fd.featuremap_id
          WHERE pub_id = '$pub_id')
        AND
        x.db_id = (SELECT d.db_id FROM db d WHERE d.name = 'LIS:cmap');";

  print "$sql\n";
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
