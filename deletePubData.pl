####
##	DATE: JANUARY 20 2015
##	AUTHOR: VIJAY SHANKER KARINGULA
##	PURPOSE: THIS CODE DELETES PUBLICATION DATA FROM DATABASE FOR ANY GIVEN CITATION NAME
####


#!usr/bin/perl

  use strict;
  use warnings;
  use DBI;
  
  my $warn =<<EOS
  Usage:  Below is an example scenario--
  
  Please Enter the name of Citation:
  Galeano, Fernandez et al., 2011b
  
EOS
;
  my $citation=$ENV{"citation"}; #Exporting the Citation name from shell to Perl
  chomp $citation;
  if (length $citation==0) {
	print "You haven't given any Citation name. Please enter the valid Citation name\n";
	die $warn;
  }

  #Connecting to the database
  my $driver="Pg";
  my $database="drupal";
  my $dsn="DBI:$driver:dbname=$database";
  my $userid='';
  my $password='';
  my $dbh=DBI->connect($dsn,$userid,$password,{RaiseError=>1, PrintError=>0, ShowErrorStatement=>1, AutoCommit=>0}); #AutoCommit=0 enables the transactions

  my $sql = "SET SEARCH_PATH=chado";# Setting to Chado Schema
  my $sth=$dbh->prepare($sql);
  $sth->execute();

  my @row;
  #Getting the publication id of the citation.
  my $pub_id=$dbh->selectrow_array("select pub_id from pub where uniquename='$citation'");
  if (not defined $pub_id) {
	print "Please enter the valid Citation Name:\n";
	$dbh->disconnect();
	die $warn;
  }
    
  #### DELETING QTL DATA ####

  #Getting all QTL feature_ids for the publication.
  $sql="SELECT DISTINCT(q.feature_id) FROM feature q
  INNER JOIN featureloc ql ON ql.feature_id=q.feature_id
  INNER JOIN feature l ON l.feature_id=ql.srcfeature_id
  INNER JOIN featurepos fp ON fp.feature_id=l.feature_id
  INNER JOIN featuremap m ON m.featuremap_id=fp.featuremap_id
  INNER JOIN featuremap_pub mp ON mp.featuremap_id=fp.featuremap_id
  WHERE mp.pub_id='$pub_id'
  AND q.type_id=(SELECT cvterm_id FROM cvterm WHERE name='QTL')";
  $sth=$dbh->prepare($sql);
  my $rv=$sth->execute();
  if ($rv<0) {
	print $DBI::errstr;
  }
  
  #For each QTL id, Loop through the following Delete Statements
  while(@row=$sth->fetchrow_array()){
	my $qtl_id=$row[0]; # Storing feature_id as $qtl_id every time
	
	#Transaction Begins
	eval{
		$dbh->do("DELETE FROM feature_project WHERE feature_id='$qtl_id'");
		
		$dbh->do("DELETE FROM feature_stock WHERE feature_id='$qtl_id'");
		
		$dbh->do("DELETE FROM feature_relationship WHERE subject_id='$qtl_id' OR object_id='$qtl_id'");
		
		$dbh->do("DELETE FROM feature_cvterm WHERE feature_id='$qtl_id'");
		
		$dbh->do("DELETE FROM featurepos WHERE feature_id='$qtl_id'");
		
		$dbh->do("DELETE FROM featureprop WHERE feature_id='$qtl_id'");
		
		$dbh->do("DELETE FROM featureloc WHERE feature_id='$qtl_id'");
		
		$dbh->do("DELETE FROM analysisfeature WHERE feature_id='$qtl_id'");
		
		$dbh->do("DELETE FROM synonym
			 WHERE synonym_id IN
			                    (SELECT synonym_id FROM feature_synonym WHERE feature_id='$qtl_id')");
		
		$dbh->do("DELETE FROM feature WHERE feature_id='$qtl_id'");
		
		$dbh->do("DELETE FROM dbxref x
			 USING feature_dbxref fx
			 WHERE fx.dbxref_id = x.dbxref_id
			 AND fx.feature_id='$qtl_id'");
	};# Transaction Ends
	
	#Error Handling & Rolling Back entire transaction if any error in the above SQL statements
	if ($@){
		local $dbh->{RaiseError}=0;
		print "Transaction aborted:$@";
		$dbh->rollback();
		}
  }

  ####  DELETING MAP SET DATA  ####
  
  #Getting all featuremap_ids for the given pub_id of Citation
  $sql=
  "SELECT m.featuremap_id FROM featuremap m
  INNER JOIN featuremap_pub mp ON mp.featuremap_id=m.featuremap_id
  WHERE mp.pub_id='$pub_id'";
  $sth=$dbh->prepare($sql);
  $sth->execute();
  
  #For each featuremap_id, Loop through the following Delete Statements
  while(@row=$sth->fetchrow_array){
	my $map_set_id=$row[0]; #Storing featuremap_id as $map_set_id every time
	
	# Keeping a count of records in the Stock table corresponding to the above featuremap_id(map_set_id)
  	my $dbxref_count=$dbh->selectrow_array("select count(*) from stock s 
							INNER JOIN featuremap_stock fs 
                                                        ON fs.stock_id=s.stock_id 
                                                        WHERE fs.featuremap_id='$map_set_id'");
	
	#Transaction Begins
	eval{
		$dbh->do("DELETE FROM feature l USING featurepos lp WHERE lp.feature_id=l.feature_id AND lp.featuremap_id='$map_set_id'");
		
		$dbh->do("DELETE FROM dbxref WHERE dbxref_id IN(SELECT dbxref_id FROM featuremap_dbxref WHERE featuremap_id ='$map_set_id')");
		
		if($dbxref_count==1){
		$dbh->do("DELETE from stock s USING featuremap_stock fs
	                        WHERE fs.stock_id=s.stock_id 
	                        AND fs.featuremap_id='$map_set_id'");
		}
		
		$dbh->do("DELETE FROM featuremap_dbxref where featuremap_id='$map_set_id'");
		
		$dbh->do("DELETE FROM featurepos where featuremap_id='$map_set_id'");	
		
	}; #Transaction Ends
	
	#Error Handling & Rolling Back entire transaction if any error in the above SQL statements
	if ($@){
		local $dbh->{RaiseError}=0;
		print "Transaction aborted:$@";
		$dbh->rollback();
        }
  }

  ####  DELETING EXPERIMENT DATA  ####
  
  #Getting all project_ids for the given pub_id of Citation
  $sql="select p.project_id from project p 
      inner join project_pub pp 
      on pp.project_id=p.project_id 
      where pp.pub_id='$pub_id'";
      $sth=$dbh->prepare($sql);
      $sth->execute();
      
   #For each project_id, Loop through the following Delete Statements   
  while(@row=$sth->fetchrow_array){
  my $experiment_id=$row[0]; #Storing project_id as $experiment_id every time
  
  #Transaction Begins
  eval{
	$dbh->do("DELETE from projectprop where project_id='$experiment_id'");
	$dbh->do("DELETE from nd_experiment_project where project_id='$experiment_id'");
	$dbh->do("DELETE from project_pub where project_id='$experiment_id'");
	}; #Transaction Ends
  
  #Error Handling & Rolling Back entire transaction if any error in the above SQL statements
  if ($@){
	local $dbh->{RaiseError}=0;
	print "Transaction aborted:$@";
	$dbh->rollback();
	}
  }

  ####  DELETING PUBLICATION DATA  ####
  
  #Transaction Begins
  eval{
	$dbh->do("DELETE from pub_dbxref where pub_id='$pub_id'");
	$dbh->do("DELETE from pubprop where pub_id='$pub_id'");
	$dbh->do("DELETE from pubauthor where pub_id='$pub_id'");
  }; #Transaction Ends
  
  #Error Handling & Rolling Back entire transaction if any error in the above SQL statements
  if ($@){
	local $dbh->{RaiseError}=0;
        print "Transaction aborted:$@";
        $dbh->rollback();
  }

  $sth->finish();
  $dbh->commit;
  $dbh->disconnect();

