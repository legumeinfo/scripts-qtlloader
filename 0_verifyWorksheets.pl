# file: verifyWorksheets.pl
#
# purpose: check spreadsheet worksheets for errors and data integrity
#
# http://search.cpan.org/~capttofu/DBD-mysql-4.022/lib/DBD/mysql.pm
# http://search.cpan.org/~timb/DBI/DBI.pm
# http://perldoc.perl.org/Encode.html
#
# history:
#  05/09/13  eksc  created
#  09/05/14  eksc  modified for latest spreadsheet revisions


  #use strict;
  use DBI;
  use Data::Dumper;
  use Getopt::Std;
  use feature 'unicode_strings';
  #use vars qw($input_dir $dbh);
  
  # Load local util library
  use File::Spec::Functions qw(rel2abs);
  use File::Basename;
  use Path::Class;
  use lib dirname(rel2abs($0));
  use CropLegumeBaseLoaderUtils;
  use HashDB;
  use PubVerif;
  use QtlExpVerif;
  use MasterMarkerVerif;
  use GeneticMapVerif;
  use TraitVerif;
  use QtlVerif;
  
  my $warn = <<EOS
    Usage:
      
    $0 [opts] data-dir
      -p check publication files
      -e check experiment file
      -g check genetic map files
      -m check marker file
      -t check trait files
      -q check QTL file
      -a check all files
      
EOS
;
  if ($#ARGV < 1) {
    die $warn;
  }
  

  # What data sets need verifying?
  my ($do_pubs, $do_experiments, $do_genetic_maps, $do_markers, $do_traits, 
      $do_qtls);
  my %cmd_opts = ();
  getopts("pegmtqa", \%cmd_opts);
  if (defined($cmd_opts{'p'}) || defined($cmd_opts{'a'})) {$do_pubs         = 1;}
  if (defined($cmd_opts{'e'}) || defined($cmd_opts{'a'})) {$do_experiments  = 1;}
  if (defined($cmd_opts{'g'}) || defined($cmd_opts{'a'})) {$do_genetic_maps = 1;}
  if (defined($cmd_opts{'m'}) || defined($cmd_opts{'a'})) {$do_markers      = 1;}
  if (defined($cmd_opts{'t'}) || defined($cmd_opts{'a'})) {$do_traits       = 1;}
  if (defined($cmd_opts{'q'}) || defined($cmd_opts{'a'})) {$do_qtls         = 1;}
  
  $input_dir = @ARGV[0];
 
 # remove these later
  #our @filepaths = <$input_dir/*.txt>;
  #our %files = map { s/$input_dir\/(.*)$/$1/; $_ => 1} @filepaths;

  # Track data warnings for entire data set:
  #$has_warnings = 0;
  
  # Get connected to db
  $dbh = connectToDB;

  # set default schema
  $sql = "SET SEARCH_PATH = chado";
  $sth = $dbh->prepare($sql);
  $sth->execute();
  
  my $berkeley_dbh = HashDB::dbh_berkeleyDB($input_dir);
  
################################################################################
####                       PUBLICATION WORKSHEETS                          #####
################################################################################
  
  if ($do_pubs) {
    PubVerif::pubVerif($input_dir,$dbh,$berkeley_dbh);
  }#do_pubs
  
################################################################################
####                      QTL EXPERIMENT WORKSHEET                         #####
################################################################################

  if ($do_experiments) {
    QtlExpVerif::qtlExpVerif($input_dir,$dbh,$berkeley_dbh);    
  }#do_experiments
  
################################################################################
####                           MARKER WORKSHEETS                           #####
################################################################################

  if ($do_markers) {
    MasterMarkerVerif::masterMarkerVerif($input_dir,$dbh,$berkeley_dbh);
  }#do_markers
  
################################################################################
####                            MAP WORKSHEETS                             #####
################################################################################

  if ($do_genetic_maps) {
    GeneticMapVerif::geneticMapVerif($input_dir,$dbh,$berkeley_dbh);
  }#do_genetic_maps
  
################################################################################
####                           TRAIT WORKSHEETS                             ####
################################################################################

  if ($do_traits) {
    traitVerif($input_dir,$dbh,$berkeley_dbh);
  }#do_traits
  
################################################################################
####                            QTL WORKSHEET                              #####
################################################################################

  if ($do_qtls) {
    QtlVerif::qtlVerif($input_dir,$dbh,$berkeley_dbh);
    #HashDB::delete_berkeleyDB($input_dir);
  }#check QTL tables

################################################################################
################################################################################
################################################################################
################################################################################

sub consensusMapExists {
  my $mapname = $_[0];
  if ($mapname ne '') {
    $sql = "SELECT featuremap_id FROM chado.featuremap WHERE name='$mapname'";
    logSQL('', "$line_count:$sql");
    $sth = doQuery($dbh, $sql);
    if ($row=$sth->fetchrow_hashref()) {
      return 1;
    }
  }
  
  return 0;
}#consensusMapExists


sub dbExists {
  my $dbname = lc($_[0]);
  if ($dbname ne '') {
    $sql = "SELECT db_id FROM chado.db WHERE LOWER(name)='$dbname'";
    logSQL('', "$line_count:$sql");
    $sth = doQuery($dbh, $sql);
    if ($row=$sth->fetchrow_hashref()) {
      return 1;
    }
  }
  
  return 0;
}#dbExists


sub scaffoldExists {
  my $chromosome = $_[0];
  my ($sql, $sth, $row);

  if (!$chromosome || $chromosome eq '' || $chromosome eq 'NULL' 
        || $chromosome eq 'none') {
    return 1;
  }
  
  $sql = "
    SELECT * FROM chado.feature
    WHERE name='$chromosome' 
      AND type_id=(SELECT cvterm_id FROM chado.cvterm 
                   WHERE name='scaffold'
                     AND cv_id=(SELECT cv_id FROM chado.cv WHERE name='local'))";
   logSQL('', "$line_count:$sql");
   $sth = doQuery($dbh, $sql);
   if ($row=$sth->fetchrow_hashref) {
     return 1;
   }

   return 0;
}#scaffoldExists

1;

