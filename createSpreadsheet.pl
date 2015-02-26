# file: createSpreadsheet.pl
#
# purpose: create a download QTL spreadsheet for a given publication
#
# http://search.cpan.org/dist/Spreadsheet-WriteExcel/lib/Spreadsheet/WriteExcel.pm
#
# history:
#  10/06/14  eksc  created

  use strict;
  use DBI;
  use Getopt::Std;
  use Encode;
  use feature 'unicode_strings';
  use Data::Dumper;

  use Spreadsheet::WriteExcel;

  # load local util library
  use File::Spec::Functions qw(rel2abs);
  use File::Basename;
  use lib dirname(rel2abs($0));
  use CropLegumeBaseLoaderUtils;

  my $warn = <<EOS
    Usage:
      
    $0 [opts] data-dir spreadsheet-name, [dataset-identifier]
    data-dir           = where to write the output file
    spreadsheet-name   = the name of the output spreadsheet
    dataset-identifier = uniquename or pub_id for dataset; 
                         only needed for QTL download
      -q download QTL dataset
      -t download all traits
    
EOS
;
  my ($do_qtls, $do_traits);
  my %cmd_opts = ();
  getopts("qt", \%cmd_opts);
  if (defined($cmd_opts{'q'})) {$do_qtls   = 1;}
  if (defined($cmd_opts{'t'})) {$do_traits = 1;}

  if ($#ARGV < 1 || $#ARGV < 2 && $do_qtls) {
    die $warn;
  }
  
  my ($data_dir, $filename, $dataset) = @ARGV;
  
  # Get connected
  my $dbh = connectToDB;
  my $sql = "set search_path=chado";
  my $sth = $dbh->prepare($sql);
  $sth->execute();

  if ($do_qtls) {
    WriteQTLSpreadsheet($dbh);
  }
  
  if ($do_traits) {
    WriteTraitSpreadsheet($dbh);
  }

  $dbh->disconnect();
  print "\n\nALL DONE\n\n";
  
  
###############################################################################
###############################################################################
###############################################################################

sub writeMapCollectionWorksheet {
  my ($dataset, $mnemonic, $format, $workbook, $dbh) = @_;
    
  my $worksheet = $workbook->add_worksheet('MAP_COLLECTION');
  
  my @heads = ('#specieslink_abv:15', 'publication_map_name:20', 'map_name:25', 
               'description:25', 'parent1:15', 'parent2:15', 'pop_size:20', 
               'pop_type:25', 'analysis_method:22', 'publink_citation:22', 
               'unit:10');
  writeHeader($format, $worksheet, @heads);
  my $results = getMapCollectionData($dataset, $mnemonic, $dbh);
  writeResults($results, $worksheet, @heads);
}#writeMapCollectionWorksheet


sub writeMapWorksheet {
  my ($dataset, $mnemonic, $format, $workbook, $dbh) = @_;
    
  my $worksheet = $workbook->add_worksheet('MAPS');
  
  my @heads = ('#specieslink_abv:15', 'map_name:22', 'map_start:10', 
               'map_end:10');
  writeHeader($format, $worksheet, @heads);
  my $results = getMapData($dataset, $mnemonic, $dbh);
  writeResults($results, $worksheet, @heads);
}#writeMapWorksheet


sub writeMapPositionWorksheet {
  my ($dataset, $mnemonic, $format, $workbook, $dbh) = @_;
    
  my $worksheet = $workbook->add_worksheet('MAP_POSITION');
  
  my @heads = ('#map_name:22', 'qtl_symbol:20', 'lg:10', 'left_end:10', 
               'right_end:10', 'QTL_peak:10', 'interval_calc_method:20', 
               'comment:30');
  writeHeader($format, $worksheet, @heads);
  my $results = getMapPositionData($dataset, $dbh);
  writeResults($results, $worksheet, @heads);
}#writeMapPositionWorksheet


sub writePubWorksheet {
  my ($dataset, $mnemonic, $format, $workbook, $dbh) = @_;
    
  my $worksheet = $workbook->add_worksheet('PUB');
  
  my @heads = ('#publink_citation:22', 'species:15', 'ref_type:15', 'year:10', 
               'title:25', 'authors:25', 'journal:20', 'volume:10', 'issue:10', 
               'pages:10', 'doi:20', 'pmid:10', 'abstract:30');
  writeHeader($format, $worksheet, @heads);
  my $results = getPubData($dataset, $mnemonic, $dbh);
  writeResults($results, $worksheet, @heads);
}#WritePubWorksheet


sub WriteQTLSpreadsheet {
  my ($dbh) = @_;
  
  # Start the spreadsheet
  my $ss_name = "$data_dir/$filename.xls";
  my $workbook  = Spreadsheet::WriteExcel->new($ss_name);
  print "\nWrite QTL dataset '$dataset' to $ss_name\n\n";
  
  # Create a format for header rows
  my $format = $workbook->add_format();
  $format->set_bold();
  $format->set_align('center');
  $format->set_color('black');
  $format->set_bg_color('gray');
  
  my $mnemonic = getSpeciesMnemonic($dataset, $dbh);
#print "Got species mnemonic $mnemonic\n";
  
  # Create publication spreadsheet
  print "Writing PUB worksheet...\n";
  writePubWorksheet($dataset, $mnemonic, $format, $workbook, $dbh);

  # Create QTL experiment spreadsheet
  print "Writing QTL_EXPERIMENT worksheet...\n";
  writeQTLExperimentWorksheet($dataset, $mnemonic, $format, $workbook, $dbh);

  # Create map collection spreadsheet
  print "Writing MAP_COLLECTION worksheet...\n";
  writeMapCollectionWorksheet($dataset, $mnemonic, $format, $workbook, $dbh);

  # Create map spreadsheet
  print "Writing MAPS worksheet...\n";
  writeMapWorksheet($dataset, $mnemonic, $format, $workbook, $dbh);
  
  # Create QTL spreadsheet
  print "Writing QTL worksheet...\n";
  writeQTLWorksheet($dataset, $mnemonic, $format, $workbook, $dbh);
  
  # Create map position spreadsheet
  print "Writing MAP_POSITION worksheet...\n";
  writeMapPositionWorksheet($dataset, $mnemonic, $format, $workbook, $dbh);
}#WriteQTLSpreadsheet


sub writeQTLWorksheet {
  my ($dataset, $mnemonic, $format, $workbook, $dbh) = @_;
    
  my $worksheet = $workbook->add_worksheet('QTL');
  
  my @heads = ('#specieslink_abv:15', 'qtl_experimentlink_name:22', 
               'expt_trait_name:25', 'expt_trait_description:25', 
               'trait_unit:10', 'qtl_symbol:20', 'qtl_identifier:10', 
               'expt_qtl_symbol:20', 'favorable_allele_source:22', 
               'treatment:18', 'analysis_method:15', 'lod:10', 
               'likelihood_ratio:18', 'marker_r2:15', 'total_r2:15', 
               'additivity:15', 'nearest_marker:15', 
               'flanking_marker_low:18', 'flanking_marker_high:18', 
               'comment:30');
  writeHeader($format, $worksheet, @heads);
  my $results = getQTLData($dataset, $dbh);
  writeResults($results, $worksheet, @heads);
}#WriteQTLWorksheet


sub writeQTLExperimentWorksheet {
  my ($dataset, $mnemonic, $format, $workbook, $dbh) = @_;
    
  my $worksheet = $workbook->add_worksheet('QTL_EXPERIMENT');
  
  my @heads = ('#specieslink_abv:15', 'publink_citation:25', 'title:30', 
               'name:23', 'description:30', 'geolocation:25', 'map_name:25', 
               'comment:30');
  writeHeader($format, $worksheet, @heads);
  my $results = getQTLExperimentData($dataset, $mnemonic, $dbh);
  writeResults($results, $worksheet, @heads);
}#writeQTLExperimentWorksheet


sub WriteTraitSpreadsheet {
  my ($dbh) = @_;
  
  # Start the spreadsheet
  my $ss_name = "$data_dir/$filename.xls";
  my $workbook  = Spreadsheet::WriteExcel->new($ss_name);
  print "\nWrite traits to $ss_name\n\n";
  
  # Create a format for header rows
  my $format = $workbook->add_format();
  $format->set_bold();
  $format->set_align('center');
  $format->set_color('black');
  $format->set_bg_color('gray');
  
  my $worksheet = $workbook->add_worksheet('Traits');
  
  my @heads = ('qtl_symbol:20', 'trait_name:25', 'trait_class:15', 
               'description:30');
  writeHeader($format, $worksheet, @heads);
  my $results = getTraitData($dataset, $dbh);
  writeResults($results, $worksheet, @heads);
}#WriteQTLSpreadsheet


sub getMapCollectionData {
  my ($dataset, $mnemonic, $dbh) = @_;
  
  my $sth = getMapCollectionNames($dataset, $dbh);

  my @results;
  while (my $row=$sth->fetchrow_hashref) {
#print "Found map " . $row->{'value'} . "\n";
    my $sql = "
      SELECT pn.value AS publication_map_name, m.name AS map_name, m.description,
             p1.name AS parent1, p2.name AS parent2, pop.value AS pop_size, 
             pt.value AS pop_type, meth.value AS analysis_method, 
             pub.uniquename AS publink_citation, u.name AS unit, 
             '$mnemonic' AS specieslink_abv
      FROM featuremap m
        INNER JOIN cvterm u ON u.cvterm_id=m.unittype_id
        
        INNER JOIN featuremap_pub fp ON fp.featuremap_id=m.featuremap_id
        INNER JOIN pub ON pub.pub_id = fp.pub_id
        
        INNER JOIN featuremap_stock fs ON fs.featuremap_id=m.featuremap_id
        INNER JOIN stock s ON s.stock_id=fs.stock_id
        
        LEFT JOIN stock_relationship sr1 
          ON sr1.object_id=s.stock_id
             AND sr1.type_id=(SELECT cvterm_id FROM cvterm 
                              WHERE name='Parent1'
                                    AND cv_id=(SELECT cv_id FROM chado.cv 
                                               WHERE name='stock_relationship'))
        LEFT JOIN stock p1 ON p1.stock_id=sr1.subject_id
        
        LEFT JOIN stock_relationship sr2 
            ON sr2.object_id=s.stock_id
             AND sr2.type_id=(SELECT cvterm_id FROM cvterm 
                              WHERE name='Parent2'
                                    AND cv_id=(SELECT cv_id FROM chado.cv 
                                               WHERE name='stock_relationship'))
        LEFT JOIN stock p2 ON p2.stock_id=sr2.subject_id
  
        LEFT JOIN featuremapprop pn 
          ON pn.featuremap_id=m.featuremap_id
             AND pn.type_id=(SELECT cvterm_id FROM cvterm
                             WHERE name='Publication Map Name'
                                   AND cv_id=(SELECT cv_id FROM cv
                                              WHERE name='featuremap_property'))
                                              
        LEFT JOIN featuremapprop pop 
          ON pop.featuremap_id=m.featuremap_id
             AND pop.type_id=(SELECT cvterm_id FROM cvterm
                              WHERE name='Population Size'
                                    AND cv_id=(SELECT cv_id FROM cv
                                               WHERE name='featuremap_property'))
                                              
        LEFT JOIN featuremapprop pt 
          ON pt.featuremap_id=m.featuremap_id
             AND pt.type_id=(SELECT cvterm_id FROM cvterm
                             WHERE name='Population Type'
                                   AND cv_id=(SELECT cv_id FROM cv
                                              WHERE name='featuremap_property'))
                                              
        LEFT JOIN featuremapprop meth 
          ON meth.featuremap_id=m.featuremap_id
             AND meth.type_id=(SELECT cvterm_id FROM cvterm
                               WHERE name='Methods'
                                     AND cv_id=(SELECT cv_id FROM cv
                                                WHERE name='featuremap_property'))
                                              
        LEFT JOIN featuremapprop cm 
          ON cm.featuremap_id=m.featuremap_id
             AND cm.type_id=(SELECT cvterm_id FROM cvterm
                             WHERE name='Featuremap Comment'
                                   AND cv_id=(SELECT cv_id FROM cv
                                              WHERE name='featuremap_property'))
                                              
        LEFT JOIN featuremap_dbxref fx ON fx.featuremap_id=m.featuremap_id
        LEFT JOIN dbxref xref ON xref.dbxref_id=fx.dbxref_id
      WHERE m.name='" . $row->{'value'} . "'";
#print "$sql\n";
    
    logSQL('', $sql);
    my $sth = doQuery($dbh, $sql);
  
    # build array of hashes
    @results = (@results, getResults($sth));
  }
  
  if ((scalar @results) == 0) {
    die "\nNo map collection data found for $dataset.\n\n";
  }
  
  return \@results;
}#getMapCollectionData


sub getMapCollectionNames {
  my ($dataset, $dbh) = @_;
  my $sql = "
    SELECT DISTINCT(pp.value) FROM projectprop pp
      INNER JOIN project p ON p.project_id=pp.project_id
      INNER JOIN project_pub ppub ON ppub.project_id=pp.project_id
      INNER JOIN pub ON pub.pub_id=ppub.pub_id
    WHERE pp.type_id=(SELECT cvterm_id FROM cvterm WHERE name='Project Map Collection')";
#print "$sql\n";
  if ($dataset =~ /^\d+$/) {
    $sql .= "
          AND pub.pub_id=$dataset";
  }
  else {
    $sql .= "
          AND pub.uniquename='$dataset'";
  }
  $sql .= "
    ORDER BY pp.value";
#print "$sql\n\n";
  logSQL('', $sql);
  return doQuery($dbh, $sql);
}#getMapCollectionNames


sub getMapData {
  my ($dataset, $mnemonic, $dbh) = @_;
  
  my $sth = getMapCollectionNames($dataset, $dbh);

  my @results;
  while (my $row=$sth->fetchrow_hashref) {
    my $sql = "
      SELECT lg.name AS map_name, start.mappos AS map_start, 
             stop.mappos AS map_end, xref.accession AS xref, 
             '$mnemonic' AS specieslink_abv
      FROM feature lg
        INNER JOIN organism o ON o.organism_id=lg.organism_id
        
        INNER JOIN featurepos start ON start.feature_id=lg.feature_id
        INNER JOIN featureposprop sp ON sp.featurepos_id=start.featurepos_id
              AND sp.type_id=(SELECT cvterm_id FROM cvterm
                              WHERE name='start'
                                    AND cv_id=(SELECT cv_id FROM chado.cv 
                                               WHERE name='featurepos_property'))
        INNER JOIN featuremap m ON m.featuremap_id=start.featuremap_id
                                                 
        INNER JOIN featurepos stop ON stop.feature_id=lg.feature_id
        INNER JOIN featureposprop ep 
          ON ep.featurepos_id=stop.featurepos_id
             and ep.type_id=(SELECT cvterm_id from cvterm
                             WHERE name='stop'
                                   AND cv_id=(SELECT cv_id FROM chado.cv 
                                              WHERE name='featurepos_property'))
                                                 
        LEFT JOIN feature_dbxref fx ON fx.feature_id=lg.feature_id
        LEFT JOIN dbxref xref ON xref.dbxref_id=fx.dbxref_id
  
      WHERE lg.type_id=(SELECT cvterm_id FROM cvterm
                        WHERE name='linkage_group'
                              AND cv_id=(select cv_id FROM cv
                                         WHERE name='sequence'))
            AND m.name='" . $row->{'value'} . "'
      ORDER BY lg.name";
#print "$sql\n";

    logSQL('', $sql);
    my $sth = doQuery($dbh, $sql);
#print "Found " . $sth->rows . " records\n";
  
    # build array of hashes
    @results = (@results, getResults($sth));
  }
#print Dumper(@results);
  
  if ((scalar @results) == 0) {
    die "\nNo linkage group map data found for $dataset.\n\n";
  }
  
  return \@results;
}#getMapData


sub getMapPositionData {
  my ($dataset, $dbh) = @_;
  
  my @heads = ('#map_name', 'qtl_symbol', 'qtl_identifier', 
               'lg', 'left_end', 'right_end', 'QTL_peak', 'interval_calc_method');
  my $sql = "
    SELECT q.name AS qtl_symbol, m.name AS map_name, lg.value AS lg,
           CAST(loc.fmin as float)/100.0 AS left_end, CAST(loc.fmax as float)/100.0 AS right_end,
           flp.value AS interval_calc_method
  
    FROM feature q
    
      INNER JOIN featureloc loc ON loc.feature_id=q.feature_id
      INNER JOIN feature lgm ON lgm.feature_id=loc.srcfeature_id
      INNER JOIN featureprop lg ON lgm.feature_id=lg.feature_id

      INNER JOIN featurepos mp ON mp.feature_id=lgm.feature_id
      INNER JOIN featureposprop mpp 
        ON mpp.featurepos_id=mp.featurepos_id
           AND mpp.type_id=(SELECT cvterm_id FROM cvterm
                            WHERE name='start'
                              AND cv_id=(SELECT cv_id FROM chado.cv 
                                         WHERE name='featurepos_property'))
      INNER JOIN featuremap m ON m.featuremap_id=mp.featuremap_id
      LEFT JOIN public.chado_featuremap cm ON cm.featuremap_id=m.featuremap_id
                                        
      LEFT JOIN featurelocprop flp 
        ON flp.featureloc_id=loc.featureloc_id
           AND flp.type_id=(SELECT cvterm_id FROM cvterm 
                            WHERE name='Interval Calculation Method'
                                  AND cv_id=(SELECT cv_id FROM cv 
                                             WHERE name='feature_property'))

      INNER JOIN feature_project fe on fe.feature_id=q.feature_id
      INNER JOIN project e on e.project_id=fe.project_id
      INNER JOIN project_pub ep on ep.project_id=e.project_id
      INNER JOIN pub on pub.pub_id=ep.pub_id";
  if ($dataset =~ /^\d+$/) {
    $sql .= "
    WHERE pub.pub_id=$dataset";
  }
  else {
    $sql .= "
    WHERE pub.uniquename='$dataset'";
  }
  $sql .= "
    ORDER BY qtl_symbol";
#print "$sql\n";

  logSQL('', $sql);
  my $sth = doQuery($dbh, $sql);

  # build array of hashes
  my @results = getResults($sth);
  if ((scalar @results) == 0) {
    die "\nNo QTL map positions found for $dataset.\n\n";
  }
  
  return \@results;
}#getMapPositionData


sub getPubData {
  my ($dataset, $mnemonic, $dbh) = @_;
  
  my $sql = "
    SELECT p.uniquename AS publink_citation, t.name AS ref_type, 
           p.pyear AS year, p.title, a.value AS authors, 
           p.series_name AS journal, p.volume, p.pages, doi.accession AS doi,
           pmid.accession AS pmid, ab.value as abstract, '$mnemonic' AS species
    FROM pub p
      INNER JOIN cvterm t on t.cvterm_id=p.type_id
      
      INNER JOIN pubprop a 
        ON a.pub_id=p.pub_id
           AND a.type_id = (SELECT cvterm_id from cvterm 
                            WHERE name='Authors'
                                  and cv_id=(SELECT cv_id FROM chado.cv 
                                             WHERE name='tripal_pub'))
      
      INNER JOIN pubprop ab 
        ON ab.pub_id=p.pub_id
           AND ab.type_id = (SELECT cvterm_id FROM cvterm 
                             WHERE name='Abstract'
                                   and cv_id=(SELECT cv_id FROM chado.cv 
                                              WHERE name='tripal_pub'))
      
      LEFT JOIN (
        SELECT dx.pub_id, d.accession 
          FROM pub_dbxref dx
            INNER JOIN dbxref d ON d.dbxref_id=dx.dbxref_id
          WHERE d.db_id = (SELECT db_id FROM db WHERE name='DOI')) doi
            ON doi.pub_id=p.pub_id
      
      LEFT JOIN (
        SELECT pmx.pub_id, pm.accession 
          FROM pub_dbxref pmx
            INNER JOIN dbxref pm ON pm.dbxref_id=pmx.dbxref_id
          WHERE pm.db_id = (SELECT db_id FROM db WHERE name='PMID')) pmid
            ON pmid.pub_id=p.pub_id";
  if ($dataset =~ /^\d+$/) {
    $sql .= "
       WHERE p.pub_id=$dataset";
  }
  else {
    $sql .= "
       WHERE p.uniquename='$dataset'";
  }
  $sql .= "
    ORDER BY publink_citation;";
#print "$sql\n";

  logSQL('', $sql);
  my $sth = doQuery($dbh, $sql);

  # build array of hashes
  my @results = getResults($sth);
  if ((scalar @results) == 0) {
    print "  No publication results found\n";
  }
  
  return \@results;
}#getPubData


sub getQTLExperimentData {
  my ($dataset, $mnemonic, $dbh) = @_;
  
  my $sql = "
  SELECT pub.uniquename AS publink_citation, p.description AS title, p.name, 
         d.value AS description, g.description AS geolocation,
         m.value AS map_name, c.value as comment, '$mnemonic' AS specieslink_abv
  FROM project p
    INNER JOIN  projectprop d 
      ON d.project_id=p.project_id
         AND d.type_id = (SELECT cvterm_id FROM cvterm  
                          WHERE name='Project Description'
                                AND cv_id=(SELECT cv_id FROM cv 
                                           WHERE name='project_property'))
    
    INNER JOIN projectprop m
      ON m.project_id=p.project_id
         AND m.type_id=(SELECT cvterm_id FROM cvterm
                        WHERE name='Project Map Collection'
                              AND cv_id = (SELECT cv_id FROM cv 
                                           WHERE name='project_property'))
    
    inner join projectprop c
      on c.project_id=p.project_id
         and c.type_id=(select cvterm_id from cvterm
                        where name='Project Comment'
                              and cv_id = (select cv_id FROM cv 
                                           where name='project_property'))
    
    inner join nd_experiment_project ep on ep.project_id=p.project_id
    inner join nd_experiment e on e.nd_experiment_id=ep.nd_experiment_id
    inner join nd_geolocation g on g.nd_geolocation_id=e.nd_geolocation_id
    
    inner join project_pub pp on pp.project_id=p.project_id
    inner join pub on pub.pub_id=pp.pub_id";
  if ($dataset =~ /^\d+$/) {
    $sql .= "
       where pub.pub_id=$dataset";
  }
  else {
    $sql .= "
       where pub.uniquename='$dataset'";
  }
  $sql .= "
  order by name";
#print "$sql\n";

  logSQL('', $sql);
  my $sth = doQuery($dbh, $sql);

  # build array of hashes
  my @results = getResults($sth);
  if ((scalar @results) == 0) {
    print "  No QTL Experiment results found\n";
  }
  
  return \@results;
}#getQTLExperimentData


sub getQTLData {
  my ($dataset, $dbh) = @_;
  
  my @heads = ('#specieslink_abv', 'qtl_experimentlink_name', 'expt_trait_name', 
               'expt_trait_description', 'trait_unit', 'qtl_symbol', 
               'qtl_identifier', 'expt_qtl_symbol', 'favorable_allele_source', 
               'treatment', 'analysis_method', 'lod', 'likelihood_ratio', 
               'marker_r2', 'total_r2	additivity', 'nearest_marker', 
               'flanking_marker_low', 'flanking_marker_high', 'comment');
  my $sql = "
    SELECT d.accession AS specieslink_abv, e.name AS qtl_experimentlink_name, 
           etn.value AS expt_trait_name, etd.value AS expt_trait_description, 
           tu.value AS trait_unit, t.name AS qtl_symbol, 
           ti.value as qtl_identifier, sy.name as expt_qtl_symbol, 
           s.name AS favorable_allele_source, qst.value AS treatment, 
           meth.value AS analysis_method, lod.rawscore AS lod, 
           lr.rawscore AS likelihood_ratio, mr2.rawscore AS marker_r2, 
           tr2.rawscore AS total_r2, add.rawscore AS additivity, 
           nm.name AS nearest_marker, fml.name AS flanking_marker_low,
           fmh.name AS flanking_marker_high, cmm.value AS comment
    FROM feature q
      INNER JOIN organism o on o.organism_id=q.organism_id
      INNER JOIN chado.organism_dbxref od ON od.organism_id=o.organism_id 
      INNER JOIN chado.dbxref d 
        on d.dbxref_id=od.dbxref_id 
           and db_id=(select db_id from db where name='uniprot:species')
      
      INNER JOIN feature_project fe on fe.feature_id=q.feature_id
      INNER JOIN project e on e.project_id=fe.project_id
      INNER JOIN project_pub ep on ep.project_id=e.project_id
      INNER JOIN pub on pub.pub_id=ep.pub_id
      
      INNER JOIN featureprop etn 
        on etn.feature_id=q.feature_id
           and etn.type_id=(select cvterm_id from cvterm
                            where name='Experiment Trait Name'
                                  and cv_id=(select cv_id from cv
                                             where name='feature_property'))
                                             
      LEFT JOIN featureprop etd 
        on etd.feature_id=q.feature_id
           and etd.type_id=(select cvterm_id from cvterm
                            where name='Experiment Trait Description'
                                  and cv_id=(select cv_id from cv
                                             where name='feature_property'))
                                             
      LEFT JOIN featureprop tu 
        on tu.feature_id=q.feature_id
           and tu.type_id=(select cvterm_id from cvterm
                            where name='Trait Unit'
                                  and cv_id=(select cv_id from cv
                                             where name='feature_property'))
                                             
      INNER JOIN featureprop ti 
        on ti.feature_id=q.feature_id
           and ti.type_id=(select cvterm_id from cvterm
                            where name='QTL Identifier'
                                  and cv_id=(select cv_id from cv
                                             where name='feature_property'))

      LEFT JOIN feature_synonym qs on qs.feature_id=q.feature_id
      LEFT JOIN synonym sy 
        on sy.synonym_id=qs.synonym_id
           and sy.type_id=(select cvterm_id from chado.cvterm
                           where name='Symbol'
                                 and cv_id=(select cv_id from chado.cv 
                                            where name='synonym_type'))
           
      LEFT JOIN featureprop qst 
        on qst.feature_id=q.feature_id
           and qst.type_id=(select cvterm_id from cvterm
                            where name='QTL Study Treatment'
                                  and cv_id=(select cv_id from cv
                                             where name='feature_property'))

      INNER JOIN featureprop meth 
        on meth.feature_id=q.feature_id
           and meth.type_id=(select cvterm_id from cvterm
                            where name='QTL Analysis Method'
                                  and cv_id=(select cv_id from cv
                                             where name='feature_property'))

      LEFT JOIN featureprop cmm 
        on cmm.feature_id=q.feature_id
           and cmm.type_id=(select cvterm_id from cvterm
                            where name='comment'
                                  and cv_id=(select cv_id from cv
                                             where name='feature_property'))
      LEFT JOIN analysisfeature lod 
        on lod.feature_id=q.feature_id
           and lod.analysis_id=(select analysis_id from analysis where name='LOD')
      
      LEFT JOIN analysisfeature lr 
        on lr.feature_id=q.feature_id
           and lr.analysis_id=(select analysis_id from analysis where name='likelihood ratio')
      
      LEFT JOIN analysisfeature mr2 
        on mr2.feature_id=q.feature_id
           and mr2.analysis_id=(select analysis_id from analysis where name='marker R2')
      
      LEFT JOIN analysisfeature tr2 
        on tr2.feature_id=q.feature_id
           and tr2.analysis_id=(select analysis_id from analysis where name='total R2')
      
      LEFT JOIN analysisfeature add 
        on add.feature_id=q.feature_id
           and add.analysis_id=(select analysis_id from analysis where name='additivity')
      
      INNER JOIN feature_cvterm tr on tr.feature_id=q.feature_id
      INNER JOIN cvterm t on t.cvterm_id=tr.cvterm_id
      INNER JOIN feature_cvtermprop trp 
        on trp.feature_cvterm_id=tr.feature_cvterm_id
           and trp.type_id=(select cvterm_id from cvterm 
                            where name='QTL Symbol' 
                                  and cv_id=(select cv_id from chado.cv 
                                             where name='local'))
                                             
      LEFT JOIN feature_stock fs 
        ON fs.feature_id=q.feature_id
           AND fs.type_id=(SELECT cvterm_id FROM chado.cvterm 
                           WHERE name='Favorable Allele Source'
                                 AND cv_id=(SELECT cv_id FROM chado.cv 
                                            WHERE name='local'))
      LEFT JOIN stock s ON s.stock_id=fs.stock_id
                                
      LEFT JOIN feature_relationship nmr
        ON nmr.subject_id=q.feature_id 
          AND nmr.type_id=(SELECT cvterm_id FROM chado.cvterm 
             WHERE name='Nearest Marker' 
               AND cv_id=(SELECT cv_id FROM chado.cv 
                          WHERE name='feature_relationship'))
      LEFT JOIN feature nm ON nm.feature_id=nmr.object_id
      
      LEFT JOIN feature_relationship fmlr 
        ON fmlr.subject_id=q.feature_id 
           AND fmlr.type_id=(SELECT cvterm_id FROM chado.cvterm 
                             WHERE name='Flanking Marker Low' 
                                   AND cv_id=(SELECT cv_id FROM chado.cv 
                                              WHERE name='feature_relationship'))
      LEFT JOIN feature fml ON fml.feature_id=fmlr.object_id
    
      LEFT JOIN feature_relationship fmhr ON fmhr.subject_id=q.feature_id 
        AND FMHR.type_id=(SELECT cvterm_id FROM chado.cvterm 
                          WHERE name='Flanking Marker High' 
                                AND cv_id=(SELECT cv_id FROM chado.cv 
                                           WHERE name='feature_relationship'))
      LEFT JOIN feature fmh ON fmh.feature_id=fmhr.object_id
    
    WHERE q.type_id=(SELECT cvterm_id FROM cvterm 
                     WHERE name='QTL'
                           AND cv_id=(SELECT cv_id FROM chado.cv 
                                      WHERE name='sequence'))";
  if ($dataset =~ /^\d+$/) {
    $sql .= "
          AND pub.pub_id=$dataset";
  }
  else {
    $sql .= "
          AND pub.uniquename='$dataset'";
  }
  $sql .= "
    ORDER BY q.name";
#print "$sql\n";
  logSQL('', $sql);
  my $sth = doQuery($dbh, $sql);

  # build array of hashes
  my @results = getResults($sth);
  if ((scalar @results) == 0) {
    die "\nUnable to find QTL data.\n\n";
  }
  
  return \@results;
}#getQTLData


sub getResults {
  my ($sth) = @_;
  my @results;
  while (my $row=$sth->fetchrow_hashref) {
    my @cols = keys %{$row};
    my %hash;
    foreach my $col (@cols) {
      $hash{$col} = $row->{$col};
    }#each column
    push @results, {%hash};
  }#each row
  
  return @results;
}#getResults


sub getSpeciesMnemonic {
  my ($dataset, $dbh) = @_;
  
  my $sql = "
    SELECT pp.value FROM chado.pubprop pp
      INNER JOIN chado.pub ON pub.pub_id=pp.pub_id
    WHERE pp.type_id=(SELECT cvterm_id FROM chado.cvterm 
                      WHERE name='Publication Species'
                            AND cv_id=(SELECT cv_id FROM chado.cv 
                                       WHERE name='tripal_pub'))";
  if ($dataset =~ /^\d+$/) {
    $sql .= "
          AND pp.pub_id=$dataset";
  }
  else {
    $sql .= "
          AND pub.uniquename='$dataset'";
  }
  
  logSQL('', $sql);
  my $sth = doQuery($dbh, $sql);
  
  if (my $row=$sth->fetchrow_hashref) {
    return $row->{'value'};
  }
  else {
    die "Unable to find species mnemonic for $dataset\n";
  }
}#getSpeciesMnemonic


sub getTraitData {
  my ($dataset, $dbh) = @_;
  
  my $sql = "
    SELECT t.name AS qtl_symbol, t.definition AS description,
           tn.name AS trait_name, tc.name AS trait_class 
    FROM chado.cvterm t
      INNER JOIN chado.cvterm_relationship tnr 
        ON tnr.subject_id=t.cvterm_id
           AND tnr.type_id=(SELECT cvterm_id FROM chado.cvterm 
                            WHERE name='Has Trait Name'
                                  AND cv_id = (SELECT cv_id FROM chado.cv 
                                               WHERE name='local'))
      INNER JOIN chado.cvterm tn ON tn.cvterm_id=tnr.object_id
      
      INNER JOIN chado.cvterm_relationship tcr 
        ON tcr.subject_id=t.cvterm_id
           AND tcr.type_id=(SELECT cvterm_id FROM chado.cvterm 
                            WHERE name='Has Trait Class'
                                  AND cv_id = (SELECT cv_id FROM chado.cv 
                                               WHERE name='local'))
      INNER JOIN chado.cvterm tc ON tc.cvterm_id=tcr.object_id
      
    WHERE t.cv_id = (SELECT cv_id FROM chado.cv WHERE name='LegumeInfo:traits')
    ORDER BY qtl_symbol";
    
  logSQL('', $sql);
  my $sth = doQuery($dbh, $sql);

  # build array of hashes
  my @results = getResults($sth);
  if ((scalar @results) == 0) {
    die "\nUnable to find traits.\n\n";
  }
  
  return \@results;
}#getTraitData


sub writeHeader {
  my ($format, $worksheet, @heads) = @_;
  
  for (my $i=0; $i<=$#heads; $i++) {
    my ($head, $width) = split ':', $heads[$i];
    $worksheet->write(0, $i, $head, $format);
    $worksheet->set_column($i, $i, $width);
  }
  $worksheet->freeze_panes(1, 0);
}#writeHeader


sub writeResults {
  my ($resultsref, $worksheet, @heads) = @_;
  return if (!$resultsref);
  
  my @results = @{$resultsref};
#print "\nresults\n" . Dumper(@results);
  my $rowcount = scalar @results;
  for (my $i=0; $i<$rowcount; $i++) {
#print "\n$i: results\n" . Dumper($results[$i]);
    my $colcount = scalar @heads;
    for (my $j=0; $j<$colcount; $j++) {
      my $head = $heads[$j];
      $head =~ s/\#//;
      $head =~ s/\:.*//;
#print "\n  $j: head: $head\n";
      $worksheet->write($i+1, $j, $results[$i]{$head});
    }#each column
  }#each results
}#writeResults


