-------------
INSTALLATION:
-------------
The QTL loading scripts require the Perl module DBI::Pg:
  $ sudo cpan
  > install DBI
  > install DBD::Pg
  
The dumper script requires:
   Spreadsheet::ParseXLSX
   Spreadsheet::ParseExcel::FmtUnicode


-----------
THE SCRIPTS
-----------
Note: once upon a time the scripts needed to be executed in the order indicated
      by their names. This is no longer true, which complicates things, rather.

Recommended order:
  0. Load cvterms (QTL_cvterms.sql) and create tables (QTL_new_tables.sql)
       Do this by hand rather than executing the .sql files to catch any errors.
  
  1. dumpSpreadsheet.pl (to get tsv tables from the worksheets)
  
  If you have traits to load:
  2. 0_verifyWorksheet.pl -t (to check trait table)
  3. 5_load_traits.pl
  
  If you have markers to load:
  4. 0_verifyWorksheet.pl -m (to check marker table)
  5. 3_load_markers.pl
  
  For the QTL spreadsheet itself:
  6. 0_verifyWorksheet.pl (-p, -g, -e, -q)
  7. 1_load_publications.pl
  8. 2_load_maps.pl
  7. 4_load_qtl_experiments.pl
  8. 6_load_qtls.pl
