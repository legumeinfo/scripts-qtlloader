!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
IMPORTANT NOTE: this information only applies if setting up a new Tripal/Chado 
resource to hold and present QTL data
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


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

Note: the steps below will only work if done in order. If you wish to only 
      verify the worksheets and not load them, the options to 
			0_verifyWorksheet.pl will need to be "stacked" (e.g. -t, then -tm, then
			-tmp, et cetera). This is because when verifying a worksheet, the verify
			script will need to check both the database and the contents of other
			worksheets to ensure the data is correct.

Recommended order: (if no QTL data at all has been loaded and starting from the beginning) :
  0. Load cvterms (QTL_cvterms.sql) and create tables (QTL_new_tables.sql)
       Do this by hand rather than executing the .sql files to catch any errors.
  
  1. dumpSpreadsheet.pl (to get tsv tables from the worksheets)
  
  If you have new traits to load:
  2. 0_verifyWorksheet.pl -t (to check trait table)
  3. 5_load_traits.pl [OBSOLETE!]
  
  If you have markers to load:
  4. 0_verifyWorksheet.pl -m (to check marker table)
  5. 3_load_markers.pl
  
  For the QTL spreadsheet itself:
  6. 0_verifyWorksheet.pl -p to check publication worksheet
  7. 1_load_publications.pl
  8. 0_verifyWorksheet.pl -g to check map worksheets
  9. 2_load_maps.pl
  10. 0_verifyWorksheet.pl -e to check QTL experiment worksheet
  11. 4_load_qtl_experiments.pl
  12. 0_verifyWorksheet.pl -q to check QTL worksheets
  13. 6_load_qtls.pl

  