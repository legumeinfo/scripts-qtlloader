# file: dumpSpreadsheet.pl
#
# purpose: dump an Excel spread sheet to a tab-delineated file with
#          properly coded unicode chars
#
# history:
#  06/03/13  eksc  created
#  09/16/13  sdash empty cell to 'NULL' in markers and QTLs

  use strict;
  use Text::Iconv;
  use Spreadsheet::XLSX;
  use Spreadsheet::ParseExcel::FmtUnicode;
  use Data::Dumper;

  use Getopt::Std;
  
  my $warn = <<EOS
    Usage:
      
    $0 [opts] spreadsheet output-dir
      -g dump genetic maps
      -p dump publication worksheets
      -m dump map worksheets
      -q dump QTL worksheets
      -a dump all worksheets
    
    NOTE: spreadsheet needs to be in the older .xls format
          (MS Office 2004 or earlier)
          
EOS
;
  if ($#ARGV < 1) {
    die $warn;
  }
  
  # What spreadsheet needs dumping?
  my ($do_maps, $do_pubs, $do_markers, $do_qtls);
  my %cmd_opts = ();
  getopts("gpmqa", \%cmd_opts);
  if (defined($cmd_opts{'g'})) { $do_maps    = 1; }
  if (defined($cmd_opts{'p'})) { $do_pubs    = 1; }
  if (defined($cmd_opts{'m'})) { $do_markers = 1; }
  if (defined($cmd_opts{'q'})) { $do_qtls    = 1; }
  if (defined($cmd_opts{'a'})) { 
    $do_pubs    = 1;
    $do_markers = 1;
    $do_qtls    = 1;
  }
  
  my ($spreadsheetfile, $out_dir) = @ARGV;
  
print "\nOpen spreadsheet $spreadsheetfile\n\n";
#  my $converter = Text::Iconv -> new ("utf-8", "windows-1251");
#  my $workbook   = Spreadsheet::XLSX->new($spreadsheetfile, $converter);
#  if (!defined $workbook) {
#    die "\nUnable to create spreadsheet parser\n";
#  }

  my $formatter = Spreadsheet::ParseExcel::FmtUnicode->new();
  my $parser   = Spreadsheet::ParseExcel->new();
  my $workbook = $parser->parse($spreadsheetfile, $formatter);
  if (!defined $workbook) {
    die $parser->error(), ".\n";
  }

  
################################################################################
#                                GENETIC MAPS                                  #
################################################################################

if ($do_maps) {
  print "dump genetic map worksheets...\n";
  for my $sheet ($workbook->worksheets()) {
    my $sheet_name = $sheet->get_name;
    if ($sheet_name =~ /^CONSENSUS_MAP/) {
      my $filename = "$out_dir/$sheet_name.txt";
      exportWorksheet($filename, $sheet);
    }#found map worksheet
  }#each worksheet
}#do maps


################################################################################
#                                PUBLICATIONS                                  #
################################################################################

if ($do_pubs) {
  print "dump publication worksheets...\n";
  for my $sheet ($workbook->worksheets()) {
    my $sheet_name = $sheet->get_name;
    if ($sheet_name =~ /^PUB/) {
      my $filename = "$out_dir/$sheet_name.txt";
      exportWorksheet($filename, $sheet);
    }#export this worksheet
  }#each worksheet
}#export pubs spreadsheet

################################################################################
#                                   MARKERS                                    #
################################################################################

if ($do_markers) {
  print "dump marker worksheets...\n";
  for my $sheet ($workbook->worksheets()) {
    my $sheet_name = $sheet->get_name;
    if ($sheet_name eq 'MARKERS') {
      my $filename = "$out_dir/$sheet_name.txt";
      exportWorksheet($filename, $sheet);
    }#export this worksheet
  }#each worksheet
  
}#export markers spreadsheet


################################################################################
#                                    QTLS                                      #
################################################################################

if ($do_qtls) {
  print "dump QTL worksheets...\n";
  for my $sheet ($workbook->worksheets()) {
    my $sheet_name = $sheet->get_name;
    if ($sheet_name =~ /QTL/ || $sheet_name =~ /TRAIT/ || $sheet_name =~ /MAP/) {
      my $filename = "$out_dir/$sheet_name.txt";
      exportWorksheet($filename, $sheet);
    }#export this worksheet
  }#each worksheet
  
}#export qtl spreadsheet


sub exportWorksheet {
  my ($filename, $sheet) = @_;
  
  open OUT, ">$filename" or die "\nUnable to open $filename: $!\n\n";
    
  my ($row_min, $row_max) = $sheet->row_range();
  my ($col_min, $col_max) = $sheet->col_range();
  
  for my $row ($row_min .. $row_max) {
    for my $col (0 .. $col_max) {
      my $cell = $sheet->get_cell($row, $col);
      my $value = 'NULL';
      if ($cell) {
        $value = $cell->value();
        $value =~ s/^\s+//; # remove non-printing chars in empty cell
        $value =~ s/\s+$//;
        $value =~ s/�/�/;
        $value =~ s/�/�/;
        if ($value eq '') {     # if cell is still empty put 'NULL' string
          $value = 'NULL';
        }
      }
      print OUT $value, "\t";
    }#each worksheet column
    
    print OUT "\n";
  }#each worksheet row
}#exportWorksheet