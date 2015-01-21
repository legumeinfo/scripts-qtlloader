# file: dumpSpreadsheet.pl
#
# purpose: dump an Excel spread sheet to a tab-delineated file with
#          properly coded unicode chars
#
# http://search.cpan.org/~doy/Spreadsheet-ParseXLSX-0.16/lib/Spreadsheet/ParseXLSX.pm
# http://search.cpan.org/dist/Spreadsheet-ParseExcel/
#
# history:
#  06/03/13  eksc  created
#  09/16/13  sdash empty cell to 'NULL' in markers and QTLs
#  08/19/14  eksc  modified to be more general

  use strict;
  use Text::Iconv;
  use Spreadsheet::ParseXLSX;
  use Spreadsheet::ParseExcel::FmtUnicode;
  use Data::Dumper;
  use Text::Unidecode qw(unidecode);

  use Getopt::Std;
  
  my $warn = <<EOS
    Usage:
      
    $0 [opts] spreadsheet output-dir
      -w worksheet-list or 'all' (default: all)
      -r row-range (default: all)
      -c column-range (default: all)
      -o html|tsv (default: tsv)
    
    Where a range can contain ranges (e.g. 1-10) and or lists (e.g. 4,6,9).
    Ranges and lists can be combined (e.g. 1-10,12,15-20)
    
    Examples:
    
    Dump worksheets 'QTL' and 'QTL_EXPERIMENT'
      perl $0 -w QTL,QTL_EXPERIMENT BlairGaleano2012_v15srk.xlsx data/
    
    Dump worksheet 'worksheet1', rows 2 through 100, columns 4, 6, and 8
      perl $0 -w worksheet1 -r 2-100 -c 4,6,8 BlairGaleano2012_v15srk.xlsx data/
    
          
EOS
;
  # What needs dumping?
  my ($row_range, $col_range, $outtype);
  my $worksheetlist = 'all';
  my %cmd_opts = ();
  getopts("w:r:c:o:", \%cmd_opts);
  if (defined($cmd_opts{'w'})) { $worksheetlist = $cmd_opts{'w'}; } 
  if (defined($cmd_opts{'r'})) { $row_range     = $cmd_opts{'r'}; }
  if (defined($cmd_opts{'c'})) { $col_range     = $cmd_opts{'c'}; }
  if (defined($cmd_opts{'o'})) { 
    $outtype = $cmd_opts{'o'}; 
  }
  else {
    $outtype = 'tsv';
  }
  
  my ($spreadsheetfile, $out_dir) = @ARGV;
  if (!$out_dir) {
    die $warn;
  }
  
  print "\nDump data from $spreadsheetfile.\n";
  print "Output file(s) of type $outtype will be written to directory $out_dir/\n\n";

  my $formatter = Spreadsheet::ParseExcel::FmtUnicode->new();
  my $parser = Spreadsheet::ParseXLSX->new;
  my $workbook = $parser->parse($spreadsheetfile);
  if (!defined $workbook) {
    die $parser->error(), ".\n";
  }

  # Parse and check worksheet list
  my @worksheets = ($worksheetlist && $worksheetlist ne 'all') 
                 ? getWorksheetsByName($worksheetlist)
                 : $workbook->worksheets();

  print "Process " . (scalar @worksheets) . " worksheets.\n";
  if (scalar @worksheets == 0) {
    $warn = "\nERROR: Unable to find any of the requested worksheets.\n\n" . $warn;
    die $warn;
  }
  
  # Parse and check ranges
  my @rows = getRange($row_range);
#  if (scalar @rows == 0) {
#    $warn = "\nERROR: Unable to find any of the requested rows.\n\n" . $warn;
#    die $warn;
#  }
#  print "Will dump " . (scalar @rows) . " rows\n";

  my @cols = getRange($col_range);
#  if (scalar @rows == 0) {
#    $warn = "\nERROR: Unable to find any of the requested columns.\n\n" . $warn;
#    die $warn;
#  }
#  print "Will dump " . (scalar @cols) . " rows\n";
  
  # valid output type?
  if ($outtype ne 'html' && $outtype ne 'tsv') {
    $warn = "\nERROR: Unknown output type [$outtype].\n\n" . $warn;
    die $warn;
  }
  
  for my $worksheet (@worksheets) {
    my $sheet_name = $worksheet->get_name;
    my $filename = "$out_dir/$sheet_name.txt";
    exportWorksheet($filename, $worksheet, \@rows, \@cols);
  }#each worksheet
  
  
  
sub exportWorksheet {
  my ($filename, $worksheet, $rowref, $colref) = @_;
  my @rows = @$rowref;
  my @cols = @$colref;

  open my $fh, ">$filename" or die "\nUnable to open $filename: $!\n\n";
  
  startFile($fh);
  
  if (!@rows || scalar @rows == 0) {
    @rows = getAllRows($worksheet);
  }
  if (!@cols || scalar @cols == 0) {
    @cols = getAllCols($worksheet);
  }
print "$filename: Dump " . (scalar @rows) . " rows\n";
print "$filename: Dump " . (scalar @cols) . " cols\n";
  
  my $row_count = 0;
  for my $row (@rows) {
    my $check_cell = $worksheet->get_cell($row, 0);
    next if (!$check_cell);
    next if ($check_cell->value() =~ /^##/);  # comments at top of worksheet
    next if ($check_cell->value() =~ /^#/ && $row_count > 0);
    startRow($fh);
    for my $col (@cols) {
      my $cell = $worksheet->get_cell($row, $col);
      my $value = 'NULL';
      if ($cell) {
        $value = $cell->value();
        unidecode($value); # convert the Unicode characters to respective english ASCII values.
        $value =~ s/^\s+//; # remove non-printing chars in empty cell
        $value =~ s/\s+$//;
        $value =~ s/�/�/;
        $value =~ s/�/�/;
        if ($value eq '') {     # if cell is still empty put 'NULL' string
          $value = 'NULL';
        }
      }
      writeCell($fh, $value);
    }#each worksheet column
    
    endRow($fh);
    $row_count++;
  }#each worksheet row
  
  endFile($fh);
  print "Data for worksheet " . $worksheet->get_name . " written to $filename\n\n";
}#exportWorksheet


sub getAllCols {
  my $worksheet = @_[0];
  my ($col_min, $col_max) = $worksheet->col_range();
  
  return ($col_min..$col_max);
}#getAllCols


sub getAllRows {
  my $worksheet = @_[0];
  my ($row_min, $row_max) = $worksheet->row_range();
  
  return ($row_min..$row_max);
}#getAllRows


sub getRange {
  my ($range) = $_[0];
  
  my @vals;
  if ($range) {
    if (!($range =~ /^[\d-,]+$/)) {
      $warn = "\nERROR: incorrect range format: [$range]. "
            . "Must be <start>-<end> with no spaces" . $warn;
      die $warn;
    }
    else {
      my @ranges = split ',', $range;
      foreach my $range (@ranges) {
        if ($range =~ /-/) {
          my ($start, $end) = split '-', $range;
          push @vals, ($start..$end);
        }
        else {
          push @vals, $range;
        }
      }
    }
  }
#print Dumper(@vals);

  return @vals;
}#getRange


sub getWorksheetsByName {
  my @worksheet_names = split ',', $_[0];
  my @worksheets;
  foreach my $name (@worksheet_names) {
    my $worksheet = $workbook->worksheet($name);
    if ($worksheet) {
      push @worksheets, $workbook->worksheet($name);
    }
  }
  return @worksheets;
}#getWorksheetsByName


sub startFile {
  my $fh = $_[0];
  if ($outtype eq 'html') {
    print $fh "<table>\n";
  }
}#startFile


sub endFile {
  my $fh = $_[0];
  if ($outtype eq 'html') {
    print $fh "</table>\n";
  }
}#endFile


sub startRow {
  my $fh = $_[0];
  if ($outtype eq 'html') {
    print $fh "  <tr>\n";
  }
}#startRow

sub endRow {
  my $fh = $_[0];
  if ($outtype eq 'html') {
    print $fh "  </tr>\n";
  }
  else {
    print $fh "\n";
  }
}#startRow

sub writeCell {
  my ($fh, $value) = @_;
  if ($outtype eq 'html') {
    print $fh "    <td>$value</td>\n";
  }
  else {
    print $fh "$value\t";
  }
}#writeCell
