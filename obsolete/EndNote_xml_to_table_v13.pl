#!/usr/bin/perl
use strict;
use warnings;

# Author: Steven Cannon

use Getopt::Long;
use feature qw(say switch);
use XML::Simple qw(:strict);
use Data::Dumper;
use File::Basename;

my ($out, $help, $print_header, $verbose);
my $format = "tripal";

GetOptions (
  "format:s" =>     \$format,
  "out:s" =>        \$out,
  "print_header" => \$print_header,
  "help" =>         \$help,
  "verbose+" =>     \$verbose,
);

if ( $verbose ) {if (defined($out)){ say "\noutput base name: [$out]\n"} else {say "\noutput is to STDOUT\n"} }

my $usage = <<EOS;
  Usage: perl $0 [-options] FILE_IN
    
  Reads a text file exported from EndNote's File/Export/(XML; Show All Fields)
  Generates a text file that can be loaded into the lisqtl 
  database tables: "pubs", "pub_authors", "pub_keywords", and "pub_urls" --
     and also a combined format, where authors, keywords, and URLs are csv-flattened
  
  NOTE: A customization needs to be made in EndNote to allow adding the fields publink_citation,
  species, PMID, and status. See notes at end of this script for details.
  
  Options:
    -out          base name for output files. If not set, then print all to stdout.
                  Suffixes for Chado loading are .PUBS.txt .PUB_AUTHORS.txt .PUB_KEYWORDS.txt .PUB_URLS.txt
                  Suffix for Tripal loading is .PUBS_COMBINED.txt
                    
    -format STRING where STRING is one of [kv, chado, tripal]
                  kv:     output as DB-key-value triples, PUBS  volume  value
                  chado:  output as a set of three tables: PUBS, PUB_AUTHORS, PUB_KEYWORDS
                  tripal: output in Tripal2 "references" format, into a single PUB_COMBINED table,
                            with authors, keywords, and URLs each flattened into single csv fields
                Default: tripal.
    -print_header print column headings (prefixed with # comment character)
                    Boolean. Default not-set (don't print).
    -help         for this message
    -verbose      for some debug info. 
                  Call twice (-v -v) to pretty-print xml from the input file to stdout.
EOS

if ($help) { die "\n$usage\n" }

unless ( $ARGV[0] ) { 
  die "\nAn EndNote XML file is required:  'EndNote_xml_to_table.pl [-options] FILE_IN'\n".
      "See  'EndNote_xml_to_table.pl -help'  for more info.\n\n"
}

my $infile = $ARGV[0];


my $xml = XMLin( $infile, KeyAttr => 1, ForceArray => [ 'record', 'author', 'keyword' , 'url' ] );

if ( defined($verbose) && $verbose > 1 ) { $Data::Dumper::Indent = 1; print Dumper($xml) }

my (%pubs, %pub_keywords, %pub_authors, %pub_urls); 

my $xml_record_ref = $xml->{'records'}->{'record'};
#my @fields = qw(publink_citation ref_type doi year); # PARTIAL list, for testing

# FULL field list - Chado "PUBS"-style output:
my @fields = qw(publink_citation species ref_type year title journal volume issue pages doi isbn pmid abstract); 
# authors, keywords, and URL are handled separately, since they may have multiple values

# FULL list - combined Tripal-style output:
my @combined_fields = qw(publink_citation species ref_type year title authors journal volume issue pages doi pmid abstract keywords urls); 
#my @combined_fields = qw(publink_citation species ref_type year title journal volume issue pages doi isbn pmid abstract); 
# These fields are flattened into csv-delimted text items: authors, keywords, and urls

##########################################################################################
# process and print results as table-key-value triplets
if ( $format =~ /kv/i ) { 

  my ( $PUB, $PUB_AU, $PUB_KEY, $PUB_URL ); 
  if ( $out ) {
    open ($PUB, ">:utf8", "$out.PUBS.txt") or die "can't open out $out.PUBS.txt: $!";
    open ($PUB_AU, ">:utf8", "$out.PUB_AUTHORS.txt") or die "can't open out $out.PUB_AUTHORS.txt: $!";
    open ($PUB_KEY, ">:utf8", "$out.PUB_KEYWORDS.txt") or die "can't open out $out.PUB_KEYWORDS.txt: $!";  
    open ($PUB_URL, ">:utf8", "$out.PUB_URLS.txt") or die "can't open out $out.PUB_URLS.txt: $!";  
  }

  # (Re)initialize the hashes
  %pubs = (); %pub_keywords = (); %pub_authors = (); %pub_urls = ();
  
  for my $r (@$xml_record_ref) {
    
    &parse_xml($r);
    
    # Print results
    for my $field (@fields) { # PRINT PUBS DATA
      &prn($PUB, "PUBS\t$field\t$pubs{$field}\n");
    }
    if ( %pub_keywords ) { # PRINT KEYWORD DATA unless there's nothing in the hash
      &prn($PUB_KEY, "PUB_KEYWORDS\tpublink_citation\t$pubs{publink_citation}\n"); # print primary key
      for my $kw ( keys %pub_keywords ) { 
        unless ($kw eq 'publink_citation') { &prn($PUB_KEY, "PUB_KEYWORD\tkeyword\t$pub_keywords{$kw}\n") }
      }
    }
    if ( %pub_authors ) { # PRINT AUTHOR DATA unless there's nothing in the hash
      &prn($PUB_AU, "PUB_AUTHORS\tpublink_citation\t$pubs{publink_citation}\n"); # print primary key
      for my $cite_order ( sort {$a <=> $b} ( keys %pub_authors ) ) { 
        unless ($cite_order eq 'publink_citation') { &prn($PUB_AU, "PUB_AUTHORS\tauthor\t$pub_authors{$cite_order}\n") }
      }
    }
    if ( %pub_urls ) { # PRINT URL DATA unless there's nothing in the hash. Also kill "<Go to ISI>" links
      &prn($PUB_URL, "PUB_URLS\tpublink_citation\t$pubs{publink_citation}\n"); # print primary key
      for my $url ( keys %pub_urls ) { 
        unless ($url eq 'publink_citation') { 
          $pub_urls{$url} =~ s/<Go to ISI>.+/NULL/i; # Kill these links, which aren't useful to us.
          &prn($PUB_URL, "PUB_URLS\turl\t$pub_urls{$url}\n") 
        }
      }
    }
  }
}


##########################################################################################
elsif ( $format =~ /chado/i ) { # print to three tables for Chado PUB-module format
  
  my ( $PUB, $PUB_AU, $PUB_KEY, $PUB_URL ); 
  if ( $out ) {
    open ($PUB, ">:utf8", "$out.PUBS.txt") or die "can't open out $out.PUBS.txt: $!";
    open ($PUB_AU, ">:utf8", "$out.PUB_AUTHORS.txt") or die "can't open out $out.PUB_AUTHORS.txt: $!";
    open ($PUB_KEY, ">:utf8", "$out.PUB_KEYWORDS.txt") or die "can't open out $out.PUB_KEYWORDS.txt: $!";  
    open ($PUB_URL, ">:utf8", "$out.PUB_URLS.txt") or die "can't open out $out.PUB_URLS.txt: $!";  
  }

  #PUB
  my $header_printed;
  if ($print_header) {
    my $header = "#" . join ("\t", @fields) . "\n";
    &prn($PUB, $header); # print header
  }
  
  # Process each xml record reference (object)
  for my $r (@$xml_record_ref) {
    &parse_xml($r);
    for my $field (@fields) { 
      &prn($PUB, "$pubs{$field}\t");
    }
    &prn($PUB, "\n");
  }
  
  #PUB_AUTHORS
  for my $r (@$xml_record_ref) {
    &parse_xml($r);
    if ( %pub_authors ) {
      if ($print_header) {
        unless ( $header_printed ) { &prn($PUB_AU, "#publink_citation\tauthor\tcite_order\n"); $header_printed = 1 }
      }
      for my $cite_order ( sort ( keys %pub_authors ) ) { 
        &prn($PUB_AU, "$pubs{publink_citation}\t");
        &prn($PUB_AU, "$pub_authors{$cite_order}\t$cite_order\n");
      }
    }
  }
  undef $header_printed;
  
  #PUB_KEYWORD
  for my $r (@$xml_record_ref) {
    &parse_xml($r);
    if ( %pub_keywords ) {
      if ($print_header) {
        unless ( $header_printed ) { &prn($PUB_KEY, "#publink_citation\tkeyword\n"); $header_printed = 1 }
      }
      for my $kw ( keys %pub_keywords ) { 
        &prn($PUB_KEY, "$pubs{publink_citation}\t");
        &prn($PUB_KEY, "$pub_keywords{$kw}\n");
      }
    }
  }
  undef $header_printed;
  
  #PUB_URL
  for my $r (@$xml_record_ref) {
    &parse_xml($r);
    if ( %pub_urls ) {
      if ($print_header) {
        unless ( $header_printed ) { &prn($PUB_URL, "#publink_citation\turl\n"); $header_printed = 1 }
      }
      my $url_count = keys %pub_urls; #sdash
      my $url_printed = 0;  #sdash
      for my $url ( keys %pub_urls ) { 
        $pub_urls{$url} =~ s/<Go to ISI>.+/NULL/i; # Kill these links, which aren't useful to us.
        if ( ($pub_urls{$url} =~ /ncbi\.nlm\.nih\.gov\/pubmed/)  && ($url_count > 1) ) { 
          next;  #sdash: Remove pubmed url :ncbi.nlm.nih.gov/pubmed if >1 urls available (pmid is already in $pubs/PUBS.txt)
        } elsif ($url_printed > 0) { 
          next;  # sdash: if printed once for this record, no more is necessary.
        }
        &prn($PUB_URL, "$pubs{publink_citation}\t");
        &prn($PUB_URL, "$pub_urls{$url}\n");
        $url_printed = 1;  
      }
    }
  }
  undef $header_printed;
  
}

##########################################################################################
elsif ( $format =~ /tripal/i ) { # print to a single table for Tripal2's references format
  
  my $PUB_COMBINED; 
  my $header_printed;
  if ( $out ) {
    open ($PUB_COMBINED, ">:utf8", "$out.PUB_COMBINED.txt") or die "can't open out $out.PUB_COMBINED.txt: $!";
  }


  #PUB_COMBINED
  # publink_citation species ref_type year title authors journal volume issue pages doi pmid abstract keywords urls
  if ($print_header) {
    my $header = "#" . join ("\t", @combined_fields) . "\n";
    &prn($PUB_COMBINED, $header); # print header
    $header_printed = 1;
  }


  for my $r (@$xml_record_ref) {
    my $authors = "";
    my $keywords = "";
    my $urls = "";
    
    # Process each xml record reference (object)
    &parse_xml($r);
    
    #PUB_AUTHORS - flattened into a single CSV record
    if ( %pub_authors ) {
      for my $cite_order ( sort ( keys %pub_authors ) ) { 
        my $author = $pub_authors{$cite_order};
        $author =~ s/,//g; # strip commas from within a name
        $author =~ s/([A-Z])\./$1/g; # strip periods from first and middle initials
        $author =~ s/([A-Z]) ([A-Z])(?![a-z])/$1$2/g; # strip spaces between first and middle initials
        $authors .="$author, ";
      }
      $authors =~ s/, $//; # get rid of trailing comma
    }
    $pubs{"authors"} = $authors;
    
    #PUB_KEYWORD - flattened into a single CSV record
    if ( %pub_keywords ) {
      for my $kw ( keys %pub_keywords ) { 
        my $keyword = $pub_keywords{$kw};
        $keyword =~ s/,//g;
        $keywords .= "$keyword, ";
      }
      $keywords =~ s/, $//; # get rid of trailing comma
    }
    $pubs{"keywords"} = $keywords;
    
    #PUB_URL - flattened into a single CSV record
    if ( %pub_urls ) {
      my $url_count = keys %pub_urls; #sdash
      my $url_printed = 0;  #sdash
      for my $url ( keys %pub_urls ) { 
        $pub_urls{$url} =~ s/<Go to ISI>.+/NULL/i; # Kill these links, which aren't useful to us.
        if ( ($pub_urls{$url} =~ /ncbi\.nlm\.nih\.gov\/pubmed/)  && ($url_count > 1) ) { 
          next;  #sdash: Remove pubmed url :ncbi.nlm.nih.gov/pubmed if >1 urls available (pmid is already in $pubs/PUBS.txt)
        } elsif ($url_printed > 0) { 
          next;  # sdash: if printed once for this record, no more is necessary.
        }
        $urls .= "$pub_urls{$url}, ";
        $url_printed = 1;  
      }
      $urls =~ s/, $//; # get rid of trailing comma
    }
    $pubs{"urls"} = $urls;
    
    for my $field (@combined_fields) { 
      &prn($PUB_COMBINED, "$pubs{$field}\t");
    }
    &prn($PUB_COMBINED, "\n");
    
  }

  

  undef $header_printed;

}

##########################################################################################
else { die "\nFormat must be either 'kv', 'chado', or 'tripal'\n" }

##########################################################################################
### subroutines ###
sub prn {
  my ($FH, $print_string) = @_; #Incoming: output filehandle, and data-element to print
  if ( defined($out) ) { print $FH $print_string }
  else { print STDOUT $print_string }
}

sub parse_xml {
  my $r = shift; # 'r' is the data structure for a record
  
  # Reinitialize the hashes
  %pubs = (); %pub_keywords = (); %pub_authors = (); %pub_urls = ();
  
  # @fields = qw(publink_citation species ref_type year title journal volume issue pages doi isbn pmid abstract)
  
  # See note at end of script: CUSTOMIZING ENDNOTE, about collecting values for $publink_citation and $pmid
  (defined( $r->{'custom1'}->{'style'}->{'content'}))                 ?  ($pubs{"publink_citation"} = $r->{'custom1'}->{'style'}->{'content'})                  : ($pubs{"publink_citation"} = "NEED_publink_citation!");
  (defined( $r->{'custom3'}->{'style'}->{'content'}))                 ?  ($pubs{"species"} =          $r->{'custom3'}->{'style'}->{'content'})                  : ($pubs{"species"} = "NEED_species!");
  (defined( $r->{'ref-type'}->{'name'}))                              ?  ($pubs{"ref_type"} =         $r->{'ref-type'}->{'name'})                               : ($pubs{"ref_type"} = "NULL");
    if ($pubs{"ref_type"} =~ /Journal Article/) { $pubs{"ref_type"} = "Journal" } # "Journal" is pub_type ontology
  (defined( $r->{'dates'}->{'year'}->{'style'}->{'content'}))         ?  ($pubs{"year"} =             $r->{'dates'}->{'year'}->{'style'}->{'content'})          : ($pubs{"year"} = "NULL");
  (defined( $r->{'titles'}->{'title'}->{'style'}->{'content'}))       ?  ($pubs{"title"} =            $r->{'titles'}->{'title'}->{'style'}->{'content'})        : ($pubs{"title"} = "NULL");
  (defined( $r->{'volume'}->{'style'}->{'content'}))                  ?  ($pubs{"volume"} =           $r->{'volume'}->{'style'}->{'content'})                   : ($pubs{"volume"} = "NULL");
  (defined( $r->{'number'}->{'style'}->{'content'}))                  ?  ($pubs{"issue"} =            $r->{'number'}->{'style'}->{'content'})                   : ($pubs{"issue"} = "NULL");
  (defined( $r->{'pages'}->{'style'}->{'content'}))                   ?  ($pubs{"pages"} =            $r->{'pages'}->{'style'}->{'content'})                    : ($pubs{"pages"} = "NULL");
  (defined( $r->{'electronic-resource-num'}->{'style'}->{'content'})) ?  ($pubs{"doi"} =              $r->{'electronic-resource-num'}->{'style'}->{'content'})  : ($pubs{"doi"} = "NULL");
  if (defined( $r->{'isbn'}->{'style'}->{'content'})) { 
    $pubs{"isbn"} =      $r->{'isbn'}->{'style'}->{'content'};     $pubs{"isbn"} =~ s/\R/; /g 
  } else { ($pubs{"isbn"} = "NULL") };
  if (defined( $r->{'custom2'}->{'style'}->{'content'})) { 
    $pubs{"pmid"} =      $r->{'custom2'}->{'style'}->{'content'};  $pubs{"pmid"} =~ s/\n/;/g; $pubs{"pmid"} =~ s/\t/;/g 
  } else { ($pubs{"pmid"} = "NULL") };
  if (defined( $r->{'abstract'}->{'style'}->{'content'})) { 
    $pubs{"abstract"} =  $r->{'abstract'}->{'style'}->{'content'}; $pubs{"abstract"} =~ s/[^[:ascii:]]+//g; $pubs{"abstract"} =~ s/\R/ /g; $pubs{"abstract"} =~ s/\t/ /g 
  } else { ($pubs{"abstract"} = "NULL") };
  # in the following "periodical" cases, use "journal" unless there is an alt-periodical,
  # and {'alt-periodical'}->{'abbr-1'} available. The trigger for this so far is "Theor Appl Genet".
  if (defined( $r->{'periodical'}->{'full-title'}->{'style'}->{'content'})) {
    $pubs{"journal"} = $r->{'periodical'}->{'full-title'}->{'style'}->{'content'}
  } 
  elsif (defined( $r->{'alt-periodical'}->{'abbr-1'}->{'style'}->{'content'})) {
    $pubs{"journal"} = $r->{'alt-periodical'}->{'abbr-1'}->{'style'}->{'content'}
  }
  elsif (defined( $r->{'alt-periodical'}->{'full-title'}->{'style'}->{'content'})) {
    $pubs{"journal"} = $r->{'alt-periodical'}->{'full-title'}->{'style'}->{'content'}
  }
  else { $pubs{"journal"} = "NULL" }; 
  
  if ($verbose) { foreach my $field (@fields) { print "$field: $pubs{$field}\n" } }
  
  # Handle the more complex hashes ...
  
  %pub_keywords = ();
  if ( defined( $r->{'keywords'}->{'keyword'} ) ) {
    my $keyw_ref = $r->{'keywords'}->{'keyword'};
    my $ct = 1;
    for my $kw ( @$keyw_ref ) {
      $pub_keywords{$ct} = $kw->{'style'}->{'content'}; 
      $ct++;
    }
  }
  
  %pub_authors = ();
  if ( defined( $r->{'contributors'}->{'authors'}->{'author'} ) ) {
    my $author_ref = $r->{'contributors'}->{'authors'}->{'author'};
    my $ct = 1;
    for my $au ( @$author_ref ) {
      $pub_authors{$ct} = $au->{'style'}->{'content'}; 
      $ct++;
    }
  }
  
  %pub_urls = ();
  if ( defined( $r->{'urls'}->{'related-urls'}->{'url'} ) ) {
    my $url_ref = $r->{'urls'}->{'related-urls'}->{'url'};
    my $ct = 1;
    for my $url ( @$url_ref ) {
      $pub_urls{$ct} = $url->{'style'}->{'content'}; 
      $pub_urls{$ct} =~ s/<Go to ISI>.+/NULL/i; # Kill these links, which aren't useful to us.
      $ct++;
    }
  }
  
  return %pubs, %pub_keywords, %pub_authors, %pub_urls; 
} # end sub parse_xml

__END__
NOTE: CUSTOMIZING ENDNOTE 
See EndNote Help topic: "Adding, Deleting, and Renaming Fields"

In the EndNote menu, choose Preferences, select the Reference Type option in the list of preferences, 
and click Modify Reference Types to open the Reference Types preference.
Use the drop-down list at the top to find the reference type that you want to change (choose Journal Article).
Select the "Modify Reference Types" button. In that window, scroll down until you see the fields Custom 1, Custom 2, etc.
Change "Custom 1" to publink_citation. This contains our citations, e.g. "Blair, Buendia et al., 2008a"
Change "Custom 2" to PMID      (will take PubMed IDs, e.g. 18784914)
Change "Custom 3" to species   (will take values like phavu, arahy, glyma, etc.)
Change "Custom 4" to status    (will take values 'not processed', 'in process', 'processed', 'uploaded'
Click "Apply to All Ref Types"
Clik OK

To add values in the new fields:
Double-click on a reference. The new fields should be present.
Add the value for publink_citation, e.g. "Smith, Jones et al., 2005a"

File menu / Export / 
    "Save file as type: XML"
    "Output style: Show All Fields"
    Export Selected References OFF (or check, but after "select"ing all references)

After export, publink_citation  will be in the xml tag <custom1>, and
              PMID              will be in the xml tag <custom2>, and
              species           will be in the xml tag <custom3>, and
              status            will be in the xml tag <custom4>
              
=====

VERSIONS

v01 Jul23'12 Basic functional version
v02 Jul23'12 Modify the EndNote field "Custom 1" to hold publink_citation 
              and the field "Custom 2" to hold map_description.
v03 Jul28'12 Add output options (-kv or default tabular), and file output to base-name. 
v04 Jul30'12 Add table for URLs, and field for species symbol. 
v05 Aug08'12 Cut editor reports for now - since QTL reports will seldom come from edited volumes or books.
v06 Aug20'12 Sync tables with those on-line at LIS. Also handle "alt-periodical" cases, such as TAG
v07 Aug31'12 Add flag to print or not-print headers on tables. 
              Switch field orders in pub_authors.
              Pluralize output file names (for rails - grrr.)
v08 May28'13 Change publink_citation to publink_citation some debugging and documentation
      Added this:
          $pubs{"doi"} =~ s/\n/ /g; # replace line return(s) with space, to handle cases with line return folowing  eg. "Artn 629"
v09 May29'13 Changed Custom 2 to PMID rather than map_description
      Removed "secondary_title" and "language"
      Replace <Go to ISI> URLs with NULL
v10 May30'13 Changed xml object reference to more recognizable variable name: xml_record_ref
      Restructuring in the main if-then-else block in parse_xml, to eliminate holdover values between records
      Don't print table name in the header.
v11 Sep26'13 S.Dash: If more than one URLs available skip the pubmed URL and print only one 
      non-pubmed URL (the pmid is already available in $pubs/PUBS.txt)     
v12 Sept4'14 SBC: Make a new output format for consistency with Tripal / Mainlab group
v13 2014-09-11: "Journal Article" --> "Journal" (pub_type ontology); Author to the form: <lastname> <initials with no .'s or spaces>

