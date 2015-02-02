# Fill in the appropiate strings for $connect_str, $user, and $pass.
# The connect string will look like:
#   DBI:Pg:dbname=drupal;host=<host>;port=<port>
# Or
#   DBI:Pg:dbname="drupal"
# If you are working from a server with Nathan's Postgres authentication
# settings (peanutbase & legumeinfo servers)

sub connectToDB {
  my $connect_str = 'DBI:Pg:dbname="drupal"';
  my $user        = '';
  my $pass        = '';

  # $g_connect_str, $g_user, and $g_pass defined in db.pl
  my $dbh = DBI->connect($connect_str, $user, $pass);

  $dbh->{AutoCommit} = 0;  # enable transactions, if possible
  $dbh->{RaiseError} = 1;

  return $dbh;
}#connectToDB

1;
