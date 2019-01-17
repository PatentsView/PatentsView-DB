use strict;
use Data::Dumper;
use Text::CSV;
use XML::Simple;
use Email::Valid;
use Cwd qw(getcwd);


# This script takes a csv file in the format:
# "patent number,TwinArch set,GI title,GI statement"
# and parses out the funding agency via Stanford NER pre-existing output. It also identifies contract and award numbers
# Be sure to change file dates

# TODO: Capture NER errors gracefully

# original author: sarora@air.org
# Last updated: 7/19/2016

#read configuration file in 

#these four lines only commented out for debug
open( my $configFile, "/usr/local/airflow/config.txt") or die("Can't find config file!");
my @config_info = <$configFile>;
my $temp = "@config_info[0]";
chomp $temp;


#my $location = $temp . "/NER_output";

my $nerDir = "/project/Development/government_interest/NER/stanford-ner-2017-06-09";
#my $omitLocsFile = "/project/Development/persistent_files/omitLocs.csv";


my $starting_dir = getcwd;
my $omitLocsFile = $startingdir ."/Development/persistent_files/omitLocs.csv"



#my $workDir = $temp . "/government_interest";
#print($workDir);
my $inFile = $temp . "merged_csvs.csv";
my $outFile = "NER_output.txt";
my $distinctOrgsFile = "distinctOrgs.txt";
my $distinctLocsFile = "distinctLocs.txt";
my $file = "ner.txt";

my $data = (); # main data hash
my %distinctOrgs;
my %distinctLocs;

# NER globally visibile vars
my $NERFC= 5000;		 # specifies number of lines to feed NER per Java call 
my @classifiers = ('classifiers/english.all.3class.distsim.crf.ser.gz', 'classifiers/english.conll.4class.distsim.crf.ser.gz', 'classifiers/english.muc.7class.distsim.crf.ser.gz');
my @nerOutDirs = ('out-3class', 'out-4class', 'out-7class');
my $simple = XML::Simple->new(ForceArray => 1);
my %omitLocs;  # use to remove country variations from a list of locations

init ();
readData();
doNer();
process();
writeOutput();


sub cleanContracts () { 
  foreach my $pn (keys (%$data)) {
    next if ($data->{$pn}->{"hasLocation"} == 0);
    
    my $giStmt = $data->{$pn}->{"giStmt"};
    my @ids = @{$data->{$pn}->{"ids"}};

    if ($giStmt =~ m/San Diego(,)?/i) {
      my @results = grep (!/((CA\s)?92152(-\d{4,4})?|72120|20012|53510|D0012|53560)/, @ids);
      $data->{$pn}->{"ids"} = \@results;
    }

    if ($giStmt =~ m/Bethesda(,)?/i) {
      my @results = grep (!/20014|20892/, @ids);
      $data->{$pn}->{"ids"} = \@results;
    }
  }
}

sub init () {
    my $csv = Text::CSV->new({  binary => 1, sep_char => ',' });
  open (my $fh, "<", "$workDir/$omitLocsFile") or die "Cannot open $omitLocsFile: $!\n";
  my $header = $csv->getline($fh); # skip header

  while (my $line = $csv->getline($fh)) {
    chomp $line; 

    (my $loc = $line->[0]) =~ s,\n|\r,,sg;
    (my $freq = $line->[1]) =~ s,\n|\r,,sg;
    (my $omit = $line->[2]) =~ s,\n|\r,,sg;

    $omitLocs{$loc} = $omit if ($omit == 1); 
  }
}



sub doNer() {
  my $i = 0; my $fc = 0;
  my @nersIn = (); 
  #chdir ($nerDir) or die "Cannot change directories $!\n";
  my @patKeys = sort (keys (%$data));
  print $#patKeys;
  print "Working on ", ($#patKeys + 1), " records.\n";

  # clean up
  # comment out first run
  #`rm -f $nerDir/in/*`;
  #foreach my $d (@nerOutDirs) {
  #  `rm -f $nerDir/$d/*`;
  #}


  foreach my $pat (@patKeys) {
    my $giStmt = $data->{$pat}->{"giStmt"};
    # remove periods in certain acronyms that won't be captured by NER otherwise
    $giStmt =~ s/N\.I\.H\./NIH/sg; 
    $giStmt =~ s/N\.S\.F\./NSF/sg;
    $giStmt =~ s/N\.A\.S\.A\./NASA/sg;
    $giStmt =~ s/C\.D\.C\./CDC/sg;
    
    push (@nersIn, $giStmt);

    if ($#nersIn >= $NERFC || $i == $#patKeys ) {
      open FILE, ">", "$nerDir/in/$fc.txt" or die "Cannot open temporary  file $nerDir/in/$fc.txt $!\n";
      #open FILE, ">", "in/$fc.txt" or die "Cannot open temporary -here-  file in/$fc.txt $!\n";
      print FILE (join ("\n", @nersIn));
      close FILE;
      $fc++;
      @nersIn= ();
    }
    $i++;
  }

  `rm -f error.log`; # remove the error log before staring a new NER run
  #my $starting_dir = getcwd;
  chdir ($nerDir) or die "Cannot change directories $!\n";
  for (my $o = 0; $o <= $#classifiers; $o++) {
    print "Staring NER on: ", $classifiers[$o], "\n";
    my $dir = getcwd;
    print $dir;
    for (my $c = 0; $c < $fc; $c++) {
      print "\tRunning NER on in/$c.txt\n"; 
      my $cmd = "java -mx500m -classpath \"stanford-ner.jar;lib/*\" edu.stanford.nlp.ie.crf.CRFClassifier -loadClassifier $classifiers[$o] -textFile in/$c.txt -outputFormat inlineXML 2>> error.log";
     
      my $output = `$cmd`;

      #open FILE, ">", "$nerDir/$nerOutDirs[$o]/$c.txt" or die "Cannot open temporary file $nerOutDirs[$o]/$file.txt: $!\n";
      open FILE, ">", "$nerOutDirs[$o]/$c.txt" or die "Cannot open temporary file $nerOutDirs[$o]/$file.txt: $!\n";
      print FILE $output;
      close FILE;
    }
  }
}

sub readData() {
  # open input file csv
  print "Reading data...\n";
  my $csv = Text::CSV->new({  binary => 1, sep_char => ',' });
  open (my $fh, "<", "$workDir/$inFile") or die "Cannot open $inFile: $!\n";
  #my $header = $csv->getline($fh); # commenting this out adn just writing without header for now

  while (my $line = $csv->getline($fh)) {
    chomp $line;

    (my $patentNo = $line->[0]) =~ s,\n|\r,,sg;
    (my $twinArch = $line->[1]) =~ s,\n|\r,,sg;
    (my $giTitle = $line->[2]) =~ s,\n|\r,,sg;
    (my $giStmt = $line->[3]) =~ s,\n|\r,,sg;

    $data->{$patentNo}->{"twinArch"} = $twinArch;
    $data->{$patentNo}->{"giTitle"} = $giTitle;
    $data->{$patentNo}->{"giStmt"} = $giStmt;
  }
  
  # print Dumper ($data), "\n";
}


# open NER results file

sub process() {
  chdir ($starting_dir) or die "Cannot change directories back $!\n";
  my $i = 0; my $c = 0;
  my %ners; # holds all lines in an NERFC block across n classifiers
  foreach my $patNum (sort(keys(%$data))) {
    my ($ids, $hasPhoneNumber) = parseNums ($data->{$patNum}->{"giStmt"});
    $data->{$patNum}->{"hasPhoneNumber"} = $hasPhoneNumber;
    $data->{$patNum}->{"ids"} = $ids;

    if ($i == 0) {
      print "Opening NER files: $c\n";
      foreach (my $o = 0; $o <= $#nerOutDirs; $o++) {
	open (INF, "<$nerDir/$nerOutDirs[$o]/$c.txt") or die "Could not open $nerOutDirs[$o]/$c.txt\n";
  #open (INF, "$nerOutDirs[$o]/$c.txt") or die "Could not open this file $nerOutDirs[$o]/$c.txt\n";
	my @nerLines = <INF>;
	close INF; 
	chomp @nerLines;
	$ners{$o} = \@nerLines;
      }

      # print Dumper(\%ners), "\n";
    }
    
    my @corgs = ();
    my @clocs = ();
    foreach (my $o = 0; $o <= $#nerOutDirs; $o++) {
      my @nerLines = @{$ners{$o}};
      my $line = $nerLines[$i];
      my ($orgs, $locs, $hasLocation) = parseNer($line);
      $data->{$patNum}->{"hasLocation"}++ if ($hasLocation);
      push (@corgs, @$orgs);
      push (@clocs, @$locs);

      $data->{$patNum}->{$nerOutDirs[$o]} = $line;
    }
    
    
    my @ulocs = uniq(@clocs);
    my @uorgs = uniq(@corgs);
    $data->{$patNum}->{"locs"} = \@ulocs;
    $data->{$patNum}->{"orgs"} = \@uorgs;

    if ($i++ >= $NERFC) {
      $i=0; $c++;
    }
  }

  cleanContracts();
  # print Dumper ($data), "\n";
}

sub trimWord ($) {
  my $word = shift; 
  # remove leading and trailing punctuation only
  $word =~ s,(^[[:punct:]])+|([[:punct:]]+$),,sg;
  return $word;
}

sub parseNums () {
  my @in = split (" ", shift); # punctuation in tact
  my @words = map ( trimWord($_), @in); # trim all beginning and ending punctuation 
  my %candidates;  # all things that look like contract or award numbers
  my %ids;  # what we deem to be an actual contract or award number 
  my $hasPhoneNumber = 0;

  # find all base contract and award segments 
  for (my $i=0; $i <= $#words; $i++) {
    (my $noPunctWord = $words[$i]) =~ s,[[:punct:]],,sg; 
    # grab all words with alphanumeric combinations or words with 5+ digits, signalling an award or contract number
    if ( ( $noPunctWord =~ m,\d+, && $noPunctWord =~ m,[[:alpha:]], && length($noPunctWord) > 3 ) || $noPunctWord =~ m/\d{5,}/ ) {
      $candidates{$i} = $words[$i];
    }
  }

  my %removes;  # to remove candidate ids that are merged below

  # for each candidate key, find compound keys across white space 
  for my $pos (keys(%candidates)) {
    my $val = $candidates{$pos};
    # check to see if the previous words are between one and four characters and consists of cap letters and/or numbers,
    # in which case it should be part of the identifier.  Mind the punctuation in the original word
    my $id = $val;
    for (my $pb = $pos - 1; $pb >= 0; $pb--) {
      if ( ($words[$pb] =~ m/^[A-Z0-9]{1,6}$/  && $words[$pb] !~ m,^A$, && $in[$pb] !~ m,[[:punct:]]$,) || 
	   ( $in[$pb] =~ m/\(\d{3,3}\)/ ) ) {  # match an identifier or an area code
	$id = $words[$pb] . " " . $id;
	$removes{$words[$pb]} = 1;
      } 
      else {
	last;
      }
    }
    
    # do the same check but forward this time.  Again mind the punctuation in the original word
    my $stop;
    if ($in[$pos] =~ m,[[:punct:]]$, ) {
      $stop = 1;
    }
    else { 
      $stop = 0;
    }
    for (my $pf = $pos + 1; $pf <= $#words; $pf++) {
      # print ("Looking forward to trimmed word $words[$pf] via $in[$pf] with stop value $stop", "\n");
      if ( $words[$pf] =~ m/^[A-Z0-9]{1,6}$/  && $words[$pf] !~ m,^A$, && !$stop ) { 
	$id = $id . " " . $words[$pf];
	$removes{$words[$pf]} = 1;
	$stop = 1 if ( $in[$pf] =~ m,[[:punct:]]$, );
      } else {
	last;
      }
    }

    $ids{$id} = 1;
  }
  
  # print "\tAll ids for record: ", Dumper (\%ids);
  # print "\tAll removes for record: ", Dumper (\%removes);
  
  # remove all items from the ids array
  my @diff = grep { not exists $removes{$_} } keys (%ids);
  
  # strip phone numbers
  my @noPhone = grep (!/^(\+?1?(\s|-)?\(?[0-9]{3,3}\)?)(\s|-)?[0-9]{3,3}(-|\s)+[0-9]{4,4}$/, @diff );
    $hasPhoneNumber = 1 if ($#noPhone < $#diff );
  
  # strip emails.  note: grep and match on @ does not seem to work :(
  my @noEmail = ();
  foreach my $np (@noPhone) {
    if (! Email::Valid->address($np)) {
      push (@noEmail, $np);
    }
  }
  return (\@noEmail, $hasPhoneNumber);
}


sub parseNer () {
  my $output = shift;
  my @orgs = ();
  my @locs = ();
  my $hasLocation = 0; 

  my $xml = "<root>" . $output . "</root>";
  $xml =~ s,&,and,sg;



  my $hr;
  eval { $hr = $simple->XMLin($xml); };


  if ($@) {
    warn "Cannot parse $xml\n" ;
    return (\@orgs, \@locs, $hasLocation);
  }

  if ($output !~ m,<PERSON>|<ORGANIZATION>|<LOCATION>, ) {
    # print "\tSkipping this piece of text because no entities have been found.\n";
    return (\@orgs, \@locs, $hasLocation);
  }

  # needed for removing address elements that might be conflated with contract and award identifiers.
  # also need to difference out US country variations
  if ( defined ($hr->{LOCATION} )) {
    my @raw = @{$hr->{LOCATION}};

    foreach my $r (@raw) {
      $distinctLocs{$r}++;
    }

    my @unique = uniq(@raw);
    @locs = grep { not exists $omitLocs{$_} } @unique;
    $hasLocation = 1 if ( $#locs >= 0 ); 
  }

  if ( defined ($hr->{ORGANIZATION} ) ) {
    my @raw = @{$hr->{ORGANIZATION}};
    @orgs = uniq(@raw);
    # Print "\tFound organizations: ", join (",", @orgs), "\n";
    foreach my $org (@orgs) {
      $distinctOrgs{$org}++;
    }
  }
  return (\@orgs, \@locs, $hasLocation);
}



sub writeOutput() {
  # print Dumper ($data), "\n";
  my $dir = getcwd;
  print $dir;

  open (OUF, ">$workDir/$outFile") or die "Cannot open here! $outFile: $!\n";
  print OUF "Patent number\tTwinArch set\tGI title\tGI statement\tOrgs\tContract/award(s)\tHas address\tHas phone\n"; 

  foreach my $patNum (sort(keys(%$data))) {
    my $orgs = $data->{$patNum}->{"orgs"};
    my $ids = $data->{$patNum}->{"ids"};
    my $out = $patNum . "\t" . $data->{$patNum}->{"twinArch"} . "\t" .  $data->{$patNum}->{"giTitle"} . "\t" . $data->{$patNum}->{"giStmt"};
    if ( defined ($orgs) ) {
      $out .= "\t" . join("|", @$orgs); 
    } else {
      $out .= "\t";
    }
    $out .= "\t\"" . join ("|", @$ids) . "\"";
    $out .= "\t" . $data->{$patNum}->{"hasLocation"} . "\t" . $data->{$patNum}->{"hasPhoneNumber"};
     print OUF $out, "\n";
  }

  close OUF;

  open (ORGSF, ">$workDir/$distinctOrgsFile") or die "Cannot open $distinctOrgsFile: $!\n";
  print ORGSF "Organization\tInstanceCount\n";
  foreach my $distinctOrg (keys (%distinctOrgs)) {
    print ORGSF "$distinctOrg\t$distinctOrgs{$distinctOrg}\n";
  }
  close ORGSF;

  open (LOCSF, ">$workDir/$distinctLocsFile") or die "Cannot open $distinctLocsFile: $!\n";
  print LOCSF "Location\tInstanceCount\n";
  foreach my $distinctLoc (keys (%distinctLocs)) {
    print LOCSF "$distinctLoc\t$distinctLocs{$distinctLoc}\n";
  }
  close LOCSF;

}

sub uniq {
  my %seen;
  return grep { !$seen{$_}++ } @_;
}
