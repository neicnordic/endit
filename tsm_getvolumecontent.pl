#!/usr/local/bin/perl
#
# vim:ts=4:sw=4:et:

use warnings;
use strict;

use Getopt::Std;
use HPC2NTSMUtil;

use Data::Dumper;

# Global variables
my ($rev);
$rev = '$Revision$';
my (%opts);


sub debug
{
    my (@args) = @_;

    if( !defined($opts{'D'})) {
        return;
    }

    print scalar(localtime), " : ", @args, "\n";
}

#############################################################
# Main
#

getopts('hf:Dku:p:N:d', \%opts) || die "Argument error";

if(defined($opts{'h'})) {
    print <<EOH;
   Get volume-file mapping data from TSM
   $0 usage:
        -h      - This help
        -f file - Destination file to store result in (REQUIRED)
        -D      - Debug output
        -k      - Keep file path
        -u id   - TSM User ID (REQUIRED)
        -p pass - File with cleartext password for above user ID (REQUIRED)
        -N node - Proxynode name to generate list for (REQUIRED)
EOH
    exit 0;
}

#############
# NORMAL MODE

foreach my $o (qw(u p N f)) {
    die "$0: option -$o required" unless(defined($opts{$o}));
}

setauth(id => $opts{u}, passfile => $opts{p});

debug "Starting:";


# Find out which storagepools are used by the node.
my @stgpools = dsm_cmd("select STGPOOL_NAME from occupancy where node_name='$opts{N}'") or die "Couldn't list stgpools for node $opts{N}";

my @volumes;

# List volumes in the storage pools
foreach my $stgpool (sort @stgpools) {
    my @t=dsm_cmd("q vol stg=$stgpool access=readwrite,readonly status=online,filling,full") or die "Failed listing vols for $stgpool";
    foreach my $line (@t) {
        my($volname, undef, undef, undef, undef, undef) = split (/\t/, $line);
        push @volumes, $volname;
    }
}

my %files;

# List the contents of all volumes
foreach my $volume (@volumes) {
    debug "Getting content of volume $volume ...";
    my @t=dsm_cmd("q content $volume node=$opts{N} damaged=no f=d") or die "Failed to query content on $volume";
    foreach my $line (@t) {
        my($nodename, $type, $fsname, $hexfsname, $fsid, $filename, $hexfilename, $isaggr, $size, $segment, $cached) = split (/\t/, $line);

        # Use the location of first segment of files on seq access volumes
        next if ($segment && $segment !~ m!^1/!);

        if($opts{k}) {
            $filename="$fsname$filename";
        }
        else {
            $filename =~ s!.*/!!;
        }
        $files{$filename} = $volume;
    }
    debug " Found " . scalar @t . " entries";
}

debug "Finished, total " . scalar (keys %files) . " entries";

debug "Writing text file $opts{f}";
open(my $fh, '>', $opts{f}) || die "Unable to open $opts{f}: $!";
my($key,$value);
while (($key,$value) = each %files) {
    print $fh "$key\t$value\n" || die "Writing $opts{f}: $!";
}
close $fh || die "Closing $opts{f}: $!";
debug "Done.";
exit(0);
