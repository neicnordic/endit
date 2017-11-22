#!/usr/local/bin/perl

#   tsm_getvolumecontent.pl - helper script to show tsm tape contents
#   Copyright (C) 2012-2017 <Niklas.Edmundsson@hpc2n.umu.se>
#
#   This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program. If not, see <http://www.gnu.org/licenses/>.

# vim:ts=4:sw=4:et:

use warnings;
use strict;

use Getopt::Std;
use File::Basename;

# Add directory of script to module search path
use lib dirname (__FILE__);
use HPC2NTSMUtil;

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

$opts{N} = "\U$opts{N}";

debug "Node: $opts{N}";


# Find out which storagepools are used by the node.
my @stgpools = dsm_cmd("select STGPOOL_NAME from occupancy where node_name='$opts{N}'") or die "Couldn't list stgpools for node $opts{N}";

if(! @stgpools) {
	die "No stgpools found for node $opts{N}";
}

foreach(@stgpools) {
	debug "stgpool: $_";
}

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

	# Just skip ahead if no filename, likely means no files on volume!
	next unless($filename);

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

my $tmpf = "$opts{f}.$$"; # Assume we're alone in the destdir
debug "Writing volume contents to file $tmpf";
open(my $fh, '>', $tmpf) || die "Unable to open $opts{f}: $!";
my($key,$value);
while (($key,$value) = each %files) {
    print $fh "$key\t$value\n" || die "Writing $opts{f}: $!";
}
close $fh || die "Closing $tmpf: $!";
debug "Done. Renaming $tmpf to $opts{f}";
rename($tmpf, $opts{f}) || die "Rename $tmpf to $opts{f}: $!";

debug "All done. Exiting.";
exit(0);
