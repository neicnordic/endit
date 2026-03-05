#!/usr/bin/perl 

#   ENDIT - Efficient Northern Dcache Interface to TSM
#   Copyright (C) 2022-2026 Mattias Wadenstein <maswan@hpc2n.umu.se>
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

use warnings;
use strict;
use JSON;
use Cwd qw(abs_path);

use Getopt::Long;
Getopt::Long::Configure (qw(no_ignore_case no_auto_abbrev));

my %opts = (
	# JSON formatted defaults file, to override the example defaults
	# for easy multi-usecase scenarios.
	defaultsfile	=> undef,
	getdefaults	=> undef,

	# Create commands to issue reads from this pool
	cmdtemplate	=> '\s %src% rh restore %id%',
	# %src% identifier default for above
	cmdsrc		=> 'sourcepool',
	# Source tape hints file
	sourcehintfile	=> undef,
	# Optional destination tape hints file
	desthintfile	=> undef,

	# Output command files in this directory
	outdir		=> "list",
	force		=> 0,

	# Optional: Chunk tape reads by this many bytes, usecase for rate
	# limiting reads is to not run out of read tape pool buffer size.
	# A typical reason for this is when the target write tape pool is
	# slower than the source read tape pool.
	# Should not be an even multiple of tape size, and this script support
	# splitting into at most 100 chunks.
	splitby		=> 0,
);


my($help);

sub usage()
{
	print <<EOH;
Usage:
	--defaults, -j		Defaults commented JSON file (optional)
	--getdefaults, -J	Only show defaults in commented JSON file format
	--sourcehints, -s	Source hint file (required)
	--desthints, -d		Destination hint file (optional)
	--cmdtemplate, -c	Command template (default example: $opts{cmdtemplate})
	--srcarg, -S		%src% argument for the template (default example: $opts{cmdsrc})
	--outdir, -D		Output directory (default: $opts{outdir})
	--force, -f		Force using an existing output directory
	--splitby, -b		Split lists by this size in bytes (default: $opts{splitby})
EOH
	exit 1;
}

GetOptions (
		"defaults|j=s" => \$opts{defaultsfile},
		"getdefaults|J=s" => \$opts{getdefaults},
		"sourcehints|s=s" => \$opts{sourcehintfile},
		"desthints|d=s" => \$opts{desthintfile},
		"cmdtemplate|commandtemplate|c=s" => \$opts{cmdtemplate},
		"srcarg|cmdsrc|S=s" => \$opts{cmdsrc},
		"outdir|D=s" => \$opts{outdir},
		"force|f" => \$opts{force},
		"splitby|b=i" => \$opts{splitby},
		"help|h" => \$help,
	)
	or die("Failed to parse command line arguments (1)");

if(scalar(@ARGV) > 0) {
	die("Failed to parse command line arguments (2)");
}

my $defaults;

# Load the defaults file before showing usage so the correct overridden
# defaults are shown.
if($opts{defaultsfile}) {
	my $df;
	my $infile;

	foreach my $f ($opts{defaultsfile},"$opts{defaultsfile}.json") {
		if(open $df, '<', $f) {
			$infile = $f;
			last;
		}
	}
	die "Neither defaults file $opts{defaultsfile} nor $opts{defaultsfile}.json found" unless($infile);

	# Read file and remove comments.
	my @file = grep(!/^\s*#/, (<$df>));

	close($df) or die "Closing $infile: $!";

	eval {
		local $SIG{__WARN__} = sub {};
		local $SIG{__DIE__} = sub {};

		# join everything into one line and decode.  decode_json croaks
		# on error, resulting in program exit on error.
		$defaults = decode_json(join "", @file);
	};
	if($@) {
		die "Parsing $infile as commented JSON failed: $@";
	}

	# Apply the defaults. No syntax checking of the values read.
	foreach my $v (qw(sourcehintfile desthintfile cmdtemplate cmdsrc outdir splitby force))
	{
		next unless($defaults->{$v});

		$opts{$v} = $defaults->{$v};
	}
}

usage() if($help);

if($opts{getdefaults}) {
	my $h;

	foreach my $v (qw(sourcehintfile desthintfile cmdtemplate cmdsrc outdir splitby force))
	{
		next if($v eq 'cmdsrc' and $opts{cmdtemplate} !~ /%src%/);
		$h->{$v} = $opts{$v} if($opts{$v});
	}

	if(-e $opts{getdefaults}) {
		die "Cowardly refusing to overwrite $opts{getdefaults}";
	}

	open(my $out, '>', $opts{getdefaults}) or die "open $opts{getdefaults}: $!";

	my $me = abs_path($0);
	print $out "#! $me -j\n";
	print $out "# $0 defaults " . scalar(localtime(time())) . "\n";
	print $out to_json($h, {canonical => 1, pretty => 1}) or die;
	close $out or die "close $out: $!";

	chmod(0755, $opts{getdefaults}) or die "chmod $opts{getdefaults}: $!";

	print "Wrote $opts{getdefaults} in commented JSON format containing current defaults\nExiting...\n";
	exit 0;
}

unless($opts{sourcehintfile}) {
	warn "sourcehints required";
	usage();
}

my $sourcehints;

open my $sh, '<', $opts{sourcehintfile} or die "open $opts{sourcehintfile}: $!";
eval {
	local $SIG{__WARN__} = sub {};
	local $SIG{__DIE__} = sub {};

	$sourcehints = decode_json(<$sh>);
};
close($sh) or die "Closing $opts{sourcehintfile}: $!";
if($@) {
	die "Parsing $opts{sourcehintfile} as JSON failed: $@";
}

my $desthints;

if($opts{desthintfile}){
	open my $dh, '<', $opts{desthintfile} or die "open $opts{desthintfile}: $!";
	eval {
		local $SIG{__WARN__} = sub {};
		local $SIG{__DIE__} = sub {};

		$desthints = decode_json(<$dh>);
	};
	close($dh) or die "Closing $opts{desthintfile}: $!";
	if($@) {
		die "Parsing $opts{desthintfile} as JSON failed: $@";
	}
}

if(-d $opts{outdir}) {
	die "Directory $opts{outdir} already exists, specify a non-existing directory with -D or override with -f" unless($opts{force});
}
else {
	mkdir $opts{outdir} or die "mkdir $opts{outdir}: $!";
	print "Created directory $opts{outdir}\n";
}

my %size;

foreach my $id (sort { $sourcehints->{$a}{order} cmp $sourcehints->{$b}{order} }keys %$sourcehints) {
	next if defined $desthints->{$id}{volid};
	$size{$sourcehints->{$id}{volid}}+=$sourcehints->{$id}{size};
	my $f = "$opts{outdir}/$sourcehints->{$id}{volid}" . sprintf(".%02d",$size{$sourcehints->{$id}{volid}}/$opts{splitby});
	open(my $fh, ">>", $f) or die "open $f: $!";
	my $s = $opts{cmdtemplate};
	$s =~ s/%src%/$opts{cmdsrc}/g;
	$s =~ s/%id%/$id/g;
	print $fh  "$s\n";
	close($fh) or die "Closing $f: $!";
}
