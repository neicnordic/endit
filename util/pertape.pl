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
use List::Util qw(max);
use File::Basename;

use Getopt::Long;
Getopt::Long::Configure (qw(no_ignore_case no_auto_abbrev));

#FIXME: Remove
use Data::Dumper;
$Data::Dumper::Indent = 1;
$Data::Dumper::Sortkeys = 1;

my %optsdefault = (
	# JSON formatted defaults file, to override the example defaults
	# for easy multi-usecase scenarios.
	defaultsfile	=> undef,
	writedefaults	=> undef,

	# Create commands to issue reads from this pool
	cmdtemplate	=> '\s %src% rh restore %id%',
	# %src% identifier default for above
	cmdsrc		=> 'sourcepool',
	# %target% identifier default for above
	cmdtarget	=> undef,
	# Source tape hints file
	sourcehintfile	=> undef,
	# Optional destination tape hints file
	desthintfile	=> undef,

	# Output command files in this directory
	outdir		=> "list",
	# Or to this JSON file instead
	outjson		=> undef,
	# Overwrite existing output
	force		=> 0,


	# Optional: Chunk tape reads by this many bytes, usecase for rate
	# limiting reads is to not run out of read tape pool buffer size.
	# A typical reason for this is when the target write tape pool is
	# slower than the source read tape pool.
	# Should not be an even multiple of tape size, and this script support
	# splitting into at most 100 chunks.
	splitby		=> 0,
);

my %opts;

my($help);

sub usage()
{
	print basename($0)." with ".($opts{defaultsfile}//"default")." settings loaded.\nUsage:\n";
	print "  --defaults, -j	Defaults from commented JSON file (".($opts{defaultsfile}//"optional").")\n";
	print "  --writedefaults, -w	Write defaults to commented JSON file\n";
	print "  --sourcehints, -s	Source hint file (".($opts{sourcehintfile}//"required").")\n";
	print "  --desthints, -d	Destination hint file (".($opts{desthintfile}//"optional").")\n";
	print "  --cmdtemplate, -c	Command template ($opts{cmdtemplate})\n";
	print "  --srcarg, -S		%src% argument for the template ($opts{cmdsrc})\n";
	print "  --targetarg, -T	%target% argument for the template (".($opts{cmdtarget}//"optional").")\n";
	print "  --outdir, -D		Output directory ($opts{outdir})\n";
	print "  --outjson, -O		Output as JSON to this file instead (".($opts{outjson}//"optional").")\n";
	print "  --force, -f		Force using an existing output directory/file (".($opts{force}?"true":"default false").")\n";
	print "  --splitby, -b		Split lists by this size in bytes (".($opts{splitby}?"$opts{splitby}":"default 0/disabled").")\n";
	exit 1;
}

GetOptions (
		"defaults|j=s" => \$opts{defaultsfile},
		"writedefaults|w=s" => \$opts{writedefaults},
		"sourcehints|s=s" => \$opts{sourcehintfile},
		"desthints|d=s" => \$opts{desthintfile},
		"cmdtemplate|commandtemplate|c=s" => \$opts{cmdtemplate},
		"srcarg|cmdsrc|S=s" => \$opts{cmdsrc},
		"targetarg|cmdtarget|T=s" => \$opts{cmdtarget},
		"outdir|D=s" => \$opts{outdir},
		"outjson|O=s" => \$opts{outjson},
		"force|f" => \$opts{force},
		"splitby|b=i" => \$opts{splitby},
		"help|h" => \$help,
	)
	or die("Failed to parse command line arguments (1)");

if(scalar(@ARGV) > 0) {
	die("Failed to parse command line arguments (2)");
}

# Load the defaults file before showing usage so the correct overridden
# defaults are shown.
if($opts{defaultsfile}) {
	my $df;
	my $infile;
	my $defaults;

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
	foreach my $v (qw(sourcehintfile desthintfile cmdtemplate cmdsrc cmdtarget outdir splitby outjson force))
	{
		next unless($defaults->{$v});

		$opts{$v} = $defaults->{$v};
	}
}


# Finally apply the defaults unless overridden.
foreach my $k (keys %optsdefault) {
	next unless(defined($optsdefault{$k}));
	next if($opts{$k});

	$opts{$k} = $optsdefault{$k};
}

usage() if($help);

if($opts{cmdtemplate} =~ /%target%/ and !$opts{cmdtarget}) {
	die "Using %target% without defining it, specify with -T";
}

if($opts{writedefaults}) {
	my $h;

	foreach my $v (qw(sourcehintfile desthintfile cmdtemplate cmdsrc cmdtarget outdir splitby outjson force))
	{
		next if($v eq 'cmdsrc' and $opts{cmdtemplate} !~ /%src%/);
		next if($v eq 'cmdtarget' and $opts{cmdtemplate} !~ /%target%/);
		next unless(exists $opts{$v});
		next unless($opts{$v});
		next if($optsdefault{$v} and $optsdefault{$v} eq $opts{$v});
		$h->{$v} = $opts{$v};
	}

	if(-e $opts{writedefaults}) {
		die "Cowardly refusing to overwrite $opts{writedefaults}";
	}

	open(my $out, '>', $opts{writedefaults}) or die "open $opts{writedefaults}: $!";

	my $me = abs_path($0);
	print $out "#! $me -j\n";
	print $out "# $0 defaults " . scalar(localtime(time())) . "\n";
	print $out to_json($h, {canonical => 1, pretty => 1}) or die;
	close $out or die "close $out: $!";

	chmod(0755, $opts{writedefaults}) or die "chmod $opts{writedefaults}: $!";

	print "Wrote $opts{writedefaults} in commented JSON format containing current defaults\nExiting...\n";
	exit 0;
}

unless($opts{sourcehintfile}) {
	warn "sourcehints required";
	usage();
}

if($opts{splitby} <= 0) {
	$opts{splitby} = ~0; # Effectively disabled
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

if($opts{outjson}) {
	if(-f $opts{outjson}) {
		die "File $opts{outjson} already exists, specify a non-existing file with -O or override with -f" unless($opts{force});
	}
}
else {
	if(-d $opts{outdir}) {
		die "Directory $opts{outdir} already exists, specify a non-existing directory with -D or override with -f" unless($opts{force});
	}
	else {
		mkdir $opts{outdir} or die "mkdir $opts{outdir}: $!";
		print "Created directory $opts{outdir}\n";
	}
}

my %size;
my %list;

# Build list of eligible items
foreach my $id (sort { $sourcehints->{$a}{order} cmp $sourcehints->{$b}{order} }keys %$sourcehints) {
	next if defined $desthints->{$id}{volid};
	$size{$sourcehints->{$id}{volid}}+=$sourcehints->{$id}{size};
	push @{$list{$sourcehints->{$id}{volid}}{int($size{$sourcehints->{$id}{volid}}/$opts{splitby})}}, $id;
}

print Dumper(\%list) if($ENV{DEBUG});

# Output JSON if that's preferred
if($opts{outjson}) {
	# A single unprocessed JSON file for the user to process as they see fit
	open(my $fh, ">", $opts{outjson}) or die "open $opts{outjson}: $!";
	print $fh encode_json(\%list);
	close($fh) or die "Closing $opts{outjson}: $!";
	print "Created file $opts{outjson}\n";
	exit 0;
}

# Find width needed
my $w = 0;
foreach my $v (sort keys %list) {
	foreach my $n (sort keys %{$list{$v}}) {
		$w = max($w, length sprintf("%d", $n));
	}
}

# Process and provide text file based output
foreach my $v (sort keys %list) {
	foreach my $n (sort keys %{$list{$v}}) {
		my $ns = sprintf("%0${w}d", $n);
		my $f = "$opts{outdir}/$v.$ns";
		open(my $fh, ">", $f) or die "open $f: $!";

		if($opts{cmdtemplate} =~ /%id(|q)list%/) {
			# %idlist% -  a single command per tape with comma
			# separated IDs.
			# %idqlist% - a single command per tape with quoted
			# comma separated IDs.
			my $idlist;
			if($1 eq 'q') {
				$idlist="'".join("','", @{$list{$v}{$n}})."'";
			}
			else {
				$idlist=join(",", @{$list{$v}{$n}});
			}
			my $s = $opts{cmdtemplate};
			$s =~ s/%src%/$opts{cmdsrc}/g;
			$s =~ s/%id(|q)list%/$idlist/;
			$s =~ s/%target%/$opts{cmdtarget}/g if($opts{cmdtarget});
			print $fh  "$s\n";
		}
		else {
			# %id% - multiple commands per tape, each with a single
			# pnfs per command.
			foreach my $id (@{$list{$v}{$n}}) {
				my $s = $opts{cmdtemplate};
				$s =~ s/%src%/$opts{cmdsrc}/g;
				$s =~ s/%id%/$id/g;
				$s =~ s/%target%/$opts{cmdtarget}/g if($opts{cmdtarget});
				print $fh  "$s\n";
			}
		}

		close($fh) or die "Closing $f: $!";
	}
}
