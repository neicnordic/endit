#!/usr/bin/perl

#   ENDIT - Efficient Northern Dcache Interface to TSM
#   Copyright (C) 2006-2017 Mattias Wadenstein <maswan@hpc2n.umu.se>
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

use IPC::Run3;
use POSIX qw(strftime);
use File::Temp qw /tempfile/;
use File::Basename;

# Add directory of script to module search path
use lib dirname (__FILE__);

use Endit qw(%conf readconf printlog getusage);

$Endit::logsuffix = 'tsmarchiver.log';

readconf();

my $filelist = "tsm-archive-files.XXXXXX";

# Try to send warn/die messages to log file
INIT {
        $SIG{__DIE__}=sub {
                printlog("DIE: $_[0]");
        };

        $SIG{__WARN__}=sub {
                print STDERR "$_[0]";
                printlog("WARN: $_[0]");
        };
}

$SIG{INT} = sub { printlog("Got SIGINT, exiting..."); exit; };
$SIG{QUIT} = sub { printlog("Got SIGQUIT, exiting..."); exit; };
$SIG{TERM} = sub { printlog("Got SIGTERM, exiting..."); exit; };

my $desclong="";
if($conf{'desc-long'}) {
	$desclong = " $conf{'desc-long'}";
}
printlog("$0: Starting$desclong...");

my $timer;
while(1) {
	my $dir = $conf{'dir'} . '/out/';

        opendir(my $dh, $dir) || die "opendir $dir: $!";
        my @files = grep { /^[0-9A-Fa-f]+$/ } readdir($dh);
        closedir($dh);

	if(!scalar(@files)) {
		# No files, just sleep until next iteration.
		sleep($conf{sleeptime});
		next;
	}

	my $usage = getusage($dir, @files);

	my $usagestr = sprintf("%.03f GiB in %d files", $usage, scalar(@files));

	my $triggerthreshold;
	# Assume threshold1_usage is smaller than threshold2_usage etc.
	for my $i (reverse(1 .. 9)) {
		my $at = "archiver_threshold${i}";
		next unless($conf{"${at}_usage"});

		if($usage >= $conf{"${at}_usage"}) {
			$triggerthreshold = $at;
			printlog "$at triggers" if($conf{debug});
			last;
		}
	}
	if(!$triggerthreshold) {
		if(!defined($timer)) {
			$timer = 0;
		}
		if($timer < $conf{archiver_timeout}) {
			printlog "Only $usagestr used, sleeping a while (slept $timer s)" if($conf{debug});
			sleep $conf{sleeptime};
			$timer += $conf{sleeptime};
			next;
		}
	}

	$timer = undef;

	my $logstr = "Trying to archive $usagestr from $dir";

	if($conf{verbose}) {
		$logstr .= " (files: " . join(" ", @files) . ")";
	}

	printlog $logstr;
	$logstr = undef;

	my $dounlink = 1;
	$dounlink=0 if($conf{debug});
	my ($fh, $fn) = tempfile($filelist, DIR=>$conf{'dir'}, UNLINK=>$dounlink);
	print $fh map { "$conf{'dir'}/out/$_\n"; } @files;
	close($fh) || die "Failed writing to $fn: $!";

	my @dsmcopts = split /, /, $conf{'dsmcopts'};
	if(!$triggerthreshold && $conf{archiver_timeout_dsmcopts}) {
		printlog "Adding archiver_timeout_dsmcopts " . $conf{archiver_timeout_dsmcopts} if($conf{debug});
		push @dsmcopts, split(/, /, $conf{archiver_timeout_dsmcopts});
	}
	if($triggerthreshold && $conf{"${triggerthreshold}_dsmcopts"}) {
		printlog "Adding ${triggerthreshold}_dsmcopts " . $conf{"${triggerthreshold}_dsmcopts"} if($conf{debug});
		push @dsmcopts, split(/, /, $conf{"${triggerthreshold}_dsmcopts"});
	}
	my $date=strftime("%Y-%m",localtime());
	my @cmd = ('dsmc','archive','-deletefiles', @dsmcopts,
		"-description=endit-$date","-filelist=$fn");
	printlog "Executing: " . join(" ", @cmd) if($conf{debug});
	my $execstart = time();
	my ($out,$err);
	if((run3 \@cmd, \undef, \$out, \$err) && $? ==0) { 
		my $duration = time()-$execstart;
		$duration = 1 unless($duration);
		my $stats = sprintf("%.2f MiB/s (%.2f files/s)", $usage*1024/$duration, scalar(@files)/$duration);
		printlog "Archive operation successful, duration $duration seconds, average rate $stats";
		printlog $out if $conf{'debug'};
		# files migrated to tape without issue
	} else {
		# something went wrong. log and hope for better luck next time?
		my $msg = "dsmc archive failure: ";
		if ($? == -1) {
			$msg .= "failed to execute: $!";
		}
		elsif ($? & 127) {
			$msg .= sprintf "child died with signal %d, %s coredump",
			       ($? & 127),  ($? & 128) ? 'with' : 'without';
		}
		else {
			$msg .= sprintf "child exited with value %d\n", $? >> 8;
		}
		printlog "$msg";
		printlog "STDERR: $err";
		printlog "STDOUT: $out";

		# Avoid spinning on persistent errors.
		sleep $conf{sleeptime};
	}
	unlink($fn) unless($conf{debug});
}
