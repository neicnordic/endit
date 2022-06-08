#!/usr/bin/perl

#   ENDIT - Efficient Northern Dcache Interface to TSM
#   Copyright (C) 2006-2017 Mattias Wadenstein <maswan@hpc2n.umu.se>
#   Copyright (C) 2018-2020 <Niklas.Edmundsson@hpc2n.umu.se>
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

use POSIX qw(strftime WNOHANG);
use File::Temp qw /tempfile/;
use File::Basename;

# Add directory of script to module search path
use lib dirname (__FILE__);

use Endit qw(%conf readconf printlog);

###########
# Variables
$Endit::logsuffix = 'tsmarchiver.log';
my $filelist = "tsm-archive-files.XXXXXX";
my $skipdelays = 0; # Set by USR1 signal handler
my @workers;
my $dsmcpid; # used by spawn_worker() signal handler

##################
# Helper functions

sub killchildren() {
        foreach(@workers) {
                kill("TERM", $_->{pid});
        }
}


# Return filessystem usage (gigabytes) given a hash reference containing
# contents and stat() size info
sub getusage {
	my ($href) = @_;

        my $size = 0;

        while(my ($k, $v) = each %{$href}) {

                $size += $v->{size}
        }

        return $size/(1024*1024*1024); # GiB
}


# Get directory contents together with partial stat() info
# Arguments:
# $dir - directory name
# $href - hash reference to return content in
sub getdir {
        my ($dir, $href) = @_;

        opendir(my $dh, $dir) || die "opendir $dir: $!";

        while(my $f = readdir($dh)) {
                next unless $f =~ /^[0-9A-Fa-f]+$/;
                my ($size, $mtime) = (stat("$dir/$f"))[7,9];
                next unless $mtime;

                $href->{$f}{size} = $size;
                $href->{$f}{mtime} = $mtime;
        }
        closedir($dh);
}

sub spawn_worker {
	my($msg, $filelist, $description, $usage_gib, $numfiles) = @_;

	my $pid = fork();

	die "cannot fork" unless defined $pid;

	if($pid) {
		# Parent process
		return $pid;
	}

	# Child process
	# printlog():s in child gets the child pid

	printlog $msg;

	sub killchild() {
		if(defined($dsmcpid)) {
			kill("TERM", $dsmcpid);
		}
	}

	$SIG{INT} = sub { printlog("Got SIGINT, exiting..."); killchild(); exit; };
	$SIG{QUIT} = sub { printlog("Got SIGQUIT, exiting..."); killchild(); exit; };
	$SIG{TERM} = sub { printlog("Got SIGTERM, exiting..."); killchild(); exit; };
	$SIG{HUP} = sub { printlog("Got SIGHUP, exiting..."); killchild(); exit; };

	my @dsmcopts = split(/, /, $conf{'dsmcopts'});

	my @cmd = ('dsmc','archive','-deletefiles', @dsmcopts,
		"-description=$description","-filelist=$filelist");
	my $cmdstr = "'" . join("' '", @cmd) . "' 2>&1";
	printlog "Executing: $cmdstr" if($conf{debug});
	my $execstart = time();

	my $dsmcfh;
	my @errmsgs;
	my @out;
	if($dsmcpid = open($dsmcfh, "-|", $cmdstr)) {
		while(<$dsmcfh>) {
			chomp;

			# Catch error messages, only printed on non-zero return
			# code from dsmc
			if(/^AN\w\d\d\d\d\w/) {
				push @errmsgs, $_;
			}
			# Save all output
			push @out, $_;
		}
	}

	if(!close($dsmcfh) && $!) {
		warn "closing pipe from dsmc: $!";
	}
	if($? == 0) {
		my $duration = time()-$execstart;
		$duration = 1 unless($duration);
		my $stats = sprintf("%.2f MiB/s (%.2f files/s)", ${usage_gib}*1024/$duration, $numfiles/$duration);
		printlog "Archive operation successful, duration $duration seconds, average rate $stats";
		if($conf{debug}) {
			printlog "dsmc output: " . join("\n", @out);
		}
		# files migrated to tape without issue
		exit 0;
	} else {
		# something went wrong. log and hope for better luck next time?
		my $msg = "dsmc archive failure: ";
		if ($? == -1) {
			$msg .= "failed to execute: $!";
		}
		elsif ($? & 127) {
			$msg .= sprintf "dsmc died with signal %d, %s coredump",
			       ($? & 127),  ($? & 128) ? 'with' : 'without';
		}
		else {
			$msg .= sprintf "dsmc exited with value %d", $? >> 8;
		}
		printlog "$msg";

		foreach my $errmsg (@errmsgs) {
			printlog "dsmc error message: $errmsg";
		}
		if($conf{verbose}) {
			printlog "dsmc output: " . join("\n", @out);
		}

		# Avoid spinning on persistent errors.
		sleep $conf{sleeptime};
		exit 1;
	}

	# Never reached
}

#################
# Implicit main()

# Try to send warn/die messages to log file, this is run just before the Perl
# runtime begins execution.
INIT {
        $SIG{__DIE__}=sub {
                printlog("DIE: $_[0]");
        };

        $SIG{__WARN__}=sub {
                print STDERR "$_[0]";
                printlog("WARN: $_[0]");
        };
}

# Turn off output buffering
$| = 1;

readconf();

chdir('/') || die "chdir /: $!";

$SIG{INT} = sub { warn("Got SIGINT, exiting...\n"); killchildren(); exit; };
$SIG{QUIT} = sub { warn("Got SIGQUIT, exiting...\n"); killchildren(); exit; };
$SIG{TERM} = sub { warn("Got SIGTERM, exiting...\n"); killchildren(); exit; };
$SIG{HUP} = sub { warn("Got SIGHUP, exiting...\n"); killchildren(); exit; };
$SIG{USR1} = sub { $skipdelays = 1; };

my $desclong="";
if($conf{'desc-long'}) {
	$desclong = " $conf{'desc-long'}";
}
printlog("$0: Starting$desclong...");

my $timer;
my $laststatestr = "";
my $outdir = $conf{dir} . '/out/';
my $lastcheck = 0;

while(1) {
	my $sleeptime = $conf{sleeptime};

	# Handle finished workers
	if(@workers) {
		$sleeptime = 1;
		@workers = map {
			my $w = $_;
			my $wres = waitpid($w->{pid}, WNOHANG);
			my $rc = $?;
			if ($wres == $w->{pid}) {
				# Child is done.
				# Intentionally not caring about
				# result codes.
				$w->{pid} = undef;
				# One or more workers finished, force check
				$lastcheck = 0;
			}
			$w;
		} @workers;
		@workers = grep { $_->{pid} } @workers;
	}

	if($lastcheck + $conf{sleeptime} > time()) {
		sleep($sleeptime);
		next;
	}

	$lastcheck = time();
	my $numworkers = scalar(@workers);

	my %files;
	getdir($outdir, \%files);

	# Use current total usage for trigger thresholds, easier for humans
	# to figure out what values to set.
	my $allusage = getusage(\%files);
	my $allusagestr = sprintf("%.03f GiB in %d files", $allusage, scalar keys %files);

	# Filter out files currently being worked on
	while (my $k = each %files) {
		foreach my $w (@workers) {
			delete $files{$k} if($w->{files}{$k});
		}
	}

	if(!%files) {
		$skipdelays = 0; # Ignore irrelevant request by USR1 signal
		my $str = "No pending files";
		if($numworkers) {
			$str .= ", $numworkers workers processing $allusagestr";
		}
		$str .= ". Sleeping for $conf{sleeptime} seconds" if($conf{debug});
		printlog $str if($conf{debug});
		sleep($sleeptime);
		next;
	}

	my $pending = getusage(\%files);
	my $pendingstr = sprintf("%.03f GiB in %d files", $pending, scalar keys %files);
	# Include number of workers and current hour in state string.
	my $statestr = "$pendingstr $numworkers " . (localtime(time()))[2];

	my $triggerlevel;
	# Assume threshold1_usage is smaller than threshold2_usage etc.
	for my $i (reverse(($numworkers+1) .. 9)) {
		my $at = "archiver_threshold${i}";
		next unless($conf{"${at}_usage"});

		if($allusage >= $conf{"${at}_usage"}) {
			$triggerlevel = $i;
			printlog "$at triggers" if($conf{debug});
			last;
		}
	}
	if(!$triggerlevel) {
		if(!defined($timer)) {
			$timer = time();
		}
		my $elapsed = time() - $timer;
		my $logstr = "$allusagestr total, $pendingstr pending worker assignment, $numworkers workers busy";

		if($skipdelays) {
			$skipdelays = 0; # Reset state set by USR1 signal
			printlog "$allusagestr below next threshold and only waited $elapsed seconds, but proceeding anyway as instructed by USR1 signal";
		}
		elsif($numworkers == 0 && $elapsed < $conf{archiver_timeout}) {
			if($conf{debug} || $conf{verbose} && $statestr ne $laststatestr) {
				my $timeleft = $conf{archiver_timeout} - $elapsed;
				printlog "$logstr ($timeleft seconds until archiver_timeout)";
			}
			$laststatestr = $statestr;
			sleep $sleeptime;
			next;
		}
		elsif($numworkers > 0) {
			if($conf{debug} || $conf{verbose} && $statestr ne $laststatestr) {
				printlog $logstr;
			}
			$laststatestr = $statestr;
			sleep $sleeptime;
			next;
		}

		# Trigger anyway
		$triggerlevel = $numworkers + 1;
	}

	$timer = undef;
	$laststatestr = "";

	my $tospawn = $triggerlevel - $numworkers;

	if($conf{debug}) {
		printlog "Workers running: $numworkers Trigger level: $triggerlevel Workers to spawn: $tospawn";
	}

	# Sort files oldest-first to preserve temporal affinity
	my @fsorted = sort {$files{$a}{mtime} <=> $files{$b}{mtime}} keys %files;
	# When spawning new workers, take the amount left to process by those
	# already running into account so we don't push too much work on the
	# worker we spawn now, causing uneven load balancing. The very naive
	# approach of just looking at the current total seems to work out
	# good enough...
	# FIXME: Future improvement is to have more strict adherence to the
	# temporal affinity and not split chunks with similar timestamps
	# between tapes/workers as we might do now...
	my $spawnsize = $allusage/$triggerlevel + 1;
	while($tospawn--) {
		my @myfsorted;
		my $mytot = 0;

		# Chomp off as much as we can chew...
		while($mytot/(1024*1024*1024) <= $spawnsize) {
			my $f = shift @fsorted;
			last unless($f);
			$mytot += $files{$f}{size};
			push @myfsorted, $f;
		}

		$mytot /= (1024*1024*1024); # GiB

		my $logstr = sprintf("Spawning worker #%d to archive %.03f GiB in %d files from $outdir", scalar(@workers)+1, $mytot, scalar(@myfsorted));
		if($conf{verbose}) {
			$logstr .= " (files: " . join(" ", @myfsorted) . ")";
		}

		my $dounlink = 1;
		$dounlink=0 if($conf{debug});
		my ($fh, $fn) = tempfile($filelist, DIR=>"$conf{dir}/requestlists", UNLINK=>$dounlink);
		print $fh map { "${outdir}$_\n"; } @myfsorted;
		close($fh) || die "Failed writing to $fn: $!";

		my $desc=strftime("ENDIT-%Y-%m-%dT%H:%M:%S%z",localtime());
		my $pid = spawn_worker($logstr, $fn, $desc, $mytot, scalar(@myfsorted));
		my %job;
		$job{pid} = $pid;
		$job{listfile} = $fn;
		$job{usage} = $mytot;
		my %myfiles;
		foreach my $f (@myfsorted) {
			$myfiles{$f} = $files{$f};
		}
		$job{files} = \%myfiles;
		push @workers, \%job;

		# Pace ourselves, and ensure unique description string.
		sleep 2;
	}
}
