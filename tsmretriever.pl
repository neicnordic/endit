#!/usr/bin/perl

#   ENDIT - Efficient Northern Dcache Interface to TSM
#   Copyright (C) 2006-2017 Mattias Wadenstein <maswan@hpc2n.umu.se>
#   Copyright (C) 2018-2023 <Niklas.Edmundsson@hpc2n.umu.se>
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
use JSON;
use File::Temp qw /tempfile/;
use File::Basename;
use Time::HiRes qw(usleep);
use Filesys::Df;
use List::Util qw(max sum0);

# Be noisy when JSON::XS is missing, consider failing hard in the future
BEGIN {
	eval "use JSON::XS";
	if($@) {
		warn "Perl module JSON::XS missing, performance is severely reduced";
	}
};

# Add directory of script to module search path
use lib dirname (__FILE__);

use Endit qw(%conf readconf printlog readconfoverride writejson writeprom);

###########
# Variables
$Endit::logsuffix = 'tsmretriever.log';
my $skipdelays = 0; # Set by USR1 signal handler

# Turn off output buffering
$| = 1;

readconf();

chdir('/') || die "chdir /: $!";

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

my @workers;
sub killchildren() {
	foreach(@workers) {
		kill("TERM", $_->{pid});
	}
}

$SIG{INT} = sub { warn("Got SIGINT, exiting...\n"); killchildren(); exit; };
$SIG{QUIT} = sub { warn("Got SIGQUIT, exiting...\n"); killchildren(); exit; };
$SIG{TERM} = sub { warn("Got SIGTERM, exiting...\n"); killchildren(); exit; };
$SIG{HUP} = sub { warn("Got SIGHUP, exiting...\n"); killchildren(); exit; };
$SIG{USR1} = sub { $skipdelays = 1; };

sub checkrequest($) {
	my $req = shift;
	my $req_filename = $conf{dir} . '/request/' . $req;
	my $state;

	return undef unless(-f $req_filename);

	for(1..10) {
		$state = undef;
		eval {
			local $SIG{__WARN__} = sub {};
			local $SIG{__DIE__} = sub {};
			local $/; # slurp whole file
			# If open failed, probably the request was finished or
			# cancelled.
			open my $rf, '<', $req_filename or return undef;
			my $json_text = <$rf>;
			close $rf;
			die "Zero-length string" if(length($json_text) == 0);
			$state = decode_json($json_text);
		};
		last if(!$@);
		usleep(100_000); # Pace ourselves
	}

	if(!$state || $state->{parent_pid} && getpgrp($state->{parent_pid})<=0)
	{
		my $s="Broken request file $req_filename";
		if($state && $state->{parent_pid}) {
			$s .= " (PPID $state->{parent_pid} dead)";
		}
		printlog "$s, removing" if $conf{debug};
		if(!unlink($req_filename) && !$!{ENOENT}) {
			printlog "unlink '$req_filename' failed: $!";
		}
		return undef;
	}

	# Avoid processing misplaced state files from non-retrieve/recall
	# actions by the ENDIT dcache plugin.
	if($state->{action} && $state->{action} ne "recall") {
		printlog "$req_filename is $state->{action}, ignoring" if $conf{debug};
		return undef;
	}

	my $in_filename = $conf{dir} . '/in/' . $req;
	my $in_filesize=(stat $in_filename)[7];
	if(defined($in_filesize) && defined($state->{file_size}) && $in_filesize == $state->{file_size}) {
		printlog "Not doing $req due to file of correct size already present" if $conf{'debug'};
		return undef;
	}

	return $state;
}


# Clean a directory.
# Removes files older than $maxage days in directory $dir.
sub cleandir($$) {
	my ($dir, $maxagedays) = @_;

	my $maxage = time() - $maxagedays*86400;

	opendir(my $rd, $dir) || die "opendir $dir: $!";

	# Only process files matching:
	# - pNFS IDs
	# - Temporary file names created by us
	my (@files) = grep { /^([0-9A-Fa-f]+|.*?\..{6})$/ } readdir($rd);

	closedir($rd);

	return unless(@files);

	foreach my $f (@files) {
		my $fn = "$dir/$f";
		my ($mtime, $ctime) = (stat $fn)[9,10];

		if(!defined($ctime)) {
			printlog "Failed to stat() file $fn: $!" if(!$!{ENOENT});
			next;
		}

		if($ctime < $maxage) {
			printlog "File $fn mtime $mtime ctime $ctime is stale, removing" if $conf{verbose};
			if(!unlink($fn) && !$!{ENOENT}) {
				printlog "unlink '$fn' failed: $!";
			}
		}
	}
}

sub readtapelist() {

        printlog "reading tape hints from $conf{retriever_hintfile}" if $conf{verbose};

	if(open my $tf, '<', $conf{retriever_hintfile}) {
		my $out;
		eval {
			local $SIG{__WARN__} = sub {};
			local $SIG{__DIE__} = sub {};

			$out = decode_json(<$tf>);
		};
		if($@) {
			warn "Parsing $conf{retriever_hintfile} as JSON failed: $@";
			warn "Falling back to parse as old format, consider regenerating hint file in current JSON format";
			seek($tf, 0, 0) || die "Unable to seek to beginning: $!";
			while (<$tf>) {
				chomp;
				my ($id,$tape) = split /\s+/;
				next unless defined $id && defined $tape;
				$out->{$id}{volid} = $tape;
			}

		}
		close($tf);
		return $out;
	}
	else {
		warn "open $conf{retriever_hintfile}: $!";
		return undef;
	}

}

# Returns: (state, avail_gib) where state:
# 0 == OK
# 1 == Backlog, don't spawn new workers
# 2 == Full, kill all workers
sub checkfree() {

	# Work with GiB sized blocks
	my $r =  df("$conf{dir}/in", 1024*1024*1024);

	return(1) unless($r);

	if($r->{blocks} < $conf{retriever_buffersize}) {
		# FS is smaller than buffersize
		warn "$conf{dir}/in size $r->{blocks} GiB smaller than configured buffer of $conf{retriever_buffersize} GiB, trying to select a suitable size.";
		$conf{retriever_buffersize} = $r->{blocks} / 2;
		warn "Chose $conf{retriever_buffersize} GiB buffer size";
	}

	my $killsize = max(1, $conf{retriever_buffersize} * (1-($conf{retriever_killthreshold}/100)) );
	my $backlogsize = max(2, $conf{retriever_buffersize} * (1-($conf{retriever_backlogthreshold}/100)) );

	if($r->{bavail} < $killsize) {
		return (2, $r->{bavail});
	}
	elsif($r->{bavail} < $backlogsize) {
		return (1, $r->{bavail});
	}

	return (0, $r->{bavail});
}

my $tapelistmodtime=0;
my $tapelist = {};
my %reqset;
my %lastmount;

my $desclong="";
if($conf{'desc-long'}) {
	$desclong = " $conf{'desc-long'}";
}
printlog("$0: Starting$desclong...");

# Clean up stale remnants left by earlier crashes/restarts, do the request list
# directory only on startup.
my $lastclean = 0;
cleandir("$conf{dir}/requestlists", 7);

my $sleeptime = 1; # Want to start with quickly doing a full cycle.

# Warning: Infinite loop. Program may not stop.
while(1) {
	my %currstats;

	# Clean in dir periodically
	if($lastclean + 86400 < time()) {
		cleandir("$conf{dir}/in", 7);
		$lastclean = time();
	}

#	load/refresh tape list
	if (exists $conf{retriever_hintfile}) {
		my $newtapemodtime = (stat $conf{retriever_hintfile})[9];
		if(defined $newtapemodtime) {
			if($newtapemodtime > $tapelistmodtime) {
				my $newtapelist = readtapelist();
				if ($newtapelist) {
					my $loadtype = "loaded";
					if(scalar(keys(%{$tapelist}))) {
						$loadtype = "reloaded";
					}
					printlog "Tape hint file $conf{retriever_hintfile} ${loadtype}, " . scalar(keys(%{$newtapelist})) . " entries.";

					$tapelist = $newtapelist;
					$tapelistmodtime = $newtapemodtime;
				}
			}
		} else {
			printlog "Warning: retriever_hintfile set to $conf{retriever_hintfile}, but this file does not seem to exist";
		}
	}

	$currstats{'retriever_hintfile_mtime'} = $tapelistmodtime;
	$currstats{'retriever_hintfile_entries'} = scalar(keys(%{$tapelist}));

#	check if any dsmc workers are done
	if(@workers) {
		my $timer = 0;
		my $atmax = 0;
		my $backoffstate = (checkfree())[0];
		$atmax = 1 if(scalar(@workers) >= $conf{'retriever_maxworkers'});

		while($timer < $sleeptime) {
			@workers = map {
				my $w = $_;
				my $wres = waitpid($w->{pid}, WNOHANG);
				my $rc = $?;
				if ($wres == $w->{pid}) {
					# Child is done
					$w->{pid} = undef;
					# Intentionally not caring about
					# results. We'll retry and if stuff is
					# really broken, the admins will notice
					# from hanging restore requests anyway.
					if(!$conf{debug}) {
						if(!unlink($w->{listfile})) {
							printlog "unlink '$w->{listfile}' failed: $!";
						}
					}
				} 
				$w;
			} @workers;
			@workers = grep { $_->{pid} } @workers;

			# Break early if we were waiting for a worker
			# to be freed up.
			if($atmax && scalar(@workers) < $conf{'retriever_maxworkers'})
			{
				last;
			}

			# Also break early if backoff state changes
			if($backoffstate != (checkfree())[0]) {
				last;
			}

			my $st = $sleeptime;
			if($atmax || $backoffstate > 0) {
				# Check frequently if waiting for free worker
				# or if we might run out of space forcing us
				# to kill the current workers.
				$st = 1;
			}
			$timer += $st;
			sleep($st);
		}
	}
	else {
		# sleep to let requester remove requests and pace ourselves
		sleep $sleeptime;
	}
	$sleeptime = $conf{sleeptime};

	readconfoverride('retriever');

	my ($dobackoff, $in_avail_gib) = checkfree();
	my $in_fill_pct = ($conf{retriever_buffersize}-$in_avail_gib) / $conf{retriever_buffersize};
	$in_fill_pct = int(max($in_fill_pct, 0)*100);
	printlog sprintf("$conf{dir}/in avail %.1f GiB, fill $in_fill_pct %%, dobackoff: $dobackoff", $in_avail_gib) if($conf{debug});

	if($dobackoff == 2 && @workers) {
		printlog sprintf("Filesystem $conf{dir}/in space low, avail %.1f GiB, fill $in_fill_pct %% > fill killthreshold $conf{retriever_killthreshold} %%, killing workers", $in_avail_gib);
		killchildren();
		sleep(1);
		next;
	}

#	read current requests
	{
		%reqset=();
		my $reqdir = "$conf{dir}/request/";
		opendir(my $rd, $reqdir) || die "opendir $reqdir: $!";
		my (@requests) = grep { /^[0-9A-Fa-f]+$/ } readdir($rd); # omit entries with extensions
		closedir($rd);
		if (@requests) {
			foreach my $req (@requests) {
#				It'd be nice to do this here, but takes way too long with a large request list. Instead we only check it when making the requestlist per tape.
#				my $reqinfo = checkrequest($req);
				my $reqfilename=$conf{dir} . '/request/' . $req;
				my $ts =(stat $reqfilename)[9];
				my $reqinfo = {timestamp => $ts } if defined $ts;
				if ($reqinfo) {
					if (!exists $reqinfo->{tape}) {
						if (my $tape = $tapelist->{$req}{volid}) {
							# Ensure name contains
							# no fs path characters
							$tape=~tr/a-zA-Z0-9.-/_/cs;
							$reqinfo->{tape} = $tape;
						} else {
							$reqinfo->{tape} = 'default';
						}
					}
					$reqset{$req} = $reqinfo;
				}
			}
		}
	}

	# Gather working stats
	my %working;
	foreach my $w (@workers) {
		while (my $k = each %{$w->{files}}) {
			if($reqset{$k}) {
				$working{$k} = $w->{files}{$k};
			}
		}
	}
	$currstats{'retriever_working_gib'} = sum0(values %working)/(1024*1024*1024);
	$currstats{'retriever_working_files'} = scalar keys %working;


	$currstats{'retriever_requests_files'} = scalar(keys(%reqset));
	$currstats{'retriever_busyworkers'} = scalar(@workers);
	$currstats{'retriever_maxworkers'} = $conf{'retriever_maxworkers'};
	$currstats{'retriever_time'} = time();
	if(defined($in_avail_gib)) {
		$currstats{'retriever_in_avail_gib'} = $in_avail_gib;
	}
	writejson(\%currstats, "$conf{'desc-short'}-retriever-stats.json");
	writeprom(\%currstats, "$conf{'desc-short'}-retriever-stats.prom");

#	if any requests and free worker
	if (%reqset && scalar(@workers) < $conf{'retriever_maxworkers'}) {
		if($dobackoff != 0) {
			printlog sprintf("Filesystem $conf{dir}/in avail %.1f GiB, fill $in_fill_pct %% > fill backlogthreshold $conf{retriever_backlogthreshold} %%, not starting more workers", $in_avail_gib) if($conf{debug} || $conf{verbose});
			next;
		}
#		make list blacklisting pending tapes
		my %usedtapes;
		my $job = {};
		if(@workers) {
			%usedtapes = map { $_->{tape} => 1 } @workers;
		}
		foreach my $name (keys %reqset) {
			my $req = $reqset{$name};
			my $tape;
			if (exists $req->{tape}) {
				$tape = $req->{tape};
			} else {
				warn "tape should have been set for $name, but setting it again!";
				$tape = 'default';
			}
			$job->{$tape}->{$name} = $req;
			if(defined($job->{$tape}->{listsize})) {
				$job->{$tape}->{listsize} ++;
			}
			else {
				$job->{$tape}->{listsize} = 1;
			}
			if(defined $job->{$tape}->{tsoldest}) {
				if($job->{$tape}->{tsoldest} > $req->{timestamp}){
					$job->{$tape}->{tsoldest} = $req->{timestamp}
				}
			} else {
				$job->{$tape}->{tsoldest}=$req->{timestamp};
			}
			if(defined $job->{$tape}->{tsnewest}) {
				if($job->{$tape}->{tsnewest} < $req->{timestamp}){
					$job->{$tape}->{tsnewest} = $req->{timestamp}
				}
			} else {
				$job->{$tape}->{tsnewest}=$req->{timestamp};
			}
		}

#		start jobs on tapes not already taken up until retriever_maxworkers
		foreach my $tape (sort { $job->{$a}->{tsoldest} <=> $job->{$b}->{tsoldest} } keys %{$job}) {
			if(scalar(@workers) >= $conf{'retriever_maxworkers'}) {
				printlog "At $conf{'retriever_maxworkers'}, not starting more jobs" if($conf{debug});
				last;
			}

			printlog "Jobs on volume $tape: oldest " . strftime("%Y-%m-%d %H:%M:%S",localtime($job->{$tape}->{tsoldest})) . " newest " .  strftime("%Y-%m-%d %H:%M:%S",localtime($job->{$tape}->{tsnewest})) if($conf{debug});

			if(exists($usedtapes{$tape})) {
				printlog "Skipping volume $tape, job already running" if($conf{debug});
				next;
			}

			if($tape ne 'default' && defined $lastmount{$tape} && $lastmount{$tape} > time - $conf{retriever_remountdelay}) {
				my $msg = "volume $tape, last mounted at " . strftime("%Y-%m-%d %H:%M:%S",localtime($lastmount{$tape})) . " which is more recent than remountdelay $conf{retriever_remountdelay}s ago";
				if($skipdelays) {
					printlog "Proceeding due to USR1 signal despite $msg";
				}
				else {
					printlog "Skipping $msg" if($conf{debug} || ($conf{verbose} && time()-$lastmount{$tape} > 2*$conf{sleeptime}));
					next;
				}
			}

			if($tape ne 'default' && $job->{$tape}->{tsoldest} > time()-$conf{retriever_reqlistfillwaitmax} && $job->{$tape}->{tsnewest} > time()-$conf{retriever_reqlistfillwait}) {
				my $msg = "volume $tape, request list $job->{$tape}->{listsize} entries and still filling, oldest " . strftime("%Y-%m-%d %H:%M:%S",localtime($job->{$tape}->{tsoldest})) . " newest " .  strftime("%Y-%m-%d %H:%M:%S",localtime($job->{$tape}->{tsnewest}));
				if($skipdelays) {
					printlog "Proceeding due to USR1 signal despite $msg";
				}
				else {
					printlog "Skipping $msg" if($conf{verbose});
					next;
				}
			}

			my ($lf, $listfile) = eval { tempfile("$tape.XXXXXX", DIR=>"$conf{dir}/requestlists", UNLINK=>0); };
			if(!$lf) {
				warn "Unable to open file in $conf{dir}/requestlists: $@";
				sleep $conf{sleeptime};
				next;
			}
			my %lfinfo;

			my $lfsize = 0;
			foreach my $name (keys %{$job->{$tape}}) {
				my $reqinfo = checkrequest($name);
				next unless($reqinfo);

				print $lf "$conf{dir}/out/$name\n";
				if($reqinfo->{file_size}) {
					$lfinfo{$name} = $reqinfo->{file_size};
					$lfsize += $reqinfo->{file_size};
				}
				else {
					$lfinfo{$name} = -1;
				}
			}
			if(!close($lf)) {
				warn "Closing $listfile failed: $!";
				if(!unlink($listfile)) {
					printlog "unlink '$listfile' failed: $!";
				}
				sleep $conf{sleeptime};
				next;
			}

			if(-z $listfile) {
				if(!unlink($listfile)) {
					printlog "unlink '$listfile' failed: $!";
				}
				next;
			}
			$lastmount{$tape} = time;

			my $lfstats = sprintf("%.2f GiB in %d files", $lfsize/(1024*1024*1024), scalar(keys(%lfinfo)));
			$lfstats .= ", oldest " . strftime("%Y-%m-%d %H:%M:%S",localtime($job->{$tape}->{tsoldest})) . " newest " .  strftime("%Y-%m-%d %H:%M:%S",localtime($job->{$tape}->{tsnewest}));
			my $lffiles = "";
			if($conf{verbose}) {
				$lffiles .= join(" ", " files:", sort(keys(%lfinfo)));
			}
			printlog "Running worker on volume $tape ($lfstats)$lffiles";

			$sleeptime = 1;
#			spawn worker
			my $pid;
			my $j;
			if ($pid = fork) {
				$j=$job->{$tape};
				$j->{pid} = $pid;
				$j->{listfile} = $listfile;
				$j->{tape} = $tape;
				$j->{files} = \%lfinfo;
				push @workers, $j;
			}
			else {
				undef %usedtapes;
				undef %reqset;
				undef $tapelist;
				undef $job;
				@workers=();
				my $dsmcpid;
				sub killchild() {
					if(defined($dsmcpid)) {
						# IBM actually recommends using
						# KILL to avoid core dumps due
						# to signal handling issues wrt
						# multi-threading.
						# See https://www.ibm.com/docs/en/storage-protect/8.1.20?topic=started-ending-session
						kill("KILL", $dsmcpid);
					}
				}

				$SIG{INT} = sub { printlog("Child got SIGINT, exiting..."); killchild(); exit; };
				$SIG{QUIT} = sub { printlog("Child got SIGQUIT, exiting..."); killchild(); exit; };
				$SIG{TERM} = sub { printlog("Child got SIGTERM, exiting..."); killchild(); exit; };
				$SIG{HUP} = sub { printlog("Child got SIGHUP, exiting..."); killchild(); exit; };


				# printlog():s in child gets the child pid
				printlog "Trying to retrieve files from volume $tape using file list $listfile";

				my $indir = $conf{dir} . '/in/';

				# Check for incomplete leftovers of retrieved files
				while(my($f, $s) = each(%lfinfo)) {
					next if($s < 0);
					my $fn = "$indir/$f";
					my $fsize = (stat($fn))[7];
					if(defined($fsize) && $fsize != $s) {
						printlog("On-disk file $fn size $fsize doesn't match request size $s, removing.") if($conf{verbose});
						if(!unlink($fn) && !$!{ENOENT}) {
							printlog "unlink '$fn' failed: $!";
						}
					}
				}
				my @dsmcopts = split(/, /, $conf{'dsmc_displayopts'});
				push @dsmcopts, split(/, /, $conf{'dsmcopts'});
				my @cmd = ('dsmc','retrieve','-replace=no','-followsymbolic=yes',@dsmcopts, "-filelist=$listfile",$indir);
				my $cmdstr = "ulimit -t $conf{dsmc_cpulimit} ; ";
				$cmdstr .= "exec '" . join("' '", @cmd) . "' 2>&1";
				printlog "Executing: $cmdstr" if($conf{debug});
				my $execstart = time();
				my @out;
				my @errmsgs;
				my $usractionreq = 0;
				my $dsmcfh;
				if($dsmcpid = open($dsmcfh, "-|", $cmdstr)) {
					while(<$dsmcfh>) {
						chomp;

						# Catch error messages, only
						# printed on non-zero return
						# code from dsmc
						if(/^AN\w\d\d\d\d\w/) {
							push @errmsgs, $_;
						}

						# Detect and save interactive
						# messages as error messages
						if(/^--- User Action is Required ---$/) {
							$usractionreq = 1;
						}
						if($usractionreq) {
							push @errmsgs, $_;
						}

						# Save all output for verbose
						# output
						push @out, $_;

						if(/^Action\s+\[.*\]\s+:/) {
							printlog "dsmc prompt detected, aborting";
							kill("KILL", $dsmcpid);
							last;
						}

					}

				}
				if(!close($dsmcfh) && $!) {
					warn "closing pipe from dsmc: $!";
				}
				if($? == 0) {
					my $duration = time()-$execstart;
					$duration = 1 unless($duration);
					my $sizestats = sprintf("%.2f GiB in %d files", $lfsize/(1024*1024*1024), scalar(keys(%lfinfo)));
					my $speedstats = sprintf("%.2f MiB/s (%.2f files/s)", $lfsize/(1024*1024*$duration), scalar(keys(%lfinfo))/$duration);
					printlog "Retrieve operation from volume $tape successful, $sizestats took $duration seconds, average rate $speedstats";

					# sleep to let requester remove requests
					sleep 3;
					exit 0;
				} else {
					my $msg = "dsmc retrieve failure volume $tape file list $listfile: ";
					if ($? == -1) {
						$msg .= "failed to execute: $!";
					}
					elsif ($? & 127) {
						$msg .= sprintf "child died with signal %d, %s coredump", ($? & 127),  ($? & 128) ? 'with' : 'without';
					}
					else {
						$msg .= sprintf "child exited with value %d", $? >> 8;
					}
					printlog "$msg";

					foreach my $errmsg (@errmsgs) {
						printlog "dsmc error message: $errmsg";
					}
					if($conf{verbose}) {
						printlog "dsmc output: " . join("\n", @out);
					}

					# Check if we got any files of
					# unexpected size. This can happen if
					# we run out of disk space, or if there
					# are duplicate archived files with
					# different file size.
					while(my($f, $s) = each(%lfinfo)) {
						my $fn = "$indir/$f";
						my $fsize = (stat($fn))[7];
						if(defined($fsize) && $fsize != $s) {
							printlog("Warning: Retrieved file $fn size $fsize but it doesn't match request size $s. If the problem persists, manual investigation and intervention is needed.");
						}
					}

					# sleep to pace ourselves if these are
					# persistent reoccurring failures
					sleep $conf{sleeptime};

					# Any number of requests broke, try
					# again later
                			exit 1;
				}
			}
		}
	}
	$skipdelays = 0; # Reset state set by USR1 signal
}
