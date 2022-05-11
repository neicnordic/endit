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

use POSIX qw(strftime WNOHANG);
use JSON;
use File::Temp qw /tempfile/;
use File::Basename;

# Add directory of script to module search path
use lib dirname (__FILE__);

use Endit qw(%conf readconf printlog);

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

	{
		local $/; # slurp whole file
		# If open failed, probably the request was finished or cancelled
		open my $rf, '<', $req_filename or return undef;
		my $json_text = <$rf>;
		$state = decode_json($json_text);
		close $rf;
	}

	if(!$state || $state->{parent_pid} && getpgrp($state->{parent_pid})<=0)
	{
		printlog "Broken request file $req_filename, removing";
		unlink $req_filename;
		return undef;
	}

	my $in_filename = $conf{dir} . '/in/' . $req;
	my $in_filesize=(stat $in_filename)[7];
	if(defined($in_filesize) && defined($state->{file_size}) && $in_filesize == $state->{file_size}) {
		printlog "Not doing $req due to file of correct size already present" if $conf{'verbose'};
		return undef;
	}

	return $state;
}

sub processing_file($$) {
	my ($worker,$file) = @_;
	if($worker) {
		return exists $worker->{files}->{$file};
	} else {
		return 0;
	}
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
			printlog "Failed to stat() file $fn: $!";
			next;
		}

		if($mtime < $maxage) {
			printlog "File $fn mtime $mtime ctime $ctime is stale, removing";
			if(!unlink($fn)) {
				printlog "unlink file $fn failed: $!";
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

my $tapelistmodtime=0;
my $tapelist = {};
my %reqset;
my %lastmount;

my $desclong="";
if($conf{'desc-long'}) {
	$desclong = " $conf{'desc-long'}";
}
printlog("$0: Starting$desclong...");

# Clean up stale remnants left by earlier crashes/restarts
cleandir("$conf{dir}/in", 30);
cleandir("$conf{dir}/requestlists", 30);

# Warning: Infinite loop. Program may not stop.
while(1) {
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

#	check if any dsmc workers are done
	if(@workers) {
		my $timer = 0;
		my $atmax = 0;
		$atmax = 1 if(scalar(@workers) >= $conf{'retriever_maxworkers'});

		while($timer < $conf{sleeptime}) {
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
					unlink $w->{listfile} unless($conf{debug});
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

			my $st = $conf{sleeptime};
			if($atmax) {
				# Check frequently if waiting for free worker
				$st = 1;
			}
			$timer += $st;
			sleep($st);
		}
	}
	else {
		# sleep to let requester remove requests and pace ourselves
		sleep $conf{sleeptime};
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

#	if any requests and free worker
	if (%reqset && scalar(@workers) < $conf{'retriever_maxworkers'}) {
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
					printlog "Skipping $msg" if($conf{verbose});
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

			my ($lf, $listfile) = tempfile("$tape.XXXXXX", DIR=>"$conf{dir}/requestlists", UNLINK=>0);
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
			close $lf or die "Closing $listfile failed: $!";

			if(-z $listfile) {
				unlink $listfile;
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

#			spawn worker
			my $pid;
			my $j;
			if ($pid = fork) {
				$j=$job->{$tape};
				$j->{pid} = $pid;
				$j->{listfile} = $listfile;
				$j->{tape} = $tape;
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
						kill("TERM", $dsmcpid);
					}
				}

				$SIG{INT} = sub { printlog("Got SIGINT, exiting..."); killchild(); exit; };
				$SIG{QUIT} = sub { printlog("Got SIGQUIT, exiting..."); killchild(); exit; };
				$SIG{TERM} = sub { printlog("Got SIGTERM, exiting..."); killchild(); exit; };
				$SIG{HUP} = sub { printlog("Got SIGHUP, exiting..."); killchild(); exit; };


				# printlog():s in child gets the child pid
				printlog "Trying to retrieve files from volume $tape using file list $listfile";

				my $indir = $conf{dir} . '/in/';

				# Check for incomplete leftovers of retrieved files
				while(my($f, $s) = each(%lfinfo)) {
					my $fn = "$indir/$f";
					my $fsize = (stat($fn))[7];
					if(defined($fsize) && $fsize != $s) {
						printlog("On-disk file $fn size $fsize doesn't match request size $s, removing.") if($conf{verbose});
						unlink($fn);
					}
				}
				my @dsmcopts = split(/, /, $conf{'dsmc_displayopts'});
				push @dsmcopts, split(/, /, $conf{'dsmcopts'});
				my @cmd = ('dsmc','retrieve','-replace=no','-followsymbolic=yes',@dsmcopts, "-filelist=$listfile",$indir);
				my $cmdstr = "'" . join("' '", @cmd) . "' 2>&1";
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
							kill("TERM", $dsmcpid);
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
