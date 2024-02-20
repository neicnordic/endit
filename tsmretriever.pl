#!/usr/bin/perl

#   ENDIT - Efficient Northern Dcache Interface to TSM
#   Copyright (C) 2006-2017 Mattias Wadenstein <maswan@hpc2n.umu.se>
#   Copyright (C) 2018-2024 <Niklas.Edmundsson@hpc2n.umu.se>
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
use JSON::XS;
use File::Temp qw /tempfile/;
use File::Basename;
use Time::HiRes qw(usleep);
use Filesys::Df;
use List::Util qw(min max sum0);

# Add directory of script to module search path
use lib dirname (__FILE__);

use Endit qw(%conf readconf printlog readconfoverride writejson writeprom);

###########
# Variables
$Endit::logname = 'tsmretriever';
my $skipdelays = 0; # Set by USR1 signal handler
# Count successfully processed files.
my $staged_bytes = 0;
my $staged_files = 0;
my $stage_retries = 0;

my %promtypehelp = (
	retriever_hintfile_mtime => {
		type => 'gauge',
		help => 'Last modification timestamp of hint file',
	},
	retriever_hintfile_entries => {
		type => 'gauge',
		help => 'Number of hintfile entries',
	},
	retriever_staged_bytes => {
		type => 'counter',
		help => 'Bytes successfully staged',
	},
	retriever_staged_files => {
		type => 'counter',
		help => 'Files successfully staged',
	},
	retriever_stage_retries => {
		type => 'counter',
		help => 'How many times a stage operation has been retried',
	},
	retriever_working_bytes => {
		type => 'gauge',
		help => 'Total size of files currently being staged by workers',
	},
	retriever_working_files => {
		type => 'gauge',
		help => 'Number of files currently being staged by workers',
	},
	retriever_requests_bytes => {
		type => 'gauge',
		help => 'Total size of files in stage request queue',
	},
	retriever_requests_files => {
		type => 'gauge',
		help => 'Number of files in stage request queue',
	},
	retriever_in_avail_bytes => {
		type => 'gauge',
		help => 'Staging in/ directory free space',
	},
	retriever_busyworkers => {
		type => 'gauge',
		help => 'Number of busy workers',
	},
	retriever_maxworkers => {
		type => 'gauge',
		help => 'Maximum number of workers',
	},
	retriever_time => {
		type => 'gauge',
		help => 'Unix time when these metrics were last updated',
	},

);

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


sub checkrequest(@) {
	my ($req,$state) = @_;
	my $req_filename = "$conf{dir_request}/$req";

	return undef unless(-f $req_filename);

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

	my $in_filename = "$conf{dir_in}/$req";
	my $in_filesize=(stat $in_filename)[7];
	if(defined($in_filesize) && defined($state->{file_size}) && $in_filesize == $state->{file_size}) {
		printlog "Not doing $req due to file of correct size $in_filesize already present, removing request" if $conf{'debug'};
		if(!unlink($req_filename) && !$!{ENOENT}) {
			printlog "unlink '$req_filename' failed: $!";
		}
		return undef;
	}

	return $state;
}


sub loadrequest($) {
	my $req = shift;
	my $req_filename = "$conf{dir_request}/$req";
	my $state;

	# Retry if file is being written as we try to read it
	for(1..25) {
		$state = undef;
		eval {
			local $SIG{__WARN__} = sub {};
			local $SIG{__DIE__} = sub {};
			local $/; # slurp whole file
			# If open failed, probably the request was finished or
			# cancelled.
			open my $rf, '<', $req_filename or return undef;
			my $json_text = <$rf>;
			my $ts = (stat $rf)[9] or die "stat failed: $!";
			close $rf or die "close failed: $!";
			die "Zero-length string" if(length($json_text) == 0);
			$state = decode_json($json_text);
			$state->{endit_req_ts} = $ts;
		};
		last if(!$@);
		usleep(20_000); # Pace ourselves for 20 ms
	}

	# Avoid processing state files from non-retrieve/recall actions by the
	# ENDIT dcache plugin.
	if($state && $state->{action} && $state->{action} ne "recall") {
		printlog "$req_filename is $state->{action}, ignoring" if $conf{debug};
		return undef;
	}

	return undef unless(checkrequest($req, $state));

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
		}
		close($tf);
		return $out;
	}
	else {
		warn "open $conf{retriever_hintfile}: $!";
		return undef;
	}
}

# Convert bytes to GiB
sub to_gib($) {
	my($bytes) = @_;

	return($bytes/(1024*1024*1024));
}

use constant {
	CF_OK	=> 0,
	CF_BACKLOG => 1,
	CF_FULL	=> 2,
};
# Returns: (state, avail_bytes) where state:
#  CF_OK      - Usage lower than any thresholds
#  CF_FULL    - Usage larger than retriever_killthreshold.
#  CF_BACKLOG - Usage larger than retriever_backlogthreshold.
sub checkfree() {

	# Work with byte sized blocks
	my $r =  df($conf{dir_in}, 1);

	return(CF_BACKLOG) unless($r);

	my $blocks_gib = to_gib($r->{blocks});
	if($blocks_gib < $conf{retriever_buffersize}) {
		# FS is smaller than buffersize
		warn "$conf{dir_in} size $blocks_gib GiB smaller than configured buffer of $conf{retriever_buffersize} GiB, trying to select a suitable size.";
		$conf{retriever_buffersize} = $blocks_gib / 2;
		warn "Chose $conf{retriever_buffersize} GiB buffer size";
	}

	my $killsize = max(1, $conf{retriever_buffersize} * (1-($conf{retriever_killthreshold}/100)) );
	my $backlogsize = max(2, $conf{retriever_buffersize} * (1-($conf{retriever_backlogthreshold}/100)) );

	if(to_gib($r->{bavail}) < $killsize) {
		return (CF_FULL, $r->{bavail});
	}
	elsif(to_gib($r->{bavail}) < $backlogsize) {
		return (CF_BACKLOG, $r->{bavail});
	}

	return (CF_OK, $r->{bavail});
}

# Handle incrementing the stage stats counters.
sub handle_stage_stats (@) {
	my ($w, $rc) = @_;

	while(my ($k, $v) = each %{$w->{files}}) {
		next if ($w->{counted}{$k});

		if(defined($rc) && $rc == 0) {
			# The easy case, all went well
			$staged_bytes += $v;
			$staged_files++;
			# Shouldn't really be needed as we should only be
			# called once with $rc defined.
			$w->{counted}{$k} = 1;
			next;
		}

		my $if = "$conf{dir_in}/$k";
		my $ifsize = (stat($if))[7];
		if(defined($ifsize) && $ifsize == $v) {
			# Staged successfully.
			$staged_bytes += $v;
			$staged_files++;
			$w->{counted}{$k} = 1;
			next;
		}

		if($rc) {
			my $rf = "$conf{dir_request}/$k";
			if(-f $rf) {
				# We know this file didn't get staged and needs
				# to be retried.
				$stage_retries++;
				# Shouldn't really be needed as we should only
				# be called once with $rc defined.
				$w->{counted}{$k} = 1;
				next;
			}
		}
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

# Clean up stale remnants left by earlier crashes/restarts, do the request list
# directory only on startup.
my $lastclean = 0;
cleandir($conf{dir_requestlists}, 7);

my $sleeptime = 1; # Want to start with quickly doing a full cycle.

# Warning: Infinite loop. Program may not stop.
while(1) {
	my %currstats;

	# Clean in dir periodically
	if($lastclean + 86400 < time()) {
		cleandir($conf{dir_in}, 7);
		$lastclean = time();
	}

	# Load/refresh tape list
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

					# Also revalidate cached requests.
					%reqset=();
				}
			}
		} else {
			printlog "Warning: retriever_hintfile set to $conf{retriever_hintfile}, but this file does not seem to exist";
		}
	}

	$currstats{'retriever_hintfile_mtime'} = $tapelistmodtime;
	$currstats{'retriever_hintfile_entries'} = scalar(keys(%{$tapelist}));

	# Check if any dsmc workers are done
	if(@workers) {
		my $timer = 0;

		while($timer < $sleeptime) {
			my $oldwcount = scalar(@workers);
			@workers = map {
				my $w = $_;
				my $wres = waitpid($w->{pid}, WNOHANG);
				my $rc = $?;
				if ($wres == $w->{pid}) {
					# Child is done
					$w->{pid} = undef;
					if(!$conf{debug}) {
						if(!unlink($w->{listfile})) {
							printlog "unlink '$w->{listfile}' failed: $!";
						}
					}
					# Also revalidate cached requests.
					%reqset=();
					# Child status is only used for
					# metrics, unprocessed files are
					# retried and if stuff is really
					# broken, the admins will notice from
					# hanging restore requests anyway.
					handle_stage_stats($w, $rc);
				}
				else {
					handle_stage_stats($w, undef);
				}
				$w;
			} @workers;
			@workers = grep { $_->{pid} } @workers;

			# Break early as soon as the number of workers changed
			# in order to update statistics.
			if(scalar(@workers) != $oldwcount) {
				last;
			}

			# Check often for changed worker state. The dcache
			# endit provider GRACE_PERIOD is 1000 ms (1s), so if we
			# check more often than that we'll get a somewhat
			# accurate view of staging progress metrics.
			$timer += 0.5;
			usleep(500_000); # 0.5s
		}
	}
	else {
		# No workers, wait for requests.
		sleep $sleeptime;
	}
	$sleeptime = $conf{sleeptime};

	$currstats{'retriever_staged_bytes'} = $staged_bytes;
	$currstats{'retriever_staged_files'} = $staged_files;
	$currstats{'retriever_stage_retries'} = $stage_retries;

	readconfoverride('retriever');

	my ($dobackoff, $in_avail_bytes) = checkfree();
	my $in_fill_pct = ($conf{retriever_buffersize}-to_gib($in_avail_bytes)) / $conf{retriever_buffersize};
	$in_fill_pct = int(max($in_fill_pct, 0)*100);
	printlog sprintf("$conf{dir_in} avail %d bytes, fill $in_fill_pct %%, dobackoff: $dobackoff", $in_avail_bytes) if($conf{debug});

	if($dobackoff == CF_FULL && @workers) {
		printlog sprintf("Filesystem $conf{dir_in} space low, avail %.1f GiB, fill $in_fill_pct %% > fill killthreshold $conf{retriever_killthreshold} %%, killing workers", to_gib($in_avail_bytes));
		killchildren();
		sleep(1);
		next;
	}

	# Read current requests
	opendir(my $rd, $conf{dir_request}) || die "opendir $conf{dir_request}: $!";
	my (@requests) = grep { /^[0-9A-Fa-f]+$/ } readdir($rd); # omit entries with extensions
	closedir($rd);
	my $requests_bytes = 0;
	if (@requests) {
		my $cachetime = time();
		foreach my $req (@requests) {
			if($reqset{$req} && $reqset{$req}->{endit_req_ct} + $conf{sleeptime} < $cachetime) {
				# Cached, check if we need to revalidate.
				my $req_filename="$conf{dir_request}/$req";
				my $ts =(stat $req_filename)[9];
				if(!$ts) {
					printlog "Invalidating $req: $!" if($conf{debug});
					delete $reqset{$req};
				}
				elsif($ts == $reqset{$req}->{endit_req_ts}) {
					$reqset{$req}->{endit_req_ct} = $cachetime;
				}
				else {
					printlog "Revalidating $req: file timestamp $ts != cached timestamp $reqset{$req}->{endit_req_ts}" if($conf{debug});
					delete $reqset{$req};
				}
			}
			# Even if it existed above, might be invalidated there
			if(!$reqset{$req}) {
				my $reqinfo = loadrequest($req);
				if ($reqinfo) {
					if (my $tape = $tapelist->{$req}{volid})
					{
						# Ensure name contains
						# no fs path characters
						$tape=~tr/a-zA-Z0-9.-/_/cs;
						$reqinfo->{tape} = $tape;
					} else {
						$reqinfo->{tape} = 'default';
					}
					$reqinfo->{endit_req_ct} = $cachetime;
					$reqset{$req} = $reqinfo;
				}
			}
		}
		while(my $req = each %reqset) {
			if($reqset{$req}->{endit_req_ct} + $conf{sleeptime} < $cachetime) {
				printlog "Invalidating $req: Not present" if($conf{debug});
				delete $reqset{$req};
			}
			elsif($reqset{$req}->{file_size}) {
				$requests_bytes += $reqset{$req}->{file_size};
			}
		}
	}
	else {
		%reqset=();
	}

	printlog scalar(%reqset) . " entries from $conf{dir_request} cached" if($conf{debug});

	# Gather working stats
	my %working;
	foreach my $w (@workers) {
		printlog "Worker $w->{pid} tape $w->{tape} listfile $w->{listfile} with " . scalar(%{$w->{files}}) . " files" if($conf{debug});
		while (my($k,$v) = each %{$w->{files}}) {
			if($reqset{$k}) {
				$working{$k} = $v;
			}
		}
		# Update tape lastmount timestamp if we're still working on it
		if($w->{tape} ne 'default') {
			$lastmount{$w->{tape}} = time();
		}
	}
	$currstats{'retriever_working_bytes'} = sum0(grep {$_>0} values %working);
	$currstats{'retriever_working_files'} = scalar keys %working;
	$currstats{'retriever_requests_files'} = scalar(keys(%reqset));
	$currstats{'retriever_requests_bytes'} = $requests_bytes;
	$currstats{'retriever_busyworkers'} = scalar(@workers);
	$currstats{'retriever_maxworkers'} = $conf{'retriever_maxworkers'};
	$currstats{'retriever_time'} = time();
	if(defined($in_avail_bytes)) {
		$currstats{'retriever_in_avail_bytes'} = $in_avail_bytes;
	}
	writejson(\%currstats, "$conf{'desc-short'}-retriever-stats.json");
	writeprom(\%currstats, "$conf{'desc-short'}-retriever-stats.prom", \%promtypehelp);

	# If any requests and free worker
	if (%reqset && scalar(@workers) < $conf{'retriever_maxworkers'}) {
		if($dobackoff != CF_OK) {
			printlog sprintf("Filesystem $conf{dir_in} avail %.1f GiB, fill $in_fill_pct %% > fill backlogthreshold $conf{retriever_backlogthreshold} %%, not starting more workers", to_gib($in_avail_bytes)) if($conf{debug} || $conf{verbose});
			next;
		}
		# Make list blacklisting pending tapes
		my %usedtapes;
		my $job = {};
		if(@workers) {
			%usedtapes = map { $_->{tape} => 1 } @workers;
		}
		while(my($name, $req) = each %reqset) {
			my $tape = $req->{tape};
			if($usedtapes{$tape}) {
				printlog "Skipping $name volume $tape, job already running" if($conf{debug});
				next;
			}
			$job->{$tape}->{$name} = $req;
			$job->{$tape}->{tsoldest} = min($job->{$tape}->{tsoldest} // $req->{endit_req_ts}, $req->{endit_req_ts});
			$job->{$tape}->{tsnewest} = max($job->{$tape}->{tsnewest} // $req->{endit_req_ts}, $req->{endit_req_ts});
		}

		# Start jobs on tapes not already taken up until retriever_maxworkers
		foreach my $tape (sort { $job->{$a}->{tsoldest} <=> $job->{$b}->{tsoldest} } keys %{$job}) {
			if(scalar(@workers) >= $conf{'retriever_maxworkers'}) {
				printlog "At $conf{'retriever_maxworkers'}, not starting more jobs" if($conf{debug});
				last;
			}

			printlog "Jobs on volume $tape: oldest " . strftime("%Y-%m-%d %H:%M:%S",localtime($job->{$tape}->{tsoldest})) . " newest " .  strftime("%Y-%m-%d %H:%M:%S",localtime($job->{$tape}->{tsnewest})) if($conf{debug});

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
				my $msg = "volume $tape, request list " . (scalar(%{$job->{$tape}})-2) . " entries and still filling, oldest " . strftime("%Y-%m-%d %H:%M:%S",localtime($job->{$tape}->{tsoldest})) . " newest " .  strftime("%Y-%m-%d %H:%M:%S",localtime($job->{$tape}->{tsnewest}));
				if($skipdelays) {
					printlog "Proceeding due to USR1 signal despite $msg";
				}
				else {
					printlog "Skipping $msg" if($conf{verbose});
					next;
				}
			}

			my ($lf, $listfile) = eval { tempfile("$tape.XXXXXX", DIR=>"$conf{dir_requestlists}", UNLINK=>0); };
			if(!$lf) {
				warn "Unable to open file in $conf{dir_requestlists}: $@";
				sleep $conf{sleeptime};
				next;
			}
			my %lfinfo;

			my $lfsize = 0;
			while(my $name = each %{$job->{$tape}}) {
				# Filter out endit-internal items
				next unless($reqset{$name});

				my $reqinfo = checkrequest($name, $reqset{$name});
				next unless($reqinfo);

				print $lf "$conf{dir_out}/$name\n";
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

			my $lfstats = sprintf("%.2f GiB in %d files", to_gib($lfsize), scalar(keys(%lfinfo)));
			$lfstats .= ", oldest " . strftime("%Y-%m-%d %H:%M:%S",localtime($job->{$tape}->{tsoldest})) . " newest " .  strftime("%Y-%m-%d %H:%M:%S",localtime($job->{$tape}->{tsnewest}));
			my $lffiles = "";
			if($conf{verbose}) {
				$lffiles .= join(" ", " files:", sort(keys(%lfinfo)));
			}

			$sleeptime = 1;
			# Spawn worker
			my $pid;
			my $j;
			if ($pid = fork) {
				printlog "Running worker PID $pid on volume $tape ($lfstats)$lffiles";
				$j=$job->{$tape};
				$j->{pid} = $pid;
				$j->{listfile} = $listfile;
				$j->{tape} = $tape;
				$j->{files} = \%lfinfo;
				push @workers, $j;
			}
			else {
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

				# Check for incomplete leftovers of retrieved files
				while(my($f, $s) = each(%lfinfo)) {
					next if($s < 0);
					my $fn = "$conf{dir_in}/$f";
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
				my @cmd = ('dsmc','retrieve','-replace=no','-followsymbolic=yes',@dsmcopts, "-filelist=$listfile","$conf{dir_in}/");
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
					my $sizestats = sprintf("%.2f GiB in %d files", to_gib($lfsize), scalar(keys(%lfinfo)));
					my $speedstats = sprintf("%.2f MiB/s (%.2f files/s)", $lfsize/(1024*1024*$duration), scalar(keys(%lfinfo))/$duration);
					printlog "Retrieve operation from volume $tape successful, $sizestats took $duration seconds, average rate $speedstats";
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
						my $fn = "$conf{dir_in}/$f";
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
