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

use POSIX qw(strftime);
use File::Temp qw /tempfile/;
use File::Basename;
use JSON;

# Be flexible in handling Schedule::Cron presence.
my $have_schedule_cron = eval
{
	# Silence errors
	local $SIG{__WARN__} = sub {};
	local $SIG{__DIE__} = sub {};

	# use is really the same as require+import
	require Schedule::Cron;
	Schedule::Cron->import();
	1;
};

# Add directory of script to module search path
use lib dirname (__FILE__);
use Endit qw(%conf readconf printlog readconfoverride);


###########
# Variables
$Endit::logsuffix = 'tsmdeleter.log';
my $filelist = "tsm-delete-files.XXXXXX";
my ($trashdir, $queuedir);
my $dounlink = 1;
my $dsmcpid;
my $needretry = 0;
my $flushqueue = 0;

# deleter_queueprocinterval shortcut mappings
# crontab-style requires Schedule::Cron
my %text2cron = (
	minutely =>'* * * * *',
	hourly =>  '0 * * * *',
	daily =>   '0 0 * * *',
	weekly =>   '0 0 * * 1',
	monthly => '0 0 1 * *',
);
# fallback triggers when strftime() output changes
my %text2fmt = (
	minutely =>'%M',
	hourly =>  '%H',
	daily =>   '%d',
	weekly =>   '%V',
	monthly => '%m',
);


##################
# Helper functions

sub killchild() {

	if(defined($dsmcpid)) {
		kill("TERM", $dsmcpid);
	}
}

# Performs dsmc delete of files in the specifiled filelist.
# Returns: undef on success
#          Listref of 0 or more successful deletions on (partial) failure
sub rundelete {
	my ($filelist) = @_;

	my($out, $err);
	my @dsmcopts = split(/, /, $conf{'dsmc_displayopts'});
	push @dsmcopts, split(/, /, $conf{'dsmcopts'});
	my @cmd = ('dsmc','delete','archive','-noprompt',
		@dsmcopts,"-filelist=$filelist");
	my $cmdstr = "'" . join("' '", @cmd) . "' 2>&1";
	printlog "Executing: $cmdstr" if($conf{debug});

	my $dsmcfh;
	my @errmsgs;
	my @out;
	if($dsmcpid = open($dsmcfh, "-|", $cmdstr)) {
		while(<$dsmcfh>) {
			chomp;

			# Catch error messages.
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

	$dsmcpid = undef;

	if($? != 0) { 
		my $reallybroken=0;
		my @deleted = ();

		# Some kind of problem occurred. The dsmc return code
		# is just mapped to the class of the error message, so
		# we need to investigate the actual messages.

		# Ignore known benign messages:
		# - ANS1278W Virtual mount point 'filespace-name' is a file
		#   system. It will be backed up as a file system.
		# => benign config warning, you have a redundant
		#    VIRTUALMOUNTPOINT entry in your dsm.sys.
		# - ANS1898I ***** Processed count files *****
		# => progress information

		# These errors gives information when some/all files
		# have already been deleted:
		# - ANS1345E No objects on the server match object-name
		# => file already deleted
		# - ANS1302E No objects on server match query
		# => all files already deleted

		# FIXME: We only have positive feedback on files that are
		# already deleted (ie nonexistant on server). We could
		# check if the summary reported by dsmc adds up to our
		# deletion file counts, ie dsmc output:
		# Total number of objects deleted:              2
		# Total number of objects failed:               4

		foreach (@errmsgs) {
			if(/^ANS1278W/ or /^ANS1898I/) {
				next;
			}
			elsif(/^ANS1302E/) {
				printlog "All files already deleted: $_" if $conf{'verbose'};
			}
			elsif(/^ANS1345E.*'(.*)'$/) {
				my $s = $1;
				$s =~ s _^.*/__;
				push @deleted, $s;
				printlog "File already deleted: $s" if $conf{'verbose'};
			}
			elsif(/^ANS1345E/) {
				# Catch if we fail to parse partial deletion
				warn "Failed to parse: $_";
			}
			else {
				$reallybroken=1;
			}
		}
		if($reallybroken) {
			# something went wrong. log and hope for better luck next time?
			my $msg = "dsmc delete failure: ";
			if ($? == -1) {
				$msg .= "failed to execute: $!";
			}
			elsif ($? & 127) {
				$msg .= sprintf "died with signal %d, %s coredump", ($? & 127),  ($? & 128) ? 'with' : 'without';
			}
			else {
				$msg .= sprintf "exited with value %d", $? >> 8;
			}
			printlog "$msg";
			if($conf{verbose}) {
				printlog "dsmc output: " . join("\n", @out);
			}
			else {
				# At a minimum, log the errors!
				printlog "dsmc errors: " . join("\n", @errmsgs);
			}

			# Return successful deletions, if any.
			return \@deleted;
		}
	}

	return undef; # Success!
}


# Add deletion requests to our queue, removing the request files which
# signals the deletion as handled to the dCache ENDIT plugin.
sub addtoqueue
{
	my @files = @_;

	if(! -d $queuedir && !mkdir($queuedir)) {
		die "mkdir $queuedir failed: $!";
	}

	my $fn;
	do {
		# Might have a corner case with a tight loop causing a file
		# collision, so just handle that.
		my $now = time();
		$fn = "$queuedir/$now";
		sleep(1) if(-f $fn);
	} while(-f $fn);

	open(my $fh, ">", $fn) or die "Failed to open $fn for writing: $!";

	print $fh encode_json(\@files),"\n";

	close($fh) or die "Failed closing $fn: $!";

	my $debugdir = "$trashdir/debug";
	if($conf{debug}) {
		if(! -d $debugdir && !mkdir($debugdir)) {
			die "mkdir $debugdir: $!";
		}
	}
	foreach my $f (@files) {
		next unless(-f "$trashdir/$f"); # Skip already deleted files

		if($conf{debug}) {
			rename("$trashdir/$f", "$debugdir/$f") or warn "Failed to move $f to $debugdir/ : $!";
		}
		else {
			if(!unlink("$trashdir/$f")) {
				printlog "unlink '$trashdir/$f' failed: $!";
			}
		}
	}

	my $logstr = "Queued " . scalar(@files) . " files for deletion";
	if($conf{verbose}) {
		$logstr .= " (files: " . join(" ", @files) . ")";
	}
	printlog $logstr;
}

# Check trash directory and add deletion requests to queue.
sub checktrashdir
{
	opendir(my $td, $trashdir) || die "opendir $trashdir: $!";
	my @files = grep { /^[0-9A-Fa-f]+$/ } readdir($td);
	closedir($td);

	if (@files > 0) {
		addtoqueue(@files);
	}
}

# Process the queue of pending deletions.
sub processqueue
{
	printlog "Processing deletion queue start" if($conf{debug});

	if(! -d $queuedir) {
		printlog "No $queuedir directory, skipping" if($conf{debug});
		return 0;
	}

	opendir(my $td, $queuedir) || die "opendir $queuedir: $!";
	my @qfiles = grep { /^[0-9]+$/ } readdir($td);
	closedir($td);

	my @files;

	foreach my $qf (@qfiles) {
                local $/; # slurp whole file
		my $qfd;
                if(!open $qfd, '<', "$queuedir/$qf") {
			warn "Opening $queuedir/$qf: $!";
			next;
		}
                my $json_text = <$qfd>;
                my $qentries = decode_json($json_text);
                close $qfd;
		push @files, @{$qentries};
		if($conf{debug}) {
			printlog "Read " . scalar(@{$qentries}) . " entries from $queuedir/$qf";
		}
        }

	printlog scalar(@files) . " files in deletion queue" if($conf{verbose});

	# Do deletions and update @files to reflect files left to delete
	if(@files) {
		my ($fh, $filename) = eval { tempfile($filelist, DIR=>"$conf{dir}/requestlists", UNLINK=>$dounlink); };
		if(!$fh) {
			warn "Failed opening filelist: $@";
			return 0;
		}
		print $fh map { "$conf{dir}/out/$_\n"; } @files;
		if(!close($fh)) {
			warn "Failed writing to $filename: $!";
			if(!unlink($filename)) {
				printlog "unlink '$filename' failed: $!";
			}
			return 0;
		}

		my $logstr = "Trying to delete " . scalar(@files) . " files";
		if($conf{debug}) {
			$logstr .= " using file list $filename";
		}
		if($conf{verbose}) {
			$logstr .= " (files: " . join(" ", @files) . ")";
		}
		printlog $logstr;

		my $partial = rundelete($filename);

		if(!defined($partial)) {
			# Success!
			printlog "Successfully deleted " . scalar(@files) . " files";
			@files = ();
		}
		elsif(@{$partial}) {
			printlog "Partial success, deleted " . scalar(@{$partial}) . " of " . scalar(@files) . " files";

			# Filter out the partial successes from @files
			my %f;
			@f{ @files } = ();
			delete @f{ @{$partial} };
			@files = keys %f;
		}

		if(!$conf{debug}) {
			if(!unlink($filename)) {
				printlog "unlink '$filename' failed: $!";
			}
		}
	}

	$needretry = scalar(@files);
	# Add files that failed to delete back into queue
	if(@files) {
		addtoqueue(@files); # Will die() on error
	}

	# Remove old queue files
	foreach my $qf (@qfiles) {
                if(!unlink("$queuedir/$qf")) {
			printlog "unlink '$queuedir/$qf' failed: $!";
		}
	}

	printlog "Processing deletion queue done" if($conf{debug});

	return 0;
}

# sleep-hook for Schedule::Cron, sleeps at most $conf{sleeptime} seconds
# at a time.
# We use this to drive our main loop iteration, both when using
# Schedule::Cron and the while-loop fallback.
sub cronsleep
{
	my ($time, $cron) = @_;

	# Perform these actions on each iteration.
	readconfoverride('deleter');
	checktrashdir();

	if($flushqueue) {
		printlog "Flushing deletion queue as instructed by USR1 signal";
		$flushqueue = 0;
		processqueue();
	}
	elsif($needretry) {
		processqueue();
	}

	# Don't sleep longer than our configured sleeptime.
	if($time > $conf{sleeptime}) {
		$time = $conf{sleeptime};
	}

	printlog "cronsleep() for $time seconds" if($conf{debug});

	sleep($time);

	return;
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
$dounlink=0 if($conf{debug});
$trashdir = "$conf{dir}/trash";
$queuedir = "$trashdir/queue";

chdir('/') || die "chdir /: $!";

$SIG{INT} = sub { warn("Got SIGINT, exiting...\n"); killchild(); exit; };
$SIG{QUIT} = sub { warn("Got SIGQUIT, exiting...\n"); killchild(); exit; };
$SIG{TERM} = sub { warn("Got SIGTERM, exiting...\n"); killchild(); exit; };
$SIG{HUP} = sub { warn("Got SIGHUP, exiting...\n"); killchild(); exit; };
$SIG{USR1} = sub { $flushqueue = 1; };


my $desclong="";
if($conf{'desc-long'}) {
	$desclong = " $conf{'desc-long'}";
}
printlog("$0: Starting$desclong...");


# Basic sanity-checking of deleter_queueprocinterval argument
my $crontime;
if($conf{deleter_queueprocinterval} =~ /\s/) {
	if(scalar(split/\s+/, $conf{deleter_queueprocinterval}) eq 5) {
		$crontime = $conf{deleter_queueprocinterval};
	}
}
elsif($text2cron{$conf{deleter_queueprocinterval}}) {
	$crontime = $text2cron{$conf{deleter_queueprocinterval}};
}
if(!$crontime) {
	die "Bad config: deleter_queueprocinterval: $conf{deleter_queueprocinterval}";
}

my $skew = int(rand(60)); # Avoid executing exactly at second 0
if($have_schedule_cron) {
	my $cron = new Schedule::Cron( \&processqueue, 
			{ sleep => \&cronsleep, nofork => 1, nostatus => 1 } );

	# Strip quotes in case someone has been ambitious in the config
	$crontime =~ s/^"//;
	$crontime =~ s/"$//;

	# Schedule::Cron has a 6th field for seconds, use this to avoid
	# executing exactly at second 0.
	$crontime .= " $skew";

	printlog "Scheduling queue processing using $crontime" if($conf{debug});

	my $next;
	# Catch bad entries to give sane error message
	eval {
		$cron->add_entry($crontime);
		$next = $cron->get_next_execution_time($crontime);
	};
	if($@) {
		die "Bad config: deleter_queueprocinterval: $conf{deleter_queueprocinterval}";
	}

	if($conf{debug} || ($conf{verbose} && $next - time() > 3600)) {
		printlog "Next deletion queue processing at " . scalar(localtime($next));
	}

	# Start scheduler and wait forever.
	# Queueing of incoming requests and other housekeeping is done in
	# cronsleep()
	$cron->run();
}
else {
	my $fmt = $text2fmt{$conf{deleter_queueprocinterval}};
	if(!$fmt) {
		warn "crontab style timespec requires Perl module Schedule::Cron";
		die "Unable to handle deleter_queueprocinterval: $conf{deleter_queueprocinterval}";
	}
	my $t = strftime($fmt, localtime(time()-$skew));

	# Fallback to while-loop if no scheduler
	while(1) {
		# Queueing of incoming requests and other housekeeping is done
		# in cronsleep()
		cronsleep $conf{sleeptime};

		# Trigger queue processing when output from strftime() changes
		if($t ne strftime($fmt, localtime(time()-$skew))) {
			$t = strftime($fmt, localtime());
			processqueue();
		}
	}
}
