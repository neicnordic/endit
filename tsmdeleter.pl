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

use POSIX qw(strftime);
use File::Temp qw /tempfile/;
use File::Basename;

# Add directory of script to module search path
use lib dirname (__FILE__);
use Endit qw(%conf readconf printlog getusage);

$Endit::logsuffix = 'tsmdeleter.log';

readconf();

my $filelist = "tsm-delete-files.XXXXXX";
my $trashdir = "$conf{'dir'}/trash";
my $dounlink = 1;
$dounlink=0 if($conf{debug});


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

sub monthdeleted {
	my $month = shift;
	my @files = @_;
	my $ndel = unlink map { "$trashdir/$month/$_"; } @files;
	if ( $ndel != @files ) {
		printlog "Unlink of old files in $month failed: $!";
	}
	if(rmdir($trashdir . '/' . $month)) {
		# done with this month!
	} else {
		printlog "Couldn't delete directory $month: $!";
	}
}


sub havedeleted {
	my @files = @_;
	my $thismonth = strftime '%Y-%m', localtime;
	my $tmdir = "$trashdir/$thismonth";

	if(! -d $tmdir) {
		if(!mkdir $tmdir) {
			printlog "mkdir $tmdir failed: $!";
			# No use in continuing, will just emit loads of errors
			return;
		}
	}
	foreach my $trf (@files) {
		# Move processed trash-files, all files will be reprocessed
		# again next month to ensure they really are deleted.
		# There are corner cases to in dsmc where status is unknown
		# so this is the easy solution to ensure deletion.
		# FIXME: An optimization is to only reprocess those files
		# which we aren't sure got deleted.

		if(!rename("$trashdir/$trf", "$tmdir/$trf")) {
			 printlog "rename $trashdir/$trf to $tmdir/$trf failed: $!";
		}
	}
}

sub rundelete {
	my $filelist=shift;
	my $reallybroken=0;
	my($out, $err);
	my @dsmcopts = split(/, /, $conf{'dsmc_displayopts'});
	push @dsmcopts, split(/, /, $conf{'dsmcopts'});
	my @cmd = ('dsmc','delete','archive','-noprompt',
		@dsmcopts,"-filelist=$filelist");
        printlog "Executing: " . join(" ", @cmd) if($conf{debug});

	my $dsmcfh;
	my @errmsgs;
	my @out;
	if(open($dsmcfh, "-|", @cmd)) {
		while(<$dsmcfh>) {
			chomp;

			# Catch error messages.
			if(/^AN\w\d\d\d\d\w/) {
				push @errmsgs, $_;
				next;
			}
			# Save all output
			push @out, $_;
		}
	}
	if(!close($dsmcfh) && $!) {
		warn "closing pipe from dsmc: $!";
	}

	if($? != 0) { 
		# Some kind of problem occurred.

		# Ignore known benign errors:
		# - ANS1345E No objects on the server match object-name
		# => file already deleted
		# - ANS1302E No objects on server match query
		# => all files already deleted
		# - ANS1278W Virtual mount point 'filespace-name' is a file
		#   system. It will be backed up as a file system.
		# => irrelevant noise
		# - ANS1898I ***** Processed count files *****
		# => progress information

		foreach (@errmsgs) {
			if(/^ANS1278W/ or /^ANS1898I/) {
				next;
			}
			elsif(/^ANS1345E/ or /^ANS1302E/) {
				printlog "File already deleted: $_" if $conf{'verbose'};
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
				$msg .= sprintf "child died with signal %d, %s coredump", ($? & 127),  ($? & 128) ? 'with' : 'without';
			}
			else {
				$msg .= sprintf "child exited with value %d\n", $? >> 8;
			}
			printlog "$msg";
			if($conf{verbose}) {
				printlog "dsmc output: " . join("\n", @out);
			}
		}
	}
	return $reallybroken;
}

sub monthsago {
	my $first = shift;
	my $second = shift;
	my ($fy,$fm) = split /-/,$first;
	my ($sy,$sm) = split /-/,$second;
	return ($fy-$sy)*12+$fm-$sm;
}

my $desclong="";
if($conf{'desc-long'}) {
	$desclong = " $conf{'desc-long'}";
}
printlog("$0: Starting$desclong...");

while(1) {
	opendir(my $td, $trashdir) || die "opendir $trashdir: $!";
	my @files = grep { /^[0-9A-Fa-f]+$/ } readdir($td);
	closedir($td);

	if (@files > 0) {
		my ($fh, $filename) = tempfile($filelist, DIR=>$conf{'dir'}, UNLINK=>$dounlink);
		print $fh map { "$conf{'dir'}/out/$_\n"; } @files;
		close($fh) || die "Failed writing to $filename: $!";
		printlog "Trying to delete " . scalar(@files) . " files from file list $filename";
		if(rundelete($filename)) {
			# Have already warned in rundelete()
		} else {
			# Success
			printlog "Successfully deleted " . scalar(@files) . " files from file list $filename";
			havedeleted(@files);
		}
		unlink($filename) unless($conf{debug});
	}
	my $thismonth = strftime '%Y-%m', localtime;

	opendir(my $tm, $trashdir) || die "opendir $trashdir: $!";
	my @olddirs = grep { /^[0-9]{4}-[0-9]{2}/ } readdir($tm);
	closedir($tm);
	foreach my $month (@olddirs) {
		if(monthsago($thismonth,$month)>1) {
			my $odh;
			my $od = $trashdir . '/' . $month;
			unless(opendir($odh, $od)) {
				warn "opendir $od: $!";
				next;
			}
			@files = grep { /^[0-9A-Fa-f]+$/ } readdir($odh);
			closedir($odh);
			if (@files > 0) {
				my ($fh, $filename) = tempfile($filelist, DIR=>$conf{'dir'}, UNLINK=>$dounlink);
				print $fh map { "$conf{'dir'}/out/$_\n"; } @files;
				close($fh) || die "Failed writing to $filename: $!";
				printlog "Retrying month $month deletion of " . scalar(@files) . " files from file list $filename";
				if(rundelete($filename)) {
					# Have already warned in rundelete()
				} else {
					# Success
					printlog "Successfully reprocessed month $month deletion of " . scalar(@files) . " files from file list $filename";
					monthdeleted($month, @files);
				}
				unlink($filename) unless($conf{debug});
			}
		}
	}

	sleep $conf{sleeptime};
}
