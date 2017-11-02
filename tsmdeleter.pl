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


use lib '/opt/endit/';
use Endit qw(%conf readconf printlog getusage);

$Endit::logsuffix = 'tsmdeleter.log';

readconf();

my $filelist = "tsm-delete-files.XXXXX";
my $trashdir = "$conf{'dir'}/trash";

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
	my @dsmcopts = split /, /, $conf{'dsmcopts'};
	my @cmd = ('dsmc','delete','archive','-noprompt',
		@dsmcopts,"-filelist=$filelist");
	if((run3 \@cmd, \undef, \$out, \$err) && $? ==0) { 
		# files removed from tape without issue
	} else {
		# ANS1345E - file already deleted
		# or ANS1302E - all files already deleted
		# Also ignore ANS1278W - irrelevant
		my @outl = split /\n/m, $out;
		my @errorcodes = grep (/^ANS/, @outl);
		foreach my $error (@errorcodes) {
			if($error =~ /^ANS1345E/ or $error =~ /^ANS1302E/ or $error =~ /^ANS1278W/ or $error =~ /^ANS1898I/) {
				printlog "File already deleted:\n$error\n" if $conf{'verbose'};
			} else {
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
			printlog "STDERR: $err";
			printlog "STDOUT: $out";
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

printlog("$0: Starting...");

# Check writability of needed dirs
my ($chkfh, $chkfn) = tempfile($filelist, DIR=>$conf{'dir'});
close($chkfh) || die "Failed closing $chkfn: $!";
unlink($chkfn);

while(1) {
	my @files = ();
	opendir(TD, $trashdir);
	@files = grep { /^[0-9A-Fa-f]+$/ } readdir(TD);
	close(TD);
	if (@files > 0) {
		my ($fh, $filename) = tempfile($filelist, DIR=>$conf{'dir'}, UNLINK=>0);
		print $fh map { "$conf{'dir'}/out/$_\n"; } @files;
		close($fh) || die "Failed writing to $filename: $!";
		printlog "Trying to delete " . scalar(@files) . " files from file list $filename";
		if(rundelete($filename)) {
			# Have already warned in rundelete()
			# Explicitly not unlink():ing failed filelist
		} else {
			# Success
			printlog "Successfully deleted " . scalar(@files) . " files from file list $filename";
			unlink($filename);
			havedeleted(@files);
		}
	}
	my $thismonth = strftime '%Y-%m', localtime;
	my @olddirs = ();
	opendir(TD, $trashdir);
	@olddirs = grep { /^[0-9]{4}-[0-9]{2}/ } readdir(TD);
	close(TD);
	foreach my $month (@olddirs) {
		if(monthsago($thismonth,$month)>1) {
			opendir(TD,$trashdir . '/' . $month);
			@files = grep { /^[0-9A-Fa-f]+$/ } readdir(TD);
			closedir(TD);
			if (@files > 0) {
				my ($fh, $filename) = tempfile($filelist, DIR=>$conf{'dir'}, UNLINK=>0);
				print $fh map { "$conf{'dir'}/out/$_\n"; } @files;
				close($fh) || die "Failed writing to $filename: $!";
				printlog "Retrying month $month deletion of " . scalar(@files) . " files from file list $filename";
				if(rundelete($filename)) {
					# Have already warned in rundelete()
					# Explicitly not unlink():ing failed filelist
				} else {
					# Success
					printlog "Successfully reprocessed month $month deletion of " . scalar(@files) . " files from file list $filename";
					unlink($filename);
					monthdeleted($month, @files);
				}
			}
		}
	}

	sleep $conf{sleeptime};
}
