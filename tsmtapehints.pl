#!/usr/bin/perl

#   ENDIT - Efficient Northern Dcache Interface to TSM
#   Copyright (C) 2018-2022 <Niklas.Edmundsson@hpc2n.umu.se>
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
use JSON;
use File::Temp qw /tempfile/;
use File::Basename;

# Add directory of script to module search path
use lib dirname (__FILE__);
use Endit qw(%conf readconf printlog);

$Endit::logsuffix = 'tsmtapehints.log';

# Turn off output buffering
$| = 1;

readconf();

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

my $dsmcpid;
sub killchild() {

	if(defined($dsmcpid)) {
		kill("TERM", $dsmcpid);
	}
}

$SIG{INT} = sub { warn("Got SIGINT, exiting...\n"); killchild(); exit; };
$SIG{QUIT} = sub { warn("Got SIGQUIT, exiting...\n"); killchild(); exit; };
$SIG{TERM} = sub { warn("Got SIGTERM, exiting...\n"); killchild(); exit; };
$SIG{HUP} = sub { warn("Got SIGHUP, exiting...\n"); killchild(); exit; };

my $desclong="";
if($conf{'desc-long'}) {
	$desclong = " $conf{'desc-long'}";
}
printlog("$0: Starting$desclong...");

my $hintfile;
if(exists $conf{retriever_hintfile}) {
	$hintfile = $conf{retriever_hintfile};
}

if($ENV{ENDIT_RETRIEVER_HINTFILE}) {
	$hintfile = $ENV{ENDIT_RETRIEVER_HINTFILE};
	warn "Overriding with env ENDIT_RETRIEVER_HINTFILE $hintfile";
}

if (!$hintfile) {
	die "retriever_hintfile not configured, nothing to do!";
}

my $hftmp = File::Temp->new(TEMPLATE => "$hintfile.XXXXXX");
my $hintfiletmp = $hftmp->filename;

my @dsmcopts = split(/, /, $conf{'dsmc_displayopts'});
push @dsmcopts, split(/, /, $conf{'dsmcopts'});
my $outdir = "$conf{dir}/out";
my @cmd = ('dsmc','query','archive','-filesonly','-detail',@dsmcopts,"$outdir/*");
my $cmdstr = "ulimit -t $conf{dsmc_cpulimit} ; ";
$cmdstr .= "'" . join("' '", @cmd) . "' 2>&1";

printlog "Executing: $cmdstr" if($conf{debug});

$dsmcpid = open(my $dsmcfh, "-|", $cmdstr) || die "can't start dsmc: $!";

my %tapelist;

# ==============
# Example output
# --------------
# IBM Spectrum Protect
# Command Line Backup-Archive Client Interface
#   Client Version 8, Release 1, Level 4.1 
#   Client date/time: 2018-06-01 15:39:04
# (c) Copyright by IBM Corporation and other(s) 1990, 2018. All Rights Reserved. 
# 
# Node Name: NODENAME
# Session established with server SERVERNAME: Linux/x86_64
#   Server Version 8, Release 1, Level 5.000
#   Server date/time: 2018-06-01 15:39:04  Last access: 2018-06-01 15:37:56
# 
# Accessing as node: ASNODENAME
#              Size  Archive Date - Time    File - Expires on - Description
#              ----  -------------------    -------------------------------
#         72,191  B  2016-04-01 02:17:31    /grid/pool/out/0000004C9A19B5FA480398AED8A6CD61107D Never endit  RetInit:STARTED  ObjHeld:NO
#          Modified: 2016-04-01 01:52:11  Accessed: 2016-04-01 01:05:51  Inode changed: 2016-04-01 01:59:24
#          Compression Type: None  Encryption Type:        None  Client-deduplicated: NO
#   Media Class: Library  Volume ID: 724216  Restore Order: 00000000-00000002-00000000-00CEF5A1
#
# Alternatively, if no files are stored on server:
# ANS1092W No files matching search criteria were found
# Also, dsmc exits with return code 8.
# --------------

my ($lastfile, $size, $timestamp);
my %hints;

my @errmsgs;

while(<$dsmcfh>) {
	chomp;

	# Catch error messages, only printed on non-zero return code from dsmc
	if(/^AN\w\d\d\d\d\w/) {
		push @errmsgs, $_;
		next;
	}
	# Match a line with a file name, save the useful info.
	# We assume that our file name doesn't contain a space character!
	elsif(m!^\s*([\d,]+)\s+(\S+)\s+(\d\d\d\d-\d\d-\d\d\s+\d\d:\d\d:\d\d)\s+$outdir/(\S+)\s+(\S+)!)
	{
		$size = $1;
		my $sizeunit = $2;
		$timestamp = $3;
		$lastfile = $4;
		my $expire = $5;

		if($sizeunit eq 'B') {
			$size =~ s/,//g;
		}
		else {
			$size = undef;
		}

		if($expire ne 'Never') {
			warn "File $lastfile will expire on $expire";
		}
		next;
	}
	elsif(!defined($lastfile)) {
		next;
	}
	# Match a line with a Volume ID and Restore Order.
	elsif(/Volume\sID:\s+(\d+).*Restore\sOrder:\s+(\S+)/) {
		my $volid = $1;
		my $restorder = $2;

		if($hints{$lastfile}) {
			if(!defined($hints{$lastfile}{duplicates})) {
				$hints{$lastfile}{duplicates} = 0;
			}
			$hints{$lastfile}{duplicates} ++;
			if($size && $hints{$lastfile}{size} && $size != $hints{$lastfile}{size})
			{
				$hints{$lastfile}{duplicatesizemismatch} = 1;
			}
		}
		else {
			$hints{$lastfile}{volid} = $volid;
			$hints{$lastfile}{order} = $restorder;
			$hints{$lastfile}{timestamp} = $timestamp;
			if(defined($size)) {
				$hints{$lastfile}{size} = $size;
			}
		}

		$lastfile = undef;
	}
}

if(!close($dsmcfh) && $!) {
	die "closing pipe from dsmc: $!";
}
elsif($? != 0) {
	my $msg = "dsmc query archive failure: ";
	if ($? == -1) {
		$msg .= "failed to execute: $!";
	}
	elsif ($? & 127) {
		$msg .= sprintf "dsmc died with signal %d, %s coredump", ($? & 127),  ($? & 128) ? 'with' : 'without';
	}
	else {
		my $rc = $? >> 8;
		if($rc == 8 && scalar(@errmsgs) == 1 && $errmsgs[0] =~ /^ANS1092W/)
		{
			# No files stored (yet), don't get too upset.
			$msg = undef;
		}
		else {
			$msg .= sprintf "dsmc exited with value %d", $rc;
		}
	}
	if($msg) {
		foreach my $errmsg (@errmsgs) {
			warn "dsmc error message: $errmsg";
		}

		die "$msg, aborting...";
	}
}

$dsmcpid = undef;

my $dupfiles = 0;
my $dupcount = 0;
while(my($k, $v) = each %hints) {
	if($v->{duplicates}) {
		$dupfiles ++;
		$dupcount += $v->{duplicates};
		my $s = "File $k has $v->{duplicates} duplicates";
		if($v->{duplicatesizemismatch}) {
			$s .= ", some with different size";
			delete($v->{duplicatesizemismatch});
		}
		warn $s;
		delete($v->{duplicates});
	}

	if(defined($v->{timestamp})) {
		delete($v->{timestamp});
	}
}

if($dupfiles) {
	warn "$dupfiles files with total $dupcount duplicates";
}

print $hftmp encode_json(\%hints),"\n";

$hftmp->unlink_on_destroy(0);
close($hftmp) || die "Writing $hintfiletmp: $!";

# File::Temp creates a file as private as possible. However, we want to
# have permissions in accordance to the user chosen umask.
chmod(0666 & ~umask(), $hintfiletmp) || die "chmod $hintfiletmp: $!";

rename($hintfiletmp, $hintfile) || die "rename $hintfiletmp $hintfile: $!";
printlog "$hintfile updated (" . scalar(keys %hints) . " hints)";
