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

use lib '/opt/endit/';
use Endit qw(%conf readconf printlog getusage);

$Endit::logsuffix = 'tsmarchiver.log';

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

$SIG{INT} = sub { printlog("Got SIGINT, exiting..."); exit; };
$SIG{QUIT} = sub { printlog("Got SIGQUIT, exiting..."); exit; };
$SIG{TERM} = sub { printlog("Got SIGTERM, exiting..."); exit; };

printlog("$0: Starting...");

while(1) {
	my $dir = $conf{'dir'} . '/out/';

        opendir(my $dh, $dir) || die "opendir $dir: $!";
        my $filecount = scalar(grep { /^[0-9A-Fa-f]+$/ } readdir($dh));
        closedir($dh);

	if(!$filecount) {
		# No files, just sleep until next iteration.
		sleep($conf{sleeptime});
		next;
	}

	my $usage = getusage($dir);
	my $timer = 0;
	while ($usage<$conf{'minusage'} && $timer <$conf{'timeout'}) {
		printlog "Only $usage GiB used, sleeping a while (slept $timer)" if($conf{verbose});
		sleep $conf{sleeptime};
		$timer+=$conf{sleeptime};
		$usage = getusage($dir);
	}

	my $usagestr = sprintf("%.03f GiB in %d files", $usage, $filecount);
	printlog "Trying to archive files from $dir - $usagestr";

	my @dsmcopts = split /, /, $conf{'dsmcopts'};
	my @cmd = ('dsmc','archive','-deletefiles', @dsmcopts,
		"-description=endit","$dir/*");
	my ($out,$err);
	if((run3 \@cmd, \undef, \$out, \$err) && $? ==0) { 
		printlog "Successfully archived files from $dir.";
		printlog $out if $conf{'verbose'};
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
}
