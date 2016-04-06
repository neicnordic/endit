#!/usr/bin/perl

use warnings;
use strict;

use IPC::Run3;
use POSIX qw(strftime);

use lib '/opt/endit/';
use Endit qw(%conf readconf printlog getusage);

$Endit::logsuffix = 'tsmarchiver.log';

readconf('/opt/endit/endit.conf');
die "No basedir!\n" unless $conf{'dir'};
warn "No logdir!\n" unless $conf{'logdir'};

printlog "No timeout!\n" unless $conf{'timeout'};
printlog "No minusage!\n" unless $conf{'minusage'};

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

while(1) {
	my $dir = $conf{'dir'} . '/out/';
	my $usage = getusage($dir);
	my $timer = 0;
	while ($usage<$conf{'minusage'} && $timer <$conf{'timeout'}) {
		# print "Only $usage used, sleeping a while (slept $timer)\n";
		sleep 60;
		$timer+=60;
		$usage = getusage($dir);
	}

	my $date=strftime "%Y%m",localtime;
	my @dsmcopts = split /, /, $conf{'dsmcopts'};
	my @cmd = ('dsmc','archive','-deletefiles', @dsmcopts,
		"-description=endit","$dir/*");
	my ($out,$err);
	if((run3 \@cmd, \undef, \$out, \$err) && $? ==0) { 
		printlog $out if $conf{'verbose'};
		# files migrated to tape without issue
	} else {
		# something went wrong. log and hope for better luck next time?
		printlog localtime() . ": warning, dsmc archive failure: $!\n";
		printlog $err;
		printlog $out;
	}
}
