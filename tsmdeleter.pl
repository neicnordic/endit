#!/usr/bin/perl

use warnings;
use strict;

use IPC::Run3;
use POSIX qw(strftime);
use File::Temp qw /tempfile/;


use lib '/opt/endit/';
use Endit qw(%conf readconf printlog getusage);

$Endit::logsuffix = 'tsmdeleter.log';

readconf('/opt/endit/endit.conf');

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
	if(-d $conf{'dir'} . '/trash/'. $thismonth) {
		# Already created
	} else {
		mkdir  $conf{'dir'} . '/trash/'. $thismonth;
	}
	foreach my $trf (@files) {
		if(rename($conf{'dir'} . '/trash/' . $trf, $conf{'dir'} . '/trash/'. $thismonth . '/' . $trf)) {
			# To be run again next month
		} else {
			 printlog "rename trash/$trf to trash/$thismonth/$trf failed: $!";
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
		# something went wrong. log and hope for better luck next time?

		# unless all is: ANS1345E - file already deleted
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
		my ($fh, $filename) = tempfile($filelist, DIR=>$conf{'dir'});
		print $fh map { "$conf{'dir'}/out/$_\n"; } @files;
		close($fh) || die "Failed writing to $filename: $!";
		if(rundelete($filename)) {
			# Have already warned in rundelete()
		} else {
			# Success
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
				my ($fh, $filename) = tempfile($filelist, DIR=>$conf{'dir'});
				print $fh map { "$conf{'dir'}/out/$_\n"; } @files;
				close($fh) || die "Failed writing to $filename: $!";
				if(rundelete($filename)) {
					# Have already warned in rundelete()
				} else {
					# Success
					unlink($filename);
					monthdeleted($month, @files);
				}
			}
		}
	}

	sleep 1800;
}
