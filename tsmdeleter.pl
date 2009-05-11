#!/usr/bin/perl

use warnings;
use strict;

use IPC::Run3;

use lib '/opt/endit/';
use Endit qw(%conf readconf printlog getusage);

$Endit::logsuffix = 'tsmarchiver.log';

readconf('/opt/endit/endit.conf');
die "No basedir!\n" unless $conf{'dir'};
my $filelist = "$conf{'dir'}/tsm-delete-files";
my $trashdir = "$conf{'dir'}/trash";

while(1) {
	my @files = ();
	opendir(TD, $trashdir);
	@files = grep { !/^\.$/ && !/^\.\.$/ } readdir(TD);
	close(TD);
	if (@files > 0) {
		unlink $filelist;
		open(FL, ">$filelist");
		print FL map { "$conf{'dir'}/out/$_\n"; } @files;
		close(FL);
		my($out, $err);
		my @dsmcopts = split /, /, $conf{'dsmcopts'};
		my @cmd = ('dsmc','delete','archive','-noprompt',
			@dsmcopts,"-filelist=$filelist");
		if((run3 \@cmd, \undef, \$out, \$err) && $? ==0) { 
			# files removed from tape without issue
			my $ndel = unlink map { "$trashdir/$_"; } @files;
			if ( $ndel != @files ) {
				printlog $out;
				printlog localtime() . ": warning, unlink of tsm deleted files failed: $!\n";
				rename $filelist, $filelist."failedunlink";
			}
		} else {
			# something went wrong. log and hope for better luck next time?

			# unless all is: ANS1345E - file already deleted
			# or ANS1302E - all files already deleted
			my @outl = split /\n/m, $out;
			my @errorcodes = grep (/^ANS/, @outl);
			my $error;
			my $reallybroken=0;
			foreach $error (@errorcodes) {
				if($error =~ /^ANS1345E/ or $error =~ /^ANS1302E/) {
					printlog "File already deleted:\n$error\n";
				} else {
					$reallybroken=1;
				}
			
			}
			if($reallybroken) {
				printlog localtime() . ": warning, dsmc remove archive failure: $!\n";
				printlog $err;
				printlog $out;
			} else {
				my $ndel = unlink map { "$trashdir/$_"; } @files;
				if ( $ndel != @files ) {
					printlog localtime() . ": warning, unlink of tsm deleted files failed: $!\n";
					rename $filelist, $filelist."failedunlink";
				}
			}
		}
	}

	sleep 1800;
}
