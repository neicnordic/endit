#!/usr/bin/perl

use warnings;
use strict;

use IPC::Run3;

####################
# Static parameters
my $trashbase = "/opt/pnfsdb/pnfs/trash";
my $trashdir = "$trashbase/1";
my $filelist = "$trashbase/tsm-delete-files";
my $basedir = "/data/dcache/out";

while(1) {
	my @files = ();
	opendir(TD, $trashdir);
	@files = grep { !/^\.$/ && !/^\.\.$/ } readdir(TD);
	close(TD);
	if (@files > 0) {
		unlink $filelist;
		open(FL, ">$filelist");
		print FL map { "$basedir/$_\n"; } @files;
		close(FL);
		my($out, $err);
		my @cmd = ('dsmc','delete','archive','-noprompt',
			"-filelist=$filelist");
		if((run3 \@cmd, \undef, \$out, \$err) && $? ==0) { 
			print $out;
			# files removed from tape without issue
			my $ndel = unlink map { "$trashdir/$_"; } @files;
			if ( $ndel != @files ) {
				print localtime() . ": warning, unlink of tsm deleted files failed: $!\n";
				rename $filelist, $filelist."failedunlink";
			}
		} else {
			# something went wrong. log and hope for better luck next time?
			print localtime() . ": warning, dsmc remove archive failure: $!\n";
			print $err;
			print $out;
		}
	}

	sleep 1800;
}
