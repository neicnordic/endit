#!/usr/bin/perl

use warnings;
use strict;

use IPC::Run3;

####################
# Static parameters
my $dir = "/data/dcache/";
my $listfile = "/data/dcache/requestlist";

while(1) {
	sleep 60;
	opendir(REQUEST,$dir . '/request/');
	my (@requests) = grep { /^[0-9A-Fa-f]+/ } readdir(REQUEST);
	closedir(REQUEST);
	next unless @requests;
	open LF, ">", $listfile;
	my $req;
	foreach $req (@requests) {
		print LF "$dir/out/$req\n";
	}
	close LF;
	my $indir = $dir . '/in/';
	my @cmd = ('dsmc','retrieve', "-filelist=$listfile",$indir);
	my ($out,$err);
	if((run3 \@cmd, \undef, \$out, \$err) && $? ==0) { 
		# files migrated from tape without issue
	} else {
		# something went wrong. figure out what files didn't make it.
		# wait for the hsm script to remove succesful requests
		sleep 60;
		open LF, "<", $listfile;
		my (@requests) = <LF>;
		close LF;
		my $outfile;
		foreach $outfile (@requests) {
			my (@l) = split /\//,$outfile;
			my $filename = $l[$#l];
			chomp $filename;
			my $req = $dir . '/request/' . $filename;
			if( -e $req ) {
				my @cmd = ('dsmc','retrieve',$outfile,$indir);
				my ($out,$err);
				if((run3 \@cmd, \undef, \$out, \$err) && $? ==0) {
					# Went fine this time. Strange..
				} else {
					print localtime() . ": warning, dsmc retrieve error:\n";
					print $err;
					print $out;
					open EF, ">", $req . '.err';
					print EF "32\n";
					close EF;
				}
			}
		}
	}
	unlink $listfile;
}
