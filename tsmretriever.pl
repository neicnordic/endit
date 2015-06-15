#!/usr/bin/perl

use warnings;
use strict;

use IPC::Run3;

use lib '/opt/endit/';
use Endit qw(%conf readconf printlog getusage);

$Endit::logsuffix = 'tsmretriever.log';

readconf('/opt/endit/endit.conf');
die "No basedir!\n" unless $conf{'dir'};
my $dir = $conf{'dir'};
my $listfile = $dir . '/requestlist';


sub checkrequest($) {
	my $req=shift;
	my $rf = $conf{'dir'} . '/request/' . $req;
	my $pid;
	if(-z $rf) {
		printlog "Zero-sized request file $rf\n";
	}
	open RF, $rf;
	while(<RF>) {
		if($_ =~ /(\d+) (\d+)/) {
			$pid = $1;
		} else {
			printlog "Broken request file $rf\n";
		}
	}
	if($pid == 0) {
		return 1;
	}
	if(getpgrp($pid) > 0) {
		return 1;
	} else {
		unlink $rf;
		return 0;
	}
}
	
while(1) {
	sleep 60;
	opendir(REQUEST,$dir . '/request/');
	my (@requests) = grep { /^[0-9A-Fa-f]+$/ } readdir(REQUEST);
	closedir(REQUEST);
	next unless @requests;
	open LF, ">", $listfile or die "Can't open listfile: $!";
	my $req;
	foreach $req (@requests) {
		if(checkrequest($req)) {
			print LF "$dir/out/$req\n";
		} else {
			printlog "Deactivating $req due to unexisting pid\n";
		}
	}
	close LF;
	my $indir = $dir . '/in/';
	my @dsmcopts = split /, /, $conf{'dsmcopts'};
	print "starting dsmc at " . localtime() . "for $#requests files\n";
	my @cmd = ('dsmc','retrieve','-replace=no','-followsymbolic=yes',@dsmcopts, "-filelist=$listfile",$indir);
	my ($in,$out,$err);
	$in="A\n";
	if((run3 \@cmd, \$in, \$out, \$err) && $? ==0) { 
		print "dsmc done successfully at " . localtime() . "\n";
		# files migrated from tape without issue
		sleep $#requests/20;
	} elsif($#requests < 10) {
		# something went wrong. figure out what files didn't make it.
		# wait for the hsm script to remove succesful requests
		# Only do this for small number of requests
		sleep 60;
		open LF, "<", $listfile;
		my (@requests) = <LF>;
		close LF;
		my $outfile;
		my $returncode;
		foreach $outfile (@requests) {
			my (@l) = split /\//,$outfile;
			my $filename = $l[$#l];
			chomp $filename;
			my $req = $dir . '/request/' . $filename;
			if( -e $req ) {
				my @cmd = ('dsmc','retrieve','-replace=no','-followsymbolic=yes',@dsmcopts,$outfile,$indir);
				my ($in, $out,$err);
				$in="A\n";
				if((run3 \@cmd, \$in, \$out, \$err) && $? ==0) {
					# Went fine this time. Strange..
				} else {
					$returncode=$? >> 8;
					printlog localtime() . ": warning, dsmc returned $returncode on $outfile:\n";
					printlog $err;
					printlog $out;
					open EF, ">", $req . '.err' or warn "Could not open $req.err file: $!\n";
					print EF "32\n";
					close EF;
				}
			}
		}
	} else {
		print "dsmc done unsuccessfully at " . localtime() . "\n";
		# Large number of requests broke, try again later
		sleep 600;
	}
	unlink $listfile;
}
