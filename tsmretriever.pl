#!/usr/bin/perl

use warnings;
use strict;

use IPC::Run3;

####################
# Static parameters
my %conf;
&readconf('/opt/endit/endit.conf');
die "No basedir!\n" unless $conf{'dir'};
my $dir = $conf{'dir'};
my $listfile = $dir . '/requestlist';

sub printlog($) {
	my $msg = shift;
	open LOGF, '>>' . $conf{'logdir'} . '/tsmretriever.log' ;
	print LOGF $msg;
	close LOGF;
}

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
	if(getpgrp($pid) > 0) {
		return 1;
	} else {
		unlink $rf;
		return 0;
	}
}
	


sub readconf($) {
        my $conffile = shift;
        my $key;
        my $val;
        open CF, '<'.$conffile or die "Can't open conffile: $!";
        while(<CF>) {
                next if $_ =~ /^#/;
		chomp;
                ($key,$val) = split /: /;
                next unless defined $val;
                $conf{$key} = $val;
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
	my @cmd = ('dsmc','retrieve','-replace=no','-followsymbolic=yes',@dsmcopts, "-filelist=$listfile",$indir);
	my ($in,$out,$err);
	$in="A\n";
	if((run3 \@cmd, \$in, \$out, \$err) && $? ==0) { 
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
				my @cmd = ('dsmc','retrieve','-replace=no','-followsymbolic=yes',@dsmcopts,$outfile,$indir);
				my ($in, $out,$err);
				$in="A\n";
				if((run3 \@cmd, \$in, \$out, \$err) && $? ==0) {
					# Went fine this time. Strange..
				} else {
					printlog localtime() . ": warning, dsmc retrieve error on $outfile:\n";
					printlog $err;
					printlog $out;
					open EF, ">", $req . '.err' or warn "Could not open $req.err file: $!\n";
					print EF "32\n";
					close EF;
				}
			}
		}
	}
	unlink $listfile;
}
