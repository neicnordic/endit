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
my $listfile = $dir . 'requestlist';

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
	open LF, ">", $listfile;
	my $req;
	foreach $req (@requests) {
		print LF "$dir/out/$req\n";
	}
	close LF;
	my $indir = $dir . '/in/';
	my @dsmcopts = split /, /, $conf{'dsmcopts'};
	my @cmd = ('dsmc','retrieve',@dsmcopts, "-filelist=$listfile",$indir);
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
				my @cmd = ('dsmc','retrieve',@dsmcopts,$outfile,$indir);
				my ($out,$err);
				if((run3 \@cmd, \undef, \$out, \$err) && $? ==0) {
					# Went fine this time. Strange..
				} else {
					print localtime() . ": warning, dsmc retrieve error on $outfile:\n";
					print $err;
					print $out;
					open EF, ">", $req . '.err' or warn "Could not open $req.err file: $!\n";
					print EF "32\n";
					close EF;
				}
			}
		}
	}
	unlink $listfile;
}
