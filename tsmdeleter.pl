#!/usr/bin/perl

use warnings;
use strict;

use IPC::Run3;

####################
# Static parameters
my %conf;
&readconf('/opt/endit/endit.conf');
die "No basedir!\n" unless $conf{'dir'};
my $filelist = "$conf{'dir'}/tsm-delete-files";
my $trashdir = "$conf{'dir'}/trash";

sub readconf($) {
        my $conffile = shift;
        my $key;
        my $val;
        open CF, '<'.$conffile or die "Can't open conffile: $!";
        while(<CF>) {
                next if $_ =~ /^#/;
                ($key,$val) = split /: /;
                next unless defined $val;
                $conf{$key} = $val;
        }
}

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
