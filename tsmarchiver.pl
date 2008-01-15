#!/usr/bin/perl

use warnings;
use strict;

use IPC::Run3;
use POSIX qw(strftime);


####################
# Static parameters
my %conf;
&readconf('/opt/endit/endit.conf');

sub printlog($) {
	my $msg = shift;
	open LF, '>>' . $conf{'logdir'} . '/tsmarchiver.log';
	print LF $msg;
	close LF;
}

printlog "No timeout!\n" unless $conf{'timeout'};
printlog "No minusage!\n" unless $conf{'minusage'};


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


# Return filessystem usage (gigabytes)
sub getusage($) {
	my $dir = shift;
	my ($out,$err,$size);
	my @cmd = ('du','-ks',$dir);
	if((run3 \@cmd, \undef, \$out, \$err) && $? ==0) {
		($size, undef) = split ' ',$out;
	} else {
		# failed to run du, return 0 for graceful degradation.
		printlog "failed to run du:\n";
		printlog $out;
		printlog $err;
		$size=0;
	}
	return $size/1024/1024;
}

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
	my @cmd = ('dsmc','archive','-v2archive','-deletefiles', @dsmcopts,
		'-deletefiles',"-description=$date","$dir/*");
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
