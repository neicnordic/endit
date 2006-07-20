#!/usr/bin/perl

use warnings;
use strict;

use IPC::Run3;
use POSIX qw(strftime);

####################
# Static parameters
my $dir = "/data/dcache/out/";
my $minusage = 100; # Gigabytes
my $timeout = 7200; # Seconds with less than $minusage - archive anyway

# Return filessystem usage (percent)
sub getusage($) {
	my $dir = shift;
	my ($out,$err,$size);
	my @cmd = ('du','-ks',$dir);
	if((run3 \@cmd, \undef, \$out, \$err) && $? ==0) {
		($size, undef) = split ' ',$out;
	} else {
		# failed to run du, return 0 for graceful degradation.
		print "failed to run du: $err\n";
		$size=0;
	}
	return $size/1024/1024;
}

while(1) {
	my $usage = getusage($dir);
	my $timer = 0;
	while ($usage<$minusage && $timer <$timeout) {
		# print "Only $usage used, sleeping a while (slept $timer)\n";
		sleep 60;
		$timer+=60;
		$usage = getusage($dir);
	}

	my $date=strftime "%Y%m",localtime;
	my @cmd = ('dsmc','archive','-v2archive','-deletefiles',
		"-description=$date","$dir/*");
	my ($out,$err);
	if((run3 \@cmd, \undef, \$out, \$err) && $? ==0) { 
		# print $out;
		# files migrated to tape without issue
	} else {
		# something went wrong. log and hope for better luck next time?
		print localtime() . ": warning, dsmc archive failure: $!\n";
		print $err;
		print $out;
	}
}
