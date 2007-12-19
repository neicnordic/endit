#!/usr/bin/perl

use warnings;
use strict;

use IPC::Run3;
use POSIX qw(strftime);

####################
# Static parameters
&readconf('/opt/d-cache/endit/endit.conf');
print "No timeout!\n" unless $conf{'timeout'};
print "No minusage!\n" unless $conf{'minusage'};

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
	while ($usage<$conf{'minusage' && $timer <$conf{'timeout'}) {
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
