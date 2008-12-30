package Endit;
use strict;
use warnings;
use IPC::Run3;
use POSIX qw(strftime);

our (@ISA, @EXPORT_OK);
BEGIN {
	require Exporter;
	@ISA = qw(Exporter);
	@EXPORT_OK = qw(%conf readconf printlog getusage dirhash);
}


my $logsuffix;
my %conf;

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

sub printlog($) {
	my $msg = shift;
	my $now = strftime '%Y-%m-%d %H:%M:%S ', localtime;
	open LF, '>>' . $conf{'logdir'} . '/' . $logsuffix or warn "Failed to open " . $conf{'logdir'} . '/' . $logsuffix . ": $!";
	print LF $now . $msg;
	close LF;
}

# Return filessystem usage (gigabytes)
sub getusage($) {
        my $dir = shift;
        my ($out,$err,$size);
        my @cmd = ('du','-ks',$dir);
        if((run3 \@cmd, \undef, \$out, \$err) && $? ==0) {
                ($size, undef) = split ' ',$out;
        } else {
                # failed to run du, probably just a disappearing file.
                printlog "failed to run du: $err\n";
		# Return > maxusage to try again in a minute or two
                return $conf{'maxusge'} + 1024;
        }
        return $size/1024/1024;
}

# Return filessystem usage (percent)
#sub getusage($) {
#	my $dir = shift;
#	my($bsize, $frsize, $blocks, $bfree, $bavail, $files, $ffree, $favail,
#		$fsid, $basetype, $flag, $namemax, $fstr) = statvfs($dir);
#
#	if(!defined($bsize)) {
#		printlog "Unable to statvfs $dir: $!\n";
#		exit 35;
#	}
#
#	my $fssize = $frsize * $blocks;
#	my $fsfree = $frsize * $bavail;
#
#	return (($fssize-$fsfree)/$fssize)*100;
#}

# Based on the text (typically pnfsid), return a number on an even spread
# between 0 and n-1. Based on the last $chars of the md5 in hex.
sub dirhash() {
	my $text = shift;
	my $n = shift;
	my $chars = 8;

	if(!defined($n)) {
		printlog "dirhash called without n!\n";
	}

	if($n > 16**$chars) {
		printlog "dirhash: warning: n > 16^chars, $n > 16**$chars\n";
	}

	my $md5 = md5_hex($text);
	
	my $md5s = substr $md5, -$chars, $chars;
	my $hash = $md5s % $n;
	return $hash;
}

1;
