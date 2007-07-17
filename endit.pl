#!/usr/bin/perl

use warnings;
use strict;
use English;

use Filesys::Statvfs;
use File::Path;
use File::Basename;
use Digest::MD5 qw(md5_hex);


####################
# Static parameters
my $dir = "/grid/dcache/";
my $pnfs = "/pnfs/hpc2n.umu.se/data/";
# Don't use pnfs, use new interface
$pnfs=undef; 
my $logfile = "/var/log/dcache/endit.log";
# Throttling for hsm put. $pool_size << $maxusage << $fs_size-$pool_size
my $maxusage = 60; # Percent

sub printlog($) {
	my $msg = shift;
	open LF, '>>' . $logfile;
	print LF $msg;
	close LF;
}

printlog 'endit.pl starting: ' . join (' ', @ARGV) . "\n";


####################
# Arguments
my $command = shift; # get|put
my $pnfsid = shift;
my $filename = shift;
my %options;

foreach my $opt (@ARGV) {
	$options{$1} = $2 if $opt =~ /^-(\w+)=(\S+)$/
		or printlog "Warning: Bad argument $opt\n";
}

if(!defined($filename)) {
        printlog "Argument error, too few arguments.\n";
        exit 35;
}

if(!defined($options{'si'})) {
	printlog "Argument error: lacking si.\n";
	exit 35;
}

# Return filessystem usage (percent)
sub getusage($) {
	my $dir = shift;
	my($bsize, $frsize, $blocks, $bfree, $bavail, $files, $ffree, $favail,
		$fsid, $basetype, $flag, $namemax, $fstr) = statvfs($dir);

	if(!defined($bsize)) {
		printlog "Unable to statvfs $dir: $!\n";
		exit 35;
	}

	my $fssize = $frsize * $blocks;
	my $fsfree = $frsize * $bavail;

	return (($fssize-$fsfree)/$fssize)*100;
}

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

if($command eq 'put') {
	my $usage = getusage($dir);
	while ($usage>$maxusage) {
		printlog "$usage used, sleeping until less than $maxusage\n";
		sleep 60;
		$usage = getusage($dir);
	}
	
	my $size;
	my $store;
	my $group;
	my $si = $options{'si'};
	my @keyval = split /;/,$si;
	foreach my $k (@keyval) {
		if($k =~ /^size=(\d+)/) {
			$size=$1;
		}
		if($k =~ /^store=(.+)/) {
			$store=$1;
		}
		if($k =~ /^group=(.+)/) {
			$group=$1;
		}
	}

	if(link($filename,$dir . '/out/' . $pnfsid)) {
		exit 0;
	} else {
		printlog "Failed to link $filename to $dir/out/$pnfsid: $!\n";
		exit 30;
	}
	if(defined $pnfs) {
		# use old pnfs metadata
		if(open FH,'>',$pnfs . '/.(access)(/' . $pnfsid . ')(1)') {
			if(!print FH "$store $group $pnfsid\n") {
				printlog "write $pnfs/.(access)(/$pnfsid)(1) failed: $!\n";
				exit 34;
			}
			close FH;
			# all is good..
		} else {
			printlog "opening $pnfs/.(access)(/$pnfsid)(1) failed: $!\n";
			exit 34;
		}
		if(open FH,'>',$pnfs.'/.(pset)('.$pnfsid.')(size)('. $size.')') {
			close FH;
			# all is good..
		} else {
			printlog "touch $pnfs/.(pset)($pnfsid)(size)($size) failed: $!\n";
			exit 32;
		}
	} else {
		# new pnfs-free interface
		print "osm://hpc2n.umu.se/?store=$store&group=$group&bfid=$pnfsid\n";
	}
}

if($command eq 'get') {
	my $si = $options{'si'};

	my $dirname = dirname($filename);

	# Now we need size out of $si
	my $size;
	my @keyval = split /;/,$si;
	foreach my $k (@keyval) {
		if($k =~ /size=(\d+)/) {
			$size=$1;
		}
	}
	
	if(-f $filename) {
		my @stat = stat $filename;
		if (defined $stat[7] && $stat[7] == $size) {
			exit 0; 
		} else {
			printlog "Asked to restore a file that already exists with a different size!\n";
			printlog "The file was $filename ($pnfsid), exiting...\n";
			exit 37;
		}
	}
	
	if(! -d $dirname) {
         	eval { mkpath($dirname) };
         	if ($@) {
			printlog "Couldn't create $dir: $@\n";
			exit 32;
         	}
	}
	
	if(link($dir . '/out/' . $pnfsid, $filename)) {
		# We got lucky, file not removed from migration dir yet
		exit 0;
	} else {
		# printlog "Debug: link failed: $!\n";
		# We are less lucky, have to get it from tape...
	}
	
	if(open FH,'>',$dir . '/request/' . $pnfsid) {
		print FH "$PID $BASETIME\n";
		close FH;
		# all is good..
	} else {
		printlog "touch $dir/request/$pnfsid failed: $!\n";
		exit 32;
	}
	
	my $insize;
	do {
		sleep 5;
		my $errfile=$dir . '/request/' . $pnfsid . '.err';
		if(-f $errfile) {
			sleep 1;
			open(IN, $errfile) || die "Unable to open $errfile: $!";
			my @err = <IN>;
			close(IN);
			unlink $errfile;
			unlink $dir . '/request/' . $pnfsid;
			unlink $dir . '/in/' . $pnfsid;
			if($err[0] > 0) {
				exit $err[0]; # Error code in first line
			} else {
				exit 32;
			}
		}
		$insize = (stat $dir . '/in/' . $pnfsid)[7];
	} until (defined $insize && $insize == $size);
	
	if(unlink $dir . '/request/' . $pnfsid) {
		# all is well..
	} else {
		printlog "Warning, unlink $dir/request/$pnfsid failed: $!\n";
	}

	if(rename $dir . '/in/' . $pnfsid, $filename) {
		exit 0;
	} else {
		printlog "mv $pnfsid $filename failed: $!\n";
		exit 33;
	}
}

exit 0;
