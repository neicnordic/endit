package Endit;
use strict;
use warnings;
use IPC::Run3;
use POSIX qw(strftime);

our (@ISA, @EXPORT_OK);
BEGIN {
	require Exporter;
	@ISA = qw(Exporter);
	@EXPORT_OK = qw(%conf readconf printlog getusage);
}


our $logsuffix;
our %conf;

sub readconf($) {
	my $conffile = shift;
	my $key;
	my $val;
#	warn "opening conffile $conffile";
	open my $cf, '<', $conffile or die "Can't open conffile: $!";
	while(<$cf>) {
		next if $_ =~ /^#/;
		chomp;
		($key,$val) = split /: /;
		next unless defined $val;
		$conf{$key} = $val;
	}
	if (!exists $conf{maxretrievers}) {
		$conf{maxretrievers} = 1;
	}
}

sub printlog($) {
	my $msg = shift;
	my $now = strftime '%Y-%m-%d %H:%M:%S ', localtime;
	my $logfilename = $conf{'logdir'} . '/' . $logsuffix;
	open my $lf, '>>', $logfilename or warn "Failed to open $logfilename: $!";
	chomp($msg);
	print $lf $now . $msg . "\n";
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
		return $conf{'maxusage'} + 1024;
	}
	return $size/1024/1024;
}

sub readtapelist($) {
	print "reading tape list" if $conf{verbose};
	my $tapefile = shift;
	my $out = {};
	open my $tf, '<', $tapefile or return undef;
	while (<$tf>) {
		chomp;
		my ($id,$tape) = split /\s+/;
		next unless defined $id && defined $tape;
		$tape=~tr/a-zA-Z0-9.-/_/cs;
		$out->{$id} = $tape;
	}
	return $out;
}

1;
