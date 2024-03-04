#   ENDIT - Efficient Northern Dcache Interface to TSM
#   Copyright (C) 2006-2017 Mattias Wadenstein <maswan@hpc2n.umu.se>
#   Copyright (C) 2018-2023 <Niklas.Edmundsson@hpc2n.umu.se>
#
#   This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program. If not, see <http://www.gnu.org/licenses/>.

package Endit;
use strict;
use warnings;
use POSIX qw(strftime);
use File::Temp qw /tempfile/;
use File::Basename;
use Sys::Hostname;
use JSON;
use Time::HiRes qw(usleep);

our (@ISA, @EXPORT_OK);
BEGIN {
	require Exporter;
	@ISA = qw(Exporter);
	@EXPORT_OK = qw(%conf readconf printlog readconfoverride writejson writeprom getgitversiontag);
}


our $logname;
our %conforig; # The original config read from the main file.
our %conf; # The current config with override(s) applied.

# Remember PID of the master process, otherwise log messages from worker
# children gets logged with different PID...
my $masterpid;

sub printlog($) {
	my $msg = shift;
	my $now = strftime '%Y-%m-%d %H:%M:%S', localtime;

	my $lf;
	if($conf{'logdir'}) {
		my $logfilename = "$conf{'logdir'}/$logname.log";
		open $lf, '>>', $logfilename or warn "Failed to open $logfilename: $!";
	}

	my $desc = "";
	if($conf{'desc-short'}) {
		$desc = "$conf{'desc-short'} ";
	}
	if(!defined($masterpid)) {
		$masterpid = $$;
	}

	my $childpid="";
	if($masterpid != $$) {
		$childpid="[$$]";
	}

	chomp($msg);
	my $str = "$now ${desc}[$masterpid]$childpid $msg\n";

	if($lf && $lf->opened) {
		print $lf $str;
		if(!close($lf)) {
			print $str;
		}
	} else {
		print $str;
	}
}

my %confold2new = (
	timeout => 'archiver_timeout',
	minusage => 'archiver_threshold1_usage',
	maxretrievers => 'retriever_maxworkers',
	tapefile => 'retriever_hintfile',
	remounttime => 'retriever_remountdelay',
);

my %confobsolete = (
	hsminstance => 1,
	remotedirs => 1,
	pollinginterval => 1,
	maxusage => 1,
	archiver_threshold1_dsmcopts => 1,
	archiver_threshold2_dsmcopts => 1,
	archiver_threshold3_dsmcopts => 1,
	archiver_threshold4_dsmcopts => 1,
	archiver_threshold5_dsmcopts => 1,
	archiver_threshold6_dsmcopts => 1,
	archiver_threshold7_dsmcopts => 1,
	archiver_threshold8_dsmcopts => 1,
	archiver_threshold9_dsmcopts => 1,
);

my %confitems = (
	'desc-short' => {
		example => 'endit-'.hostname,
		desc => 'Short description of this instance, written on each log line',
	},
	'desc-long' => {
		example => 'ENDIT on host '.hostname,
		desc => 'Long description of this instance, written on startup',
	},
	dir => {
		example => '/grid/pool',
		desc => 'Base directory',
	},
	logdir => {
		example => '/var/log/dcache',
		desc => 'Log directory',
	},
	confoverridefile => {
		default => '/run/endit/conf-override.json',
		desc => 'JSON format file for runtime configuration overrides',
		# Intended for automatic/dynamic configuration changes, like
		# limiting archiver/retriever sessions based on current load.
	},
	currstatsdir => {
		default => '/run/endit',
		desc => 'Directory name for JSON format current status files',
		# Intended for consumption by tools and/or metric logging.
	},
	dsmcopts => {
		example => '-asnode=EXAMPLENODE, -errorlogname=/var/log/dcache/dsmerror.log',
		desc => 'Base options to dsmc, ", "-delimited list',
	},
	dsmc_displayopts => {
		default => '-dateformat=3, -timeformat=1, -numberformat=1',
		# dsmc display options, not intended to be modified by users.
		# Used for all commands except archive.
		# ", "-delimited list.
	},
	dsmc_cpulimit => {
		default => 172800,
		desc => 'Default CPU ulimit for spawned dsmc processes (seconds)',
	},
	sleeptime => {
		default => 60,
		desc => 'Sleep for this many seconds between each cycle',
		reqposint => 1,
	},
	archiver_timeout => {
		default => 21600,
		example => 21600,
		desc => "Push to tape anyway after these many seconds.\nThis should be significantly shorter than the dCache store timeout (commonly 1 day).",
		reqposint => 1,
		canoverride => 1,
	},
	archiver_retrytimeout => {
		default => 3600,
		desc => "Timeout used instead of archiver_timeout when in retry situations, usually significantly lower than the archiver_timeout to avoid hitting the dCache store timeout.",
		reqposint => 1,
		canoverride => 1,
	},
	archiver_timeout_dsmcopts => {
		desc => 'Extra dsmcopts for archiver_timeout',
	},
	archiver_threshold1_usage => {
		default => 500,
		example => 500,
		desc => "Require this usage, in gigabytes, before migrating to tape using 1 session.\nTune this to be 20-30 minutes or more of tape activity.",
		reqposint => 1,
		canoverride => 1,
	},
	archiver_threshold2_usage => {
		example => 2000,
		desc => "When exceeding this usage, in gigabytes, use 2 sessions.\nThis is used to trigger an additional tape session if one\nsession can't keep up. Recommended setting is somewhere between\ntwice the archiver_threshold1_usage and 20% of the total pool size.",
		reqint => 1,
		canoverride => 1,
	},
	archiver_threshold3_usage => {
		desc => "Also archiver_threshold3 ... archiver_threshold9 available if needed.\nThe number corresponds to the number of sessions spawned.",
		reqint => 1,
		canoverride => 1,
	},
	archiver_threshold4_usage => {
		reqint => 1,
		canoverride => 1,
	},
	archiver_threshold5_usage => {
		reqint => 1,
		canoverride => 1,
	},
	archiver_threshold6_usage => {
		reqint => 1,
		canoverride => 1,
	},
	archiver_threshold7_usage => {
		reqint => 1,
		canoverride => 1,
	},
	archiver_threshold8_usage => {
		reqint => 1,
		canoverride => 1,
	},
	archiver_threshold9_usage => {
		reqint => 1,
		canoverride => 1,
	},
	archiver_threshold0_usage => {
		# Quirk to avoid code duplication when ramping down workers.
		# MUST NOT be changed.
		default => 0,
	},
	retriever_maxworkers => {
		default => 1,
		example => 3,
		desc => "Maximum number of concurrent dsmc retrievers.\nNote: Node must have MAXNUMMP increased from default 1.",
		canoverride => 1,
	},
	retriever_remountdelay => {
		# IBM tapes are specified to endure at least 20000 mounts
		# during the lifetime. That's 6 mounts per day for 10 years. 
		# Assuming the worst-case load peaks are not continuous
		# 12 mounts per day or 2 hours (7200 s) is a reasonable default.
		default => 7200,
		desc => "When in concurrent mode, don't remount tapes more often than this, seconds",
		reqposint => 1,
		canoverride => 1,
	},
	retriever_hintfile => {
		example => "/var/spool/endit/tapehints/EXAMPLENODE.json",
		desc => "Tape hints file for concurrent dsmc retrievers. Generate by\nperiodically running either tsmtapehints.pl (on node running ENDIT) or\ntsm_getvolumecontent.pl (requires TSM server credentials) for the\n-asnode user you configured in dsmcopts",
	},
	retriever_reqlistfillwait => {
		default => 600,
		desc => "Wait this long after last request before starting a worker for a volume, seconds",
		reqposint => 1,
		canoverride => 1,
	},
	retriever_reqlistfillwaitmax => {
		default => 7200,
		desc => "Force start a worker after this long even if the request list for this volume is still filling up, seconds",
		reqposint => 1,
		canoverride => 1,
	},
	retriever_buffersize => {
		default => 1000,
		desc => "Size (in GiB) on filesystem reserved for the ENDIT retriever to buffer files during retrieval before they are processed by dCache.",
		reqposint => 1,
		canoverride => 1,
	},
	retriever_backlogthreshold => {
		default => 10,
		desc => "Threshold (in %), when filesystem use of the ENDIT reserved buffersize goes above this don't start new workers",
		reqposint => 1,
		canoverride => 1,
	},
	retriever_killthreshold => {
		default => 95,
		desc => "Threshold (in %), when filesystem use of the ENDIT reserved buffersize goes above this running workers are killed to avoid filling the filesystem",
		reqposint => 1,
		canoverride => 1,
	},

	deleter_queueprocinterval => {
		default => "monthly",
		desc => 'Queue processing interval (hourly/daily/weekly/monthly or crontab style time definition ie "0 0 1 * *")',
	},

	verbose => {
		default => 1,
		desc => 'Enable verbose logging including processed files (1 to enable, 0 to disable)',
		canoverride => 1,
	},
	debug => {
		default => 0,
		desc => 'Enable debug mode/logging (1 to enable, 0 to disable)',
		canoverride => 1,
	},
);

# Sort function that orders component specific configuration directives
# after common ones.
sub confdirsort {
	return 1 if($a=~/_/ && $b!~/_/);
	return -1 if($b=~/_/ && $a!~/_/);

	return $a cmp $b;
}

# Check if a configuration item is valid.
# Returns non-empty string with error if invalid.
sub checkitem {
	my($item, $value, $override) = @_;

	if($confobsolete{$item}) {
		return "Config directive $item OBSOLETE";
	}
	elsif(!$confitems{$item}) {
		return "Config directive $item UNKNOWN";
	}
	elsif($override && !$confitems{$item}{canoverride}) {
		return "Cannot override config directive $item";
	}
	elsif($confitems{$item}{reqint} && $value!~/^\d+$/) {
		return "Config directive $item value $value must be an integer";
	}
	elsif($confitems{$item}{reqposint} && ($value!~/^\d+$/ || $value<1)) {
		return "Config directive $item value $value must be positive integer";
	}

	return ""; # Success
}

# Check if archiver thresholds are valid.
# Returns non-empty string with error if invalid.
sub checkarchthres {
	my($href) = @_;

	my $prev = 0;

	for my $i (1 .. 9) {
		my $k = "archiver_threshold${i}_usage";
		next unless(defined($href->{$k}));
		if($href->{$k} <= $prev) {
			return "$k $href->{$k} smaller than lower threshold $prev";
		}
	}

	return "";
}

sub logconfdiff {
	my($old, $new, $scope) = @_;
	my %done;

	foreach my $k (sort keys %{$old}) {
		$done{$k} = 1;
		if(defined($new->{$k})) {
			my $p = 0;
			if(!$scope || $k!~/_/) {
				$p = 1;
			}
			if($k=~/^${scope}_/) {
				$p = 1;
			}
			if($p && $old->{$k} ne $new->{$k}) {
				printlog "Conf: Changed: $k $old->{$k} -> $new->{$k}";
			}
		}
		else {
			printlog "Conf: Removed: $k $old->{$k}";
		}
	}

	foreach my $k (sort keys %{$new}) {
		next if($done{$k});
		printlog "Conf: Added: $k $new->{$k}";
	}
}

sub writesampleconf() {

	my($fh, $fn) = tempfile("endit.conf.sample.XXXXXX", UNLINK=>0, TMPDIR=>1);

	# File::Temp creates a file as private as possible. However, we want to
	# have permissions in accordance to the user chosen umask.
	chmod(0666 & ~umask(), $fn) || die "chmod $fn: $!";

	print $fh "# ENDIT daemons sample configuration file.\n";
	print $fh "# Generated on " . scalar(localtime(time()));
	my $ver = getgitversiontag();
	if($ver) {
		my $me = fileparse(__FILE__);
		print $fh " by $me version $ver";
	}
	print $fh "\n";
	print $fh "\n";
	print $fh "# Note, comments have to start with # in the first character of the line\n";
	print $fh "# Otherwise, simple \"key: value\" pairs\n";

	foreach my $k (sort confdirsort keys %confitems) {
		next unless($confitems{$k}{desc});

		print $fh "\n";
		my @desc = split(/\n/, $confitems{$k}{desc});
		print $fh "# ", join("\n# ", @desc), "\n";
		if(defined($confitems{$k}{default})) {
			print $fh "# (default $confitems{$k}{default})\n";
		}

		if(defined($confitems{$k}{example})) {
			print $fh "$k: $confitems{$k}{example}\n";
		}
		elsif(defined($confitems{$k}{default})) {
			print $fh "# $k: $confitems{$k}{default}\n";
		}
		else {
			print $fh "# $k:\n";
		}
	}

	close($fh) || warn "Closing $fn: $!";

	printlog "Sample configuration file written to $fn";
}

sub readconf() {
	my $conffile = dirname (__FILE__) .  '/endit.conf';

	# Apply defaults
	foreach my $k (keys %confitems) {
		next unless(defined($confitems{$k}{default}));

		$conforig{$k} = $confitems{$k}{default};
	}

	if($ENV{ENDIT_CONFIG}) {
		$conffile = $ENV{ENDIT_CONFIG};
	}

	printlog "Using configuration file $conffile";

	my $cf;
	if(!open $cf, '<', $conffile) {
		warn "Can't open $conffile: $!";
		writesampleconf();
		die "No configuration, exiting...";
	}
	while(<$cf>) {
		next if $_ =~ /^#/;
		chomp;
		next unless($_);
		next if(/^\s+$/);

		my($key,$val) = split /:\s+/;
		if(!defined($key) || !defined($val) || $key =~ /^\s/ || $key =~ /\s$/) {
			die "Aborting on garbage config line: '$_'";
			next;
		}

		if($confold2new{$key}) {
			warn "Config directive $key deprecated, please use $confold2new{$key} instead";
			$key = $confold2new{$key};
		}

		my $err = checkitem($key, $val);
		if($err) {
			warn "$err, skipping";
			next;
		}

		$conforig{$key} = $val;
	}

	# Verify that required parameters are defined
	foreach my $param (qw{dir logdir dsmcopts desc-short desc-long}) {
		if(!defined($conforig{$param})) {
			die "$conffile: $param is a required parameter, exiting";
		}
	}

	my $err = checkarchthres(\%conforig);
	if($err) {
		die "$err, exiting";
	}

	# Verify that dir is present
	if(! -d "$conforig{dir}") {
		die "Required directory $conforig{dir} missing, exiting";
	}
	# Verify that required subdirs are present and writable
	foreach my $subdir (qw{in out request requestlists trash queue}) {
		my $sd = "$conforig{dir}/$subdir";
		if(! -d $sd && !mkdir($sd)) {
			die "mkdir $sd failed: $!";
		}
		my($fh, $fn) = tempfile(".endit.XXXXXX", DIR=>"$sd"); # croak():s on error

		close($fh);
		unlink($fn);

		# Provide subdirs as variables
		$conforig{"dir_$subdir"} = $sd;
	}

	if(-d $conforig{currstatsdir}) {
		my($fh, $fn) = tempfile(".endit.XXXXXX", DIR=>$conforig{currstatsdir}); # croak():s on error
		close($fh);
		unlink($fn);
	}
	else {
		warn "currstatsdir $conforig{currstatsdir} missing";
	}

	# Expose this configuration
	%conf = %conforig;
}

my $lastoverrideload = 0;
sub readconfoverride {
	my($scope) = @_;
	my $j;

	if(! -f $conf{confoverridefile}) {
		if($lastoverrideload > 0) {
			logconfdiff(\%conf, \%conforig, $scope);
			# Return to original configuration
			%conf = %conforig;
			$lastoverrideload = 0;
		}
		return;
	}

	my $mtime = (stat(_))[9];
	if(!$mtime) {
		# Some kind of error, just return and hope next try succeeds.
		return;
	}

	if($lastoverrideload > $mtime) {
		# We've already loaded this file.
		return;
	}

	# Be forgiving if file is non-atomically created
	for(1..10) {
		$j = undef;
		eval {
			local $SIG{__WARN__} = sub {};
			local $SIG{__DIE__} = sub {};
			local $/; # slurp whole file
			open my $rf, '<', $conf{confoverridefile} or die "open: $!";
			my $json_text = <$rf>;
			close $rf;
			die "Zero-length string" if(length($json_text) == 0);
			$j = decode_json($json_text);
		};
		last if(!$@);
		usleep(100_000); # Pace ourselves
	}

	if(!$j) {
		if($@) {
			warn "Configuration override $conf{confoverridefile} exists but couldn't be loaded, last error was: $@";
		}
		return;
	}

	# Don't try loading this again.
	$lastoverrideload = time();

	my %confnew = %conforig;
	foreach my $k (sort keys %{$j}) {
		if(!$k || !$j->{$k}) {
			warn "Aborting override load: empty configuration override $k $j->{$k}";
			return;
		}
		my $err = checkitem($k, $j->{$k}, 1);
		if($err) {
			warn "Aborting override load: $err";
			return;
		}
		$confnew{$k} = $j->{$k};

		# Archiver thresholds needs some special handling, since
		# reducing the number of sessions requires deleting items that
		# might be defined in the regular config.
		if($k =~ /^archiver_threshold[2-9]_usage$/ && $j->{$k} == 0) {
			delete $confnew{$k};
		}
	}

	my $err = checkarchthres(\%confnew);
	if($err) {
		warn "Aborting override load: $err";
		return;
	}

	logconfdiff(\%conf, \%confnew, $scope);

	# Apply this configuration
	%conf = %confnew;
}

sub writejson {
	my($ref, $name) = @_;

	return undef unless(-d $conf{currstatsdir});

	my($fh, $fn) = tempfile("$name.XXXXXX", DIR=>$conf{currstatsdir});

	print $fh encode_json($ref),"\n";

        if(!close($fh)) {
		warn "Closing $fn: $!";
		unlink($fn);
		return undef;
	}

	chmod(0644, $fn); # Ensure world readable

	if(!rename($fn, "$conf{currstatsdir}/$name")) {
		warn "Rename $fn $conf{currstatsdir}/$name: $!";
		unlink($fn);
		return undef;
	}

	printlog "Wrote $conf{currstatsdir}/$name" if($conf{debug});

	return 1;
}

sub writeprom {
	my($ref, $name, $typehelp) = @_;

	return undef unless(-d $conf{currstatsdir});

	my($fh, $fn) = tempfile("$name.XXXXXX", DIR=>$conf{currstatsdir});

	foreach my $k (sort keys %{$ref}) {
		warn "writeprom(): No typehelp for metric $k" unless($typehelp->{$k});
		my $help = $typehelp->{$k}{help} // "endit_${k} metric";
		print $fh "# HELP endit_${k} ENDIT $logname $help.\n";
		my $type = $typehelp->{$k}{type} // "gauge";
		print $fh "# TYPE endit_${k} $type\n";
		my $l = "hsm=\"$conf{'desc-short'}\"";
		foreach my $lk (sort keys %{$typehelp->{$k}{labels}}) {
			$l .= ",$lk=\"$typehelp->{$k}{labels}{$lk}\"";
		}

		print $fh "endit_${k}{$l} $ref->{$k}\n";
	}

        if(!close($fh)) {
		warn "Closing $fn: $!";
		unlink($fn);
		return undef;
	}

	chmod(0644, $fn); # Ensure world readable

	if(!rename($fn, "$conf{currstatsdir}/$name")) {
		warn "Rename $fn $conf{currstatsdir}/$name: $!";
		unlink($fn);
		return undef;
	}

	printlog "Wrote $conf{currstatsdir}/$name" if($conf{debug});

	return 1;
}


# Tries to return a version tag to be used in metrics/logs, this is only
# supported when deployed via a git checkout.
sub getgitversiontag {
	my $mydir = dirname (__FILE__);

	my $ver;

	if(-d "$mydir/.git") {
		$ver = `git -C '$mydir' --git-dir='$mydir/.git' describe --tags --dirty`;
	}

	if($ver) {
		chomp $ver;
		return $ver;
	}
}

1;
