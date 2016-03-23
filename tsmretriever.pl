#!/usr/bin/perl

use warnings;
use strict;

use IPC::Run3;
use POSIX qw( WNOHANG );
use JSON;
use Data::Dumper;

use lib '/opt/endit/';
use Endit qw(%conf readconf printlog getusage);

$Endit::logsuffix = 'tsmretriever.log';

readconf('/opt/endit/endit.conf');
die "No basedir!\n" unless $conf{'dir'};
my $dir = $conf{'dir'};
my $listfilecounter = 0;

sub namelistfile() {
	++$listfilecounter;
	return "$dir/requestlist.$listfilecounter";
}


sub checkrequest($) {
	my $req = shift;
	my $req_filename = $conf{'dir'} . '/request/' . $req;
	my $parent_pid;
	{
		local $/; # slurp whole file
		open my $rf, '<', $req_filename;
		my $json_text = <$rf>;
		my $state = decode_json($json_text);
		if (defined $state && exists $state->{parent_pid}) {
			$parent_pid = $state->{parent_pid};
		}
	}
	if($parent_pid && getpgrp($parent_pid) > 0) {
		return { parent_pid => $parent_pid };
	} else {
		printlog "Broken request file $req_filename\n";
		unlink $req_filename;
		return undef;
	}
}

sub processing_file($$) {
	my ($worker,$file) = @_;
	if($worker) {
		return exists $worker->{files}->{$file};
	} else {
		return 0;
	}
}

my $tapelistmodtime=0;
my $tapelist = {};
my %reqset;
my @workers;

# Warning: Infinite loop. Program may not stop.
while(1) {
#	sleep to let requester remove requests and pace ourselves
	sleep 6;

#	check if any dsmc workers are done
	if($#workers>0) {
		@workers = map {
			my $w = $_;

			my $wres = waitpid($w->{pid}, WNOHANG);
			my $rc = $?;
			if ($wres == $w->{pid}) {
#				Child is done
				$w->{pid} = undef;
#				TODO(zao): What happens on success/failure?
				if ($? == 0) {
				}
				unlink $w->{listfile};
			} 
			$w;
		} @workers;
		@workers = grep { $_->{pid} } @workers;
	}

#	refresh tape list
	if (exists $conf{tapefile}) {
		my $tapefilename = $conf{tapefile};
		my $newtapemodtime = (stat $tapefilename)[9];
		if ($newtapemodtime > $tapelistmodtime) {
			my $newtapelist = Endit::readtapelist($tapefilename);
			if ($newtapelist) {
				$tapelist = $newtapelist;
				$tapelistmodtime = $newtapemodtime;
			}
		}
	}

#	read current requests
	{
		%reqset=();
		opendir(REQUEST,$dir . '/request/');
		my (@requests) = grep { /^[0-9A-Fa-f]+$/ } readdir(REQUEST); # omit entries with extensions
		closedir(REQUEST);
		if (@requests) {
			foreach my $req (@requests) {
				my $reqinfo = checkrequest($req);
				if ($reqinfo) {
					if (!exists $reqinfo->{tape}) {
						if (my $tape = $tapelist->{$req}) {
							$reqinfo->{tape} = $tape;
						} else {
							$reqinfo->{tape} = 'default';
						}
					}
					$reqset{$req} = $reqinfo;
				}
			}
		}
	}

print Dumper(\%reqset);

#	if any requests and free worker
	if (%reqset && $#workers < $conf{'maxretrievers'}) {
#		make list blacklisting pending tapes
		my %usedtapes;
		my $job = {};
		if($#workers >0) {
			%usedtapes = map { $_->{tape} => 1 } @workers;
		}
print Dumper(\%usedtapes);
		foreach my $name (keys %reqset) {
			my $req = $reqset{$name};
			my $tape;
			if (exists $req->{tape}) {
				$tape = $req->{tape};
			} else {
				printlog "Warning: tape should have been set for $name, but setting it again!\n";
				$tape = 'default';
			}
			$job->{$tape}->{$name} = $req;
		}

#		start jobs on tapes not already taken up until maxretrievers
		foreach my $tape (keys $job) {
			last if $#workers >= $conf{'maxretrievers'}-1;
			next if exists $usedtapes{$tape};
			my $listfile = "$dir/requestlists/$tape";
			open my $lf, ">", $listfile or die "Can't open listfile: $!";
			foreach my $name (keys $job->{$tape}) {
				print $lf "$dir/out/$name\n";
			}
			close $lf;

#			spawn worker
			my $pid;
			my $j;
			if ($pid = fork) {
				$j=$job->{$tape};
				$j->{pid} = $pid;
				$j->{listfile} = $listfile;
				$j->{tape} = $tape;
				push @workers, $j;
			}
			else {
				my $indir = $dir . '/in/';
				my @dsmcopts = split /, /, $conf{'dsmcopts'};
				my @cmd = ('dsmc','retrieve','-replace=no','-followsymbolic=yes',@dsmcopts, "-filelist=$listfile",$indir);
				# my @cmd = ('md5sum', "$listfile");
				my ($in,$out,$err);
				open my $lf, "<", $listfile;
				my (@requests) = <$lf>;
				close $lf;
				$in="A\n";
				if((run3 \@cmd, \$in, \$out, \$err) && $? == 0) {
					# files migrated from tape without issue
					exit 0;
				} elsif($#requests < 10) {
					# something went wrong. figure out what files didn't make it.
					# wait for the hsm script to remove successful requests
					sleep 60;
					open my $lf, "<", $listfile;
					my (@requests) = <$lf>;
					close $lf;
					my $outfile;
					my $returncode = 0;
					foreach $outfile (@requests) {
						my (@l) = split /\//,$outfile;
						my $filename = $l[$#l];
						chomp $filename;
						my $req = $dir . '/request/' . $filename;
						if( -e $req ) {
							my @cmd = ('dsmc','retrieve','-replace=no','-followsymbolic=yes',@dsmcopts,$outfile,$indir);
							my ($in, $out,$err);
							$in="A\n";
							if((run3 \@cmd, \$in, \$out, \$err) && $? == 0) {
								# Went fine this time. Strange..
							} else {
								$returncode = $? >> 8;
								printlog localtime() . ": warning, dsmc returned $returncode on $outfile:\n";
								printlog $err;
								printlog $out;
								open EF, ">", $req . '.err' or warn "Could not open $req.err file: $!\n";
								print EF "32\n";
								close EF;
							}
						}
					}
#					Last returncode of the failed single-file runs.
					exit $returncode;
				} else {
			                printlog "dsmc retrieve done unsuccessfully at " . localtime() . "\n";
                			# Large number of requests broke, try again later
                			exit 1;
				}
			}
		}
	}
}
