#!/usr/bin/perl

#   ENDIT - Efficient Northern Dcache Interface to TSM
#   Copyright (C) 2006-2017 Mattias Wadenstein <maswan@hpc2n.umu.se>
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

use warnings;
use strict;

use IPC::Run3;
use POSIX qw( WNOHANG );
use JSON;
use Data::Dumper;

use lib '/opt/endit/';
use Endit qw(%conf readconf printlog getusage);

$Endit::logsuffix = 'tsmretriever.log';

readconf();

my $dir = $conf{'dir'};
my $listfilecounter = 0;

# Try to send warn/die messages to log file
INIT {
        $SIG{__DIE__}=sub {
                printlog("DIE: $_[0]");
        };

        $SIG{__WARN__}=sub {
                print STDERR "$_[0]";
                printlog("WARN: $_[0]");
        };
}

$SIG{INT} = sub { printlog("Got SIGINT, exiting..."); exit; };
$SIG{QUIT} = sub { printlog("Got SIGQUIT, exiting..."); exit; };
$SIG{TERM} = sub { printlog("Got SIGTERM, exiting..."); exit; };

sub checkrequest($) {
	my $req = shift;
	my $req_filename = $conf{'dir'} . '/request/' . $req;
	my $parent_pid;
	{
		local $/; # slurp whole file
#		If open failed, probably the request was finished or cancelled
		open my $rf, '<', $req_filename or return undef;
		my $json_text = <$rf>;
		my $state = decode_json($json_text);
		if (defined $state && exists $state->{parent_pid}) {
			$parent_pid = $state->{parent_pid};
		}
		my $in_filename = $conf{'dir'} . '/in/' . $req;
		my $in_filesize=(stat $in_filename)[7];
		if(defined $in_filesize && $in_filesize == $state->{file_size}) {
			printlog "Not doing $req due to file of correct size already in in\n" if $conf{'verbose'};
			return undef;
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
my %lastmount;
my @workers;

printlog("$0: Starting...");

# Warning: Infinite loop. Program may not stop.
while(1) {
#	sleep to let requester remove requests and pace ourselves
	sleep $conf{sleeptime};

#	check if any dsmc workers are done
	if(@workers) {
		@workers = map {
			my $w = $_;
			my $wres = waitpid($w->{pid}, WNOHANG);
			my $rc = $?;
			if ($wres == $w->{pid}) {
#				Child is done
				$w->{pid} = undef;
#				Intentionally not caring about results. We'll retry and if stuff is really broken, the admins will notice from hanging restore requests anywau
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
		if(defined $newtapemodtime) {
			if ($newtapemodtime > $tapelistmodtime) {
				my $newtapelist = Endit::readtapelist($tapefilename);
				if ($newtapelist) {
					$tapelist = $newtapelist;
					$tapelistmodtime = $newtapemodtime;
				}
			} 
		} else {
			printlog "Warning: tapefile set to $conf{tapefile} in endit.conf, but this file does not seem to exist\n";
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
#				It'd be nice to do this here, but takes way too long with a large request list. Instead we only check it when making the requestlist per tape.
#				my $reqinfo = checkrequest($req);
				my $reqfilename=$dir . '/request/' . $req;
				my $ts =(stat $reqfilename)[9];
				my $reqinfo = {timestamp => $ts } if defined $ts;
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

#	if any requests and free worker
	if (%reqset && $#workers < $conf{'maxretrievers'}-1) {
#		make list blacklisting pending tapes
		my %usedtapes;
		my $job = {};
		if(@workers) {
			%usedtapes = map { $_->{tape} => 1 } @workers;
		}
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
			if(defined $job->{$tape}->{timestamp}) {
				if($job->{$tape}->{timestamp} > $req->{timestamp}){
					$job->{$tape}->{timestamp} = $req->{timestamp}
				}
			} else {
				$job->{$tape}->{timestamp}=$req->{timestamp};
			}
		}

#		start jobs on tapes not already taken up until maxretrievers
		foreach my $tape (sort { $job->{$a}->{timestamp} <=> $job->{$b}->{timestamp} } keys %{$job}) {
			last if $#workers >= $conf{'maxretrievers'}-1;
print "oldest job on tape $tape: $job->{$tape}->{timestamp}\n";
			next if exists $usedtapes{$tape};
			next if $tape ne 'default' and defined $lastmount{$tape} && $lastmount{$tape} > time - $conf{remounttime};
			my $listfile = "$dir/requestlists/$tape";
			open my $lf, ">", $listfile or die "Can't open $listfile: $!";
			foreach my $name (keys %{$job->{$tape}}) {
				next unless checkrequest($name);
				print $lf "$dir/out/$name\n";
			}
			close $lf or die "Closing $listfile failed: $!";

			if(-z $listfile) {
				unlink $listfile;
				next;
			}
			$lastmount{$tape} = time;
print "running worker on $tape\n";

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
				undef %usedtapes;
				undef %reqset;
				undef $tapelist;
				undef $job;
				@workers=();

				printlog "Trying to retrieve files from tape $tape";

				my $indir = $dir . '/in/';
				my @dsmcopts = split /, /, $conf{'dsmcopts'};
				my @cmd = ('dsmc','retrieve','-replace=no','-followsymbolic=yes',@dsmcopts, "-filelist=$listfile",$indir);
				my ($in,$out,$err);
				$in="A\n";
				if((run3 \@cmd, \$in, \$out, \$err) && $? == 0) {
					# files migrated from tape without issue
					printlog "Successfully retrieved files from tape $tape";
					exit 0;
				} else {
					my $msg = "dsmc retrieve failure: ";
					if ($? == -1) {
						$msg .= "failed to execute: $!";
					}
					elsif ($? & 127) {
						$msg .= sprintf "child died with signal %d, %s coredump", ($? & 127),  ($? & 128) ? 'with' : 'without';
					}
					else {
						$msg .= sprintf "child exited with value %d\n", $? >> 8;
					}
					printlog "$msg";
					printlog "STDERR: $err";
					printlog "STDOUT: $out";

                			# Any number of requests broke, try again later
                			exit 1;
				}
			}
		}
	}
}
