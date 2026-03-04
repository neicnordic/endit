#!/usr/bin/perl 

#   ENDIT - Efficient Northern Dcache Interface to TSM
#   Copyright (C) 2022-2026 Mattias Wadenstein <maswan@hpc2n.umu.se>
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
use JSON;

# Optinal: Chunk tape reads by this many bytes, usecase for rate limiting reads as to not
# run out of read tape pool buffer size. Should not be an even multiple of tape size, and
# this script support splitting into at most 100 chunks.
my $splitby = 55000000000000;
# Create commands to issue reads from this pool
my $sourcepool = "alice_hpc_ku_dk_r06";
# Create command files in this directory
my $listdir = "list";

my $tapelist;

die "usage pertape.pl sourcehints.json [desthints.json]\n" unless $ARGV[0];

if(open my $tf, '<', $ARGV[0]) {
  eval {
   local $SIG{__WARN__} = sub {};
   local $SIG{__DIE__} = sub {};

   $tapelist = decode_json(<$tf>);
  };
  if($@) {
   die "Parsing $ARGV[0] as JSON failed: $@";
  }
} else { die $!; }

my $done;

if($ARGV[1]){
  if(open my $tf, '<', $ARGV[1]) {
    eval {
     local $SIG{__WARN__} = sub {};
     local $SIG{__DIE__} = sub {};
  
     $done = decode_json(<$tf>);
    };
    if($@) {
     die "Parsing $ARGV[1] as JSON failed: $@";
    }
  } else { die $!; }
}

my %size;

foreach my $pnfsid (sort { $tapelist->{$a}{order} cmp $tapelist->{$b}{order} }keys %$tapelist) {
	next if defined $done->{$pnfsid}{volid};
	$size{$tapelist->{$pnfsid}{volid}}+=$tapelist->{$pnfsid}{size};
	open(my $fh, ">>", "$listdir/$tapelist->{$pnfsid}{volid}" . sprintf(".%02d",$size{$tapelist->{$pnfsid}{volid}}/$splitby));
	print $fh  "\\s $sourcepool rh restore $pnfsid\n";
	close($fh) or die "Close error: $!";
}
