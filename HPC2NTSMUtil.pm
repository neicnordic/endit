#   HPC2NTSMUtil - simplify using dsmadmc to interact with TSM server
#   Copyright (C) 2012-2021 <Niklas.Edmundsson@hpc2n.umu.se>
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

package HPC2NTSMUtil;

# vim:ts=4:sw=4:et:

use warnings;
use strict;
use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);
use Carp;
use Time::Local;


require Exporter;
require AutoLoader;

@ISA = qw(Exporter AutoLoader);
# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.
@EXPORT = qw(
    setauth
    dsm_cmd
    timestamp_to_time
    actdt_to_time
    humanamount
    tsmamount2bytes
);
@EXPORT_OK = qw(
);
$VERSION = '0.03';

my $userid;
my $password;

# Run a TSM administrative command
# Argument: Command
# Returns: One of:
#          undef on failure
#          A single-item list with an empty string if no output returned
#          A list of returned output, each line with TAB-separated columns
sub dsm_cmd ($)
{
    my $cmd = shift;
    my $fh;
    my @result;
    my @diag;

    if(!$userid || !$password) {
        croak "setauth() not called before dsm_cmd";
        return undef;
    }
    
    if(!open($fh, "dsmadmc -tracefile=/dev/null -errorlogname=/dev/null -id=$userid -pa=$password -dataonly=yes -tabdelim \"$cmd\"|")) {
        croak "Failed 1 to invoke dsmadmc $cmd: $!";
        return undef;
    }

    while(<$fh>) {
        chomp;
        # dsmadmc just loves to spew out ANRNNNN status messages like:
        # ANR2034E SELECT: No match found using this criteria.
        # ANS8001I Return code 11.
        # ANS1043S Quotes are not matched
        # etc.
        if(/^AN\S\d\d\d\d\S\s/) {
            # Save to be able to print useful error if it is an error...
            push @diag, $_;
            next;
        }
        push @result, $_;
    }
    unless(close $fh) {
        if($? == -1) {
            croak "Failed to execute dsmadmc: $!";
            return undef;
        }
        elsif($? & 127) {
            my $str = sprintf "dsmadmc died with signal %d, %s coredump\n",
               ($? & 127),  ($? & 128) ? 'with' : 'without';
            carp $str;
            return undef;
        }
        else {
            my $ret = $? >> 8;
            if($ret != 11) {
                foreach(@diag) {
                    carp($_);
                }
                carp("dsmadmc exited with value $ret");
                return undef;
            }
        }
    }

    # Indicate an empty result by returning a list with an empty string.
    if(!(@result)) {
        push @result, "";
    }

    return @result;
}


# Set auth info
# Argument: Hash with id == userid, passfile == file with password
# Returns: servername, croaks on failure.
sub setauth {
    my %args = (@_);

    croak "No id" unless($args{id});

    if($args{passfile}) {
        open(my $fh, $args{passfile}) || croak "Unable to open $args{passfile}: $!";
        chomp($args{password} = <$fh>);
        close($fh);
    }
    croak "No password" unless($args{password});

    # Save data to our private variables
    $userid = $args{id};
    $password = $args{password};

    # Verify that the auth works, and that the server is alive.
    my @srvstat = dsm_cmd("query status");
    if(!@srvstat || !defined($srvstat[0])) {
        croak "Auth setup failed";
    }
    # Use select instead, makes this less kludgy?
    foreach(@srvstat) {
        next if(/^storage.*management/i);
        if(/^(\S+)/) {
            return $1;
        }
    }

    return "SETAUTH OK BUT FAILED TO GET SERVER NAME";
}


# Convert DB timestamps of the form 2004-08-13 15:44:41.000000 to unix time
sub timestamp_to_time($)
{
    my $s = shift;

    if($s =~ /(\d\d\d\d)-(\d\d)-(\d\d)\s+(\d\d):(\d\d):(\d\d)/) {
        return timelocal($6, $5, $4, $3, $2-1, $1);
    }

    return undef;
}


# Convert strings of the forms:
# 03/16/07   16:01:54
# 05/15/2010 12:46:00
# to unix time
sub actdt_to_time($)
{
    my $s = shift;

    if($s =~ m|(\d\d)/(\d\d)/\d{0,2}?(\d\d)\s+(\d\d):(\d\d):(\d\d)|) {
        return timelocal($6, $5, $4, $2, $1-1, $3+2000);
    }

    return undef;
}


# Print amounts in a nice way for humans (ie. 131072 -> 128kiB etc).
sub humanamount(@) {
    my $in = shift;
    my $suffix = shift;
    my $ret;
    my $addsuffix=1;

    if($suffix && $suffix=~/^-/) {
        $addsuffix=0;
        $suffix =~ s/^-//;
    }

    # Real units
    my $ki = 1024;
    my $mi = $ki*1024;
    my $gi = $mi*1024;
    my $ti = $gi*1024;
    my $pi = $ti*1024;
    my $ei = $pi*1024;
    my $zi = $ei*1024;
    my $yi = $zi*1024;

    # Storage-style units
    my $k = 1000;
    my $m = $k*1000;
    my $g = $m*1000;
    my $t = $g*1000;
    my $p = $t*1000;
    my $e = $p*1000;
    my $z = $e*1000;
    my $y = $z*1000;

    # Auto-scale unless locked to a specific unit.
    unless($suffix) {
        if($in > $yi) {
            $suffix = "YiB";
        }
        elsif($in > $zi) {
            $suffix = "ZiB";
        }
        elsif($in > $ei) {
            $suffix = "EiB";
        }
        elsif($in > $pi) {
            $suffix = "PiB";
        }
        elsif($in > $ti) {
            $suffix = "TiB";
        }
        elsif($in > $gi) {
            $suffix = "GiB";
        }
        elsif($in > $mi) {
            $suffix = "MiB";
        }
        elsif($in > $ki) {
            $suffix = "kiB";
        }
        else {
            $suffix = "  B";
        }
    }

    if($suffix eq "YiB") {
        $ret = $in/$yi;
    }
    elsif($suffix eq "ZiB") {
        $ret = $in/$zi;
    }
    elsif($suffix eq "EiB") {
        $ret = $in/$ei;
    }
    elsif($suffix eq "PiB") {
        $ret = $in/$pi;
    }
    elsif($suffix eq "TiB") {
        $ret = $in/$ti;
    }
    elsif($suffix eq "GiB") {
        $ret = $in/$gi;
    }
    elsif($suffix eq "MiB") {
        $ret = $in/$mi;
    }
    elsif($suffix eq "kiB") {
        $ret = $in/$ki;
    }
    elsif($suffix eq "YB") {
        $ret = $in/$y;
    }
    elsif($suffix eq "ZB") {
        $ret = $in/$z;
    }
    elsif($suffix eq "EB") {
        $ret = $in/$e;
    }
    elsif($suffix eq "PB") {
        $ret = $in/$p;
    }
    elsif($suffix eq "TB") {
        $ret = $in/$t;
    }
    elsif($suffix eq "GB") {
        $ret = $in/$g;
    }
    elsif($suffix eq "MB") {
        $ret = $in/$m;
    }
    elsif($suffix eq "kB") {
        $ret = $in/$k;
    }
    elsif($suffix =~ /\s+B$/) {
        $ret = $in;
        $suffix = "  B";
    }
    else {
        die "Internal error: Unknown suffix $suffix";
    }

    if($addsuffix) {
        $suffix=" $suffix";
    }
    else {
        $suffix="";
    }

    if($ret < 10 && $ret != 0) {
        $ret = sprintf("%.1f$suffix", $ret);
    }
    else {
        $ret = int($ret)."$suffix";
    }
    
    return $ret;
}


# Convert TSM human readable amount to bytes.
# Example 11.46 GB -> 12305081303
#         1,779.01 M -> 1865427189

sub tsmamount2bytes(@) {
    my $str = shift;

    $str =~ s/,//g;
    if($str =~ /([\d,\.]+)(\s+(\S*)|)/) {
        my $size = $1;
        my $prefix = $3;

        $size =~ s/,//g;
        $prefix = "" unless($prefix);

        if($prefix =~ /^K/i) {
            $size *= 1024;
        }
        elsif($prefix =~ /^M/i) {
            $size *= 1024*1024;
        }
        elsif($prefix =~ /^G/i) {
            $size *= 1024*1024*1024;
        }
        elsif($prefix =~ /^T/i) {
            $size *= 1024*1024*1024*1024;
        }
        elsif($prefix =~ /^P/i) {
            $size *= 1024*1024*1024*1024*1024;
        }
        $size = int($size);

        return $size;
    }

    return undef;
}
1;
