# ENDIT daemons - related utilities

This directory contains a set of utilties found useful in an environment
using the ENDIT daemons and related components.

# emulate-dsmc

This is a drop-in replacement of `dsmc` that just moves the files
to another location on the same host. Useful for small testing
deployments, CI/CD pipelines, etc - but probably not for anything
useful with real data.

This is simplistic and trusts input, if there is a line with something like
$HOME/.ssh/id_rsa in the -filelist, expect it to be read/written/removed.
Same goes for code execution. These side effects are unlikely if it is
just used from the ENDIT daemons though.

Note that it doesn't fully try to emulate the dsmc behavior and output,
just enough to trigger ENDIT daemon error handling.

# pertape.pl

This utility creates lists based on tape/volume content. It assumes that
you have access to the relevant tape hint file, see
[the main README](../README.md#tape-hint-file) for more information on
ENDIT tape hint files.

The default invocation generates commands to restore files from tapes
listed in the tape hint file, grouped by tape name in separate files.
This is intended to be used for restoring files on a source dCache pool
in order to transfer the files to a target dCache pool using a migration
job.

If only a source hint file is provided the output includes all files
listed in that hint file. Providing a destination hint file will exclude
all files found on the destination making it easy to continue a partial
operation.

Invoking:

    pertape.pl -s /tmp/sourcehints.json -d /tmp/desthints.json

would generate lists grouped by tape for all files found in
sourcehints.json, excluding any file already present in desthints.json.

Invoke pertape.pl with the `-h` flag to see all options. For example the
output directory can be changed with the `-D` flag, the `-b` option can
be used to limit the total size of files contained in each output file
and the `-O` flag outputs a single JSON file with only the data instead
of plain text files with commands grouped per tape.

## Command template and arguments

To make the pertape.pl tool more versatile the generated output can be
adapted by changing the *command template*.

The default command template is:

    \s %src% rh restore %id%

The arguments come in two categories, the ID:s that are file names/PNFS
ID:s as listed in the hint file and static arguments for source/target
that can be provided to pertape.pl with arguments.

| Argument | Origin | Effect |
| -------- | ----- | ------ |
| `%id%` | Hint file | Replaced with PNFS ID, one command per ID. |
| `%idlist%` | Hint file | Replaced with a comma separated list of PNFS ID:s, one command with all IDs |
| `%idqlist%` | Hint file | Replaced with a quoted comma separated list of PNFS ID:s, one command with all ID:s. Useful with SQL `IN(...)` constructs. |
| `%src%` | Argument | Replaced with the provided string passed using the `-S` flag, defaults to `sourcepool`. |
| `%target%` | Argument | Replaced with the provides string passed using the `-T` flag. |

The command template and arguments are most useful when combined with
the defaults file feature.

## Defaults file

To avoid tedious repetition and allow tailoring commands pertape.pl can
use defaults files. The `-j` flag specifies a defaults file to load and the `-w`
flag is used to write a defaults file. To make life even easier the
defaults file is an executable script using pertape.pl as interpreter.

As an example, when processing a tape pool named pool1 one might want to
run pertape.pl multiple times with lots of arguments. In order to create
a custom `pertape-pool1-split` script invoke:

    pertape.pl -s /tmp/sourcehints.json -d /tmp/desthints.json -S pool1 -b 50000000000 -w pertape-pool1-split

When invoking the newly created defaults file script by
`./pertape-pool1-split -h` one can see that the above options are
applied as defaults and `./pertape-pool1-split` can be invoked without
any additional arguments. Command line options override the defaults
file, for example directory can be changed using the `-D` flag.

## pertape-storageinfoof

This pertape.pl defaults script outputs commands to do `storageinfoof` on all
files/PNFS IDs.

The command template is:

    \s storageinfoof %id%

**NOTE:** The script assumes that the ENDIT daemons installation
directory is `/opt/endit/` - if you choose another location you need to
edit the script accordingly.

## pertape-cacheinfoof

This pertape.pl defaults script outputs commands to do `storageinfoof` on all
files/PNFS IDs.

The command template is:

    \s cacheinfoof %id%

**NOTE:** The script assumes that the ENDIT daemons installation
directory is `/opt/endit/` - if you choose another location you need to
edit the script accordingly.

## pertape-migcopy

This pertape.pl defaults script outputs commands to start migration jobs
for all files/PNFS IDs.

The command template is:

    \s %src% migration copy id=pertape -pnfsid=%idlist% -target=pool %target%"

You most likely want to use the `-S` and `-T` arguments to provide a
%src% and %target% argument that makes sense in your environment.

**NOTE:** The script assumes that the ENDIT daemons installation
directory is `/opt/endit/` - if you choose another location you need to
edit the script accordingly.

## pertape-idlist

This pertape.pl defaults script outputs the files/PNFS IDs, one per
line.

The command template is:

    %id%

**NOTE:** The script assumes that the ENDIT daemons installation
directory is `/opt/endit/` - if you choose another location you need to
edit the script accordingly.

## pertape-sqlloclist

This pertape.pl defaults script outputs SQL commands to list the
locations of all files/PNFS IDs.

The command template is:

    SELECT ti.ipnfsid,tl.ilocation FROM t_inodes ti,t_locationinfo tl WHERE ti.ipnfsid IN(%idqlist%) AND ti.inumber=tl.inumber;

**NOTE:** The script assumes that the ENDIT daemons installation
directory is `/opt/endit/` - if you choose another location you need to
edit the script accordingly.
