# ENDIT - Efficient Northern Dcache Interface to TSM

ENDIT daemons

## Concept

Use the same filesystem as an HSM staging area, using hardlinks to "store"
data and then use batch processes to archive and restore data to/from tape.

ENDIT is comprised of an ENDIT dCache plugin and the ENDIT daemons.

The IBM Storage Protect (Spectrum Protect, TSM) client is used to perform
the actual transfer of files to/from the tape system.

# Requirements

The ENDIT daemons are known to work on Perl 5.10 onwards.

At least the following Perl modules need to be installed:

* JSON
  * `libjson-perl` (deb), `perl-JSON` (rpm)
* JSON::XS (approx 100 times faster parsing compared to pure-perl JSON)
  * `libjson-xs-perl` (deb), `perl-JSON-XS` (rpm)
* Schedule::Cron (optional, allows for crontab style specification of deletion queue processinginterval)
  * `libschedule-cron-perl` (deb), `perl-Schedule-Cron` (rpm)
* Filesys::Df
  * `libfilesys-df-perl` (deb), `perl-Filesys-Df` (rpm)

A recent version of the IBM Storage Protect (TSM) client is recommended, as of this writing
v8.1.11 or later, due to [commit 796a02a](https://github.com/neicnordic/endit/commit/796a02a8996f0bc7934721c053f43e0543affedc)
and [IBM APAR IT33143](https://www.ibm.com/support/pages/apar/IT33143).

# Installation and Configuration

All dCache tape pools needs both the ENDIT dCache plugin and the ENDIT daemons
installed. 

If needed, more verbose instructions are available in the NeIC wiki at
https://wiki.neic.no/wiki/DCache_TSM_interface

## TSM (IBM Storage Protect)

Setup TSM so that the user running dCache can `dsmc archive` and `dsmc
retrieve` files. If you want to have several pool-nodes talking to tape, we
recommend setting up a TSM proxy node that you can share across machines
using `dsmc -asnode=NODENAME`. Due to recent changes in TSM client authentication
we strongly recommend not using a machine-global TSM node, but instead creating
a dedicated TSM node for each dCache runtime user. See
the [IBM documentation re non-root usage](https://www.ibm.com/docs/en/spectrum-protect/8.1.15?topic=cspc-enable-non-root-users-manage-their-own-data)
for the recommended setup.

A dCache hsminstance typically maps into a dedicated TSM proxy node. With a
proxy node you can have multiple read and write pool nodes to the same data in
TSM. Different TSM nodes need to have different hsminstances.

The common ENDIT-optimized TSM storage hierarchy setup is to have a dedicated domain for each proxy node
and define a tape storage pool as the archive copygroup destination. Since `tsmarchiver.pl`
batches archive operations into larger chunks there is limited benefit of
spooling data to disk on the TSM server before moving it to tape.

For each TSM node defined on your TSM server, ensure that the following options are
correct for your environment:
* `MAXNUMMP` - Increase to the sum of concurrent/parallel `dsmc archive` and
`dsmc retrieve` sessions plus some margin to avoid errors when tapes are concurrently
being mounted/dismounted.
* `SPLITLARGEObjects` - set to No to optimize for tape.

On your TSM client machine (ie. dCache pool machine), ensure that you have set the appropriate tuning options for
optimizing performance in a tape environment, see the [IBM documentation on Using high performance tape drives](https://www.ibm.com/docs/en/spectrum-protect/8.1.15?topic=tuning-high-performance-tape-drives) for further details.
It is also recommended to define the `out` directory as a separate file system in TSM using the `VirtualMountPoint`
configuration option.

Typical `dsm.sys` excerpt:
```
TXNBYTELIMIT      10G
VIRTUALMountpoint /grid/pool/out
```

We also recommend disabling ACL support as this is file system specific
(as in you can't restore files to a different file system type) thus having
it enabled makes it hard to change the setup in the future.

Typical `dsm.opt` excerpt:
```
SKIPACL YES
```

If the machine is running scheduled TSM backups you want to exclude the pool filesystem(s) from the backup.

Typical system `include-exclude` file excerpt:
```
exclude.dir     /grid/pool*/.../*
exclude.fs      /grid/pool*/.../*
```

## dCache

The [ENDIT dCache plugin](https://github.com/neicnordic/dcache-endit-provider/)
needs to be installed on the pool.

To get good store performance the dCache pool must be tuned for continuous
flushing.

To get any efficiency in retrieves, you need to allow a large number of
concurrent restores and have a long timeout for them.

Note that since ENDIT v2 a late allocation scheme is used in order to
expose all pending read requests to the pools. This minimizes tape
remounts and thus optimizes access. For new installations, and when
upgrading from ENDIT v1 to v2, note that:

- The dCache pool size needs to be set lower than the actual file space
  size, 1 TiB lower if the default `retriever_buffersize` is used.
- You need to allow a really large amount of concurrent restores and
  thus might need an even larger restore timeout. ENDIT has been verified with
  1 million requests on a single tape pool with modest hardware, central
  dCache resources on your site might well limit this number.

The configuration of the ENDIT dCache plugin is done through the dCache
admin interface.

## ENDIT daemons

Download the ENDIT daemons to a directory of your choice, `/opt/endit` is our
suggestion. To make future upgrades easier we recommend to clone directly from the
GitHub repository.

Execute one of the daemons (for example `tsmretriever.pl`) once in order to generate
a sample configuration file. When no configuration is found the ENDIT daemons will
generate a sample file and write it to a random file name shown in the output, and
then exit.

Review the sample configuration, tune it to your needs and copy it to the
location where ENDIT expects to find it (or use the `ENDIT_CONFIG`
environment variable, see below). The following items needs special
attention:

- `dir` - The pool base directory.
- `desc-short` - Strongly recommended to set to match the dCache
  `pool.name`.

Starting from a generated sample configuration is highly recommended as it is the main
documentation for the ENDIT daemon configuration file, and also contains an example on
how to enable multiple session support for archiving and retrieving files. The
multiple session archive support in `tsmarchiver.pl` adapts to the backlog, ie
how much data needs to be stored to TSM, according to your configuration choices.
The multiple session retrieve support in `tsmretriever.pl` requires a tape hint file,
see below, that enables running multiple sessions each accessing a single tape.

On startup, the ENDIT daemons will check/create needed subdirectories in
the base directory, as specified by the `dir` configuration directive in
`endit.conf`.

After starting dcache you also need to start the three scripts:

* `tsmarchiver.pl`
* `tsmretriever.pl`
* `tsmdeleter.pl`

See [startup/README.md](startup/README.md) for details/examples.

By default the ENDIT daemons creates files with statistics in the
`/run/endit` directory, `tmpfiles.d` can be used to create the directory
on boot, here is an example `/etc/tmpfiles.d/endit.conf` snippet:

```
d /run/endit 0755 dcache dcache
```
Note that it's by design to have the directory and the statistics files
world-readable, they contain no secrets and usually needs to be accessed
by other processes such as the Prometheus `node_exporter`.

To enable concurrent retrieves from multiple tapes you must use a tape hint
file, a file that provides info on which tape volume files are stored.

## Tape hint file

The tape hint file name to be loaded by `tsmretriever.pl` is set using the
`retriever_hintfile` specifier in the configuration file.

This file can be generated either from the TSM server side or from the
TSM client side using one of the provided scripts. Choose the one that
works best in your environment.

In general we recommend to run only one script per TSM proxy node name
and distribute the resulting file to all affected hosts running ENDIT.
Running multiple scripts works, but may put unnecessary strain on your
TSM server database.

Updating the file by running the script daily is recommended.

### `tsm_getvolumecontent.pl`

This method communicates with the TSM server. It has the following
requirements:

* The `dsmadmc` utility set up to communicate properly with the TSM
  server.
* A TSM server administrative user (no extra privilege needed).

Benefits:

* Volume names are the real tape volume names as used by TSM
* Tests have shown this method to be approximately a factor 2 faster
  than using `tsmtapehints.pl`

Drawbacks:

* More cumbersome to set up:
  * Requires `dsmadmc`
  * Requires close cooperation with TSM server admins due to admin user etc.
  * Requires TSM admin user password in a clear-text file

### `tsmtapehints.pl`

This method runs together with the other ENDIT daemons and uses the dsmc
command as specified by the ENDIT configuration file to list file
information.

Benefits:

* Easier to set up:
  * Uses the ENDIT configuration file
  * Only needs periodic invocation (crontab, systemd timer)
* Performs some sanity checking, in particular detection of duplicates
  of archived files (multiple tape copies of the same file object)

Drawbacks:

* Volume names are numeric IDs, good enough to group files correctly but
  not easily usable by a TSM admin to identify a specific tape volume in case
  of issues.
* Slower, tests have shown a factor of 2 slowdown compared to
  `tsm_getvolumecontents.pl`

# Multiple instances

To run multiple instances for different tape pools on one host, the `ENDIT_CONFIG` environment variable can be set
to use a different configuration file. This is not to be confused with enabling parallel/multiple archive and
retrieve operations for one pool which is done using options in the ENDIT daemon configuration file.

# Bypassing delays/threshold/timers when testing

The ENDIT daemons are designed to avoid unnecessary tape mounts, and achieves
this by employing various thresholds and timers as explained in the example
configuration file.

However, when doing functional tests or error recovery related to the tape
system it can be really frustrating having to wait longer than
necessary. For these situations it's suitable to use the `USR1` signal
handling in the ENDIT daemons. In general, the `USR1` signal tells the
daemons to disregard all timers and thresholds and perform any pending
actions immediately.

# Temporary configuration overrides

It's possible to (temporarily) override select configuration items using
a separate JSON configuration file.

This makes it possible for sites to automate some load balancing tasks,
for example implementing backoff mechanisms for sites where lots of reads
queued results in starving writes.

Since this is focused on on-the-fly automatic solutions, the
configuration override file is a JSON file to make it easy to create it
using whatever tool that's suitable for the job. It is assumed that the
main endit.conf configuration file is under the control of some
configuration management tool such as Puppet, Ansible, etc; and thus not
suitable for on-the-fly manipulation.

The default file location chosen is `/run/endit/conf-override.json` with
the motivation that overrides are temporary.

# Statistics

The ENDIT daemons generate statistics in JSON and Prometheus
`node_exporter` formatted files, by default in the `/run/endit`
directory. The current implementation dumps the ENDIT internals
unprocessed, sizes are generally GiB denoted by `_gib` in the metric
name. The best documentation for now are the ENDIT daemon scripts,
[UTSL](https://en.wiktionary.org/wiki/UTSL) :-)

It is strongly recommended to set `desc-short` in `endit.conf` to match
the dCache `pool.name` since this is used to tag metrics with supposedly
unique `hsm` tags in order to be able to differentiate metrics on hosts
running multiple pools.

When using `node_exporter`, the suggested implementation is to simply
symlink the ENDIT `.prom` into your `node_exporter` directory.

# Migration and/or decommission

When migrating ENDIT service to another host (typically when renewing
hardware), ensure that pending operations have finished before shutting
down ENDIT and the dCache pool.

* Check the `trash/` and `trash/queue/` directories, they should both
  contain no files.
  * If the `trash/` directory has files in it, the dCache pool is getting
    deletion requests. Take actions to prevent this. tsmdeleter will
    queue the deletion requests on the next iteration cycle (default
    every minute).
  * If the `trash/queue/` directory has files in it, there are queued
    deletion requests. Either wait until the queue is processed (default
    once per month) or force queue processing by sending a `USR1` signal
    to the `tsmdeleter.pl` process. Review the `tsmdeleter.log` for
    progress and double-check the `trash/queue/` directory afterwards.
* Check the `out/` directory, it should not contain any files.
  * If the `out/` directory has files in it, data is being staged to the
    dCache  pool. Take actions to prevent this. Either wait until
    tsmarchiver processes the staging queue (default up to 6 hours) or
    force staging by sending a `USR1` signal to the `tsmarchiver.pl`
    process. Review the `tsmarchiver.log` for progress and double-check
    the `out/` directory afterwards.

# Collaboration

It's all healthy perl, no icky surprises, we hope. Patches, suggestions, etc are
most welcome.

## License

GPL-3.0, see [LICENSE](LICENSE)

## Versioning

[Semantic Versioning 2.0.0](https://semver.org/)

# Contributors

This project existed for approximately 10 years before it was added to GitHub,
with contributions from:

* Mattias Wadenstein <maswan@hpc2n.umu.se>
* Lars Viklund <lars@hpc2n.umu.se>
* Niklas Edmundsson <nikke@hpc2n.umu.se>
