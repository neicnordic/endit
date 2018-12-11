# ENDIT - Efficient Northern Dcache Interface to TSM

## Concept

Use the same filesystem as an HSM staging area, using hardlinks to "store"
data and then use batch processes to archive and restore data to/from tape.

ENDIT is comprised of an ENDIT dCache plugin and the ENDIT daemons.

# Requirements

At least the following Perl modules need to be installed:

* JSON
* JSON::XS (highly recommended, approx 100 times faster parsing compared to pure-perl JSON)

It is known to work on Perl >= 5.14, but notably 5.10 does not work.

# Installation and Configuration

All dCache tape pools needs both the ENDIT dCache plugin and the ENDIT daemons
installed. 

More verbose instructions are available at
https://wiki.neic.no/wiki/DCache_TSM_interface


## TSM (IBM Spectrum Protect)

Setup TSM so that the user running dCache can "dsmc archive" and "dsmc
retrieve" files. If you want to have several pool-nodes talking to tape, you
probably want to setup a TSM proxy node that you can share accross machines
using dsmc -asnode=NODENAME.

A dCache hsminstance typically maps into a dedicated TSM proxy node. With a
proxy node you can have multiple read and write pool nodes to the same data in
TSM. Different TSM nodes need to have different hsminstances.

Note that you need to increase the node MAXNUMMP setting to the sum of
concurrent dsmc archive and retrieve sessions.

## dCache

The [ENDIT dCache plugin](https://github.com/neicnordic/dcache-endit-provider/)
needs to be installed on the pool.

To get good store performance the dCache pool must be tuned for continuous
flushing.

To get any efficiency in retrieves, you need to allow a large number of
concurrent restores and have a long timeout for them.

## ENDIT daemons

Download the ENDIT daemons to a directory of your choice, /opt/endit is our
suggestion.

Run one of the daemons (for example tsmretriever.pl) in order to generate
a sample configuration file. This is only done when no config file is found,
and is always written to a random file name shown in the output.

Review the sample configuration, tune it to your needs and copy it to the
location where ENDIT expects to find it (or use the ENDIT_CONFIG environment variable, see below).

After installing, you need to create the directories "in", "out", "request",
"requestlists" and "trash" in the same filesystem as the pool. ENDIT
daemons check the existence and permissions of needed directories on
startup.

After starting dcache you also need to start the three scripts:

* tsmarchiver.pl
* tsmretriever.pl
* tsmdeleter.pl

See [startup/README.md](startup/README.md) for details/examples.

To enable concurrent retrieves from multiple tapes you must use a tape hint
file, a file that provides info on which tape volume files are stored.

## Tape hint file

The tape hint file name to be loaded by tsmretriever.pl is set using the
retriever_hintfile specifier in the configuration file.

This file can be generated either from the TSM server side or from the
TSM client side using one of the provided scripts. Choose the one that
works best in your environment.

In general we recommend to run only one script per TSM proxy node name
and distribute the resulting file to all affected hosts running ENDIT.
Running multiple scripts works, but may put unnecessary strain on your
TSM server database.

Updating the file by running the script daily is recommended.

### tsm_getvolumecontent.pl

This method communicates with the TSM server. It has the following
requirements:

* The dsmadmc utility set up to communicate properly with the TSM
  server.
* A TSM server administrative user (no extra privilege needed).

Benefits:

* Volume names are the real tape volume names as used by TSM
* Tests have shown this method to be approximately a factor 2 faster
  than using tsmtapehints.pl

Drawbacks:

* More cumbersome to set up:
  * Requires dsmadmc
  * Requires close cooperation with TSM server admins due to admin user etc.
  * Requires TSM admin user password in a clear-text file

### tsmtapehints.pl

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
  tsm_getvolumecontents.pl

# Multiple instances

To run multiple instances, the ENDIT_CONFIG environment variable can be set
to use a different configuration file.

The configuration of the endit-provider dCache plugin is done through the
admin interface.

# Collaboration

It's all healthy perl, no icky surprises, I hope. Patches, suggestions, etc are
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
