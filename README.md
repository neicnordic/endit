# ENDIT - Efficient Northern Dcache Interface to TSM

## Concept

Use the same filesystem as an HSM staging area, using hardlinks to "store"
data and then use batch processes to archive and restore data to/from tape.

ENDIT is comprised of an ENDIT dCache plugin and the ENDIT daemons.

# Requirements

At least the following Perl module need to be installed:

* JSON

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
