# ENDIT - Efficient Northern Dcache Interface to TSM

## Concept

Use the same filesystem as an HSM staging area, using hardlinks to "store"
data and then use batch processes to archive and restore data to/from tape.

# Requirements

At least the following Perl modules need to be installed:

* JSON, IPC::Run3

It is known to work on Perl >= 5.14, but notably 5.10 does not work

# Configuration

Both the dcache pool (with endit HSM provider) and the perl daemons need
to agree on a hsminstance, that typically maps into a TSM proxy node. With
a proxy node you can have multiple read and write pool nodes to the same
data in TSM. Different TSM nodes need to have different hsminstances.

To get any efficiency in retrieves, you need to allow a large number of
concurrent restores and have a long timeout for them.

It is also recommended to tune "minusage" and "timeout" to your needs, this
is the threshhold definitions for when to flush to tape.

After installing, you need to create the directories "in", "out", "request",
"requestlists" and "trash" in the same filesystem as the pool.

After starting dcache you also need to start the three scripts:

* tsmarchiver.pl
* tsmretriever.pl
* tsmdeleter.pl

See [startup/] for details/examples.

# Multiple instances

To run multiple instances, the ENDIT_CONFIG environment variable can be set
to use a different configuration file.

The configuration of the endit-provider dCache plugin is done through the
admin interface.

# Collaboration

It's all healthy perl, no icky surprises, I hope. Patches, suggestions, etc are
most welcome.

License: GPL

# Contributors

This project existed for approximately 10 years before it was added to GitHub,
with contributions from:

* Mattias Wadenstein <maswan@hpc2n.umu.se>
* Lars Viklund <lars@hpc2n.umu.se>
* Niklas Edmundsson <nikke@hpc2n.umu.se>
