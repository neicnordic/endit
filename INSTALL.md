# Installation

More verbose instructions are available at
https://wiki.neic.no/wiki/DCache_TSM_interface

## Quick version

*  Setup TSM so that the user running dcache can "dsmc archive" and "dsmc
   retrieve" files. If you want to have several pool-nodes talking to tape,
   you probably want to setup a TSM node that you can share accross machines.
*  Install dependencies, IPC::Run3 and JSON, requires perl >= 5.14
*  Unpack the tarball and put the perl script in an appropriate place,
   in this text file it is assumed they go into $DCACHE_LOCATION/jobs/
*  Create the filesystem for the pool ($fs in this file)
*  In the filesystem create directory the directories "pool", "in", "out",
   "request", "requestlists".
*  For the dCache pools, the "normal" size of almost the whole fs is appropriate.
   Define a dCache pool with this size on $fs/pool/.
*  Adjust the paths and hsminstance etc in the congfig file to match the install.
*  Start tsmarchiver.pl, tsmretriever.pl, and tsmdeleter.pl in the background.
*  Install the dCache endit provider-
*  Register the endit provider as an osm tape interface in dcache.