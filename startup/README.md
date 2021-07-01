# Example system startup scripts

These startup scripts can be used have ENDIT start on system boot.

## systemd

### Installation/activation

* cp systemd/endit* /etc/systemd/system/
* Review /etc/systemd/system/endit-*.service:
  * Uncomment and set ENDIT_CONFIG to appropriate value if needed (default is /opt/endit/endit.conf)
  * Uncomment and set DSM_CONFIG to the per-user dsm.opt file
  * The above changes can of course also be managed as drop-in files in the matching /etc/systemd/system/endit-*.service.d/ directories if you want to separate upstream version files from your local modifications.
* systemctl daemon-reload
* systemctl enable endit.target

### Starting

* systemctl start endit.target

### Status

* systemctl status "endit*"
* systemctl is-active endit.target

### Stopping

* systemctl stop "endit*"
