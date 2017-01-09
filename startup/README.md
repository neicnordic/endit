# Example system startup scripts

These startup scripts can be used have ENDIT start on system boot.

## systemd

### Installation/activation

* cp systemd/endit* /etc/systemd/system/
* systemctl daemon-reload
* systemctl enable endit.target

### Starting

* systemctl start endit.target

### Status

* systemctl status "endit*"
* systemctl is-active endit.target

### Stopping

* systemctl stop "endit*"
