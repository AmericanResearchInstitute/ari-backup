from __future__ import with_statement
import yaml

# Reads and validates settings from /etc/ari-backup/ari-backup.conf.yaml and
# makes them available as properties of this module.  

# let's set some sane defaults
rdiff_backup_path = '/usr/bin/rdiff-backup'
remote_user = 'root'
ssh_path = '/usr/bin/ssh'
ssh_compression = False

with open('/etc/ari-backup/ari-backup.conf.yaml', 'r') as conf_file:
    conf = yaml.load(conf_file)

# Any supported settings should be included in this block, otherwise an
# exception will be thrown for an unrecognized setting.
for k, v in conf.iteritems():
    if k == 'backup_store_path': backup_store_path = v
    elif k == 'rdiff_backup_path': rdiff_backup_path = v
    elif k == 'remote_user': remote_user = v
    elif k == 'remote_snapshot_mount_root': remote_snapshot_mount_root = v
    elif k == 'ssh_compression': ssh_compression = v
    elif k == 'ssh_path': ssh_path = v
    else:
        raise Exception('{key}:{value} is not a recognized setting'.format(key=k, value=v))

# TODO How can we support a logging_level setting with Python logging using
# its own constants that are hard to reference in YAML?
