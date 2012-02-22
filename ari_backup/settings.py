import yaml

from __future__ import with_statement
from logger import Logger

# Reads and validates settings from /etc/ari-backup/ari-backup.conf.yaml and
# makes them available as properties of this module.  

# let's set some sane defaults
rdiff_backup_path = '/usr/bin/rdiff-backup'
remote_user = 'root'
ssh_path = '/usr/bin/ssh'
ssh_compression = False

# setup logging
log = Logger('ARIBackup (settings)')

with open('/etc/ari-backup/ari-backup.conf.yaml', 'r') as conf_file:
    conf = yaml.load(conf_file)

# Any supported settings should be included in this block, otherwise an
# exception will be thrown for an unrecognized setting.
# TODO We need a way for extensions to ARIBackup to register settings
for k, v in conf.iteritems():
    if k == 'backup_store_path': backup_store_path = v
    elif k == 'rdiff_backup_path': rdiff_backup_path = v
    elif k == 'remote_user': remote_user = v
    elif k == 'snapshot_mount_root': snapshot_mount_root = v
    elif k == 'ssh_compression': ssh_compression = v
    elif k == 'ssh_path': ssh_path = v
    elif k == 'rsync_path': rsync_path = v
    elif k == 'rsync_options': rsync_options = v
    else:
        log.warning('{key}:{value} is not a recognized setting'.format(key=k, value=v))

# TODO How can we support a logging_level setting with Python logging using
# its own constants that are hard to reference in YAML?
