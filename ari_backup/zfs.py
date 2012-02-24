from datetime import datetime, timedelta

from ari_backup import LVMBackup, settings

class ZFSLVMBackup(LVMBackup):
    def __init__(self, label, source_hostname, rsync_dst, zfs_hostname, dataset_name, snapshot_expiration_days):
        # assign instance vars specific to this class
        self.rsync_dst = rsync_dst
        self.zfs_hostname = zfs_hostname
        self.dataset_name = dataset_name

        # bring in some overridable settings
        self.rsync_options = settings.rsync_options
        self.snapshot_prefix = settings.zfs_snapshot_prefix

        # the timestamp format we're going to use when naming our snapshots
        self.snapshot_timestamp_format = '%Y-%m-%d--%H-%M'

        # call our super class's constructor to enable LVM snapshot management
        super(ZFSLVMBackup, self).__init__(label, source_hostname, None)

        self.post_job_hook_list.append((self._create_zfs_snapshot, {}))
        self.post_job_hook_list.append(
            (self._remove_zfs_snapshots_older_than, {'days': snapshot_expiration_days})
        )


    def _run_backup(self):
        # TODO Throw an exception if we see things in the include or exclude
        # lists since we don't use them in this class?

        self.logger.debug('ZFSLVMBackup._run_backup started')
        # We add a trailing slash to the src path otherwise rsync will make a
        # subdirectory at the destination, even if the destination is already
        # a directory.
        if self.source_hostname == 'localhost':
            rsync_src = self.snapshot_mount_point_base_path + '/'
        else:
            rsync_src = '{remote_user}@{source_hostname}::{snapshot_mount_point_base_path}/'.format(
                remote_user = self.remote_user,
                source_hostname = self.source_hostname,
                snapshot_mount_point_base_path = self.snapshot_mount_point_base_path
            )

        command = '{rsync_path} {rsync_options} {src} {dst}'.format(
            rsync_path = settings.rsync_path,
            rsync_options = self.rsync_options,
            src = rsync_src,
            dst = self.rsync_dst
        )

        self._run_command(command)
        self.logger.debug('ZFSLVMBackup._run_backup completed')


    def _create_zfs_snapshot(self, error_case):
        if not error_case:
            self.logger.info('creating ZFS snapshot...')
            snapshot_name = self.snapshot_prefix + datetime.now().strftime(self.snapshot_timestamp_format)
            command = 'zfs snapshot {dataset_name}@{snapshot_name}'.format(
                dataset_name=self.dataset_name, snapshot_name=snapshot_name)

            self._run_command(command, self.zfs_hostname)


    def _remove_zfs_snapshots_older_than(self, days, error_case):
        if not error_case:
            self.logger.info('removing expired ZFS snapshots...')
            expiration = datetime.now() - timedelta(days=days)

            # Let's find all the snapshots for this dataset
            command = 'zfs get -rH -o name,value type {dataset_name}'.format(dataset_name=self.dataset_name)
            (stdout, stderr) = self._run_command(command, self.zfs_hostname)

            snapshots = []
            # Sometimes we get extra lines which are empty,
            # so we'll strip the lines.
            for line in stdout.strip().split('\n'):
                name, dataset_type = line.split('\t')
                if dataset_type == 'snapshot':
                    # Let's try to only consider destroying snapshots made by us ;)
                    if name.split('@')[1].startswith(self.snapshot_prefix):
                        snapshots.append(name)

            # destroy expired snapshots
            for snapshot in snapshots:
                command = 'zfs get -H -o value creation {snapshot}'.format(snapshot=snapshot)
                (stdout, stderr) = self._run_command(command, self.zfs_hostname)
                creation_time = datetime.strptime(stdout.strip(), '%a %b %d %H:%M %Y')
                if creation_time <= expiration:
                    self._run_command('zfs destroy {snapshot}'.format(snapshot=snapshot), self.zfs_hostname)
                    self.logger.info('{snapshot} destroyed'.format(snapshot=snapshot))
