from ari_backup import LVMBackup, settings

class NexentaLVMBackup(LVMBackup):
    def __init__(self, source_hostname, label, rsync_dst_url):
        self.rsync_dst_url = rsync_dst_url

        # We'll bring in the rsync_option from our settings, but it is a var
        # that the end-user is welcome to override.
        self.rsync_options = settings.rsync_options

        # call our super class's constructor to enable LVM snapshot management
        super(NexentaLVMBackup, self).__init__(source_hostname, label, None)

        # TODO setup hooks to create ZFS snapshots


    def _run_backup(self):
        # TODO Throw an exception if we see things in the include or exclude
        # lists since we don't use them in this class?

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
            dst = self.rsync_dst_url
        )

        self._run_command(command)
