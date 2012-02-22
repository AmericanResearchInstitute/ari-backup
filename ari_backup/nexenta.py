from ari_backup import LVMBackup, settings

class NexentaLVMBackup(LVMBackup):
    def __init__(self, source_hostname, label, rsync_dst_url):
        self.rsync_dst_url = rsync_dst_url

        # call our super class's constructor to enable LVM snapshot management
        super(self.__class__, self).__init__(source_hostname, label, None)

        # TODO setup hooks to create ZFS snapshots


    def _run_backup(self):
        if source_hostname == 'localhost':
            rsync_src = self.snapshot_mount_point_base_path
        else:
            rsync_src = '{remote_user}@{source_hostname}::{snapshot_mount_point_base_path}'.format(
                remote_user = self.remote_user,
                source_hostname = self.source_hostname,
                snapshot_mount_point_base_path = self.snapshot_mount_point_base_path
            )

        command = '{rsync_path} {rsync_options} {src} {dst}'.format(
            rsync_path = settings.rsync_path,
            rsync_option = settings.rsync_options,
            src = rsync_src,
            dst = rsync_dst_url
        )

        self.logger.info(command)
