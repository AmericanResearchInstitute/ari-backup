import os
import settings
import subprocess
import shlex
import sys

from logger import Logger

def exit(logger, message, error=True):
    '''Wrapper around sys.exit to faciliate good logging

    This just functionalizes some code that gets used a lot in here,
    logging a message and appropriately exiting.

    It default to an error on exit, but can be used to exit nicely.
    Usually (always?) an errorless exit means "keep going", though.

    '''
    # Make sure to stringify the message, sometimes we just get ints back :(
    message = str(message)
    if error:
        logger.error(message)
        sys.exit(1)
    else:
        logger.info(message)
        sys.exit(0)

class ARIBackup(object):
    '''Wrapper around rdiff-backup

    It provides facilities to run
    remote backups with additional custom features to help manage LVM snapshots
    for backup purposes.  It was written with a Xen/LVM type environment in mind.
    Arbitrary remote job execution is also provided.

    '''
    def __init__(self, label, source_hostname, remove_older_than_timespec=None):
        # This is the host that has the source data.
        self.source_hostname = source_hostname

        # The name of the backup job (this will be the name of the directory in the backup store
        # that has the data).
        self.label = label

        # We'll bring in the remote_user from our settings, but it is a var
        # that the end-user is welcome to override.
        self.remote_user = settings.remote_user

        # setup logging
        self.logger = Logger('ARIBackup ({label})'.format(label=label))

        # Include nothing by default
        self.include_dir_list = []
        self.include_file_list = []
        # Exclude nothing by default
        # We'll put the '**' exclude on the end of the arg_list later
        self.exclude_dir_list = []
        self.exclude_file_list = []

        # initialize hook lists
        self.pre_job_hook_list = []
        self.post_job_hook_list = []

        if remove_older_than_timespec != None:
            self.post_job_hook_list.append((
                self._remove_older_than,
                {'timespec': remove_older_than_timespec}))

        self.logger.info('initialized')


    def _process_pre_job_hooks(self):
        self.logger.info('processing pre-job hooks...')
        for task in self.pre_job_hook_list:
            # Let's do some assignments for readability
            hook = task[0]
            kwargs = task[1]
            hook(**kwargs)


    def _process_post_job_hooks(self, error_case=False):
        if error_case:
            self.logger.info('processing post-job hooks for error case...')
        else:
            self.logger.info('processing post-job hooks...')

        for task in self.post_job_hook_list:
            # Let's do some assignments for readability
            hook = task[0]
            kwargs = task[1]
            kwargs.update({'error_case': error_case})
            hook(**kwargs)


    def _run_command(self, command, host='localhost'):
        '''Runs an arbitrary command on host.

        Given an input string or list, we attempt to execute it on the host via
        SSH unless host is "localhost".

        '''
        # make args a list if it's not already so
        if type(command) == str:
            args = shlex.split(command)
        elif type(command) == list:
            args = command
        else:
            raise Exception('_run_command: command arg must be str or list')

        # add SSH arguments if this is a remote command
        if host != 'localhost':
            ssh_args = shlex.split('%s %s@%s' % (settings.ssh_path, self.remote_user, host))
            args = ssh_args + args

        try:
            self.logger.info('_run_command %r' % args)
            p = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()

            if stdout:
                self.logger.info(stdout)
            if stderr:
                self.logger.error(stderr)
            exitcode = p.returncode
        except IOError:
            exit(self.logger, 'Unable to execute %s. Exiting' % args)

        if exitcode > 0:
            error_message = ('[{host}] A command terminated with errors and likely requires intervention. The '
                'command attempted was "{command}".').format(
                    host=host, command=command)
            raise Exception(error_message)


    def run_backup(self):
        try:
            self._process_pre_job_hooks()
            self._run_backup()
            self._process_post_job_hooks()
        except Exception, e:
            self.logger.error((str(e)))
            self.logger.error('performing clean up...')
            self._process_post_job_hooks(error_case=True)


    def _run_backup(self):
        '''Run typical rdiff-backup backup job.

        This generates an argument list as expected by rdiff_backup,
        and then passes it directly to the rdiff_backup module

        '''
        self.logger.info('_run_backup started')

        # Init our arguments list with the path to rdiff-backup.
        # This will be in the format we'd normally pass to the command-line
        # e.g. [ '--include', '/dir/to/include', '--exclude', '/dir/to/exclude']
        arg_list = [settings.rdiff_backup_path]

        # setup some default rdiff-backup options
        # TODO provide a way to override these
        arg_list.append('--exclude-device-files')
        arg_list.append('--exclude-fifos')
        arg_list.append('--exclude-sockets')

        # Bring the terminal verbosity down so that we only see errors
        arg_list += ['--terminal-verbosity', '1']

        # This conditional reads strangely, but that's because rdiff-backup
        # not only defaults to having SSH compression enabled, it also doesn't
        # have an option to explicitly enable it -- only one to disable it.
        if not settings.ssh_compression:
            arg_list.append('--ssh-no-compression')

        # Populate self.argument list
        for exclude_dir in self.exclude_dir_list:
            arg_list.append('--exclude')
            arg_list.append(exclude_dir)

        for exclude_file in self.exclude_file_list:
            arg_list.append('--exclude-filelist')
            arg_list.append(exclude_file)

        for include_dir in self.include_dir_list:
            arg_list.append('--include')
            # TODO: Is this \x20 dealio important? If so, shouldn't it be on exclude_dir too?
            arg_list.append(include_dir.replace(' ', '\x20'))

        for include_file in self.include_file_list:
            arg_list.append('--include-filelist')
            arg_list.append(include_file)

        # Exclude everything else
        arg_list.append('--exclude')
        arg_list.append('**')

        # Add a source argument
        if self.source_hostname == 'localhost':
            arg_list.append('/')
        else:
            arg_list.append('%s@%s::/' % (self.remote_user, self.source_hostname) )

        # Add a destination argument
        arg_list.append('%s/%s' % ( settings.backup_store_path, self.label) )

        # Rdiff-backup GO!
        self._run_command(arg_list)
        self.logger.info('_run_backup completed')


    def _remove_older_than(self, timespec, error_case=False):
        '''Trims increments older than timespec

        Post-job hook that uses rdiff-backup's --remove-old-than feature to
        trim old increments from the backup history

        '''
        if not error_case: 
            self.logger.info('remove_older_than %s started' % timespec)

            arg_list = [settings.rdiff_backup_path]
            arg_list.append('--force')
            arg_list.append('--remove-older-than')
            arg_list.append(timespec)
            arg_list.append('%s/%s' % (settings.backup_store_path, self.label))

            self._run_command(arg_list)
            self.logger.info('remove_older_than %s completed' % timespec)


    def __del__(self):
        '''Our deconstructor, mostly for logging that things have stopped.

        This is especially useful for knowing that the script has
        abruptly stopped due to a syntax error or something similar.

        '''
        self.logger.info('stopped')


class LVMBackup(ARIBackup):
    def __init__(self, source_hostname, label):
        super(self.__class__, self).__init__(source_hostname, label)
        # This is a list of 2-tuples, where each inner 2-tuple expresses the lv to back up, and
        # the mount point for that LV. For example, (('hostname/root, '/'),)
        self.lv_list = []
        # a list of tuples with the snapshot paths and where they should be
        # mounted
        self.lv_snap_shots = []
        self.snap_shot_mount_point_base_path = os.path.join(settings.remote_snapshot_mount_root, self.label)

    def _create_snap_shots(self):
        '''Creates snapshots of all the volumns listed in self.lvList.'''
        for v in self.lv_list:
            vg_name, lv_name = v[0].split('/')
            new_lv_name = lv_name + '-rdiff'
            mount_path = '%s%s' % (self.snap_shot_mount_point_base_path, v[1])

            # v[2] might be mount options for this LV. We'd like to pass that info along if available
            # to lvSnapShots.
            try:
                self.lv_snap_shots.append((vg_name + '/' + new_lv_name, mount_path, v[2]))
            except IndexError:
                self.lv_snap_shots.append((vg_name + '/' + new_lv_name, mount_path))

            self.runJobOnVMHost('lvcreate -s -L 1G %s -n %s' % (v[0], newLVName))

    def _deleteSnapShots(self):
        for v in self.lvSnapShots:
            # the -f will only
            self.runJobOnVMHost('lvremove -f %s' % v[0])

    def _mountSnapShots(self):
        for v in self.lvSnapShots:
            # mkdir the mount point
            self.runJobOnVMHost('mkdir -p %s' % v[1])
            # mount the LV, possibly with mount options
            # The 'mountpoint ... ||' syntax is used to ensure we don't mount
            # where something's already mounted. TODO: If the target mountpoint
            # is already a mount point, this currently fails silently
            try:
                self.runJobOnVMHost('mountpoint -q %s || mount -o %s %s %s' %
                        (v[1], v[2], '/dev/' + v[0], v[1])
                )
            except IndexError:
                self.runJobOnVMHost('mountpoint -q %s || mount %s %s' %
                        (v[1], '/dev/' + v[0], v[1])
                )

    def _umountSnapShots(self):
        localLVSnapShots = self.lvSnapShots
        # we want to umount these LVs in reverse order as this should ensure
        # that we umount the deepest paths first
        localLVSnapShots.reverse()
        for v in localLVSnapShots:
            self.runJobOnVMHost('umount %s' % v[1])
            self.runJobOnVMHost('rmdir %s' % v[1])

    def runSnapShotBackup(self):
        '''Run backup of LVM snapshots

        This generates an argument list as expected by rdiff_backup,
        and then passes it directly to the rdiff_backup module

        '''
        self.logger.info('runSnapShotBackup started')

        if not self.sshCompression:
            self.argList.append('--ssh-no-compression')

        self._createSnapShots()
        self._mountSnapShots()

        # Populate self.argument list
        for includeDir in self.includeDirList:
            includeDir = '%s%s' % (self.snapShotMountPointBasePath, includeDir)
            self.argList.append('--include')
            self.argList.append(includeDir.replace(' ', '\x20'))

        for excludeDir in self.excludeDirList:
            excludeDir = '%s%s' % (self.snapShotMountPointBasePath, excludeDir)
            self.argList.append('--exclude')
            self.argList.append(excludeDir)

        # I removed support for the includeFileList and excludeFileList in this
        # method, as it would take some cooking and I doubt we'll use it.

        # Exclude everything else
        self.argList.append('--exclude')
        self.argList.append('**')

        # Configure a tmpdir on the backup volume
        # TODO Custom Tempdir not supported until rdiff-backup 1.1.13
        # self.argList.append('--tempdir')
        # self.argList.append(self.backupTmp)

        # Let rdiff know where it's aimed
        self.argList.append('%s@%s::%s' % (self.userName, self.vmHostName, self.snapShotMountPointBasePath) )
        self.argList.append('%s/%s' % ( self.backupStore, self.hostName) )

        # Call rdiff-backup, spawning a new thread and waiting for it
        # to execute before finishing
        try:
            rdiff_backup.Main.error_check_Main(self.argList)
        except:
            exit(self.logger, 'An unforseen excpetion has arisen. Exiting.')

        self._umountSnapShots()
        self._deleteSnapShots()

        self.logger.info('runSnapShotBackup completed')
