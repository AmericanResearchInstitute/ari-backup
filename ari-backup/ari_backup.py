import os
import rdiff_backup.Main
import sys
import subprocess
import shlex

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

class AriBackup:
    '''Wrapper around rdiff-backup

    It provides facilities to run
    remote backups with additional custom features to help manage LVM snapshots
    for backup purposes.  It was written with a Xen/LVM type environment in mind.
    Arbitrary remote job execution is also provided.

    '''
    def __init__(self, source_hostname, label, **kwargs):
        # This is the host that has the source data.
        self.source_hostname = source_hostname

        # The name of the backup job (this will be the name of the directory in the backup store
        # that has the data).
        self.label = label

        # setup logging
        self.logger = Logger('AriBackup ({label})'.format(label=label))

        # Include nothing by default
        self.include_dir_list = []
        self.include_file_list = []
        # Exclude nothing by default
        # Add the '**' exclude to the end of the list later
        self.exclude_dir_list = []
        self.exclude_file_list = []

        # Read the settings from YAML, and mash them with stuff from kwargs
        self._process_settings(**kwargs)

        self.logger.info('initialized')

    def run_job(self, command, host='localhost'):
        try:
            return self._run_job(command, host)
        except Exception, e:
            exit(str(e))

    def _run_job(self, command, host='localhost'):
        '''Runs an arbitrary job on the remote host.

        Given an input string, we attempt to execute it on the remote
        host via SSH.

        '''
        # Spawn off an SSH child process to do the given command
        # Wait for ssh to end before processing
        try:
            if host == 'localhost':
                args = shlex.split(command)
            else:
                args = shlex.split('%s %s@%s %s' % (self.sshExec,
                    self.userName, self.hostName, command.strip('"')))
            self.logger.info('runJob %r' % args)
            p = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()

            if stdout:
                self.logger.info(stdout)
            if stderr:
                self.logger.error(stderr)
            exitcode = p.returncode
        except IOError:
            exit(self.logger, 'Unable to execute %s. Exiting' % self.sshExec)

        if exitcode > 0:
            error_message = ('[{host}] A command terminated with errors and likely requires intervention. The '
                'command attempted was "{command}".').format(
                    host=host, command=command)
            raise Exception(error_message)


    def run_backup(self):
        try:
            self._run_backup()
        except Exception, e:
            exit(str(e))


    def _run_backup(self):
        '''Run typical rdiff-backup backup job.

        This generates an argument list as expected by rdiff_backup,
        and then passes it directly to the rdiff_backup module

        '''
        self._run_pre_backup_hooks()

        self.logger.info('run_backup started')

        # Define argument list to be passed to the rdiff_backup module
        # This will be in the format we'd normally pass to the command-line
        # i.e. [ '--include', '/dir/to/include', '--exclude', '/dir/to/exclude']
        arg_list = []

        # setup some default rdiff-backup options
        arg_list.append('--exclude-device-files')
        arg_list.append('--exclude-fifos')
        arg_list.append('--exclude-sockets')

        # Bring the terminal verbosity down so that we only see errors
        arg_list += ['--terminal-verbosity', '1']

        if not self.ssh_compression:
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

        # Let rdiff know where it's aimed
        if self.source_hostname == 'localhost':
            arg_list.append('/')
        else:
            arg_list.append('%s@%s::/' % (self.username, self.source_hostname) )
        arg_list.append('%s/%s' % ( self.backup_store, self.label) )

        rdiff_backup.Main.error_check_Main(arg_list)

        self.logger.info('run_backup completed')

        self._run_post_backup_hooks()


    def remove_older_than(self, timespec):
        '''Trims old backups older than timestamp

        Uses rdiff-backup's --remove-old-than feature to trim old
        increments from the backup history

        '''
        self.logger.info('remove_older_than %s started' % timespec)

        arg_list = []
        arg_list.append('--force')
        arg_list.append('--remove-older-than')
        arg_list.append(timespec)
        arg_list.append('%s/%s' % (self.backup_store, self.label))

        rdiff_backup.Main.error_check_Main(arg_list)

        self.logger.info('remove_older_than %s completed' % timespec)


    def _rdiff_backup(self, arg_list):
        # TODO: Does this *really* start a new thread? Does that even matter for our code?
        # Call rdiff-backup, spawning a new thread and waiting for it
        # to execute before finishing
        try:
        except:
            exit(self.logger, 'An unforseen excpetion has arisen. Exiting.')


    def _process_settings(self, **kwargs):
        ###################
        # These are in YAML
        ###################
        # TODO: Add validation on these paths
        self.ssh_exec = '/usr/bin/ssh'
        self.backup_store = '/srv/backup-store'
        # These will be set by the backup script
        self.remote_username = ''
        # Default to no SSH compression
        self.ssh_compression = False
        ##################

        settings = yaml
        for key, value in enumerate(kwargs):
            settings[key] = value
        for key, value in enumerate(settings):
            setattr(self, key, value)


    def __del__(self):
        '''Our deconstructor, mostly for logging that things have stopped.

        This is especially useful for knowing that the script has
        abruptly stopped due to a syntax error or something similar.

        '''
        self.logger.info('stopped')


class LVMBackup(AriBackup):
    def __init__(self, source_hostname, label, *args, **kwargs):
        super(self.__class__, self).__init__(source_hostname, label, *args, **kwargs)
        # This is a list of 2-tuples, where each inner 2-tuple expresses the lv to back up, and
        # the mount point for that LV. For example, (('hostname/root, '/'),)
        self.lv_list = []
        # a list of tuples with the snapshot paths and where they should be
        # mounted
        self.lv_snap_shots = []
        self.snap_shot_mount_point_base_path = os.path.join('/tmp', self.label)

    def _createSnapShots(self):
        '''Creates snapshots of all the volumns listed in self.lvList.'''
        for v in self.lvList:
            vgName, lvName = v[0].split('/')
            newLVName = lvName + '-rdiff'
            mountPath = '%s%s' % (self.snapShotMountPointBasePath, v[1])

            # v[2] might be mount options for this LV. We'd like to pass that info along if available
            # to lvSnapShots.
            try:
                self.lvSnapShots.append((vgName + '/' + newLVName, mountPath, v[2]))
            except IndexError:
                self.lvSnapShots.append((vgName + '/' + newLVName, mountPath))

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
