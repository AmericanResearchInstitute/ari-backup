import os
import sys
import subprocess
import shlex

from logger import Logger

def exit(logger, message, error=True):
    '''
    Wrapper around sys.exit to faciliate good logging
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
    '''
    This class is a wrapper around rdiff-backup.  It provides facilities to run
    remote backups with additional custom features to help manage LVM snapshots
    for backup purposes.  It was written with a Xen/LVM type environment in mind.
    Arbitrary remote job execution is also provided.
    '''
    def __init__(self, hostname):
        # Gotta have a hostname...
        self.hostName = hostname

        # setup logging
        self.logger = Logger('ariBackup (%s)' % hostname)

        # TODO: Add validation on these paths
        self.sshExec = '/usr/bin/ssh'
        self.backupExec = '/usr/bin/rdiff-backup'
        self.backupStore = '/srv/backup-store'
        self.backupTmp = '%s/tmp' % ( self.backupStore )

        # These will be set by the backup script
        self.userName = ''
        # these may be set if we're doing a snap shop backup
        self.vmHostName = ''
        self.lvList = []
        # a list of tuples with the snapshot paths and where they should be
        # mounted
        self.lvSnapShots = []
        self.snapShotMountPointBasePath = os.path.join('/tmp', self.hostName)

        # Include nothing by default
        self.includeDirList = []
        self.includeFileList = []

        # Exclude nothing by default
        # Add the '**' exclude to the end of the list later
        self.excludeDirList = []
        self.excludeFileList = []

        # Define argument list to be passed to the rdiff_backup module
        # This will be in the format we'd normally pass to the command-line
        # i.e. [ '--include', '/dir/to/include', '--exclude', '/dir/to/exclude']
        self.argList = []

		# setup some default rdiff-backup options
        self.argList.append('--exclude-device-files')
        self.argList.append('--exclude-fifos')
        self.argList.append('--exclude-sockets')

        # Bring the terminal verbosity down so that we only see errors
        self.argList += ['--terminal-verbosity', '1']

        # Default to no SSH compression
        self.sshCompression = False

        self.logger.info('initialized')

    def __del__(self):
        '''
        This deconstructor is mostly for logging that things have stopped. This
        especially useful to know that the script has abruptly stopped due to a
        syntax error or something similar.
        '''
        self.logger.info('stopped')

    def runJob(self, command):
        '''
        This takes a given input string and attempts to execute
        it on the remote host via SSH
        '''
        # Spawn off an SSH child process to do the given command
        # Wait for ssh to end before processing
        try:
            args = shlex.split('%s %s@%s %s' % (self.sshExec, self.userName, self.hostName, command.strip('"')))
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

        # TODO This is strange, comparing the value of an exit code to a string.  Perhaps this should be an int 0, not a string '0'?
        if exitcode > '0':
            self.logger.error('%s terminated with errors and likely requires intervention. The command attempted was "%s". Backups will attempt to proceed.' %
                (self.sshExec, command)
            )

        return exitcode

    def runLocalJob(self, command):
        '''
        This takes a given input string and attempts to execute
        it on the local host
        '''
        try:
            args = shlex.split(command)
            self.logger.info('runLocalJob %r' % args)
            p = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()

            if stdout:
                self.logger.info(stdout)
            if stderr:
                self.logger.error(stderr)
            exitcode = p.returncode

        except IOError:
            exit(self.logger, 'Unable to execute %s. Exiting' % command)

        # TODO This is strange, comparing the value of an exit code to a string.  Perhaps this should be an int 0, not a string '0'?
        if exitcode > '0':
            self.logger.error('%s terminated with errors and likely requires intervention. Backups will attempt to proceed.' %
                command
            )

        return exitcode

    def runJobOnVMHost(self, command):
        '''
        This takes a given input string and attempts to execute
        it on the VM host via SSH
        '''

        # Set up a log prefix so we know what VM host is being used in
        # output of communicate() handles
        log_prefix = '(vmHost %s) ' % self.vmHostName

        # Spawn off an SSH child process to do the given command
        # Wait for ssh to end before processing
        try:
            args = shlex.split('%s %s@%s "%s"' % (self.sshExec, self.userName, self.vmHostName, command.strip('"')))
            self.logger.info('runJobOnVMHost %r' % args)
            p = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()

            if stdout:
                self.logger.info(log_prefix + stdout)
            if stderr:
                self.logger.error(log_prefix + stderr)
            exitcode = p.returncode
        except IOError:
            exit(self.logger, 'Unable to execute %s. Exiting' % self.sshExec)

        # TODO This is strange, comparing the value of an exit code to a string.  Perhaps this should be an int 0, not a string '0'?
        if exitcode > '0':
            self.logger.error('%s terminated with errors and likely requires intervention. The command attempted was "%s". Backups will attempt to proceed.' %
                (self.sshExec, command)
            )

        return exitcode

    def runBackup(self):
        '''
        This generates an argument list as expected by rdiff_backup,
        and then passes it directly to the rdiff_backup module
        '''

        self.logger.info('runBackup started')

        if not self.sshCompression:
            self.argList.append('--ssh-no-compression')

        # Populate self.argument list
        for excludeDir in self.excludeDirList:
            self.argList.append('--exclude')
            self.argList.append(excludeDir)

        for excludeFile in self.excludeFileList:
            self.argList.append('--exclude-filelist')
            self.argList.append(excludeFile)

        for includeDir in self.includeDirList:
            self.argList.append('--include')
            self.argList.append(includeDir.replace(' ', '\x20'))

        for includeFile in self.includeFileList:
            self.argList.append('--include-filelist')
            self.argList.append(includeFile)

        # Exclude everything else
        self.argList.append('--exclude')
        self.argList.append('**')

        # Configure a tmpdir on the backup volume
        # TODO Custom Tempdir not supported until rdiff-backup 1.1.13
        # self.argList.append('--tempdir')
        # self.argList.append(self.backupTmp)

        # Let rdiff know where it's aimed
        self.argList.append('%s@%s::/' % (self.userName, self.hostName) )
        self.argList.append('%s/%s' % ( self.backupStore, self.hostName) )

        # Call rdiff-backup, spawning a new thread and waiting for it
        # to execute before finishing
        try:
            import rdiff_backup.Main
            rdiff_backup.Main.error_check_Main(self.argList)
        except IOError:
            exit(self.logger, 'Unable to execute %s. Exiting...' % self.backupExec)
        except:
            exit(self.logger, 'An unforseen excpetion has arisen. Exiting.')

        self.logger.info('runBackup completed')

    def doMysqlDump(self, gzip=False, routines=True):
        '''
        Constructs a suitable mysqldump command-line and pass to runJob
        gzip is a bool, and will pipe through gzip to all.sql.gz
        '''

        dumpExec = "mysqldump -u backup --all-databases --force"

        if routines:
            dumpExec = '%s --routines' % dumpExec

        # We shouldn't ever need to gzip, but you can if you want to.
        if gzip:
            gzPipe = "gzip --rsyncable --to-stdout"
            outFile = "/var/backups/mysql/all.sql.gz"
            command = "%s | %s > %s" % (dumpExec, gzPipe, outFile)
        else:
            outFile = "/var/backups/mysql/all.sql"
            command = "%s > %s" % (dumpExec, outFile)

        self.runJob(command)

    def _createSnapShots(self):
        '''
        Creates snapshot logical volumes of all the volumns listed in
        self.lvList
        '''

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
        '''
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
            import rdiff_backup.Main
            rdiff_backup.Main.error_check_Main(self.argList)
        except IOError:
            exit(self.logger, 'Unable to execute %s. Exiting' % self.backupExec)
        except:
            exit(self.logger, 'An unforseen excpetion has arisen. Exiting.')

        self._umountSnapShots()
        self._deleteSnapShots()

        self.logger.info('runSnapShotBackup completed')

    def removeOlderThan(self, timespec):
        '''
        uses rdiff-backup's --remove-old-than feature to trim old
        increments from the backup history
        '''

        self.logger.info('removeOlderThan %s started' % timespec)
        # use a local argList since we're doing something entirely different
        # than a backup
        argList = []
        argList.append('--force')
        argList.append('--remove-older-than')
        argList.append(timespec)
        argList.append('%s/%s' % (self.backupStore, self.hostName))

        # Call rdiff-backup, spawning a new thread and waiting for it
        # to execute before finishing
        try:
            import rdiff_backup.Main
            rdiff_backup.Main.error_check_Main(argList)
            self.logger.info('removeOlderThan %s completed' % timespec)
        except IOError:
            exit(self.logger, 'Unable to execute %s. Exiting...' % self.backupExec)
        except:
            exit(self.logger, 'An unforseen exception has arisen. Exiting...')
