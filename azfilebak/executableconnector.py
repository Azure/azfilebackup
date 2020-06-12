# coding=utf-8

# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
# --------------------------------------------------------------------------

"""
ExecutableConnector
"""

import shlex
import subprocess
import logging
import psutil
import os

class ExecutableConnector(object):
    """Drive the command that executes the backup."""

    def __init__(self, backup_configuration):
        self.backup_configuration = backup_configuration

    def assemble_backup_command(self, sources, exclude, rate = None):
        """Assemble backup command line from configuration."""

        # Base command
        cmd = 'tar cpzf - --hard-dereference --sparse'

        # Add explicit excludes
        excludes = exclude.split(',')
        for i in excludes:
            cmd += ' --exclude ' + i

        # Exclude /dev, /run, /sys
        # Also exclude /mnt/resource which is a volatile file system on Azure VMs
        if '/dev' not in excludes:
            cmd += ' --exclude /dev'
        if '/run' not in excludes:
            cmd += ' --exclude /run'
        if '/sys' not in excludes:
            cmd += ' --exclude /sys'
        if '/mnt/resource' not in excludes:
            cmd += ' --exclude /mnt/resource'

        # Exclude any mount point of type 'proc'
        mounts = psutil.disk_partitions(True)
        procmounts = [m.mountpoint for m in mounts if m.fstype == 'proc']
        for mnt in procmounts:
            cmd += ' --exclude ' + mnt

        # Add the path to archive
        cmd += ' ' + sources

        return cmd

    def check_pv_installed(self):
        for path in os.environ["PATH"].split(os.pathsep):
            pv = os.path.join(path, "pv")
            if os.path.isfile(pv) and os.access(pv, os.X_OK):
                return True
        logging.warn("Rate limit set but pv cannot be found! Skipping rate limit")
        return False

    def run_backup_command(self, command, rate=None):
        """Create a backup for a given fileset."""
        args = shlex.split(command)

        logging.info("Executing %s", ' '.join(args))

        proc = subprocess.Popen(
            args,
            stdout=subprocess.PIPE
        )

        if rate is not None and rate != "0" and self.check_pv_installed():
           proc2 = subprocess.Popen(['pv', '-L %sm' % rate], stdout=subprocess.PIPE, stdin=proc.stdout)
           return proc2

        return proc
