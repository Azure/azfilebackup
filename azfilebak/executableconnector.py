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

class ExecutableConnector(object):
    """Drive the command that executes the backup."""

    def __init__(self, backup_configuration):
        self.backup_configuration = backup_configuration

    def assemble_backup_command(self, sources, exclude):
        """Assemble backup command line from configuration."""

        # Base command
        cmd = 'tar cpzf - --hard-dereference'

        # Add explicit excludes
        excludes = exclude.split(',')
        for i in excludes:
            cmd += ' --exclude ' + i

        # Exclude /dev, /proc, /run, /sys
        if '/dev' not in excludes:
            cmd += ' --exclude /dev'
        if '/run' not in excludes:
            cmd += ' --exclude /run'
        if '/sys' not in excludes:
            cmd += ' --exclude /sys'

        # Exclude any mount point of type 'proc'
        mounts = psutil.disk_partitions(True)
        procmounts = [m.mountpoint for m in mounts if m.fstype == 'proc']
        for mnt in procmounts:
            cmd += ' --exclude ' + mnt

        # Add the path to archive
        cmd += ' ' + sources

        return cmd

    def run_backup_command(self, command):
        """Create a backup for a given fileset."""
        args = shlex.split(command)

        logging.info("Executing %s", ' '.join(args))

        proc = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        return proc
