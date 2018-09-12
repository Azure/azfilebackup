# coding=utf-8
"""
ExecutableConnector
"""

import shlex
import subprocess
import logging

from .naming import Naming
from .funcmodule import printe, out, log_stdout_stderr
from .backupexception import BackupException

class ExecutableConnector(object):
    """Drive the command that executes the backup."""

    def __init__(self, backup_configuration):
        self.backup_configuration = backup_configuration

    def create_backup(self, fileset, is_full):
        """Create a backup for a given fileset."""

        # Determine command arguments from configuration
        command = self.backup_configuration.get_backup_command(fileset)
        args = shlex.split(command)

        logging.debug("Executing %s", args[0])

        proc = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        return proc
