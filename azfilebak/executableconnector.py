# coding=utf-8

import subprocess
import os
import logging

from .naming import Naming
from .funcmodule import printe, out, log_stdout_stderr
from .backupexception import BackupException

class ExecutableConnector:
    def __init__(self, backup_configuration):
        self.backup_configuration = backup_configuration

