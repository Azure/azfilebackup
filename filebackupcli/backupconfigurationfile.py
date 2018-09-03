# coding=utf-8

import re
import logging
import os.path
from .funcmodule import printe
from .backupexception import BackupException

class BackupConfigurationFile:
    def __init__(self, filename):
        if not os.path.isfile(filename):
            raise(BackupException("Cannot find configuration file {}:".format(filename)))
        self.filename = filename

        try:
            vals = BackupConfigurationFile.read_key_value_file(filename=self.filename)
            logging.debug("Configuration {}".format(str(vals)))
        except Exception as e:
            raise(BackupException("Error parsing config file {}:\n{}".format(filename, e.message)))

    def get_value(self, key):
        values = BackupConfigurationFile.read_key_value_file(filename=self.filename)
        return values[key]

    @staticmethod
    def read_key_value_file(filename):
        """
            >>> values = BackupConfigurationFile.read_key_value_file(filename="config.txt.template")
            >>> values["sap.CID"]
            'AZU'
        """
        with open(filename, mode='rt') as file:
            lines = file.readlines()
            # skip comments and empty lines
            content_lines = filter(lambda line: not re.match(r"^\s*#|^\s*$", line), lines) 
            content = dict(line.split(":", 1) for line in content_lines)
            return dict(map(lambda x: (x[0].strip(), x[1].strip()), content.iteritems()))
