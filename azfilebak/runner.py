# coding=utf-8

# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
# --------------------------------------------------------------------------

"""Runner module."""

import sys
import os
import os.path
import getpass
import socket
import logging
import argparse
import pid

from .backupagent import BackupAgent
from .backupconfiguration import BackupConfiguration
from .scheduleparser import ScheduleParser
from .timing import Timing
from .backupexception import BackupException
from .__init__ import version

class Runner(object):
    """
    The Runner class parses the command line arguments and calls the
    BackupAgent to execute the actual actions.
    """

    @staticmethod
    def configure_logging():
        """Configure logging."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)-15s pid-%(process)d line-%(lineno)d %(levelname)s: \"%(message)s\""
        )
        # Turn down verbosity on some of the third-party libraries
        logging.getLogger('azure.storage').setLevel(logging.ERROR)
        logging.getLogger('urllib3.connectionpool').setLevel(logging.ERROR)
        logging.getLogger('msrestazure.azure_active_directory').setLevel(logging.ERROR)

    @staticmethod
    def arg_parser():
        """Parse arguments."""
        parser = argparse.ArgumentParser()

        commands = parser.add_argument_group("commands")

        commands.add_argument("-f", "--full-backup", help="Perform backup for configuration", action="store_true")
        commands.add_argument("-r", "--restore", help="Perform restore for date")
        commands.add_argument("-l", "--list-backups", help="Lists all backups in Azure storage",
                              action="store_true")
        commands.add_argument("-p", "--prune-old-backups",
                              help="Removes old backups from Azure storage ('--prune-old-backups 30d' removes files older 30 days)")
        commands.add_argument("-x", "--show-configuration",
                              help="Shows the VM's configuration values",
                              action="store_true")
        commands.add_argument("-u", "--unit-tests", help="Run unit tests", action="store_true")

        options = parser.add_argument_group("options")

        options.add_argument("-y", "--force",
                             help="Perform forceful backup (ignores age of last backup or business hours)",
                             action="store_true")

        options.add_argument("-F", "--fileset", help="Select fileset(s) to backup or restore ('--fileset A,B,C')")

        options.add_argument("-C", "--container", help="Override container name to use (for list and restore)")

        options.add_argument("-s", "--stream",
                             help="Stream restore data to stdout",
                             action="store_true")

        options.add_argument("-o",  "--output-dir", help="Specify target folder for backup files")

        options.add_argument("-c", "--config", help="the path to the config file")

        options.add_argument("-d", "--debug",
                             help="display debug messages",
                             action="store_true")

        options.add_argument("-R", "--rate-limit",
                             help="Limits the rate the backup is written/read [value in MB/s]")

        return parser

    @staticmethod
    def log_script_invocation():
        """Return a string containing invocation details"""
        return ", ".join([
            "Script version v{}".format(version()),
            "Script arguments: {}".format(str(sys.argv)),
            "Current directory: {}".format(os.getcwd()),
            "User: {} (uid {}, gid {})".format(getpass.getuser(), os.getuid(), os.getgid),
            "Hostname: {}".format(socket.gethostname()),
            "uname: {}".format(str(os.uname())),
            "ProcessID: {}".format(os.getpid()),
            "Parent ProcessID: {}".format(os.getppid())
        ])

    @staticmethod
    def get_config_file(args):
        """Check the existence of the configuration file and returns its absolute path."""
        if args.config:
            config_file = os.path.abspath(args.config)
        else:
            config_file = '/usr/sap/backup/backup.conf'
        if not os.path.isfile(config_file):
            raise BackupException("Cannot find configuration file '{}'".format(config_file))
        return config_file

    @staticmethod
    def get_output_dir(args, cnf):
        """Determine output (temp) directory and check that it is usable."""

        if args.output_dir:
            output_dir = os.path.abspath(args.output_dir)
            specified_via = "dir was user-supplied via command line"
        elif cnf.get_standard_local_directory():
            output_dir = os.path.abspath(cnf.get_standard_local_directory())
            specified_via = "dir is specified in config file {}".format(args.config)
        else:
            output_dir = os.path.abspath("/tmp")
            specified_via = "fallback dir"

        logging.debug("Output dir (%s): %s", specified_via, output_dir)

        if not os.path.exists(output_dir):
            raise BackupException("Directory {} does not exist".format(output_dir))

        try:
            test_file_name = os.path.join(output_dir, '__delete_me_ase_backup_test__.txt')
            with open(test_file_name, 'wt') as testfile:
                testfile.write("Hallo")
            os.remove(test_file_name)
        except Exception:
            raise BackupException("Directory {} ({}) is not writable"
                                  .format(output_dir, specified_via))

        return output_dir

    @staticmethod
    def get_filesets(args):
        """ Determine filesets to backup or restore."""
        if args.fileset:
            filesets = args.fileset.split(",")
            logging.debug("User manually selected filesets: %s", str(filesets))
            return filesets

        logging.debug("User did not select filesets, will use default fileset")
        return []

    @staticmethod
    def main():
        """Main method."""

        Runner.configure_logging()
        parser = Runner.arg_parser()
        args = parser.parse_args()

        if args.debug:
            logging.getLogger().setLevel(logging.DEBUG)

        logging.debug(Runner.log_script_invocation())

        config_file = Runner.get_config_file(args=args)
        backup_configuration = BackupConfiguration(config_file)
        backup_agent = BackupAgent(backup_configuration)
        output_dir = Runner.get_output_dir(args, backup_configuration)
        filesets = Runner.get_filesets(args)

        force = args.force
        rate = args.rate_limit

        for line in backup_agent.get_configuration_printable(output_dir=output_dir):
            logging.debug(line)

        if args.full_backup:
            try:
                with pid.PidFile(pidname='fileset-backup-full') as _p:
                    backup_agent.backup(filesets=filesets, is_full=args.full_backup, force=force, rate=rate)
            except pid.PidFileAlreadyLockedError:
                logging.warn("Skip full backup, already running")
        elif args.restore:
            if args.restore.endswith('.tar.gz'):
                # Restore using blob name
                if filesets:
                    logging.warn("Ignoring fileset (blob name provided)")
                backup_agent.restore_blob(
                    blobname=args.restore,
                    output_dir=output_dir,
                    stream=args.stream,
                    container=args.container)
            else:
                # Restore using fileset + timestamp
                try:
                    Timing.parse(args.restore)
                except Exception:
                    raise BackupException("Cannot parse restore point \"{}\"".format(args.restore))
                if len(filesets) > 1 and args.stream:
                    raise BackupException("Cannot stream more than one fileset")
                backup_agent.restore(
                    restore_point=args.restore,
                    output_dir=output_dir,
                    filesets=filesets,
                    stream=args.stream,
                    container=args.container)
        elif args.list_backups:
            backup_agent.list_backups(filesets=filesets, container=args.container)
        elif args.prune_old_backups:
            age = ScheduleParser.parse_timedelta(args.prune_old_backups)
            backup_agent.prune_old_backups(older_than=age, filesets=filesets)
        elif args.show_configuration:
            print(backup_agent.show_configuration(output_dir=output_dir))
        else:
            parser.print_help()
