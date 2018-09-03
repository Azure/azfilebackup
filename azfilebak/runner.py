# coding=utf-8

import logging
import argparse
import pid
import sys
import os
import getpass
import socket
import os.path

from .funcmodule import printe
from .backupagent import BackupAgent
from .backupconfiguration import BackupConfiguration
from .scheduleparser import ScheduleParser
from .timing import Timing
from .backupexception import BackupException
from .__init__ import version

class Runner:
    @staticmethod
    def configure_logging():
        logging.basicConfig(
            filename="asebackupcli.log",
            level=logging.DEBUG,
            format="%(asctime)-15s pid-%(process)d line-%(lineno)d %(levelname)s: \"%(message)s\""
            )
        logging.getLogger('azure.storage').setLevel(logging.FATAL)

    @staticmethod
    def arg_parser():
        parser = argparse.ArgumentParser()
        requiredNamed = parser.add_argument_group("required arguments")
        requiredNamed.add_argument("-c",  "--config", help="the path to the config file")

        commands = parser.add_argument_group("commands")
        commands.add_argument("-f",  "--full-backup", help="Perform full backup", action="store_true")
        commands.add_argument("-r",  "--restore", help="Perform restore for date")
        commands.add_argument("-l",  "--list-backups", help="Lists all backups in Azure storage", action="store_true")
        commands.add_argument("-p",  "--prune-old-backups", help="Removes old backups from Azure storage ('--prune-old-backups 30d' removes files older 30 days)")
        commands.add_argument("-x",  "--show-configuration", help="Shows the VM's configuration values", action="store_true")
        commands.add_argument("-u",  "--unit-tests", help="Run unit tests", action="store_true")

        options = parser.add_argument_group("options")

        options.add_argument("-o",  "--output-dir", help="Specify target folder for backup files")
        options.add_argument("-y",  "--force", help="Perform forceful backup (ignores age of last backup or business hours)", action="store_true")
        options.add_argument("-s",  "--skip-upload", help="Skip uploads of backup files", action="store_true")
        return parser

    @staticmethod
    def log_script_invocation():
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
    def get_config_file(args, parser):
        if args.config:
            config_file = os.path.abspath(args.config)
            if not os.path.isfile(config_file):
                raise(BackupException("Cannot find configuration file '{}'".format(config_file)))

            return config_file
        else:
            parser.print_help()

            raise(BackupException("Please specify a configuration file."))

    @staticmethod
    def get_output_dir(args):

        if args.output_dir:
            output_dir = os.path.abspath(args.output_dir)
            specified_via = "dir was user-supplied via command line"
        elif args.config:
            output_dir = os.path.abspath(BackupConfiguration(args.config).get_standard_local_directory())
            specified_via = "dir is specified in config file {}".format(args.config)
        else:
            output_dir = os.path.abspath("/tmp")
            specified_via = "fallback dir"

        logging.debug("Output dir ({}): {}".format(specified_via, output_dir))

        if not os.path.exists(output_dir):
            raise BackupException("Directory {} does not exist".format(output_dir))

        try:
            test_file_name = os.path.join(output_dir, '__delete_me_ase_backup_test__.txt')
            with open(test_file_name,'wt') as testfile:
                testfile.write("Hallo")
            os.remove(test_file_name)
        except Exception:
            raise BackupException("Directory {} ({}) is not writable".format(output_dir, specified_via))

        return output_dir


    @staticmethod
    def get_databases(args):
        if args.databases:
            databases = args.databases.split(",")
            logging.debug("User manually selected databases: {}".format(str(databases)))
            return databases
        else:
            logging.debug("User did not select databases, trying to backup all databases")
            return []

    @staticmethod
    def run_unit_tests():
        import doctest
        # import unittest
        # suite = unittest.TestSuite()
        # result = unittest.TestResult()
        # finder = doctest.DocTestFinder(exclude_empty=False)
        # suite.addTest(doctest.DocTestSuite('backupagent', test_finder=finder))
        # suite.run(result)
        doctest.testmod() # doctest.testmod(verbose=True)

    @staticmethod
    def main():
        Runner.configure_logging()
        logging.info("#######################################################################################################")
        logging.info("#                                                                                                     #")
        logging.info("#######################################################################################################")
        parser = Runner.arg_parser()
        args = parser.parse_args()

        if args.unit_tests:
            Runner.run_unit_tests()
            return

        logging.debug(Runner.log_script_invocation())

        config_file = Runner.get_config_file(args=args, parser=parser)
        backup_configuration = BackupConfiguration(config_file)
        backup_agent = BackupAgent(backup_configuration)
        output_dir = Runner.get_output_dir(args)
        databases = Runner.get_databases(args)

        use_streaming=args.stream_upload
        skip_upload=args.skip_upload
        force=args.force

        for line in backup_agent.get_configuration_printable(output_dir=output_dir):
            logging.debug(line)

        if args.full_backup:
            try:
                #is_full, databases, output_dir, force, skip_upload, use_streaming
                with pid.PidFile(pidname='files-backup-full', piddir=".") as _p:
                    backup_agent.backup(is_full=True, databases=databases, output_dir=output_dir, force=force, skip_upload=skip_upload, use_streaming=use_streaming)
            except pid.PidFileAlreadyLockedError:
                logging.warn("Skip full backup, already running")
        elif args.restore:
            try:
                Timing.parse(args.restore)
            except Exception:
                raise BackupException("Cannot parse restore point \"{}\"".format(args.restore))

            backup_agent.restore(restore_point=args.restore, output_dir=output_dir, databases=databases)
        elif args.list_backups:
            backup_agent.list_backups(databases=databases)
        elif args.prune_old_backups:
            age = ScheduleParser.parse_timedelta(args.prune_old_backups)
            backup_agent.prune_old_backups(older_than=age, databases=databases)
        elif args.unit_tests:
            Runner.run_unit_tests()
        elif args.show_configuration:
            print(backup_agent.show_configuration(output_dir=output_dir))
        else:
            parser.print_help()
