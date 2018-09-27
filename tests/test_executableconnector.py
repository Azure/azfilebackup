"""Unit tests for executableconnector."""
import os
import subprocess
import unittest
from azfilebak import backupconfiguration
from azfilebak import executableconnector
from tests.loggedtestcase import LoggedTestCase

class TestExecutableConnector(LoggedTestCase):
    """Unit tests for class ExecutableConnector."""

    def setUp(self):
        self.cfg = backupconfiguration.BackupConfiguration(config_filename="sample_backup.conf")
        self.connector = executableconnector.ExecutableConnector(self.cfg)

    def test_create_backup(self):
        """Test create backup."""
        proc = self.connector.run_backup_command('echo')
        (_stdoutdata, stderrdata) = proc.communicate(None)
        if proc.returncode != 0:
            print stderrdata
        self.assertEqual(proc.returncode, 0)

    def test_assemble_backup_command(self):
        """
        Test assemble_backup_command
        The tests are different if run within a Docker container,
        a development machine, or a generic Linux box,
        since the list of proc file systems is different.
        TODO: should mock the whole thing
        """
        is_darwin = subprocess.check_output('uname').strip() == 'Darwin'
        is_docker = os.path.exists('/.dockerenv')
        is_linux = subprocess.check_output('uname').strip() == 'Linux'
        if is_docker:
            # We are in the matrix
            cmd = self.connector.assemble_backup_command('/', '/dev')
            self.assertEquals(cmd, 'tar cpzf - --hard-dereference --exclude /dev --exclude /run --exclude /sys --exclude /proc --exclude /proc/bus --exclude /proc/fs --exclude /proc/irq --exclude /proc/sys --exclude /proc/sysrq-trigger /')
            cmd = self.connector.assemble_backup_command('/', '/proc,/dev')
            self.assertEquals(cmd, 'tar cpzf - --hard-dereference --exclude /proc --exclude /dev --exclude /run --exclude /sys --exclude /proc --exclude /proc/bus --exclude /proc/fs --exclude /proc/irq --exclude /proc/sys --exclude /proc/sysrq-trigger /')
            cmd = self.connector.assemble_backup_command('/', '/foo,/bar')
            self.assertEquals(cmd, 'tar cpzf - --hard-dereference --exclude /foo --exclude /bar --exclude /dev --exclude /run --exclude /sys --exclude /proc --exclude /proc/bus --exclude /proc/fs --exclude /proc/irq --exclude /proc/sys --exclude /proc/sysrq-trigger /')
        elif is_darwin:
            # We are on a Mac dev machine
            cmd = self.connector.assemble_backup_command('/', '/dev')
            self.assertEquals(cmd, 'tar cpzf - --hard-dereference --exclude /dev --exclude /run --exclude /sys /')
            cmd = self.connector.assemble_backup_command('/', '/proc,/dev')
            self.assertEquals(cmd, 'tar cpzf - --hard-dereference --exclude /proc --exclude /dev --exclude /run --exclude /sys /')
            cmd = self.connector.assemble_backup_command('/', '/foo,/bar')
            self.assertEquals(cmd, 'tar cpzf - --hard-dereference --exclude /foo --exclude /bar --exclude /dev --exclude /run --exclude /sys /')
        elif is_linux:
            # We are on a generic Linux machine, e.g. VSTS build agent
            cmd = self.connector.assemble_backup_command('/', '/dev')
            self.assertEquals(cmd, 'tar cpzf - --hard-dereference --exclude /dev --exclude /run --exclude /sys --exclude /proc /')
            cmd = self.connector.assemble_backup_command('/', '/proc,/dev')
            self.assertEquals(cmd, 'tar cpzf - --hard-dereference --exclude /proc --exclude /dev --exclude /run --exclude /sys --exclude /proc /')
            cmd = self.connector.assemble_backup_command('/', '/foo,/bar')
            self.assertEquals(cmd, 'tar cpzf - --hard-dereference --exclude /foo --exclude /bar --exclude /dev --exclude /run --exclude /sys --exclude /proc /')

if __name__ == '__main__':
    unittest.main()
