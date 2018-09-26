"""Unit tests for executableconnector."""
import os
import unittest
from azfilebak import backupconfiguration
from azfilebak import executableconnector

class TestExecutableConnector(unittest.TestCase):
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
        since the list of proc file systems is different.
        """
        if os.path.exists('/.dockerenv'):
            # We are in the matrix
            cmd = self.connector.assemble_backup_command('/', '/dev')
            self.assertEquals(cmd, 'tar cpzf - --hard-dereference --exclude /dev --exclude /run --exclude /sys --exclude /proc --exclude /proc/bus --exclude /proc/fs --exclude /proc/irq --exclude /proc/sys --exclude /proc/sysrq-trigger /')
            cmd = self.connector.assemble_backup_command('/', '/proc,/dev')
            self.assertEquals(cmd, 'tar cpzf - --hard-dereference --exclude /proc --exclude /dev --exclude /run --exclude /sys --exclude /proc --exclude /proc/bus --exclude /proc/fs --exclude /proc/irq --exclude /proc/sys --exclude /proc/sysrq-trigger /')
            cmd = self.connector.assemble_backup_command('/', '/foo,/bar')
            self.assertEquals(cmd, 'tar cpzf - --hard-dereference --exclude /foo --exclude /bar --exclude /dev --exclude /run --exclude /sys --exclude /proc --exclude /proc/bus --exclude /proc/fs --exclude /proc/irq --exclude /proc/sys --exclude /proc/sysrq-trigger /')
        else:
            # We are on a dev machine
            cmd = self.connector.assemble_backup_command('/', '/dev')
            self.assertEquals(cmd, 'tar cpzf - --hard-dereference --exclude /dev --exclude /run --exclude /sys /')
            cmd = self.connector.assemble_backup_command('/', '/proc,/dev')
            self.assertEquals(cmd, 'tar cpzf - --hard-dereference --exclude /proc --exclude /dev --exclude /run --exclude /sys /')
            cmd = self.connector.assemble_backup_command('/', '/foo,/bar')
            self.assertEquals(cmd, 'tar cpzf - --hard-dereference --exclude /foo --exclude /bar --exclude /dev --exclude /run --exclude /sys /')

if __name__ == '__main__':
    unittest.main()
