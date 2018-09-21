"""Unit tests for executableconnector."""
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
        """Test assemble_backup_command"""
        cmd = self.connector.assemble_backup_command('/', '/proc')
        self.assertEquals(cmd, 'tar cpzf - --hard-dereference --exclude /proc /')
        cmd = self.connector.assemble_backup_command('/', '/proc,/dev')
        self.assertEquals(cmd, 'tar cpzf - --hard-dereference --exclude /proc --exclude /dev /')

if __name__ == '__main__':
    unittest.main()
