"""Unit tests for executableconnector."""
import unittest
from azfilebak import backupconfiguration
from azfilebak import executableconnector

class TestExecutableConnector(unittest.TestCase):
    """Unit tests for class ExecutableConnector."""

    def setUp(self):
        self.cfg = backupconfiguration.BackupConfiguration(config_filename="config.txt")
        self.connector = executableconnector.ExecutableConnector(self.cfg)

    def test_create_backup(self):
        """Test create backup."""
        proc = self.connector.create_backup(fileset='tmp_dir', is_full=True)
        proc.wait()
        if proc.returncode != 0:
            print proc.stderr.read()
        self.assertEqual(proc.returncode, 0)

if __name__ == '__main__':
    unittest.main()
