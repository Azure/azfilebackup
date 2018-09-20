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
        proc = self.connector.create_backup(fileset='tmp_dir', is_full=True)
        (stdoutdata, stderrdata) = proc.communicate(None)
        if proc.returncode != 0:
            print stderrdata
        self.assertEqual(proc.returncode, 0)

if __name__ == '__main__':
    unittest.main()
