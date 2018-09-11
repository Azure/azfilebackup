"""Unit tests for backupconfigurationfile."""
import unittest
from azfilebak import backupconfigurationfile

class TestBackupConfigurationFile(unittest.TestCase):
    """Unit tests for class BackupConfigurationFile."""

    def test_read_key_value_file(self):
        """test read_key_value_file"""
        values = backupconfigurationfile.BackupConfigurationFile.read_key_value_file(
            filename="config.txt")
        self.assertEqual(values['local_temp_directory'], '/tmp')
        self.assertEqual(values['azure.storage.account_name'], 'testhecbackup')
        self.assertEqual(values['azure.storage.container_name'], 'immutab')

    def test_get_value(self):
        """test get_value"""
        config = backupconfigurationfile.BackupConfigurationFile(filename="config.txt")
        self.assertEqual(config.get_value('local_temp_directory'), '/tmp')
        self.assertEqual(config.get_value('azure.storage.account_name'), 'testhecbackup')
        self.assertEqual(config.get_value('azure.storage.container_name'), 'immutab')

if __name__ == '__main__':
    unittest.main()
