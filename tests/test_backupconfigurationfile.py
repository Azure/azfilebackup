import unittest
from azfilebak import backupconfigurationfile

class TestBackupConfigurationFile(unittest.TestCase):

    def test_read_key_value_file(self):
        values = backupconfigurationfile.BackupConfigurationFile.read_key_value_file(filename="config.txt.template")
        self.assertEqual(values['local_temp_directory'], '/tmp')
        self.assertEqual(values['azure.storage.account_name'], 'sadjfksjlahfkj')
        self.assertEqual(values['azure.storage.container_name'], 'immutab')

    def test_get_value(self):
        config = backupconfigurationfile.BackupConfigurationFile(filename="config.txt.template")
        self.assertEqual(config.get_value('local_temp_directory'), '/tmp')
        self.assertEqual(config.get_value('azure.storage.account_name'), 'sadjfksjlahfkj')
        self.assertEqual(config.get_value('azure.storage.container_name'), 'immutab')

if __name__ == '__main__':
    unittest.main()
