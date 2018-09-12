"""Unit tests for backupagent."""
import json
import unittest
from mock import patch
from azfilebak import backupconfiguration
from azfilebak import backupagent
from azfilebak.azurevminstancemetadata import AzureVMInstanceMetadata

class TestBackupAgent(unittest.TestCase):
    """Unit tests for class BackupAgent."""

    def setUp(self):
        self.json_meta = (
            '{ "compute": { "subscriptionId": "724467b5-bee4-484b-bf13-d6a5505d2b51",'
            '"resourceGroupName": "backuptest", "name": "somevm",'
            '"tags":"fs_backup_interval_min:24h;fs_backup_interval_max:3d;'
            'db_backup_window_1:111111 111000 000000 011111;'
            'db_backup_window_2:111111 111000 000000 011111;'
            'db_backup_window_3:111111 111000 000000 011111;'
            'db_backup_window_4:111111 111000 000000 011111;'
            'db_backup_window_5:111111 111000 000000 011111;'
            'db_backup_window_6:111111 111111 111111 111111;'
            'db_backup_window_7:111111 111111 111111 111111" } }'
        )

        self.meta = AzureVMInstanceMetadata(
            lambda: (json.JSONDecoder()).decode(self.json_meta)
        )

        self.patcher1 = patch('azfilebak.azurevminstancemetadata.AzureVMInstanceMetadata.create_instance',
                              return_value=self.meta)
        self.patcher1.start()

        self.cfg = backupconfiguration.BackupConfiguration(config_filename="config.txt")
        self.agent = backupagent.BackupAgent(self.cfg)

    def test_backup_single_fileset(self):
        """Test backup single fileset."""
        # Force a full backup
        blob = self.agent.backup_single_fileset('tmp_dir', True, True)
        # Check the blob exists
        container = self.cfg.azure_storage_container_name
        self.assertTrue(self.cfg.storage_client.exists(container, blob))

    def tearDown(self):
        self.patcher1.stop()

if __name__ == '__main__':
    unittest.main()
