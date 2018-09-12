"""Unit tests for backupconfiguration."""
import os
import json
import unittest
from mock import patch
from azfilebak.backupconfiguration import BackupConfiguration
from azfilebak.azurevminstancemetadata import AzureVMInstanceMetadata

class TestBackupConfiguration(unittest.TestCase):
    """Unit tests for class BackupConfiguration."""

    def setUp(self):
        self.json_meta = '{ "compute": { "subscriptionId": "724467b5-bee4-484b-bf13-d6a5505d2b51", \
        "resourceGroupName": "backuptest", "name": "somevm", \
        "tags":"fs_backup_interval_min:24h;fs_backup_interval_max:3d;\
        db_backup_window_1:111111 111000 000000 011111;\
        db_backup_window_2:111111 111000 000000 011111;\
        db_backup_window_3:111111 111000 000000 011111;\
        db_backup_window_4:111111 111000 000000 011111;\
        db_backup_window_5:111111 111000 000000 011111;\
        db_backup_window_6:111111 111111 111111 111111;\
        db_backup_window_7:111111 111111 111111 111111" } }'

        self.meta = AzureVMInstanceMetadata(
            lambda: (json.JSONDecoder()).decode(self.json_meta)
        )

        self.patcher1 = patch('azfilebak.azurevminstancemetadata.AzureVMInstanceMetadata.create_instance',
                              return_value=self.meta)
        self.patcher1.start()

        self.cfg = BackupConfiguration(config_filename="config.txt")

    def test_cfg_file_value(self):
        """test cfg_file_value"""
        self.assertEqual(self.cfg.cfg_file_value('local_temp_directory'), '/tmp')

    def test_get_value(self):
        """test get_value"""
        self.assertEqual(self.cfg.get_value('vm_name'), 'somevm')

    def test_get_vm_name(self):
        """test get_vm_name"""
        self.assertEqual(self.cfg.get_vm_name(), 'somevm')

    def test_get_subscription_id(self):
        """test get_subscription_id"""
        self.assertEqual(self.cfg.get_subscription_id(), '724467b5-bee4-484b-bf13-d6a5505d2b51')

    def test_get_azure_storage_account_name(self):
        """test get_azure_storage_account_name"""
        self.assertEqual(self.cfg.get_azure_storage_account_name(), 'testhecbackup')

    def test_get_backup_command(self):
        """test get_commandline"""
        self.assertEqual(self.cfg.get_backup_command('tmp_dir'), 'tar cvzf - /tmp --ignore-failed-read')
        self.assertEqual(self.cfg.get_backup_command('osdisk'),
                         'tar cvzf - / --exclude=/dev --exclude=/proc --exclude=/run --exclude=/sys')

    def test_get_restore_command(self):
        """test get_commandline"""
        self.assertEqual(self.cfg.get_restore_command('tmp_dir'), 'tar xvzf -')
        self.assertEqual(self.cfg.get_restore_command('osdisk'), 'tar xvzf -')

    def test_get_filesets(self):
        """test get_filesets"""
        filesets = self.cfg.get_filesets()
        self.assertIn('tmp_dir', filesets)
        self.assertIn('osdisk', filesets)
        self.assertIn('testecho', filesets)

    def test_storage_client(self):
        """test storage_client"""
        if not os.environ.has_key('STORAGE_KEY'):
            return True
        client = self.cfg.storage_client
        self.assertEqual(client.protocol, 'https')

    def tearDown(self):
        self.patcher1.stop()

if __name__ == '__main__':
    unittest.main()
