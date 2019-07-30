# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
# --------------------------------------------------------------------------

"""Unit tests for backupagent."""

import json
import unittest
from mock import patch
from azfilebak import backupconfiguration
from azfilebak import backupagent
from azfilebak.azurevminstancemetadata import AzureVMInstanceMetadata
from azfilebak.businesshours import BusinessHours
from azfilebak.scheduleparser import ScheduleParser
from azfilebak.naming import Naming
from tests.loggedtestcase import LoggedTestCase
from azfilebak.backupexception import BackupException

class TestBackupAgent(LoggedTestCase):
    """Unit tests for class BackupAgent."""

    def setUp(self):
        self.json_meta = open('sample_instance_metadata.json').read()

        self.meta = AzureVMInstanceMetadata(
            lambda: (json.JSONDecoder()).decode(self.json_meta)
        )

        self.patcher1 = patch('azfilebak.azurevminstancemetadata.AzureVMInstanceMetadata.create_instance',
                              return_value=self.meta)
        self.patcher1.start()

        # Mock `dmidecode` execution
        self.patcher2 = patch('subprocess.check_output',
                              return_value='UUID000')
        self.patcher2.start()

        self.cfg = backupconfiguration.BackupConfiguration(config_filename="sample_backup.conf")
        self.agent = backupagent.BackupAgent(self.cfg)

    def test_should_run_full_backup(self):
        """Test should_run_full_backup"""
        sample_data = (
            "bkp_fs_schedule:"
            "mo:111111 111000 000000 011111, "
            "tu:111111 111000 000000 011111, "
            "we:111111 111000 000000 011111, "
            "th:111111 111000 000000 011111, "
            "fr:111111 111000 000000 011111, "
            "sa:111111 111111 111111 111111, "
            "su:111111 111111 111111 111111, "
            "min:1d, "
            "max:3d"
        )

        business_hours = BusinessHours.parse_tag_str(sample_data)
        db_backup_interval_min = ScheduleParser.parse_timedelta("24h")
        db_backup_interval_max = ScheduleParser.parse_timedelta("3d")
        five_day_backup = "20180601_010000"
        two_day_backup = "20180604_010000"
        same_day_backup = "20180606_010000"
        during_business_hours = "20180606_150000"
        outside_business_hours = "20180606_220000"

        # Forced
        self.assertTrue(self.agent.should_run_full_backup(
            now_time=during_business_hours, force=True,
            latest_full_backup_timestamp=same_day_backup,
            business_hours=business_hours,
            db_backup_interval_min=db_backup_interval_min,
            db_backup_interval_max=db_backup_interval_max))
        # Forced
        self.assertTrue(self.agent.should_run_full_backup(
            now_time=during_business_hours, force=True,
            latest_full_backup_timestamp=two_day_backup,
            business_hours=business_hours,
            db_backup_interval_min=db_backup_interval_min,
            db_backup_interval_max=db_backup_interval_max))
        # Forced
        self.assertTrue(self.agent.should_run_full_backup(
            now_time=during_business_hours, force=True,
            latest_full_backup_timestamp=five_day_backup,
            business_hours=business_hours,
            db_backup_interval_min=db_backup_interval_min,
            db_backup_interval_max=db_backup_interval_max))
        # respecting business hours, and not needed anyway
        self.assertFalse(self.agent.should_run_full_backup(
            now_time=during_business_hours, force=False,
            latest_full_backup_timestamp=same_day_backup,
            business_hours=business_hours,
            db_backup_interval_min=db_backup_interval_min,
            db_backup_interval_max=db_backup_interval_max))
        # respecting business hours
        self.assertFalse(self.agent.should_run_full_backup(
            now_time=during_business_hours, force=False,
            latest_full_backup_timestamp=two_day_backup,
            business_hours=business_hours,
            db_backup_interval_min=db_backup_interval_min,
            db_backup_interval_max=db_backup_interval_max))
        # a really old backup, so we ignore business hours
        self.assertTrue(self.agent.should_run_full_backup(
            now_time=during_business_hours, force=False,
            latest_full_backup_timestamp=five_day_backup,
            business_hours=business_hours,
            db_backup_interval_min=db_backup_interval_min,
            db_backup_interval_max=db_backup_interval_max))
        # outside_business_hours, but same_day_backup, so no backup
        self.assertFalse(self.agent.should_run_full_backup(
            now_time=outside_business_hours, force=False,
            latest_full_backup_timestamp=same_day_backup,
            business_hours=business_hours,
            db_backup_interval_min=db_backup_interval_min,
            db_backup_interval_max=db_backup_interval_max))
        # outside_business_hours and need to backup
        self.assertTrue(self.agent.should_run_full_backup(
            now_time=outside_business_hours, force=False,
            latest_full_backup_timestamp=two_day_backup,
            business_hours=business_hours,
            db_backup_interval_min=db_backup_interval_min,
            db_backup_interval_max=db_backup_interval_max))
        # a really old backup
        self.assertTrue(self.agent.should_run_full_backup(
            now_time=outside_business_hours, force=False,
            latest_full_backup_timestamp=five_day_backup,
            business_hours=business_hours,
            db_backup_interval_min=db_backup_interval_min,
            db_backup_interval_max=db_backup_interval_max))
        # Forced
        self.assertTrue(self.agent.should_run_full_backup(
            now_time=outside_business_hours, force=True,
            latest_full_backup_timestamp=same_day_backup,
            business_hours=business_hours,
            db_backup_interval_min=db_backup_interval_min,
            db_backup_interval_max=db_backup_interval_max))
        # Forced
        self.assertTrue(self.agent.should_run_full_backup(
            now_time=outside_business_hours, force=True,
            latest_full_backup_timestamp=two_day_backup,
            business_hours=business_hours,
            db_backup_interval_min=db_backup_interval_min,
            db_backup_interval_max=db_backup_interval_max))
        # Forced
        self.assertTrue(self.agent.should_run_full_backup(
            now_time=outside_business_hours, force=True,
            latest_full_backup_timestamp=five_day_backup,
            business_hours=business_hours,
            db_backup_interval_min=db_backup_interval_min,
            db_backup_interval_max=db_backup_interval_max))

    def test_backup_single_fileset(self):
        """Test backup single fileset."""
        # Force a full backup
        blob = self.agent.backup_single_fileset('tmpdir', True, True)
        # Check the blob exists
        container = self.cfg.azure_storage_container_name
        self.assertTrue(self.cfg.storage_client.exists(container, blob))

    def test_backup_single_fileset_tar_fail(self):
        """Test backup single fileset + tar failure."""
        self.assertRaises(BackupException, self.agent.backup_single_fileset, 'tarfail', True, True)

    def test_existing_backups(self):
        """Test list of existing backups."""
        # This test assumes that there are some existing backups for tmpdir
        # and for no other fileset.
        backups_one = self.agent.existing_backups(['tmpdir'])
        self.assertGreater(len(backups_one), 0)
        backups_all = self.agent.existing_backups([])
        self.assertGreater(len(backups_all), 0)
        # Non-existing fileset
        backups_none = self.agent.existing_backups(['XXX'])
        self.assertEquals(len(backups_none), 0)

    def test_list_backups(self):
        """Test list of existing backups."""
        # This test assumes that there are some existing backups for tmpdir
        # and for no other fileset.
        self.agent.list_backups(['tmpdir'])
        self.agent.list_backups([])
        # Also test overriding container name
        container = self.cfg.azure_storage_container_name
        self.agent.list_backups(['tmpdir'], container)
        # Non-existing fileset
        self.agent.list_backups(['XXX'])
        return True

    def test_restore_single_fileset(self):
        """Test restoring a single fileset."""
        # We should have a backup from the preceding test cases.
        backups = self.agent.existing_backups_for_fileset('tmpdir', True)
        blob_name = backups.popitem()[1][0]
        (fileset, _is_full, timestamp, vmname) = Naming.parse_blobname(blob_name)
        self.agent.restore_single_fileset(fileset, timestamp, '/tmp')
        # TODO: test that expected files were indeed restored...
        return True

    def test_restore_blob(self):
        """Test restoring a blob."""
        # We should have a backup from the preceding test cases.
        backups = self.agent.existing_backups_for_fileset('tmpdir', True)
        blob_name = backups.popitem()[1][0]
        self.agent.restore_blob(blob_name, '/tmp')
        # TODO: test that expected files were indeed restored...
        return True

    def test_prune_old_backups(self):
        """Test prune_old_backups."""
        # Delete backups older than 7 days
        age = age = ScheduleParser.parse_timedelta('8d')
        self.agent.prune_old_backups(age, ['tmpdir'])
        # TODO: test the backup was effectively deleted
        return True

    def test_show_configuration(self):
        """Test show_configuration."""
        conf = self.agent.show_configuration('/tmp')
        print conf
        return True

    def test_backup_default(self):
        """Test backup_default."""
        self.agent.backup_default(True, True)
        return True

    def test_restore_default_fileset(self):
        """Test restoring the default fileset."""
        # We should have a backup from the preceding test cases.
        backups = self.agent.existing_backups_for_fileset('fs', True)
        blob_name = backups.popitem()[1][0]
        (fileset, _is_full, timestamp, vmname) = Naming.parse_blobname(blob_name)
        self.agent.restore(timestamp, '/tmp', [])
        # TODO: test that expected files were indeed restored...
        return True

    def test_restore_default_fileset_override_container(self):
        """Test restoring a single fileset and override the container name."""
        # We should have a backup from the preceding test cases.
        backups = self.agent.existing_backups_for_fileset('fs', True)
        blob_name = backups.popitem()[1][0]
        (fileset, _is_full, timestamp, vmname) = Naming.parse_blobname(blob_name)
        container = self.cfg.azure_storage_container_name
        self.agent.restore(timestamp, '/tmp', [], container=container)
        # TODO: test that expected files were indeed restored...
        return True

    def test_get_notification_message(self):
        """Test get_notification_message."""
        json_str = self.agent.get_notification_message(True, "20180601_112429", "20180601_112430", True, 42, '/container/blob.tar.gz', None)
        obj = json.loads(json_str)
        self.assertEqual(obj["system-id"], "AZ3")
        self.assertEqual(obj["hostname"], "hec99v106014")

    def tearDown(self):
        self.patcher1.stop()
        self.patcher2.stop()

if __name__ == '__main__':
    unittest.main()
