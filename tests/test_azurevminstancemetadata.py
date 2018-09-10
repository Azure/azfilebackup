"""Unit tests for azurevminstancemetadata."""
import json
import unittest
from mock import patch
from azfilebak.azurevminstancemetadata import AzureVMInstanceMetadata

class TestAzureVMInstanceMetadata(unittest.TestCase):
    """Unit tests for class AzureVMInstanceMetadata."""

    def setUp(self):
        self.json_meta = '{"compute":{"location":"westeurope","name":"test-backup","offer":"SLES","osType":"Linux",\
        "placementGroupId":"","platformFaultDomain":"0","platformUpdateDomain":"0","publisher":"SUSE",\
        "resourceGroupName":"test-backup","sku":"12-SP3","subscriptionId":"252281c3-8a06-4af8-8f3f-d6af13e4fde3",\
        "tags":"","version":"2018.09.04","vmId":"e247fed6-a198-4918-94cd-79203b1e744a",\
        "vmScaleSetName":"","vmSize":"Standard_DS1_v2","zone":""},\
        "network":{"interface":[{"ipv4":{"ipAddress":[{"privateIpAddress":"10.0.0.4",\
        "publicIpAddress":"40.113.100.99"}],"subnet":[{"address":"10.0.0.0","prefix":"24"}]},\
        "ipv6":{"ipAddress":[]},"macAddress":"000D3A4543C9"}]}}'

        self.json_meta_with_tags = '{"compute":{"location":"westeurope","name":"test-backup","offer":"SLES","osType":"Linux",\
        "placementGroupId":"","platformFaultDomain":"0","platformUpdateDomain":"0","publisher":"SUSE",\
        "resourceGroupName":"test-backup","sku":"12-SP3","subscriptionId":"252281c3-8a06-4af8-8f3f-d6af13e4fde3",\
        "tags":"fs_backup_interval_min:24h;fs_backup_interval_max:3d;\
        db_backup_window_1:111111 111000 000000 011111;\
        db_backup_window_2:111111 111000 000000 011111;\
        db_backup_window_3:111111 111000 000000 011111;\
        db_backup_window_4:111111 111000 000000 011111;\
        db_backup_window_5:111111 111000 000000 011111;\
        db_backup_window_6:111111 111111 111111 111111;\
        db_backup_window_7:111111 111111 111111 111111","version":"2018.09.04","vmId":"e247fed6-a198-4918-94cd-79203b1e744a",\
        "vmScaleSetName":"","vmSize":"Standard_DS1_v2","zone":""},\
        "network":{"interface":[{"ipv4":{"ipAddress":[{"privateIpAddress":"10.0.0.4",\
        "publicIpAddress":"40.113.100.99"}],"subnet":[{"address":"10.0.0.0","prefix":"24"}]},\
        "ipv6":{"ipAddress":[]},"macAddress":"000D3A4543C9"}]}}'

        self.patcher1 = patch('azfilebak.azurevminstancemetadata.AzureVMInstanceMetadata.request_metadata',
                              return_value=(json.JSONDecoder()).decode(self.json_meta_with_tags))
        self.patcher1.start()

        self.meta = AzureVMInstanceMetadata.create_instance()

    def test_get_tags(self):
        """test get_tags with empty tags property"""
        tags = self.meta.get_tags()
        self.assertEqual(len(tags), 9)
        self.assertEqual(tags['fs_backup_interval_min'], '24h')
        self.assertEqual(tags['fs_backup_interval_max'], '3d')

    def test_vm_name(self):
        """test vm_name"""
        self.assertEqual(self.meta.vm_name, 'test-backup')

    def test_subscription_id(self):
        """test subscription_id"""
        self.assertEqual(self.meta.subscription_id, '252281c3-8a06-4af8-8f3f-d6af13e4fde3')

    def test_resource_group_name(self):
        """test resource_group_name"""
        self.assertEqual(self.meta.resource_group_name, 'test-backup')

    def test_location(self):
        """test location"""
        self.assertEqual(self.meta.location, 'westeurope')

    def tearDown(self):
        self.patcher1.stop()

if __name__ == '__main__':
    unittest.main()
