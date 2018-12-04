# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
# --------------------------------------------------------------------------

"""Unit tests for azurevminstancemetadata."""

import json
import unittest
from mock import patch
from azfilebak.azurevminstancemetadata import AzureVMInstanceMetadata
from tests.loggedtestcase import LoggedTestCase

class TestAzureVMInstanceMetadata(LoggedTestCase):
    """Unit tests for class AzureVMInstanceMetadata."""

    def setUp(self):
        self.json_meta_with_tags = open('sample_instance_metadata.json').read()

        self.patcher1 = patch('azfilebak.azurevminstancemetadata.AzureVMInstanceMetadata.request_metadata',
                              return_value=(json.JSONDecoder()).decode(self.json_meta_with_tags))
        self.patcher1.start()

        self.meta = AzureVMInstanceMetadata.create_instance()

    def test_get_tags(self):
        """test get_tags with empty tags property"""
        tags = self.meta.get_tags()
        self.assertEqual(len(tags), 6)
        self.assertEqual(tags['Name'], 'hec42v106014')

    def test_vm_name(self):
        """test vm_name"""
        self.assertEqual(self.meta.vm_name, 'hec99v106014')

    def test_subscription_id(self):
        """test subscription_id"""
        self.assertEqual(self.meta.subscription_id, '2e394ee6-2714-4080-88c3-ecfc33d85147')

    def test_resource_group_name(self):
        """test resource_group_name"""
        self.assertEqual(self.meta.resource_group_name, 'HEC42-AZ1-westeurope-1')

    def test_location(self):
        """test location"""
        self.assertEqual(self.meta.location, 'westeurope')

    def tearDown(self):
        self.patcher1.stop()

if __name__ == '__main__':
    unittest.main()
