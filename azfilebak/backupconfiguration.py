# coding=utf-8
"""BackupConfiguration module."""

import os
from azure.storage.blob import BlockBlobService
from msrestazure.azure_active_directory import MSIAuthentication
from .azurevminstancemetadata import AzureVMInstanceMetadata
from .backupconfigurationfile import BackupConfigurationFile
from .businesshours import BusinessHours
from .scheduleparser import ScheduleParser
from .backupexception import BackupException

class BackupConfiguration(object):
    """Access configuration values."""

    def __init__(self, config_filename):
        """
        >>> cfg = BackupConfiguration(config_filename="config.txt")
        >>> cfg.get_value("sap.CID")
        'ABC'
        >>> cfg.get_db_backup_interval_min()
        datetime.timedelta(1)
        >>> some_tuesday_evening = "20180605_215959"
        >>> cfg.get_business_hours().is_backup_allowed_time(some_tuesday_evening)
        True
        """
        self.cfg_file = BackupConfigurationFile(filename=config_filename)
        self.instance_metadata = AzureVMInstanceMetadata.create_instance()
        self._block_blob_service = None

        #
        # This dict contains function callbacks (lambdas) that
        # return the value based on the current value
        #
        self.data = {
            "commandline": lambda: self.cfg_file_value("commandline"),
            "local_temp_directory": lambda: self.cfg_file_value("local_temp_directory"),
            "azure.storage.account_name": lambda: self.cfg_file_value("azure.storage.account_name"),
            "azure.storage.container_name":
                lambda: self.cfg_file_value("azure.storage.container_name"),

            "vm_name": lambda: self.instance_metadata.vm_name,
            "subscription_id": lambda: self.instance_metadata.subscription_id,
            "resource_group_name": lambda: self.instance_metadata.resource_group_name,
            "location": lambda: self.instance_metadata.location,

            "fs_backup_interval_min": lambda: ScheduleParser.parse_timedelta(
                self.instance_metadata_tag_value("fs_backup_interval_min")
            ),
            "fs_backup_interval_max": lambda: ScheduleParser.parse_timedelta(
                self.instance_metadata_tag_value("fs_backup_interval_max")
            ),

            #
            # Even though we read the file system backup business hours, we use the db_ prefix
            # instead of fs_ prefix, because number of Azure Tags is limited.
            #
            "backup.businesshours": lambda: BusinessHours(
                self.instance_metadata.get_tags(), prefix="db_backup_window"
            )
        }

    def cfg_file_value(self, name):
        """Get value from configuration."""
        try:
            return self.cfg_file.get_value(name)
        except Exception:
            raise BackupException("Cannot read value {} from config file '{}'".format(
                name, self.cfg_file.filename
            ))

    def instance_metadata_tag_value(self, name):
        """Get instance metadata tag."""
        try:
            return self.instance_metadata.get_tags()[name]
        except Exception:
            raise BackupException("Cannot read value {} from VM's tag configuration".format(name))

    def get_value(self, key):
        """Get value by name."""
        return self.data[key]()

    def get_vm_name(self):
        """Get VM name."""
        return self.get_value("vm_name")

    def get_subscription_id(self):
        """Get Azure Subscription ID"""
        return self.get_value("subscription_id")

    def get_resource_group_name(self):
        """Get Resource Group name."""
        return self.get_value("resource_group_name")

    def get_location(self):
        """Get location."""
        return self.get_value("location")

    def get_backup_command(self, configuration_name):
        """Get backup command line for given fileset."""
        return self.cfg_file_value("command.backup.{}".format(configuration_name))

    def get_restore_command(self, configuration_name):
        """Get restore command line for given fileset."""
        return self.cfg_file_value("command.restore.{}".format(configuration_name))

    def get_fs_backup_interval_min(self):
        """Get minimum backup interval."""
        return self.get_value("fs_backup_interval_min")

    def get_fs_backup_interval_max(self):
        """Get maximum backup interval."""
        return self.get_value("fs_backup_interval_max")

    def get_business_hours(self):
        """Get business hours."""
        return self.get_value("backup.businesshours")

    def get_standard_local_directory(self):
        """Get temporary directory."""
        return self.get_value("local_temp_directory")

    def get_azure_storage_account_name(self):
        """Get storage account name."""
        return self.get_value("azure.storage.account_name")

    def get_filesets(self):
        """Return a list of filesets, extrapolated from the command.backup configuration values."""
        config_keys = self.cfg_file.get_keys_prefix('command.backup')
        return [k.replace('command.backup.', '') for k in config_keys]

    @property
    def azure_storage_container_name(self):
        """Get storage container name."""
        return self.get_value("azure.storage.container_name")

    @property
    def storage_client(self):
        """Create or return BlockBlobService client."""
        if not self._block_blob_service:
            account_name = self.get_azure_storage_account_name()
            if os.environ.has_key('STORAGE_KEY'):
                # We got the storage key through an environment variable
                # (mostly for testing purposes)
                self._block_blob_service = BlockBlobService(
                    account_name=account_name,
                    account_key=os.environ['STORAGE_KEY'])
            else:
                #
                # Use the Azure Managed Service Identity ('MSI') to fetch an
                # Azure AD token to talk to Azure Storage (PREVIEW!!!)
                #
                token_credential = MSIAuthentication(
                    resource='https://{account_name}.blob.core.windows.net'.format(
                        account_name=account_name))
                self._block_blob_service = BlockBlobService(
                    account_name=account_name,
                    token_credential=token_credential)

            _created = self._block_blob_service.create_container(
                container_name=self.azure_storage_container_name)

        return self._block_blob_service
