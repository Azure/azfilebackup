# coding=utf-8

# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
# --------------------------------------------------------------------------

"""BackupConfiguration module."""

import os
import logging
import subprocess
from azure.storage.blob import BlockBlobService
from msrestazure.azure_active_directory import MSIAuthentication
from azfilebak.azurevminstancemetadata import AzureVMInstanceMetadata
from azfilebak.backupconfigurationfile import BackupConfigurationFile
from azfilebak.businesshours import BusinessHours
from azfilebak.scheduleparser import ScheduleParser
from azfilebak.backupexception import BackupException

DEFAULT_NOTIFICATION_COMMAND = "/usr/sbin/ticmcmc --stdin"

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

    # Retrieve config values from different sources: config file, environment, metadata

    def cfg_file_value(self, name):
        """Get value from configuration."""
        try:
            return self.cfg_file.get_value(name)
        except Exception:
            raise BackupException("Cannot read value {} from config file '{}'".format(
                name, self.cfg_file.filename
            ))

    def instance_metadata_tag_value(self, name):
        """Get value from instance metadata tag."""
        try:
            return self.instance_metadata.get_tags()[name]
        except Exception:
            raise BackupException("Cannot read value {} from VM's tag configuration".format(name))

    def environment_value(self, name):
        """Get value from OS environment variable."""
        if not os.environ.has_key(name):
            return None
        return os.environ[name]

    # These values come exclusively from instance metadata

    def get_vm_name(self):
        """Get VM name."""
        return self.instance_metadata.vm_name

    def get_subscription_id(self):
        """Get Azure Subscription ID"""
        return self.instance_metadata.subscription_id

    def get_resource_group_name(self):
        """Get Resource Group name."""
        return self.instance_metadata.resource_group_name

    def get_location(self):
        """Get location."""
        return self.instance_metadata.location

    def get_backup_command(self, configuration_name):
        """Get backup command line for given fileset."""
        return self.cfg_file_value("command.backup.{}".format(configuration_name))

    def get_restore_command(self, configuration_name):
        """Get restore command line for given fileset."""
        return self.cfg_file_value("command.restore.{}".format(configuration_name))

    def get_filesets(self):
        """Return a list of filesets, extrapolated from the command.backup configuration values."""
        config_keys = self.cfg_file.get_keys_prefix('command.backup')
        return [k.replace('command.backup.', '') for k in config_keys]

    # These values come from the instance metadata tags

    def get_fs_backup_interval_min(self):
        """Get minimum backup interval."""
        return ScheduleParser.parse_timedelta(
            BusinessHours(
                self.instance_metadata.get_tags()
            ).min
        )

    def get_fs_backup_interval_max(self):
        """Get maximum backup interval."""
        return ScheduleParser.parse_timedelta(
            BusinessHours(
                self.instance_metadata.get_tags()
            ).max
        )

    def get_business_hours(self):
        """Get business hours."""
        return BusinessHours(
            self.instance_metadata.get_tags()
        )

    # These values come from the configuration file

    def get_standard_local_directory(self):
        """Get temporary directory."""
        if self.cfg_file.key_exists('local_temp_directory'):
            return self.cfg_file_value("local_temp_directory")
        return None

    def get_default_fileset(self):
        """Get the default fileset."""
        dbtype = self.cfg_file_value("DEFAULT.dbtype").lower()
        if dbtype != 'ase' and dbtype != 'hana':
            return 'ci'
        return dbtype

    def get_fileset_sources(self, fileset):
        """Get fileset sources."""
        return self.cfg_file_value("fs.{}.sources".format(fileset))

    def get_fileset_exclude(self, fileset):
        """Get fileset sources."""
        return self.cfg_file_value("fs.{}.exclude".format(fileset))

    def get_notification_command(self):
        """Get notification command with fall back to default."""
        if self.cfg_file.key_exists('notification_command'):
            return self.cfg_file_value("notification_command")
        return DEFAULT_NOTIFICATION_COMMAND

    # These values are obtained from various system configuration or tools

    def get_system_uuid(self):
        """
        Try to get a Serial property from the instance metadata tags. If that fails,
        get system-uuid property from dmidecode, where Azure puts a unique VM identifier.
        """
        try:
            uuid = self.instance_metadata_tag_value('Serial')
        except BackupException:
            # TODO: this is system dependent, should check dmidecode exists and fall back
            uuid = subprocess.check_output(["sudo", "dmidecode", "--string", "system-uuid"])
        return uuid.strip()

    # These are should be computed unless they are
    # overloaded using config file or tag

    def get_azure_storage_account_name(self):
        """
        Get storage account name. It can be specified explicitly in a
        instance metadata tag, otherwise is assembled using configuration
        information.
        """
        try:
            account = self.instance_metadata_tag_value('bkp_storage_account')
            logging.debug("Using storage account name from instance metadata: %s", account)
        except BackupException:
            cid = self.cfg_file_value("DEFAULT.CID").lower()
            name = self.get_vm_name()[0:5]
            account = "sa{}{}backup0001".format(name, cid)
            logging.debug("No storage account in instance metadata, using generated: %s", account)
        return account

    @property
    def azure_storage_container_name(self):
        """
        Get storage container name. It can be specified explicitly in the
        configuration file, otherwise will default to the VM name.
        """
        if self.cfg_file.key_exists('azure.blob.container_name'):
            return self.cfg_file_value('azure.blob.container_name')
        return self.get_vm_name()

    # The storage client is exposed as a property of the configuration.

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

        return self._block_blob_service
