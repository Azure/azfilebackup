    # coding=utf-8

from azure.storage.blob import BlockBlobService
from msrestazure.azure_active_directory import MSIAuthentication
from .azurevminstancemetadata import AzureVMInstanceMetadata
from .backupconfigurationfile import BackupConfigurationFile
from .businesshours import BusinessHours
from .scheduleparser import ScheduleParser
from .backupexception import BackupException

class BackupConfiguration:
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
        # This dict contains function callbacks (lambdas) to return the value based on the current value
        #
        self.data = {
            "commandline": lambda: self.cfg_file_value("commandline"),
            "local_temp_directory": lambda: self.cfg_file_value("local_temp_directory"),
            "azure.storage.account_name": lambda: self.cfg_file_value("azure.storage.account_name"),
            "azure.storage.container_name": lambda: self.cfg_file_value("azure.storage.container_name"),

            "vm_name": lambda: self.instance_metadata.vm_name,
            "subscription_id": lambda: self.instance_metadata.subscription_id,
            "resource_group_name": lambda: self.instance_metadata.resource_group_name,
            "location": lambda: self.instance_metadata.location,

            "fs_backup_interval_min": lambda: ScheduleParser.parse_timedelta(self.instance_metadata_tag_value("fs_backup_interval_min")),
            "fs_backup_interval_max": lambda: ScheduleParser.parse_timedelta(self.instance_metadata_tag_value("fs_backup_interval_max")),

            #
            # Even though we read the file system backup business hours, we use the db_ prefix instead of fs_ prefix, 
            # because number of Azure Tags is limited. 
            #
            "backup.businesshours": lambda: BusinessHours(self.instance_metadata.get_tags(), prefix="db_backup_window")
        }
    
    def cfg_file_value(self, name):
        try:
            return self.cfg_file.get_value(name)
        except Exception:
            raise(BackupException("Cannot read value {} from config file '{}'".format(name, self.cfg_file.filename)))

    def instance_metadata_tag_value(self, name):
        try:
            return self.instance_metadata.get_tags()[name]
        except Exception:
            raise(BackupException("Cannot read value {} from VM's tag configuration".format(name)))

    def get_value(self, key): return self.data[key]()
    def get_vm_name(self): return self.get_value("vm_name")
    def get_subscription_id(self): return self.get_value("subscription_id")
    def get_resource_group_name(self): return self.get_value("resource_group_name")
    def get_location(self): return self.get_value("location")

    def get_commandline(self, configuration_name): return self.cfg_file_value("commandline.{}".format(configuration_name))
    def get_fs_backup_interval_min(self): return self.get_value("fs_backup_interval_min")
    def get_fs_backup_interval_max(self): return self.get_value("fs_backup_interval_max")
    def get_business_hours(self): return self.get_value("backup.businesshours")
    def get_standard_local_directory(self): return self.get_value("local_temp_directory")

    def get_azure_storage_account_name(self): return self.get_value("azure.storage.account_name")

    @property 
    def azure_storage_container_name(self): return self.get_value("azure.storage.container_name")

    @property
    def storage_client(self):
        if not self._block_blob_service:
            account_name=self.get_azure_storage_account_name()
            # 
            # Use the Azure Managed Service Identity ('MSI') to fetch an Azure AD token to talk to Azure Storage (PREVIEW!!!)
            # 
            token_credential = MSIAuthentication(
                resource='https://{account_name}.blob.core.windows.net'.format(account_name=account_name))
            self._block_blob_service = BlockBlobService(
                account_name=account_name, 
                token_credential=token_credential)
            _created = self._block_blob_service.create_container(
                container_name=self.azure_storage_container_name)
        return self._block_blob_service
