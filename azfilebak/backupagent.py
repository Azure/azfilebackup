# coding=utf-8

import sys
import logging
import os
import datetime
import json
import urllib2
import uuid
import time

from .naming import Naming
from .timing import Timing
from .executableconnector import ExecutableConnector

class BackupAgent(object):
    """
    The backup business logic implementation.
    """

    def __init__(self, backup_configuration):
        self.backup_configuration = backup_configuration
        self.executable_connector = ExecutableConnector(self.backup_configuration)

    #
    # Listing methods.
    #

    def existing_backups_for_fileset(self, fileset, is_full):
        """Retrieve list of existing backups for a single fileset."""
        existing_blobs_dict = dict()
        marker = None
        while True:
            results = self.backup_configuration.storage_client.list_blobs(
                container_name=self.backup_configuration.azure_storage_container_name,
                prefix=Naming.construct_blobname_prefix(fileset=fileset, is_full=is_full),
                marker=marker)
            for blob in results:
                blob_name = blob.name
                parts = Naming.parse_blobname(blob_name)
                if parts is None:
                    continue
                start_timestamp = parts[2]
                if not existing_blobs_dict.has_key(start_timestamp):
                    existing_blobs_dict[start_timestamp] = []
                existing_blobs_dict[start_timestamp].append(blob_name)
            if results.next_marker:
                marker = results.next_marker
            else:
                break
        return existing_blobs_dict

    def existing_backups(self, filesets=None):
        """Retrieve list of existing backups."""
        existing_blobs_list = list()
        marker = None

        results = list(
            self.backup_configuration.storage_client.list_blobs(
                container_name=self.backup_configuration.azure_storage_container_name,
                marker=marker)
        )

        results = sorted(results, key=lambda x: x.name)

        for blob in results:
            blob_name = blob.name
            parts = Naming.parse_blobname(blob_name)
            if parts is None:
                continue

            (fileset_of_existing_blob, _is_full, _start_timestamp) = parts

            if not filesets or fileset_of_existing_blob in filesets:
                existing_blobs_list.append(blob_name)

        return existing_blobs_list

    #
    # Scheduling methods.
    #

    def latest_backup_timestamp(self, fileset, is_full):
        """Return the timestamp for the latest backup for a given fileset."""
        existing_blobs_dict = self.existing_backups_for_fileset(fileset=fileset, is_full=is_full)
        if not existing_blobs_dict.keys():
            return "19000101_000000"
        return Timing.sort(existing_blobs_dict.keys())[-1:][0]

    @staticmethod
    def should_run_full_backup(now_time, force, latest_full_backup_timestamp,
                               business_hours, db_backup_interval_min, db_backup_interval_max):
        """
        Determine whether a backup should be executed.
        """
        allowed_by_business = business_hours.is_backup_allowed_time(now_time)
        age_of_latest_backup_in_storage = Timing.time_diff(latest_full_backup_timestamp, now_time)
        min_interval_allows_backup = age_of_latest_backup_in_storage > db_backup_interval_min
        max_interval_requires_backup = age_of_latest_backup_in_storage > db_backup_interval_max
        perform_full_backup = (
            allowed_by_business and min_interval_allows_backup
            or max_interval_requires_backup or force)

        # logging.info("Full backup requested. Current time: {now}. Last backup in storage: {last}. Age of backup {age}".format(now=now_time, last=latest_full_backup_timestamp, age=age_of_latest_backup_in_storage))
        # logging.info("Backup requirements: min=\"{min}\" max=\"{max}\"".format(min=db_backup_interval_min,max=db_backup_interval_max))
        # logging.info("Forced by user: {force}. Backup allowed by business hours: {allowed_by_business}. min_interval_allows_backup={min_interval_allows_backup}. max_interval_requires_backup={max_interval_requires_backup}".format(force=force, allowed_by_business=allowed_by_business, min_interval_allows_backup=min_interval_allows_backup, max_interval_requires_backup=max_interval_requires_backup))
        # logging.info("Decision to backup: {perform_full_backup}.".format(perform_full_backup=perform_full_backup))

        return perform_full_backup

    @staticmethod
    def should_run_tran_backup(now_time, force, latest_tran_backup_timestamp,
                               log_backup_interval_min):
        """Determine if a 'tran' backup can be performed according to backup window rules."""
        if force:
            return True

        age_of_latest_backup_in_storage = Timing.time_diff(latest_tran_backup_timestamp, now_time)
        min_interval_allows_backup = age_of_latest_backup_in_storage > log_backup_interval_min
        perform_tran_backup = min_interval_allows_backup
        return perform_tran_backup

    def should_run_backup(self, fileset, is_full, force, start_timestamp):
        """Determine if a backup can be performed according to backup window rules."""
        if is_full:
            result = BackupAgent.should_run_full_backup(
                now_time=start_timestamp,
                force=force,
                latest_full_backup_timestamp=self.latest_backup_timestamp(fileset=fileset, is_full=is_full),
                business_hours=self.backup_configuration.get_business_hours(),
                db_backup_interval_min=self.backup_configuration.get_fs_backup_interval_min(),
                db_backup_interval_max=self.backup_configuration.get_fs_backup_interval_max())
        else:
            result = BackupAgent.should_run_tran_backup(
                now_time=start_timestamp,
                force=force,
                latest_tran_backup_timestamp=self.latest_backup_timestamp(fileset=fileset, is_full=is_full),
                log_backup_interval_min=self.backup_configuration.get_log_backup_interval_min())

        return result

    #
    # Backup methods.
    #

    def backup(self, filesets, is_full, force):
        """Backup a list of filesets."""
        filesets_to_backup = filesets
        if not filesets_to_backup:
            # If not fileset specified, determine the default backup configuration
            self.backup_default(is_full, force)
        else:
            for fileset in filesets_to_backup:
                self.backup_single_fileset(fileset=fileset, is_full=is_full, force=force)

    def backup_default(self, is_full, force):
        """Determine default backup configuration."""
        fs = self.backup_configuration.get_default_fileset()
        logging.info("Backup request for default fileset: %s", fs)
        # Get the sources and exclude
        sources = self.backup_configuration.get_fileset_sources(fs)
        exclude = self.backup_configuration.get_fileset_exclude(fs)
        # Assemble the tar command
        command = self.executable_connector.assemble_backup_command(sources, exclude)
        # Run it
        self.backup_single_fileset(fs, is_full, force, command)
        return

    def backup_all_filesets(self, is_full, force):
        """Backup all the filesets."""
        filesets_to_backup = self.backup_configuration.get_filesets()
        for fileset in filesets_to_backup:
            self.backup_single_fileset(fileset=fileset, is_full=is_full, force=force)

    def backup_single_fileset(self, fileset, is_full, force, command=None):
        """
        Backup a single fileset using the specified command.
        If no command is provided, it will be looked up in the config file.
        """
        logging.info("Backup request for fileset: %s", fileset)

        # Determine if backup can run according to schedule
        start_timestamp = Timing.now_localtime()
        if not self.should_run_backup(
                fileset=fileset, is_full=is_full,
                force=force, start_timestamp=start_timestamp):
            logging.warn("Skipping backup of fileset %s", fileset)
            return

        # Final destination container 
        dest_container_name = self.backup_configuration.azure_storage_container_name
        # Name of the backup blob
        blob_name = Naming.construct_blobname(fileset=fileset,
                                              is_full=is_full,
                                              start_timestamp=start_timestamp)

        # Command to run to execute the backup
        if not command:
            command = self.backup_configuration.get_backup_command(fileset)

        try:
            # Run the backup command
            proc = self.executable_connector.run_backup_command(
                command)

            logging.info(
                "Streaming backup to blob: %s in container: %s",
                blob_name, dest_container_name)

            # Stream backup command stdout to the blob
            storage_client = self.backup_configuration.storage_client
            storage_client.create_blob_from_stream(
                container_name=dest_container_name,
                blob_name=blob_name, stream=proc.stdout,
                use_byte_buffer=True, max_connections=1)

            # Wait for the command to terminate
            proc.wait()
        except Exception as ex:
            logging.error("Failed to stream blob: %s", ex.message)
            raise ex

        logging.info("Finished streaming blob: %s", blob_name)

        # Return name of new blob
        return blob_name

    #
    # List methods.
    #

    def list_backups(self, filesets=None):
        """Print a list of existing backups."""
        baks_list = self.existing_backups(filesets=filesets or [])
        for blobname in baks_list:
            #filename = Naming.blobname_to_filename(blobname)
            parts = Naming.parse_blobname(blobname)
            print "fs {fileset: <30} start {begin} ({type})".format(
                fileset=parts[0],
                begin=parts[2],
                type=Naming.backup_type_str(parts[1]))

    #
    # Prune methods.
    #

    def prune_old_backups(self, older_than, filesets):
        """
        Delete (prune) old backups from Azure storage.
        """
        minimum_deletable_age = datetime.timedelta(7, 0)
        logging.warn("Deleting files older than %s", older_than)
        if older_than < minimum_deletable_age:
            msg = "Will not delete files younger than {}, ignoring".format(minimum_deletable_age)
            logging.warn(msg)
            return

        marker = None
        while True:
            results = self.backup_configuration.storage_client.list_blobs(
                container_name=self.backup_configuration.azure_storage_container_name,
                marker=marker)
            for blob in results:
                parts = Naming.parse_blobname(blob.name)
                if parts is None:
                    continue

                (fileset, _is_full, start_timestamp) = parts
                if (fileset != None) and not fileset in filesets:
                    continue

                diff = Timing.time_diff(start_timestamp, Timing.now_localtime())
                delete = diff > older_than

                if delete:
                    logging.warn("Deleting %s", blob.name)
                    self.backup_configuration.storage_client.delete_blob(
                        container_name=self.backup_configuration.azure_storage_container_name,
                        blob_name=blob.name)
                else:
                    logging.warn("Keeping %s", blob.name)

            if results.next_marker:
                marker = results.next_marker
            else:
                break

    #
    # Restore methods.
    #

    def restore(self, restore_point, output_dir, filesets, stream=False):
        """ Restore backups."""
        logging.info("Retriving point-in-time restore %s for filesets %s",
                     restore_point, str(filesets))
        for fileset in filesets:
            self.restore_single_fileset(fileset=fileset,
                                        output_dir=output_dir,
                                        restore_point=restore_point,
                                        stream=stream)

    def restore_single_fileset(self, fileset, restore_point, output_dir, stream=False):
        """ Restore backup for a single fileset."""
        
        blob_to_restore = Naming.construct_blobname(fileset, True, restore_point)
        file_name = Naming.construct_filename(fileset, True, restore_point)
        file_path = os.path.join(output_dir, file_name)
        
        storage_client = self.backup_configuration.storage_client

        if stream:
            storage_client.get_blob_to_stream(
                container_name=self.backup_configuration.azure_storage_container_name,
                blob_name=blob_to_restore,
                stream=sys.stdout,
                max_connections=1
            )
        else:
            storage_client.get_blob_to_path(
                container_name=self.backup_configuration.azure_storage_container_name,
                blob_name=blob_to_restore,
                file_path=file_path,
                max_connections=1
            )

        logging.debug("Downloaded dump %s", file_path)

    def list_restore_blobs(self, fileset):
        """Determine list of blobs needed to restore a backup."""
        existing_blobs = []
        marker = None
        while True:
            results = self.backup_configuration.storage_client.list_blobs(
                container_name=self.backup_configuration.azure_storage_container_name,
                prefix="{fileset}_".format(fileset=fileset),
                marker=marker)
            for blob in results:
                existing_blobs.append(blob.name)
            if results.next_marker:
                marker = results.next_marker
            else:
                break
        # Keep tar files
        return [b for b in existing_blobs if b.endswith('.tar.gz')]

    #
    # Configuration commands.
    #

    def show_configuration(self, output_dir):
        """Return printable string of configuration parameters."""
        return "\n".join(self.get_configuration_printable(output_dir=output_dir))

    def get_configuration_printable(self, output_dir):
        """Get array of configuration parameters."""
        business_hours = self.backup_configuration.get_business_hours()
        day_f = lambda d: [None, "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][d]
        b2s_f = lambda x: {True:"1", False:"0"}[x]
        s_f = lambda day: "business_hours_{}:                 {}".format(day_f(day), "".join(map(b2s_f, business_hours.hours[day])))
        hours = list(map(s_f, range(1, 8)))

        return [
            "azure.vm_name:                      {}".format(self.backup_configuration.get_vm_name()),
            "azure.resource_group_name:          {}".format(self.backup_configuration.get_resource_group_name()),
            "azure.subscription_id:              {}".format(self.backup_configuration.get_subscription_id()),
            "",
            "Output dir:                         {}".format(output_dir),
            "",
            "fs_backup_interval_min:             {}".format(self.backup_configuration.get_fs_backup_interval_min()),
            "fs_backup_interval_max:             {}".format(self.backup_configuration.get_fs_backup_interval_max()),
        ] + hours + [
            "",
            "azure_storage_container_name:       {}".format(self.backup_configuration.azure_storage_container_name),
            "azure_storage_account_name:         {}".format(self.backup_configuration.get_azure_storage_account_name())
        ]

    #
    # Integration commands. (e.g. TIC)
    #

    def send_notification(self, url, aseservername, db_name, is_full, start_timestamp, end_timestamp, success, data_in_MB, error_msg=None):
        """Send a notification to TIC."""
        data = {
            "SourceSystem" :"Azure",
            "BackupManagementType_s": "AzureWorkload", 
            "BackupItemType_s": "SAPASEDatabase",
            "TenantId" :"unknown",
            "SubscriptionId": self.backup_configuration.get_subscription_id(),
            "Resource": self.backup_configuration.get_vm_name(),
            "BackupItemFriendlyName_s": db_name,
            "BackupItemUniqueId_s": "{location};{storageaccountid};{resourcetype};{resourcegroupname};{resourcename};{aseservername};{db_name}".format(
                location=self.backup_configuration.get_location(),
                storageaccountid=self.backup_configuration.get_azure_storage_account_name(),
                resourcetype="compute",
                resourcegroupname=self.backup_configuration.get_resource_group_name(),
                resourcename=self.backup_configuration.get_vm_name(),
                aseservername=aseservername,
                db_name=db_name),
            "JobUniqueId_g": str(uuid.uuid4()),
            "JobStatus_s": {True:"Completed", False:"Failed"}[success],
            "JobFailureCode_s": {None:"Success", error_msg:error_msg}[error_msg], # "Success OperationCancelledBecauseConflictingOperationRunningUserError",
            "JobOperationSubType_s": {True:"Full", False:"Log"}[is_full],
            "TimeGenerated": time.strftime("%Y-%m-%dT%H:%M:%SZ", Timing.parse(end_timestamp)), #"2018-07-12T14:46:09.726Z",
            "JobStartDateTime_s": time.strftime("%Y-%m-%d %H:%M:%SZ", Timing.parse(start_timestamp)), # "2018-07-13 04:33:00Z",
            "JobDurationInSecs_s": "{}".format(int(Timing.time_diff_in_seconds(start_timestamp, end_timestamp))),
            "DataTransferredInMB_s": "{}".format(int(data_in_MB))
        }
        req = urllib2.Request(url)
        req.add_header('Content-Type', 'application/json')
        response = urllib2.urlopen(req, json.dumps(data))
        return response.read()
