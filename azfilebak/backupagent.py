# coding=utf-8

import logging
import os
import datetime
import threading
from itertools import groupby
import json
import urllib2
import uuid
import time

from .funcmodule import printe, out, log_stdout_stderr
from .naming import Naming
from .timing import Timing
from .executableconnector import ExecutableConnector
from .backupexception import BackupException
from .streamingthread import StreamingThread

class BackupAgent(object):
    """
    The backup business logic implementation.
    """

    def __init__(self, backup_configuration):
        self.backup_configuration = backup_configuration
        self.executable_connector = ExecutableConnector(self.backup_configuration)

    """
    Scheduling methods.
    """

    def existing_backups_for_fileset(self, fileset, is_full):
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
                if parts == None:
                    continue

                end_time_of_existing_blob = parts[3]
                if not existing_blobs_dict.has_key(end_time_of_existing_blob):
                    existing_blobs_dict[end_time_of_existing_blob] = []
                existing_blobs_dict[end_time_of_existing_blob].append(blob_name)

            if results.next_marker:
                marker = results.next_marker
            else:
                break
        return existing_blobs_dict

    def existing_backups(self, filesets=[]):
        existing_blobs_dict = dict()
        marker = None
        while True:
            results = self.backup_configuration.storage_client.list_blobs(
                container_name=self.backup_configuration.azure_storage_container_name,
                marker=marker)

            for blob in results:
                blob_name = blob.name
                parts = Naming.parse_blobname(blob_name)
                if parts == None:
                    continue

                (dbname_of_existing_blob, _is_full, _start_timestamp, end_time_of_existing_blob, _stripe_index, _stripe_count) = parts
                if len(filesets) == 0 or dbname_of_existing_blob in filesets:
                    if not existing_blobs_dict.has_key(end_time_of_existing_blob):
                        existing_blobs_dict[end_time_of_existing_blob] = []
                    existing_blobs_dict[end_time_of_existing_blob].append(blob_name)

            if results.next_marker:
                marker = results.next_marker
            else:
                break
        return existing_blobs_dict

    def latest_backup_timestamp(self, fileset, is_full):
        existing_blobs_dict = self.existing_backups_for_fileset(fileset=fileset, is_full=is_full)
        if len(existing_blobs_dict.keys()) == 0:
            return "19000101_000000"
        return Timing.sort(existing_blobs_dict.keys())[-1:][0]

    @staticmethod
    def should_run_full_backup(now_time, force, latest_full_backup_timestamp, business_hours, db_backup_interval_min, db_backup_interval_max):
        """
            Determine whether a backup should be executed. 

            >>> business_hours=BusinessHours.parse_tag_str(BusinessHours._BusinessHours__sample_data())
            >>> db_backup_interval_min=ScheduleParser.parse_timedelta("24h")
            >>> db_backup_interval_max=ScheduleParser.parse_timedelta("3d")
            >>> five_day_backup =        "20180601_010000"
            >>> two_day_backup =         "20180604_010000"
            >>> same_day_backup =        "20180606_010000"
            >>> during_business_hours  = "20180606_150000"
            >>> outside_business_hours = "20180606_220000"
            >>> 
            >>> # Forced
            >>> BackupAgent.should_run_full_backup(now_time=during_business_hours, force=True, latest_full_backup_timestamp=same_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            True
            >>> # Forced
            >>> BackupAgent.should_run_full_backup(now_time=during_business_hours, force=True, latest_full_backup_timestamp=two_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            True
            >>> # Forced
            >>> BackupAgent.should_run_full_backup(now_time=during_business_hours, force=True, latest_full_backup_timestamp=five_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            True
            >>> # respecting business hours, and not needed anyway
            >>> BackupAgent.should_run_full_backup(now_time=during_business_hours, force=False, latest_full_backup_timestamp=same_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            False
            >>> # respecting business hours
            >>> BackupAgent.should_run_full_backup(now_time=during_business_hours, force=False, latest_full_backup_timestamp=two_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            False
            >>> # a really old backup, so we ignore business hours
            >>> BackupAgent.should_run_full_backup(now_time=during_business_hours, force=False, latest_full_backup_timestamp=five_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            True
            >>> # outside_business_hours, but same_day_backup, so no backup
            >>> BackupAgent.should_run_full_backup(now_time=outside_business_hours, force=False, latest_full_backup_timestamp=same_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            False
            >>> # outside_business_hours and need to backup
            >>> BackupAgent.should_run_full_backup(now_time=outside_business_hours, force=False, latest_full_backup_timestamp=two_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            True
            >>> # a really old backup
            >>> BackupAgent.should_run_full_backup(now_time=outside_business_hours, force=False, latest_full_backup_timestamp=five_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            True
            >>> # Forced
            >>> BackupAgent.should_run_full_backup(now_time=outside_business_hours, force=True, latest_full_backup_timestamp=same_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            True
            >>> # Forced
            >>> BackupAgent.should_run_full_backup(now_time=outside_business_hours, force=True, latest_full_backup_timestamp=two_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            True
            >>> # Forced
            >>> BackupAgent.should_run_full_backup(now_time=outside_business_hours, force=True, latest_full_backup_timestamp=five_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            True
        """
        allowed_by_business = business_hours.is_backup_allowed_time(now_time)
        age_of_latest_backup_in_storage = Timing.time_diff(latest_full_backup_timestamp, now_time)
        min_interval_allows_backup = age_of_latest_backup_in_storage > db_backup_interval_min
        max_interval_requires_backup = age_of_latest_backup_in_storage > db_backup_interval_max
        perform_full_backup = (allowed_by_business and min_interval_allows_backup or max_interval_requires_backup or force)

        # logging.info("Full backup requested. Current time: {now}. Last backup in storage: {last}. Age of backup {age}".format(now=now_time, last=latest_full_backup_timestamp, age=age_of_latest_backup_in_storage))
        # logging.info("Backup requirements: min=\"{min}\" max=\"{max}\"".format(min=db_backup_interval_min,max=db_backup_interval_max))
        # logging.info("Forced by user: {force}. Backup allowed by business hours: {allowed_by_business}. min_interval_allows_backup={min_interval_allows_backup}. max_interval_requires_backup={max_interval_requires_backup}".format(force=force, allowed_by_business=allowed_by_business, min_interval_allows_backup=min_interval_allows_backup, max_interval_requires_backup=max_interval_requires_backup))
        # logging.info("Decision to backup: {perform_full_backup}.".format(perform_full_backup=perform_full_backup))

        return perform_full_backup

    @staticmethod
    def should_run_tran_backup(now_time, force, latest_tran_backup_timestamp, log_backup_interval_min):
        if force:
            return True

        age_of_latest_backup_in_storage = Timing.time_diff(latest_tran_backup_timestamp, now_time)
        min_interval_allows_backup = age_of_latest_backup_in_storage > log_backup_interval_min
        perform_tran_backup = min_interval_allows_backup
        return perform_tran_backup 

    def should_run_backup(self, fileset, is_full, force, start_timestamp):
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

    """
    Backup methods.
    """

    def backup(self, filesets, is_full, force):
        filesets_to_backup = filesets
        if filesets_to_backup.len() == 0:
            filesets_to_backup = self.backup_configuration.get_filesets()
 
        for fileset in filesets_to_backup:
            self.backup_single_fileset(fileset=fileset, is_full=is_full, force=force)

    def backup_single_fileset(self, fileset, is_full, force):
        start_timestamp = Timing.now_localtime()
        if not self.should_run_backup(fileset=fileset, is_full=is_full, force=force, start_timestamp=start_timestamp):
            out("Skipping backup of fileset {}".format(fileset))
            return

        # Create temporary container to hold the backup blob
        temp_container_name = Naming.temp_container_name(fileset, start_timestamp)
        storage_client = self.backup_configuration.storage_client
        storage_client.create_container(container_name=temp_container_name)
        # Final destination container 
        dest_container_name = self.backup_configuration.azure_storage_container_name
        # Name of the backup blob
        blob_name = Naming.construct_blobname(fileset=fileset,
                                              is_full=is_full,
                                              start_timestamp=start_timestamp,
                                              end_timestamp='temp')

        try:
            # Run the backup command
            proc = self.executable_connector.create_backup(
                fileset=fileset, is_full=is_full)

            # Stream backup command stdout to the blob
            storage_client.create_blob_from_stream(
                container_name=temp_container_name,
                blob_name=blob_name, stream=proc.stdout,
                use_byte_buffer=True, max_connections=1)

            # Wait for the command to terminate
            proc.wait()
        except Exception as ex:
            # Delete the temporary container
            storage_client.delete_container(container_name=temp_container_name)
            raise

        end_timestamp = Timing.now_localtime()

        # Rename the backup blob:
        # - copy from temp_container_name/old_blob_name to dest_container_name/new_blob_name)
        # - delete temp_container_name

        new_blob_name = Naming.construct_blobname(fileset=fileset,
                                                  is_full=is_full,
                                                  start_timestamp=start_timestamp,
                                                  end_timestamp=end_timestamp)
        copy_source = storage_client.make_blob_url(temp_container_name, blob_name)
        copy = storage_client.copy_blob(dest_container_name, new_blob_name, copy_source=copy_source)

        # Wait for copy to succeed
        while copy.status != 'success':
            logging.debug("Waiting for copy; status: %s", copy.status)
            time.sleep(30)
            copy = storage_client.get_blob_properties(dest_container_name, new_blob_name).properties.copy

        # Delete sources
        storage_client.delete_container(container_name=temp_container_name)

        # Return name of new blob
        return new_blob_name

    """
    Other commands. (TBD)
    """

    def list_backups(self, filesets = []):
        baks_dict = self.existing_backups(filesets=filesets)
        for end_timestamp in baks_dict.keys():
            # http://mark-dot-net.blogspot.com/2014/03/python-equivalents-of-linq-methods.html
            stripes = baks_dict[end_timestamp]
            stripes = map(lambda blobname: {
                    "blobname":blobname,
                    "filename": Naming.blobname_to_filename(blobname),
                    "parts": Naming.parse_blobname(blobname)
                }, stripes)
            stripes = map(lambda x: {
                    "blobname": x["blobname"],
                    "filename": x["filename"],
                    "parts": x["parts"],
                    "dbname": x["parts"][0],
                    "is_full": x["parts"][1],
                    "begin": x["parts"][2],
                    "end": x["parts"][3],
                    "stripe_index": x["parts"][4],
                }, stripes)

            group_by_key=lambda x: "db {dbname: <30} start {begin} end {end} ({type})".format(
                dbname=x["dbname"], end=x["end"], begin=x["begin"], type=Naming.backup_type_str(x["is_full"]))

            for group, values in groupby(stripes, key=group_by_key): 
                files = list(map(lambda s: s["stripe_index"], values))
                print("{backup} {files}".format(backup=group, files=files))

    def prune_old_backups(self, older_than, databases):
        """
            Delete (prune) old backups from Azure storage. 
        """
        minimum_deletable_age = datetime.timedelta(7, 0)
        logging.warn("Deleting files older than {}".format(older_than))
        if (older_than < minimum_deletable_age):
            msg="This script does not delete files younger than {}, ignoring this order".format(minimum_deletable_age)
            logging.warn(msg)
            return

        marker = None
        while True:
            results = self.backup_configuration.storage_client.list_blobs(
                container_name=self.backup_configuration.azure_storage_container_name,
                marker=marker)
            for blob in results:
                parts = Naming.parse_blobname(blob.name)
                if (parts == None):
                    continue

                (dbname, _is_full, _start_timestamp, end_timestamp, _stripe_index, _stripe_count) = parts
                if (databases != None) and not (dbname in databases):
                    continue

                diff = Timing.time_diff(end_timestamp, Timing.now_localtime())
                delete = diff > older_than

                if delete:
                    logging.warn("Deleting {}".format(blob.name))
                    self.backup_configuration.storage_client.delete_blob(
                        container_name=self.backup_configuration.azure_storage_container_name,
                        blob_name=blob.name)
                else:
                    logging.warn("Keeping {}".format(blob.name))

            if results.next_marker:
                marker = results.next_marker
            else:
                break

    def restore(self, restore_point, output_dir, databases):
        print("Retriving point-in-time restore {} for databases {}".format(restore_point, str(databases)))
        databases = self.executable_connector.determine_databases(user_selected_databases=databases, is_full=True)
        skip_dbs = self.backup_configuration.get_databases_to_skip()
        databases = filter(lambda db: not (db in skip_dbs), databases)
        for dbname in databases:
            self.restore_single_db(dbname=dbname, output_dir=output_dir, restore_point=restore_point)

    def restore_single_db(self, dbname, restore_point, output_dir):
        blobs = self.list_restore_blobs(dbname=dbname)
        times = map(Naming.parse_blobname, blobs)
        restore_files = Timing.files_needed_for_recovery(times, restore_point, 
            select_end_date=lambda x: x[3], select_is_full=lambda x: x[1])

        storage_client = self.backup_configuration.storage_client
        for (dbname, is_full, start_timestamp, end_timestamp, stripe_index, stripe_count) in restore_files:
            if is_full:
                # For full database files, download the SQL description
                ddlgen_file_name=Naming.construct_ddlgen_name(dbname=dbname, start_timestamp=start_timestamp)
                ddlgen_file_path=os.path.join(output_dir, ddlgen_file_name)
                if storage_client.exists(container_name=self.backup_configuration.azure_storage_container_name, blob_name=ddlgen_file_name):
                    storage_client.get_blob_to_path(
                        container_name=self.backup_configuration.azure_storage_container_name,
                        blob_name=ddlgen_file_name, file_path=ddlgen_file_path)
                    out("Downloaded ddlgen description {}".format(ddlgen_file_path))

            blob_name = "{dbname}_{type}_{start}--{end}_S{idx:03d}-{cnt:03d}.cdmp".format(
                dbname=dbname, type=Naming.backup_type_str(is_full), 
                start=start_timestamp, end=end_timestamp,
                idx=stripe_index, cnt=stripe_count)
            file_name = "{dbname}_{type}_{start}_S{idx:03d}-{cnt:03d}.cdmp".format(
                dbname=dbname, type=Naming.backup_type_str(is_full), 
                start=start_timestamp, idx=stripe_index, cnt=stripe_count)

            file_path = os.path.join(output_dir, file_name)
            storage_client.get_blob_to_path(
                container_name=self.backup_configuration.azure_storage_container_name,
                blob_name=blob_name,
                file_path=file_path)
            out("Downloaded dump {}".format(file_path))

    def list_restore_blobs(self, dbname):
        existing_blobs = []
        marker = None
        while True:
            results = self.backup_configuration.storage_client.list_blobs(
                container_name=self.backup_configuration.azure_storage_container_name,
                prefix="{dbname}_".format(dbname=dbname), 
                marker=marker)
            for blob in results:
                existing_blobs.append(blob.name)
            if results.next_marker:
                marker = results.next_marker
            else:
                break
        #
        # restrict to dump files
        #
        return filter(lambda x: x.endswith(".cdmp"), existing_blobs)

    def show_configuration(self, output_dir):
        return "\n".join(self.get_configuration_printable(output_dir=output_dir))

    def get_configuration_printable(self, output_dir):
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
            "skipped databases:                  {}".format(self.backup_configuration.get_databases_to_skip()),
            "fs_backup_interval_min:             {}".format(self.backup_configuration.get_fs_backup_interval_min()),
            "fs_backup_interval_max:             {}".format(self.backup_configuration.get_fs_backup_interval_max()),
        ] + hours + [
            "",
            "azure_storage_container_name:       {}".format(self.backup_configuration.azure_storage_container_name),
            "azure_storage_account_name:         {}".format(self.backup_configuration.get_azure_storage_account_name())
        ]

    def send_notification(self, url, aseservername, db_name, is_full, start_timestamp, end_timestamp, success, data_in_MB, error_msg=None):
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
