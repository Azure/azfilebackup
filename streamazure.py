#!/usr/bin/env python2.7
#
# coding=utf-8
#


# # run a backup
# tar cvz -f - . | ./streamazure.py -b
# 
# # list backups. Lists only backups which come from the same VM
# ./streamazure.py -l
# 
# # restore files
# ./streamazure.py -r 20180709_173834 | tar tvfz -


from __future__ import print_function
import logging
import tempfile
import time
import sys
import uuid
import datetime
import requests
import json
import subprocess
import argparse
import re
import threading
import os
import os.path
from azure.storage.blob import BlockBlobService

def printe(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def lazy_property(fn):
    '''Decorator that makes a property lazy-evaluated.
    '''
    attr_name = '_lazy_' + fn.__name__

    @property
    def _lazy_property(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fn(self))
        return getattr(self, attr_name)
    return _lazy_property

class BackupException(Exception):
    pass

class AzureVMInstanceMetadata:
    @staticmethod
    def request_metadata(api_version="2017-12-01"):
        url="http://169.254.169.254/metadata/instance?api-version={v}".format(v=api_version)
        try:
            response = requests.get(url=url, headers={"Metadata": "true"})
            return response.json()
        except Exception as e:
            raise(BackupException("Failed to connect to Azure instance metadata endpoint {}:\n{}".format(url, e.message)))

    @staticmethod
    def test_data():
        return '{{ "compute": {{ "name":"vm3728739", "tags":"azure_storage_account_name:{};azure_storage_account_key:{};fs_backup_interval_min:24h;fs_backup_interval_max:3d" }} }}'.format(
            os.environ["SAMPLE_STORAGE_ACCOUNT_NAME"],os.environ["SAMPLE_STORAGE_ACCOUNT_KEY"]
        )

    @staticmethod
    def create_instance():
        return AzureVMInstanceMetadata(lambda: (json.JSONDecoder()).decode(AzureVMInstanceMetadata.test_data()))
        return AzureVMInstanceMetadata(lambda: AzureVMInstanceMetadata.request_metadata())

    def __init__(self, req):
        self.req = req

    @lazy_property
    def json(self): 
        return self.req()

    def get_tags(self):
        try:
            tags_value = str(self.json['compute']['tags'])
            if tags_value == None:
                return dict()
            return dict(kvp.split(":", 1) for kvp in (tags_value.split(";")))
        except Exception as e:
            raise(BackupException("Cannot parse tags value from instance metadata endpoint: {}".format(e.message)))

    @property
    def vm_name(self):
        try:
            return str(self.json["compute"]["name"])
        except Exception:
            raise(BackupException("Cannot read VM name from instance metadata endpoint"))

class Timing:
    time_format="%Y%m%d_%H%M%S"

    @staticmethod
    def now_localtime(): return time.strftime(Timing.time_format, time.localtime())

    @staticmethod 
    def parse(time_str):
        """
            >>> Timing.parse("20180605_215959")
            time.struct_time(tm_year=2018, tm_mon=6, tm_mday=5, tm_hour=21, tm_min=59, tm_sec=59, tm_wday=1, tm_yday=156, tm_isdst=-1)
        """
        return time.strptime(time_str, Timing.time_format)

    @staticmethod
    def time_diff(str1, str2):
        """
            >>> Timing.time_diff("20180106_120000", "20180106_120010")
            datetime.timedelta(0, 10)
            >>> Timing.time_diff("20180106_110000", "20180106_120010")
            datetime.timedelta(0, 3610)
        """
        t1 = Timing.parse(str1)
        dt1 = datetime.datetime(year=t1.tm_year, month=t1.tm_mon, day=t1.tm_mday, hour=t1.tm_hour, minute=t1.tm_min, second=t1.tm_sec)
        t2 = Timing.parse(str2)
        dt2 = datetime.datetime(year=t2.tm_year, month=t2.tm_mon, day=t2.tm_mday, hour=t2.tm_hour, minute=t2.tm_min, second=t2.tm_sec)
        return dt2 - dt1

    @staticmethod
    def __recovery_sample_data_sorted():
        return [
            {'end_date':'20180101_010000', 'is_full':True,  'stripe_index':1, 'stripe_count':3},
            {'end_date':'20180101_010000', 'is_full':True,  'stripe_index':2, 'stripe_count':3},
            {'end_date':'20180101_010000', 'is_full':True,  'stripe_index':3, 'stripe_count':3},
            {'end_date':'20180101_011000', 'is_full':False, 'stripe_index':1, 'stripe_count':1},
            {'end_date':'20180101_012000', 'is_full':False, 'stripe_index':1, 'stripe_count':1},
            {'end_date':'20180101_013000', 'is_full':False, 'stripe_index':1, 'stripe_count':1},
            {'end_date':'20180101_014000', 'is_full':True,  'stripe_index':1, 'stripe_count':3},
            {'end_date':'20180101_014000', 'is_full':True,  'stripe_index':2, 'stripe_count':3},
            {'end_date':'20180101_014000', 'is_full':True,  'stripe_index':3, 'stripe_count':3},
            {'end_date':'20180101_015000', 'is_full':False, 'stripe_index':1, 'stripe_count':1},
            {'end_date':'20180101_020000', 'is_full':False, 'stripe_index':1, 'stripe_count':1},
            {'end_date':'20180101_021000', 'is_full':False, 'stripe_index':1, 'stripe_count':2},
            {'end_date':'20180101_021000', 'is_full':False, 'stripe_index':2, 'stripe_count':2},
            {'end_date':'20180101_022000', 'is_full':True,  'stripe_index':1, 'stripe_count':3},
            {'end_date':'20180101_022000', 'is_full':True,  'stripe_index':2, 'stripe_count':3},
            {'end_date':'20180101_022000', 'is_full':True,  'stripe_index':3, 'stripe_count':3},
            {'end_date':'20180101_023000', 'is_full':False, 'stripe_index':1, 'stripe_count':1},
            {'end_date':'20180101_024000', 'is_full':False, 'stripe_index':1, 'stripe_count':1},
            {'end_date':'20180101_025000', 'is_full':False, 'stripe_index':1, 'stripe_count':1},
            {'end_date':'20180101_030000', 'is_full':True,  'stripe_index':1, 'stripe_count':3},
            {'end_date':'20180101_030000', 'is_full':True,  'stripe_index':2, 'stripe_count':3},
            {'end_date':'20180101_030000', 'is_full':True,  'stripe_index':3, 'stripe_count':3},
            {'end_date':'20180101_031000', 'is_full':False, 'stripe_index':1, 'stripe_count':1},
            {'end_date':'20180101_032000', 'is_full':False, 'stripe_index':1, 'stripe_count':2},
            {'end_date':'20180101_032000', 'is_full':False, 'stripe_index':2, 'stripe_count':2},
            {'end_date':'20180101_033000', 'is_full':False, 'stripe_index':1, 'stripe_count':1}
        ]

    @staticmethod
    def __recovery_sample_data():
        return [
            {'end_date':'20180101_011000', 'is_full':False, 'stripe_index':1, 'stripe_count':1},
            {'end_date':'20180101_022000', 'is_full':True,  'stripe_index':3, 'stripe_count':3},
            {'end_date':'20180101_012000', 'is_full':False, 'stripe_index':1, 'stripe_count':1},
            {'end_date':'20180101_015000', 'is_full':False, 'stripe_index':1, 'stripe_count':1},
            {'end_date':'20180101_023000', 'is_full':False, 'stripe_index':1, 'stripe_count':1},
            {'end_date':'20180101_024000', 'is_full':False, 'stripe_index':1, 'stripe_count':1},
            {'end_date':'20180101_025000', 'is_full':False, 'stripe_index':1, 'stripe_count':1},
            {'end_date':'20180101_031000', 'is_full':False, 'stripe_index':1, 'stripe_count':1},
            {'end_date':'20180101_032000', 'is_full':False, 'stripe_index':1, 'stripe_count':2},
            {'end_date':'20180101_032000', 'is_full':False, 'stripe_index':2, 'stripe_count':2},
            {'end_date':'20180101_022000', 'is_full':True,  'stripe_index':2, 'stripe_count':3},
            {'end_date':'20180101_020000', 'is_full':False, 'stripe_index':1, 'stripe_count':1},
            {'end_date':'20180101_021000', 'is_full':False, 'stripe_index':1, 'stripe_count':2},
            {'end_date':'20180101_021000', 'is_full':False, 'stripe_index':2, 'stripe_count':2},
            {'end_date':'20180101_013000', 'is_full':False, 'stripe_index':1, 'stripe_count':1},
            {'end_date':'20180101_033000', 'is_full':False, 'stripe_index':1, 'stripe_count':1},
            {'end_date':'20180101_022000', 'is_full':True,  'stripe_index':1, 'stripe_count':3},
            {'end_date':'20180101_030000', 'is_full':True,  'stripe_index':1, 'stripe_count':3},
            {'end_date':'20180101_010000', 'is_full':True,  'stripe_index':1, 'stripe_count':3},
            {'end_date':'20180101_010000', 'is_full':True,  'stripe_index':2, 'stripe_count':3},
            {'end_date':'20180101_010000', 'is_full':True,  'stripe_index':3, 'stripe_count':3},
            {'end_date':'20180101_014000', 'is_full':True,  'stripe_index':1, 'stripe_count':3},
            {'end_date':'20180101_014000', 'is_full':True,  'stripe_index':2, 'stripe_count':3},
            {'end_date':'20180101_014000', 'is_full':True,  'stripe_index':3, 'stripe_count':3},
            {'end_date':'20180101_030000', 'is_full':True,  'stripe_index':2, 'stripe_count':3},
            {'end_date':'20180101_030000', 'is_full':True,  'stripe_index':3, 'stripe_count':3}
        ]

    @staticmethod
    def sort(times, selector=lambda x: x):
        """
            >>> Timing.sort(['20180110_120000', '20180105_120000', '20180101_120000'])
            ['20180101_120000', '20180105_120000', '20180110_120000']
            >>> Timing.sort(['20180105_120000', '20180110_120000', '20180105_120000', '20180101_120000'])
            ['20180101_120000', '20180105_120000', '20180105_120000', '20180110_120000']
            >>> pick_end_date=lambda x: x["end_date"]
            >>> Timing.sort(times=Timing._Timing__recovery_sample_data(), selector=pick_end_date)==[{'is_full': True, 'stripe_index': 1, 'end_date': '20180101_010000', 'stripe_count': 3}, {'is_full': True, 'stripe_index': 2, 'end_date': '20180101_010000', 'stripe_count': 3}, {'is_full': True, 'stripe_index': 3, 'end_date': '20180101_010000', 'stripe_count': 3}, {'is_full': False, 'stripe_index': 1, 'end_date': '20180101_011000', 'stripe_count': 1}, {'is_full': False, 'stripe_index': 1, 'end_date': '20180101_012000', 'stripe_count': 1}, {'is_full': False, 'stripe_index': 1, 'end_date': '20180101_013000', 'stripe_count': 1}, {'is_full': True, 'stripe_index': 1, 'end_date': '20180101_014000', 'stripe_count': 3}, {'is_full': True, 'stripe_index': 2, 'end_date': '20180101_014000', 'stripe_count': 3}, {'is_full': True, 'stripe_index': 3, 'end_date': '20180101_014000', 'stripe_count': 3}, {'is_full': False, 'stripe_index': 1, 'end_date': '20180101_015000', 'stripe_count': 1}, {'is_full': False, 'stripe_index': 1, 'end_date': '20180101_020000', 'stripe_count': 1}, {'is_full': False, 'stripe_index': 1, 'end_date': '20180101_021000', 'stripe_count': 2}, {'is_full': False, 'stripe_index': 2, 'end_date': '20180101_021000', 'stripe_count': 2}, {'is_full': True, 'stripe_index': 1, 'end_date': '20180101_022000', 'stripe_count': 3}, {'is_full': True, 'stripe_index': 2, 'end_date': '20180101_022000', 'stripe_count': 3}, {'is_full': True, 'stripe_index': 3, 'end_date': '20180101_022000', 'stripe_count': 3}, {'is_full': False, 'stripe_index': 1, 'end_date': '20180101_023000', 'stripe_count': 1}, {'is_full': False, 'stripe_index': 1, 'end_date': '20180101_024000', 'stripe_count': 1}, {'is_full': False, 'stripe_index': 1, 'end_date': '20180101_025000', 'stripe_count': 1}, {'is_full': True, 'stripe_index': 1, 'end_date': '20180101_030000', 'stripe_count': 3}, {'is_full': True, 'stripe_index': 2, 'end_date': '20180101_030000', 'stripe_count': 3}, {'is_full': True, 'stripe_index': 3, 'end_date': '20180101_030000', 'stripe_count': 3}, {'is_full': False, 'stripe_index': 1, 'end_date': '20180101_031000', 'stripe_count': 1}, {'is_full': False, 'stripe_index': 1, 'end_date': '20180101_032000', 'stripe_count': 2}, {'is_full': False, 'stripe_index': 2, 'end_date': '20180101_032000', 'stripe_count': 2}, {'is_full': False, 'stripe_index': 1, 'end_date': '20180101_033000', 'stripe_count': 1}]
            True
            >>> 
            >>> map(pick_end_date, Timing.sort(times=Timing._Timing__recovery_sample_data(), selector=pick_end_date))
            ['20180101_010000', '20180101_010000', '20180101_010000', '20180101_011000', '20180101_012000', '20180101_013000', '20180101_014000', '20180101_014000', '20180101_014000', '20180101_015000', '20180101_020000', '20180101_021000', '20180101_021000', '20180101_022000', '20180101_022000', '20180101_022000', '20180101_023000', '20180101_024000', '20180101_025000', '20180101_030000', '20180101_030000', '20180101_030000', '20180101_031000', '20180101_032000', '20180101_032000', '20180101_033000']
        """
        return sorted(times, cmp=lambda a, b: Timing.time_diff_in_seconds(selector(b), selector(a)))

    @staticmethod
    def time_diff_in_seconds(timestr_1, timestr_2):
        """
            >>> Timing.time_diff_in_seconds("20180106_120000", "20180106_120010")
            10
            >>> Timing.time_diff_in_seconds("20180106_110000", "20180106_120010")
            3610
        """
        return int(Timing.time_diff(timestr_1, timestr_2).total_seconds())

    @staticmethod
    def files_needed_for_recovery(times, restore_point, 
                                 select_end_date=lambda x: x["end_date"], 
                                 select_is_full=lambda x: x["is_full"]):
        """
            >>> times=Timing._Timing__recovery_sample_data()
            >>> Timing.files_needed_for_recovery(times=times, restore_point='20180101_023200')
            [{'is_full': False, 'stripe_index': 1, 'end_date': '20180101_023000', 'stripe_count': 1}, {'is_full': False, 'stripe_index': 1, 'end_date': '20180101_024000', 'stripe_count': 1}, {'is_full': True, 'stripe_index': 1, 'end_date': '20180101_022000', 'stripe_count': 3}, {'is_full': True, 'stripe_index': 2, 'end_date': '20180101_022000', 'stripe_count': 3}, {'is_full': True, 'stripe_index': 3, 'end_date': '20180101_022000', 'stripe_count': 3}]
        """

        create_tuple=lambda x: (select_end_date(x), select_is_full(x))
        end_date_from_tuple=lambda x: x[0]
        index_of_files_to_download = set()
        for x in Timing.sort(list(set(map(create_tuple, times))), end_date_from_tuple):
            x_end_date = end_date_from_tuple(x)
            x_is_full = x[1]
            x_is_before = Timing.time_diff_in_seconds(restore_point, x_end_date) <= 0
            if x_is_full and x_is_before:
                index_of_files_to_download = set()
            index_of_files_to_download.add(x)
            if not x_is_before:
                break

        files_to_download = []
        for x in Timing.sort(times, select_end_date):
            if create_tuple(x) in index_of_files_to_download:
                files_to_download.append(x)

        result = Timing.sort(files_to_download, select_end_date)

        logging.debug("Files which must be fetched for {}: {}".format(restore_point, str(result)))

        return result

class ScheduleParser:
    """
        Parse time duration statements such as `7d` for 7 days or `1h 30m` for 90 minutes. 
    """

    @staticmethod
    def __from_atom(time):
        """
            >>> ScheduleParser._ScheduleParser__from_atom('7d')
            datetime.timedelta(7)
            >>> ScheduleParser._ScheduleParser__from_atom('2w')
            datetime.timedelta(14)
        """
        try:
            num = int(time[:-1])
            unit = time[-1:]
            return {
                "w": lambda w: datetime.timedelta(days=7*w),
                "d": lambda d: datetime.timedelta(days=d),
                "h": lambda h: datetime.timedelta(hours=h),
                "m": lambda m: datetime.timedelta(minutes=m),
                "s": lambda s: datetime.timedelta(seconds=s)
            }[unit](num)
        except Exception as e:
            raise(BackupException("Cannot parse value '{}' into duration: {}".format(time, e.message)))

    @staticmethod
    def parse_timedelta(time_val):
        """
            Parses a time delta.

            >>> ScheduleParser.parse_timedelta('1w 3d 2s')
            datetime.timedelta(10, 2)
            >>> ScheduleParser.parse_timedelta('7d')
            datetime.timedelta(7)
            >>> ScheduleParser.parse_timedelta('7d 20s')
            datetime.timedelta(7, 20)
            >>> ScheduleParser.parse_timedelta('1d 1h 1m 1s')
            datetime.timedelta(1, 3661)
            >>> ScheduleParser.parse_timedelta('1d 23h 59m 59s')
            datetime.timedelta(1, 86399)
            >>> ScheduleParser.parse_timedelta('1d 23h 59m 60s')
            datetime.timedelta(2)
        """
        try:
            no_spaces = time_val.replace(" ", "")
            atoms = re.findall(r"(\d+[wdhms])", no_spaces)
            durations = map(lambda time: ScheduleParser.__from_atom(time), atoms)
            return reduce(lambda x, y: x + y, durations)
        except Exception as e:
            raise(BackupException("Cannot parse value '{}' into duration: {}".format(time_val, e.message)))

class UploadThread(threading.Thread):
    def __init__(self, storage_client, container_name, blob_name, stream):
        super(UploadThread, self).__init__()
        
        self.storage_client = storage_client
        self.container_name = container_name
        self.blob_name = blob_name
        self.stream = stream
        self.exception = None

    def get_exception(self):
        return self.exception

    def run(self):
        try:
            logging.debug("Start streaming upload to {}/{}".format(self.container_name, self.blob_name))

            self.storage_client.create_blob_from_stream(
                container_name=self.container_name,
                blob_name=self.blob_name, stream=self.stream,
                use_byte_buffer=True, max_connections=1)

            logging.debug("Finished streaming upload of {}/{}".format(self.container_name, self.blob_name))
        except Exception as e:
            self.exception = e

def client_and_container():
    config = AzureVMInstanceMetadata.create_instance()
    account_name=config.get_tags()["azure_storage_account_name"]
    account_key=config.get_tags()["azure_storage_account_key"]
    storage_client = BlockBlobService(account_name=account_name, account_key=account_key)
    container_name = "backup"
    return config, storage_client, container_name

def backup(args):
    config, storage_client, container_name = client_and_container()

    blob_name = "{}_{}.tar.gz".format(config.vm_name, Timing.now_localtime())

    if not storage_client.exists(container_name=container_name):
        storage_client.create_container(container_name=container_name)

    t = UploadThread(storage_client, container_name, blob_name, stream=sys.stdin)
    t.start() 
    t.join()
    exception_during_upload = t.get_exception()
    if exception_during_upload != None:
        printe("Problem during upload: {}".format(exception_during_upload.message))

def restore(args):
    config, storage_client, container_name = client_and_container()

    try:
        Timing.parse(args.restore)
    except Exception:
        printe("{} is not a valid time".format(args.restore))
        sys.exit(1)

    blob_name = "{}_{}.tar.gz".format(config.vm_name, args.restore)
    printe("Restoring {}".format(blob_name))
    storage_client.get_blob_to_stream(container_name=container_name, blob_name=blob_name, stream=sys.stdout)

def list_backups(args):
    config, storage_client, container_name = client_and_container()

    existing_blobs = []
    marker = None
    while True:
        results = storage_client.list_blobs(
            container_name=container_name,
            prefix="{vmname}_".format(vmname=config.vm_name), 
            marker=marker)
        for blob in results:
            existing_blobs.append(blob.name)
        if results.next_marker:
            marker = results.next_marker
        else:
            break

    for blob in filter(lambda x: x.endswith(".tar.gz"), existing_blobs):
        print("{}".format(blob))

def main():
    parser = argparse.ArgumentParser()
    commands = parser.add_argument_group("commands")
    commands.add_argument("-b", "--backup", help="Perform backup", action="store_true")
    commands.add_argument("-r", "--restore", help="Perform restore")
    commands.add_argument("-l", "--list", help="List backups in storage", action="store_true")
    options = parser.add_argument_group("options")
    options.add_argument("-n", "--name", help="Name for the backup", action="store_true")
    args = parser.parse_args()

    if args.backup:
        backup(args)
    elif args.restore:
        restore(args)
    elif args.list:
        list_backups(args)
    else:
        printe("Select backup or restore")
        sys.exit(1)

if __name__ == '__main__':
    main()
