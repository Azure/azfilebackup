#!/usr/bin/env python2.7
#
# coding=utf-8
#

from __future__ import print_function
import sys
import json
import urllib2
import argparse
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
            return json.loads(urllib2.urlopen(urllib2.Request(url, None, {'metadata': 'true'})).read())
        except Exception as e:
            raise(BackupException("Failed to connect to Azure instance metadata endpoint {}:\n{}".format(url, e.message)))

    @staticmethod
    def test_data():
        return '{{ "compute": {{ "name":"vm3728739", "tags":"storage_account_name:{};storage_account_key:{};fs_backup_interval_min:24h;fs_backup_interval_max:3d" }} }}'.format(
            os.environ["SAMPLE_STORAGE_ACCOUNT_NAME"],os.environ["SAMPLE_STORAGE_ACCOUNT_KEY"]
        )

    @staticmethod
    def create_instance():
        #return AzureVMInstanceMetadata(lambda: (json.JSONDecoder()).decode(AzureVMInstanceMetadata.test_data()))
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

def client_and_container():
    config = AzureVMInstanceMetadata.create_instance()
    account_name=config.get_tags()["storage_account_name"]
    account_key=config.get_tags()["storage_account_key"]
    storage_client = BlockBlobService(account_name=account_name, account_key=account_key)
    container_name = "backup"
    return storage_client, container_name

def backup(args):
    storage_client, container_name = client_and_container()
    blob_name = args.backup
    # printe("Backup to {}".format(storage_client.make_blob_url(container_name, blob_name)))

    try:
        if not storage_client.exists(container_name=container_name):
            storage_client.create_container(container_name=container_name)

        storage_client.create_blob_from_stream(
            container_name=container_name,
            blob_name=blob_name, stream=sys.stdin,
            use_byte_buffer=True, max_connections=1)
    except Exception as e:
        raise BackupException(e.message)

def restore(args):
    storage_client, container_name = client_and_container()
    blob_name = args.restore
    # printe("Restore from {}".format(storage_client.make_blob_url(container_name, blob_name)))

    try:
        storage_client.get_blob_to_stream(
            container_name=container_name, 
            blob_name=blob_name, stream=sys.stdout,
            max_connections=1)
    except Exception as e:
        raise BackupException(e.message)

def list_backups(args):
    storage_client, container_name = client_and_container()

    existing_blobs = []
    marker = None
    while True:
        results = storage_client.list_blobs(
            container_name=container_name,
            marker=marker)
        for blob in results:
            existing_blobs.append(blob.name)
        if results.next_marker:
            marker = results.next_marker
        else:
            break

    for blob in existing_blobs:
        print("{}".format(blob))

def main():
    parser = argparse.ArgumentParser()
    commands = parser.add_argument_group("commands")
    commands.add_argument("-b", "--backup", help="Perform backup")
    commands.add_argument("-r", "--restore", help="Perform restore")
    commands.add_argument("-l", "--list", help="List backups in storage", action="store_true")
    args = parser.parse_args()

    if args.backup:
        backup(args)
    elif args.restore:
        restore(args)
    elif args.list:
        list_backups(args)
    else:
        parser.print_help()
        sys.exit(1)

if __name__ == '__main__':
    try:
        main()
    except BackupException as be:
        printe(be.message)
