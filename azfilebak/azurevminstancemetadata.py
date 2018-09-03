# coding=utf-8

import urllib2
import json
from .backupexception import BackupException

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

class AzureVMInstanceMetadata:
    @staticmethod
    def request_metadata(api_version="2017-12-01"):
        url="http://169.254.169.254/metadata/instance?api-version={v}".format(v=api_version)
        try:
            return json.loads(urllib2.urlopen(urllib2.Request(url, None, {'metadata': 'true'})).read())
        except Exception as e:
            raise(BackupException("Failed to connect to Azure instance metadata endpoint {}:\n{}".format(url, e.message)))

    @staticmethod
    def create_instance():
        """
            >>> json_meta = '{ "compute": { "subscriptionId": "724467b5-bee4-484b-bf13-d6a5505d2b51", "resourceGroupName": "backuptest", "name": "somevm", "tags":"db_backup_interval_min:24h;db_backup_interval_max:3d;log_backup_interval_min:600s;log_backup_interval_max:30m;db_backup_window_1:111111 111000 000000 011111;db_backup_window_2:111111 111000 000000 011111;db_backup_window_3:111111 111000 000000 011111;db_backup_window_4:111111 111000 000000 011111;db_backup_window_5:111111 111000 000000 011111;db_backup_window_6:111111 111111 111111 111111;db_backup_window_7:111111 111111 111111 111111" } }'
            >>> meta = AzureVMInstanceMetadata(lambda: (json.JSONDecoder()).decode(json_meta))
            >>> meta.vm_name
            'somevm'
        """
        # return AzureVMInstanceMetadata(lambda: (json.JSONDecoder()).decode('{ "compute": { "subscriptionId": "724467b5-bee4-484b-bf13-d6a5505d2b51", "resourceGroupName": "backuptest", "name": "somevm", "tags":"db_backup_interval_min:24h;db_backup_interval_max:3d;log_backup_interval_min:600s;log_backup_interval_max:30m;db_backup_window_1:111111 111000 000000 011111;db_backup_window_2:111111 111000 000000 011111;db_backup_window_3:111111 111000 000000 011111;db_backup_window_4:111111 111000 000000 011111;db_backup_window_5:111111 111000 000000 011111;db_backup_window_6:111111 111111 111111 111111;db_backup_window_7:111111 111111 111111 111111" } }'))
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
    def subscription_id(self): 
        try:
            return str(self.json["compute"]["subscriptionId"])
        except Exception:
            raise(BackupException("Cannot read subscriptionId from instance metadata endpoint"))

    @property
    def resource_group_name(self):
        try:
            return str(self.json["compute"]["resourceGroupName"])
        except Exception:
            raise(BackupException("Cannot read resourceGroupName from instance metadata endpoint"))

    @property
    def location(self):
        try:
            return str(self.json["compute"]["location"])
        except Exception:
            raise(BackupException("Cannot read location from instance metadata endpoint"))

    @property
    def vm_name(self):
        try:
            return str(self.json["compute"]["name"])
        except Exception:
            raise(BackupException("Cannot read VM name from instance metadata endpoint"))
