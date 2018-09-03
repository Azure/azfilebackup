# coding=utf-8


import re

from .timing import Timing
from .backupexception import BackupException

class BusinessHours:
    """
        Process business hour statements, such as determine wheter a certain point in time is within or outside business hours.
    """

    standard_prefix="db_backup_window"

    @staticmethod
    def __sample_data():
        return (
            "db_backup_window_1:111111 111000 000000 011111;"
            "db_backup_window_2:111111 111000 000000 011111;"
            "db_backup_window_3:111111 111000 000000 011111;"
            "db_backup_window_4:111111 111000 000000 011111;"
            "db_backup_window_5:111111 111000 000000 011111;"
            "db_backup_window_6:111111 111111 111111 111111;"
            "db_backup_window_7:111111 111111 111111 111111"
            )

    @staticmethod
    def parse_tag_str(tags_value, prefix=standard_prefix):
        """
            >>> BusinessHours.parse_tag_str(BusinessHours._BusinessHours__sample_data(), 'db_backup_window').tags['db_backup_window_1']
            '111111 111000 000000 011111'
        """
        try:
            tags = dict(kvp.split(":", 1) for kvp in (tags_value.split(";")))
            return BusinessHours(tags=tags, prefix=prefix)
        except Exception as e:
            raise(BackupException("Error parsing business hours '{}': {}".format(tags_value, e.message)))

    @staticmethod
    def parse_day(day_values):
        """
            >>> BusinessHours.parse_day('111111 111000 000000 011111')
            [True, True, True, True, True, True, True, True, True, False, False, False, False, False, False, False, False, False, False, True, True, True, True, True]
        """
        try:
            hour_strs = re.findall(r"([01])", day_values)
            durations = map(lambda x: {"1":True, "0":False}[x], hour_strs)
            return durations
        except Exception as e:
            raise(BackupException("Error parsing business hours '{}': {}".format(day_values, e.message)))

    def __init__(self, tags, prefix=standard_prefix):
        """
            >>> sample_data = BusinessHours._BusinessHours__sample_data()
            >>> BusinessHours.parse_tag_str(sample_data).hours[1]
            [True, True, True, True, True, True, True, True, True, False, False, False, False, False, False, False, False, False, False, True, True, True, True, True]
        """
        self.tags = tags
        self.prefix = prefix
        self.hours = dict()
        for day in range(1, 8):
            name = "{prefix}_{day}".format(prefix=prefix, day=day)
            if not tags.has_key(name):
                raise(BackupException("Missing VM tag {}".format(name)))
            x = tags[name]
            self.hours[day] = BusinessHours.parse_day(x)

    def is_backup_allowed_dh(self, day, hour):
        """
            >>> sample_data = BusinessHours._BusinessHours__sample_data()
            >>> sample_hours = BusinessHours.parse_tag_str(sample_data)
            >>> sample_hours.is_backup_allowed_dh(day=1, hour=4)
            True
            >>> sample_hours.is_backup_allowed_dh(day=1, hour=11)
            False
            >>> sample_hours.is_backup_allowed_dh(day=7, hour=11)
            True
        """
        return self.hours[day][hour]

    def is_backup_allowed_time(self, time):
        """
            >>> sample_data = BusinessHours._BusinessHours__sample_data()
            >>> sample_hours = BusinessHours.parse_tag_str(sample_data)
            >>> some_tuesday_evening = "20180605_215959"
            >>> sample_hours.is_backup_allowed_time(some_tuesday_evening)
            True
            >>> some_tuesday_noon = "20180605_115500"
            >>> sample_hours.is_backup_allowed_time(some_tuesday_noon)
            False
            >>> some_sunday_noon = "20180610_115500"
            >>> sample_hours.is_backup_allowed_time(some_sunday_noon)
            True
        """
        # time.struct_time.tm_wday is range [0, 6], Monday is 0
        t = Timing.parse(time)
        return self.is_backup_allowed_dh(day=1 + t.tm_wday, hour=t.tm_hour)

    def is_backup_allowed_now_localtime(self):
        return self.is_backup_allowed_time(time=Timing.now_localtime())
