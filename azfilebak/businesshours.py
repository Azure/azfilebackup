# coding=utf-8

import re

from azfilebak.timing import Timing
from azfilebak.backupexception import BackupException

class BusinessHours(object):
    """
    Process business hour statements, such as determine wheter a certain
    point in time is within or outside business hours.
    """

    default_schedule = "bkp_fs_schedule"

    @staticmethod
    def __sample_data():
        return (
            "bkp_fs_schedule:"
            "mo:111111 111000 000000 011111, "
            "tu:111111 111000 000000 011111, "
            "we:111111 111000 000000 011111, "
            "th:111111 111000 000000 011111, "
            "fr:111111 111000 000000 011111, "
            "sa:111111 111111 111111 111111, "
            "su:111111 111111 111111 111111, "
            "min:1d, "
            "max:3d;"
            "bkp_db_schedule:mo:111111111111111111111111,"
            "tu:111111111111111111111111, "
            "we:111111111111111111111111, "
            "th:111111111111111111111111, "
            "fr:111111111111111111111111, "
            "sa:111111111111111111111111, "
            "su:111111111111111111111111, "
            "min:1d, "
            "max:3d"
        )

    def __init__(self, tags, schedule=default_schedule):
        """
        Given a dictionary of all the instance metadata tags,
        extracts the tag containing the schedule information
        and parses the values.
        >>> sample_data = BusinessHours._BusinessHours__sample_data()
        >>> BusinessHours.parse_tag_str(sample_data).hours[1]
        [True, True, True, True, True, True, True, True, True, False, False, False, False, False, False, False, False, False, False, True, True, True, True, True]
        """

        # Get the schedule tag and remove space
        schedule_tag = tags[schedule].replace(' ', '')

        # Parse days from tag
        self.tags = dict(d.split(':', 1) for d in schedule_tag.split(','))

        # Parse hours
        self.hours = dict()
        weekdays = ['mo', 'tu', 'we', 'th', 'fr', 'sa', 'su']

        for day in range(0, 7):
            if not self.tags.has_key(weekdays[day]):
                raise BackupException("Missing schedule for {}".format(weekdays[day]))
            self.hours[day+1] = BusinessHours.parse_day(self.tags[weekdays[day]])
        
        # Also retrieve min/max retention values from tag
        if not self.tags.has_key('min'):
            raise BackupException("Missing value for min")
        self.min = self.tags['min']

        if not self.tags.has_key('max'):
            raise BackupException("Missing value for max")
        self.max = self.tags['max']

    @staticmethod
    def parse_tag_str(tags_value, schedule=default_schedule):
        """
        >>> BusinessHours.parse_tag_str(BusinessHours._BusinessHours__sample_data()).tags['mo']
        '111111111000000000011111'
        """
        try:
            tags = dict(kvp.split(":", 1) for kvp in (tags_value.split(";")))
            return BusinessHours(tags=tags, schedule=schedule)
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
