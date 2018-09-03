# coding=utf-8

import datetime
import re
from .backupexception import BackupException

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
