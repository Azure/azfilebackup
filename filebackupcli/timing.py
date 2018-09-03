# coding=utf-8

import time
import datetime
import logging
from .backupexception import BackupException

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
