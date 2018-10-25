# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
# --------------------------------------------------------------------------

"""Unit tests for timing."""

import time
import datetime
import unittest
from azfilebak.timing import Timing
from tests.loggedtestcase import LoggedTestCase

class TestTiming(LoggedTestCase):
    """Unit tests for class Timing."""

    @staticmethod
    def __recovery_sample_data_sorted():
        return [
            {'start_date':'20180101_010000', 'is_full':True},
            {'start_date':'20180101_010000', 'is_full':True},
            {'start_date':'20180101_010000', 'is_full':True},
            {'start_date':'20180101_011000', 'is_full':False},
            {'start_date':'20180101_012000', 'is_full':False},
            {'start_date':'20180101_013000', 'is_full':False},
            {'start_date':'20180101_014000', 'is_full':True},
            {'start_date':'20180101_014000', 'is_full':True},
            {'start_date':'20180101_014000', 'is_full':True},
            {'start_date':'20180101_015000', 'is_full':False},
            {'start_date':'20180101_020000', 'is_full':False},
            {'start_date':'20180101_021000', 'is_full':False},
            {'start_date':'20180101_021000', 'is_full':False},
            {'start_date':'20180101_022000', 'is_full':True},
            {'start_date':'20180101_022000', 'is_full':True},
            {'start_date':'20180101_022000', 'is_full':True},
            {'start_date':'20180101_023000', 'is_full':False},
            {'start_date':'20180101_024000', 'is_full':False},
            {'start_date':'20180101_025000', 'is_full':False},
            {'start_date':'20180101_030000', 'is_full':True},
            {'start_date':'20180101_030000', 'is_full':True},
            {'start_date':'20180101_030000', 'is_full':True},
            {'start_date':'20180101_031000', 'is_full':False},
            {'start_date':'20180101_032000', 'is_full':False},
            {'start_date':'20180101_032000', 'is_full':False},
            {'start_date':'20180101_033000', 'is_full':False}
        ]

    @staticmethod
    def __recovery_sample_data():
        return [
            {'start_date':'20180101_011000', 'is_full':False},
            {'start_date':'20180101_022000', 'is_full':True},
            {'start_date':'20180101_012000', 'is_full':False},
            {'start_date':'20180101_015000', 'is_full':False},
            {'start_date':'20180101_023000', 'is_full':False},
            {'start_date':'20180101_024000', 'is_full':False},
            {'start_date':'20180101_025000', 'is_full':False},
            {'start_date':'20180101_031000', 'is_full':False},
            {'start_date':'20180101_032000', 'is_full':False},
            {'start_date':'20180101_032000', 'is_full':False},
            {'start_date':'20180101_022000', 'is_full':True},
            {'start_date':'20180101_020000', 'is_full':False},
            {'start_date':'20180101_021000', 'is_full':False},
            {'start_date':'20180101_021000', 'is_full':False},
            {'start_date':'20180101_013000', 'is_full':False},
            {'start_date':'20180101_033000', 'is_full':False},
            {'start_date':'20180101_022000', 'is_full':True},
            {'start_date':'20180101_030000', 'is_full':True},
            {'start_date':'20180101_010000', 'is_full':True},
            {'start_date':'20180101_010000', 'is_full':True},
            {'start_date':'20180101_010000', 'is_full':True},
            {'start_date':'20180101_014000', 'is_full':True},
            {'start_date':'20180101_014000', 'is_full':True},
            {'start_date':'20180101_014000', 'is_full':True},
            {'start_date':'20180101_030000', 'is_full':True},
            {'start_date':'20180101_030000', 'is_full':True}
        ]

    def test_parse(self):
        """Test parse."""
        res = Timing.parse("20180605_215959")
        self.assertEqual(
            time.struct_time(
                (2018, 6, 5, 21, 59, 59, 1, 156, -1)),
            res)

    def test_time_diff(self):
        """Test time_diff."""
        self.assertEqual(
            Timing.time_diff("20180106_120000", "20180106_120010"),
            datetime.timedelta(0, 10))
        self.assertEqual(
            Timing.time_diff("20180106_110000", "20180106_120010"),
            datetime.timedelta(0, 3610))

    def test_sort(self):
        """Test sort."""
        self.assertEqual(
            Timing.sort(['20180110_120000', '20180105_120000', '20180101_120000']),
            ['20180101_120000', '20180105_120000', '20180110_120000'])
        self.assertEqual(
            Timing.sort(
                ['20180105_120000', '20180110_120000', '20180105_120000', '20180101_120000']),
            ['20180101_120000', '20180105_120000', '20180105_120000', '20180110_120000'])
        pick_start_date = lambda x: x["start_date"]
        self.assertEqual(
            Timing.sort(
                times=self.__recovery_sample_data(),
                selector=pick_start_date),
            [
                {'is_full': True,  'start_date': '20180101_010000'},
                {'is_full': True,  'start_date': '20180101_010000'},
                {'is_full': True,  'start_date': '20180101_010000'},
                {'is_full': False, 'start_date': '20180101_011000'},
                {'is_full': False, 'start_date': '20180101_012000'},
                {'is_full': False, 'start_date': '20180101_013000'},
                {'is_full': True,  'start_date': '20180101_014000'},
                {'is_full': True,  'start_date': '20180101_014000'},
                {'is_full': True,  'start_date': '20180101_014000'},
                {'is_full': False, 'start_date': '20180101_015000'},
                {'is_full': False, 'start_date': '20180101_020000'},
                {'is_full': False, 'start_date': '20180101_021000'},
                {'is_full': False, 'start_date': '20180101_021000'},
                {'is_full': True,  'start_date': '20180101_022000'},
                {'is_full': True,  'start_date': '20180101_022000'},
                {'is_full': True,  'start_date': '20180101_022000'},
                {'is_full': False, 'start_date': '20180101_023000'},
                {'is_full': False, 'start_date': '20180101_024000'},
                {'is_full': False, 'start_date': '20180101_025000'},
                {'is_full': True,  'start_date': '20180101_030000'},
                {'is_full': True,  'start_date': '20180101_030000'},
                {'is_full': True,  'start_date': '20180101_030000'},
                {'is_full': False, 'start_date': '20180101_031000'},
                {'is_full': False, 'start_date': '20180101_032000'},
                {'is_full': False, 'start_date': '20180101_032000'},
                {'is_full': False, 'start_date': '20180101_033000'}
            ])
        self.assertEquals(
            map(pick_start_date, Timing.sort(times=self.__recovery_sample_data(), selector=pick_start_date)),
            ['20180101_010000', '20180101_010000', '20180101_010000', '20180101_011000', '20180101_012000', '20180101_013000', '20180101_014000', '20180101_014000', '20180101_014000', '20180101_015000', '20180101_020000', '20180101_021000', '20180101_021000', '20180101_022000', '20180101_022000', '20180101_022000', '20180101_023000', '20180101_024000', '20180101_025000', '20180101_030000', '20180101_030000', '20180101_030000', '20180101_031000', '20180101_032000', '20180101_032000', '20180101_033000'])

if __name__ == '__main__':
    unittest.main()
