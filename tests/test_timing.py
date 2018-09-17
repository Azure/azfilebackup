"""Unit tests for timing."""
import time
import datetime
import unittest
from azfilebak.timing import Timing

class TestTiming(unittest.TestCase):
    """Unit tests for class Timing."""

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
        pick_end_date = lambda x: x["end_date"]
        self.assertEqual(
            Timing.sort(
                times=self.__recovery_sample_data(),
                selector=pick_end_date),
            [
                {'is_full': True, 'stripe_index': 1, 'end_date': '20180101_010000', 'stripe_count': 3},
                {'is_full': True, 'stripe_index': 2, 'end_date': '20180101_010000', 'stripe_count': 3},
                {'is_full': True, 'stripe_index': 3, 'end_date': '20180101_010000', 'stripe_count': 3},
                {'is_full': False, 'stripe_index': 1, 'end_date': '20180101_011000', 'stripe_count': 1},
                {'is_full': False, 'stripe_index': 1, 'end_date': '20180101_012000', 'stripe_count': 1},
                {'is_full': False, 'stripe_index': 1, 'end_date': '20180101_013000', 'stripe_count': 1},
                {'is_full': True, 'stripe_index': 1, 'end_date': '20180101_014000', 'stripe_count': 3},
                {'is_full': True, 'stripe_index': 2, 'end_date': '20180101_014000', 'stripe_count': 3},
                {'is_full': True, 'stripe_index': 3, 'end_date': '20180101_014000', 'stripe_count': 3},
                {'is_full': False, 'stripe_index': 1, 'end_date': '20180101_015000', 'stripe_count': 1},
                {'is_full': False, 'stripe_index': 1, 'end_date': '20180101_020000', 'stripe_count': 1},
                {'is_full': False, 'stripe_index': 1, 'end_date': '20180101_021000', 'stripe_count': 2},
                {'is_full': False, 'stripe_index': 2, 'end_date': '20180101_021000', 'stripe_count': 2},
                {'is_full': True, 'stripe_index': 1, 'end_date': '20180101_022000', 'stripe_count': 3},
                {'is_full': True, 'stripe_index': 2, 'end_date': '20180101_022000', 'stripe_count': 3},
                {'is_full': True, 'stripe_index': 3, 'end_date': '20180101_022000', 'stripe_count': 3},
                {'is_full': False, 'stripe_index': 1, 'end_date': '20180101_023000', 'stripe_count': 1},
                {'is_full': False, 'stripe_index': 1, 'end_date': '20180101_024000', 'stripe_count': 1},
                {'is_full': False, 'stripe_index': 1, 'end_date': '20180101_025000', 'stripe_count': 1},
                {'is_full': True, 'stripe_index': 1, 'end_date': '20180101_030000', 'stripe_count': 3},
                {'is_full': True, 'stripe_index': 2, 'end_date': '20180101_030000', 'stripe_count': 3},
                {'is_full': True, 'stripe_index': 3, 'end_date': '20180101_030000', 'stripe_count': 3},
                {'is_full': False, 'stripe_index': 1, 'end_date': '20180101_031000', 'stripe_count': 1},
                {'is_full': False, 'stripe_index': 1, 'end_date': '20180101_032000', 'stripe_count': 2},
                {'is_full': False, 'stripe_index': 2, 'end_date': '20180101_032000', 'stripe_count': 2},
                {'is_full': False, 'stripe_index': 1, 'end_date': '20180101_033000', 'stripe_count': 1}
            ])
        self.assertEquals(
            map(pick_end_date, Timing.sort(times=self.__recovery_sample_data(), selector=pick_end_date)),
            ['20180101_010000', '20180101_010000', '20180101_010000', '20180101_011000', '20180101_012000', '20180101_013000', '20180101_014000', '20180101_014000', '20180101_014000', '20180101_015000', '20180101_020000', '20180101_021000', '20180101_021000', '20180101_022000', '20180101_022000', '20180101_022000', '20180101_023000', '20180101_024000', '20180101_025000', '20180101_030000', '20180101_030000', '20180101_030000', '20180101_031000', '20180101_032000', '20180101_032000', '20180101_033000'])

    def test_files_needed_for_recovery(self):
        """Test files_needed_for_recovery."""
        times = self.__recovery_sample_data()
        self.assertEqual(
            Timing.files_needed_for_recovery(times=times, restore_point='20180101_023200'),
            [
                {'is_full': True, 'stripe_index': 1, 'end_date': '20180101_022000', 'stripe_count': 3},
                {'is_full': True, 'stripe_index': 2, 'end_date': '20180101_022000', 'stripe_count': 3},
                {'is_full': True, 'stripe_index': 3, 'end_date': '20180101_022000', 'stripe_count': 3},
                {'is_full': False, 'stripe_index': 1, 'end_date': '20180101_023000', 'stripe_count': 1},
                {'is_full': False, 'stripe_index': 1, 'end_date': '20180101_024000', 'stripe_count': 1}
            ])

if __name__ == '__main__':
    unittest.main()
