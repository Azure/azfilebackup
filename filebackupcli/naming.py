# coding=utf-8

import os
import re

from .timing import Timing
from .backupexception import BackupException

class Naming:
    @staticmethod
    def backup_type_str(is_full):
        """
            >>> Naming.backup_type_str(is_full=True)
            'full'
            >>> Naming.backup_type_str(is_full=False)
            'tran'
        """
        return ({True:"full", False:"tran"})[is_full]

    @staticmethod
    def type_str_is_full(type_str):
        """
            >>> Naming.type_str_is_full('full')
            True
            >>> Naming.type_str_is_full('tran')
            False
        """
        return ({"full":True, "tran":False})[type_str]

    @staticmethod
    def construct_filename(dbname, is_full, start_timestamp, stripe_index, stripe_count):
        """
            >>> Naming.construct_filename(dbname="test1db", is_full=True, start_timestamp="20180601_112429", stripe_index=2, stripe_count=101)
            'test1db_full_20180601_112429_S002-101.cdmp'
            >>> Naming.construct_filename(dbname="test1db", is_full=False, start_timestamp="20180601_112429", stripe_index=2, stripe_count=101)
            'test1db_tran_20180601_112429_S002-101.cdmp'
        """
        return "{dbname}_{type}_{start_timestamp}_S{idx:03d}-{cnt:03d}.cdmp".format(
            dbname=dbname, 
            type=Naming.backup_type_str(is_full), 
            start_timestamp=start_timestamp,
            idx=int(stripe_index), cnt=int(stripe_count))

    @staticmethod
    def local_filesystem_name(directory, dbname, is_full, start_timestamp, stripe_index, stripe_count):
        file_name = Naming.construct_filename(
            dbname, is_full, start_timestamp, stripe_index, stripe_count)
        return os.path.join(directory, file_name)

    @staticmethod
    def pipe_name(output_dir, dbname, is_full, stripe_index, stripe_count):
        return os.path.join(output_dir, "backup_{}_{}_{:03d}_{:03d}.cdmp_pipe".format(
            dbname, Naming.backup_type_str(is_full), stripe_index, stripe_count))

    @staticmethod
    def pipe_names(dbname, is_full, stripe_count, output_dir):
        return map(lambda stripe_index: Naming.pipe_name(
                output_dir=output_dir, dbname=dbname, is_full=is_full, 
                stripe_index=stripe_index, stripe_count=stripe_count), 
            range(1, stripe_count + 1))

    @staticmethod
    def construct_blobname_prefix(dbname, is_full):
        """
            >>> Naming.construct_blobname_prefix(dbname="test1db", is_full=True)
            'test1db_full_'
        """
        return "{dbname}_{type}_".format(dbname=dbname, type=Naming.backup_type_str(is_full))

    @staticmethod
    def construct_blobname(dbname, is_full, start_timestamp, end_timestamp, stripe_index, stripe_count):
        """
            >>> Naming.construct_blobname(dbname="test1db", is_full=True, start_timestamp="20180601_112429", end_timestamp="20180601_131234", stripe_index=2, stripe_count=101)
            'test1db_full_20180601_112429--20180601_131234_S002-101.cdmp'
        """
        return "{dbname}_{type}_{start}--{end}_S{idx:03d}-{cnt:03d}.cdmp".format(
            dbname=dbname, 
            type=Naming.backup_type_str(is_full), 
            start=start_timestamp, end=end_timestamp,
            idx=int(stripe_index), cnt=int(stripe_count))

    @staticmethod
    def construct_ddlgen_name(dbname, start_timestamp):
        return "{dbname}_ddlgen_{start}.sql".format(
            dbname=dbname, start=start_timestamp)

    @staticmethod
    def parse_filename(filename):
        """
            >>> Naming.parse_filename('test1db_full_20180601_112429_S002-101.cdmp')
            ('test1db', True, '20180601_112429', 2, 101)
            >>> Naming.parse_filename('test1db_tran_20180601_112429_S02-08.cdmp')
            ('test1db', False, '20180601_112429', 2, 8)
            >>> Naming.parse_filename('bad_input') == None
            True
        """
        m=re.search(r'(?P<dbname>\S+?)_(?P<type>full|tran)_(?P<start>\d{8}_\d{6})_S(?P<idx>\d+)-(?P<cnt>\d+)\.cdmp', filename)
        if (m == None):
            return None

        (dbname, is_full, start_timestamp, stripe_index, stripe_count) = (m.group('dbname'), Naming.type_str_is_full(m.group('type')), m.group('start'), int(m.group('idx')), int(m.group('cnt')))

        return dbname, is_full, start_timestamp, stripe_index, stripe_count

    @staticmethod
    def parse_blobname(filename):
        """
            >>> Naming.parse_blobname('test1db_full_20180601_112429--20180601_131234_S002-101.cdmp')
            ('test1db', True, '20180601_112429', '20180601_131234', 2, 101)
            >>> Naming.parse_blobname('test1db_tran_20180601_112429--20180601_131234_S2-008.cdmp')
            ('test1db', False, '20180601_112429', '20180601_131234', 2, 8)
            >>> Naming.parse_filename('bad_input') == None
            True
        """
        m=re.search(r'(?P<dbname>\S+?)_(?P<type>full|tran)_(?P<start>\d{8}_\d{6})--(?P<end>\d{8}_\d{6})_S(?P<idx>\d+)-(?P<cnt>\d+)\.cdmp', filename)
        if (m == None):
            return None

        (dbname, is_full, start_timestamp, end_timestamp, stripe_index, stripe_count) = (m.group('dbname'), Naming.type_str_is_full(m.group('type')), m.group('start'), m.group('end'), int(m.group('idx')), int(m.group('cnt')))

        return dbname, is_full, start_timestamp, end_timestamp, stripe_index, stripe_count

    @staticmethod
    def blobname_to_filename(blobname):
        parts = Naming.parse_blobname(blobname)
        return Naming.construct_filename(
                    dbname=parts[0],
                    is_full=parts[1],
                    start_timestamp=parts[2],
                    # skip parts[3] which is end-timestamp
                    stripe_index=parts[4],
                    stripe_count=parts[5])
