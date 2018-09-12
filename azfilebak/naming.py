# coding=utf-8
"""Timing module."""

import os
import re

class Naming(object):
    """Utility functions to generate file and blob names."""

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
    def construct_filename(fileset, is_full, start_timestamp):
        """
        >>> Naming.construct_filename(fileset="test1fs", is_full=True, start_timestamp="20180601_112429")
        'test1fs_full_20180601_112429.tar'
        >>> Naming.construct_filename(fileset="test1fs", is_full=False, start_timestamp="20180601_112429")
        'test1fs_tran_20180601_112429.tar'
        """
        return "{fileset}_{type}_{start_timestamp}.tar".format(
            fileset=fileset,
            type=Naming.backup_type_str(is_full),
            start_timestamp=start_timestamp)

    @staticmethod
    def local_filesystem_name(directory, fileset, is_full, start_timestamp):
        """
        Construct file name with directory.

        >>> Naming.local_filesystem_name(directory="/tmp", fileset="test1fs", is_full=True, start_timestamp="20180601_112429")
        '/tmp/test1fs_full_20180601_112429.tar'
        """
        file_name = Naming.construct_filename(
            fileset, is_full, start_timestamp)
        return os.path.join(directory, file_name)

    @staticmethod
    def construct_blobname_prefix(fileset, is_full):
        """
        >>> Naming.construct_blobname_prefix(fileset="test1fs", is_full=True)
        'test1fs_full_'
        """
        return "{fileset}_{type}_".format(fileset=fileset, type=Naming.backup_type_str(is_full))

    @staticmethod
    def construct_blobname(fileset, is_full, start_timestamp, end_timestamp):
        """
        >>> Naming.construct_blobname(fileset="test1fs", is_full=True, start_timestamp="20180601_112429", end_timestamp="20180601_131234")
        'test1fs_full_20180601_112429--20180601_131234.tar'
        """
        return "{fileset}_{type}_{start}--{end}.tar".format(
            fileset=fileset,
            type=Naming.backup_type_str(is_full),
            start=start_timestamp, end=end_timestamp)

    @staticmethod
    def parse_filename(filename):
        """
        >>> Naming.parse_filename('test1fs_full_20180601_112429.tar')
        ('test1fs', True, '20180601_112429')
        >>> Naming.parse_filename('test1fs_tran_20180601_112429.tar')
        ('test1fs', False, '20180601_112429')
        >>> Naming.parse_filename('bad_input') == None
        True
        """
        m = re.search(r'(?P<fileset>\S+?)_(?P<type>full|tran)_(?P<start>\d{8}_\d{6})\.tar', filename)
        if m is None:
            return None

        (fileset, is_full, start_timestamp) = (m.group('fileset'), Naming.type_str_is_full(m.group('type')), m.group('start'))

        return fileset, is_full, start_timestamp

    @staticmethod
    def parse_blobname(filename):
        """
        >>> Naming.parse_blobname('test1fs_full_20180601_112429--20180601_131234.tar')
        ('test1fs', True, '20180601_112429', '20180601_131234')
        >>> Naming.parse_blobname('test1fs_tran_20180601_112429--20180601_131234.tar')
        ('test1fs', False, '20180601_112429', '20180601_131234')
        >>> Naming.parse_filename('bad_input') == None
        True
        """
        m = re.search(r'(?P<fileset>\S+?)_(?P<type>full|tran)_(?P<start>\d{8}_\d{6})--(?P<end>\d{8}_\d{6})\.tar', filename)
        if m is None:
            return None

        (fileset, is_full, start_timestamp, end_timestamp) = (m.group('fileset'), Naming.type_str_is_full(m.group('type')), m.group('start'), m.group('end'))

        return fileset, is_full, start_timestamp, end_timestamp

    @staticmethod
    def blobname_to_filename(blobname):
        """Convert blob name to file name."""
        parts = Naming.parse_blobname(blobname)
        return Naming.construct_filename(
            fileset=parts[0],
            is_full=parts[1],
            start_timestamp=parts[2])

    @staticmethod
    def temp_container_name(fileset, start_timestamp):
        """Temporary container name to upload the blob to."""
        return "tmp-{fileset}-{start_timestamp}".format(
            fileset=fileset,
            start_timestamp=start_timestamp
            ).replace("_", "-").lower()
