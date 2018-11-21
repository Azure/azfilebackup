# coding=utf-8

# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
# --------------------------------------------------------------------------

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
            'incr'
        """
        return ({True:"full", False:"incr"})[is_full]

    @staticmethod
    def type_str_is_full(type_str):
        """
            >>> Naming.type_str_is_full('full')
            True
            >>> Naming.type_str_is_full('incr')
            False
        """
        return ({"full":True, "incr":False})[type_str]

    @staticmethod
    def construct_filename(fileset, is_full, start_timestamp, vmname):
        """
        >>> Naming.construct_filename(fileset="test1fs", is_full=True, start_timestamp="20180601_112429", vmname="vm1")
        'test1fs_vm1_full_20180601_112429.tar.gz'
        >>> Naming.construct_filename(fileset="test1fs", is_full=False, start_timestamp="20180601_112429", vmname="vm1")
        'test1fs_vm1_incr_20180601_112429.tar.gz'
        """
        return "{fileset}_{vmname}_{type}_{start_timestamp}.tar.gz".format(
            fileset=fileset,
            vmname=vmname,
            type=Naming.backup_type_str(is_full),
            start_timestamp=start_timestamp)

    @staticmethod
    def local_filesystem_name(directory, fileset, is_full, start_timestamp, vmname):
        """
        Construct file name with directory.

        >>> Naming.local_filesystem_name(directory="/tmp", fileset="test1fs", is_full=True, start_timestamp="20180601_112429", vmname="vm1")
        '/tmp/test1fs_vm1_full_20180601_112429.tar.gz'
        """
        file_name = Naming.construct_filename(
            fileset, is_full, start_timestamp, vmname)
        return os.path.join(directory, file_name)

    @staticmethod
    def construct_blobname_prefix(fileset, is_full, vmname):
        """
        >>> Naming.construct_blobname_prefix(fileset="test1fs", is_full=True, vmname="vm1")
        'test1fs_vm1_full_'
        """
        return "{fileset}_{vmname}_{type}_".format(fileset=fileset, vmname=vmname, type=Naming.backup_type_str(is_full))

    @staticmethod
    def construct_blobname(fileset, is_full, start_timestamp, vmname):
        """
        >>> Naming.construct_blobname(fileset="test1fs", is_full=True, start_timestamp="20180601_112429", vmname="vm1")
        'test1fs_vm1_full_20180601_112429.tar.gz'
        """
        return "{fileset}_{vmname}_{type}_{start}.tar.gz".format(
            fileset=fileset,
            vmname=vmname,
            type=Naming.backup_type_str(is_full),
            start=start_timestamp)

    @staticmethod
    def parse_filename(filename):
        """
        >>> Naming.parse_filename('test1fs_vm1_full_20180601_112429.tar.gz')
        ('test1fs', True, '20180601_112429', 'vm1')
        >>> Naming.parse_filename('test1fs_vm1_incr_20180601_112429.tar.gz')
        ('test1fs', False, '20180601_112429', 'vm1')
        >>> Naming.parse_filename('bad_input') == None
        True
        """
        match = re.search(r'(?P<fileset>\S+?)_(?P<vmname>\S+?)_(?P<type>full|incr)_(?P<start>\d{8}_\d{6})\.tar.gz', filename)
        if match is None:
            return None

        (fileset, is_full, start_timestamp, vmname) = (
            match.group('fileset'),
            Naming.type_str_is_full(match.group('type')),
            match.group('start'),
            match.group('vmname'))

        return fileset, is_full, start_timestamp, vmname

    @staticmethod
    def parse_blobname(filename):
        """
        >>> Naming.parse_blobname('test1fs_vm1_full_20180601_112429.tar.gz')
        ('test1fs', True, '20180601_112429', 'vm1')
        >>> Naming.parse_blobname('test1fs_vm1_incr_20180601_112429.tar.gz')
        ('test1fs', False, '20180601_112429', 'vm1')
        >>> Naming.parse_filename('bad_input') == None
        True
        """
        match = re.search(r'(?P<fileset>\S+?)_(?P<vmname>\S+?)_(?P<type>full|incr)_(?P<start>\d{8}_\d{6})\.tar.gz', filename)
        if match is None:
            return None

        (fileset, is_full, start_timestamp, vmname) = (
            match.group('fileset'),
            Naming.type_str_is_full(match.group('type')),
            match.group('start'),
            match.group('vmname'))

        return fileset, is_full, start_timestamp, vmname

    @staticmethod
    def blobname_to_filename(blobname):
        """Convert blob name to file name."""
        parts = Naming.parse_blobname(blobname)
        return Naming.construct_filename(
            fileset=parts[0],
            is_full=parts[1],
            start_timestamp=parts[2],
            vmname=parts[3])

    @staticmethod
    def temp_container_name(fileset, start_timestamp):
        """Temporary container name to upload the blob to."""
        return "tmp-{fileset}-{start_timestamp}".format(
            fileset=fileset,
            start_timestamp=start_timestamp
            ).replace("_", "-").lower()
