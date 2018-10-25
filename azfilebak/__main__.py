#!/usr/bin/env python2.7
# coding=utf-8

# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
# --------------------------------------------------------------------------

from .funcmodule import printe
from .runner import Runner
from .backupexception import BackupException

def main():
    try:
        Runner.main()
        exit(0)
    except BackupException as be:
        printe("{}".format(be.message))
        exit(-1)

if __name__ == '__main__':
    main()
