# coding=utf-8

# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
# --------------------------------------------------------------------------

from __future__ import print_function
import sys
import logging

def printe(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def out(message):
    logging.info(message)
    print(message)

def log_stdout_stderr(stdout, stderr):
    if len(stdout) > 0:
        for l in stdout.split("\n"):
            logging.info(l)
    if len(stderr) > 0:
        for l in stderr.split("\n"):
            logging.warning(l)

