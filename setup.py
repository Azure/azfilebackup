# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
# --------------------------------------------------------------------------

from setuptools import setup

setup(
    name='azfilebak',
    version='1.0-beta6',
    packages=['azfilebak'],
    description="A backup utility for file systems into Azure blob storage",
    author="Microsoft Corporation",
    author_email='opensource@microsoft.com',
    entry_points={
        'console_scripts': [
            'azfilebak = azfilebak.__main__:main'
        ]
    },
    install_requires=[
        'pid>=2.2.0',
        'azure-storage-common>=1.3.0',
        'azure-storage-blob>=1.3.0,<=2.0.1',
        'msrestazure>=0.4.14',
        'pytz>=2019.1',
        'tzlocal>=1.5.1',
        'psutil>=5.6.6'
    ],
    tests_require=[
        'mock<4.0.0'
    ])
