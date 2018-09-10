from setuptools import setup

setup(
    name = 'azfilebak',
    version = '0.0.1',
    packages = ['azfilebak'],
    description="A backup utility for file systems into Azure blob storage",
    author="Dr. Christian Geuer-Pollmann",
    author_email='chgeuer@microsoft.com',
    entry_points = {
        'console_scripts': [
            'azfilebak = azfilebak.__main__:main'
        ]
    },
    install_requires=[
        'pid>=2.2.0',
        'azure-storage-common>=1.3.0',
        'azure-storage-blob>=1.3.0',
        'msrestazure>=0.4.14'
    ],
    tests_require=[
        'mock'
    ])
