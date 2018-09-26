# Filesystem backup utility for Azure Blob Storage

This tool uses tar to perform backups of a file system and upload the archive to Azure Blob Storage.

## Installation

### Using `virtualenv`

The less intrusive way to install the tool is to use `virtualenv`:

```shell
pip install --user virtualenv
~/.local/bin/virtualenv --python=python2.7 ~/azfilebak
source ~/azfilebak/bin/activate
```

### psutil

If the target machine does not have development tools installed, the installation of the `psutil` library will fail.

A binary release of `psutil` is provided for your convenience. Install it using `pip` before running the next step.

```
pip install https://github.com/tomconte/azfilebak/raw/master/bdist/psutil-5.4.7-cp27-cp27mu-linux_x86_64.whl
```

### Install the tool

You can install directly from a release URL:

```
pip install https://link-to-release-tbd.com/release/azfilebak-0.0.1.tar.gz
```

Or if you downloaded the distribution archive locally:

```
pip install dist/azfilebak-0.0.1.tar.gz
```

## Usage

```
sudo $HOME/azfilebak/bin/azfilebak --full --force
```
