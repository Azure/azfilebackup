# Filesystem backup utility for Azure Blob Storage

[![Build Status](https://dev.azure.com/tcontemsft/tconte/_apis/build/status/tomconte.azfilebak)](https://dev.azure.com/tcontemsft/tconte/_build/latest?definitionId=1)

This Python-based tool uses `tar` to perform backups of a file system and upload the archive to Azure Blob Storage. In most cases, you should first evaluate [Azure Backup](https://azure.microsoft.com/en-us/services/backup/) which is a fully-managed backup service for virtual machines running both in Azure and on-premises. If for any reason Azure Backup is not applicable in your environment, this tool might be useful to you.

## Current status: **alpha**

## Features and limitations

This tool requires Python 2.7 and should work on any Linux distribution. It was extensively tested on SUSE Enterprise Linux 12.

Features:

- Perform full backups
- Can be automated via `cron`
- Configure file system to backup and files to exclude
- Storage account to use is determined by an instance tag
- Storage container to use is determined by configuration file
- Backup schedule is defined using instance tags

Limitations:

- Azure virtual machine: this tool is designed to run on an Azure virtual machine. It uses the Azure environment, like tags in the instance metadata, to determine how it should run.

- Azure Blob Storage: this tool is designed to upload the backup archives to an Azure Blob Storage account. It uses [managed identities](https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/overview) for authentication, which means that storage credentials don't need to be saved on the machine.

## Installation using `pip`

### Dependency to `psutil`

The tool has a dependency to the [`psutils`](https://psutil.readthedocs.io/en/latest/) Python library. This library only provides a source distribution, which means that it must be compiled on the target machine. If the target machine does not have development tools installed, the installation of the `psutil` library will fail. Please install the development tools before installing using the `pip` method.

### Using `virtualenv`

The less intrusive way to install and test the tool is to use `virtualenv`. Here is how you can create and activate a virtual environment:

```shell
pip install --user virtualenv
~/.local/bin/virtualenv --python=python2.7 ~/azfilebak
source ~/azfilebak/bin/activate
```

### Install the tool

You can install directly from a release URL:

```
pip install https://github.com/Azure/azfilebackup/releases/download/v1.0-alpha1/azfilebak-1.0a1.tar.gz
```

Or if you downloaded the distribution archive locally:

```
pip install dist/azfilebak-0.0.1.tar.gz
```

## Configuration

### Storage account and managed identity

You will need an Azure Storage account to store the archives, and you will need to configure a system-assigned managed identity to authorize the virtual machine to access the storage account. You can follow this tutorial to set this up: [Use a Linux VM system-assigned managed identity to access Azure Storage](https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/tutorial-linux-vm-access-storage).

### Tags

Tags are used to govern the backup schedule and configure certain parameters. This allows controlling the backup process without having to modify the configuration file on the machine. The files `test-set-vm-tags-arm.json` and `test-set-vm-tags.sh` how tags should be defined.

## Usage

Run a full backup now, disregarding the schedule constraints:

```
sudo $HOME/azfilebak/bin/azfilebak --full --force
```

## Development

The tool requires Python 2.7.

Install requirements:

```
pip install -r requirements.txt
```

Run tests:

```
make test
```

### Tests

Some of the tests need to access a real Azure storage account. The name of the account to use can be changed in the file `sample_instance_metadata.json`:

```
"tags": "StorageAccount:sahec99az1backup0001;
```

The storage account key can be specified via an environment variable:

```
export STORAGE_KEY='xxx'
```

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
