# Filesystem backup utility for Azure Blob Storage

[![Build Status](https://dev.azure.com/tcontemsft/tconte/_apis/build/status/tomconte.azfilebak)](https://dev.azure.com/tcontemsft/tconte/_build/latest?definitionId=1)

This tool uses tar to perform backups of a file system and upload the archive to Azure Blob Storage.

## Installation

### Using `virtualenv`

The less intrusive way to install the tool is to use `virtualenv`:

```shell
pip install --user virtualenv
~/.local/bin/virtualenv --python=python2.7 ~/azfilebak
source ~/azfilebak/bin/activate
```

### psutil

If the target machine does not have development tools installed, the installation of the `psutil` library will fail.

A binary release of `psutil` is provided for your convenience. Install it using `pip` before running the next step.

```
pip install https://github.com/tomconte/azfilebak/raw/master/bdist/psutil-5.4.7-cp27-cp27mu-linux_x86_64.whl
```

### Install the tool

You can install directly from a release URL:

```
pip install https://github.com/tomconte/azfilebak/releases/download/v1.0-alpha1/azfilebak-1.0a1.tar.gz
```

Or if you downloaded the distribution archive locally:

```
pip install dist/azfilebak-0.0.1.tar.gz
```

## Usage

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
