#!/bin/bash

envname=backup_files

pip install --user virtualenv
~/.local/bin/virtualenv --python=python2.7 ~/${envname}
source ~/${envname}/bin/activate
pip install git+https://github.com/chgeuer/python_backup_files.git#egg=azfilebak

ln -s ~/${envname}/bin/python_backup_files ~/bin
