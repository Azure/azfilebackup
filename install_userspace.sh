#!/bin/bash

envname=azfilebak

pip install --user virtualenv
~/.local/bin/virtualenv --python=python2.7 ~/${envname}
source ~/${envname}/bin/activate
pip install git+https://github.com/chgeuer/azfilebak.git#egg=azfilebak

ln -s ~/${envname}/bin/azfilebak ~/bin
