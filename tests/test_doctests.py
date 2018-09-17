import unittest
import doctest
from azfilebak import azurevminstancemetadata
from azfilebak import backupconfiguration
from azfilebak import backupconfigurationfile
from azfilebak import businesshours
from azfilebak import naming
from azfilebak import scheduleparser
from azfilebak import timing
from azfilebak import backupagent

def load_tests(loader, tests, ignore):
    tests.addTests(doctest.DocTestSuite(azurevminstancemetadata))
    #tests.addTests(doctest.DocTestSuite(backupconfiguration))
    tests.addTests(doctest.DocTestSuite(backupconfigurationfile))
    tests.addTests(doctest.DocTestSuite(businesshours))
    tests.addTests(doctest.DocTestSuite(naming))
    tests.addTests(doctest.DocTestSuite(scheduleparser))
    tests.addTests(doctest.DocTestSuite(timing))
    tests.addTests(doctest.DocTestSuite(backupagent))
    return tests
