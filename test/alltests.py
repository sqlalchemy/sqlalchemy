import testbase
import unittest

testbase.echo = False

#test

def suite():
    modules_to_test = (
        # core utilities
        'historyarray', 
        'attributes', 
        'dependency',
        
        # connectivity
        'pool', 
        
        # schema/tables
        'engines', 
        'testtypes',
        'indexes',

        # SQL syntax
        'select',
        'selectable',
        
        # assorted round-trip tests
        'query',
        
        # defaults, sequences (postgres/oracle)
        'defaults',
        
        # ORM selecting
        'mapper',
        'eagertest1',
        'eagertest2',
        
        # ORM persistence
        'objectstore',
        
        # cyclical ORM persistence
        'cycles',
        
        # more select/persistence, backrefs
        'manytomany',
        'onetoone',
        'inheritance',
        
        # extensions
        'proxy_engine',
        #'wsgi_test',
        
        )
    alltests = unittest.TestSuite()
    for module in map(__import__, modules_to_test):
        alltests.addTest(unittest.findTestCases(module, suiteClass=None))
    return alltests

import sys
sys.stdout = sys.stderr

if __name__ == '__main__':
    testbase.runTests(suite())
