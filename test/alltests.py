import testbase
import unittest

def suite():
    modules_to_test = (
        # core utilities
        'historyarray',
        'attributes', 
        'dependency',

        # connectivity, execution
        'pool', 
        'transaction',
        
        # schema/tables
        'reflection', 
        'testtypes',
        'indexes',

        # SQL syntax
        'select',
        'selectable',
        'case_statement', 
        
        # assorted round-trip tests
        'query',
        
        # defaults, sequences (postgres/oracle)
        'defaults',
        
        # ORM selecting
        'mapper',
        'selectresults',
        'lazytest1',
        'eagertest1',
        'eagertest2',
        
        # ORM persistence
        'sessioncontext', 
        'objectstore',
        'cascade',
        'relationships',
        'association',
        
        # cyclical ORM persistence
        'cycles',
        
        # more select/persistence, backrefs
        'entity',
        'manytomany',
        'onetoone',
        'inheritance',
        'inheritance2',
        'polymorph',
        
        # extensions
        'proxy_engine',
        'activemapper',
        'sqlsoup'
        
        #'wsgi_test',
        
        )
    alltests = unittest.TestSuite()
    for module in map(__import__, modules_to_test):
        alltests.addTest(unittest.findTestCases(module, suiteClass=None))
    return alltests


if __name__ == '__main__':
    testbase.runTests(suite())
