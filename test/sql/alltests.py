import testbase
import unittest


def suite():
    modules_to_test = (
        'sql.testtypes',
        'sql.constraints',

        # SQL syntax
        'sql.select',
        'sql.selectable',
        'sql.case_statement', 
        'sql.labels',
        'sql.unicode',
        
        # assorted round-trip tests
        'sql.query',
        'sql.quote',
        'sql.rowcount',
        
        # defaults, sequences (postgres/oracle)
        'sql.defaults',
        )
    alltests = unittest.TestSuite()
    for name in modules_to_test:
        mod = __import__(name)
        for token in name.split('.')[1:]:
            mod = getattr(mod, token)
        alltests.addTest(unittest.findTestCases(mod, suiteClass=None))
    return alltests



if __name__ == '__main__':
    testbase.main(suite())
