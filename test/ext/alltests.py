import testbase
import unittest, doctest

def suite():
    unittest_modules = ['ext.activemapper',
                        'ext.selectresults',
                        'ext.assignmapper',
                        'ext.orderinglist',
                        'ext.associationproxy']
    doctest_modules = ['sqlalchemy.ext.sqlsoup']

    alltests = unittest.TestSuite()
    for name in unittest_modules:
        mod = __import__(name)
        for token in name.split('.')[1:]:
            mod = getattr(mod, token)
        alltests.addTest(unittest.findTestCases(mod, suiteClass=None))
    for name in doctest_modules:
        alltests.addTest(doctest.DocTestSuite(name))
    return alltests


if __name__ == '__main__':
    testbase.main(suite())
