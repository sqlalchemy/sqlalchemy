import testenv; testenv.configure_for_tests()
import doctest, sys, unittest

def suite():
    unittest_modules = ['ext.activemapper',
                        'ext.assignmapper',
                        'ext.declarative',
                        'ext.orderinglist',
                        'ext.associationproxy']

    if sys.version_info >= (2, 4):
        doctest_modules = ['sqlalchemy.ext.sqlsoup']
    else:
        doctest_modules = []

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
    testenv.main(suite())
