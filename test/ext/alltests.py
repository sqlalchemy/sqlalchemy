import testenv; testenv.configure_for_tests()
import doctest, sys, unittest


def suite():
    unittest_modules = (
        'ext.declarative',
        'ext.orderinglist',
        'ext.associationproxy',
        'ext.serializer',
        )

    if sys.version_info < (2, 4):
        doctest_modules = ()
    else:
        doctest_modules = (
            ('sqlalchemy.ext.orderinglist', {'optionflags': doctest.ELLIPSIS}),
            ('sqlalchemy.ext.sqlsoup', {})
            )

    alltests = unittest.TestSuite()
    for name in unittest_modules:
        mod = __import__(name)
        for token in name.split('.')[1:]:
            mod = getattr(mod, token)
        alltests.addTest(unittest.findTestCases(mod, suiteClass=None))
    for name, opts in doctest_modules:
        alltests.addTest(doctest.DocTestSuite(name, **opts))
    return alltests


if __name__ == '__main__':
    testenv.main(suite())
