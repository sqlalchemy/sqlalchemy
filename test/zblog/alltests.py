import testenv; testenv.configure_for_tests()
from testlib import sa_unittest as unittest

def suite():
    modules_to_test = (
        'zblog.tests',
        )
    alltests = unittest.TestSuite()
    for name in modules_to_test:
        mod = __import__(name)
        for token in name.split('.')[1:]:
            mod = getattr(mod, token)
        alltests.addTest(unittest.findTestCases(mod, suiteClass=None))
    return alltests


if __name__ == '__main__':
    testenv.main(suite())
