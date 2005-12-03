import testbase
import unittest

testbase.echo = False

def suite():
    modules_to_test = ('attributes', 'historyarray', 'pool', 'engines', 'query', 'columns', 'sequence', 'select', 'types', 'mapper', 'objectstore', 'manytomany', 'dependency')
#    modules_to_test = ('engines', 'mapper')
    alltests = unittest.TestSuite()
    for module in map(__import__, modules_to_test):
        alltests.addTest(unittest.findTestCases(module, suiteClass=None))
    return alltests

if __name__ == '__main__':
    testbase.runTests(suite())
