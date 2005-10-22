import unittest
import testbase

testbase.echo = False

def suite():
    modules_to_test = ('attributes', 'historyarray', 'pool', 'engines', 'query', 'types', 'mapper', 'objectstore')
    alltests = unittest.TestSuite()
    for module in map(__import__, modules_to_test):
        alltests.addTest(unittest.findTestCases(module))
    return alltests

if __name__ == '__main__':
    testbase.runTests(suite())
