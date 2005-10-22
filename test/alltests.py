import unittest
import testbase

testbase.echo = False


modules_to_test = ('attributes', 'historyarray', 'pool', 'engines', 'query', 'types', 'mapper', 'objectstore')

if __name__ == '__main__':
    testbase.runTests(suite())
