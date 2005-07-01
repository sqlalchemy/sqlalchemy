import unittest


class PersistTest(unittest.TestCase):
    def __init__(self, *args, **params):
        unittest.TestCase.__init__(self, *args, **params)
        

def runTests(suite):
    runner = unittest.TextTestRunner(verbosity = 2, descriptions =1)
    runner.run(suite)

