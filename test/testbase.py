import unittest

echo = True

class PersistTest(unittest.TestCase):
    def __init__(self, *args, **params):
        unittest.TestCase.__init__(self, *args, **params)
    def echo(self, text):
        if echo:
            print text
        

class AssertMixin(PersistTest):
    def assert_result(self, result, class_, *objects):
        if echo:
            print repr(result)
        self.assert_list(result, class_, objects)
    def assert_list(self, result, class_, list):
        self.assert_(len(result) == len(list), "result list is not the same size as test list, for class " + class_.__name__)
        for i in range(0, len(list)):
            self.assert_row(class_, result[i], list[i])
    def assert_row(self, class_, rowobj, desc):
        self.assert_(rowobj.__class__ is class_, "item class is not " + repr(class_))
        for key, value in desc.iteritems():
            if isinstance(value, tuple):
                if isinstance(value[1], list):
                    self.assert_list(getattr(rowobj, key), value[0], value[1])
                else:
                    self.assert_row(value[0], getattr(rowobj, key), value[1])
            else:
                self.assert_(getattr(rowobj, key) == value, "attribute %s value %s does not match %s" % (key, getattr(rowobj, key), value))

def runTests(suite):
    runner = unittest.TextTestRunner(verbosity = 2, descriptions =1)
    runner.run(suite)

