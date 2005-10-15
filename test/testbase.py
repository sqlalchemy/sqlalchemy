import unittest
import StringIO
import sqlalchemy.engine as engine

echo = True

class PersistTest(unittest.TestCase):
    def __init__(self, *args, **params):
        unittest.TestCase.__init__(self, *args, **params)
    def echo(self, text):
        if echo:
            print text
    def capture_exec(self, db, callable_):
        e = db.echo
        b = db.logger
        buffer = StringIO.StringIO()
        db.logger = buffer
        db.echo = True
        try:
            callable_()
            if echo:
                print buffer.getvalue()
            return buffer.getvalue()
        finally:
            db.logger = b
            db.echo = e

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
    def assert_enginesql(self, db, callable_, result):
        self.assert_(self.capture_exec(db, callable_) == result, result)

class EngineAssert(object):
    def __init__(self, engine):
        self.__dict__['engine'] = engine
        self.__dict__['realexec'] = engine.execute
        self.set_assert_list(None, None)
    def __getattr__(self, key):
        return getattr(self.engine, key)
    def __setattr__(self, key, value):
        setattr(self.__dict__['engine'], key, value)
    def set_assert_list(self, unittest, list):
        self.__dict__['unittest'] = unittest
        self.__dict__['assert_list'] = list
    def execute(self, statement, parameters, **kwargs):
        # TODO: get this to work
        if self.assert_list is None:
            return
        item = self.assert_list.pop()
        (query, params) = item
        self.unittest.assert_(statement == query and params == parameters)
        return self.realexec(statement, parameters, **kwargs)
        
def runTests(suite):
    runner = unittest.TextTestRunner(verbosity = 2, descriptions =1)
    runner.run(suite)

