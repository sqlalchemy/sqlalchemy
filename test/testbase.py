import unittest
import StringIO
import sqlalchemy.engine as engine
import re, sys
import sqlalchemy.databases.postgres as postgres

echo = True
db = None


class PersistTest(unittest.TestCase):
    """persist base class, provides default setUpAll, tearDownAll and echo functionality"""
    def __init__(self, *args, **params):
        unittest.TestCase.__init__(self, *args, **params)
    def echo(self, text):
        if echo:
            print text
    def setUpAll(self):
        pass
    def tearDownAll(self):
        pass


class AssertMixin(PersistTest):
    """given a list-based structure of keys/properties which represent information within an object structure, and
    a list of actual objects, asserts that the list of objects corresponds to the structure."""
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
    def assert_sql(self, db, callable_, list):
        db.set_assert_list(self, list)
        try:
            callable_()
        finally:
            db.set_assert_list(None, None)
        
class EngineAssert(object):
    """decorates a SQLEngine object to match the incoming queries against a set of assertions."""
    def __init__(self, engine):
        self.engine = engine
        self.realexec = engine.execute
        engine.execute = self.execute
        self.echo = engine.echo
        self.logger = engine.logger
        self.set_assert_list(None, None)
    def __getattr__(self, key):
        return getattr(self.engine, key)
    def set_assert_list(self, unittest, list):
        self.unittest = unittest
        self.assert_list = list
        if list is not None:
            self.assert_list.reverse()
    def execute(self, statement, parameters, **kwargs):
        self.engine.echo = self.echo
        self.engine.logger = self.logger
        
        if self.assert_list is not None and not (isinstance(self.engine, postgres.PGSQLEngine) and re.search(r'%\(.*oid\)s', statement, re.S)):
            item = self.assert_list.pop()
            (query, params) = item
            if callable(params):
                params = params()
                
            if isinstance(self.engine, postgres.PGSQLEngine):
                query = re.sub(r':([\w_]+)', r"%(\1)s", query)

            self.unittest.assert_(statement == query and params == parameters, "Testing for query '%s' params %s, received '%s' with params %s" % (query, repr(params), statement, repr(parameters)))
        return self.realexec(statement, parameters, **kwargs)


class TTestSuite(unittest.TestSuite):
    """override unittest.TestSuite to provide per-TestCase class setUpAll() and tearDownAll() functionality"""
    def __init__(self, tests=()):
        if len(tests) >0 and isinstance(tests[0], PersistTest):
            self._initTest = tests[0]
        else:
            self._initTest = None
        unittest.TestSuite.__init__(self, tests)

    def do_run(self, result):
	"""nice job unittest !  you switched __call__ and run() between py2.3 and 2.4 thereby
	making straight subclassing impossible !"""
        for test in self._tests:
            if result.shouldStop:
                break
            test(result)
        return result

    def run(self, result):
        return self(result)

    def __call__(self, result):
        try:
            if self._initTest is not None:
                self._initTest.setUpAll()
        except:
            result.addError(self._initTest, self.__exc_info())
            pass
        try:
            return self.do_run(result)
        finally:
            try:
                if self._initTest is not None:
                    self._initTest.tearDownAll()
            except:
                result.addError(self._initTest, self.__exc_info())
                pass

    def __exc_info(self):
        """Return a version of sys.exc_info() with the traceback frame
           minimised; usually the top level of the traceback frame is not
           needed.
           ripped off out of unittest module since its double __
        """
        exctype, excvalue, tb = sys.exc_info()
        if sys.platform[:4] == 'java': ## tracebacks look different in Jython
            return (exctype, excvalue, tb)
        return (exctype, excvalue, tb)

unittest.TestLoader.suiteClass = TTestSuite
       
(param, DBTYPE) = (sys.argv.pop(1), sys.argv.pop(1))
if (param != '--db'):
    raise "--db <sqlite|postgres|oracle|sqlite_file> param required"
        
if DBTYPE == 'sqlite':
    db = engine.create_engine('sqlite', {'filename':':memory:'}, echo = echo)
elif DBTYPE == 'sqlite_file':
    db = engine.create_engine('sqlite', {'filename':'querytest.db'}, echo = echo)
elif DBTYPE == 'postgres':
    db = engine.create_engine('postgres', {'database':'test', 'host':'127.0.0.1', 'user':'scott', 'password':'tiger'}, echo=echo)

db = EngineAssert(db)

                    
def runTests(suite):
    runner = unittest.TextTestRunner(verbosity = 2, descriptions =1)
    runner.run(suite)
    
def main():
    unittest.main()
