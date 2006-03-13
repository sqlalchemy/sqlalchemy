import unittest
import StringIO
import sqlalchemy.engine as engine
import sqlalchemy.ext.proxy as proxy
import sqlalchemy.schema as schema
import re, sys

echo = True
#echo = False
#echo = 'debug'
db = None
db_uri = None

def parse_argv():
    # we are using the unittest main runner, so we are just popping out the 
    # arguments we need instead of using our own getopt type of thing
    global db, db_uri
    
    DBTYPE = 'sqlite'
    PROXY = False
    
    if len(sys.argv) >= 3:
        if sys.argv[1] == '--dburi':
            (param, db_uri) =  (sys.argv.pop(1), sys.argv.pop(1))
        elif sys.argv[1] == '--db':
            (param, DBTYPE) = (sys.argv.pop(1), sys.argv.pop(1))

    
    if (None == db_uri):
        p = DBTYPE.split('.')
        if len(p) > 1:
            arg = p[0]
            DBTYPE = p[1]
            if arg == 'proxy':
                PROXY = True
        if DBTYPE == 'sqlite':
            db_uri = 'sqlite://filename=:memory:'
        elif DBTYPE == 'sqlite_file':
            db_uri = 'sqlite://filename=querytest.db'
        elif DBTYPE == 'postgres':
            db_uri = 'postgres://database=test&port=5432&host=127.0.0.1&user=scott&password=tiger'
        elif DBTYPE == 'mysql':
            db_uri = 'mysql://database=test&host=127.0.0.1&user=scott&password=tiger'
        elif DBTYPE == 'oracle':
            db_uri = 'oracle://user=scott&password=tiger'

    if not db_uri:
        raise "Could not create engine.  specify --db <sqlite|sqlite_file|postgres|mysql|oracle> to test runner."

    if PROXY:
        db = proxy.ProxyEngine(echo=echo, default_ordering=True)
        db.connect(db_uri)
    else:
        db = engine.create_engine(db_uri, echo=echo, default_ordering=True)
    db = EngineAssert(db)

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
    def assert_sql(self, db, callable_, list, with_sequences=None):
        if with_sequences is not None and (db.engine.name == 'postgres' or db.engine.name == 'oracle'):
            db.set_assert_list(self, with_sequences)
        else:
            db.set_assert_list(self, list)
        try:
            callable_()
        finally:
            db.set_assert_list(None, None)
    def assert_sql_count(self, db, callable_, count):
        db.sql_count = 0
        try:
            callable_()
        finally:
            self.assert_(db.sql_count == count, "desired statement count %d does not match %d" % (count, db.sql_count))

class EngineAssert(proxy.BaseProxyEngine):
    """decorates a SQLEngine object to match the incoming queries against a set of assertions."""
    def __init__(self, engine):
        self._engine = engine
        self.realexec = engine.post_exec
        self.realexec.im_self.post_exec = self.post_exec
        self.logger = engine.logger
        self.set_assert_list(None, None)
        self.sql_count = 0
    def get_engine(self):
        return self._engine
    def set_engine(self, e):
        self._engine = e
#    def __getattr__(self, key):
 #       return getattr(self.engine, key)
    def set_assert_list(self, unittest, list):
        self.unittest = unittest
        self.assert_list = list
        if list is not None:
            self.assert_list.reverse()
    def _set_echo(self, echo):
        self.engine.echo = echo
    echo = property(lambda s: s.engine.echo, _set_echo)
    def post_exec(self, proxy, compiled, parameters, **kwargs):
        self.engine.logger = self.logger
        statement = str(compiled)
        statement = re.sub(r'\n', '', statement)

        if self.assert_list is not None:
            item = self.assert_list[-1]
            if not isinstance(item, dict):
                item = self.assert_list.pop()
            else:
                # asserting a dictionary of statements->parameters
                # this is to specify query assertions where the queries can be in 
                # multiple orderings
                if not item.has_key('_converted'):
                    for key in item.keys():
                        ckey = self.convert_statement(key)
                        item[ckey] = item[key]
                        if ckey != key:
                            del item[key]
                    item['_converted'] = True
                try:
                    entry = item.pop(statement)
                    if len(item) == 1:
                        self.assert_list.pop()
                    item = (statement, entry)
                except KeyError:
                    self.unittest.assert_(False, "Testing for one of the following queries: %s, received '%s'" % (repr([k for k in item.keys()]), statement))

            (query, params) = item
            if callable(params):
                params = params()

            query = self.convert_statement(query)

            self.unittest.assert_(statement == query and (params is None or params == parameters), "Testing for query '%s' params %s, received '%s' with params %s" % (query, repr(params), statement, repr(parameters)))
        self.sql_count += 1
        return self.realexec(proxy, compiled, parameters, **kwargs)

    def convert_statement(self, query):
        paramstyle = self.engine.paramstyle
        if paramstyle == 'named':
            pass
        elif paramstyle =='pyformat':
            query = re.sub(r':([\w_]+)', r"%(\1)s", query)
        else:
            # positional params
            repl = None
            if paramstyle=='qmark':
                repl = "?"
            elif paramstyle=='format':
                repl = r"%s"
            elif paramstyle=='numeric':
                repl = None
            query = re.sub(r':([\w_]+)', repl, query)
        return query
        
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

parse_argv()

                    
def runTests(suite):
    runner = unittest.TextTestRunner(verbosity = 2, descriptions =1)
    runner.run(suite)
    
def main():
    unittest.main()


