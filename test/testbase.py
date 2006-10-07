import sys
sys.path.insert(0, './lib/')

import unittest
import StringIO
import sqlalchemy.engine as engine
import sqlalchemy.ext.proxy as proxy
import sqlalchemy.pool as pool
#import sqlalchemy.schema as schema
import re
import sqlalchemy
import optparse


db = None
metadata = None
db_uri = None
echo = True

# redefine sys.stdout so all those print statements go to the echo func
local_stdout = sys.stdout
class Logger(object):
    def write(self, msg):
        if echo:
            local_stdout.write(msg)
    def flush(self):
        pass

def echo_text(text):
    print text

def parse_argv():
    # we are using the unittest main runner, so we are just popping out the 
    # arguments we need instead of using our own getopt type of thing
    global db, db_uri, metadata
    
    DBTYPE = 'sqlite'
    PROXY = False


    parser = optparse.OptionParser(usage = "usage: %prog [options] files...")
    parser.add_option("--dburi", action="store", dest="dburi", help="database uri (overrides --db)")
    parser.add_option("--db", action="store", dest="db", default="sqlite", help="prefab database uri (sqlite, sqlite_file, postgres, mysql, oracle, oracle8, mssql, firebird)")
    parser.add_option("--mockpool", action="store_true", dest="mockpool", help="use mock pool")
    parser.add_option("--verbose", action="store_true", dest="verbose", help="full debug echoing")
    parser.add_option("--quiet", action="store_true", dest="quiet", help="be totally quiet")
    parser.add_option("--log-info", action="append", dest="log_info", help="turn on info logging for <LOG> (multiple OK)")
    parser.add_option("--log-debug", action="append", dest="log_debug", help="turn on debug logging for <LOG> (multiple OK)")
    parser.add_option("--nothreadlocal", action="store_true", dest="nothreadlocal", help="dont use thread-local mod")
    parser.add_option("--enginestrategy", action="store", default=None, dest="enginestrategy", help="engine strategy (plain or threadlocal, defaults to SA default)")

    (options, args) = parser.parse_args()
    sys.argv[1:] = args
    
    if options.dburi:
        db_uri = param = options.dburi
    elif options.db:
        DBTYPE = param = options.db


    opts = {} 
    if (None == db_uri):
        if DBTYPE == 'sqlite':
            db_uri = 'sqlite:///:memory:'
        elif DBTYPE == 'sqlite_file':
            db_uri = 'sqlite:///querytest.db'
        elif DBTYPE == 'postgres':
            db_uri = 'postgres://scott:tiger@127.0.0.1:5432/test'
        elif DBTYPE == 'mysql':
            db_uri = 'mysql://scott:tiger@127.0.0.1:3306/test'
        elif DBTYPE == 'oracle':
            db_uri = 'oracle://scott:tiger@127.0.0.1:1521'
        elif DBTYPE == 'oracle8':
            db_uri = 'oracle://scott:tiger@127.0.0.1:1521'
            opts = {'use_ansi':False}
        elif DBTYPE == 'mssql':
            db_uri = 'mssql://scott:tiger@SQUAWK\\SQLEXPRESS/test'
        elif DBTYPE == 'firebird':
            db_uri = 'firebird://sysdba:s@localhost/tmp/test.fdb'

    if not db_uri:
        raise "Could not create engine.  specify --db <sqlite|sqlite_file|postgres|mysql|oracle|oracle8|mssql|firebird> to test runner."

    if not options.nothreadlocal:
        __import__('sqlalchemy.mods.threadlocal')
        sqlalchemy.mods.threadlocal.uninstall_plugin()

    global echo
    echo = options.verbose and not options.quiet
    
    global quiet
    quiet = options.quiet
    
    if options.enginestrategy is not None:
        opts['strategy'] = options.enginestrategy    
    if options.mockpool:
        db = engine.create_engine(db_uri, default_ordering=True, poolclass=MockPool, **opts)
    else:
        db = engine.create_engine(db_uri, default_ordering=True, **opts)
    db = EngineAssert(db)

    import logging
    logging.basicConfig()
    if options.log_info is not None:
        for elem in options.log_info:
            logging.getLogger(elem).setLevel(logging.INFO)
    if options.log_debug is not None:
        for elem in options.log_debug:
            logging.getLogger(elem).setLevel(logging.DEBUG)
    metadata = sqlalchemy.BoundMetaData(db)
    
def unsupported(*dbs):
    """a decorator that marks a test as unsupported by one or more database implementations"""
    def decorate(func):
        name = db.name
        for d in dbs:
            if d == name:
                def lala(self):
                    echo_text("'" + func.__name__ + "' unsupported on DB implementation '" + name + "'")
                lala.__name__ = func.__name__
                return lala
        else:
            return func
    return decorate

def supported(*dbs):
    """a decorator that marks a test as supported by one or more database implementations"""
    def decorate(func):
        name = db.name
        for d in dbs:
            if d == name:
                return func
        else:
            def lala(self):
                echo_text("'" + func.__name__ + "' unsupported on DB implementation '" + name + "'")
            lala.__name__ = func.__name__
            return lala
    return decorate

        
class PersistTest(unittest.TestCase):
    """persist base class, provides default setUpAll, tearDownAll and echo functionality"""
    def __init__(self, *args, **params):
        unittest.TestCase.__init__(self, *args, **params)
    def echo(self, text):
        echo_text(text)
    def install_threadlocal(self):
        sqlalchemy.mods.threadlocal.install_plugin()
    def uninstall_threadlocal(self):
        sqlalchemy.mods.threadlocal.uninstall_plugin()
    def setUpAll(self):
        pass
    def tearDownAll(self):
        pass
    def shortDescription(self):
        """overridden to not return docstrings"""
        return None

class MockPool(pool.Pool):
    """this pool is hardcore about only one connection being used at a time."""
    def __init__(self, creator, **params):
        pool.Pool.__init__(self, creator, **params)
        self.connection = pool._ConnectionRecord(self)
        self._conn = self.connection
        
    def status(self):
        return "MockPool"

    def create_connection(self):
        raise "Invalid"

    def do_return_conn(self, conn):
        assert conn is self._conn and self.connection is None
        self.connection = conn

    def do_return_invalid(self, conn):
        pass
        raise "Invalid"

    def do_get(self):
        assert self.connection is not None
        c = self.connection
        self.connection = None
        return c

class AssertMixin(PersistTest):
    """given a list-based structure of keys/properties which represent information within an object structure, and
    a list of actual objects, asserts that the list of objects corresponds to the structure."""
    def assert_result(self, result, class_, *objects):
        result = list(result)
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

        self.real_execution_context = engine.dialect.create_execution_context
        engine.dialect.create_execution_context = self.execution_context
        
        self.logger = engine.logger
        self.set_assert_list(None, None)
        self.sql_count = 0
    def get_engine(self):
        return self._engine
    def set_engine(self, e):
        self._engine = e
    def set_assert_list(self, unittest, list):
        self.unittest = unittest
        self.assert_list = list
        if list is not None:
            self.assert_list.reverse()
    def _set_echo(self, echo):
        self.engine.echo = echo
    echo = property(lambda s: s.engine.echo, _set_echo)
    
    def execution_context(self):
        def post_exec(engine, proxy, compiled, parameters, **kwargs):
            ctx = e
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
                    params = params(ctx)
                if params is not None and isinstance(params, list) and len(params) == 1:
                    params = params[0]
                        
                query = self.convert_statement(query)
                self.unittest.assert_(statement == query and (params is None or params == parameters), "Testing for query '%s' params %s, received '%s' with params %s" % (query, repr(params), statement, repr(parameters)))
            self.sql_count += 1
            return realexec(ctx, proxy, compiled, parameters, **kwargs)

        e = self.real_execution_context()
        realexec = e.post_exec
        realexec.im_self.post_exec = post_exec
        return e
        
    def convert_statement(self, query):
        paramstyle = self.engine.dialect.paramstyle
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
    sys.stdout = Logger()    
    runner = unittest.TextTestRunner(verbosity = quiet and 1 or 2)
    runner.run(suite)
    
def main():
    
    if len(sys.argv[1:]):
        suite =unittest.TestLoader().loadTestsFromNames(sys.argv[1:], __import__('__main__'))
    else:
        suite = unittest.TestLoader().loadTestsFromModule(__import__('__main__'))


    runTests(suite)


