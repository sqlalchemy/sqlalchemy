import sys
import os, unittest, StringIO, re, ConfigParser
sys.path.insert(0, os.path.join(os.getcwd(), 'lib'))
import sqlalchemy
from sqlalchemy import sql, engine, pool
import sqlalchemy.engine.base as base
import optparse
from sqlalchemy.schema import MetaData
from sqlalchemy.orm import clear_mappers

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

    base_config = """
[db]
sqlite=sqlite:///:memory:
sqlite_file=sqlite:///querytest.db
postgres=postgres://scott:tiger@127.0.0.1:5432/test
mysql=mysql://scott:tiger@127.0.0.1:3306/test
oracle=oracle://scott:tiger@127.0.0.1:1521
oracle8=oracle://scott:tiger@127.0.0.1:1521/?use_ansi=0
mssql=mssql://scott:tiger@SQUAWK\\SQLEXPRESS/test
firebird=firebird://sysdba:s@localhost/tmp/test.fdb
"""
    config = ConfigParser.ConfigParser()
    config.readfp(StringIO.StringIO(base_config))
    config.read(['test.cfg', os.path.expanduser('~/.satest.cfg')])

    parser = optparse.OptionParser(usage = "usage: %prog [options] [tests...]")
    parser.add_option("--dburi", action="store", dest="dburi", help="database uri (overrides --db)")
    parser.add_option("--db", action="store", dest="db", default="sqlite", help="prefab database uri (%s)" % ', '.join(config.options('db')))
    parser.add_option("--mockpool", action="store_true", dest="mockpool", help="use mock pool (asserts only one connection used)")
    parser.add_option("--verbose", action="store_true", dest="verbose", help="enable stdout echoing/printing")
    parser.add_option("--quiet", action="store_true", dest="quiet", help="suppress unittest output")
    parser.add_option("--log-info", action="append", dest="log_info", help="turn on info logging for <LOG> (multiple OK)")
    parser.add_option("--log-debug", action="append", dest="log_debug", help="turn on debug logging for <LOG> (multiple OK)")
    parser.add_option("--nothreadlocal", action="store_true", dest="nothreadlocal", help="dont use thread-local mod")
    parser.add_option("--enginestrategy", action="store", default=None, dest="enginestrategy", help="engine strategy (plain or threadlocal, defaults to plain)")
    parser.add_option("--coverage", action="store_true", dest="coverage", help="Dump a full coverage report after running")
    parser.add_option("--reversetop", action="store_true", dest="topological", help="Reverse the collection ordering for topological sorts (helps reveal dependency issues)")
    parser.add_option("--serverside", action="store_true", dest="serverside", help="Turn on server side cursors for PG")
    parser.add_option("--require", action="append", dest="require", help="Require a particular driver or module version", default=[])
    
    (options, args) = parser.parse_args()
    sys.argv[1:] = args
    
    if options.dburi:
        db_uri = param = options.dburi
        DBTYPE = db_uri[:db_uri.index(':')]
    elif options.db:
        DBTYPE = param = options.db

    if options.require or (config.has_section('require') and
                           config.items('require')):
        try:
            import pkg_resources
        except ImportError:
            raise "setuptools is required for version requirements"

        cmdline = []
        for requirement in options.require:
            pkg_resources.require(requirement)
            cmdline.append(re.split('\s*(<!>=)', requirement, 1)[0])

        if config.has_section('require'):
            for label, requirement in config.items('require'):
                if not label == DBTYPE or label.startswith('%s.' % DBTYPE):
                    continue
                seen = [c for c in cmdline if requirement.startswith(c)]
                if seen:
                    continue
                pkg_resources.require(requirement)
        
    opts = {}
    if (None == db_uri):
        if DBTYPE not in config.options('db'):
            raise ("Could not create engine.  specify --db <%s> to " 
                   "test runner." % '|'.join(config.options('db')))

        db_uri = config.get('db', DBTYPE)

    if not db_uri:
        raise "Could not create engine.  specify --db <sqlite|sqlite_file|postgres|mysql|oracle|oracle8|mssql|firebird> to test runner."

    if not options.nothreadlocal:
        __import__('sqlalchemy.mods.threadlocal')
        sqlalchemy.mods.threadlocal.uninstall_plugin()

    global echo
    echo = options.verbose and not options.quiet
    
    global quiet
    quiet = options.quiet
    
    global with_coverage
    with_coverage = options.coverage

    if options.serverside:
        opts['server_side_cursors'] = True
    
    if options.enginestrategy is not None:
        opts['strategy'] = options.enginestrategy    
    if options.mockpool:
        db = engine.create_engine(db_uri, poolclass=pool.AssertionPool, **opts)
    else:
        db = engine.create_engine(db_uri, **opts)

    # decorate the dialect's create_execution_context() method
    # to produce a wrapper
    create_context = db.dialect.create_execution_context
    def create_exec_context(*args, **kwargs):
        return ExecutionContextWrapper(create_context(*args, **kwargs))
    db.dialect.create_execution_context = create_exec_context
    
    global testdata
    testdata = TestData(db)
    
    if options.topological:
        from sqlalchemy.orm import unitofwork
        from sqlalchemy import topological
        class RevQueueDepSort(topological.QueueDependencySorter):
            def __init__(self, tuples, allitems):
                self.tuples = list(tuples)
                self.allitems = list(allitems)
                self.tuples.reverse()
                self.allitems.reverse()
        topological.QueueDependencySorter = RevQueueDepSort
        unitofwork.DependencySorter = RevQueueDepSort
            
    import logging
    logging.basicConfig()
    if options.log_info is not None:
        for elem in options.log_info:
            logging.getLogger(elem).setLevel(logging.INFO)
    if options.log_debug is not None:
        for elem in options.log_debug:
            logging.getLogger(elem).setLevel(logging.DEBUG)
    metadata = sqlalchemy.MetaData(db)
    
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
        global testdata
        testdata = TestData(db)
        if with_sequences is not None and (db.engine.name == 'postgres' or db.engine.name == 'oracle'):
            testdata.set_assert_list(self, with_sequences)
        else:
            testdata.set_assert_list(self, list)
        try:
            callable_()
        finally:
            testdata.set_assert_list(None, None)

    def assert_sql_count(self, db, callable_, count):
        global testdata
        testdata = TestData(db)
        try:
            callable_()
        finally:
            self.assert_(testdata.sql_count == count, "desired statement count %d does not match %d" % (count, testdata.sql_count))

    def capture_sql(self, db, callable_):
        global testdata
        testdata = TestData(db)
        buffer = StringIO.StringIO()
        testdata.buffer = buffer
        try:
            callable_()
            return buffer.getvalue()
        finally:
            testdata.buffer = None
            
class ORMTest(AssertMixin):
    keep_mappers = False
    keep_data = False
    def setUpAll(self):
        global metadata
        metadata = MetaData(db)
        self.define_tables(metadata)
        metadata.create_all()
        self.insert_data()
    def define_tables(self, metadata):
        raise NotImplementedError()
    def insert_data(self):
        pass
    def get_metadata(self):
        return metadata
    def tearDownAll(self):
        metadata.drop_all()
    def tearDown(self):
        if not self.keep_mappers:
            clear_mappers()
        if not self.keep_data:
            for t in metadata.table_iterator(reverse=True):
                t.delete().execute().close()

class TestData(object):
    def __init__(self, engine):
        self._engine = engine
        self.logger = engine.logger
        self.set_assert_list(None, None)
        self.sql_count = 0
        self.buffer = None
        
    def set_assert_list(self, unittest, list):
        self.unittest = unittest
        self.assert_list = list
        if list is not None:
            self.assert_list.reverse()
    
class ExecutionContextWrapper(object):
    def __init__(self, ctx):
        self.__dict__['ctx'] = ctx
    def __getattr__(self, key):
        return getattr(self.ctx, key)
    def __setattr__(self, key, value):
        setattr(self.ctx, key, value)
        
    def post_exec(self):
        ctx = self.ctx
        statement = unicode(ctx.compiled)
        statement = re.sub(r'\n', '', ctx.statement)
        if db.engine.name == 'mssql' and statement.endswith('; select scope_identity()'):
            statement = statement[:-25]
        if testdata.buffer is not None:
            testdata.buffer.write(statement + "\n")

        if testdata.assert_list is not None:
            item = testdata.assert_list[-1]
            if not isinstance(item, dict):
                item = testdata.assert_list.pop()
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
                        testdata.assert_list.pop()
                    item = (statement, entry)
                except KeyError:
                    print "Testing for one of the following queries: %s, received '%s'" % (repr([k for k in item.keys()]), statement)
                    self.unittest.assert_(False, "Testing for one of the following queries: %s, received '%s'" % (repr([k for k in item.keys()]), statement))

            (query, params) = item
            if callable(params):
                params = params(ctx)
            if params is not None and isinstance(params, list) and len(params) == 1:
                params = params[0]
            
            if isinstance(ctx.compiled_parameters, sql.ClauseParameters):
                parameters = ctx.compiled_parameters.get_original_dict()
            elif isinstance(ctx.compiled_parameters, list):
                parameters = [p.get_original_dict() for p in ctx.compiled_parameters]
                    
            query = self.convert_statement(query)
            testdata.unittest.assert_(statement == query and (params is None or params == parameters), "Testing for query '%s' params %s, received '%s' with params %s" % (query, repr(params), statement, repr(parameters)))
        testdata.sql_count += 1
        self.ctx.post_exec()
        
    def convert_statement(self, query):
        paramstyle = self.ctx.dialect.paramstyle
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
    if with_coverage:
        return cover(lambda:runner.run(suite))
    else:
        return runner.run(suite)

def covered_files():
    for rec in os.walk(os.path.dirname(sqlalchemy.__file__)):                          
        for x in rec[2]:
            if x.endswith('.py'):
                yield os.path.join(rec[0], x)

def cover(callable_):
    import coverage
    coverage_client = coverage.the_coverage
    coverage_client.get_ready()
    coverage_client.exclude('#pragma[: ]+[nN][oO] [cC][oO][vV][eE][rR]')
    coverage_client.erase()
    coverage_client.start()
    try:
        return callable_()
    finally:
        global echo
        echo=True
        coverage_client.stop()
        coverage_client.save()
        coverage_client.report(list(covered_files()), show_missing=False, ignore_errors=False)

def main(suite=None):
    
    if not suite:
        if len(sys.argv[1:]):
            suite =unittest.TestLoader().loadTestsFromNames(sys.argv[1:], __import__('__main__'))
        else:
            suite = unittest.TestLoader().loadTestsFromModule(__import__('__main__'))

    result = runTests(suite)
    sys.exit(not result.wasSuccessful())



