"""TestCase and TestSuite artifacts and testing decorators."""

# monkeypatches unittest.TestLoader.suiteClass at import time

import testbase
import unittest, re, sys, os, operator
from cStringIO import StringIO
import testlib.config as config
sql, MetaData, clear_mappers, Session = None, None, None, None


__all__ = ('PersistTest', 'AssertMixin', 'ORMTest', 'SQLCompileTest')

_ops = { '<': operator.lt,
         '>': operator.gt,
         '==': operator.eq,
         '!=': operator.ne,
         '<=': operator.le,
         '>=': operator.ge,
         'in': operator.contains,
         'between': lambda val, pair: val >= pair[0] and val <= pair[1],
         }

def unsupported(*dbs):
    """Mark a test as unsupported by one or more database implementations"""
    
    def decorate(fn):
        fn_name = fn.__name__
        def maybe(*args, **kw):
            if config.db.name in dbs:
                print "'%s' unsupported on DB implementation '%s'" % (
                    fn_name, config.db.name)
                return True
            else:
                return fn(*args, **kw)
        try:
            maybe.__name__ = fn_name
        except:
            pass
        return maybe
    return decorate

def supported(*dbs):
    """Mark a test as supported by one or more database implementations"""
    
    def decorate(fn):
        fn_name = fn.__name__
        def maybe(*args, **kw):
            if config.db.name in dbs:
                return fn(*args, **kw)
            else:
                print "'%s' unsupported on DB implementation '%s'" % (
                    fn_name, config.db.name)
                return True
        try:
            maybe.__name__ = fn_name
        except:
            pass
        return maybe
    return decorate

def exclude(db, op, spec):
    """Mark a test as unsupported by specific database server versions.

    Stackable, both with other excludes and supported/unsupported. Examples::
      # Not supported by mydb versions less than 1, 0
      @exclude('mydb', '<', (1,0))
      # Other operators work too
      @exclude('bigdb', '==', (9,0,9))
      @exclude('yikesdb', 'in', ((0, 3, 'alpha2'), (0, 3, 'alpha3')))
    """

    def decorate(fn):
        fn_name = fn.__name__
        def maybe(*args, **kw):
            if config.db.name != db:
                return fn(*args, **kw)

            have = config.db.dialect.server_version_info(
                config.db.contextual_connect())

            oper = hasattr(op, '__call__') and op or _ops[op]

            if oper(have, spec):
                print "'%s' unsupported on DB %s version '%s'" % (
                    fn_name, config.db.name, have)
                return True
            else:
                return fn(*args, **kw)
        try:
            maybe.__name__ = fn_name
        except:
            pass
        return maybe
    return decorate

def against(*queries):
    """Boolean predicate, compares to testing database configuration.

    Given one or more dialect names, returns True if one is the configured
    database engine.

    Also supports comparison to database version when provided with one or
    more 3-tuples of dialect name, operator, and version specification::

      testing.against('mysql', 'postgres')
      testing.against(('mysql', '>=', (5, 0, 0))
    """

    for query in queries:
        if isinstance(query, basestring):
            if config.db.name == query:
                return True
        else:
            name, op, spec = query
            if config.db.name != name:
                continue

            have = config.db.dialect.server_version_info(
                config.db.contextual_connect())

            oper = hasattr(op, '__call__') and op or _ops[op]
            if oper(have, spec):
                return True
    return False

class TestData(object):
    """Tracks SQL expressions as they are executed via an instrumented ExecutionContext."""
    
    def __init__(self):
        self.set_assert_list(None, None)
        self.sql_count = 0
        self.buffer = None
        
    def set_assert_list(self, unittest, list):
        self.unittest = unittest
        self.assert_list = list
        if list is not None:
            self.assert_list.reverse()

testdata = TestData()


class ExecutionContextWrapper(object):
    """instruments the ExecutionContext created by the Engine so that SQL expressions
    can be tracked."""
    
    def __init__(self, ctx):
        global sql
        if sql is None:
            from sqlalchemy import sql

        self.__dict__['ctx'] = ctx
    def __getattr__(self, key):
        return getattr(self.ctx, key)
    def __setattr__(self, key, value):
        setattr(self.ctx, key, value)
        
    def post_execution(self):
        ctx = self.ctx
        statement = unicode(ctx.compiled)
        statement = re.sub(r'\n', '', ctx.statement)
        if config.db.name == 'mssql' and statement.endswith('; select scope_identity()'):
            statement = statement[:-25]
        if testdata.buffer is not None:
            testdata.buffer.write(statement + "\n")

        if testdata.assert_list is not None:
            assert len(testdata.assert_list), "Received query but no more assertions: %s" % statement
            item = testdata.assert_list[-1]
            if not isinstance(item, dict):
                item = testdata.assert_list.pop()
            else:
                # asserting a dictionary of statements->parameters
                # this is to specify query assertions where the queries can be in 
                # multiple orderings
                if '_converted' not in item:
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
                    assert False, "Testing for one of the following queries: %s, received '%s'" % (repr([k for k in item.keys()]), statement)

            (query, params) = item
            if callable(params):
                params = params(ctx)
            if params is not None and not isinstance(params, list):
                params = [params]
            
            from sqlalchemy.sql.util import ClauseParameters
            parameters = [p.get_original_dict() for p in ctx.compiled_parameters]
                    
            query = self.convert_statement(query)
            testdata.unittest.assert_(statement == query and (params is None or params == parameters), "Testing for query '%s' params %s, received '%s' with params %s" % (query, repr(params), statement, repr(parameters)))
        testdata.sql_count += 1
        self.ctx.post_execution()
        
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

class PersistTest(unittest.TestCase):

    def __init__(self, *args, **params):
        unittest.TestCase.__init__(self, *args, **params)

    def setUpAll(self):
        pass

    def tearDownAll(self):
        pass

    def shortDescription(self):
        """overridden to not return docstrings"""
        return None

class SQLCompileTest(PersistTest):
    def assert_compile(self, clause, result, params=None, checkparams=None, dialect=None):
        if dialect is None:
            dialect = getattr(self, '__dialect__', None)
        
        if params is None:
            keys = None
        else:
            keys = params.keys()
                
        c = clause.compile(column_keys=keys, dialect=dialect)

        print "\nSQL String:\n" + str(c) + repr(c.params)

        cc = re.sub(r'\n', '', str(c))

        self.assert_(cc == result, "\n'" + cc + "'\n does not match \n'" + result + "'")

        if checkparams is not None:
            if isinstance(checkparams, list):
                self.assert_(c.params.get_raw_list({}) == checkparams, "params dont match ")
            else:
                self.assert_(c.params.get_original_dict() == checkparams, "params dont match" + repr(c.params))

class AssertMixin(PersistTest):
    """given a list-based structure of keys/properties which represent information within an object structure, and
    a list of actual objects, asserts that the list of objects corresponds to the structure."""
    
    def assert_result(self, result, class_, *objects):
        result = list(result)
        print repr(result)
        self.assert_list(result, class_, objects)
        
    def assert_list(self, result, class_, list):
        self.assert_(len(result) == len(list),
                     "result list is not the same size as test list, " +
                     "for class " + class_.__name__)
        for i in range(0, len(list)):
            self.assert_row(class_, result[i], list[i])
            
    def assert_row(self, class_, rowobj, desc):
        self.assert_(rowobj.__class__ is class_,
                     "item class is not " + repr(class_))
        for key, value in desc.iteritems():
            if isinstance(value, tuple):
                if isinstance(value[1], list):
                    self.assert_list(getattr(rowobj, key), value[0], value[1])
                else:
                    self.assert_row(value[0], getattr(rowobj, key), value[1])
            else:
                self.assert_(getattr(rowobj, key) == value,
                             "attribute %s value %s does not match %s" % (
                             key, getattr(rowobj, key), value))
                
    def assert_sql(self, db, callable_, list, with_sequences=None):
        global testdata
        testdata = TestData()
        if with_sequences is not None and (config.db.name == 'postgres' or
                                           config.db.name == 'oracle'):
            testdata.set_assert_list(self, with_sequences)
        else:
            testdata.set_assert_list(self, list)
        try:
            callable_()
        finally:
            testdata.set_assert_list(None, None)

    def assert_sql_count(self, db, callable_, count):
        global testdata
        testdata = TestData()
        try:
            callable_()
        finally:
            self.assert_(testdata.sql_count == count,
                         "desired statement count %d does not match %d" % (
                         count, testdata.sql_count))

    def capture_sql(self, db, callable_):
        global testdata
        testdata = TestData()
        buffer = StringIO()
        testdata.buffer = buffer
        try:
            callable_()
            return buffer.getvalue()
        finally:
            testdata.buffer = None

_otest_metadata = None
class ORMTest(AssertMixin):
    keep_mappers = False
    keep_data = False
    metadata = None
    
    def setUpAll(self):
        global MetaData, _otest_metadata

        if MetaData is None:
            from sqlalchemy import MetaData
        
        if self.metadata is None:
            _otest_metadata = MetaData(config.db)
        else:
            _otest_metadata = self.metadata
            _otest_metadata.bind = config.db
        self.define_tables(_otest_metadata)
        _otest_metadata.create_all()
        self.insert_data()

    def define_tables(self, _otest_metadata):
        raise NotImplementedError()

    def insert_data(self):
        pass

    def get_metadata(self):
        return _otest_metadata

    def tearDownAll(self):
        global clear_mappers
        if clear_mappers is None:
            from sqlalchemy.orm import clear_mappers

        clear_mappers()
        _otest_metadata.drop_all()

    def tearDown(self):
        global Session
        if Session is None:
            from sqlalchemy.orm.session import Session
        Session.close_all()
        global clear_mappers
        if clear_mappers is None:
            from sqlalchemy.orm import clear_mappers

        if not self.keep_mappers:
            clear_mappers()
        if not self.keep_data:
            for t in _otest_metadata.table_iterator(reverse=True):
                try:
                    t.delete().execute().close()
                except Exception, e:
                    print "EXCEPTION DELETING...", e


class TTestSuite(unittest.TestSuite):
    """A TestSuite with once per TestCase setUpAll() and tearDownAll()"""

    def __init__(self, tests=()):
        if len(tests) >0 and isinstance(tests[0], PersistTest):
            self._initTest = tests[0]
        else:
            self._initTest = None
        unittest.TestSuite.__init__(self, tests)

    def do_run(self, result):
        # nice job unittest !  you switched __call__ and run() between py2.3
        # and 2.4 thereby making straight subclassing impossible !
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
            # skip tests if global setup fails
            ex = self.__exc_info()
            for test in self._tests:
                result.addError(test, ex)
            return False
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

# monkeypatch
unittest.TestLoader.suiteClass = TTestSuite


class DevNullWriter(object):
    def write(self, msg):
        pass
    def flush(self):
        pass

def runTests(suite):
    verbose = config.options.verbose
    quiet = config.options.quiet
    orig_stdout = sys.stdout

    try:
        if not verbose or quiet:
            sys.stdout = DevNullWriter()
        runner = unittest.TextTestRunner(verbosity = quiet and 1 or 2)
        return runner.run(suite)
    finally:
        if not verbose or quiet:
            sys.stdout = orig_stdout

def main(suite=None):
    if not suite:
        if sys.argv[1:]:
            suite =unittest.TestLoader().loadTestsFromNames(
                sys.argv[1:], __import__('__main__'))
        else:
            suite = unittest.TestLoader().loadTestsFromModule(
                __import__('__main__'))

    result = runTests(suite)
    sys.exit(not result.wasSuccessful())
