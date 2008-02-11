"""TestCase and TestSuite artifacts and testing decorators."""

# monkeypatches unittest.TestLoader.suiteClass at import time

import itertools, os, operator, re, sys, unittest, warnings
from cStringIO import StringIO
import testlib.config as config
from testlib.compat import *

sql, sqltypes, schema, MetaData, clear_mappers, Session, util = None, None, None, None, None, None, None
sa_exceptions = None

__all__ = ('TestBase', 'AssertsExecutionResults', 'ComparesTables', 'ORMTest', 'AssertsCompiledSQL')

_ops = { '<': operator.lt,
         '>': operator.gt,
         '==': operator.eq,
         '!=': operator.ne,
         '<=': operator.le,
         '>=': operator.ge,
         'in': operator.contains,
         'between': lambda val, pair: val >= pair[0] and val <= pair[1],
         }

# sugar ('testing.db'); set here by config() at runtime
db = None

def fails_if(callable_):
    """Mark a test as expected to fail if callable_ returns True.

    If the callable returns false, the test is run and reported as normal.
    However if the callable returns true, the test is expected to fail and the
    unit test logic is inverted: if the test fails, a success is reported.  If
    the test succeeds, a failure is reported.
    """

    docstring = getattr(callable_, '__doc__', None) or callable_.__name__
    description = docstring.split('\n')[0]

    def decorate(fn):
        fn_name = fn.__name__
        def maybe(*args, **kw):
            if not callable_():
                return fn(*args, **kw)
            else:
                try:
                    fn(*args, **kw)
                except Exception, ex:
                    print ("'%s' failed as expected (condition: %s): %s " % (
                        fn_name, description, str(ex)))
                    return True
                else:
                    raise AssertionError(
                        "Unexpected success for '%s' (condition: %s)" %
                        (fn_name, description))
        return _function_named(maybe, fn_name)
    return decorate


def future(fn):
    """Mark a test as expected to unconditionally fail.

    Takes no arguments, omit parens when using as a decorator.
    """

    fn_name = fn.__name__
    def decorated(*args, **kw):
        try:
            fn(*args, **kw)
        except Exception, ex:
            print ("Future test '%s' failed as expected: %s " % (
                fn_name, str(ex)))
            return True
        else:
            raise AssertionError(
                "Unexpected success for future test '%s'" % fn_name)
    return _function_named(decorated, fn_name)

def fails_on(*dbs):
    """Mark a test as expected to fail on one or more database implementations.

    Unlike ``unsupported``, tests marked as ``fails_on`` will be run
    for the named databases.  The test is expected to fail and the unit test
    logic is inverted: if the test fails, a success is reported.  If the test
    succeeds, a failure is reported.
    """

    def decorate(fn):
        fn_name = fn.__name__
        def maybe(*args, **kw):
            if config.db.name not in dbs:
                return fn(*args, **kw)
            else:
                try:
                    fn(*args, **kw)
                except Exception, ex:
                    print ("'%s' failed as expected on DB implementation "
                           "'%s': %s" % (
                        fn_name, config.db.name, str(ex)))
                    return True
                else:
                    raise AssertionError(
                        "Unexpected success for '%s' on DB implementation '%s'" %
                        (fn_name, config.db.name))
        return _function_named(maybe, fn_name)
    return decorate

def fails_on_everything_except(*dbs):
    """Mark a test as expected to fail on most database implementations.

    Like ``fails_on``, except failure is the expected outcome on all
    databases except those listed.
    """

    def decorate(fn):
        fn_name = fn.__name__
        def maybe(*args, **kw):
            if config.db.name in dbs:
                return fn(*args, **kw)
            else:
                try:
                    fn(*args, **kw)
                except Exception, ex:
                    print ("'%s' failed as expected on DB implementation "
                           "'%s': %s" % (
                        fn_name, config.db.name, str(ex)))
                    return True
                else:
                    raise AssertionError(
                        "Unexpected success for '%s' on DB implementation '%s'" %
                        (fn_name, config.db.name))
        return _function_named(maybe, fn_name)
    return decorate

def unsupported(*dbs):
    """Mark a test as unsupported by one or more database implementations.

    'unsupported' tests will be skipped unconditionally.  Useful for feature
    tests that cause deadlocks or other fatal problems.
    """

    def decorate(fn):
        fn_name = fn.__name__
        def maybe(*args, **kw):
            if config.db.name in dbs:
                print "'%s' unsupported on DB implementation '%s'" % (
                    fn_name, config.db.name)
                return True
            else:
                return fn(*args, **kw)
        return _function_named(maybe, fn_name)
    return decorate

def exclude(db, op, spec):
    """Mark a test as unsupported by specific database server versions.

    Stackable, both with other excludes and other decorators. Examples::

      # Not supported by mydb versions less than 1, 0
      @exclude('mydb', '<', (1,0))
      # Other operators work too
      @exclude('bigdb', '==', (9,0,9))
      @exclude('yikesdb', 'in', ((0, 3, 'alpha2'), (0, 3, 'alpha3')))
    """

    def decorate(fn):
        fn_name = fn.__name__
        def maybe(*args, **kw):
            if _is_excluded(db, op, spec):
                print "'%s' unsupported on DB %s version '%s'" % (
                    fn_name, config.db.name, _server_version())
                return True
            else:
                return fn(*args, **kw)
        return _function_named(maybe, fn_name)
    return decorate

def _is_excluded(db, op, spec):
    """Return True if the configured db matches an exclusion specification.

    db:
      A dialect name
    op:
      An operator or stringified operator, such as '=='
    spec:
      A value that will be compared to the dialect's server_version_info
      using the supplied operator.

    Examples::
      # Not supported by mydb versions less than 1, 0
      _is_excluded('mydb', '<', (1,0))
      # Other operators work too
      _is_excluded('bigdb', '==', (9,0,9))
      _is_excluded('yikesdb', 'in', ((0, 3, 'alpha2'), (0, 3, 'alpha3')))
    """

    if config.db.name != db:
        return False

    version = _server_version()

    oper = hasattr(op, '__call__') and op or _ops[op]
    return oper(version, spec)

def _server_version(bind=None):
    """Return a server_version_info tuple."""

    if bind is None:
        bind = config.db
    return bind.dialect.server_version_info(bind.contextual_connect())

def emits_warning(*messages):
    """Mark a test as emitting a warning.

    With no arguments, squelches all SAWarning failures.  Or pass one or more
    strings; these will be matched to the root of the warning description by
    warnings.filterwarnings().
    """

    # TODO: it would be nice to assert that a named warning was
    # emitted. should work with some monkeypatching of warnings,
    # and may work on non-CPython if they keep to the spirit of
    # warnings.showwarning's docstring.
    # - update: jython looks ok, it uses cpython's module
    def decorate(fn):
        def safe(*args, **kw):
            global sa_exceptions
            if sa_exceptions is None:
                import sqlalchemy.exceptions as sa_exceptions

            if not messages:
                filters = [dict(action='ignore',
                                category=sa_exceptions.SAWarning)]
            else:
                filters = [dict(action='ignore',
                                message=message,
                                category=sa_exceptions.SAWarning)
                           for message in messages ]
            for f in filters:
                warnings.filterwarnings(**f)
            try:
                return fn(*args, **kw)
            finally:
                resetwarnings()
        return _function_named(safe, fn.__name__)
    return decorate

def uses_deprecated(*messages):
    """Mark a test as immune from fatal deprecation warnings.

    With no arguments, squelches all SADeprecationWarning failures.
    Or pass one or more strings; these will be matched to the root
    of the warning description by warnings.filterwarnings().

    As a special case, you may pass a function name prefixed with //
    and it will be re-written as needed to match the standard warning
    verbiage emitted by the sqlalchemy.util.deprecated decorator.
    """

    def decorate(fn):
        def safe(*args, **kw):
            global sa_exceptions
            if sa_exceptions is None:
                import sqlalchemy.exceptions as sa_exceptions

            if not messages:
                filters = [dict(action='ignore',
                                category=sa_exceptions.SADeprecationWarning)]
            else:
                filters = [dict(action='ignore',
                                message=message,
                                category=sa_exceptions.SADeprecationWarning)
                           for message in
                           [ (m.startswith('//') and
                              ('Call to deprecated function ' + m[2:]) or m)
                             for m in messages] ]

            for f in filters:
                warnings.filterwarnings(**f)
            try:
                return fn(*args, **kw)
            finally:
                resetwarnings()
        return _function_named(safe, fn.__name__)
    return decorate

def resetwarnings():
    """Reset warning behavior to testing defaults."""

    global sa_exceptions
    if sa_exceptions is None:
        import sqlalchemy.exceptions as sa_exceptions

    warnings.resetwarnings()
    warnings.filterwarnings('error', category=sa_exceptions.SADeprecationWarning)
    warnings.filterwarnings('error', category=sa_exceptions.SAWarning)

    if sys.version_info < (2, 4):
        warnings.filterwarnings('ignore', category=FutureWarning)


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

def rowset(results):
    """Converts the results of sql execution into a plain set of column tuples.

    Useful for asserting the results of an unordered query.
    """

    return set([tuple(row) for row in results])


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

            parameters = ctx.compiled_parameters

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

class TestBase(unittest.TestCase):
    # A sequence of dialect names to exclude from the test class.
    __unsupported_on__ = ()

    # If present, test class is only runnable for the *single* specified
    # dialect.  If you need multiple, use __unsupported_on__ and invert.
    __only_on__ = None

    # A sequence of no-arg callables. If any are True, the entire testcase is
    # skipped.
    __skip_if__ = None

    def __init__(self, *args, **params):
        unittest.TestCase.__init__(self, *args, **params)

    def setUpAll(self):
        pass

    def tearDownAll(self):
        pass

    def shortDescription(self):
        """overridden to not return docstrings"""
        return None

    if not hasattr(unittest.TestCase, 'assertTrue'):
        assertTrue = unittest.TestCase.failUnless
    if not hasattr(unittest.TestCase, 'assertFalse'):
        assertFalse = unittest.TestCase.failIf

class AssertsCompiledSQL(object):
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

        self.assertEquals(cc, result)

        if checkparams is not None:
            self.assertEquals(c.construct_params(params), checkparams)

class ComparesTables(object):
    def assert_tables_equal(self, table, reflected_table):
        global sqltypes, schema
        if sqltypes is None:
            import sqlalchemy.types as sqltypes
        if schema is None:
            import sqlalchemy.schema as schema
        base_mro = sqltypes.TypeEngine.__mro__
        assert len(table.c) == len(reflected_table.c)
        for c, reflected_c in zip(table.c, reflected_table.c):
            self.assertEquals(c.name, reflected_c.name)
            assert reflected_c is reflected_table.c[c.name]
            self.assertEquals(c.primary_key, reflected_c.primary_key)
            self.assertEquals(c.nullable, reflected_c.nullable)
            assert len(
                set(type(reflected_c.type).__mro__).difference(base_mro).intersection(
                set(type(c.type).__mro__).difference(base_mro)
                )
            ) > 0, "Type '%s' doesn't correspond to type '%s'" % (reflected_c.type, c.type)
            
            if isinstance(c.type, sqltypes.String):
                self.assertEquals(c.type.length, reflected_c.type.length)

            self.assertEquals(set([f.column.name for f in c.foreign_keys]), set([f.column.name for f in reflected_c.foreign_keys]))
            if c.default:
                assert isinstance(reflected_c.default, schema.PassiveDefault)
            elif not c.primary_key or not against('postgres'):
                assert reflected_c.default is None
        
        assert len(table.primary_key) == len(reflected_table.primary_key)
        for c in table.primary_key:
            assert reflected_table.primary_key.columns[c.name]

    
class AssertsExecutionResults(object):
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

    def assert_unordered_result(self, result, cls, *expected):
        """As assert_result, but the order of objects is not considered.

        The algorithm is very expensive but not a big deal for the small
        numbers of rows that the test suite manipulates.
        """

        global util
        if util is None:
            from sqlalchemy import util

        class frozendict(dict):
            def __hash__(self):
                return id(self)

        found = util.IdentitySet(result)
        expected = set([frozendict(e) for e in expected])

        for wrong in itertools.ifilterfalse(lambda o: type(o) == cls, found):
            self.fail('Unexpected type "%s", expected "%s"' % (
                type(wrong).__name__, cls.__name__))

        if len(found) != len(expected):
            self.fail('Unexpected object count "%s", expected "%s"' % (
                len(found), len(expected)))

        NOVALUE = object()
        def _compare_item(obj, spec):
            for key, value in spec.iteritems():
                if isinstance(value, tuple):
                    try:
                        self.assert_unordered_result(
                            getattr(obj, key), value[0], *value[1])
                    except AssertionError:
                        return False
                else:
                    if getattr(obj, key, NOVALUE) != value:
                        return False
            return True

        for expected_item in expected:
            for found_item in found:
                if _compare_item(found_item, expected_item):
                    found.remove(found_item)
                    break
            else:
                self.fail(
                    "Expected %s instance with attributes %s not found." % (
                    cls.__name__, repr(expected_item)))
        return True

    def assert_sql(self, db, callable_, list, with_sequences=None):
        global testdata
        testdata = TestData()
        if with_sequences is not None and config.db.name in ('firebird', 'oracle', 'postgres'):
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
        callable_()
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
class ORMTest(TestBase, AssertsExecutionResults):
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
            if self.metadata.bind is None:
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
        if len(tests) > 0 and isinstance(tests[0], TestBase):
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
        init = getattr(self, '_initTest', None)
        if init is not None:
            if (hasattr(init, '__unsupported_on__') and
                config.db.name in init.__unsupported_on__):
                print "'%s' unsupported on DB implementation '%s'" % (
                    init.__class__.__name__, config.db.name)
                return True
            if (getattr(init, '__only_on__', None) not in (None,config.db.name)):
                print "'%s' unsupported on DB implementation '%s'" % (
                    init.__class__.__name__, config.db.name)
                return True
            if (getattr(init, '__skip_if__', False)):
                for c in getattr(init, '__skip_if__'):
                    if c():
                        print "'%s' skipped by %s" % (
                            init.__class__.__name__, c.__name__)
                        return True
            for rule in getattr(init, '__excluded_on__', ()):
                if _is_excluded(*rule):
                    print "'%s' unsupported on DB %s version %s" % (
                        init.__class__.__name__, config.db.name,
                        _server_version())
                    return True
            try:
                resetwarnings()
                init.setUpAll()
            except:
                # skip tests if global setup fails
                ex = self.__exc_info()
                for test in self._tests:
                    result.addError(test, ex)
                return False
        try:
            resetwarnings()
            return self.do_run(result)
        finally:
            try:
                resetwarnings()
                if init is not None:
                    init.tearDownAll()
            except:
                result.addError(init, self.__exc_info())
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
