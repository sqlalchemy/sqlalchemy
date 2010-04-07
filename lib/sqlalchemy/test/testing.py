"""TestCase and TestSuite artifacts and testing decorators."""

import itertools
import operator
import re
import sys
import types
import warnings
from cStringIO import StringIO

from sqlalchemy.test import config, assertsql, util as testutil
from sqlalchemy.util import function_named, py3k
from engines import drop_all_tables

from sqlalchemy import exc as sa_exc, util, types as sqltypes, schema, pool, orm
from sqlalchemy.engine import default
from nose import SkipTest

    
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

# more sugar, installed by __init__
requires = None

def fails_if(callable_, reason=None):
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
        return function_named(maybe, fn_name)
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
    return function_named(decorated, fn_name)

def db_spec(*dbs):
    dialects = set([x for x in dbs if '+' not in x])
    drivers = set([x[1:] for x in dbs if x.startswith('+')])
    specs = set([tuple(x.split('+')) for x in dbs if '+' in x and x not in drivers])

    def check(engine):
        return engine.name in dialects or \
            engine.driver in drivers or \
            (engine.name, engine.driver) in specs
    
    return check
        

def fails_on(dbs, reason):
    """Mark a test as expected to fail on the specified database 
    implementation.

    Unlike ``crashes``, tests marked as ``fails_on`` will be run
    for the named databases.  The test is expected to fail and the unit test
    logic is inverted: if the test fails, a success is reported.  If the test
    succeeds, a failure is reported.
    """

    spec = db_spec(dbs)
     
    def decorate(fn):
        fn_name = fn.__name__
        def maybe(*args, **kw):
            if not spec(config.db):
                return fn(*args, **kw)
            else:
                try:
                    fn(*args, **kw)
                except Exception, ex:
                    print ("'%s' failed as expected on DB implementation "
                            "'%s+%s': %s" % (
                        fn_name, config.db.name, config.db.driver, reason))
                    return True
                else:
                    raise AssertionError(
                         "Unexpected success for '%s' on DB implementation '%s+%s'" %
                         (fn_name, config.db.name, config.db.driver))
        return function_named(maybe, fn_name)
    return decorate

def fails_on_everything_except(*dbs):
    """Mark a test as expected to fail on most database implementations.

    Like ``fails_on``, except failure is the expected outcome on all
    databases except those listed.
    """

    spec = db_spec(*dbs)
    
    def decorate(fn):
        fn_name = fn.__name__
        def maybe(*args, **kw):
            if spec(config.db):
                return fn(*args, **kw)
            else:
                try:
                    fn(*args, **kw)
                except Exception, ex:
                    print ("'%s' failed as expected on DB implementation "
                            "'%s+%s': %s" % (
                        fn_name, config.db.name, config.db.driver, str(ex)))
                    return True
                else:
                    raise AssertionError(
                      "Unexpected success for '%s' on DB implementation '%s+%s'" %
                      (fn_name, config.db.name, config.db.driver))
        return function_named(maybe, fn_name)
    return decorate

def crashes(db, reason):
    """Mark a test as unsupported by a database implementation.

    ``crashes`` tests will be skipped unconditionally.  Use for feature tests
    that cause deadlocks or other fatal problems.

    """
    carp = _should_carp_about_exclusion(reason)
    spec = db_spec(db)
    def decorate(fn):
        fn_name = fn.__name__
        def maybe(*args, **kw):
            if spec(config.db):
                msg = "'%s' unsupported on DB implementation '%s+%s': %s" % (
                    fn_name, config.db.name, config.db.driver, reason)
                print msg
                if carp:
                    print >> sys.stderr, msg
                return True
            else:
                return fn(*args, **kw)
        return function_named(maybe, fn_name)
    return decorate

def _block_unconditionally(db, reason):
    """Mark a test as unsupported by a database implementation.

    Will never run the test against any version of the given database, ever,
    no matter what.  Use when your assumptions are infallible; past, present
    and future.

    """
    carp = _should_carp_about_exclusion(reason)
    spec = db_spec(db)
    def decorate(fn):
        fn_name = fn.__name__
        def maybe(*args, **kw):
            if spec(config.db):
                msg = "'%s' unsupported on DB implementation '%s+%s': %s" % (
                    fn_name, config.db.name, config.db.driver, reason)
                print msg
                if carp:
                    print >> sys.stderr, msg
                return True
            else:
                return fn(*args, **kw)
        return function_named(maybe, fn_name)
    return decorate

def only_on(db, reason):
    carp = _should_carp_about_exclusion(reason)
    spec = db_spec(db)
    def decorate(fn):
        fn_name = fn.__name__
        def maybe(*args, **kw):
            if spec(config.db):
                return fn(*args, **kw)
            else:
                msg = "'%s' unsupported on DB implementation '%s+%s': %s" % (
                    fn_name, config.db.name, config.db.driver, reason)
                print msg
                if carp:
                    print >> sys.stderr, msg
                return True
        return function_named(maybe, fn_name)
    return decorate
    
def exclude(db, op, spec, reason):
    """Mark a test as unsupported by specific database server versions.

    Stackable, both with other excludes and other decorators. Examples::

      # Not supported by mydb versions less than 1, 0
      @exclude('mydb', '<', (1,0))
      # Other operators work too
      @exclude('bigdb', '==', (9,0,9))
      @exclude('yikesdb', 'in', ((0, 3, 'alpha2'), (0, 3, 'alpha3')))

    """
    carp = _should_carp_about_exclusion(reason)
    
    def decorate(fn):
        fn_name = fn.__name__
        def maybe(*args, **kw):
            if _is_excluded(db, op, spec):
                msg = "'%s' unsupported on DB %s version '%s': %s" % (
                    fn_name, config.db.name, _server_version(), reason)
                print msg
                if carp:
                    print >> sys.stderr, msg
                return True
            else:
                return fn(*args, **kw)
        return function_named(maybe, fn_name)
    return decorate

def _should_carp_about_exclusion(reason):
    """Guard against forgotten exclusions."""
    assert reason
    for _ in ('todo', 'fixme', 'xxx'):
        if _ in reason.lower():
            return True
    else:
        if len(reason) < 4:
            return True

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

    vendor_spec = db_spec(db)

    if not vendor_spec(config.db):
        return False

    version = _server_version()

    oper = hasattr(op, '__call__') and op or _ops[op]
    return oper(version, spec)

def _server_version(bind=None):
    """Return a server_version_info tuple."""

    if bind is None:
        bind = config.db
    
    # force metadata to be retrieved
    conn = bind.connect()
    version = getattr(bind.dialect, 'server_version_info', ())
    conn.close()
    return version

def skip_if(predicate, reason=None):
    """Skip a test if predicate is true."""
    reason = reason or predicate.__name__
    carp = _should_carp_about_exclusion(reason)
    
    def decorate(fn):
        fn_name = fn.__name__
        def maybe(*args, **kw):
            if predicate():
                msg = "'%s' skipped on DB %s version '%s': %s" % (
                    fn_name, config.db.name, _server_version(), reason)
                print msg
                if carp:
                    print >> sys.stderr, msg
                return True
            else:
                return fn(*args, **kw)
        return function_named(maybe, fn_name)
    return decorate

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
            # todo: should probably be strict about this, too
            filters = [dict(action='ignore',
                            category=sa_exc.SAPendingDeprecationWarning)]
            if not messages:
                filters.append(dict(action='ignore',
                                     category=sa_exc.SAWarning))
            else:
                filters.extend(dict(action='ignore',
                                     message=message,
                                     category=sa_exc.SAWarning)
                                for message in messages)
            for f in filters:
                warnings.filterwarnings(**f)
            try:
                return fn(*args, **kw)
            finally:
                resetwarnings()
        return function_named(safe, fn.__name__)
    return decorate

def emits_warning_on(db, *warnings):
    """Mark a test as emitting a warning on a specific dialect.

    With no arguments, squelches all SAWarning failures.  Or pass one or more
    strings; these will be matched to the root of the warning description by
    warnings.filterwarnings().
    """
    spec = db_spec(db)
    
    def decorate(fn):
        def maybe(*args, **kw):
            if isinstance(db, basestring):
                if not spec(config.db):
                    return fn(*args, **kw)
                else:
                    wrapped = emits_warning(*warnings)(fn)
                    return wrapped(*args, **kw)
            else:
                if not _is_excluded(*db):
                    return fn(*args, **kw)
                else:
                    wrapped = emits_warning(*warnings)(fn)
                    return wrapped(*args, **kw)
        return function_named(maybe, fn.__name__)
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
            # todo: should probably be strict about this, too
            filters = [dict(action='ignore',
                            category=sa_exc.SAPendingDeprecationWarning)]
            if not messages:
                filters.append(dict(action='ignore',
                                    category=sa_exc.SADeprecationWarning))
            else:
                filters.extend(
                    [dict(action='ignore',
                          message=message,
                          category=sa_exc.SADeprecationWarning)
                     for message in
                     [ (m.startswith('//') and
                        ('Call to deprecated function ' + m[2:]) or m)
                       for m in messages] ])

            for f in filters:
                warnings.filterwarnings(**f)
            try:
                return fn(*args, **kw)
            finally:
                resetwarnings()
        return function_named(safe, fn.__name__)
    return decorate

def resetwarnings():
    """Reset warning behavior to testing defaults."""

    warnings.filterwarnings('ignore',
                            category=sa_exc.SAPendingDeprecationWarning) 
    warnings.filterwarnings('error', category=sa_exc.SADeprecationWarning)
    warnings.filterwarnings('error', category=sa_exc.SAWarning)

#    warnings.simplefilter('error')

    if sys.version_info < (2, 4):
        warnings.filterwarnings('ignore', category=FutureWarning)

def global_cleanup_assertions():
    """Check things that have to be finalized at the end of a test suite.
    
    Hardcoded at the moment, a modular system can be built here
    to support things like PG prepared transactions, tables all
    dropped, etc.
    
    """

    testutil.lazy_gc()
    assert not pool._refs
    
    

def against(*queries):
    """Boolean predicate, compares to testing database configuration.

    Given one or more dialect names, returns True if one is the configured
    database engine.

    Also supports comparison to database version when provided with one or
    more 3-tuples of dialect name, operator, and version specification::

      testing.against('mysql', 'postgresql')
      testing.against(('mysql', '>=', (5, 0, 0))
    """

    for query in queries:
        if isinstance(query, basestring):
            if db_spec(query)(config.db):
                return True
        else:
            name, op, spec = query
            if not db_spec(name)(config.db):
                continue

            have = _server_version()

            oper = hasattr(op, '__call__') and op or _ops[op]
            if oper(have, spec):
                return True
    return False

def _chain_decorators_on(fn, *decorators):
    """Apply a series of decorators to fn, returning a decorated function."""
    for decorator in reversed(decorators):
        fn = decorator(fn)
    return fn

def rowset(results):
    """Converts the results of sql execution into a plain set of column tuples.

    Useful for asserting the results of an unordered query.
    """

    return set([tuple(row) for row in results])


def eq_(a, b, msg=None):
    """Assert a == b, with repr messaging on failure."""
    assert a == b, msg or "%r != %r" % (a, b)

def ne_(a, b, msg=None):
    """Assert a != b, with repr messaging on failure."""
    assert a != b, msg or "%r == %r" % (a, b)

def is_(a, b, msg=None):
    """Assert a is b, with repr messaging on failure."""
    assert a is b, msg or "%r is not %r" % (a, b)

def is_not_(a, b, msg=None):
    """Assert a is not b, with repr messaging on failure."""
    assert a is not b, msg or "%r is %r" % (a, b)

def startswith_(a, fragment, msg=None):
    """Assert a.startswith(fragment), with repr messaging on failure."""
    assert a.startswith(fragment), msg or "%r does not start with %r" % (
        a, fragment)

def assert_raises(except_cls, callable_, *args, **kw):
    try:
        callable_(*args, **kw)
        success = False
    except except_cls, e:
        success = True
    
    # assert outside the block so it works for AssertionError too !
    assert success, "Callable did not raise an exception"

def assert_raises_message(except_cls, msg, callable_, *args, **kwargs):
    try:
        callable_(*args, **kwargs)
        assert False, "Callable did not raise an exception"
    except except_cls, e:
        assert re.search(msg, str(e)), "%r !~ %s" % (msg, e)

def fail(msg):
    assert False, msg
    
def fixture(table, columns, *rows):
    """Insert data into table after creation."""
    def onload(event, schema_item, connection):
        insert = table.insert()
        column_names = [col.key for col in columns]
        connection.execute(insert, [dict(zip(column_names, column_values))
                                    for column_values in rows])
    table.append_ddl_listener('after-create', onload)

def provide_metadata(fn):
    """Provides a bound MetaData object for a single test, 
    drops it afterwards."""
    def maybe(*args, **kw):
        metadata = schema.MetaData(db)
        context = dict(fn.func_globals)
        context['metadata'] = metadata
        # jython bug #1034
        rebound = types.FunctionType(
            fn.func_code, context, fn.func_name, fn.func_defaults,
            fn.func_closure)
        try:
            return rebound(*args, **kw)
        finally:
            metadata.drop_all()
    return function_named(maybe, fn.__name__)
    
def resolve_artifact_names(fn):
    """Decorator, augment function globals with tables and classes.

    Swaps out the function's globals at execution time. The 'global' statement
    will not work as expected inside a decorated function.

    """
    # This could be automatically applied to framework and test_ methods in
    # the MappedTest-derived test suites but... *some* explicitness for this
    # magic is probably good.  Especially as 'global' won't work- these
    # rebound functions aren't regular Python..
    #
    # Also: it's lame that CPython accepts a dict-subclass for globals, but
    # only calls dict methods.  That would allow 'global' to pass through to
    # the func_globals.
    def resolved(*args, **kwargs):
        self = args[0]
        context = dict(fn.func_globals)
        for source in self._artifact_registries:
            context.update(getattr(self, source))
        # jython bug #1034
        rebound = types.FunctionType(
            fn.func_code, context, fn.func_name, fn.func_defaults,
            fn.func_closure)
        return rebound(*args, **kwargs)
    return function_named(resolved, fn.func_name)

class adict(dict):
    """Dict keys available as attributes.  Shadows."""
    def __getattribute__(self, key):
        try:
            return self[key]
        except KeyError:
            return dict.__getattribute__(self, key)

    def get_all(self, *keys):
        return tuple([self[key] for key in keys])


class TestBase(object):
    # A sequence of database names to always run, regardless of the
    # constraints below.
    __whitelist__ = ()

    # A sequence of requirement names matching testing.requires decorators
    __requires__ = ()

    # A sequence of dialect names to exclude from the test class.
    __unsupported_on__ = ()

    # If present, test class is only runnable for the *single* specified
    # dialect.  If you need multiple, use __unsupported_on__ and invert.
    __only_on__ = None

    # A sequence of no-arg callables. If any are True, the entire testcase is
    # skipped.
    __skip_if__ = None

    _artifact_registries = ()

    def assert_(self, val, msg=None):
        assert val, msg
        
class AssertsCompiledSQL(object):
    def assert_compile(self, clause, result, params=None, checkparams=None, dialect=None, use_default_dialect=False):
        if use_default_dialect:
            dialect = default.DefaultDialect()
            
        if dialect is None:
            dialect = getattr(self, '__dialect__', None)

        kw = {}
        if params is not None:
            kw['column_keys'] = params.keys()
        
        if isinstance(clause, orm.Query):
            context = clause._compile_context()
            context.statement.use_labels = True
            clause = context.statement
            
        c = clause.compile(dialect=dialect, **kw)

        param_str = repr(getattr(c, 'params', {}))
        # Py3K
        #param_str = param_str.encode('utf-8').decode('ascii', 'ignore')
        
        print "\nSQL String:\n" + str(c) + param_str
        
        cc = re.sub(r'[\n\t]', '', str(c))
        
        eq_(cc, result, "%r != %r on dialect %r" % (cc, result, dialect))

        if checkparams is not None:
            eq_(c.construct_params(params), checkparams)

class ComparesTables(object):
    def assert_tables_equal(self, table, reflected_table, strict_types=False):
        assert len(table.c) == len(reflected_table.c)
        for c, reflected_c in zip(table.c, reflected_table.c):
            eq_(c.name, reflected_c.name)
            assert reflected_c is reflected_table.c[c.name]
            eq_(c.primary_key, reflected_c.primary_key)
            eq_(c.nullable, reflected_c.nullable)
            
            if strict_types:
                assert type(reflected_c.type) is type(c.type), \
                    "Type '%s' doesn't correspond to type '%s'" % (reflected_c.type, c.type)
            else:
                self.assert_types_base(reflected_c, c)

            if isinstance(c.type, sqltypes.String):
                eq_(c.type.length, reflected_c.type.length)

            eq_(set([f.column.name for f in c.foreign_keys]), set([f.column.name for f in reflected_c.foreign_keys]))
            if c.server_default:
                assert isinstance(reflected_c.server_default,
                                  schema.FetchedValue)

        assert len(table.primary_key) == len(reflected_table.primary_key)
        for c in table.primary_key:
            assert reflected_table.primary_key.columns[c.name] is not None
    
    def assert_types_base(self, c1, c2):
        assert c1.type._compare_type_affinity(c2.type),\
                "On column %r, type '%s' doesn't correspond to type '%s'" % \
                (c1.name, c1.type, c2.type)

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

        class frozendict(dict):
            def __hash__(self):
                return id(self)

        found = util.IdentitySet(result)
        expected = set([frozendict(e) for e in expected])

        for wrong in itertools.ifilterfalse(lambda o: type(o) == cls, found):
            fail('Unexpected type "%s", expected "%s"' % (
                type(wrong).__name__, cls.__name__))

        if len(found) != len(expected):
            fail('Unexpected object count "%s", expected "%s"' % (
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
                fail(
                    "Expected %s instance with attributes %s not found." % (
                    cls.__name__, repr(expected_item)))
        return True

    def assert_sql_execution(self, db, callable_, *rules):
        assertsql.asserter.add_rules(rules)
        try:
            callable_()
            assertsql.asserter.statement_complete()
        finally:
            assertsql.asserter.clear_rules()
            
    def assert_sql(self, db, callable_, list_, with_sequences=None):
        if with_sequences is not None and config.db.name in ('firebird', 'oracle', 'postgresql'):
            rules = with_sequences
        else:
            rules = list_
        
        newrules = []
        for rule in rules:
            if isinstance(rule, dict):
                newrule = assertsql.AllOf(*[
                    assertsql.ExactSQL(k, v) for k, v in rule.iteritems()
                ])
            else:
                newrule = assertsql.ExactSQL(*rule)
            newrules.append(newrule)
            
        self.assert_sql_execution(db, callable_, *newrules)

    def assert_sql_count(self, db, callable_, count):
        self.assert_sql_execution(db, callable_, assertsql.CountStatements(count))


