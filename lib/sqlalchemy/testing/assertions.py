# testing/assertions.py
# Copyright (C) 2005-2015 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from __future__ import absolute_import

from . import util as testutil
from sqlalchemy import pool, orm, util
from sqlalchemy.engine import default, url
from sqlalchemy.util import decorator
from sqlalchemy import types as sqltypes, schema, exc as sa_exc
import warnings
import re
from .exclusions import db_spec, _is_excluded
from . import assertsql
from . import config
from .util import fail
import contextlib
from . import mock


def expect_warnings(*messages, **kw):
    """Context manager which expects one or more warnings.

    With no arguments, squelches all SAWarnings emitted via
    sqlalchemy.util.warn and sqlalchemy.util.warn_limited.   Otherwise
    pass string expressions that will match selected warnings via regex;
    all non-matching warnings are sent through.

    The expect version **asserts** that the warnings were in fact seen.

    Note that the test suite sets SAWarning warnings to raise exceptions.

    """
    return _expect_warnings(sa_exc.SAWarning, messages, **kw)


@contextlib.contextmanager
def expect_warnings_on(db, *messages, **kw):
    """Context manager which expects one or more warnings on specific
    dialects.

    The expect version **asserts** that the warnings were in fact seen.

    """
    spec = db_spec(db)

    if isinstance(db, util.string_types) and not spec(config._current):
        yield
    elif not _is_excluded(*db):
        yield
    else:
        with expect_warnings(*messages, **kw):
            yield


def emits_warning(*messages):
    """Decorator form of expect_warnings().

    Note that emits_warning does **not** assert that the warnings
    were in fact seen.

    """

    @decorator
    def decorate(fn, *args, **kw):
        with expect_warnings(assert_=False, *messages):
            return fn(*args, **kw)

    return decorate


def expect_deprecated(*messages, **kw):
    return _expect_warnings(sa_exc.SADeprecationWarning, messages, **kw)


def emits_warning_on(db, *messages):
    """Mark a test as emitting a warning on a specific dialect.

    With no arguments, squelches all SAWarning failures.  Or pass one or more
    strings; these will be matched to the root of the warning description by
    warnings.filterwarnings().

    Note that emits_warning_on does **not** assert that the warnings
    were in fact seen.

    """
    @decorator
    def decorate(fn, *args, **kw):
        with expect_warnings_on(db, *messages):
            return fn(*args, **kw)

    return decorate


def uses_deprecated(*messages):
    """Mark a test as immune from fatal deprecation warnings.

    With no arguments, squelches all SADeprecationWarning failures.
    Or pass one or more strings; these will be matched to the root
    of the warning description by warnings.filterwarnings().

    As a special case, you may pass a function name prefixed with //
    and it will be re-written as needed to match the standard warning
    verbiage emitted by the sqlalchemy.util.deprecated decorator.

    Note that uses_deprecated does **not** assert that the warnings
    were in fact seen.

    """

    @decorator
    def decorate(fn, *args, **kw):
        with expect_deprecated(*messages, assert_=False):
            return fn(*args, **kw)
    return decorate


@contextlib.contextmanager
def _expect_warnings(exc_cls, messages, regex=True, assert_=True):

    if regex:
        filters = [re.compile(msg, re.I) for msg in messages]
    else:
        filters = messages

    seen = set(filters)

    real_warn = warnings.warn

    def our_warn(msg, exception, *arg, **kw):
        if not issubclass(exception, exc_cls):
            return real_warn(msg, exception, *arg, **kw)

        if not filters:
            return

        for filter_ in filters:
            if (regex and filter_.match(msg)) or \
                    (not regex and filter_ == msg):
                seen.discard(filter_)
                break
        else:
            real_warn(msg, exception, *arg, **kw)

    with mock.patch("warnings.warn", our_warn):
        yield

    if assert_:
        assert not seen, "Warnings were not seen: %s" % \
            ", ".join("%r" % (s.pattern if regex else s) for s in seen)


def global_cleanup_assertions():
    """Check things that have to be finalized at the end of a test suite.

    Hardcoded at the moment, a modular system can be built here
    to support things like PG prepared transactions, tables all
    dropped, etc.

    """
    _assert_no_stray_pool_connections()

_STRAY_CONNECTION_FAILURES = 0


def _assert_no_stray_pool_connections():
    global _STRAY_CONNECTION_FAILURES

    # lazy gc on cPython means "do nothing."  pool connections
    # shouldn't be in cycles, should go away.
    testutil.lazy_gc()

    # however, once in awhile, on an EC2 machine usually,
    # there's a ref in there.  usually just one.
    if pool._refs:

        # OK, let's be somewhat forgiving.
        _STRAY_CONNECTION_FAILURES += 1

        print("Encountered a stray connection in test cleanup: %s"
              % str(pool._refs))
        # then do a real GC sweep.   We shouldn't even be here
        # so a single sweep should really be doing it, otherwise
        # there's probably a real unreachable cycle somewhere.
        testutil.gc_collect()

    # if we've already had two of these occurrences, or
    # after a hard gc sweep we still have pool._refs?!
    # now we have to raise.
    if pool._refs:
        err = str(pool._refs)

        # but clean out the pool refs collection directly,
        # reset the counter,
        # so the error doesn't at least keep happening.
        pool._refs.clear()
        _STRAY_CONNECTION_FAILURES = 0
        assert False, "Stray connection refused to leave "\
            "after gc.collect(): %s" % err
    elif _STRAY_CONNECTION_FAILURES > 10:
        assert False, "Encountered more than 10 stray connections"
        _STRAY_CONNECTION_FAILURES = 0


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
    except except_cls:
        success = True

    # assert outside the block so it works for AssertionError too !
    assert success, "Callable did not raise an exception"


def assert_raises_message(except_cls, msg, callable_, *args, **kwargs):
    try:
        callable_(*args, **kwargs)
        assert False, "Callable did not raise an exception"
    except except_cls as e:
        assert re.search(
            msg, util.text_type(e), re.UNICODE), "%r !~ %s" % (msg, e)
        print(util.text_type(e).encode('utf-8'))


class AssertsCompiledSQL(object):
    def assert_compile(self, clause, result, params=None,
                       checkparams=None, dialect=None,
                       checkpositional=None,
                       check_prefetch=None,
                       use_default_dialect=False,
                       allow_dialect_select=False,
                       literal_binds=False):
        if use_default_dialect:
            dialect = default.DefaultDialect()
        elif allow_dialect_select:
            dialect = None
        else:
            if dialect is None:
                dialect = getattr(self, '__dialect__', None)

            if dialect is None:
                dialect = config.db.dialect
            elif dialect == 'default':
                dialect = default.DefaultDialect()
            elif isinstance(dialect, util.string_types):
                dialect = url.URL(dialect).get_dialect()()

        kw = {}
        compile_kwargs = {}

        if params is not None:
            kw['column_keys'] = list(params)

        if literal_binds:
            compile_kwargs['literal_binds'] = True

        if isinstance(clause, orm.Query):
            context = clause._compile_context()
            context.statement.use_labels = True
            clause = context.statement

        if compile_kwargs:
            kw['compile_kwargs'] = compile_kwargs

        c = clause.compile(dialect=dialect, **kw)

        param_str = repr(getattr(c, 'params', {}))

        if util.py3k:
            param_str = param_str.encode('utf-8').decode('ascii', 'ignore')
            print(
                ("\nSQL String:\n" +
                 util.text_type(c) +
                 param_str).encode('utf-8'))
        else:
            print(
                "\nSQL String:\n" +
                util.text_type(c).encode('utf-8') +
                param_str)

        cc = re.sub(r'[\n\t]', '', util.text_type(c))

        eq_(cc, result, "%r != %r on dialect %r" % (cc, result, dialect))

        if checkparams is not None:
            eq_(c.construct_params(params), checkparams)
        if checkpositional is not None:
            p = c.construct_params(params)
            eq_(tuple([p[x] for x in c.positiontup]), checkpositional)
        if check_prefetch is not None:
            eq_(c.prefetch, check_prefetch)


class ComparesTables(object):

    def assert_tables_equal(self, table, reflected_table, strict_types=False):
        assert len(table.c) == len(reflected_table.c)
        for c, reflected_c in zip(table.c, reflected_table.c):
            eq_(c.name, reflected_c.name)
            assert reflected_c is reflected_table.c[c.name]
            eq_(c.primary_key, reflected_c.primary_key)
            eq_(c.nullable, reflected_c.nullable)

            if strict_types:
                msg = "Type '%s' doesn't correspond to type '%s'"
                assert isinstance(reflected_c.type, type(c.type)), \
                    msg % (reflected_c.type, c.type)
            else:
                self.assert_types_base(reflected_c, c)

            if isinstance(c.type, sqltypes.String):
                eq_(c.type.length, reflected_c.type.length)

            eq_(
                set([f.column.name for f in c.foreign_keys]),
                set([f.column.name for f in reflected_c.foreign_keys])
            )
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
        print(repr(result))
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
        for key, value in desc.items():
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

        class immutabledict(dict):
            def __hash__(self):
                return id(self)

        found = util.IdentitySet(result)
        expected = set([immutabledict(e) for e in expected])

        for wrong in util.itertools_filterfalse(lambda o:
                                                isinstance(o, cls), found):
            fail('Unexpected type "%s", expected "%s"' % (
                type(wrong).__name__, cls.__name__))

        if len(found) != len(expected):
            fail('Unexpected object count "%s", expected "%s"' % (
                len(found), len(expected)))

        NOVALUE = object()

        def _compare_item(obj, spec):
            for key, value in spec.items():
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

    def sql_execution_asserter(self, db=None):
        if db is None:
            from . import db as db

        return assertsql.assert_engine(db)

    def assert_sql_execution(self, db, callable_, *rules):
        with self.sql_execution_asserter(db) as asserter:
            callable_()
        asserter.assert_(*rules)

    def assert_sql(self, db, callable_, rules):

        newrules = []
        for rule in rules:
            if isinstance(rule, dict):
                newrule = assertsql.AllOf(*[
                    assertsql.CompiledSQL(k, v) for k, v in rule.items()
                ])
            else:
                newrule = assertsql.CompiledSQL(*rule)
            newrules.append(newrule)

        self.assert_sql_execution(db, callable_, *newrules)

    def assert_sql_count(self, db, callable_, count):
        self.assert_sql_execution(
            db, callable_, assertsql.CountStatements(count))

    @contextlib.contextmanager
    def assert_execution(self, *rules):
        assertsql.asserter.add_rules(rules)
        try:
            yield
            assertsql.asserter.statement_complete()
        finally:
            assertsql.asserter.clear_rules()

    def assert_statement_count(self, count):
        return self.assert_execution(assertsql.CountStatements(count))
