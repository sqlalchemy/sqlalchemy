"""Enhance nose with extra options and behaviors for running SQLAlchemy tests.

When running ./sqla_nose.py, this module is imported relative to the
"plugins" package as a top level package by the sqla_nose.py runner,
so that the plugin can be loaded with the rest of nose including the coverage
plugin before any of SQLAlchemy itself is imported, so that coverage works.

When third party libraries use this plugin, it can be imported
normally as "from sqlalchemy.testing.plugin import noseplugin".

"""
import os
import ConfigParser

from nose.plugins import Plugin
from nose import SkipTest
from . import config

from .config import _log, _list_dbs, _zero_timeout, \
    _engine_strategy, _server_side_cursors, pre_configure,\
    post_configure

# late imports
fixtures = None
engines = None
exclusions = None
warnings = None
profiling = None
assertions = None
requirements = None
util = None
file_config = None

class NoseSQLAlchemy(Plugin):
    """
    Handles the setup and extra properties required for testing SQLAlchemy
    """
    enabled = True

    name = 'sqla_testing'
    score = 100

    def options(self, parser, env=os.environ):
        Plugin.options(self, parser, env)
        opt = parser.add_option
        opt("--log-info", action="callback", type="string", callback=_log,
            help="turn on info logging for <LOG> (multiple OK)")
        opt("--log-debug", action="callback", type="string", callback=_log,
            help="turn on debug logging for <LOG> (multiple OK)")
        opt("--require", action="append", dest="require", default=[],
            help="require a particular driver or module version (multiple OK)")
        opt("--db", action="store", dest="db", default="default",
            help="Use prefab database uri")
        opt('--dbs', action='callback', callback=_list_dbs,
            help="List available prefab dbs")
        opt("--dburi", action="store", dest="dburi",
            help="Database uri (overrides --db)")
        opt("--dropfirst", action="store_true", dest="dropfirst",
            help="Drop all tables in the target database first")
        opt("--mockpool", action="store_true", dest="mockpool",
            help="Use mock pool (asserts only one connection used)")
        opt("--zero-timeout", action="callback", callback=_zero_timeout,
            help="Set pool_timeout to zero, applies to QueuePool only")
        opt("--low-connections", action="store_true", dest="low_connections",
            help="Use a low number of distinct connections - i.e. for Oracle TNS"
        )
        opt("--enginestrategy", action="callback", type="string",
            callback=_engine_strategy,
            help="Engine strategy (plain or threadlocal, defaults to plain)")
        opt("--reversetop", action="store_true", dest="reversetop", default=False,
            help="Use a random-ordering set implementation in the ORM (helps "
                  "reveal dependency issues)")
        opt("--with-cdecimal", action="store_true", dest="cdecimal", default=False,
            help="Monkeypatch the cdecimal library into Python 'decimal' for all tests")
        opt("--unhashable", action="store_true", dest="unhashable", default=False,
            help="Disallow SQLAlchemy from performing a hash() on mapped test objects.")
        opt("--noncomparable", action="store_true", dest="noncomparable", default=False,
            help="Disallow SQLAlchemy from performing == on mapped test objects.")
        opt("--truthless", action="store_true", dest="truthless", default=False,
            help="Disallow SQLAlchemy from truth-evaluating mapped test objects.")
        opt("--serverside", action="callback", callback=_server_side_cursors,
            help="Turn on server side cursors for PG")
        opt("--mysql-engine", action="store", dest="mysql_engine", default=None,
            help="Use the specified MySQL storage engine for all tables, default is "
                 "a db-default/InnoDB combo.")
        opt("--table-option", action="append", dest="tableopts", default=[],
            help="Add a dialect-specific table option, key=value")
        opt("--write-profiles", action="store_true", dest="write_profiles", default=False,
                help="Write/update profiling data.")
        global file_config
        file_config = ConfigParser.ConfigParser()
        file_config.read(['setup.cfg', 'test.cfg', os.path.expanduser('~/.satest.cfg')])
        config.file_config = file_config

    def configure(self, options, conf):
        Plugin.configure(self, options, conf)
        self.options = options
        for fn in pre_configure:
            fn(self.options, file_config)

    def begin(self):
        # Lazy setup of other options (post coverage)
        for fn in post_configure:
            fn(self.options, file_config)

        # late imports, has to happen after config as well
        # as nose plugins like coverage
        global util, fixtures, engines, exclusions, \
                        assertions, warnings, profiling
        from sqlalchemy.testing import fixtures, engines, exclusions, \
                        assertions, warnings, profiling
        from sqlalchemy import util

    def describeTest(self, test):
        return ""

    def wantFunction(self, fn):
        if fn.__module__.startswith('sqlalchemy.testing'):
            return False

    def wantClass(self, cls):
        """Return true if you want the main test selector to collect
        tests from this class, false if you don't, and None if you don't
        care.

        :Parameters:
           cls : class
             The class being examined by the selector

        """

        if not issubclass(cls, fixtures.TestBase):
            return False
        elif cls.__name__.startswith('_'):
            return False
        else:
            return True

    def _do_skips(self, cls):
        from sqlalchemy.testing import config
        if hasattr(cls, '__requires__'):
            def test_suite():
                return 'ok'
            test_suite.__name__ = cls.__name__
            for requirement in cls.__requires__:
                check = getattr(config.requirements, requirement)
                check(test_suite)()

        if cls.__unsupported_on__:
            spec = exclusions.db_spec(*cls.__unsupported_on__)
            if spec(config.db):
                raise SkipTest(
                    "'%s' unsupported on DB implementation '%s'" % (
                     cls.__name__, config.db.name)
                    )

        if getattr(cls, '__only_on__', None):
            spec = exclusions.db_spec(*util.to_list(cls.__only_on__))
            if not spec(config.db):
                raise SkipTest(
                    "'%s' unsupported on DB implementation '%s'" % (
                     cls.__name__, config.db.name)
                    )

        if getattr(cls, '__skip_if__', False):
            for c in getattr(cls, '__skip_if__'):
                if c():
                    raise SkipTest("'%s' skipped by %s" % (
                        cls.__name__, c.__name__)
                    )

        for db, op, spec in getattr(cls, '__excluded_on__', ()):
            exclusions.exclude(db, op, spec,
                    "'%s' unsupported on DB %s version %s" % (
                    cls.__name__, config.db.name,
                    exclusions._server_version(config.db)))

    def beforeTest(self, test):
        warnings.resetwarnings()
        profiling._current_test = test.id()

    def afterTest(self, test):
        engines.testing_reaper._after_test_ctx()
        warnings.resetwarnings()

    def startContext(self, ctx):
        if not isinstance(ctx, type) \
            or not issubclass(ctx, fixtures.TestBase):
            return
        self._do_skips(ctx)

    def stopContext(self, ctx):
        if not isinstance(ctx, type) \
            or not issubclass(ctx, fixtures.TestBase):
            return
        engines.testing_reaper._stop_test_ctx()
        if not config.options.low_connections:
            assertions.global_cleanup_assertions()
