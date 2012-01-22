import logging
import os
import re
import sys
import time
import warnings
import ConfigParser
import StringIO

import nose.case
from nose.plugins import Plugin
from nose import SkipTest
from test.bootstrap import config

from test.bootstrap.config import (
    _create_testing_engine, _engine_pool, _engine_strategy, _engine_uri, _list_dbs, _log,
    _prep_testing_database, _require, _reverse_topological, _server_side_cursors,
    _monkeypatch_cdecimal, _zero_timeout,
    _set_table_options, base_config, db, db_label, db_url, file_config, post_configure, 
    pre_configure)

log = logging.getLogger('nose.plugins.sqlalchemy')

class NoseSQLAlchemy(Plugin):
    """
    Handles the setup and extra properties required for testing SQLAlchemy
    """
    enabled = True

    # nose 1.0 will allow us to replace the old "sqlalchemy" plugin,
    # if installed, using the same name, but nose 1.0 isn't released yet...
    name = '_sqlalchemy'
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
        opt("--db", action="store", dest="db", default="sqlite",
            help="Use prefab database uri")
        opt('--dbs', action='callback', callback=_list_dbs,
            help="List available prefab dbs")
        opt("--dburi", action="store", dest="dburi",
            help="Database uri (overrides --db)")
        opt("--dropfirst", action="store_true", dest="dropfirst",
            help="Drop all tables in the target database first (use with caution on Oracle, "
            "MS-SQL)")
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

        global file_config
        file_config = ConfigParser.ConfigParser()
        file_config.readfp(StringIO.StringIO(base_config))
        file_config.read(['test.cfg', os.path.expanduser('~/.satest.cfg')])
        config.file_config = file_config

    def configure(self, options, conf):
        Plugin.configure(self, options, conf)
        self.options = options
        for fn in pre_configure:
            fn(self.options, file_config)

    def begin(self):
        global testing, requires, util, fixtures, engines
        from test.lib import testing, requires, fixtures, engines
        from sqlalchemy import util

        testing.db = db
        testing.requires = requires

        # Lazy setup of other options (post coverage)
        for fn in post_configure:
            fn(self.options, file_config)

    def describeTest(self, test):
        return ""

    def wantFunction(self, fn):
        if fn.__module__.startswith('test.lib') or \
            fn.__module__.startswith('test.bootstrap'):
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
        else:
            if hasattr(cls, 'setup_class'):
                existing_setup = cls.setup_class.im_func
            else:
                existing_setup = None
            @classmethod
            def setup_class(cls):
                self._do_skips(cls)
                if existing_setup:
                    existing_setup(cls)
            cls.setup_class = setup_class

            return True

    def _do_skips(self, cls):
        if hasattr(cls, '__requires__'):
            def test_suite(): return 'ok'
            test_suite.__name__ = cls.__name__
            for requirement in cls.__requires__:
                check = getattr(requires, requirement)
                check(test_suite)()

        if cls.__unsupported_on__:
            spec = testing.db_spec(*cls.__unsupported_on__)
            if spec(testing.db):
                raise SkipTest(
                    "'%s' unsupported on DB implementation '%s'" % (
                     cls.__name__, testing.db.name)
                    )

        if getattr(cls, '__only_on__', None):
            spec = testing.db_spec(*util.to_list(cls.__only_on__))
            if not spec(testing.db):
                raise SkipTest(
                    "'%s' unsupported on DB implementation '%s'" % (
                     cls.__name__, testing.db.name)
                    )

        if getattr(cls, '__skip_if__', False):
            for c in getattr(cls, '__skip_if__'):
                if c():
                    raise SkipTest("'%s' skipped by %s" % (
                        cls.__name__, c.__name__)
                    )

        for db, op, spec in getattr(cls, '__excluded_on__', ()):
            testing.exclude(db, op, spec, "'%s' unsupported on DB %s version %s" % (
                    cls.__name__, testing.db.name,
                    testing._server_version()))

    def beforeTest(self, test):
        testing.resetwarnings()

    def afterTest(self, test):
        engines.testing_reaper._after_test_ctx()
        testing.resetwarnings()

    def stopContext(self, ctx):
        engines.testing_reaper._stop_test_ctx()
        if not config.options.low_connections:
            testing.global_cleanup_assertions()
