import logging
import os
import re
import sys
import time
import warnings
import ConfigParser
import StringIO
from config import db, db_label, db_url, file_config, base_config, \
                           post_configure, \
                           _list_dbs, _server_side_cursors, _engine_strategy, \
                           _engine_uri, _require, _engine_pool, \
                           _create_testing_engine, _prep_testing_database, \
                           _set_table_options, _reverse_topological, _log
from sqlalchemy.test import testing, config, requires
from nose.plugins import Plugin
from nose.util import tolist
import nose.case

log = logging.getLogger('nose.plugins.sqlalchemy')

class NoseSQLAlchemy(Plugin):
    """
    Handles the setup and extra properties required for testing SQLAlchemy
    """
    enabled = True
    name = 'sqlalchemy'
    score = 100

    def options(self, parser, env=os.environ):
        Plugin.options(self, parser, env)
        opt = parser.add_option
        #opt("--verbose", action="store_true", dest="verbose",
            #help="enable stdout echoing/printing")
        #opt("--quiet", action="store_true", dest="quiet", help="suppress output")
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
            help="Drop all tables in the target database first (use with caution on Oracle, MS-SQL)")
        opt("--mockpool", action="store_true", dest="mockpool",
            help="Use mock pool (asserts only one connection used)")
        opt("--enginestrategy", action="callback", type="string",
            callback=_engine_strategy,
            help="Engine strategy (plain or threadlocal, defaults to plain)")
        opt("--reversetop", action="store_true", dest="reversetop", default=False,
            help="Reverse the collection ordering for topological sorts (helps "
                  "reveal dependency issues)")
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

        import testing, requires
        testing.db = db
        testing.requires = requires

        # Lazy setup of other options (post coverage)
        for fn in post_configure:
            fn(options, file_config)
        
    def describeTest(self, test):
        return ""
        
    def wantClass(self, cls):
        """Return true if you want the main test selector to collect
        tests from this class, false if you don't, and None if you don't
        care.

        :Parameters:
           cls : class
             The class being examined by the selector

        """

        if not issubclass(cls, testing.TestBase):
            return False
        else:
            if (hasattr(cls, '__whitelist__') and
                testing.db.name in cls.__whitelist__):
                return True
            else:
                return not self.__should_skip_for(cls)
    
    def __should_skip_for(self, cls):
        if hasattr(cls, '__requires__'):
            def test_suite(): return 'ok'
            for requirement in cls.__requires__:
                check = getattr(requires, requirement)
                if check(test_suite)() != 'ok':
                    # The requirement will perform messaging.
                    return True
        if (hasattr(cls, '__unsupported_on__') and
            testing.db.name in cls.__unsupported_on__):
            print "'%s' unsupported on DB implementation '%s'" % (
                cls.__class__.__name__, testing.db.name)
            return True
        if (getattr(cls, '__only_on__', None) not in (None, testing.db.name)):
            print "'%s' unsupported on DB implementation '%s'" % (
                cls.__class__.__name__, testing.db.name)
            return True
        if (getattr(cls, '__skip_if__', False)):
            for c in getattr(cls, '__skip_if__'):
                if c():
                    print "'%s' skipped by %s" % (
                        cls.__class__.__name__, c.__name__)
                    return True
        for rule in getattr(cls, '__excluded_on__', ()):
            if testing._is_excluded(*rule):
                print "'%s' unsupported on DB %s version %s" % (
                    cls.__class__.__name__, testing.db.name,
                    _server_version())
                return True
        return False

    #def begin(self):
        #pass

    def beforeTest(self, test):
        testing.resetwarnings()

    def afterTest(self, test):
        testing.resetwarnings()
        
    #def handleError(self, test, err):
        #pass

    #def finalize(self, result=None):
        #pass
