import optparse, os, sys, re, ConfigParser, StringIO, time, warnings
logging = None

__all__ = 'parser', 'configure', 'options',

db = None
db_label, db_url, db_opts = None, None, {}

options = None
file_config = None

base_config = """
[db]
sqlite=sqlite:///:memory:
sqlite_file=sqlite:///querytest.db
postgres=postgres://scott:tiger@127.0.0.1:5432/test
postgresql=postgres://scott:tiger@127.0.0.1:5432/test
mysql=mysql://scott:tiger@127.0.0.1:3306/test
oracle=oracle://scott:tiger@127.0.0.1:1521
oracle8=oracle://scott:tiger@127.0.0.1:1521/?use_ansi=0
mssql=mssql://scott:tiger@SQUAWK\\SQLEXPRESS/test
firebird=firebird://sysdba:masterkey@localhost//tmp/test.fdb
maxdb=maxdb://MONA:RED@/maxdb1
"""

def _log(option, opt_str, value, parser):
    global logging
    if not logging:
        import logging
        logging.basicConfig()

    if opt_str.endswith('-info'):
        logging.getLogger(value).setLevel(logging.INFO)
    elif opt_str.endswith('-debug'):
        logging.getLogger(value).setLevel(logging.DEBUG)


def _list_dbs(*args):
    print "Available --db options (use --dburi to override)"
    for macro in sorted(file_config.options('db')):
        print "%20s\t%s" % (macro, file_config.get('db', macro))
    sys.exit(0)

def _server_side_cursors(options, opt_str, value, parser):
    db_opts['server_side_cursors'] = True

def _engine_strategy(options, opt_str, value, parser):
    if value:
        db_opts['strategy'] = value

class _ordered_map(object):
    def __init__(self):
        self._keys = list()
        self._data = dict()

    def __setitem__(self, key, value):
        if key not in self._keys:
            self._keys.append(key)
        self._data[key] = value

    def __iter__(self):
        for key in self._keys:
            yield self._data[key]

# at one point in refactoring, modules were injecting into the config
# process.  this could probably just become a list now.
post_configure = _ordered_map()

def _engine_uri(options, file_config):
    global db_label, db_url
    db_label = 'sqlite'
    if options.dburi:
        db_url = options.dburi
        db_label = db_url[:db_url.index(':')]
    elif options.db:
        db_label = options.db
        db_url = None

    if db_url is None:
        if db_label not in file_config.options('db'):
            raise RuntimeError(
                "Unknown engine.  Specify --dbs for known engines.")
        db_url = file_config.get('db', db_label)
post_configure['engine_uri'] = _engine_uri

def _require(options, file_config):
    if not(options.require or
           (file_config.has_section('require') and
            file_config.items('require'))):
        return

    try:
        import pkg_resources
    except ImportError:
        raise RuntimeError("setuptools is required for version requirements")

    cmdline = []
    for requirement in options.require:
        pkg_resources.require(requirement)
        cmdline.append(re.split('\s*(<!>=)', requirement, 1)[0])

    if file_config.has_section('require'):
        for label, requirement in file_config.items('require'):
            if not label == db_label or label.startswith('%s.' % db_label):
                continue
            seen = [c for c in cmdline if requirement.startswith(c)]
            if seen:
                continue
            pkg_resources.require(requirement)
post_configure['require'] = _require

def _engine_pool(options, file_config):
    if options.mockpool:
        from sqlalchemy import pool
        db_opts['poolclass'] = pool.AssertionPool
post_configure['engine_pool'] = _engine_pool

def _create_testing_engine(options, file_config):
    from sqlalchemy.test import engines, testing
    global db
    db = engines.testing_engine(db_url, db_opts)
    testing.db = db
post_configure['create_engine'] = _create_testing_engine

def _prep_testing_database(options, file_config):
    from sqlalchemy.test import engines
    from sqlalchemy import schema

    try:
        # also create alt schemas etc. here?
        if options.dropfirst:
            e = engines.utf8_engine()
            existing = e.table_names()
            if existing:
                print "Dropping existing tables in database: " + db_url
                try:
                    print "Tables: %s" % ', '.join(existing)
                except:
                    pass
                print "Abort within 5 seconds..."
                time.sleep(5)
                md = schema.MetaData(e, reflect=True)
                md.drop_all()
            e.dispose()
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception, e:
        warnings.warn(RuntimeWarning(
            "Error checking for existing tables in testing "
            "database: %s" % e))
post_configure['prep_db'] = _prep_testing_database

def _set_table_options(options, file_config):
    from sqlalchemy.test import schema

    table_options = schema.table_options
    for spec in options.tableopts:
        key, value = spec.split('=')
        table_options[key] = value

    if options.mysql_engine:
        table_options['mysql_engine'] = options.mysql_engine
post_configure['table_options'] = _set_table_options

def _reverse_topological(options, file_config):
    if options.reversetop:
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
post_configure['topological'] = _reverse_topological

