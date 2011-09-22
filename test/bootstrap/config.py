import optparse, os, sys, re, ConfigParser, time, warnings

# 2to3
import StringIO

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
postgresql=postgresql://scott:tiger@127.0.0.1:5432/test
postgres=postgresql://scott:tiger@127.0.0.1:5432/test
pg8000=postgresql+pg8000://scott:tiger@127.0.0.1:5432/test
postgresql_jython=postgresql+zxjdbc://scott:tiger@127.0.0.1:5432/test
mysql_jython=mysql+zxjdbc://scott:tiger@127.0.0.1:5432/test
mysql=mysql://scott:tiger@127.0.0.1:3306/test
pymysql=mysql+pymysql://scott:tiger@127.0.0.1:3306/test?use_unicode=0&charset=utf8
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

def _zero_timeout(options, opt_str, value, parser):
    warnings.warn("--zero-timeout testing option is now on in all cases")

def _engine_strategy(options, opt_str, value, parser):
    if value:
        db_opts['strategy'] = value

pre_configure = []
post_configure = []

def _setup_options(opt, file_config):
    global options
    options = opt
pre_configure.append(_setup_options)

def _monkeypatch_cdecimal(options, file_config):
    if options.cdecimal:
        import sys
        import cdecimal
        sys.modules['decimal'] = cdecimal
pre_configure.append(_monkeypatch_cdecimal)

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
post_configure.append(_engine_uri)

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
post_configure.append(_require)

def _engine_pool(options, file_config):
    if options.mockpool:
        from sqlalchemy import pool
        db_opts['poolclass'] = pool.AssertionPool
post_configure.append(_engine_pool)

def _create_testing_engine(options, file_config):
    from test.lib import engines, testing
    global db
    db = engines.testing_engine(db_url, db_opts)
    testing.db = db
post_configure.append(_create_testing_engine)

def _prep_testing_database(options, file_config):
    from test.lib import engines
    from sqlalchemy import schema

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

post_configure.append(_prep_testing_database)

def _set_table_options(options, file_config):
    from test.lib import schema

    table_options = schema.table_options
    for spec in options.tableopts:
        key, value = spec.split('=')
        table_options[key] = value

    if options.mysql_engine:
        table_options['mysql_engine'] = options.mysql_engine
post_configure.append(_set_table_options)

def _reverse_topological(options, file_config):
    if options.reversetop:
        from sqlalchemy.orm import unitofwork, session, mapper, dependency
        from sqlalchemy.util import topological
        from test.lib.util import RandomSet
        topological.set = unitofwork.set = session.set = mapper.set = dependency.set = RandomSet
post_configure.append(_reverse_topological)

