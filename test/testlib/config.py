import testbase
import optparse, os, sys, ConfigParser, StringIO
logging, require = None, None

__all__ = 'parser', 'configure', 'options',

db, db_uri, db_type, db_label = None, None, None, None

options = None
file_config = None

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

parser = optparse.OptionParser(usage = "usage: %prog [options] [tests...]")

def configure():
    global options, config
    global getopts_options, file_config

    file_config = ConfigParser.ConfigParser()
    file_config.readfp(StringIO.StringIO(base_config))
    file_config.read(['test.cfg', os.path.expanduser('~/.satest.cfg')])

    # Opt parsing can fire immediate actions, like logging and coverage
    (options, args) = parser.parse_args()
    sys.argv[1:] = args

    # Lazy setup of other options (post coverage)
    for fn in post_configure:
        fn(options, file_config)

    return options, file_config

def _log(option, opt_str, value, parser):
    global logging
    if not logging:
        import logging
        logging.basicConfig()

    if opt_str.endswith('-info'):
        logging.getLogger(value).setLevel(logging.INFO)
    elif opt_str.endswith('-debug'):
        logging.getLogger(value).setLevel(logging.DEBUG)

def _start_coverage(option, opt_str, value, parser):
    import sys, atexit, coverage
    true_out = sys.stdout

    def _iter_covered_files():
        import sqlalchemy
        for rec in os.walk(os.path.dirname(sqlalchemy.__file__)):
            for x in rec[2]:
                if x.endswith('.py'):
                    yield os.path.join(rec[0], x)
    def _stop():
        coverage.stop()
        true_out.write("\nPreparing coverage report...\n")
        coverage.report(list(_iter_covered_files()),
                        show_missing=False, ignore_errors=False,
                        file=true_out)
    atexit.register(_stop)
    coverage.erase()
    coverage.start()
    
def _list_dbs(*args):
    print "Available --db options (use --dburi to override)"
    for macro in sorted(file_config.options('db')):
        print "%20s\t%s" % (macro, file_config.get('db', macro))
    sys.exit(0)

opt = parser.add_option
opt("--verbose", action="store_true", dest="verbose",
    help="enable stdout echoing/printing")
opt("--quiet", action="store_true", dest="quiet", help="suppress output")
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
opt("--mockpool", action="store_true", dest="mockpool",
    help="Use mock pool (asserts only one connection used)")
opt("--enginestrategy", action="store", dest="enginestrategy", default=None,
    help="Engine strategy (plain or threadlocal, defaults toplain)")
opt("--reversetop", action="store_true", dest="reversetop", default=False,
    help="Reverse the collection ordering for topological sorts (helps "
          "reveal dependency issues)")
opt("--serverside", action="store_true", dest="serverside",
    help="Turn on server side cursors for PG")
opt("--mysql-engine", action="store", dest="mysql_engine", default=None,
    help="Use the specified MySQL storage engine for all tables, default is "
         "a db-default/InnoDB combo.")
opt("--table-option", action="append", dest="tableopts", default=[],
    help="Add a dialect-specific table option, key=value")
opt("--coverage", action="callback", callback=_start_coverage,
    help="Dump a full coverage report after running tests")
opt("--profile", action="append", dest="profile_targets", default=[],
    help="Enable a named profile target (multiple OK.)")
opt("--profile-sort", action="store", dest="profile_sort", default=None,
    help="Sort profile stats with this comma-separated sort order")
opt("--profile-limit", type="int", action="store", dest="profile_limit",
    default=None,
    help="Limit function count in profile stats")

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
    
post_configure = _ordered_map()

def _engine_uri(options, file_config):
    global db_label, db_uri
    db_label = 'sqlite'
    if options.dburi:
        db_uri = options.dburi
        db_label = db_uri[:db_uri.index(':')]
    elif options.db:
        db_label = options.db
        db_uri = None

    if db_uri is None:
        if db_label not in file_config.options('db'):
            raise RuntimeError(
                "Unknown engine.  Specify --dbs for known engines.")
        db_uri = file_config.get('db', db_label)
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

def _create_testing_engine(options, file_config):
    from sqlalchemy import engine, schema
    global db, db_type
    engine_opts = {}
    if options.serverside:
        engine_opts['server_side_cursors'] = True
    
    if options.enginestrategy is not None:
        engine_opts['strategy'] = options.enginestrategy    

    if options.mockpool:
        db = engine.create_engine(db_uri, poolclass=pool.AssertionPool,
                                  **engine_opts)
    else:
        db = engine.create_engine(db_uri, **engine_opts)
    db_type = db.name

    print "Dropping existing tables in database: " + db_uri
    md = schema.MetaData(db, reflect=True)
    md.drop_all()
    
    # decorate the dialect's create_execution_context() method
    # to produce a wrapper
    from testlib.testing import ExecutionContextWrapper

    create_context = db.dialect.create_execution_context
    def create_exec_context(*args, **kwargs):
        return ExecutionContextWrapper(create_context(*args, **kwargs))
    db.dialect.create_execution_context = create_exec_context
post_configure['create_engine'] = _create_testing_engine

def _set_table_options(options, file_config):
    import testlib.schema
    
    table_options = testlib.schema.table_options
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

def _set_profile_targets(options, file_config):
    from testlib import profiling
    
    profile_config = profiling.profile_config

    for target in options.profile_targets:
        profile_config['targets'].add(target)

    if options.profile_sort:
        profile_config['sort'] = options.profile_sort.split(',')

    if options.profile_limit:
        profile_config['limit'] = options.profile_limit

    if options.quiet:
        profile_config['report'] = False

    # magic "all" target
    if 'all' in profiling.all_targets:
        targets = profile_config['targets']
        if 'all' in targets and len(targets) != 1:
            targets.clear()
            targets.add('all')
post_configure['profile_targets'] = _set_profile_targets
