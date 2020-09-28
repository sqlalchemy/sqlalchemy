import collections
import logging

from . import config
from . import engines
from ..engine import url as sa_url
from ..util import compat

log = logging.getLogger(__name__)

FOLLOWER_IDENT = None


class register(object):
    def __init__(self):
        self.fns = {}

    @classmethod
    def init(cls, fn):
        return register().for_db("*")(fn)

    def for_db(self, dbname):
        def decorate(fn):
            self.fns[dbname] = fn
            return self

        return decorate

    def __call__(self, cfg, *arg):
        if isinstance(cfg, compat.string_types):
            url = sa_url.make_url(cfg)
        elif isinstance(cfg, sa_url.URL):
            url = cfg
        else:
            url = cfg.db.url
        backend = url.get_backend_name()
        if backend in self.fns:
            return self.fns[backend](cfg, *arg)
        else:
            return self.fns["*"](cfg, *arg)


def create_follower_db(follower_ident):
    for cfg in _configs_for_db_operation():
        log.info("CREATE database %s, URI %r", follower_ident, cfg.db.url)
        create_db(cfg, cfg.db, follower_ident)


def setup_config(db_url, options, file_config, follower_ident):
    # load the dialect, which should also have it set up its provision
    # hooks

    dialect = sa_url.make_url(db_url).get_dialect()
    dialect.load_provisioning()

    if follower_ident:
        db_url = follower_url_from_main(db_url, follower_ident)
    db_opts = {}
    update_db_opts(db_url, db_opts)
    eng = engines.testing_engine(db_url, db_opts)
    post_configure_engine(db_url, eng, follower_ident)
    eng.connect().close()

    cfg = config.Config.register(eng, db_opts, options, file_config)
    if follower_ident:
        configure_follower(cfg, follower_ident)
    return cfg


def drop_follower_db(follower_ident):
    for cfg in _configs_for_db_operation():
        log.info("DROP database %s, URI %r", follower_ident, cfg.db.url)
        drop_db(cfg, cfg.db, follower_ident)


def _configs_for_db_operation():
    hosts = set()

    for cfg in config.Config.all_configs():
        cfg.db.dispose()

    for cfg in config.Config.all_configs():
        url = cfg.db.url
        backend = url.get_backend_name()
        host_conf = (backend, url.username, url.host, url.database)

        if host_conf not in hosts:
            yield cfg
            hosts.add(host_conf)

    for cfg in config.Config.all_configs():
        cfg.db.dispose()


@register.init
def create_db(cfg, eng, ident):
    """Dynamically create a database for testing.

    Used when a test run will employ multiple processes, e.g., when run
    via `tox` or `pytest -n4`.
    """
    raise NotImplementedError("no DB creation routine for cfg: %s" % eng.url)


@register.init
def drop_db(cfg, eng, ident):
    """Drop a database that we dynamically created for testing."""
    raise NotImplementedError("no DB drop routine for cfg: %s" % eng.url)


@register.init
def update_db_opts(db_url, db_opts):
    """Set database options (db_opts) for a test database that we created."""
    pass


@register.init
def post_configure_engine(url, engine, follower_ident):
    """Perform extra steps after configuring an engine for testing.

    (For the internal dialects, currently only used by sqlite.)
    """
    pass


@register.init
def follower_url_from_main(url, ident):
    """Create a connection URL for a dynamically-created test database.

    :param url: the connection URL specified when the test run was invoked
    :param ident: the pytest-xdist "worker identifier" to be used as the
                  database name
    """
    url = sa_url.make_url(url)
    url.database = ident
    return url


@register.init
def configure_follower(cfg, ident):
    """Create dialect-specific config settings for a follower database."""
    pass


@register.init
def run_reap_dbs(url, ident):
    """Remove databases that were created during the test process, after the
    process has ended.

    This is an optional step that is invoked for certain backends that do not
    reliably release locks on the database as long as a process is still in
    use. For the internal dialects, this is currently only necessary for
    mssql and oracle.
    """
    pass


def reap_dbs(idents_file):
    log.info("Reaping databases...")

    urls = collections.defaultdict(set)
    idents = collections.defaultdict(set)
    dialects = {}

    with open(idents_file) as file_:
        for line in file_:
            line = line.strip()
            db_name, db_url = line.split(" ")
            url_obj = sa_url.make_url(db_url)
            if db_name not in dialects:
                dialects[db_name] = url_obj.get_dialect()
                dialects[db_name].load_provisioning()
            url_key = (url_obj.get_backend_name(), url_obj.host)
            urls[url_key].add(db_url)
            idents[url_key].add(db_name)

    for url_key in urls:
        url = list(urls[url_key])[0]
        ident = idents[url_key]
        run_reap_dbs(url, ident)


@register.init
def temp_table_keyword_args(cfg, eng):
    """Specify keyword arguments for creating a temporary Table.

    Dialect-specific implementations of this method will return the
    kwargs that are passed to the Table method when creating a temporary
    table for testing, e.g., in the define_temp_tables method of the
    ComponentReflectionTest class in suite/test_reflection.py
    """
    raise NotImplementedError(
        "no temp table keyword args routine for cfg: %s" % eng.url
    )
