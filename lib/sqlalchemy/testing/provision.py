import collections
import logging

from . import config
from . import engines
from ..dialects.mssql.provision import _reap_mssql_dbs
from ..dialects.oracle.provision import _reap_oracle_dbs
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


# def configure_follower(follower_ident):
#     for cfg in config.Config.all_configs():
#         _configure_follower(cfg, follower_ident)


def setup_config(db_url, options, file_config, follower_ident):
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
    """write a docstring """
    raise NotImplementedError("no DB creation routine for cfg: %s" % eng.url)


@register.init
def drop_db(cfg, eng, ident):
    """write a docstring """
    raise NotImplementedError("no DB drop routine for cfg: %s" % eng.url)


@register.init
def update_db_opts(db_url, db_opts):
    """write a docstring """
    pass


@register.init
def configure_follower(cfg, ident):
    """write a docstring """
    pass


@register.init
def post_configure_engine(url, engine, follower_ident):
    """write a docstring """
    pass


@register.init
def follower_url_from_main(url, ident):
    """write a docstring """
    url = sa_url.make_url(url)
    url.database = ident
    return url


def reap_dbs(idents_file):
    log.info("Reaping databases...")

    urls = collections.defaultdict(set)
    idents = collections.defaultdict(set)

    with open(idents_file) as file_:
        for line in file_:
            line = line.strip()
            db_name, db_url = line.split(" ")
            url_obj = sa_url.make_url(db_url)
            url_key = (url_obj.get_backend_name(), url_obj.host)
            urls[url_key].add(db_url)
            idents[url_key].add(db_name)

    for url_key in urls:
        backend = url_key[0]
        url = list(urls[url_key])[0]
        ident = idents[url_key]
        if backend == "oracle":
            _reap_oracle_dbs(url, ident)
        elif backend == "mssql":
            _reap_mssql_dbs(url, ident)
