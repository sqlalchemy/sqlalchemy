# plugin/plugin_base.py
# Copyright (C) 2005-2018 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Testing extensions.

this module is designed to work as a testing-framework-agnostic library,
so that we can continue to support nose and also begin adding new
functionality via py.test.

"""

from __future__ import absolute_import

import sys
import re

py3k = sys.version_info >= (3, 0)

if py3k:
    import configparser
else:
    import ConfigParser as configparser

# late imports
fixtures = None
engines = None
exclusions = None
warnings = None
profiling = None
assertions = None
requirements = None
config = None
testing = None
util = None
file_config = None


logging = None
include_tags = set()
exclude_tags = set()
options = None


def setup_options(make_option):
    make_option("--log-info", action="callback", type="string", callback=_log,
                help="turn on info logging for <LOG> (multiple OK)")
    make_option("--log-debug", action="callback",
                type="string", callback=_log,
                help="turn on debug logging for <LOG> (multiple OK)")
    make_option("--db", action="append", type="string", dest="db",
                help="Use prefab database uri. Multiple OK, "
                "first one is run by default.")
    make_option('--dbs', action='callback', zeroarg_callback=_list_dbs,
                help="List available prefab dbs")
    make_option("--dburi", action="append", type="string", dest="dburi",
                help="Database uri.  Multiple OK, "
                "first one is run by default.")
    make_option("--dropfirst", action="store_true", dest="dropfirst",
                help="Drop all tables in the target database first")
    make_option("--backend-only", action="store_true", dest="backend_only",
                help="Run only tests marked with __backend__")
    make_option("--nomemory", action="store_true", dest="nomemory",
                help="Don't run memory profiling tests")
    make_option("--postgresql-templatedb", type="string",
                help="name of template database to use for Postgresql "
                     "CREATE DATABASE (defaults to current database)")
    make_option("--low-connections", action="store_true",
                dest="low_connections",
                help="Use a low number of distinct connections - "
                "i.e. for Oracle TNS")
    make_option("--write-idents", type="string", dest="write_idents",
                help="write out generated follower idents to <file>, "
                "when -n<num> is used")
    make_option("--reversetop", action="store_true",
                dest="reversetop", default=False,
                help="Use a random-ordering set implementation in the ORM "
                "(helps reveal dependency issues)")
    make_option("--requirements", action="callback", type="string",
                callback=_requirements_opt,
                help="requirements class for testing, overrides setup.cfg")
    make_option("--with-cdecimal", action="store_true",
                dest="cdecimal", default=False,
                help="Monkeypatch the cdecimal library into Python 'decimal' "
                "for all tests")
    make_option("--include-tag", action="callback", callback=_include_tag,
                type="string",
                help="Include tests with tag <tag>")
    make_option("--exclude-tag", action="callback", callback=_exclude_tag,
                type="string",
                help="Exclude tests with tag <tag>")
    make_option("--write-profiles", action="store_true",
                dest="write_profiles", default=False,
                help="Write/update failing profiling data.")
    make_option("--force-write-profiles", action="store_true",
                dest="force_write_profiles", default=False,
                help="Unconditionally write/update profiling data.")


def configure_follower(follower_ident):
    """Configure required state for a follower.

    This invokes in the parent process and typically includes
    database creation.

    """
    from sqlalchemy.testing import provision
    provision.FOLLOWER_IDENT = follower_ident


def memoize_important_follower_config(dict_):
    """Store important configuration we will need to send to a follower.

    This invokes in the parent process after normal config is set up.

    This is necessary as py.test seems to not be using forking, so we
    start with nothing in memory, *but* it isn't running our argparse
    callables, so we have to just copy all of that over.

    """
    dict_['memoized_config'] = {
        'include_tags': include_tags,
        'exclude_tags': exclude_tags
    }


def restore_important_follower_config(dict_):
    """Restore important configuration needed by a follower.

    This invokes in the follower process.

    """
    global include_tags, exclude_tags
    include_tags.update(dict_['memoized_config']['include_tags'])
    exclude_tags.update(dict_['memoized_config']['exclude_tags'])


def read_config():
    global file_config
    file_config = configparser.ConfigParser()
    file_config.read(['setup.cfg', 'test.cfg'])


def pre_begin(opt):
    """things to set up early, before coverage might be setup."""
    global options
    options = opt
    for fn in pre_configure:
        fn(options, file_config)


def set_coverage_flag(value):
    options.has_coverage = value

_skip_test_exception = None


def set_skip_test(exc):
    global _skip_test_exception
    _skip_test_exception = exc


def post_begin():
    """things to set up later, once we know coverage is running."""
    # Lazy setup of other options (post coverage)
    for fn in post_configure:
        fn(options, file_config)

    # late imports, has to happen after config as well
    # as nose plugins like coverage
    global util, fixtures, engines, exclusions, \
        assertions, warnings, profiling,\
        config, testing
    from sqlalchemy import testing # noqa
    from sqlalchemy.testing import fixtures, engines, exclusions  # noqa
    from sqlalchemy.testing import assertions, warnings, profiling # noqa
    from sqlalchemy.testing import config  # noqa
    from sqlalchemy import util  # noqa
    warnings.setup_filters()



def _log(opt_str, value, parser):
    global logging
    if not logging:
        import logging
        logging.basicConfig()

    if opt_str.endswith('-info'):
        logging.getLogger(value).setLevel(logging.INFO)
    elif opt_str.endswith('-debug'):
        logging.getLogger(value).setLevel(logging.DEBUG)


def _list_dbs(*args):
    print("Available --db options (use --dburi to override)")
    for macro in sorted(file_config.options('db')):
        print("%20s\t%s" % (macro, file_config.get('db', macro)))
    sys.exit(0)


def _requirements_opt(opt_str, value, parser):
    _setup_requirements(value)


def _exclude_tag(opt_str, value, parser):
    exclude_tags.add(value.replace('-', '_'))


def _include_tag(opt_str, value, parser):
    include_tags.add(value.replace('-', '_'))

pre_configure = []
post_configure = []


def pre(fn):
    pre_configure.append(fn)
    return fn


def post(fn):
    post_configure.append(fn)
    return fn


@pre
def _setup_options(opt, file_config):
    global options
    options = opt


@pre
def _set_nomemory(opt, file_config):
    if opt.nomemory:
        exclude_tags.add("memory_intensive")


@pre
def _monkeypatch_cdecimal(options, file_config):
    if options.cdecimal:
        import cdecimal
        sys.modules['decimal'] = cdecimal


@post
def _init_skiptest(options, file_config):
    from sqlalchemy.testing import config

    config._skip_test_exception = _skip_test_exception


@post
def _engine_uri(options, file_config):
    from sqlalchemy.testing import config
    from sqlalchemy import testing
    from sqlalchemy.testing import provision

    if options.dburi:
        db_urls = list(options.dburi)
    else:
        db_urls = []

    if options.db:
        for db_token in options.db:
            for db in re.split(r'[,\s]+', db_token):
                if db not in file_config.options('db'):
                    raise RuntimeError(
                        "Unknown URI specifier '%s'.  "
                        "Specify --dbs for known uris."
                        % db)
                else:
                    db_urls.append(file_config.get('db', db))

    if not db_urls:
        db_urls.append(file_config.get('db', 'default'))

    config._current = None
    for db_url in db_urls:

        if options.write_idents and provision.FOLLOWER_IDENT: # != 'master':
            with open(options.write_idents, "a") as file_:
                file_.write(provision.FOLLOWER_IDENT + " " + db_url + "\n")

        cfg = provision.setup_config(
            db_url, options, file_config, provision.FOLLOWER_IDENT)

        if not config._current:
            cfg.set_as_current(cfg, testing)


@post
def _requirements(options, file_config):

    requirement_cls = file_config.get('sqla_testing', "requirement_cls")
    _setup_requirements(requirement_cls)


def _setup_requirements(argument):
    from sqlalchemy.testing import config
    from sqlalchemy import testing

    if config.requirements is not None:
        return

    modname, clsname = argument.split(":")

    # importlib.import_module() only introduced in 2.7, a little
    # late
    mod = __import__(modname)
    for component in modname.split(".")[1:]:
        mod = getattr(mod, component)
    req_cls = getattr(mod, clsname)

    config.requirements = testing.requires = req_cls()


@post
def _prep_testing_database(options, file_config):
    from sqlalchemy.testing import config, util
    from sqlalchemy.testing.exclusions import against
    from sqlalchemy import schema, inspect

    if options.dropfirst:
        for cfg in config.Config.all_configs():
            e = cfg.db
            inspector = inspect(e)
            try:
                view_names = inspector.get_view_names()
            except NotImplementedError:
                pass
            else:
                for vname in view_names:
                    e.execute(schema._DropView(
                        schema.Table(vname, schema.MetaData())
                    ))

            if config.requirements.schemas.enabled_for_config(cfg):
                try:
                    view_names = inspector.get_view_names(
                        schema="test_schema")
                except NotImplementedError:
                    pass
                else:
                    for vname in view_names:
                        e.execute(schema._DropView(
                            schema.Table(vname, schema.MetaData(),
                                         schema="test_schema")
                        ))

            util.drop_all_tables(e, inspector)

            if config.requirements.schemas.enabled_for_config(cfg):
                util.drop_all_tables(e, inspector, schema=cfg.test_schema)

            if against(cfg, "postgresql"):
                from sqlalchemy.dialects import postgresql
                for enum in inspector.get_enums("*"):
                    e.execute(postgresql.DropEnumType(
                        postgresql.ENUM(
                            name=enum['name'],
                            schema=enum['schema'])))


@post
def _reverse_topological(options, file_config):
    if options.reversetop:
        from sqlalchemy.orm.util import randomize_unitofwork
        randomize_unitofwork()


@post
def _post_setup_options(opt, file_config):
    from sqlalchemy.testing import config
    config.options = options
    config.file_config = file_config


@post
def _setup_profiling(options, file_config):
    from sqlalchemy.testing import profiling
    profiling._profile_stats = profiling.ProfileStatsFile(
        file_config.get('sqla_testing', 'profile_file'))


def want_class(cls):
    if not issubclass(cls, fixtures.TestBase):
        return False
    elif cls.__name__.startswith('_'):
        return False
    elif config.options.backend_only and not getattr(cls, '__backend__',
                                                     False):
        return False
    else:
        return True


def want_method(cls, fn):
    if not fn.__name__.startswith("test_"):
        return False
    elif fn.__module__ is None:
        return False
    elif include_tags:
        return (
            hasattr(cls, '__tags__') and
            exclusions.tags(cls.__tags__).include_test(
                include_tags, exclude_tags)
        ) or (
            hasattr(fn, '_sa_exclusion_extend') and
            fn._sa_exclusion_extend.include_test(
                include_tags, exclude_tags)
        )
    elif exclude_tags and hasattr(cls, '__tags__'):
        return exclusions.tags(cls.__tags__).include_test(
            include_tags, exclude_tags)
    elif exclude_tags and hasattr(fn, '_sa_exclusion_extend'):
        return fn._sa_exclusion_extend.include_test(include_tags, exclude_tags)
    else:
        return True


def generate_sub_tests(cls, module):
    if getattr(cls, '__backend__', False):
        for cfg in _possible_configs_for_cls(cls):
            orig_name = cls.__name__

            # we can have special chars in these names except for the
            # pytest junit plugin, which is tripped up by the brackets
            # and periods, so sanitize

            alpha_name = re.sub('[_\[\]\.]+', '_', cfg.name)
            alpha_name = re.sub('_+$', '', alpha_name)
            name = "%s_%s" % (cls.__name__, alpha_name)
            subcls = type(
                name,
                (cls, ),
                {
                    "_sa_orig_cls_name": orig_name,
                    "__only_on_config__": cfg
                }
            )
            setattr(module, name, subcls)
            yield subcls
    else:
        yield cls


def start_test_class(cls):
    _do_skips(cls)
    _setup_engine(cls)


def stop_test_class(cls):
    #from sqlalchemy import inspect
    #assert not inspect(testing.db).get_table_names()
    engines.testing_reaper._stop_test_ctx()
    try:
        if not options.low_connections:
            assertions.global_cleanup_assertions()
    finally:
        _restore_engine()


def _restore_engine():
    config._current.reset(testing)


def final_process_cleanup():
    engines.testing_reaper._stop_test_ctx_aggressive()
    assertions.global_cleanup_assertions()
    _restore_engine()


def _setup_engine(cls):
    if getattr(cls, '__engine_options__', None):
        eng = engines.testing_engine(options=cls.__engine_options__)
        config._current.push_engine(eng, testing)


def before_test(test, test_module_name, test_class, test_name):

    # like a nose id, e.g.:
    # "test.aaa_profiling.test_compiler.CompileTest.test_update_whereclause"

    name = getattr(test_class, '_sa_orig_cls_name', test_class.__name__)

    id_ = "%s.%s.%s" % (test_module_name, name, test_name)

    profiling._current_test = id_


def after_test(test):
    engines.testing_reaper._after_test_ctx()


def _possible_configs_for_cls(cls, reasons=None):
    all_configs = set(config.Config.all_configs())

    if cls.__unsupported_on__:
        spec = exclusions.db_spec(*cls.__unsupported_on__)
        for config_obj in list(all_configs):
            if spec(config_obj):
                all_configs.remove(config_obj)

    if getattr(cls, '__only_on__', None):
        spec = exclusions.db_spec(*util.to_list(cls.__only_on__))
        for config_obj in list(all_configs):
            if not spec(config_obj):
                all_configs.remove(config_obj)

    if getattr(cls, '__only_on_config__', None):
        all_configs.intersection_update([cls.__only_on_config__])

    if hasattr(cls, '__requires__'):
        requirements = config.requirements
        for config_obj in list(all_configs):
            for requirement in cls.__requires__:
                check = getattr(requirements, requirement)

                skip_reasons = check.matching_config_reasons(config_obj)
                if skip_reasons:
                    all_configs.remove(config_obj)
                    if reasons is not None:
                        reasons.extend(skip_reasons)
                    break

    if hasattr(cls, '__prefer_requires__'):
        non_preferred = set()
        requirements = config.requirements
        for config_obj in list(all_configs):
            for requirement in cls.__prefer_requires__:
                check = getattr(requirements, requirement)

                if not check.enabled_for_config(config_obj):
                    non_preferred.add(config_obj)
        if all_configs.difference(non_preferred):
            all_configs.difference_update(non_preferred)

    return all_configs


def _do_skips(cls):
    reasons = []
    all_configs = _possible_configs_for_cls(cls, reasons)

    if getattr(cls, '__skip_if__', False):
        for c in getattr(cls, '__skip_if__'):
            if c():
                config.skip_test("'%s' skipped by %s" % (
                    cls.__name__, c.__name__)
                )

    if not all_configs:
        if getattr(cls, '__backend__', False):
            msg = "'%s' unsupported for implementation '%s'" % (
                cls.__name__, cls.__only_on__)
        else:
            msg = "'%s' unsupported on any DB implementation %s%s" % (
                cls.__name__,
                ", ".join(
                    "'%s(%s)+%s'" % (
                        config_obj.db.name,
                        ".".join(
                            str(dig) for dig in
                            config_obj.db.dialect.server_version_info),
                        config_obj.db.driver
                    )
                  for config_obj in config.Config.all_configs()
                ),
                ", ".join(reasons)
            )
        config.skip_test(msg)
    elif hasattr(cls, '__prefer_backends__'):
        non_preferred = set()
        spec = exclusions.db_spec(*util.to_list(cls.__prefer_backends__))
        for config_obj in all_configs:
            if not spec(config_obj):
                non_preferred.add(config_obj)
        if all_configs.difference(non_preferred):
            all_configs.difference_update(non_preferred)

    if config._current not in all_configs:
        _setup_config(all_configs.pop(), cls)


def _setup_config(config_obj, ctx):
    config._current.push(config_obj, testing)
