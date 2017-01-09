# testing/config.py
# Copyright (C) 2005-2017 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import collections

requirements = None
db = None
db_url = None
db_opts = None
file_config = None
test_schema = None
test_schema_2 = None
_current = None

try:
    from unittest import SkipTest as _skip_test_exception
except ImportError:
    _skip_test_exception = None


class Config(object):
    def __init__(self, db, db_opts, options, file_config):
        self.db = db
        self.db_opts = db_opts
        self.options = options
        self.file_config = file_config
        self.test_schema = "test_schema"
        self.test_schema_2 = "test_schema_2"

    _stack = collections.deque()
    _configs = {}

    @classmethod
    def register(cls, db, db_opts, options, file_config):
        """add a config as one of the global configs.

        If there are no configs set up yet, this config also
        gets set as the "_current".
        """
        cfg = Config(db, db_opts, options, file_config)

        cls._configs[cfg.db.name] = cfg
        cls._configs[(cfg.db.name, cfg.db.dialect)] = cfg
        cls._configs[cfg.db] = cfg
        return cfg

    @classmethod
    def set_as_current(cls, config, namespace):
        global db, _current, db_url, test_schema, test_schema_2, db_opts
        _current = config
        db_url = config.db.url
        db_opts = config.db_opts
        test_schema = config.test_schema
        test_schema_2 = config.test_schema_2
        namespace.db = db = config.db

    @classmethod
    def push_engine(cls, db, namespace):
        assert _current, "Can't push without a default Config set up"
        cls.push(
            Config(
                db, _current.db_opts, _current.options, _current.file_config),
            namespace
        )

    @classmethod
    def push(cls, config, namespace):
        cls._stack.append(_current)
        cls.set_as_current(config, namespace)

    @classmethod
    def reset(cls, namespace):
        if cls._stack:
            cls.set_as_current(cls._stack[0], namespace)
            cls._stack.clear()

    @classmethod
    def all_configs(cls):
        for cfg in set(cls._configs.values()):
            yield cfg

    @classmethod
    def all_dbs(cls):
        for cfg in cls.all_configs():
            yield cfg.db

    def skip_test(self, msg):
        skip_test(msg)


def skip_test(msg):
    raise _skip_test_exception(msg)

