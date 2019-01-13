# testing/fixtures.py
# Copyright (C) 2005-2019 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import sys

import sqlalchemy as sa
from . import assertions
from . import config
from . import schema
from .engines import drop_all_tables
from .entities import BasicEntity
from .entities import ComparableEntity
from .util import adict
from .. import event
from .. import util
from ..ext.declarative import declarative_base
from ..ext.declarative import DeclarativeMeta


# whether or not we use unittest changes things dramatically,
# as far as how py.test collection works.


class TestBase(object):
    # A sequence of database names to always run, regardless of the
    # constraints below.
    __whitelist__ = ()

    # A sequence of requirement names matching testing.requires decorators
    __requires__ = ()

    # A sequence of dialect names to exclude from the test class.
    __unsupported_on__ = ()

    # If present, test class is only runnable for the *single* specified
    # dialect.  If you need multiple, use __unsupported_on__ and invert.
    __only_on__ = None

    # A sequence of no-arg callables. If any are True, the entire testcase is
    # skipped.
    __skip_if__ = None

    def assert_(self, val, msg=None):
        assert val, msg

    # apparently a handful of tests are doing this....OK
    def setup(self):
        if hasattr(self, "setUp"):
            self.setUp()

    def teardown(self):
        if hasattr(self, "tearDown"):
            self.tearDown()


class TablesTest(TestBase):

    # 'once', None
    run_setup_bind = "once"

    # 'once', 'each', None
    run_define_tables = "once"

    # 'once', 'each', None
    run_create_tables = "once"

    # 'once', 'each', None
    run_inserts = "each"

    # 'each', None
    run_deletes = "each"

    # 'once', None
    run_dispose_bind = None

    bind = None
    metadata = None
    tables = None
    other = None

    @classmethod
    def setup_class(cls):
        cls._init_class()

        cls._setup_once_tables()

        cls._setup_once_inserts()

    @classmethod
    def _init_class(cls):
        if cls.run_define_tables == "each":
            if cls.run_create_tables == "once":
                cls.run_create_tables = "each"
            assert cls.run_inserts in ("each", None)

        cls.other = adict()
        cls.tables = adict()

        cls.bind = cls.setup_bind()
        cls.metadata = sa.MetaData()
        cls.metadata.bind = cls.bind

    @classmethod
    def _setup_once_inserts(cls):
        if cls.run_inserts == "once":
            cls._load_fixtures()
            cls.insert_data()

    @classmethod
    def _setup_once_tables(cls):
        if cls.run_define_tables == "once":
            cls.define_tables(cls.metadata)
            if cls.run_create_tables == "once":
                cls.metadata.create_all(cls.bind)
            cls.tables.update(cls.metadata.tables)

    def _setup_each_tables(self):
        if self.run_define_tables == "each":
            self.tables.clear()
            if self.run_create_tables == "each":
                drop_all_tables(self.metadata, self.bind)
            self.metadata.clear()
            self.define_tables(self.metadata)
            if self.run_create_tables == "each":
                self.metadata.create_all(self.bind)
            self.tables.update(self.metadata.tables)
        elif self.run_create_tables == "each":
            drop_all_tables(self.metadata, self.bind)
            self.metadata.create_all(self.bind)

    def _setup_each_inserts(self):
        if self.run_inserts == "each":
            self._load_fixtures()
            self.insert_data()

    def _teardown_each_tables(self):
        # no need to run deletes if tables are recreated on setup
        if self.run_define_tables != "each" and self.run_deletes == "each":
            with self.bind.connect() as conn:
                for table in reversed(self.metadata.sorted_tables):
                    try:
                        conn.execute(table.delete())
                    except sa.exc.DBAPIError as ex:
                        util.print_(
                            ("Error emptying table %s: %r" % (table, ex)),
                            file=sys.stderr,
                        )

    def setup(self):
        self._setup_each_tables()
        self._setup_each_inserts()

    def teardown(self):
        self._teardown_each_tables()

    @classmethod
    def _teardown_once_metadata_bind(cls):
        if cls.run_create_tables:
            drop_all_tables(cls.metadata, cls.bind)

        if cls.run_dispose_bind == "once":
            cls.dispose_bind(cls.bind)

        cls.metadata.bind = None

        if cls.run_setup_bind is not None:
            cls.bind = None

    @classmethod
    def teardown_class(cls):
        cls._teardown_once_metadata_bind()

    @classmethod
    def setup_bind(cls):
        return config.db

    @classmethod
    def dispose_bind(cls, bind):
        if hasattr(bind, "dispose"):
            bind.dispose()
        elif hasattr(bind, "close"):
            bind.close()

    @classmethod
    def define_tables(cls, metadata):
        pass

    @classmethod
    def fixtures(cls):
        return {}

    @classmethod
    def insert_data(cls):
        pass

    def sql_count_(self, count, fn):
        self.assert_sql_count(self.bind, fn, count)

    def sql_eq_(self, callable_, statements):
        self.assert_sql(self.bind, callable_, statements)

    @classmethod
    def _load_fixtures(cls):
        """Insert rows as represented by the fixtures() method."""
        headers, rows = {}, {}
        for table, data in cls.fixtures().items():
            if len(data) < 2:
                continue
            if isinstance(table, util.string_types):
                table = cls.tables[table]
            headers[table] = data[0]
            rows[table] = data[1:]
        for table in cls.metadata.sorted_tables:
            if table not in headers:
                continue
            cls.bind.execute(
                table.insert(),
                [
                    dict(zip(headers[table], column_values))
                    for column_values in rows[table]
                ],
            )


class RemovesEvents(object):
    @util.memoized_property
    def _event_fns(self):
        return set()

    def event_listen(self, target, name, fn, **kw):
        self._event_fns.add((target, name, fn))
        event.listen(target, name, fn, **kw)

    def teardown(self):
        for key in self._event_fns:
            event.remove(*key)
        super_ = super(RemovesEvents, self)
        if hasattr(super_, "teardown"):
            super_.teardown()


class _ORMTest(object):
    @classmethod
    def teardown_class(cls):
        sa.orm.session.close_all_sessions()
        sa.orm.clear_mappers()


class ORMTest(_ORMTest, TestBase):
    pass


class MappedTest(_ORMTest, TablesTest, assertions.AssertsExecutionResults):
    # 'once', 'each', None
    run_setup_classes = "once"

    # 'once', 'each', None
    run_setup_mappers = "each"

    classes = None

    @classmethod
    def setup_class(cls):
        cls._init_class()

        if cls.classes is None:
            cls.classes = adict()

        cls._setup_once_tables()
        cls._setup_once_classes()
        cls._setup_once_mappers()
        cls._setup_once_inserts()

    @classmethod
    def teardown_class(cls):
        cls._teardown_once_class()
        cls._teardown_once_metadata_bind()

    def setup(self):
        self._setup_each_tables()
        self._setup_each_classes()
        self._setup_each_mappers()
        self._setup_each_inserts()

    def teardown(self):
        sa.orm.session.close_all_sessions()
        self._teardown_each_mappers()
        self._teardown_each_classes()
        self._teardown_each_tables()

    @classmethod
    def _teardown_once_class(cls):
        cls.classes.clear()
        _ORMTest.teardown_class()

    @classmethod
    def _setup_once_classes(cls):
        if cls.run_setup_classes == "once":
            cls._with_register_classes(cls.setup_classes)

    @classmethod
    def _setup_once_mappers(cls):
        if cls.run_setup_mappers == "once":
            cls._with_register_classes(cls.setup_mappers)

    def _setup_each_mappers(self):
        if self.run_setup_mappers == "each":
            self._with_register_classes(self.setup_mappers)

    def _setup_each_classes(self):
        if self.run_setup_classes == "each":
            self._with_register_classes(self.setup_classes)

    @classmethod
    def _with_register_classes(cls, fn):
        """Run a setup method, framing the operation with a Base class
        that will catch new subclasses to be established within
        the "classes" registry.

        """
        cls_registry = cls.classes

        class FindFixture(type):
            def __init__(cls, classname, bases, dict_):
                cls_registry[classname] = cls
                return type.__init__(cls, classname, bases, dict_)

        class _Base(util.with_metaclass(FindFixture, object)):
            pass

        class Basic(BasicEntity, _Base):
            pass

        class Comparable(ComparableEntity, _Base):
            pass

        cls.Basic = Basic
        cls.Comparable = Comparable
        fn()

    def _teardown_each_mappers(self):
        # some tests create mappers in the test bodies
        # and will define setup_mappers as None -
        # clear mappers in any case
        if self.run_setup_mappers != "once":
            sa.orm.clear_mappers()

    def _teardown_each_classes(self):
        if self.run_setup_classes != "once":
            self.classes.clear()

    @classmethod
    def setup_classes(cls):
        pass

    @classmethod
    def setup_mappers(cls):
        pass


class DeclarativeMappedTest(MappedTest):
    run_setup_classes = "once"
    run_setup_mappers = "once"

    @classmethod
    def _setup_once_tables(cls):
        pass

    @classmethod
    def _with_register_classes(cls, fn):
        cls_registry = cls.classes

        class FindFixtureDeclarative(DeclarativeMeta):
            def __init__(cls, classname, bases, dict_):
                cls_registry[classname] = cls
                return DeclarativeMeta.__init__(cls, classname, bases, dict_)

        class DeclarativeBasic(object):
            __table_cls__ = schema.Table

        _DeclBase = declarative_base(
            metadata=cls.metadata,
            metaclass=FindFixtureDeclarative,
            cls=DeclarativeBasic,
        )
        cls.DeclarativeBasic = _DeclBase
        fn()

        if cls.metadata.tables and cls.run_create_tables:
            cls.metadata.create_all(config.db)
