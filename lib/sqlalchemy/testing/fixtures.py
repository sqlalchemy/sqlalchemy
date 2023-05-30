# testing/fixtures.py
# Copyright (C) 2005-2023 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
# mypy: ignore-errors


from __future__ import annotations

import itertools
import random
import re
import sys
from typing import Any

import sqlalchemy as sa
from . import assertions
from . import config
from . import mock
from . import schema
from .assertions import eq_
from .assertions import ne_
from .entities import BasicEntity
from .entities import ComparableEntity
from .entities import ComparableMixin  # noqa
from .util import adict
from .util import drop_all_tables_from_metadata
from .. import Column
from .. import event
from .. import func
from .. import Integer
from .. import select
from .. import Table
from .. import util
from ..orm import DeclarativeBase
from ..orm import events as orm_events
from ..orm import MappedAsDataclass
from ..orm import registry
from ..schema import sort_tables_and_constraints
from ..sql import visitors
from ..sql.elements import ClauseElement


@config.mark_base_test_class()
class TestBase:
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

    # if True, the testing reaper will not attempt to touch connection
    # state after a test is completed and before the outer teardown
    # starts
    __leave_connections_for_teardown__ = False

    def assert_(self, val, msg=None):
        assert val, msg

    @config.fixture()
    def nocache(self):
        _cache = config.db._compiled_cache
        config.db._compiled_cache = None
        yield
        config.db._compiled_cache = _cache

    @config.fixture()
    def connection_no_trans(self):
        eng = getattr(self, "bind", None) or config.db

        with eng.connect() as conn:
            yield conn

    @config.fixture()
    def connection(self):
        global _connection_fixture_connection

        eng = getattr(self, "bind", None) or config.db

        conn = eng.connect()
        trans = conn.begin()

        _connection_fixture_connection = conn
        yield conn

        _connection_fixture_connection = None

        if trans.is_active:
            trans.rollback()
        # trans would not be active here if the test is using
        # the legacy @provide_metadata decorator still, as it will
        # run a close all connections.
        conn.close()

    @config.fixture()
    def close_result_when_finished(self):
        to_close = []
        to_consume = []

        def go(result, consume=False):
            to_close.append(result)
            if consume:
                to_consume.append(result)

        yield go
        for r in to_consume:
            try:
                r.all()
            except:
                pass
        for r in to_close:
            try:
                r.close()
            except:
                pass

    @config.fixture()
    def registry(self, metadata):
        reg = registry(
            metadata=metadata,
            type_annotation_map={
                str: sa.String().with_variant(
                    sa.String(50), "mysql", "mariadb", "oracle"
                )
            },
        )
        yield reg
        reg.dispose()

    @config.fixture
    def decl_base(self, metadata):
        _md = metadata

        class Base(DeclarativeBase):
            metadata = _md
            type_annotation_map = {
                str: sa.String().with_variant(
                    sa.String(50), "mysql", "mariadb", "oracle"
                )
            }

        yield Base
        Base.registry.dispose()

    @config.fixture
    def dc_decl_base(self, metadata):
        _md = metadata

        class Base(MappedAsDataclass, DeclarativeBase):
            metadata = _md
            type_annotation_map = {
                str: sa.String().with_variant(
                    sa.String(50), "mysql", "mariadb"
                )
            }

        yield Base
        Base.registry.dispose()

    @config.fixture()
    def future_connection(self, future_engine, connection):
        # integrate the future_engine and connection fixtures so
        # that users of the "connection" fixture will get at the
        # "future" connection
        yield connection

    @config.fixture()
    def future_engine(self):
        yield

    @config.fixture()
    def testing_engine(self):
        from . import engines

        def gen_testing_engine(
            url=None,
            options=None,
            future=None,
            asyncio=False,
            transfer_staticpool=False,
            share_pool=False,
        ):
            if options is None:
                options = {}
            options["scope"] = "fixture"
            return engines.testing_engine(
                url=url,
                options=options,
                asyncio=asyncio,
                transfer_staticpool=transfer_staticpool,
                share_pool=share_pool,
            )

        yield gen_testing_engine

        engines.testing_reaper._drop_testing_engines("fixture")

    @config.fixture()
    def async_testing_engine(self, testing_engine):
        def go(**kw):
            kw["asyncio"] = True
            return testing_engine(**kw)

        return go

    @config.fixture
    def fixture_session(self):
        return fixture_session()

    @config.fixture()
    def metadata(self, request):
        """Provide bound MetaData for a single test, dropping afterwards."""

        from ..sql import schema

        metadata = schema.MetaData()
        request.instance.metadata = metadata
        yield metadata
        del request.instance.metadata

        if (
            _connection_fixture_connection
            and _connection_fixture_connection.in_transaction()
        ):
            trans = _connection_fixture_connection.get_transaction()
            trans.rollback()
            with _connection_fixture_connection.begin():
                drop_all_tables_from_metadata(
                    metadata, _connection_fixture_connection
                )
        else:
            drop_all_tables_from_metadata(metadata, config.db)

    @config.fixture(
        params=[
            (rollback, second_operation, begin_nested)
            for rollback in (True, False)
            for second_operation in ("none", "execute", "begin")
            for begin_nested in (
                True,
                False,
            )
        ]
    )
    def trans_ctx_manager_fixture(self, request, metadata):
        rollback, second_operation, begin_nested = request.param

        t = Table("test", metadata, Column("data", Integer))
        eng = getattr(self, "bind", None) or config.db

        t.create(eng)

        def run_test(subject, trans_on_subject, execute_on_subject):
            with subject.begin() as trans:
                if begin_nested:
                    if not config.requirements.savepoints.enabled:
                        config.skip_test("savepoints not enabled")
                    if execute_on_subject:
                        nested_trans = subject.begin_nested()
                    else:
                        nested_trans = trans.begin_nested()

                    with nested_trans:
                        if execute_on_subject:
                            subject.execute(t.insert(), {"data": 10})
                        else:
                            trans.execute(t.insert(), {"data": 10})

                        # for nested trans, we always commit/rollback on the
                        # "nested trans" object itself.
                        # only Session(future=False) will affect savepoint
                        # transaction for session.commit/rollback

                        if rollback:
                            nested_trans.rollback()
                        else:
                            nested_trans.commit()

                        if second_operation != "none":
                            with assertions.expect_raises_message(
                                sa.exc.InvalidRequestError,
                                "Can't operate on closed transaction "
                                "inside context "
                                "manager.  Please complete the context "
                                "manager "
                                "before emitting further commands.",
                            ):
                                if second_operation == "execute":
                                    if execute_on_subject:
                                        subject.execute(
                                            t.insert(), {"data": 12}
                                        )
                                    else:
                                        trans.execute(t.insert(), {"data": 12})
                                elif second_operation == "begin":
                                    if execute_on_subject:
                                        subject.begin_nested()
                                    else:
                                        trans.begin_nested()

                    # outside the nested trans block, but still inside the
                    # transaction block, we can run SQL, and it will be
                    # committed
                    if execute_on_subject:
                        subject.execute(t.insert(), {"data": 14})
                    else:
                        trans.execute(t.insert(), {"data": 14})

                else:
                    if execute_on_subject:
                        subject.execute(t.insert(), {"data": 10})
                    else:
                        trans.execute(t.insert(), {"data": 10})

                    if trans_on_subject:
                        if rollback:
                            subject.rollback()
                        else:
                            subject.commit()
                    else:
                        if rollback:
                            trans.rollback()
                        else:
                            trans.commit()

                    if second_operation != "none":
                        with assertions.expect_raises_message(
                            sa.exc.InvalidRequestError,
                            "Can't operate on closed transaction inside "
                            "context "
                            "manager.  Please complete the context manager "
                            "before emitting further commands.",
                        ):
                            if second_operation == "execute":
                                if execute_on_subject:
                                    subject.execute(t.insert(), {"data": 12})
                                else:
                                    trans.execute(t.insert(), {"data": 12})
                            elif second_operation == "begin":
                                if hasattr(trans, "begin"):
                                    trans.begin()
                                else:
                                    subject.begin()
                            elif second_operation == "begin_nested":
                                if execute_on_subject:
                                    subject.begin_nested()
                                else:
                                    trans.begin_nested()

            expected_committed = 0
            if begin_nested:
                # begin_nested variant, we inserted a row after the nested
                # block
                expected_committed += 1
            if not rollback:
                # not rollback variant, our row inserted in the target
                # block itself would be committed
                expected_committed += 1

            if execute_on_subject:
                eq_(
                    subject.scalar(select(func.count()).select_from(t)),
                    expected_committed,
                )
            else:
                with subject.connect() as conn:
                    eq_(
                        conn.scalar(select(func.count()).select_from(t)),
                        expected_committed,
                    )

        return run_test


_connection_fixture_connection = None


class FutureEngineMixin:
    """alembic's suite still using this"""


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
    _tables_metadata = None
    tables = None
    other = None
    sequences = None

    @config.fixture(autouse=True, scope="class")
    def _setup_tables_test_class(self):
        cls = self.__class__
        cls._init_class()

        cls._setup_once_tables()

        cls._setup_once_inserts()

        yield

        cls._teardown_once_metadata_bind()

    @config.fixture(autouse=True, scope="function")
    def _setup_tables_test_instance(self):
        self._setup_each_tables()
        self._setup_each_inserts()

        yield

        self._teardown_each_tables()

    @property
    def tables_test_metadata(self):
        return self._tables_metadata

    @classmethod
    def _init_class(cls):
        if cls.run_define_tables == "each":
            if cls.run_create_tables == "once":
                cls.run_create_tables = "each"
            assert cls.run_inserts in ("each", None)

        cls.other = adict()
        cls.tables = adict()
        cls.sequences = adict()

        cls.bind = cls.setup_bind()
        cls._tables_metadata = sa.MetaData()

    @classmethod
    def _setup_once_inserts(cls):
        if cls.run_inserts == "once":
            cls._load_fixtures()
            with cls.bind.begin() as conn:
                cls.insert_data(conn)

    @classmethod
    def _setup_once_tables(cls):
        if cls.run_define_tables == "once":
            cls.define_tables(cls._tables_metadata)
            if cls.run_create_tables == "once":
                cls._tables_metadata.create_all(cls.bind)
            cls.tables.update(cls._tables_metadata.tables)
            cls.sequences.update(cls._tables_metadata._sequences)

    def _setup_each_tables(self):
        if self.run_define_tables == "each":
            self.define_tables(self._tables_metadata)
            if self.run_create_tables == "each":
                self._tables_metadata.create_all(self.bind)
            self.tables.update(self._tables_metadata.tables)
            self.sequences.update(self._tables_metadata._sequences)
        elif self.run_create_tables == "each":
            self._tables_metadata.create_all(self.bind)

    def _setup_each_inserts(self):
        if self.run_inserts == "each":
            self._load_fixtures()
            with self.bind.begin() as conn:
                self.insert_data(conn)

    def _teardown_each_tables(self):
        if self.run_define_tables == "each":
            self.tables.clear()
            if self.run_create_tables == "each":
                drop_all_tables_from_metadata(self._tables_metadata, self.bind)
            self._tables_metadata.clear()
        elif self.run_create_tables == "each":
            drop_all_tables_from_metadata(self._tables_metadata, self.bind)

        savepoints = getattr(config.requirements, "savepoints", False)
        if savepoints:
            savepoints = savepoints.enabled

        # no need to run deletes if tables are recreated on setup
        if (
            self.run_define_tables != "each"
            and self.run_create_tables != "each"
            and self.run_deletes == "each"
        ):
            with self.bind.begin() as conn:
                for table in reversed(
                    [
                        t
                        for (t, fks) in sort_tables_and_constraints(
                            self._tables_metadata.tables.values()
                        )
                        if t is not None
                    ]
                ):
                    try:
                        if savepoints:
                            with conn.begin_nested():
                                conn.execute(table.delete())
                        else:
                            conn.execute(table.delete())
                    except sa.exc.DBAPIError as ex:
                        print(
                            ("Error emptying table %s: %r" % (table, ex)),
                            file=sys.stderr,
                        )

    @classmethod
    def _teardown_once_metadata_bind(cls):
        if cls.run_create_tables:
            drop_all_tables_from_metadata(cls._tables_metadata, cls.bind)

        if cls.run_dispose_bind == "once":
            cls.dispose_bind(cls.bind)

        cls._tables_metadata.bind = None

        if cls.run_setup_bind is not None:
            cls.bind = None

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
    def insert_data(cls, connection):
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
            if isinstance(table, str):
                table = cls.tables[table]
            headers[table] = data[0]
            rows[table] = data[1:]
        for table, fks in sort_tables_and_constraints(
            cls._tables_metadata.tables.values()
        ):
            if table is None:
                continue
            if table not in headers:
                continue
            with cls.bind.begin() as conn:
                conn.execute(
                    table.insert(),
                    [
                        dict(zip(headers[table], column_values))
                        for column_values in rows[table]
                    ],
                )


class NoCache:
    @config.fixture(autouse=True, scope="function")
    def _disable_cache(self):
        _cache = config.db._compiled_cache
        config.db._compiled_cache = None
        yield
        config.db._compiled_cache = _cache


class RemovesEvents:
    @util.memoized_property
    def _event_fns(self):
        return set()

    def event_listen(self, target, name, fn, **kw):
        self._event_fns.add((target, name, fn))
        event.listen(target, name, fn, **kw)

    @config.fixture(autouse=True, scope="function")
    def _remove_events(self):
        yield
        for key in self._event_fns:
            event.remove(*key)


class RemoveORMEventsGlobally:
    @config.fixture(autouse=True)
    def _remove_listeners(self):
        yield
        orm_events.MapperEvents._clear()
        orm_events.InstanceEvents._clear()
        orm_events.SessionEvents._clear()
        orm_events.InstrumentationEvents._clear()
        orm_events.QueryEvents._clear()


_fixture_sessions = set()


def fixture_session(**kw):
    kw.setdefault("autoflush", True)
    kw.setdefault("expire_on_commit", True)

    bind = kw.pop("bind", config.db)

    sess = sa.orm.Session(bind, **kw)
    _fixture_sessions.add(sess)
    return sess


def _close_all_sessions():
    # will close all still-referenced sessions
    sa.orm.session.close_all_sessions()
    _fixture_sessions.clear()


def stop_test_class_inside_fixtures(cls):
    _close_all_sessions()
    sa.orm.clear_mappers()


def after_test():
    if _fixture_sessions:
        _close_all_sessions()


class ORMTest(TestBase):
    pass


class MappedTest(TablesTest, assertions.AssertsExecutionResults):
    # 'once', 'each', None
    run_setup_classes = "once"

    # 'once', 'each', None
    run_setup_mappers = "each"

    classes: Any = None

    @config.fixture(autouse=True, scope="class")
    def _setup_tables_test_class(self):
        cls = self.__class__
        cls._init_class()

        if cls.classes is None:
            cls.classes = adict()

        cls._setup_once_tables()
        cls._setup_once_classes()
        cls._setup_once_mappers()
        cls._setup_once_inserts()

        yield

        cls._teardown_once_class()
        cls._teardown_once_metadata_bind()

    @config.fixture(autouse=True, scope="function")
    def _setup_tables_test_instance(self):
        self._setup_each_tables()
        self._setup_each_classes()
        self._setup_each_mappers()
        self._setup_each_inserts()

        yield

        sa.orm.session.close_all_sessions()
        self._teardown_each_mappers()
        self._teardown_each_classes()
        self._teardown_each_tables()

    @classmethod
    def _teardown_once_class(cls):
        cls.classes.clear()

    @classmethod
    def _setup_once_classes(cls):
        if cls.run_setup_classes == "once":
            cls._with_register_classes(cls.setup_classes)

    @classmethod
    def _setup_once_mappers(cls):
        if cls.run_setup_mappers == "once":
            cls.mapper_registry, cls.mapper = cls._generate_registry()
            cls._with_register_classes(cls.setup_mappers)

    def _setup_each_mappers(self):
        if self.run_setup_mappers != "once":
            (
                self.__class__.mapper_registry,
                self.__class__.mapper,
            ) = self._generate_registry()

        if self.run_setup_mappers == "each":
            self._with_register_classes(self.setup_mappers)

    def _setup_each_classes(self):
        if self.run_setup_classes == "each":
            self._with_register_classes(self.setup_classes)

    @classmethod
    def _generate_registry(cls):
        decl = registry(metadata=cls._tables_metadata)
        return decl, decl.map_imperatively

    @classmethod
    def _with_register_classes(cls, fn):
        """Run a setup method, framing the operation with a Base class
        that will catch new subclasses to be established within
        the "classes" registry.

        """
        cls_registry = cls.classes

        class _Base:
            def __init_subclass__(cls) -> None:
                assert cls_registry is not None
                cls_registry[cls.__name__] = cls
                super().__init_subclass__()

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

        class _DeclBase(DeclarativeBase):
            __table_cls__ = schema.Table
            metadata = cls._tables_metadata
            type_annotation_map = {
                str: sa.String().with_variant(
                    sa.String(50), "mysql", "mariadb", "oracle"
                )
            }

            def __init_subclass__(cls, **kw) -> None:
                assert cls_registry is not None
                cls_registry[cls.__name__] = cls
                super().__init_subclass__(**kw)

        cls.DeclarativeBasic = _DeclBase

        # sets up cls.Basic which is helpful for things like composite
        # classes
        super()._with_register_classes(fn)

        if cls._tables_metadata.tables and cls.run_create_tables:
            cls._tables_metadata.create_all(config.db)


class ComputedReflectionFixtureTest(TablesTest):
    run_inserts = run_deletes = None

    __backend__ = True
    __requires__ = ("computed_columns", "table_reflection")

    regexp = re.compile(r"[\[\]\(\)\s`'\"]*")

    def normalize(self, text):
        return self.regexp.sub("", text).lower()

    @classmethod
    def define_tables(cls, metadata):
        from .. import Integer
        from .. import testing
        from ..schema import Column
        from ..schema import Computed
        from ..schema import Table

        Table(
            "computed_default_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("normal", Integer),
            Column("computed_col", Integer, Computed("normal + 42")),
            Column("with_default", Integer, server_default="42"),
        )

        t = Table(
            "computed_column_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("normal", Integer),
            Column("computed_no_flag", Integer, Computed("normal + 42")),
        )

        if testing.requires.schemas.enabled:
            t2 = Table(
                "computed_column_table",
                metadata,
                Column("id", Integer, primary_key=True),
                Column("normal", Integer),
                Column("computed_no_flag", Integer, Computed("normal / 42")),
                schema=config.test_schema,
            )

        if testing.requires.computed_columns_virtual.enabled:
            t.append_column(
                Column(
                    "computed_virtual",
                    Integer,
                    Computed("normal + 2", persisted=False),
                )
            )
            if testing.requires.schemas.enabled:
                t2.append_column(
                    Column(
                        "computed_virtual",
                        Integer,
                        Computed("normal / 2", persisted=False),
                    )
                )
        if testing.requires.computed_columns_stored.enabled:
            t.append_column(
                Column(
                    "computed_stored",
                    Integer,
                    Computed("normal - 42", persisted=True),
                )
            )
            if testing.requires.schemas.enabled:
                t2.append_column(
                    Column(
                        "computed_stored",
                        Integer,
                        Computed("normal * 42", persisted=True),
                    )
                )


class CacheKeyFixture:
    def _compare_equal(self, a, b, compare_values):
        a_key = a._generate_cache_key()
        b_key = b._generate_cache_key()

        if a_key is None:
            assert a._annotations.get("nocache")

            assert b_key is None
        else:
            eq_(a_key.key, b_key.key)
            eq_(hash(a_key.key), hash(b_key.key))

            for a_param, b_param in zip(a_key.bindparams, b_key.bindparams):
                assert a_param.compare(b_param, compare_values=compare_values)
        return a_key, b_key

    def _run_cache_key_fixture(self, fixture, compare_values):
        case_a = fixture()
        case_b = fixture()

        for a, b in itertools.combinations_with_replacement(
            range(len(case_a)), 2
        ):
            if a == b:
                a_key, b_key = self._compare_equal(
                    case_a[a], case_b[b], compare_values
                )
                if a_key is None:
                    continue
            else:
                a_key = case_a[a]._generate_cache_key()
                b_key = case_b[b]._generate_cache_key()

                if a_key is None or b_key is None:
                    if a_key is None:
                        assert case_a[a]._annotations.get("nocache")
                    if b_key is None:
                        assert case_b[b]._annotations.get("nocache")
                    continue

                if a_key.key == b_key.key:
                    for a_param, b_param in zip(
                        a_key.bindparams, b_key.bindparams
                    ):
                        if not a_param.compare(
                            b_param, compare_values=compare_values
                        ):
                            break
                    else:
                        # this fails unconditionally since we could not
                        # find bound parameter values that differed.
                        # Usually we intended to get two distinct keys here
                        # so the failure will be more descriptive using the
                        # ne_() assertion.
                        ne_(a_key.key, b_key.key)
                else:
                    ne_(a_key.key, b_key.key)

            # ClauseElement-specific test to ensure the cache key
            # collected all the bound parameters that aren't marked
            # as "literal execute"
            if isinstance(case_a[a], ClauseElement) and isinstance(
                case_b[b], ClauseElement
            ):
                assert_a_params = []
                assert_b_params = []

                for elem in visitors.iterate(case_a[a]):
                    if elem.__visit_name__ == "bindparam":
                        assert_a_params.append(elem)

                for elem in visitors.iterate(case_b[b]):
                    if elem.__visit_name__ == "bindparam":
                        assert_b_params.append(elem)

                # note we're asserting the order of the params as well as
                # if there are dupes or not.  ordering has to be
                # deterministic and matches what a traversal would provide.
                eq_(
                    sorted(a_key.bindparams, key=lambda b: b.key),
                    sorted(
                        util.unique_list(assert_a_params), key=lambda b: b.key
                    ),
                )
                eq_(
                    sorted(b_key.bindparams, key=lambda b: b.key),
                    sorted(
                        util.unique_list(assert_b_params), key=lambda b: b.key
                    ),
                )

    def _run_cache_key_equal_fixture(self, fixture, compare_values):
        case_a = fixture()
        case_b = fixture()

        for a, b in itertools.combinations_with_replacement(
            range(len(case_a)), 2
        ):
            self._compare_equal(case_a[a], case_b[b], compare_values)


def insertmanyvalues_fixture(
    connection, randomize_rows=False, warn_on_downgraded=False
):
    dialect = connection.dialect
    orig_dialect = dialect._deliver_insertmanyvalues_batches
    orig_conn = connection._exec_insertmany_context

    class RandomCursor:
        __slots__ = ("cursor",)

        def __init__(self, cursor):
            self.cursor = cursor

        # only this method is called by the deliver method.
        # by not having the other methods we assert that those aren't being
        # used

        def fetchall(self):
            rows = self.cursor.fetchall()
            rows = list(rows)
            random.shuffle(rows)
            return rows

    def _deliver_insertmanyvalues_batches(
        cursor, statement, parameters, generic_setinputsizes, context
    ):
        if randomize_rows:
            cursor = RandomCursor(cursor)
        for batch in orig_dialect(
            cursor, statement, parameters, generic_setinputsizes, context
        ):
            if warn_on_downgraded and batch.is_downgraded:
                util.warn("Batches were downgraded for sorted INSERT")

            yield batch

    def _exec_insertmany_context(
        dialect,
        context,
    ):
        with mock.patch.object(
            dialect,
            "_deliver_insertmanyvalues_batches",
            new=_deliver_insertmanyvalues_batches,
        ):
            return orig_conn(dialect, context)

    connection._exec_insertmany_context = _exec_insertmany_context
