import decimal
import gc
import itertools
import multiprocessing
import pickle
import weakref

import sqlalchemy as sa
from sqlalchemy import and_
from sqlalchemy import ClauseElement
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import types
from sqlalchemy import Unicode
from sqlalchemy import util
from sqlalchemy.dialects import mysql
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import registry
from sqlalchemy.dialects import sqlite
from sqlalchemy.engine import result
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.engine.processors import to_decimal_processor_factory
from sqlalchemy.orm import aliased
from sqlalchemy.orm import attributes
from sqlalchemy.orm import clear_mappers
from sqlalchemy.orm import configure_mappers
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import join as orm_join
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import Load
from sqlalchemy.orm import relationship
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import subqueryload
from sqlalchemy.orm.session import _sessions
from sqlalchemy.sql import column
from sqlalchemy.sql import util as sql_util
from sqlalchemy.sql.base import DialectKWArgs
from sqlalchemy.sql.util import visit_binary_product
from sqlalchemy.sql.visitors import cloned_traverse
from sqlalchemy.sql.visitors import replacement_traverse
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import pickleable
from sqlalchemy.testing.entities import ComparableEntity
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from sqlalchemy.testing.util import gc_collect
from ..orm import _fixtures


class A(ComparableEntity):
    pass


class B(ComparableEntity):
    pass


class ASub(A):
    pass


def assert_cycles(expected=0):
    def decorate(fn):
        def go():
            fn()  # warmup, configure mappers, caches, etc.

            gc_collect()
            gc_collect()
            gc_collect()  # multiple calls seem to matter

            # gc.set_debug(gc.DEBUG_COLLECTABLE)
            try:
                return fn()  # run for real
            finally:
                unreachable = gc_collect()
                assert unreachable <= expected
                gc_collect()

        return go

    return decorate


def profile_memory(
    maxtimes=250, assert_no_sessions=True, get_num_objects=None
):
    def decorate(func):
        # run the test N times.  if length of gc.get_objects()
        # keeps growing, assert false

        def get_objects_skipping_sqlite_issue():
            # pysqlite keeps adding weakref objects which only
            # get reset after 220 iterations.  We'd like to keep these
            # tests under 50 iterations and ideally about ten, so
            # just filter them out so that we get a "flatline" more quickly.

            if testing.against("sqlite+pysqlite"):
                return [
                    o
                    for o in gc.get_objects()
                    if not isinstance(o, weakref.ref)
                ]
            else:
                return gc.get_objects()

        def profile(queue, func_args):
            # give testing.db a brand new pool and don't
            # touch the existing pool, since closing a socket
            # in the subprocess can affect the parent
            testing.db.pool = testing.db.pool.recreate()

            gc_collect()
            samples = []
            max_ = 0
            max_grew_for = 0
            success = False
            until_maxtimes = 0
            try:
                while True:
                    if until_maxtimes >= maxtimes // 5:
                        break
                    for x in range(5):
                        try:
                            func(*func_args)
                        except Exception as err:
                            queue.put(
                                (
                                    "result",
                                    False,
                                    "Test raised an exception: %r" % err,
                                )
                            )

                            raise

                        gc_collect()

                        samples.append(
                            get_num_objects()
                            if get_num_objects is not None
                            else len(get_objects_skipping_sqlite_issue())
                        )

                    if assert_no_sessions:
                        assert len(_sessions) == 0, "%d sessions remain" % (
                            len(_sessions),
                        )

                    # queue.put(('samples', samples))

                    latest_max = max(samples[-5:])
                    if latest_max > max_:
                        queue.put(
                            (
                                "status",
                                "Max grew from %s to %s, max has "
                                "grown for %s samples"
                                % (max_, latest_max, max_grew_for),
                            )
                        )
                        max_ = latest_max
                        max_grew_for += 1
                        until_maxtimes += 1
                        continue
                    else:
                        queue.put(
                            (
                                "status",
                                "Max remained at %s, %s more attempts left"
                                % (max_, max_grew_for),
                            )
                        )
                        max_grew_for -= 1
                        if max_grew_for == 0:
                            success = True
                            break
            except Exception as err:
                queue.put(("result", False, "got exception: %s" % err))
            else:
                if not success:
                    queue.put(
                        (
                            "result",
                            False,
                            "Ran for a total of %d times, memory kept "
                            "growing: %r" % (maxtimes, samples),
                        )
                    )

                else:
                    queue.put(("result", True, "success"))

        def run_plain(*func_args):
            import queue as _queue

            q = _queue.Queue()
            profile(q, func_args)

            while True:
                row = q.get()
                typ = row[0]
                if typ == "samples":
                    print("sample gc sizes:", row[1])
                elif typ == "status":
                    print(row[1])
                elif typ == "result":
                    break
                else:
                    assert False, "can't parse row"
            assert row[1], row[2]

        # return run_plain

        def run_in_process(*func_args):
            # see
            # https://docs.python.org/3.14/whatsnew/3.14.html
            # #incompatible-changes - the default run type is no longer
            # "fork", but since we are running closures in the process
            # we need forked mode
            ctx = multiprocessing.get_context("fork")
            queue = ctx.Queue()
            proc = ctx.Process(target=profile, args=(queue, func_args))
            proc.start()
            while True:
                row = queue.get()
                typ = row[0]
                if typ == "samples":
                    print("sample gc sizes:", row[1])
                elif typ == "status":
                    print(row[1])
                elif typ == "result":
                    break
                else:
                    assert False, "can't parse row"
            proc.join()
            assert row[1], row[2]

        return run_in_process

    return decorate


def assert_no_mappers():
    clear_mappers()
    gc_collect()


class EnsureZeroed(fixtures.ORMTest):
    def setup_test(self):
        _sessions.clear()
        clear_mappers()

        # enable query caching, however make the cache small so that
        # the tests don't take too long.  issues w/ caching include making
        # sure sessions don't get stuck inside of it.  However it will
        # make tests like test_mapper_reset take a long time because mappers
        # are very much a part of what's in the cache.
        self.engine = engines.testing_engine(
            options={"use_reaper": False, "query_cache_size": 10}
        )


@testing.add_to_marker.memory_intensive
class MemUsageTest(EnsureZeroed):
    __requires__ = ("cpython", "no_windows")

    def test_type_compile(self):
        from sqlalchemy.dialects.sqlite.base import dialect as SQLiteDialect

        cast = sa.cast(column("x"), sa.Integer)

        @profile_memory()
        def go():
            dialect = SQLiteDialect()
            cast.compile(dialect=dialect)

        go()

    @testing.requires.cextensions
    def test_DecimalResultProcessor_init(self):
        @profile_memory()
        def go():
            to_decimal_processor_factory(dict, 10)

        go()

    @testing.requires.cextensions
    def test_DecimalResultProcessor_process(self):
        @profile_memory()
        def go():
            to_decimal_processor_factory(decimal.Decimal, 10)(1.2)

        go()

    @testing.requires.cextensions
    def test_cycles_in_row(self):
        tup = result.result_tuple(["a", "b", "c"])

        @profile_memory()
        def go():
            obj = {"foo": {}}
            obj["foo"]["bar"] = obj

            row = tup([1, 2, obj])

            obj["foo"]["row"] = row

            del row

        go()

    def test_ad_hoc_types(self):
        """test storage of bind processors, result processors
        in dialect-wide registry."""

        eng = engines.testing_engine()
        for args in (
            (types.Integer, {}),
            (types.String, {}),
            (types.PickleType, {}),
            (types.Enum, "a", "b", "c", {}),
            (sqlite.DATETIME, {}),
            (postgresql.ENUM, "a", "b", "c", {"name": "pgenum"}),
            (types.Interval, {}),
            (postgresql.INTERVAL, {}),
            (mysql.VARCHAR, {}),
        ):

            @profile_memory()
            def go():
                kwargs = args[-1]
                posargs = args[1:-1]
                type_ = args[0](*posargs, **kwargs)
                bp = type_._cached_bind_processor(eng.dialect)
                rp = type_._cached_result_processor(eng.dialect, 0)
                bp, rp  # strong reference

            go()

        assert not eng.dialect._type_memos

    @testing.fails()
    def test_fixture_failure(self):
        class Foo:
            pass

        stuff = []

        @profile_memory(maxtimes=20)
        def go():
            stuff.extend(Foo() for i in range(100))

        go()

    def test_clone_expression(self):
        # this test is for the memory issue "fixed" in #7823, where clones
        # no longer carry along all past elements.
        # However, due to #7903, we can't at the moment use a
        # BindParameter here - these have to continue to carry along all
        # the previous clones for now.  So the test here only works with
        # expressions that dont have BindParameter objects in them.

        root_expr = column("x", Integer) == column("y", Integer)

        expr = root_expr

        @profile_memory()
        def go():
            nonlocal expr

            expr = cloned_traverse(expr, {}, {})

        go()

    def test_tv_render_derived(self):
        root_expr = func.some_fn().table_valued()
        expr = root_expr

        @profile_memory()
        def go():
            nonlocal expr

            expr = expr.render_derived()

        go()


@testing.add_to_marker.memory_intensive
class MemUsageWBackendTest(fixtures.MappedTest, EnsureZeroed):
    __requires__ = "cpython", "posix", "memory_process_intensive", "no_asyncio"
    __sparse_backend__ = True

    # ensure a pure growing test trips the assertion
    @testing.fails_if(lambda: True)
    def test_fixture(self):
        class Foo:
            pass

        x = []

        @profile_memory(maxtimes=10)
        def go():
            x[-1:] = [Foo(), Foo(), Foo(), Foo(), Foo(), Foo()]

        go()

    def test_session(self):
        metadata = MetaData()

        table1 = Table(
            "mytable",
            metadata,
            Column(
                "col1",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("col2", String(30)),
        )

        table2 = Table(
            "mytable2",
            metadata,
            Column(
                "col1",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("col2", String(30)),
            Column("col3", Integer, ForeignKey("mytable.col1")),
        )

        metadata.create_all(self.engine)

        m1 = self.mapper_registry.map_imperatively(
            A,
            table1,
            properties={
                "bs": relationship(
                    B, cascade="all, delete", order_by=table2.c.col1
                )
            },
        )
        m2 = self.mapper_registry.map_imperatively(B, table2)

        @profile_memory()
        def go():
            with Session(self.engine) as sess:
                a1 = A(col2="a1")
                a2 = A(col2="a2")
                a3 = A(col2="a3")
                a1.bs.append(B(col2="b1"))
                a1.bs.append(B(col2="b2"))
                a3.bs.append(B(col2="b3"))
                for x in [a1, a2, a3]:
                    sess.add(x)
                sess.commit()

                alist = sess.query(A).order_by(A.col1).all()
                eq_(
                    [
                        A(col2="a1", bs=[B(col2="b1"), B(col2="b2")]),
                        A(col2="a2", bs=[]),
                        A(col2="a3", bs=[B(col2="b3")]),
                    ],
                    alist,
                )

                for a in alist:
                    sess.delete(a)
                sess.commit()

        go()

        metadata.drop_all(self.engine)
        del m1, m2
        assert_no_mappers()

    def test_sessionmaker(self):
        @profile_memory()
        def go():
            sessmaker = sessionmaker(bind=self.engine)
            sess = sessmaker()
            r = sess.execute(select(1))
            r.close()
            sess.close()
            del sess
            del sessmaker

        go()

    @testing.emits_warning("Compiled statement cache for mapper.*")
    @testing.emits_warning("Compiled statement cache for lazy loader.*")
    @testing.crashes("sqlite", ":memory: connection not suitable here")
    def test_orm_many_engines(self):
        metadata = MetaData()

        table1 = Table(
            "mytable",
            metadata,
            Column(
                "col1",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("col2", String(30)),
        )

        table2 = Table(
            "mytable2",
            metadata,
            Column(
                "col1",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("col2", String(30)),
            Column("col3", Integer, ForeignKey("mytable.col1")),
        )

        metadata.create_all(self.engine)

        m1 = self.mapper_registry.map_imperatively(
            A,
            table1,
            properties={
                "bs": relationship(
                    B, cascade="all, delete", order_by=table2.c.col1
                )
            },
            _compiled_cache_size=50,
        )
        m2 = self.mapper_registry.map_imperatively(
            B, table2, _compiled_cache_size=50
        )

        @profile_memory()
        def go():
            engine = engines.testing_engine(
                options={
                    "logging_name": "FOO",
                    "pool_logging_name": "BAR",
                    "use_reaper": False,
                }
            )
            with Session(engine) as sess:
                a1 = A(col2="a1")
                a2 = A(col2="a2")
                a3 = A(col2="a3")
                a1.bs.append(B(col2="b1"))
                a1.bs.append(B(col2="b2"))
                a3.bs.append(B(col2="b3"))
                for x in [a1, a2, a3]:
                    sess.add(x)
                sess.commit()

                alist = sess.query(A).order_by(A.col1).all()
                eq_(
                    [
                        A(col2="a1", bs=[B(col2="b1"), B(col2="b2")]),
                        A(col2="a2", bs=[]),
                        A(col2="a3", bs=[B(col2="b3")]),
                    ],
                    alist,
                )

                for a in alist:
                    sess.delete(a)
                sess.commit()

            engine.dispose()

        go()

        metadata.drop_all(self.engine)
        del m1, m2
        assert_no_mappers()

    @testing.emits_warning("Compiled statement cache for.*")
    def test_many_updates(self):
        metadata = MetaData()

        wide_table = Table(
            "t",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            *[Column("col%d" % i, Integer) for i in range(10)],
        )

        class Wide:
            pass

        self.mapper_registry.map_imperatively(
            Wide, wide_table, _compiled_cache_size=10
        )

        metadata.create_all(self.engine)
        with Session(self.engine) as session:
            w1 = Wide()
            session.add(w1)
            session.commit()
        del session
        counter = [1]

        @profile_memory()
        def go():
            with Session(self.engine) as session:
                w1 = session.query(Wide).first()
                x = counter[0]
                dec = 10
                while dec > 0:
                    # trying to count in binary here,
                    # works enough to trip the test case
                    if pow(2, dec) < x:
                        setattr(w1, "col%d" % dec, counter[0])
                        x -= pow(2, dec)
                    dec -= 1
                session.commit()
            counter[0] += 1

        try:
            go()
        finally:
            metadata.drop_all(self.engine)

    @testing.requires.savepoints
    def test_savepoints(self):
        metadata = MetaData()

        some_table = Table(
            "t",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
        )

        class SomeClass:
            pass

        self.mapper_registry.map_imperatively(SomeClass, some_table)

        metadata.create_all(self.engine)

        with Session(self.engine) as session:
            target_strings = (
                session.connection().dialect.identifier_preparer._strings
            )

        @profile_memory(
            assert_no_sessions=False,
            get_num_objects=lambda: len(target_strings),
        )
        def go():
            with Session(self.engine) as session, session.begin():
                sc = SomeClass()
                session.add(sc)
                with session.begin_nested():
                    session.query(SomeClass).first()

        try:
            go()
        finally:
            metadata.drop_all(self.engine)

    @testing.crashes("mysql+cymysql", "blocking")
    def test_unicode_warnings(self):
        metadata = MetaData()
        table1 = Table(
            "mytable",
            metadata,
            Column(
                "col1",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("col2", Unicode(30)),
        )
        metadata.create_all(self.engine)
        i = [1]

        # the times here is cranked way up so that we can see
        # pysqlite clearing out its internal buffer and allow
        # the test to pass
        @testing.emits_warning()
        @profile_memory()
        def go():
            # execute with a non-unicode object. a warning is emitted,
            # this warning shouldn't clog up memory.

            with self.engine.connect() as conn:
                conn.execute(
                    table1.select().where(table1.c.col2 == "foo%d" % i[0])
                )
            i[0] += 1

        try:
            go()
        finally:
            metadata.drop_all(self.engine)

    def test_warnings_util(self):
        counter = itertools.count()
        import warnings

        warnings.filterwarnings("ignore", "memusage warning.*")

        @profile_memory()
        def go():
            util.warn_limited(
                "memusage warning, param1: %s, param2: %s",
                (next(counter), next(counter)),
            )

        go()

    def test_mapper_reset(self):
        metadata = MetaData()

        table1 = Table(
            "mytable",
            metadata,
            Column(
                "col1",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("col2", String(30)),
        )

        table2 = Table(
            "mytable2",
            metadata,
            Column(
                "col1",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("col2", String(30)),
            Column("col3", Integer, ForeignKey("mytable.col1")),
        )

        @profile_memory()
        def go():
            self.mapper_registry.map_imperatively(
                A,
                table1,
                properties={"bs": relationship(B, order_by=table2.c.col1)},
            )
            self.mapper_registry.map_imperatively(B, table2)

            sess = Session(self.engine, autoflush=False)
            a1 = A(col2="a1")
            a2 = A(col2="a2")
            a3 = A(col2="a3")
            a1.bs.append(B(col2="b1"))
            a1.bs.append(B(col2="b2"))
            a3.bs.append(B(col2="b3"))
            for x in [a1, a2, a3]:
                sess.add(x)
            sess.flush()
            sess.expunge_all()

            alist = sess.query(A).order_by(A.col1).all()
            eq_(
                [
                    A(col2="a1", bs=[B(col2="b1"), B(col2="b2")]),
                    A(col2="a2", bs=[]),
                    A(col2="a3", bs=[B(col2="b3")]),
                ],
                alist,
            )

            for a in alist:
                sess.delete(a)
            sess.flush()
            sess.close()
            clear_mappers()

        metadata.create_all(self.engine)
        try:
            go()
        finally:
            metadata.drop_all(self.engine)
        assert_no_mappers()

    def test_alias_pathing(self):
        metadata = MetaData()

        a = Table(
            "a",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("bid", Integer, ForeignKey("b.id")),
            Column("type", String(30)),
        )

        asub = Table(
            "asub",
            metadata,
            Column("id", Integer, ForeignKey("a.id"), primary_key=True),
            Column("data", String(30)),
        )

        b = Table(
            "b",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
        )
        self.mapper_registry.map_imperatively(
            A, a, polymorphic_identity="a", polymorphic_on=a.c.type
        )
        self.mapper_registry.map_imperatively(
            ASub, asub, inherits=A, polymorphic_identity="asub"
        )
        self.mapper_registry.map_imperatively(
            B, b, properties={"as_": relationship(A)}
        )

        metadata.create_all(self.engine)
        sess = Session(self.engine)
        a1 = ASub(data="a1")
        a2 = ASub(data="a2")
        a3 = ASub(data="a3")
        b1 = B(as_=[a1, a2, a3])
        sess.add(b1)
        sess.commit()
        del sess

        # sqlite has a slow enough growth here
        # that we have to run it more times to see the
        # "dip" again
        @profile_memory(maxtimes=120)
        def go():
            sess = Session(self.engine)
            sess.query(B).options(subqueryload(B.as_.of_type(ASub))).all()
            sess.close()
            del sess

        try:
            go()
        finally:
            metadata.drop_all(self.engine)
        clear_mappers()

    def test_path_registry(self):
        metadata = MetaData()
        a = Table(
            "a",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("foo", Integer),
            Column("bar", Integer),
        )
        b = Table(
            "b",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("a_id", ForeignKey("a.id")),
        )
        m1 = self.mapper_registry.map_imperatively(
            A, a, properties={"bs": relationship(B)}
        )
        self.mapper_registry.map_imperatively(B, b)

        @profile_memory()
        def go():
            ma = sa.inspect(aliased(A))
            m1._path_registry[m1.attrs.bs][ma][m1.attrs.bar]

        go()
        clear_mappers()

    def test_with_inheritance(self):
        metadata = MetaData()

        table1 = Table(
            "mytable",
            metadata,
            Column(
                "col1",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("col2", String(30)),
        )

        table2 = Table(
            "mytable2",
            metadata,
            Column(
                "col1",
                Integer,
                ForeignKey("mytable.col1"),
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("col3", String(30)),
        )

        @profile_memory()
        def go():
            class A(ComparableEntity):
                pass

            class B(A):
                pass

            clear_mappers()
            self.mapper_registry.map_imperatively(
                A,
                table1,
                polymorphic_on=table1.c.col2,
                polymorphic_identity="a",
            )
            self.mapper_registry.map_imperatively(
                B, table2, inherits=A, polymorphic_identity="b"
            )

            sess = Session(self.engine, autoflush=False)
            a1 = A()
            a2 = A()
            b1 = B(col3="b1")
            b2 = B(col3="b2")
            for x in [a1, a2, b1, b2]:
                sess.add(x)
            sess.flush()
            sess.expunge_all()

            alist = sess.query(A).order_by(A.col1).all()
            eq_([A(), A(), B(col3="b1"), B(col3="b2")], alist)

            for a in alist:
                sess.delete(a)
            sess.flush()

            # don't need to clear_mappers()
            del B
            del A

        metadata.create_all(self.engine)
        try:
            go()
        finally:
            metadata.drop_all(self.engine)
        assert_no_mappers()

    def test_with_manytomany(self):
        metadata = MetaData()

        table1 = Table(
            "mytable",
            metadata,
            Column(
                "col1",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("col2", String(30)),
        )

        table2 = Table(
            "mytable2",
            metadata,
            Column(
                "col1",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("col2", String(30)),
        )

        table3 = Table(
            "t1tot2",
            metadata,
            Column("t1", Integer, ForeignKey("mytable.col1")),
            Column("t2", Integer, ForeignKey("mytable2.col1")),
        )

        @profile_memory()
        def go():
            class A(ComparableEntity):
                pass

            class B(ComparableEntity):
                pass

            self.mapper_registry.map_imperatively(
                A,
                table1,
                properties={
                    "bs": relationship(
                        B, secondary=table3, backref="as", order_by=table3.c.t1
                    )
                },
            )
            self.mapper_registry.map_imperatively(B, table2)

            sess = Session(self.engine, autoflush=False)
            a1 = A(col2="a1")
            a2 = A(col2="a2")
            b1 = B(col2="b1")
            b2 = B(col2="b2")
            a1.bs.append(b1)
            a2.bs.append(b2)
            for x in [a1, a2]:
                sess.add(x)
            sess.flush()
            sess.expunge_all()

            alist = sess.query(A).order_by(A.col1).all()
            eq_([A(bs=[B(col2="b1")]), A(bs=[B(col2="b2")])], alist)

            for a in alist:
                sess.delete(a)
            sess.flush()

            # mappers necessarily find themselves in the compiled cache,
            # so to allow them to be GC'ed clear out the cache
            self.engine.clear_compiled_cache()
            del B
            del A

        metadata.create_all(self.engine)
        try:
            go()
        finally:
            metadata.drop_all(self.engine)
        assert_no_mappers()

    def test_many_discarded_relationships(self):
        """a use case that really isn't supported, nonetheless we can
        guard against memleaks here so why not"""

        m1 = MetaData()
        t1 = Table("t1", m1, Column("id", Integer, primary_key=True))
        t2 = Table(
            "t2",
            m1,
            Column("id", Integer, primary_key=True),
            Column("t1id", ForeignKey("t1.id")),
        )

        class T1:
            pass

        t1_mapper = self.mapper_registry.map_imperatively(T1, t1)

        @testing.emits_warning(r"This declarative base")
        @testing.expect_deprecated(r"User-placed attribute .* is replacing")
        @profile_memory()
        def go():
            class T2:
                pass

            t2_mapper = self.mapper_registry.map_imperatively(T2, t2)
            t1_mapper.add_property("bar", relationship(t2_mapper))
            s1 = Session(testing.db)
            # this causes the path_registry to be invoked
            s1.query(t1_mapper)._compile_context()

        go()

    # fails on newer versions of pysqlite due to unusual memory behavior
    # in pysqlite itself. background at:
    # https://thread.gmane.org/gmane.comp.python.db.pysqlite.user/2290

    @testing.crashes("mysql+cymysql", "blocking")
    def test_join_cache(self):
        metadata = MetaData()
        table1 = Table(
            "table1",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(30)),
        )
        table2 = Table(
            "table2",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(30)),
            Column("t1id", Integer, ForeignKey("table1.id")),
        )

        class Foo:
            pass

        class Bar:
            pass

        self.mapper_registry.map_imperatively(
            Foo,
            table1,
            properties={
                "bars": relationship(
                    self.mapper_registry.map_imperatively(Bar, table2)
                )
            },
        )
        metadata.create_all(self.engine)
        session = sessionmaker(self.engine)

        @profile_memory()
        def go():
            s = aliased(Bar, table2.select().subquery())
            sess = session()
            sess.query(Foo).join(s, Foo.bars).all()
            sess.rollback()

        try:
            go()
        finally:
            metadata.drop_all(self.engine)


class SomeFoo(DialectKWArgs, ClauseElement):
    pass


class FooDialect(DefaultDialect):
    construct_arguments = [
        (
            SomeFoo,
            {
                "bar": False,
                "bat": False,
            },
        )
    ]


@testing.add_to_marker.memory_intensive
class CycleTest(_fixtures.FixtureTest):
    __requires__ = ("cpython", "no_windows")

    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()

    def test_query(self):
        User, Address = self.classes("User", "Address")
        configure_mappers()

        s = fixture_session()

        @assert_cycles()
        def go():
            return s.query(User).all()

        go()

    @testing.fixture
    def foo_dialect(self):
        registry.register("foo", __name__, "FooDialect")

        yield
        registry.deregister("foo")

    def test_dialect_kwargs(self, foo_dialect):

        @assert_cycles()
        def go():
            ff = SomeFoo()

            ff._validate_dialect_kwargs({"foo_bar": True})

            eq_(ff.dialect_options["foo"]["bar"], True)

            eq_(ff.dialect_options["foo"]["bat"], False)

            eq_(ff.dialect_kwargs["foo_bar"], True)
            eq_(ff.dialect_kwargs["foo_bat"], False)

            ff.dialect_kwargs["foo_bat"] = True
            eq_(ff.dialect_options["foo"]["bat"], True)

        go()

    def test_session_execute_orm(self):
        User, Address = self.classes("User", "Address")
        configure_mappers()

        s = fixture_session()

        @assert_cycles()
        def go():
            stmt = select(User)
            s.execute(stmt)

        go()

    def test_cache_key(self):
        User, Address = self.classes("User", "Address")
        configure_mappers()

        @assert_cycles()
        def go():
            stmt = select(User)
            stmt._generate_cache_key()

        go()

    def test_proxied_attribute(self):
        from sqlalchemy.ext import hybrid

        users = self.tables.users

        class Foo:
            @hybrid.hybrid_property
            def user_name(self):
                return self.name

        self.mapper_registry.map_imperatively(Foo, users)

        # unfortunately there's a lot of cycles with an aliased()
        # for now, however calling upon clause_element does not seem
        # to make it worse which is what this was looking to test.
        #
        # update as of #8796.  clause_element makes it a little bit worse
        # as we now generate more metrics for the .c collection.
        @assert_cycles(79)
        def go():
            a1 = aliased(Foo)
            a1.user_name.__clause_element__()

        go()

    def test_query_alias(self):
        User, Address = self.classes("User", "Address")
        configure_mappers()

        s = fixture_session()

        u1 = aliased(User)

        @assert_cycles()
        def go():
            s.query(u1).all()

        go()

    def test_entity_path_w_aliased(self):
        User, Address = self.classes("User", "Address")
        configure_mappers()

        @assert_cycles()
        def go():
            u1 = aliased(User)
            inspect(u1)._path_registry[User.addresses.property]

        go()

    def test_orm_objects_from_query(self):
        User, Address = self.classes("User", "Address")
        configure_mappers()

        s = fixture_session()

        def generate():
            objects = s.query(User).filter(User.id == 7).all()
            gc_collect()
            return objects

        @assert_cycles()
        def go():
            generate()

        go()

    def test_orm_objects_from_query_w_selectinload(self):
        User, Address = self.classes("User", "Address")

        s = fixture_session()

        def generate():
            objects = s.query(User).options(selectinload(User.addresses)).all()
            gc_collect()
            return objects

        @assert_cycles()
        def go():
            generate()

        go()

    def test_selectinload_option_unbound(self):
        User, Address = self.classes("User", "Address")

        @assert_cycles()
        def go():
            selectinload(User.addresses)

        go()

    def test_selectinload_option_bound(self):
        User, Address = self.classes("User", "Address")

        @assert_cycles()
        def go():
            Load(User).selectinload(User.addresses)

        go()

    def test_orm_path(self):
        User, Address = self.classes("User", "Address")

        @assert_cycles()
        def go():
            inspect(User)._path_registry[User.addresses.property][
                inspect(Address)
            ]

        go()

    def test_joinedload_option_unbound(self):
        User, Address = self.classes("User", "Address")

        @assert_cycles()
        def go():
            joinedload(User.addresses)

        go()

    def test_joinedload_option_bound(self):
        User, Address = self.classes("User", "Address")

        @assert_cycles()
        def go():
            l1 = Load(User).joinedload(User.addresses)
            l1._generate_cache_key()

        go()

    def test_orm_objects_from_query_w_joinedload(self):
        User, Address = self.classes("User", "Address")

        s = fixture_session()

        def generate():
            objects = s.query(User).options(joinedload(User.addresses)).all()
            gc_collect()
            return objects

        @assert_cycles()
        def go():
            generate()

        go()

    def test_query_filtered(self):
        User, Address = self.classes("User", "Address")

        s = fixture_session()

        @assert_cycles()
        def go():
            return s.query(User).filter(User.id == 7).all()

        go()

    def test_query_joins(self):
        User, Address = self.classes("User", "Address")

        s = fixture_session()

        # cycles here are due to ClauseElement._cloned_set, others
        # as of cache key
        @assert_cycles(4)
        def go():
            s.query(User).join(User.addresses).all()

        go()

    def test_query_joinedload(self):
        User, Address = self.classes("User", "Address")

        s = fixture_session()

        def generate():
            s.query(User).options(joinedload(User.addresses)).all()

        # cycles here are due to ClauseElement._cloned_set and Load.context,
        # others as of cache key.  The orm.instances() function now calls
        # dispose() on both the context and the compiled state to try
        # to reduce these cycles.
        @assert_cycles(18)
        def go():
            generate()

        go()

    def test_plain_join(self):
        users, addresses = self.tables("users", "addresses")

        @assert_cycles()
        def go():
            str(users.join(addresses).compile(testing.db))

        go()

    def test_plain_join_select(self):
        users, addresses = self.tables("users", "addresses")

        # cycles here are due to ClauseElement._cloned_set, others
        # as of cache key
        @assert_cycles(7)
        def go():
            s = select(users).select_from(users.join(addresses))
            state = s._compile_state_factory(s, s.compile(testing.db))
            state.froms

        go()

    def test_orm_join(self):
        User, Address = self.classes("User", "Address")

        @assert_cycles()
        def go():
            str(orm_join(User, Address, User.addresses).compile(testing.db))

        go()

    def test_join_via_query_relationship(self):
        User, Address = self.classes("User", "Address")
        configure_mappers()

        s = fixture_session()

        @assert_cycles()
        def go():
            s.query(User).join(User.addresses)

        go()

    def test_join_via_query_to_entity(self):
        User, Address = self.classes("User", "Address")
        configure_mappers()

        s = fixture_session()

        @assert_cycles()
        def go():
            s.query(User).join(Address)

        go()

    def test_result_fetchone(self):
        User, Address = self.classes("User", "Address")
        configure_mappers()

        s = fixture_session()

        stmt = s.query(User).join(User.addresses).statement

        @assert_cycles(4)
        def go():
            result = s.connection(bind_arguments=dict(mapper=User)).execute(
                stmt
            )
            while True:
                row = result.fetchone()
                if row is None:
                    break

        go()

    def test_result_fetchall(self):
        User, Address = self.classes("User", "Address")
        configure_mappers()

        s = fixture_session()

        stmt = s.query(User).join(User.addresses).statement

        @assert_cycles(4)
        def go():
            result = s.execute(stmt)
            rows = result.fetchall()  # noqa

        go()

    def test_result_fetchmany(self):
        User, Address = self.classes("User", "Address")
        configure_mappers()

        s = fixture_session()

        stmt = s.query(User).join(User.addresses).statement

        @assert_cycles(4)
        def go():
            result = s.execute(stmt)
            for partition in result.partitions(3):
                pass

        go()

    def test_result_fetchmany_unique(self):
        User, Address = self.classes("User", "Address")
        configure_mappers()

        s = fixture_session()

        stmt = s.query(User).join(User.addresses).statement

        @assert_cycles(4)
        def go():
            result = s.execute(stmt)
            for partition in result.unique().partitions(3):
                pass

        go()

    def test_core_select_from_orm_query(self):
        User, Address = self.classes("User", "Address")
        configure_mappers()

        s = fixture_session()

        stmt = s.query(User).join(User.addresses).statement

        # ORM query using future select for .statement is adding
        # some ORMJoin cycles here during compilation.  not worth trying to
        # find it
        @assert_cycles(4)
        def go():
            s.execute(stmt)

        go()

    def test_adapt_statement_replacement_traversal(self):
        User, Address = self.classes("User", "Address")

        statement = select(User).select_from(
            orm_join(User, Address, User.addresses)
        )

        @assert_cycles()
        def go():
            replacement_traverse(statement, {}, lambda x: None)

        go()

    def test_adapt_statement_cloned_traversal(self):
        User, Address = self.classes("User", "Address")

        statement = select(User).select_from(
            orm_join(User, Address, User.addresses)
        )

        @assert_cycles()
        def go():
            cloned_traverse(statement, {}, {})

        go()

    def test_column_adapter_lookup(self):
        User, Address = self.classes("User", "Address")

        u1 = aliased(User)

        @assert_cycles()
        def go():
            adapter = sql_util.ColumnAdapter(inspect(u1).selectable)
            adapter.columns[User.id]

        go()

    def test_orm_aliased(self):
        User, Address = self.classes("User", "Address")

        @assert_cycles()
        def go():
            u1 = aliased(User)
            inspect(u1)

        go()

    @testing.fails()
    def test_the_counter(self):
        @assert_cycles()
        def go():
            x = []
            x.append(x)

        go()

    def test_weak_sequence(self):
        class Foo:
            pass

        f = Foo()

        @assert_cycles()
        def go():
            util.WeakSequence([f])

        go()

    @testing.provide_metadata
    def test_optimized_get(self):
        Base = declarative_base(metadata=self.metadata)

        class Employee(Base):
            __tablename__ = "employee"
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            type = Column(String(10))
            __mapper_args__ = {"polymorphic_on": type}

        class Engineer(Employee):
            __tablename__ = " engineer"
            id = Column(ForeignKey("employee.id"), primary_key=True)

            engineer_name = Column(String(50))
            __mapper_args__ = {"polymorphic_identity": "engineer"}

        Base.metadata.create_all(testing.db)

        s = Session(testing.db)
        s.add(Engineer(engineer_name="wally"))
        s.commit()
        s.close()

        @assert_cycles()
        def go():
            e1 = s.query(Employee).first()
            e1.engineer_name

        go()

    def test_visit_binary_product(self):
        a, b, q, e, f, j, r = (column(chr_) for chr_ in "abqefjr")

        expr = and_((a + b) == q + func.sum(e + f), j == r)

        def visit(expr, left, right):
            pass

        @assert_cycles()
        def go():
            visit_binary_product(visit, expr)

        go()

    def test_session_transaction(self):
        @assert_cycles()
        def go():
            s = Session(testing.db)
            s.connection()
            s.close()

        go()

    def test_session_commit_rollback(self):
        # this is enabled by #5074
        @assert_cycles()
        def go():
            s = Session(testing.db)
            s.connection()
            s.commit()

        go()

        @assert_cycles()
        def go():
            s = Session(testing.db)
            s.connection()
            s.rollback()

        go()

    def test_session_multi_transaction(self):
        @assert_cycles()
        def go():
            s = Session(testing.db)
            assert s._transaction is None

            s.connection()

            s.close()
            assert s._transaction is None

            s.connection()
            assert s._transaction is not None

            s.close()

        go()


@testing.add_to_marker.memory_intensive
class MiscMemoryIntensiveTests(fixtures.TestBase):
    @testing.fixture
    def user_fixture(self, decl_base):
        class User(decl_base):
            __tablename__ = "user"

            id = Column(Integer, primary_key=True)
            name = Column(String(50))

        decl_base.metadata.create_all(testing.db)
        yield User

    @testing.requires.predictable_gc
    def test_gced_delete_on_rollback(self, user_fixture):
        User = user_fixture

        s = fixture_session()
        u1 = User(name="ed")
        s.add(u1)
        s.commit()

        s.delete(u1)
        u1_state = attributes.instance_state(u1)
        assert u1_state in s.identity_map.all_states()
        assert u1_state in s._deleted
        s.flush()
        assert u1_state not in s.identity_map.all_states()
        assert u1_state not in s._deleted
        del u1
        gc_collect()
        gc_collect()
        gc_collect()
        assert u1_state.obj() is None

        s.rollback()
        # new in 1.1, not in identity map if the object was
        # gc'ed and we restore snapshot; we've changed update_impl
        # to just skip this object
        assert u1_state not in s.identity_map.all_states()

        # in any version, the state is replaced by the query
        # because the identity map would switch it
        u1 = s.query(User).filter_by(name="ed").one()
        assert u1_state not in s.identity_map.all_states()

        eq_(s.scalar(select(func.count("*")).select_from(User.__table__)), 1)
        s.delete(u1)
        s.flush()
        eq_(s.scalar(select(func.count("*")).select_from(User.__table__)), 0)
        s.commit()


class WeakIdentityMapTest(_fixtures.FixtureTest):
    run_inserts = None

    def run_up_to_n_times(self, fn, times):
        error = None
        for _ in range(times):
            try:
                fn()
            except Exception as err:
                error = err
                continue
            else:
                break
        else:
            if error:
                raise error

    @testing.requires.predictable_gc
    def test_weakref(self):
        """test the weak-referencing identity map, which strongly-
        references modified items."""

        users, User = self.tables.users, self.classes.User
        self.mapper_registry.map_imperatively(User, users)

        def go():
            with Session(testing.db) as s:
                gc_collect()

                s.add(User(name="ed"))
                s.flush()
                assert not s.dirty

                user = s.query(User).one()

                # heisenberg the GC a little bit, since #7823 caused a lot more
                # GC when mappings are set up, larger test suite started
                # failing on this being gc'ed
                user_is = user._sa_instance_state
                del user
                gc_collect()
                gc_collect()
                gc_collect()
                assert user_is.obj() is None

                assert len(s.identity_map) == 0

                user = s.query(User).one()
                user.name = "fred"
                del user
                gc_collect()
                assert len(s.identity_map) == 1
                assert len(s.dirty) == 1
                assert None not in s.dirty
                s.flush()
                gc_collect()
                assert not s.dirty
                assert not s.identity_map

                user = s.query(User).one()
                assert user.name == "fred"
                assert s.identity_map

        self.run_up_to_n_times(go, 10)

    @testing.requires.predictable_gc
    def test_weakref_pickled(self):
        users, User = self.tables.users, pickleable.User
        self.mapper_registry.map_imperatively(User, users)

        def go():
            with Session(testing.db) as s:
                gc_collect()

                s.add(User(name="ed"))
                s.flush()
                assert not s.dirty

                user = s.query(User).one()
                user.name = "fred"
                s.expunge(user)

                u2 = pickle.loads(pickle.dumps(user))

                del user
                s.add(u2)

                del u2
                gc_collect()

                assert len(s.identity_map) == 1
                assert len(s.dirty) == 1
                assert None not in s.dirty
                s.flush()
                gc_collect()
                assert not s.dirty

                assert not s.identity_map

        self.run_up_to_n_times(go, 10)

    @testing.requires.predictable_gc
    def test_weakref_with_cycles_o2m(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={"addresses": relationship(Address, backref="user")},
        )
        self.mapper_registry.map_imperatively(Address, addresses)
        gc_collect()

        def go():
            with Session(testing.db) as s:
                s.add(
                    User(name="ed", addresses=[Address(email_address="ed1")])
                )
                s.commit()

                user = s.query(User).options(joinedload(User.addresses)).one()
                user.addresses[0].user  # lazyload
                eq_(
                    user,
                    User(name="ed", addresses=[Address(email_address="ed1")]),
                )

                del user
                gc_collect()
                assert len(s.identity_map) == 0

                user = s.query(User).options(joinedload(User.addresses)).one()
                user.addresses[0].email_address = "ed2"
                user.addresses[0].user  # lazyload
                del user
                gc_collect()
                assert len(s.identity_map) == 2

                s.commit()
                user = s.query(User).options(joinedload(User.addresses)).one()
                eq_(
                    user,
                    User(name="ed", addresses=[Address(email_address="ed2")]),
                )

        self.run_up_to_n_times(go, 10)

    @testing.requires.predictable_gc
    def test_weakref_with_cycles_o2o(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        s = fixture_session()
        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "address": relationship(Address, backref="user", uselist=False)
            },
        )
        self.mapper_registry.map_imperatively(Address, addresses)
        gc_collect()

        s.add(User(name="ed", address=Address(email_address="ed1")))
        s.commit()

        user = s.query(User).options(joinedload(User.address)).one()
        user.address.user
        eq_(user, User(name="ed", address=Address(email_address="ed1")))

        del user
        gc_collect()
        assert len(s.identity_map) == 0

        user = s.query(User).options(joinedload(User.address)).one()
        user.address.email_address = "ed2"
        user.address.user  # lazyload

        del user
        gc_collect()
        assert len(s.identity_map) == 2

        s.commit()
        user = s.query(User).options(joinedload(User.address)).one()
        eq_(user, User(name="ed", address=Address(email_address="ed2")))
