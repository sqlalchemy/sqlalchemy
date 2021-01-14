from sqlalchemy import and_
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import join
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.orm import aliased
from sqlalchemy.orm import Bundle
from sqlalchemy.orm import configure_mappers
from sqlalchemy.orm import defaultload
from sqlalchemy.orm import defer
from sqlalchemy.orm import join as orm_join
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import Load
from sqlalchemy.orm import mapper
from sqlalchemy.orm import relationship
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.testing import config
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import profiling
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table


class NoCache(object):
    run_setup_bind = "each"

    @classmethod
    def setup_test_class(cls):
        cls._cache = config.db._compiled_cache
        config.db._compiled_cache = None

    @classmethod
    def teardown_test_class(cls):
        config.db._compiled_cache = cls._cache


class MergeTest(NoCache, fixtures.MappedTest):
    __requires__ = ("python_profiling_backend",)

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "parent",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(20)),
        )
        Table(
            "child",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(20)),
            Column(
                "parent_id", Integer, ForeignKey("parent.id"), nullable=False
            ),
        )

    @classmethod
    def setup_classes(cls):
        class Parent(cls.Basic):
            pass

        class Child(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        Child, Parent, parent, child = (
            cls.classes.Child,
            cls.classes.Parent,
            cls.tables.parent,
            cls.tables.child,
        )

        mapper(
            Parent,
            parent,
            properties={"children": relationship(Child, backref="parent")},
        )
        mapper(Child, child)

    @classmethod
    def insert_data(cls, connection):
        parent, child = cls.tables.parent, cls.tables.child

        connection.execute(parent.insert(), {"id": 1, "data": "p1"})
        connection.execute(
            child.insert(), {"id": 1, "data": "p1c1", "parent_id": 1}
        )

    def test_merge_no_load(self):
        Parent = self.classes.Parent

        sess = fixture_session()
        sess2 = fixture_session()
        p1 = sess.query(Parent).get(1)
        p1.children

        # down from 185 on this this is a small slice of a usually
        # bigger operation so using a small variance

        sess2._legacy_transaction()  # autobegin

        @profiling.function_call_count(variance=0.20)
        def go1():
            return sess2.merge(p1, load=False)

        p2 = go1()

        # third call, merge object already present. almost no calls.

        sess2._legacy_transaction()  # autobegin

        @profiling.function_call_count(variance=0.10, warmup=1)
        def go2():
            return sess2.merge(p2, load=False)

        go2()

    def test_merge_load(self):
        Parent = self.classes.Parent

        sess = fixture_session()
        sess2 = fixture_session()
        p1 = sess.query(Parent).get(1)
        p1.children

        # preloading of collection took this down from 1728 to 1192
        # using sqlite3 the C extension took it back up to approx. 1257
        # (py2.6)

        sess2._legacy_transaction()  # autobegin

        @profiling.function_call_count(variance=0.10)
        def go():
            sess2.merge(p1)

        go()

        # one more time, count the SQL

        def go2():
            sess2.merge(p1)

        sess2 = sessionmaker(testing.db)()
        self.assert_sql_count(testing.db, go2, 2)


class LoadManyToOneFromIdentityTest(NoCache, fixtures.MappedTest):

    """test overhead associated with many-to-one fetches.

    Prior to the refactor of LoadLazyAttribute and
    query._get(), the load from identity map took 2x
    as many calls (65K calls here instead of around 33K)
    to load 1000 related objects from the identity map.

    """

    __requires__ = ("python_profiling_backend",)

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "parent",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String(20)),
            Column("child_id", Integer, ForeignKey("child.id")),
        )

        Table(
            "child",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String(20)),
        )

    @classmethod
    def setup_classes(cls):
        class Parent(cls.Basic):
            pass

        class Child(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        Child, Parent, parent, child = (
            cls.classes.Child,
            cls.classes.Parent,
            cls.tables.parent,
            cls.tables.child,
        )

        mapper(Parent, parent, properties={"child": relationship(Child)})
        mapper(Child, child)

    @classmethod
    def insert_data(cls, connection):
        parent, child = cls.tables.parent, cls.tables.child

        connection.execute(
            child.insert(),
            [{"id": i, "data": "c%d" % i} for i in range(1, 251)],
        )
        connection.execute(
            parent.insert(),
            [
                {
                    "id": i,
                    "data": "p%dc%d" % (i, (i % 250) + 1),
                    "child_id": (i % 250) + 1,
                }
                for i in range(1, 1000)
            ],
        )

    def test_many_to_one_load_no_identity(self):
        Parent = self.classes.Parent

        sess = fixture_session()
        parents = sess.query(Parent).all()

        @profiling.function_call_count(variance=0.2)
        def go():
            for p in parents:
                p.child

        go()

    def test_many_to_one_load_identity(self):
        Parent, Child = self.classes.Parent, self.classes.Child

        sess = fixture_session()
        parents = sess.query(Parent).all()
        children = sess.query(Child).all()
        children  # strong reference

        @profiling.function_call_count()
        def go():
            for p in parents:
                p.child

        go()


class MergeBackrefsTest(NoCache, fixtures.MappedTest):
    __requires__ = ("python_profiling_backend",)

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "a",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("c_id", Integer, ForeignKey("c.id")),
        )
        Table(
            "b",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("a_id", Integer, ForeignKey("a.id")),
        )
        Table("c", metadata, Column("id", Integer, primary_key=True))
        Table(
            "d",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("a_id", Integer, ForeignKey("a.id")),
        )

    @classmethod
    def setup_classes(cls):
        class A(cls.Basic):
            pass

        class B(cls.Basic):
            pass

        class C(cls.Basic):
            pass

        class D(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        A, B, C, D = cls.classes.A, cls.classes.B, cls.classes.C, cls.classes.D
        a, b, c, d = cls.tables.a, cls.tables.b, cls.tables.c, cls.tables.d
        mapper(
            A,
            a,
            properties={
                "bs": relationship(B, backref="a"),
                "c": relationship(C, backref="as"),
                "ds": relationship(D, backref="a"),
            },
        )
        mapper(B, b)
        mapper(C, c)
        mapper(D, d)

    @classmethod
    def insert_data(cls, connection):
        A, B, C, D = cls.classes.A, cls.classes.B, cls.classes.C, cls.classes.D
        s = Session(connection)
        s.add_all(
            [
                A(
                    id=i,
                    bs=[B(id=(i * 5) + j) for j in range(1, 5)],
                    c=C(id=i),
                    ds=[D(id=(i * 5) + j) for j in range(1, 5)],
                )
                for i in range(1, 5)
            ]
        )
        s.commit()

    @profiling.function_call_count(variance=0.10)
    def test_merge_pending_with_all_pks(self):
        A, B, C, D = (
            self.classes.A,
            self.classes.B,
            self.classes.C,
            self.classes.D,
        )
        s = fixture_session()
        for a in [
            A(
                id=i,
                bs=[B(id=(i * 5) + j) for j in range(1, 5)],
                c=C(id=i),
                ds=[D(id=(i * 5) + j) for j in range(1, 5)],
            )
            for i in range(1, 5)
        ]:
            s.merge(a)


class DeferOptionsTest(NoCache, fixtures.MappedTest):
    __requires__ = ("python_profiling_backend",)

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "a",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("x", String(5)),
            Column("y", String(5)),
            Column("z", String(5)),
            Column("q", String(5)),
            Column("p", String(5)),
            Column("r", String(5)),
        )

    @classmethod
    def setup_classes(cls):
        class A(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        A = cls.classes.A
        a = cls.tables.a
        mapper(A, a)

    @classmethod
    def insert_data(cls, connection):
        A = cls.classes.A
        s = Session(connection)
        s.add_all(
            [
                A(
                    id=i,
                    **dict(
                        (letter, "%s%d" % (letter, i))
                        for letter in ["x", "y", "z", "p", "q", "r"]
                    )
                )
                for i in range(1, 1001)
            ]
        )
        s.commit()

    @profiling.function_call_count(variance=0.10)
    def test_baseline(self):
        # as of [ticket:2778], this is at 39025
        A = self.classes.A
        s = fixture_session()
        s.query(A).all()

    @profiling.function_call_count(variance=0.10)
    def test_defer_many_cols(self):
        # with [ticket:2778], this goes from 50805 to 32817,
        # as it should be fewer function calls than the baseline
        A = self.classes.A
        s = fixture_session()
        s.query(A).options(
            *[defer(letter) for letter in ["x", "y", "z", "p", "q", "r"]]
        ).all()


class AttributeOverheadTest(NoCache, fixtures.MappedTest):
    __requires__ = ("python_profiling_backend",)

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "parent",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(20)),
        )
        Table(
            "child",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(20)),
            Column(
                "parent_id", Integer, ForeignKey("parent.id"), nullable=False
            ),
        )

    @classmethod
    def setup_classes(cls):
        class Parent(cls.Basic):
            pass

        class Child(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        Child, Parent, parent, child = (
            cls.classes.Child,
            cls.classes.Parent,
            cls.tables.parent,
            cls.tables.child,
        )

        mapper(
            Parent,
            parent,
            properties={"children": relationship(Child, backref="parent")},
        )
        mapper(Child, child)

    def test_attribute_set(self):
        Parent, Child = self.classes.Parent, self.classes.Child
        p1 = Parent()
        c1 = Child()

        @profiling.function_call_count()
        def go():
            for i in range(30):
                c1.parent = p1
                c1.parent = None
                c1.parent = p1
                del c1.parent

        go()

    def test_collection_append_remove(self):
        Parent, Child = self.classes.Parent, self.classes.Child
        p1 = Parent()
        children = [Child() for i in range(100)]

        @profiling.function_call_count()
        def go():
            for child in children:
                p1.children.append(child)
            for child in children:
                p1.children.remove(child)

        go()


class SessionTest(NoCache, fixtures.MappedTest):
    __requires__ = ("python_profiling_backend",)

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "parent",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(20)),
        )
        Table(
            "child",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(20)),
            Column(
                "parent_id", Integer, ForeignKey("parent.id"), nullable=False
            ),
        )

    @classmethod
    def setup_classes(cls):
        class Parent(cls.Basic):
            pass

        class Child(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        Child, Parent, parent, child = (
            cls.classes.Child,
            cls.classes.Parent,
            cls.tables.parent,
            cls.tables.child,
        )

        mapper(
            Parent,
            parent,
            properties={"children": relationship(Child, backref="parent")},
        )
        mapper(Child, child)

    def test_expire_lots(self):
        Parent, Child = self.classes.Parent, self.classes.Child
        obj = [
            Parent(children=[Child() for j in range(10)]) for i in range(10)
        ]

        sess = fixture_session()
        sess.add_all(obj)
        sess.flush()

        @profiling.function_call_count()
        def go():
            sess.expire_all()

        go()


class QueryTest(NoCache, fixtures.MappedTest):
    __requires__ = ("python_profiling_backend",)

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "parent",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data1", String(20)),
            Column("data2", String(20)),
            Column("data3", String(20)),
            Column("data4", String(20)),
        )

    @classmethod
    def setup_classes(cls):
        class Parent(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        Parent = cls.classes.Parent
        parent = cls.tables.parent

        mapper(Parent, parent)

    def _fixture(self):
        Parent = self.classes.Parent
        sess = fixture_session()
        sess.add_all(
            [
                Parent(data1="d1", data2="d2", data3="d3", data4="d4")
                for i in range(10)
            ]
        )
        sess.commit()
        sess.close()

    def test_query_cols(self):
        Parent = self.classes.Parent
        self._fixture()
        sess = fixture_session()

        # warm up cache
        for attr in [Parent.data1, Parent.data2, Parent.data3, Parent.data4]:
            attr.__clause_element__()

        @profiling.function_call_count()
        def go():
            for i in range(10):
                q = sess.query(
                    Parent.data1, Parent.data2, Parent.data3, Parent.data4
                )

                q.all()

        go()


class SelectInEagerLoadTest(NoCache, fixtures.MappedTest):
    """basic test for selectin() loading, which uses a lambda query.

    For the previous "baked query" version of this, statement caching
    was still taking effect as the selectinloader used its own baked
    query cache.  in 1.4 we align the loader caches with the global
    "cache_size" (tenatitively) so the callcount has gone up to accommodate
    for 3x the compilations.

    """

    __requires__ = ("python_profiling_backend",)

    @classmethod
    def define_tables(cls, metadata):

        Table(
            "a",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("x", Integer),
            Column("y", Integer),
        )
        Table(
            "b",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("a_id", ForeignKey("a.id")),
            Column("x", Integer),
            Column("y", Integer),
        )
        Table(
            "c",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("b_id", ForeignKey("b.id")),
            Column("x", Integer),
            Column("y", Integer),
        )

    @classmethod
    def setup_classes(cls):
        class A(cls.Basic):
            pass

        class B(cls.Basic):
            pass

        class C(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        A, B, C = cls.classes("A", "B", "C")
        a, b, c = cls.tables("a", "b", "c")

        mapper(A, a, properties={"bs": relationship(B)})
        mapper(B, b, properties={"cs": relationship(C)})
        mapper(C, c)

    @classmethod
    def insert_data(cls, connection):
        A, B, C = cls.classes("A", "B", "C")
        s = Session(connection)
        s.add(A(bs=[B(cs=[C()]), B(cs=[C()])]))
        s.commit()

    def test_round_trip_results(self):
        A, B, C = self.classes("A", "B", "C")

        sess = fixture_session()

        q = sess.query(A).options(selectinload(A.bs).selectinload(B.cs))

        # note this value went up when we removed query._attributes;
        # this is because the test was previously making use of the same
        # loader option state repeatedly without rebuilding it.

        @profiling.function_call_count(warmup=1)
        def go():
            for i in range(100):
                obj = q.all()
                list(obj)
                sess.close()

        go()


class JoinedEagerLoadTest(NoCache, fixtures.MappedTest):
    __requires__ = ("python_profiling_backend",)

    @classmethod
    def define_tables(cls, metadata):
        def make_some_columns():
            return [Column("c%d" % i, Integer) for i in range(10)]

        Table(
            "a",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            *make_some_columns()
        )
        Table(
            "b",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("a_id", ForeignKey("a.id")),
            *make_some_columns()
        )
        Table(
            "c",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("b_id", ForeignKey("b.id")),
            *make_some_columns()
        )
        Table(
            "d",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("c_id", ForeignKey("c.id")),
            *make_some_columns()
        )
        Table(
            "e",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("a_id", ForeignKey("a.id")),
            *make_some_columns()
        )
        Table(
            "f",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("e_id", ForeignKey("e.id")),
            *make_some_columns()
        )
        Table(
            "g",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("e_id", ForeignKey("e.id")),
            *make_some_columns()
        )

    @classmethod
    def setup_classes(cls):
        class A(cls.Basic):
            pass

        class B(cls.Basic):
            pass

        class C(cls.Basic):
            pass

        class D(cls.Basic):
            pass

        class E(cls.Basic):
            pass

        class F(cls.Basic):
            pass

        class G(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        A, B, C, D, E, F, G = cls.classes("A", "B", "C", "D", "E", "F", "G")
        a, b, c, d, e, f, g = cls.tables("a", "b", "c", "d", "e", "f", "g")

        mapper(A, a, properties={"bs": relationship(B), "es": relationship(E)})
        mapper(B, b, properties={"cs": relationship(C)})
        mapper(C, c, properties={"ds": relationship(D)})
        mapper(D, d)
        mapper(E, e, properties={"fs": relationship(F), "gs": relationship(G)})
        mapper(F, f)
        mapper(G, g)

    @classmethod
    def insert_data(cls, connection):
        A, B, C, D, E, F, G = cls.classes("A", "B", "C", "D", "E", "F", "G")
        s = Session(connection)
        s.add(
            A(
                bs=[B(cs=[C(ds=[D()])]), B(cs=[C()])],
                es=[E(fs=[F()], gs=[G()])],
            )
        )
        s.commit()

    def test_build_query(self):
        A, B, C, D, E, F, G = self.classes("A", "B", "C", "D", "E", "F", "G")

        sess = fixture_session()

        @profiling.function_call_count()
        def go():
            for i in range(100):
                q = sess.query(A).options(
                    joinedload(A.bs).joinedload(B.cs).joinedload(C.ds),
                    joinedload(A.es).joinedload(E.fs),
                    defaultload(A.es).joinedload(E.gs),
                )
                q._compile_context()

        go()

    def test_fetch_results(self):
        A, B, C, D, E, F, G = self.classes("A", "B", "C", "D", "E", "F", "G")

        sess = Session(testing.db)

        q = sess.query(A).options(
            joinedload(A.bs).joinedload(B.cs).joinedload(C.ds),
            joinedload(A.es).joinedload(E.fs),
            defaultload(A.es).joinedload(E.gs),
        )

        compile_state = q._compile_state()

        from sqlalchemy.orm.context import ORMCompileState

        @profiling.function_call_count(warmup=1)
        def go():
            for i in range(100):
                # NOTE: this test was broken in
                # 77f1b7d236dba6b1c859bb428ef32d118ec372e6 because we started
                # clearing out the attributes after the first iteration.   make
                # sure the attributes are there every time.
                assert compile_state.attributes
                exec_opts = {}
                bind_arguments = {}
                ORMCompileState.orm_pre_session_exec(
                    sess,
                    compile_state.select_statement,
                    {},
                    exec_opts,
                    bind_arguments,
                    is_reentrant_invoke=False,
                )

                r = sess.connection().execute(
                    compile_state.statement,
                    execution_options=exec_opts,
                    bind_arguments=bind_arguments,
                )

                r.context.compiled.compile_state = compile_state
                obj = ORMCompileState.orm_setup_cursor_result(
                    sess,
                    compile_state.statement,
                    {},
                    exec_opts,
                    {},
                    r,
                )
                list(obj.unique())
                sess.close()

        go()


class JoinConditionTest(NoCache, fixtures.DeclarativeMappedTest):
    __requires__ = ("python_profiling_backend",)

    @classmethod
    def setup_classes(cls):
        class A(cls.DeclarativeBasic):
            __tablename__ = "a"

            id = Column(Integer, primary_key=True)
            b_id = Column(ForeignKey("b.id"))
            b = relationship("B")

        class B(cls.DeclarativeBasic):
            __tablename__ = "b"

            id = Column(Integer, primary_key=True)
            d_id = Column(ForeignKey("d.id"))

        class C(cls.DeclarativeBasic):
            __tablename__ = "c"

            id = Column(Integer, primary_key=True)
            a_id = Column(ForeignKey("a.id"))
            d_id = Column(ForeignKey("d.id"))

        class D(cls.DeclarativeBasic):
            __tablename__ = "d"

            id = Column(Integer, primary_key=True)

        j = join(B, D, B.d_id == D.id).join(C, C.d_id == D.id)

        A.d = relationship(
            "D",
            secondary=j,
            primaryjoin=and_(A.b_id == B.id, A.id == C.a_id),
            secondaryjoin=D.id == B.d_id,
            uselist=False,
            viewonly=True,
        )

    def test_a_to_b_plain(self):
        A, B = self.classes("A", "B")

        # should not use aliasing or adaption so should be cheap
        @profiling.function_call_count(times=50, warmup=1)
        def go():
            orm_join(A, B, A.b)

        go()

    def test_a_to_b_aliased(self):
        A, B = self.classes("A", "B")

        a1 = aliased(A)

        # uses aliasing, therefore adaption which is expensive
        @profiling.function_call_count(times=50, warmup=1)
        def go():
            orm_join(a1, B, a1.b)

        go()

    def test_a_to_d(self):
        A, D = self.classes("A", "D")

        # the join condition between A and D uses a secondary selectable  with
        # overlap so incurs aliasing, which is expensive, there is also a check
        # that determines that this overlap exists which is not currently
        # cached
        @profiling.function_call_count(times=50, warmup=1)
        def go():
            orm_join(A, D, A.d)

        go()

    def test_a_to_d_aliased(self):
        A, D = self.classes("A", "D")

        a1 = aliased(A)

        # aliased, uses adaption therefore expensive
        @profiling.function_call_count(times=50, warmup=1)
        def go():
            orm_join(a1, D, a1.d)

        go()


class BranchedOptionTest(NoCache, fixtures.MappedTest):
    __requires__ = ("python_profiling_backend",)

    @classmethod
    def define_tables(cls, metadata):
        def make_some_columns():
            return [Column("c%d" % i, Integer) for i in range(2)]

        Table(
            "a",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            *make_some_columns()
        )
        Table(
            "b",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("a_id", ForeignKey("a.id")),
            *make_some_columns()
        )
        Table(
            "c",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("b_id", ForeignKey("b.id")),
            *make_some_columns()
        )
        Table(
            "d",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("b_id", ForeignKey("b.id")),
            *make_some_columns()
        )
        Table(
            "e",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("b_id", ForeignKey("b.id")),
            *make_some_columns()
        )
        Table(
            "f",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("b_id", ForeignKey("b.id")),
            *make_some_columns()
        )
        Table(
            "g",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("a_id", ForeignKey("a.id")),
            *make_some_columns()
        )

    @classmethod
    def setup_classes(cls):
        class A(cls.Basic):
            pass

        class B(cls.Basic):
            pass

        class C(cls.Basic):
            pass

        class D(cls.Basic):
            pass

        class E(cls.Basic):
            pass

        class F(cls.Basic):
            pass

        class G(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        A, B, C, D, E, F, G = cls.classes("A", "B", "C", "D", "E", "F", "G")
        a, b, c, d, e, f, g = cls.tables("a", "b", "c", "d", "e", "f", "g")

        mapper(A, a, properties={"bs": relationship(B), "gs": relationship(G)})
        mapper(
            B,
            b,
            properties={
                "cs": relationship(C),
                "ds": relationship(D),
                "es": relationship(E),
                "fs": relationship(F),
            },
        )
        mapper(C, c)
        mapper(D, d)
        mapper(E, e)
        mapper(F, f)
        mapper(G, g)

        configure_mappers()

    def test_query_opts_unbound_branching(self):
        A, B, C, D, E, F, G = self.classes("A", "B", "C", "D", "E", "F", "G")

        base = joinedload(A.bs)
        opts = [
            base.joinedload(B.cs),
            base.joinedload(B.ds),
            base.joinedload(B.es),
            base.joinedload(B.fs),
        ]

        q = fixture_session().query(A)

        context = q._compile_state()

        @profiling.function_call_count(warmup=1)
        def go():
            q2 = q.options(opts)
            context.query = q2
            context.attributes = q2._attributes = {
                "_unbound_load_dedupes": set()
            }
            for opt in q2._with_options:
                opt.process_compile_state(context)

        go()

    def test_query_opts_key_bound_branching(self):
        A, B, C, D, E, F, G = self.classes("A", "B", "C", "D", "E", "F", "G")

        base = Load(A).joinedload(A.bs)
        opts = [
            base.joinedload(B.cs),
            base.joinedload(B.ds),
            base.joinedload(B.es),
            base.joinedload(B.fs),
        ]

        q = fixture_session().query(A)

        context = q._compile_state()

        @profiling.function_call_count(warmup=1)
        def go():
            q2 = q.options(opts)
            context.query = q2
            context.attributes = q2._attributes = {
                "_unbound_load_dedupes": set()
            }
            for opt in q2._with_options:
                opt.process_compile_state(context)

        go()


class AnnotatedOverheadTest(NoCache, fixtures.MappedTest):
    __requires__ = ("python_profiling_backend",)

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "a",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(50)),
        )

    @classmethod
    def setup_classes(cls):
        class A(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        A = cls.classes.A
        a = cls.tables.a

        mapper(A, a)

    @classmethod
    def insert_data(cls, connection):
        A = cls.classes.A
        s = Session(connection)
        s.add_all([A(data="asdf") for i in range(5)])
        s.commit()

    def test_no_bundle(self):
        A = self.classes.A
        s = fixture_session()

        q = s.query(A).select_from(A)

        @profiling.function_call_count(warmup=1)
        def go():
            for i in range(100):
                q.all()

        go()

    def test_no_entity_wo_annotations(self):
        A = self.classes.A
        a = self.tables.a
        s = fixture_session()

        q = s.query(a.c.data).select_from(A)

        @profiling.function_call_count(warmup=1)
        def go():
            for i in range(100):
                q.all()

        go()

    def test_no_entity_w_annotations(self):
        A = self.classes.A
        s = fixture_session()
        q = s.query(A.data).select_from(A)

        @profiling.function_call_count(warmup=1)
        def go():
            for i in range(100):
                q.all()

        go()

    def test_entity_w_annotations(self):
        A = self.classes.A
        s = fixture_session()
        q = s.query(A, A.data).select_from(A)

        @profiling.function_call_count(warmup=1)
        def go():
            for i in range(100):
                q.all()

        go()

    def test_entity_wo_annotations(self):
        A = self.classes.A
        a = self.tables.a
        s = fixture_session()
        q = s.query(A, a.c.data).select_from(A)

        @profiling.function_call_count(warmup=1)
        def go():
            for i in range(100):
                q.all()

        go()

    def test_no_bundle_wo_annotations(self):
        A = self.classes.A
        a = self.tables.a
        s = fixture_session()
        q = s.query(a.c.data, A).select_from(A)

        @profiling.function_call_count(warmup=1)
        def go():
            for i in range(100):
                q.all()

        go()

    def test_no_bundle_w_annotations(self):
        A = self.classes.A
        s = fixture_session()
        q = s.query(A.data, A).select_from(A)

        @profiling.function_call_count(warmup=1)
        def go():
            for i in range(100):
                q.all()

        go()

    def test_bundle_wo_annotation(self):
        A = self.classes.A
        a = self.tables.a
        s = fixture_session()
        q = s.query(Bundle("ASdf", a.c.data), A).select_from(A)

        @profiling.function_call_count(warmup=1)
        def go():
            for i in range(100):
                q.all()

        go()

    def test_bundle_w_annotation(self):
        A = self.classes.A
        s = fixture_session()
        q = s.query(Bundle("ASdf", A.data), A).select_from(A)

        @profiling.function_call_count(warmup=1)
        def go():
            for i in range(100):
                q.all()

        go()
