import sqlalchemy
from sqlalchemy import Column
from sqlalchemy import Enum
from sqlalchemy import ForeignKey
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy.ext.declarative import ConcreteBase
from sqlalchemy.orm import aliased
from sqlalchemy.orm import join as ormjoin
from sqlalchemy.orm import relationship
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import profiling
from sqlalchemy.util import classproperty


class EnumTest(fixtures.TestBase):
    __requires__ = ("cpython", "python_profiling_backend")

    def setup_test(self):
        class SomeEnum:
            # Implements PEP 435 in the minimal fashion needed by SQLAlchemy

            _members = {}

            @classproperty
            def __members__(cls):
                """simulate a very expensive ``__members__`` getter"""
                for i in range(10):
                    x = {}
                    x.update({k: v for k, v in cls._members.items()}.copy())
                return x.copy()

            def __init__(self, name, value):
                self.name = name
                self.value = value
                self._members[name] = self
                setattr(self.__class__, name, self)

        for i in range(400):
            SomeEnum("some%d" % i, i)

        self.SomeEnum = SomeEnum

    @profiling.function_call_count()
    def test_create_enum_from_pep_435_w_expensive_members(self):
        Enum(self.SomeEnum, omit_aliases=False)


class CacheKeyTest(fixtures.TestBase):
    __requires__ = ("cpython", "python_profiling_backend")

    @testing.fixture(scope="class")
    def mapping_fixture(self):
        # note in order to work nicely with "fixture" we are emerging
        # a whole new model of setup/teardown, since pytest "fixture"
        # sort of purposely works badly with setup/teardown

        registry = sqlalchemy.orm.registry()

        metadata = MetaData()
        parent = Table(
            "parent",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String(20)),
        )
        child = Table(
            "child",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String(20)),
            Column(
                "parent_id", Integer, ForeignKey("parent.id"), nullable=False
            ),
        )

        class Parent(testing.entities.BasicEntity):
            pass

        class Child(testing.entities.BasicEntity):
            pass

        registry.map_imperatively(
            Parent,
            parent,
            properties={"children": relationship(Child, backref="parent")},
        )
        registry.map_imperatively(Child, child)

        registry.configure()

        yield Parent, Child

        registry.dispose()

    @testing.fixture(scope="function")
    def stmt_fixture_one(self, mapping_fixture):
        Parent, Child = mapping_fixture

        return [
            (
                select(Parent.id, Child.id)
                .select_from(ormjoin(Parent, Child, Parent.children))
                .where(Child.id == 5)
            )
            for i in range(100)
        ]

    @profiling.function_call_count(variance=0.15, warmup=2)
    def test_statement_key_is_cached(self, stmt_fixture_one):
        current_key = None
        for stmt in stmt_fixture_one:
            key = stmt._generate_cache_key()
            assert key is not None
            if current_key:
                eq_(key, current_key)
            else:
                current_key = key

    def test_statement_key_is_not_cached(
        self, stmt_fixture_one, mapping_fixture
    ):
        Parent, Child = mapping_fixture

        # run a totally different statement so that everything cache
        # related not specific to the statement is warmed up
        some_other_statement = (
            select(Parent.id, Child.id)
            .join_from(Parent, Child, Parent.children)
            .where(Parent.id == 5)
        )
        some_other_statement._generate_cache_key()

        @profiling.function_call_count(variance=0.15, warmup=0)
        def go():
            current_key = None
            for stmt in stmt_fixture_one:
                key = stmt._generate_cache_key()
                assert key is not None
                if current_key:
                    eq_(key, current_key)
                else:
                    current_key = key

        go()


class CCLookupTest(fixtures.RemoveORMEventsGlobally, fixtures.TestBase):
    __requires__ = ("cpython", "python_profiling_backend")

    @testing.fixture
    def t1(self, metadata):
        return Table(
            "t1",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("x1", Integer),
            Column("x2", Integer),
            Column("x3", Integer),
            Column("x4", Integer),
            Column("x5", Integer),
            Column("x6", Integer),
            Column("x7", Integer),
            Column("x8", Integer),
            Column("x9", Integer),
            Column("x10", Integer),
        )

    @testing.fixture
    def inheritance_model(self, decl_base):
        class Employee(ConcreteBase, decl_base):
            __tablename__ = "employee"
            id = Column(Integer, primary_key=True)
            name = Column(String(50))

            x1 = Column(Integer)
            x2 = Column(Integer)
            x3 = Column(Integer)
            x4 = Column(Integer)
            x5 = Column(Integer)
            x6 = Column(Integer)
            x7 = Column(Integer)
            x8 = Column(Integer)
            x9 = Column(Integer)
            x10 = Column(Integer)
            x11 = Column(Integer)
            x12 = Column(Integer)
            x13 = Column(Integer)
            x14 = Column(Integer)
            x15 = Column(Integer)
            x16 = Column(Integer)

            __mapper_args__ = {
                "polymorphic_identity": "employee",
                "concrete": True,
            }

        class Manager(Employee):
            __tablename__ = "manager"
            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            manager_data = Column(String(40))

            m1 = Column(Integer)
            m2 = Column(Integer)
            m3 = Column(Integer)
            m4 = Column(Integer)
            m5 = Column(Integer)
            m6 = Column(Integer)
            m7 = Column(Integer)
            m8 = Column(Integer)
            m9 = Column(Integer)
            m10 = Column(Integer)
            m11 = Column(Integer)
            m12 = Column(Integer)
            m13 = Column(Integer)
            m14 = Column(Integer)
            m15 = Column(Integer)
            m16 = Column(Integer)

            __mapper_args__ = {
                "polymorphic_identity": "manager",
                "concrete": True,
            }

        class Engineer(Employee):
            __tablename__ = "engineer"
            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            engineer_info = Column(String(40))

            e1 = Column(Integer)
            e2 = Column(Integer)
            e3 = Column(Integer)
            e4 = Column(Integer)
            e5 = Column(Integer)
            e6 = Column(Integer)
            e7 = Column(Integer)
            e8 = Column(Integer)
            e9 = Column(Integer)
            e10 = Column(Integer)
            e11 = Column(Integer)
            e12 = Column(Integer)
            e13 = Column(Integer)
            e14 = Column(Integer)
            e15 = Column(Integer)
            e16 = Column(Integer)

            __mapper_args__ = {
                "polymorphic_identity": "engineer",
                "concrete": True,
            }

        decl_base.registry.configure()

        return Employee

    @testing.combinations(
        ("require_embedded",), ("no_embedded",), argnames="require_embedded"
    )
    def test_corresponding_column_isolated(self, t1, require_embedded):

        subq = select(t1).union_all(select(t1)).subquery()

        target = subq.c.x7
        src = t1.c.x7

        subq.c

        require_embedded = require_embedded == "require_embedded"

        @profiling.function_call_count(variance=0.15, warmup=1)
        def go():
            assert (
                subq.corresponding_column(
                    src, require_embedded=require_embedded
                )
                is target
            )

        go()

    @testing.combinations(
        ("require_embedded",), ("no_embedded",), argnames="require_embedded"
    )
    def test_gen_subq_to_table_single_corresponding_column(
        self, t1, require_embedded
    ):

        src = t1.c.x7

        require_embedded = require_embedded == "require_embedded"

        @profiling.function_call_count(variance=0.15, warmup=1)
        def go():
            subq = select(t1).union_all(select(t1)).subquery()

            target = subq.c.x7
            assert (
                subq.corresponding_column(
                    src, require_embedded=require_embedded
                )
                is target
            )

        go()

    @testing.combinations(
        ("require_embedded",), ("no_embedded",), argnames="require_embedded"
    )
    def test_gen_subq_to_table_many_corresponding_column(
        self, t1, require_embedded
    ):

        require_embedded = require_embedded == "require_embedded"

        @profiling.function_call_count(variance=0.15, warmup=1)
        def go():
            subq = select(t1).union_all(select(t1)).subquery()

            for name in ("x%d" % i for i in range(1, 10)):

                target = subq.c[name]
                src = t1.c[name]

                assert (
                    subq.corresponding_column(
                        src, require_embedded=require_embedded
                    )
                    is target
                )

        go()

    @testing.combinations(
        ("require_embedded",), ("no_embedded",), argnames="require_embedded"
    )
    def test_gen_subq_aliased_class_select(
        self, t1, require_embedded, inheritance_model
    ):

        A = inheritance_model

        require_embedded = require_embedded == "require_embedded"

        @profiling.function_call_count(variance=0.15, warmup=1)
        def go():

            a1a1 = aliased(A)
            a1a2 = aliased(A)
            subq = select(a1a1).union_all(select(a1a2)).subquery()

            a1 = aliased(A, subq)

            inspect(a1).__clause_element__()

        go()

    @testing.combinations(
        ("require_embedded",), ("no_embedded",), argnames="require_embedded"
    )
    def test_gen_subq_aliased_class_select_cols(
        self, t1, require_embedded, inheritance_model
    ):

        A = inheritance_model

        require_embedded = require_embedded == "require_embedded"

        @profiling.function_call_count(variance=0.15, warmup=1)
        def go():

            a1a1 = aliased(A)
            a1a2 = aliased(A)
            subq = select(a1a1).union_all(select(a1a2)).subquery()

            a1 = aliased(A, subq)

            select(a1.x1, a1.x2, a1.x3, a1.x4)

        go()
