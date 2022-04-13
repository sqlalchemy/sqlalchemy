import sqlalchemy
from sqlalchemy import Column
from sqlalchemy import Enum
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
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
