from unittest.mock import call
from unittest.mock import Mock

import sqlalchemy as sa
from sqlalchemy import cast
from sqlalchemy import column
from sqlalchemy import desc
from sqlalchemy import event
from sqlalchemy import exc as sa_exc
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Identity
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import literal_column
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy.engine import default
from sqlalchemy.engine import result_tuple
from sqlalchemy.orm import aliased
from sqlalchemy.orm import attributes
from sqlalchemy.orm import clear_mappers
from sqlalchemy.orm import column_property
from sqlalchemy.orm import contains_alias
from sqlalchemy.orm import contains_eager
from sqlalchemy.orm import defaultload
from sqlalchemy.orm import defer
from sqlalchemy.orm import deferred
from sqlalchemy.orm import foreign
from sqlalchemy.orm import instrumentation
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import strategies
from sqlalchemy.orm import subqueryload
from sqlalchemy.orm import synonym
from sqlalchemy.orm import undefer
from sqlalchemy.orm import with_polymorphic
from sqlalchemy.orm.strategy_options import lazyload
from sqlalchemy.orm.strategy_options import noload
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import assertions
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import eq_ignore_whitespace
from sqlalchemy.testing import expect_deprecated
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import mock
from sqlalchemy.testing.assertions import expect_noload_deprecation
from sqlalchemy.testing.assertions import in_
from sqlalchemy.testing.entities import ComparableEntity
from sqlalchemy.testing.fixtures import CacheKeyFixture
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from . import _fixtures
from .inheritance import _poly_fixtures
from .inheritance._poly_fixtures import Manager
from .inheritance._poly_fixtures import Person
from .test_default_strategies import DefaultStrategyOptionsTestFixtures
from .test_deferred import InheritanceTest as _deferred_InheritanceTest
from .test_dynamic import _DynamicFixture
from .test_dynamic import _WriteOnlyFixture
from .test_options import PathTest
from .test_options import QueryTest as OptionsQueryTest
from .test_query import QueryTest

join_aliased_dep = (
    r"The ``aliased`` and ``from_joinpoint`` keyword arguments to "
    r"Query.join\(\)"
)

w_polymorphic_dep = (
    r"The Query.with_polymorphic\(\) method is "
    "considered legacy as of the 1.x series"
)

join_chain_dep = (
    r"Passing a chain of multiple join conditions to Query.join\(\)"
)

undefer_needs_chaining = (
    r"The \*addl_attrs on orm.(?:un)?defer is deprecated.  "
    "Please use method chaining"
)

join_tuple_form = (
    r"Query.join\(\) will no longer accept tuples as "
    "arguments in SQLAlchemy 2.0."
)


query_wparent_dep = (
    r"The Query.with_parent\(\) method is considered legacy as of the 1.x"
)

query_get_dep = r"The Query.get\(\) method is considered legacy as of the 1.x"

with_polymorphic_dep = (
    r"The Query.with_polymorphic\(\) method is considered legacy as of "
    r"the 1.x series of SQLAlchemy and will be removed in 2.0"
)

merge_result_dep = (
    r"The merge_result\(\) function is considered legacy as of the 1.x series"
)

dep_exc_wildcard = (
    r"The undocumented `.{WILDCARD}` format is deprecated and will be removed "
    r"in a future version as it is believed to be unused. If you have been "
    r"using this functionality, please comment on Issue #4390 on the "
    r"SQLAlchemy project tracker."
)


def _aliased_join_warning(arg=None):
    return testing.expect_warnings(
        "An alias is being generated automatically against joined entity "
        "Mapper" + (arg if arg else "")
    )


def _aliased_join_deprecation(arg=None):
    return testing.expect_deprecated(
        "An alias is being generated automatically against joined entity "
        "Mapper" + (arg if arg else "")
    )


class GetTest(QueryTest):
    def test_get(self):
        User = self.classes.User

        s = fixture_session()
        with assertions.expect_deprecated_20(query_get_dep):
            assert s.query(User).get(19) is None
        with assertions.expect_deprecated_20(query_get_dep):
            u = s.query(User).get(7)
        with assertions.expect_deprecated_20(query_get_dep):
            u2 = s.query(User).get(7)
        assert u is u2
        s.expunge_all()
        with assertions.expect_deprecated_20(query_get_dep):
            u2 = s.query(User).get(7)
        assert u is not u2

    def test_loader_options(self):
        User = self.classes.User

        s = fixture_session()

        with assertions.expect_deprecated_20(query_get_dep):
            u1 = s.query(User).options(joinedload(User.addresses)).get(8)
        eq_(len(u1.__dict__["addresses"]), 3)

    def test_no_criterion_when_already_loaded(self):
        """test that get()/load() does not use preexisting filter/etc.
        criterion, even when we're only using the identity map."""

        User, Address = self.classes.User, self.classes.Address

        s = fixture_session()

        s.get(User, 7)

        q = s.query(User).join(User.addresses).filter(Address.user_id == 8)
        with assertions.expect_deprecated_20(query_get_dep):
            with assertions.expect_raises_message(
                sa_exc.InvalidRequestError,
                r"Query.get\(\) being called on a Query with existing "
                "criterion.",
            ):
                q.get(7)

    def test_no_criterion(self):
        """test that get()/load() does not use preexisting filter/etc.
        criterion"""

        User, Address = self.classes.User, self.classes.Address

        s = fixture_session()

        q = s.query(User).join(User.addresses).filter(Address.user_id == 8)

        with assertions.expect_deprecated_20(query_get_dep):
            with assertions.expect_raises_message(
                sa_exc.InvalidRequestError,
                r"Query.get\(\) being called on a Query with existing "
                "criterion.",
            ):
                q.get(7)

        with assertions.expect_deprecated_20(query_get_dep):
            with assertions.expect_raises_message(
                sa_exc.InvalidRequestError,
                r"Query.get\(\) being called on a Query with existing "
                "criterion.",
            ):
                s.query(User).filter(User.id == 7).get(19)

        # order_by()/get() doesn't raise
        with assertions.expect_deprecated_20(query_get_dep):
            s.query(User).order_by(User.id).get(8)

    def test_get_against_col(self):
        User = self.classes.User

        s = fixture_session()

        with assertions.expect_deprecated_20(query_get_dep):
            with assertions.expect_raises_message(
                sa_exc.InvalidRequestError,
                r"get\(\) can only be used against a single mapped class.",
            ):
                s.query(User.id).get(5)

    def test_only_full_mapper_zero(self):
        User, Address = self.classes.User, self.classes.Address

        s = fixture_session()
        q = s.query(User, Address)

        with assertions.expect_deprecated_20(query_get_dep):
            with assertions.expect_raises_message(
                sa_exc.InvalidRequestError,
                r"get\(\) can only be used against a single mapped class.",
            ):
                q.get(5)


class PickleTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(30), nullable=False),
            test_needs_acid=True,
            test_needs_fk=True,
        )

        Table(
            "addresses",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("user_id", None, ForeignKey("users.id")),
            Column("email_address", String(50), nullable=False),
            test_needs_acid=True,
            test_needs_fk=True,
        )
        Table(
            "orders",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("user_id", None, ForeignKey("users.id")),
            Column("address_id", None, ForeignKey("addresses.id")),
            Column("description", String(30)),
            Column("isopen", Integer),
            test_needs_acid=True,
            test_needs_fk=True,
        )
        Table(
            "dingalings",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("address_id", None, ForeignKey("addresses.id")),
            Column("data", String(30)),
            test_needs_acid=True,
            test_needs_fk=True,
        )

    def _option_test_fixture(self):
        users, addresses, dingalings = (
            self.tables.users,
            self.tables.addresses,
            self.tables.dingalings,
        )

        # these must be module level for pickling
        from .test_pickled import Address
        from .test_pickled import Dingaling
        from .test_pickled import User

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={"addresses": relationship(Address, backref="user")},
        )
        self.mapper_registry.map_imperatively(
            Address,
            addresses,
            properties={"dingaling": relationship(Dingaling)},
        )
        self.mapper_registry.map_imperatively(Dingaling, dingalings)
        sess = fixture_session()
        u1 = User(name="ed")
        u1.addresses.append(Address(email_address="ed@bar.com"))
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        return sess, User, Address, Dingaling


class SynonymTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    @classmethod
    def setup_mappers(cls):
        (
            users,
            Keyword,
            items,
            order_items,
            orders,
            Item,
            User,
            Address,
            keywords,
            Order,
            item_keywords,
            addresses,
        ) = (
            cls.tables.users,
            cls.classes.Keyword,
            cls.tables.items,
            cls.tables.order_items,
            cls.tables.orders,
            cls.classes.Item,
            cls.classes.User,
            cls.classes.Address,
            cls.tables.keywords,
            cls.classes.Order,
            cls.tables.item_keywords,
            cls.tables.addresses,
        )

        cls.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "name_syn": synonym("name"),
                "addresses": relationship(Address),
                "orders": relationship(
                    Order, backref="user", order_by=orders.c.id
                ),  # o2m, m2o
                "orders_syn": synonym("orders"),
                "orders_syn_2": synonym("orders_syn"),
            },
        )
        cls.mapper_registry.map_imperatively(Address, addresses)
        cls.mapper_registry.map_imperatively(
            Order,
            orders,
            properties={
                "items": relationship(Item, secondary=order_items),  # m2m
                "address": relationship(Address),  # m2o
                "items_syn": synonym("items"),
            },
        )
        cls.mapper_registry.map_imperatively(
            Item,
            items,
            properties={
                "keywords": relationship(
                    Keyword, secondary=item_keywords
                )  # m2m
            },
        )
        cls.mapper_registry.map_imperatively(Keyword, keywords)


class MiscDeprecationsTest(fixtures.TestBase):
    def test_unloaded_expirable(self, decl_base):
        class A(decl_base):
            __tablename__ = "a"
            id = mapped_column(Integer, Identity(), primary_key=True)
            x = mapped_column(
                Integer,
            )
            y = mapped_column(Integer, deferred=True)

        decl_base.metadata.create_all(testing.db)
        with Session(testing.db) as sess:
            obj = A(x=1, y=2)
            sess.add(obj)
            sess.commit()

        with expect_deprecated(
            "The InstanceState.unloaded_expirable attribute is deprecated.  "
            "Please use InstanceState.unloaded."
        ):
            eq_(inspect(obj).unloaded, {"id", "x", "y"})
            eq_(inspect(obj).unloaded_expirable, inspect(obj).unloaded)

    def test_evaluator_is_private(self):
        with expect_deprecated(
            "Direct use of 'EvaluatorCompiler' is not supported, and this "
            "name will be removed in a future release.  "
            "'_EvaluatorCompiler' is for internal use only"
        ):
            from sqlalchemy.orm.evaluator import EvaluatorCompiler

        from sqlalchemy.orm.evaluator import _EvaluatorCompiler

        is_(EvaluatorCompiler, _EvaluatorCompiler)

    @testing.combinations(
        ("init", True),
        ("kw_only", True),
        ("default", 5),
        ("default_factory", lambda: 10),
        argnames="paramname, value",
    )
    def test_column_property_dc_attributes(self, paramname, value):
        with expect_deprecated(
            rf"The column_property.{paramname} parameter is deprecated "
            r"for column_property\(\)",
        ):
            column_property(column("q"), **{paramname: value})

    def test_column_property_dc_attributes_still_function(self, dc_decl_base):
        with expect_deprecated(
            r"The column_property.init parameter is deprecated "
            r"for column_property\(\)",
            r"The column_property.default parameter is deprecated "
            r"for column_property\(\)",
            r"The column_property.default_factory parameter is deprecated "
            r"for column_property\(\)",
            r"The column_property.kw_only parameter is deprecated "
            r"for column_property\(\)",
        ):

            class MyClass(dc_decl_base):
                __tablename__ = "a"

                id: Mapped[int] = mapped_column(primary_key=True, init=False)
                data: Mapped[str] = mapped_column()

                const1: Mapped[str] = column_property(
                    data + "asdf", init=True, default="foobar"
                )
                const2: Mapped[str] = column_property(
                    data + "asdf",
                    init=True,
                    default_factory=lambda: "factory_foo",
                )
                const3: Mapped[str] = column_property(
                    data + "asdf", init=True, kw_only=True
                )

            m1 = MyClass(data="d1", const3="c3")
            eq_(m1.const1, "foobar")
            eq_(m1.const2, "factory_foo")
            eq_(m1.const3, "c3")

        with expect_raises_message(
            TypeError, "missing 1 required keyword-only argument: 'const3'"
        ):
            MyClass(data="d1")


class DeprecatedQueryTest(_fixtures.FixtureTest, AssertsCompiledSQL):
    __dialect__ = "default"

    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()

    def test_invalid_column(self):
        User = self.classes.User

        s = fixture_session()
        q = s.query(User.id)

        with testing.expect_deprecated(r"Query.add_column\(\) is deprecated"):
            q = q.add_column(User.name)

        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name FROM users",
        )

    def test_text_as_column(self):
        User = self.classes.User

        s = fixture_session()

        # TODO: this works as of "use rowproxy for ORM keyed tuple"
        # Ieb9085e9bcff564359095b754da9ae0af55679f0
        # but im not sure how this relates to things here
        q = s.query(User.id, text("users.name"))
        self.assert_compile(
            q, "SELECT users.id AS users_id, users.name FROM users"
        )
        eq_(q.all(), [(7, "jack"), (8, "ed"), (9, "fred"), (10, "chuck")])

        # same here, this was "passing string names to Query.columns"
        # deprecation message, that's gone here?
        assert_raises_message(
            sa.exc.ArgumentError,
            "Textual column expression 'name' should be explicitly",
            s.query,
            User.id,
            "name",
        )

    def test_query_as_scalar(self):
        User = self.classes.User

        s = fixture_session()
        with assertions.expect_deprecated(
            r"The Query.as_scalar\(\) method is deprecated and will "
            "be removed in a future release."
        ):
            s.query(User).as_scalar()

    def test_apply_labels(self):
        User = self.classes.User

        with testing.expect_deprecated_20(
            r"The Query.with_labels\(\) and Query.apply_labels\(\) "
            "method is considered legacy"
        ):
            q = fixture_session().query(User).apply_labels().statement

        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name FROM users",
        )

    def test_with_labels(self):
        User = self.classes.User

        with testing.expect_deprecated_20(
            r"The Query.with_labels\(\) and Query.apply_labels\(\) "
            "method is considered legacy"
        ):
            q = fixture_session().query(User).with_labels().statement

        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name FROM users",
        )


class LazyLoadOptSpecificityTest(fixtures.DeclarativeMappedTest):
    """test for [ticket:3963]"""

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class A(Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)
            bs = relationship("B")

        class B(Base):
            __tablename__ = "b"
            id = Column(Integer, primary_key=True)
            a_id = Column(ForeignKey("a.id"))
            cs = relationship("C")

        class C(Base):
            __tablename__ = "c"
            id = Column(Integer, primary_key=True)
            b_id = Column(ForeignKey("b.id"))

    @classmethod
    def insert_data(cls, connection):
        A, B, C = cls.classes("A", "B", "C")
        s = Session(connection)
        s.add(A(id=1, bs=[B(cs=[C()])]))
        s.add(A(id=2))
        s.commit()

    def _run_tests(self, query, expected):
        def go():
            for a, _ in query:
                for b in a.bs:
                    b.cs

        self.assert_sql_count(testing.db, go, expected)


class DeprecatedMapperTest(
    fixtures.RemovesEvents, _fixtures.FixtureTest, AssertsCompiledSQL
):
    __dialect__ = "default"

    def test_listen_on_mapper_mapper_event_fn(self, registry):
        from sqlalchemy.orm import mapper

        m1 = Mock()

        with expect_deprecated(
            r"The `sqlalchemy.orm.mapper\(\)` symbol is deprecated and "
            "will be removed"
        ):

            @event.listens_for(mapper, "before_configured")
            def go():
                m1()

        @registry.mapped
        class MyClass:
            __tablename__ = "t1"
            id = Column(Integer, primary_key=True)

        registry.configure()
        eq_(m1.mock_calls, [call()])

    def test_listen_on_mapper_instrumentation_event_fn(self, registry):
        from sqlalchemy.orm import mapper

        m1 = Mock()

        with expect_deprecated(
            r"The `sqlalchemy.orm.mapper\(\)` symbol is deprecated and "
            "will be removed"
        ):

            @event.listens_for(mapper, "init")
            def go(target, args, kwargs):
                m1(target, args, kwargs)

        @registry.mapped
        class MyClass:
            __tablename__ = "t1"
            id = Column(Integer, primary_key=True)

        mc = MyClass(id=5)
        eq_(m1.mock_calls, [call(mc, (), {"id": 5})])

    def test_we_couldnt_remove_mapper_yet(self):
        """test that the mapper() function is present but raises an
        informative error when used.

        The function itself was to be removed as of 2.0, however we forgot
        to mark deprecated the use of the function as an event target,
        so it needs to stay around for another cycle at least.

        """

        class MyClass:
            pass

        t1 = Table("t1", MetaData(), Column("id", Integer, primary_key=True))

        from sqlalchemy.orm import mapper

        with assertions.expect_raises_message(
            sa_exc.InvalidRequestError,
            r"The 'sqlalchemy.orm.mapper\(\)' function is removed as of "
            "SQLAlchemy 2.0.",
        ):
            mapper(MyClass, t1)

    def test_deferred_scalar_loader_name_change(self):
        class Foo:
            pass

        def myloader(*arg, **kw):
            pass

        instrumentation.register_class(Foo)
        manager = instrumentation.manager_of_class(Foo)

        with testing.expect_deprecated(
            "The ClassManager.deferred_scalar_loader attribute is now named "
            "expired_attribute_loader"
        ):
            manager.deferred_scalar_loader = myloader

        is_(manager.expired_attribute_loader, myloader)

        with testing.expect_deprecated(
            "The ClassManager.deferred_scalar_loader attribute is now named "
            "expired_attribute_loader"
        ):
            is_(manager.deferred_scalar_loader, myloader)

    def test_comparable_column(self):
        users, User = self.tables.users, self.classes.User

        class MyComparator(sa.orm.properties.ColumnProperty.Comparator):
            __hash__ = None

            def __eq__(self, other):
                # lower case comparison
                return func.lower(self.__clause_element__()) == func.lower(
                    other
                )

            def intersects(self, other):
                # non-standard comparator
                return self.__clause_element__().op("&=")(other)

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "name": sa.orm.column_property(
                    users.c.name, comparator_factory=MyComparator
                )
            },
        )

        assert_raises_message(
            AttributeError,
            "Neither 'InstrumentedAttribute' object nor "
            "'MyComparator' object associated with User.name has "
            "an attribute 'nonexistent'",
            getattr,
            User.name,
            "nonexistent",
        )

        eq_(
            str(
                (User.name == "ed").compile(
                    dialect=sa.engine.default.DefaultDialect()
                )
            ),
            "lower(users.name) = lower(:lower_1)",
        )
        eq_(
            str(
                (User.name.intersects("ed")).compile(
                    dialect=sa.engine.default.DefaultDialect()
                )
            ),
            "users.name &= :name_1",
        )

    def test_add_property(self):
        users = self.tables.users

        assert_col = []

        class User(ComparableEntity):
            def _get_name(self):
                assert_col.append(("get", self._name))
                return self._name

            def _set_name(self, name):
                assert_col.append(("set", name))
                self._name = name

            name = property(_get_name, _set_name)

        m = self.mapper_registry.map_imperatively(User, users)

        m.add_property("_name", deferred(users.c.name))
        m.add_property("name", synonym("_name"))

        sess = fixture_session()
        assert sess.get(User, 7)

        u = sess.query(User).filter_by(name="jack").one()

        def go():
            eq_(u.name, "jack")
            eq_(assert_col, [("get", "jack")], str(assert_col))

        self.sql_count_(1, go)

    @testing.variation("prop_type", ["relationship", "col_prop"])
    def test_prop_replacement_warns(self, prop_type: testing.Variation):
        users, User = self.tables.users, self.classes.User
        addresses, Address = self.tables.addresses, self.classes.Address

        m = self.mapper(
            User,
            users,
            properties={
                "foo": column_property(users.c.name),
                "addresses": relationship(Address),
            },
        )
        self.mapper(Address, addresses)

        if prop_type.relationship:
            key = "addresses"
            new_prop = relationship(Address)
        elif prop_type.col_prop:
            key = "foo"
            new_prop = column_property(users.c.name)
        else:
            prop_type.fail()

        with expect_deprecated(
            f"Property User.{key} on Mapper|User|users being replaced "
            f"with new property User.{key}; the old property will "
            "be discarded",
        ):
            m.add_property(key, new_prop)


class ViewonlyFlagWarningTest(fixtures.MappedTest):
    """test for #4993.

    In 1.4, this moves to test/orm/test_cascade, deprecation warnings
    become errors, will then be for #4994.

    """

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(30)),
        )
        Table(
            "orders",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("user_id", Integer),
            Column("description", String(30)),
        )

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            pass

        class Order(cls.Comparable):
            pass

    @testing.combinations(
        ("passive_deletes", True),
        ("passive_updates", False),
        ("enable_typechecks", False),
        ("active_history", True),
    )
    def test_viewonly_warning(self, flag, value):
        Order = self.classes.Order

        with testing.expect_warnings(
            r"Setting %s on relationship\(\) while also setting "
            "viewonly=True does not make sense" % flag
        ):
            kw = {
                "viewonly": True,
                "primaryjoin": self.tables.users.c.id
                == foreign(self.tables.orders.c.user_id),
            }
            kw[flag] = value
            rel = relationship(Order, **kw)

            eq_(getattr(rel, flag), value)


class InstancesTest(QueryTest, AssertsCompiledSQL):
    @testing.fails(
        "ORM refactor not allowing this yet, "
        "we may just abandon this use case"
    )
    def test_from_alias_one(self):
        User, addresses, users = (
            self.classes.User,
            self.tables.addresses,
            self.tables.users,
        )

        query = (
            users.select(users.c.id == 7)
            .union(users.select(users.c.id > 7))
            .alias("ulist")
            .outerjoin(addresses)
            .select(order_by=[text("ulist.id"), addresses.c.id])
        )
        sess = fixture_session()
        q = sess.query(User)

        # note this has multiple problems because we aren't giving Query
        # the statement where it would be able to create an adapter
        def go():
            with testing.expect_deprecated(
                r"Using the Query.instances\(\) method without a context",
                "Retrieving row values using Column objects with only "
                "matching names",
            ):
                result = list(
                    q.options(
                        contains_alias("ulist"), contains_eager("addresses")
                    ).instances(query.execute())
                )
            assert self.static.user_address_result == result

        self.assert_sql_count(testing.db, go, 1)

    def test_from_alias_two_old_way(self):
        User, addresses, users = (
            self.classes.User,
            self.tables.addresses,
            self.tables.users,
        )

        query = (
            users.select()
            .where(users.c.id == 7)
            .union(users.select().where(users.c.id > 7))
            .alias("ulist")
            .outerjoin(addresses)
            .select()
            .order_by(text("ulist.id"), addresses.c.id)
        )
        sess = fixture_session()
        q = sess.query(User)

        def go():
            with testing.expect_deprecated(
                "The AliasOption object is not necessary for entities to be "
                "matched up to a query",
            ):
                result = (
                    q.options(
                        contains_alias("ulist"), contains_eager(User.addresses)
                    )
                    .from_statement(query)
                    .all()
                )
            assert self.static.user_address_result == result

        self.assert_sql_count(testing.db, go, 1)

    def test_contains_eager(self):
        users, addresses, User = (
            self.tables.users,
            self.tables.addresses,
            self.classes.User,
        )

        sess = fixture_session()

        selectquery = (
            users.outerjoin(addresses)
            .select()
            .where(users.c.id < 10)
            .order_by(users.c.id, addresses.c.id)
        )
        q = sess.query(User)

        def go():
            with testing.expect_deprecated(
                r"The Query.instances\(\) method is deprecated",
                r"Using the Query.instances\(\) method without a context",
            ):
                result = list(
                    q.options(contains_eager(User.addresses)).instances(
                        sess.execute(selectquery)
                    )
                )
            assert self.static.user_address_result[0:3] == result

        self.assert_sql_count(testing.db, go, 1)

        sess.expunge_all()

        def go():
            with testing.expect_deprecated(
                r"The Query.instances\(\) method is deprecated",
                r"Using the Query.instances\(\) method without a context",
            ):
                result = list(
                    q.options(contains_eager(User.addresses)).instances(
                        sess.connection().execute(selectquery)
                    )
                )
            assert self.static.user_address_result[0:3] == result

        self.assert_sql_count(testing.db, go, 1)

    def test_contains_eager_aliased_instances(self):
        addresses, users, User = (
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        sess = fixture_session()
        q = sess.query(User)

        adalias = addresses.alias("adalias")
        selectquery = (
            users.outerjoin(adalias)
            .select()
            .order_by(users.c.id, adalias.c.id)
        )

        # note this has multiple problems because we aren't giving Query
        # the statement where it would be able to create an adapter
        def go():
            with testing.expect_deprecated(
                r"Using the Query.instances\(\) method without a context",
                r"The Query.instances\(\) method is deprecated and will be "
                r"removed in a future release.",
            ):
                result = list(
                    q.options(
                        contains_eager(User.addresses, alias=adalias)
                    ).instances(sess.connection().execute(selectquery))
                )
            assert self.static.user_address_result == result

        self.assert_sql_count(testing.db, go, 1)

    def test_contains_eager_multi_alias(self):
        orders, items, users, order_items, User = (
            self.tables.orders,
            self.tables.items,
            self.tables.users,
            self.tables.order_items,
            self.classes.User,
        )

        Order = self.classes.Order

        sess = fixture_session()
        q = sess.query(User)

        oalias = orders.alias("o1")
        ialias = items.alias("i1")
        query = (
            users.outerjoin(oalias)
            .outerjoin(order_items)
            .outerjoin(ialias)
            .select()
            .order_by(users.c.id, oalias.c.id, ialias.c.id)
        )

        # test using Alias with more than one level deep

        # new way:
        # from sqlalchemy.orm.strategy_options import Load
        # opt = Load(User).contains_eager('orders', alias=oalias).
        #     contains_eager('items', alias=ialias)

        def go():
            with testing.expect_deprecated(
                r"Using the Query.instances\(\) method without a context",
                r"The Query.instances\(\) method is deprecated and will be "
                r"removed in a future release.",
            ):
                result = list(
                    q.options(
                        contains_eager(User.orders, alias=oalias),
                        defaultload(User.orders).contains_eager(
                            Order.items, alias=ialias
                        ),
                    ).instances(sess.connection().execute(query))
                )
            assert self.static.user_order_result == result

        self.assert_sql_count(testing.db, go, 1)


class ImmediateTest(_fixtures.FixtureTest):
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        Address, addresses, users, User = (
            cls.classes.Address,
            cls.tables.addresses,
            cls.tables.users,
            cls.classes.User,
        )

        cls.mapper_registry.map_imperatively(Address, addresses)

        cls.mapper_registry.map_imperatively(
            User, users, properties=dict(addresses=relationship(Address))
        )

    def test_value(self):
        User = self.classes.User

        sess = fixture_session()

        with testing.expect_deprecated(r"Query.value\(\) is deprecated"):
            eq_(sess.query(User).filter_by(id=7).value(User.id), 7)
        with testing.expect_deprecated(r"Query.value\(\) is deprecated"):
            eq_(
                sess.query(User.id, User.name).filter_by(id=7).value(User.id),
                7,
            )
        with testing.expect_deprecated(r"Query.value\(\) is deprecated"):
            eq_(sess.query(User).filter_by(id=0).value(User.id), None)

        sess.bind = testing.db
        with testing.expect_deprecated(r"Query.value\(\) is deprecated"):
            eq_(sess.query().value(sa.literal_column("1").label("x")), 1)

    def test_value_cancels_loader_opts(self):
        User = self.classes.User

        sess = fixture_session()

        q = (
            sess.query(User)
            .filter(User.name == "ed")
            .options(joinedload(User.addresses))
        )

        with testing.expect_deprecated(r"Query.value\(\) is deprecated"):
            q = q.value(func.count(literal_column("*")))


class MixedEntitiesTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_values(self):
        Address, User = (
            self.classes.Address,
            self.classes.User,
        )

        sess = fixture_session()

        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            assert list(sess.query(User).values()) == list()

        q = sess.query(User)

        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            q2 = q.order_by(User.id).values(
                User.name, User.name + " " + cast(User.id, String(50))
            )
        eq_(
            list(q2),
            [
                ("jack", "jack 7"),
                ("ed", "ed 8"),
                ("fred", "fred 9"),
                ("chuck", "chuck 10"),
            ],
        )

        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            q2 = (
                q.join(User.addresses)
                .filter(User.name.like("%e%"))
                .order_by(User.id, Address.id)
                .values(User.name, Address.email_address)
            )
        eq_(
            list(q2),
            [
                ("ed", "ed@wood.com"),
                ("ed", "ed@bettyboop.com"),
                ("ed", "ed@lala.com"),
                ("fred", "fred@fred.com"),
            ],
        )

        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            q2 = (
                q.join(User.addresses)
                .filter(User.name.like("%e%"))
                .order_by(desc(Address.email_address))
                .slice(1, 3)
                .values(User.name, Address.email_address)
            )
        eq_(list(q2), [("ed", "ed@wood.com"), ("ed", "ed@lala.com")])

        adalias = aliased(Address)
        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            q2 = (
                q.join(adalias, User.addresses)
                .filter(User.name.like("%e%"))
                .order_by(adalias.email_address)
                .values(User.name, adalias.email_address)
            )
        eq_(
            list(q2),
            [
                ("ed", "ed@bettyboop.com"),
                ("ed", "ed@lala.com"),
                ("ed", "ed@wood.com"),
                ("fred", "fred@fred.com"),
            ],
        )

        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            q2 = q.values(func.count(User.name))
        assert next(q2) == (4,)

    def test_values_specific_order_by(self):
        User = self.classes.User

        sess = fixture_session()

        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            assert list(sess.query(User).values()) == list()

    @testing.fails_on("mssql", "FIXME: unknown")
    @testing.fails_on(
        "oracle", "Oracle doesn't support boolean expressions as columns"
    )
    @testing.fails_on(
        "postgresql+pg8000",
        "pg8000 parses the SQL itself before passing on "
        "to PG, doesn't parse this",
    )
    @testing.fails_on(
        "postgresql+asyncpg",
        "Asyncpg uses preprated statements that are not compatible with how "
        "sqlalchemy passes the query. Fails with "
        'ERROR:  column "users.name" must appear in the GROUP BY clause'
        " or be used in an aggregate function",
    )
    def test_values_with_boolean_selects(self):
        """Tests a values clause that works with select boolean
        evaluations"""

        User = self.classes.User

        sess = fixture_session()

        q = sess.query(User)
        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            q2 = (
                q.group_by(User.name.like("%j%"))
                .order_by(desc(User.name.like("%j%")))
                .values(
                    User.name.like("%j%"), func.count(User.name.like("%j%"))
                )
            )
        eq_(list(q2), [(True, 1), (False, 3)])

        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            q2 = q.order_by(desc(User.name.like("%j%"))).values(
                User.name.like("%j%")
            )
        eq_(list(q2), [(True,), (False,), (False,), (False,)])


class InheritedJoinTest(
    fixtures.NoCache,
    _poly_fixtures._Polymorphic,
    _poly_fixtures._PolymorphicFixtureBase,
    AssertsCompiledSQL,
):
    run_setup_mappers = "once"
    __dialect__ = "default"

    def test_join_w_subq_adapt(self):
        """test #8162"""

        Company, Manager, Engineer = self.classes(
            "Company", "Manager", "Engineer"
        )

        sess = fixture_session()

        with _aliased_join_warning():
            self.assert_compile(
                sess.query(Engineer)
                .join(Company, Company.company_id == Engineer.company_id)
                .outerjoin(Manager, Company.company_id == Manager.company_id)
                .filter(~Engineer.company.has()),
                "SELECT engineers.person_id AS engineers_person_id, "
                "people.person_id AS people_person_id, "
                "people.company_id AS people_company_id, "
                "people.name AS people_name, people.type AS people_type, "
                "engineers.status AS engineers_status, "
                "engineers.engineer_name AS engineers_engineer_name, "
                "engineers.primary_language AS engineers_primary_language "
                "FROM people JOIN engineers "
                "ON people.person_id = engineers.person_id "
                "JOIN companies ON companies.company_id = people.company_id "
                "LEFT OUTER JOIN (people AS people_1 JOIN managers AS "
                "managers_1 ON people_1.person_id = managers_1.person_id) "
                "ON companies.company_id = people_1.company_id "
                "WHERE NOT (EXISTS (SELECT 1 FROM companies "
                "WHERE companies.company_id = people.company_id))",
                use_default_dialect=True,
            )

    def test_join_to_selectable(self):
        people, Company, engineers, Engineer = (
            self.tables.people,
            self.classes.Company,
            self.tables.engineers,
            self.classes.Engineer,
        )

        sess = fixture_session()

        with _aliased_join_deprecation():
            self.assert_compile(
                sess.query(Company)
                .join(people.join(engineers), Company.employees)
                .filter(Engineer.name == "dilbert"),
                "SELECT companies.company_id AS companies_company_id, "
                "companies.name AS companies_name "
                "FROM companies JOIN (people "
                "JOIN engineers ON people.person_id = "
                "engineers.person_id) ON companies.company_id = "
                "people.company_id WHERE people.name = :name_1",
                use_default_dialect=True,
            )

    def test_join_to_subclass_selectable_auto_alias(self):
        Company, Engineer = self.classes("Company", "Engineer")
        people, engineers = self.tables("people", "engineers")

        sess = fixture_session()

        with _aliased_join_deprecation():
            eq_(
                sess.query(Company)
                .join(people.join(engineers), Company.employees)
                .filter(Engineer.primary_language == "java")
                .all(),
                [self.c1],
            )

        # occurs for 2.0 style query also
        with _aliased_join_deprecation():
            stmt = (
                select(Company)
                .join(people.join(engineers), Company.employees)
                .filter(Engineer.primary_language == "java")
            )
            results = sess.scalars(stmt)
        eq_(results.all(), [self.c1])

    def test_join_to_subclass_two(self):
        Company, Engineer = self.classes("Company", "Engineer")
        people, engineers = self.tables("people", "engineers")

        sess = fixture_session()

        with _aliased_join_deprecation():
            eq_(
                sess.query(Company)
                .join(people.join(engineers), Company.employees)
                .filter(Engineer.primary_language == "java")
                .all(),
                [self.c1],
            )

    def test_join_to_subclass_six_selectable_auto_alias(self):
        Company, Engineer = self.classes("Company", "Engineer")
        people, engineers = self.tables("people", "engineers")

        sess = fixture_session()

        with _aliased_join_deprecation():
            eq_(
                sess.query(Company)
                .join(people.join(engineers), Company.employees)
                .join(Engineer.machines)
                .all(),
                [self.c1, self.c2],
            )

    def test_join_to_subclass_six_point_five_selectable_auto_alias(self):
        Company, Engineer = self.classes("Company", "Engineer")
        people, engineers = self.tables("people", "engineers")

        sess = fixture_session()

        with _aliased_join_deprecation():
            eq_(
                sess.query(Company)
                .join(people.join(engineers), Company.employees)
                .join(Engineer.machines)
                .filter(Engineer.name == "dilbert")
                .all(),
                [self.c1],
            )

    def test_join_to_subclass_seven_selectable_auto_alias(self):
        Company, Engineer, Machine = self.classes(
            "Company", "Engineer", "Machine"
        )
        people, engineers = self.tables("people", "engineers")

        sess = fixture_session()

        with _aliased_join_deprecation():
            eq_(
                sess.query(Company)
                .join(people.join(engineers), Company.employees)
                .join(Engineer.machines)
                .filter(Machine.name.ilike("%thinkpad%"))
                .all(),
                [self.c1],
            )


class MultiplePathTest(fixtures.MappedTest, AssertsCompiledSQL):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "t1",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(30)),
        )
        Table(
            "t2",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(30)),
        )

        Table(
            "t1t2_1",
            metadata,
            Column("t1id", Integer, ForeignKey("t1.id")),
            Column("t2id", Integer, ForeignKey("t2.id")),
        )

        Table(
            "t1t2_2",
            metadata,
            Column("t1id", Integer, ForeignKey("t1.id")),
            Column("t2id", Integer, ForeignKey("t2.id")),
        )


class BindSensitiveStringifyTest(fixtures.MappedTest):
    def _fixture(self):
        # building a totally separate metadata /mapping here
        # because we need to control if the MetaData is bound or not

        class User:
            pass

        m = MetaData()
        user_table = Table(
            "users",
            m,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
        )

        clear_mappers()
        self.mapper_registry.map_imperatively(User, user_table)
        return User

    def _dialect_fixture(self):
        class MyDialect(default.DefaultDialect):
            default_paramstyle = "qmark"

        from sqlalchemy.engine import base

        return base.Engine(mock.Mock(), MyDialect(), mock.Mock())

    def _test(self, bound_session, session_present, expect_bound):
        if bound_session:
            eng = self._dialect_fixture()
        else:
            eng = None

        User = self._fixture()

        s = Session(eng if bound_session else None)
        q = s.query(User).filter(User.id == 7)
        if not session_present:
            q = q.with_session(None)

        eq_ignore_whitespace(
            str(q),
            (
                "SELECT users.id AS users_id, users.name AS users_name "
                "FROM users WHERE users.id = ?"
                if expect_bound
                else "SELECT users.id AS users_id, users.name AS users_name "
                "FROM users WHERE users.id = :id_1"
            ),
        )

    def test_query_bound_session(self):
        self._test(True, True, True)

    def test_query_no_session(self):
        self._test(False, False, False)

    def test_query_unbound_session(self):
        self._test(False, True, False)


class DeprecationScopedSessionTest(fixtures.MappedTest):
    def test_config_errors(self):
        sm = sessionmaker()

        def go():
            s = sm()
            s._is_asyncio = True
            return s

        Session = scoped_session(go)

        with expect_deprecated(
            "Using `scoped_session` with asyncio is deprecated and "
            "will raise an error in a future version. "
            "Please use `async_scoped_session` instead."
        ):
            Session()
        Session.remove()


class RequirementsTest(fixtures.MappedTest):
    """Tests the contract for user classes."""

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "ht1",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("value", String(10)),
        )
        Table(
            "ht2",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("ht1_id", Integer, ForeignKey("ht1.id")),
            Column("value", String(10)),
        )
        Table(
            "ht3",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("value", String(10)),
        )
        Table(
            "ht4",
            metadata,
            Column("ht1_id", Integer, ForeignKey("ht1.id"), primary_key=True),
            Column("ht3_id", Integer, ForeignKey("ht3.id"), primary_key=True),
        )
        Table(
            "ht5",
            metadata,
            Column("ht1_id", Integer, ForeignKey("ht1.id"), primary_key=True),
        )
        Table(
            "ht6",
            metadata,
            Column("ht1a_id", Integer, ForeignKey("ht1.id"), primary_key=True),
            Column("ht1b_id", Integer, ForeignKey("ht1.id"), primary_key=True),
            Column("value", String(10)),
        )


class SubOptionsTest(PathTest, OptionsQueryTest):
    run_create_tables = False
    run_inserts = None
    run_deletes = None

    def _assert_opts(self, q, sub_opt, non_sub_opts):
        attr_a = {}

        for val in sub_opt._to_bind:
            val._bind_loader(
                [
                    ent.entity_zero
                    for ent in q._compile_state()._lead_mapper_entities
                ],
                q._compile_options._current_path,
                attr_a,
                False,
            )

        attr_b = {}

        for opt in non_sub_opts:
            for val in opt._to_bind:
                val._bind_loader(
                    [
                        ent.entity_zero
                        for ent in q._compile_state()._lead_mapper_entities
                    ],
                    q._compile_options._current_path,
                    attr_b,
                    False,
                )

        for k, l in attr_b.items():
            if not l.strategy:
                del attr_b[k]

        def strat_as_tuple(strat):
            return (
                strat.strategy,
                strat.local_opts,
                strat.propagate_to_loaders,
                strat._of_type,
                strat.is_class_strategy,
                strat.is_opts_only,
            )

        eq_(
            {path: strat_as_tuple(load) for path, load in attr_a.items()},
            {path: strat_as_tuple(load) for path, load in attr_b.items()},
        )


class PolyCacheKeyTest(CacheKeyFixture, _poly_fixtures._Polymorphic):
    run_setup_mappers = "once"
    run_inserts = None
    run_deletes = None

    def _stmt_20(self, *elements):
        return tuple(
            elem._statement_20() if isinstance(elem, sa.orm.Query) else elem
            for elem in elements
        )

    def test_wp_queries(self):
        Person, Manager, Engineer, Boss = self.classes(
            "Person", "Manager", "Engineer", "Boss"
        )

        def two():
            wp = with_polymorphic(Person, [Manager, Engineer])

            return fixture_session().query(wp)

        def three():
            wp = with_polymorphic(Person, [Manager, Engineer])

            return fixture_session().query(wp).filter(wp.name == "asdfo")

        def three_a():
            wp = with_polymorphic(Person, [Manager, Engineer], flat=True)

            return fixture_session().query(wp).filter(wp.name == "asdfo")

        def five():
            subq = (
                select(Person)
                .outerjoin(Manager)
                .outerjoin(Engineer)
                .subquery()
            )
            wp = with_polymorphic(Person, [Manager, Engineer], subq)

            return fixture_session().query(wp).filter(wp.name == "asdfo")

        self._run_cache_key_fixture(
            lambda: self._stmt_20(two(), three(), three_a(), five()),
            compare_values=True,
        )


class ParentTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_o2m(self):
        User, orders, Order = (
            self.classes.User,
            self.tables.orders,
            self.classes.Order,
        )

        sess = fixture_session()
        q = sess.query(User)

        u1 = q.filter_by(name="jack").one()

        # test auto-lookup of property
        with assertions.expect_deprecated_20(query_wparent_dep):
            o = sess.query(Order).with_parent(u1).all()
        assert [
            Order(description="order 1"),
            Order(description="order 3"),
            Order(description="order 5"),
        ] == o

        # test with explicit property
        with assertions.expect_deprecated_20(query_wparent_dep):
            o = sess.query(Order).with_parent(u1, property=User.orders).all()
        assert [
            Order(description="order 1"),
            Order(description="order 3"),
            Order(description="order 5"),
        ] == o

        with assertions.expect_deprecated_20(query_wparent_dep):
            # test generative criterion
            o = sess.query(Order).with_parent(u1).filter(orders.c.id > 2).all()
        assert [
            Order(description="order 3"),
            Order(description="order 5"),
        ] == o

    def test_select_from(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        u1 = sess.get(User, 7)
        with assertions.expect_deprecated_20(query_wparent_dep):
            q = sess.query(Address).select_from(Address).with_parent(u1)
        self.assert_compile(
            q,
            "SELECT addresses.id AS addresses_id, "
            "addresses.user_id AS addresses_user_id, "
            "addresses.email_address AS addresses_email_address "
            "FROM addresses WHERE :param_1 = addresses.user_id",
            {"param_1": 7},
        )

    def test_from_entity_query_entity(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        u1 = sess.get(User, 7)
        with assertions.expect_deprecated_20(query_wparent_dep):
            q = sess.query(User, Address).with_parent(
                u1, User.addresses, from_entity=Address
            )
        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name, "
            "addresses.id AS addresses_id, addresses.user_id "
            "AS addresses_user_id, "
            "addresses.email_address AS addresses_email_address "
            "FROM users, addresses "
            "WHERE :param_1 = addresses.user_id",
            {"param_1": 7},
        )

    def test_select_from_alias(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        u1 = sess.get(User, 7)
        a1 = aliased(Address)
        with assertions.expect_deprecated_20(query_wparent_dep):
            q = sess.query(a1).with_parent(u1)
        self.assert_compile(
            q,
            "SELECT addresses_1.id AS addresses_1_id, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS addresses_1_email_address "
            "FROM addresses AS addresses_1 "
            "WHERE :param_1 = addresses_1.user_id",
            {"param_1": 7},
        )

    def test_select_from_alias_explicit_prop(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        u1 = sess.get(User, 7)
        a1 = aliased(Address)
        with assertions.expect_deprecated_20(query_wparent_dep):
            q = sess.query(a1).with_parent(u1, User.addresses)
        self.assert_compile(
            q,
            "SELECT addresses_1.id AS addresses_1_id, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS addresses_1_email_address "
            "FROM addresses AS addresses_1 "
            "WHERE :param_1 = addresses_1.user_id",
            {"param_1": 7},
        )

    def test_select_from_alias_from_entity(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        u1 = sess.get(User, 7)
        a1 = aliased(Address)
        a2 = aliased(Address)
        with assertions.expect_deprecated_20(query_wparent_dep):
            q = sess.query(a1, a2).with_parent(
                u1, User.addresses, from_entity=a2
            )
        self.assert_compile(
            q,
            "SELECT addresses_1.id AS addresses_1_id, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS addresses_1_email_address, "
            "addresses_2.id AS addresses_2_id, "
            "addresses_2.user_id AS addresses_2_user_id, "
            "addresses_2.email_address AS addresses_2_email_address "
            "FROM addresses AS addresses_1, "
            "addresses AS addresses_2 WHERE :param_1 = addresses_2.user_id",
            {"param_1": 7},
        )

    def test_select_from_alias_of_type(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        u1 = sess.get(User, 7)
        a1 = aliased(Address)
        a2 = aliased(Address)
        with assertions.expect_deprecated_20(query_wparent_dep):
            q = sess.query(a1, a2).with_parent(u1, User.addresses.of_type(a2))
        self.assert_compile(
            q,
            "SELECT addresses_1.id AS addresses_1_id, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS addresses_1_email_address, "
            "addresses_2.id AS addresses_2_id, "
            "addresses_2.user_id AS addresses_2_user_id, "
            "addresses_2.email_address AS addresses_2_email_address "
            "FROM addresses AS addresses_1, "
            "addresses AS addresses_2 WHERE :param_1 = addresses_2.user_id",
            {"param_1": 7},
        )

    def test_noparent(self):
        Item, User = self.classes.Item, self.classes.User

        sess = fixture_session()
        q = sess.query(User)

        u1 = q.filter_by(name="jack").one()

        with assertions.expect_deprecated_20(query_wparent_dep):
            with assertions.expect_raises_message(
                sa_exc.InvalidRequestError,
                "Could not locate a property which relates "
                "instances of class 'Item' to instances of class 'User'",
            ):
                q = sess.query(Item).with_parent(u1)

    def test_m2m(self):
        Item, Keyword = self.classes.Item, self.classes.Keyword

        sess = fixture_session()
        i1 = sess.query(Item).filter_by(id=2).one()
        with assertions.expect_deprecated_20(query_wparent_dep):
            k = sess.query(Keyword).with_parent(i1).all()
        assert [
            Keyword(name="red"),
            Keyword(name="small"),
            Keyword(name="square"),
        ] == k

    def test_with_transient(self):
        User, Order = self.classes.User, self.classes.Order

        sess = fixture_session()

        q = sess.query(User)
        u1 = q.filter_by(name="jack").one()
        utrans = User(id=u1.id)
        with assertions.expect_deprecated_20(query_wparent_dep):
            o = sess.query(Order).with_parent(utrans, User.orders)
        eq_(
            [
                Order(description="order 1"),
                Order(description="order 3"),
                Order(description="order 5"),
            ],
            o.all(),
        )

    def test_with_pending_autoflush(self):
        Order, User = self.classes.Order, self.classes.User

        sess = fixture_session()

        o1 = sess.query(Order).first()
        opending = Order(id=20, user_id=o1.user_id)
        sess.add(opending)
        with assertions.expect_deprecated_20(query_wparent_dep):
            eq_(
                sess.query(User).with_parent(opending, Order.user).one(),
                User(id=o1.user_id),
            )

    def test_with_pending_no_autoflush(self):
        Order, User = self.classes.Order, self.classes.User

        sess = fixture_session(autoflush=False)

        o1 = sess.query(Order).first()
        opending = Order(user_id=o1.user_id)
        sess.add(opending)
        with assertions.expect_deprecated_20(query_wparent_dep):
            eq_(
                sess.query(User).with_parent(opending, Order.user).one(),
                User(id=o1.user_id),
            )

    def test_unique_binds_union(self):
        """bindparams used in the 'parent' query are unique"""
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        u1, u2 = sess.query(User).order_by(User.id)[0:2]

        with assertions.expect_deprecated_20(query_wparent_dep):
            q1 = sess.query(Address).with_parent(u1, User.addresses)
        with assertions.expect_deprecated_20(query_wparent_dep):
            q2 = sess.query(Address).with_parent(u2, User.addresses)

        self.assert_compile(
            q1.union(q2),
            "SELECT anon_1.addresses_id AS anon_1_addresses_id, "
            "anon_1.addresses_user_id AS anon_1_addresses_user_id, "
            "anon_1.addresses_email_address AS "
            "anon_1_addresses_email_address FROM (SELECT addresses.id AS "
            "addresses_id, addresses.user_id AS addresses_user_id, "
            "addresses.email_address AS addresses_email_address FROM "
            "addresses WHERE :param_1 = addresses.user_id UNION SELECT "
            "addresses.id AS addresses_id, addresses.user_id AS "
            "addresses_user_id, addresses.email_address "
            "AS addresses_email_address "
            "FROM addresses WHERE :param_2 = addresses.user_id) AS anon_1",
            checkparams={"param_1": 7, "param_2": 8},
        )


class MergeResultTest(_fixtures.FixtureTest):
    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()

    def _fixture(self):
        User = self.classes.User

        s = fixture_session()
        u1, u2, u3, u4 = (
            User(id=1, name="u1"),
            User(id=2, name="u2"),
            User(id=7, name="u3"),
            User(id=8, name="u4"),
        )
        s.query(User).filter(User.id.in_([7, 8])).all()
        s.close()
        return s, [u1, u2, u3, u4]

    def test_single_entity(self):
        s, (u1, u2, u3, u4) = self._fixture()
        User = self.classes.User

        q = s.query(User)
        collection = [u1, u2, u3, u4]

        with assertions.expect_deprecated_20(merge_result_dep):
            it = q.merge_result(collection)
        eq_([x.id for x in it], [1, 2, 7, 8])

    def test_single_column(self):
        User = self.classes.User

        s = fixture_session()

        q = s.query(User.id)
        collection = [(1,), (2,), (7,), (8,)]
        with assertions.expect_deprecated_20(merge_result_dep):
            it = q.merge_result(collection)
        eq_(list(it), [(1,), (2,), (7,), (8,)])

    def test_entity_col_mix_plain_tuple(self):
        s, (u1, u2, u3, u4) = self._fixture()
        User = self.classes.User

        q = s.query(User, User.id)
        collection = [(u1, 1), (u2, 2), (u3, 7), (u4, 8)]
        with assertions.expect_deprecated_20(merge_result_dep):
            it = q.merge_result(collection)
        it = list(it)
        eq_([(x.id, y) for x, y in it], [(1, 1), (2, 2), (7, 7), (8, 8)])
        eq_(list(it[0]._mapping.keys()), ["User", "id"])

    def test_entity_col_mix_keyed_tuple(self):
        s, (u1, u2, u3, u4) = self._fixture()
        User = self.classes.User

        q = s.query(User, User.id)

        row = result_tuple(["User", "id"])

        def kt(*x):
            return row(x)

        collection = [kt(u1, 1), kt(u2, 2), kt(u3, 7), kt(u4, 8)]
        with assertions.expect_deprecated_20(merge_result_dep):
            it = q.merge_result(collection)
        it = list(it)
        eq_([(x.id, y) for x, y in it], [(1, 1), (2, 2), (7, 7), (8, 8)])
        eq_(list(it[0]._mapping.keys()), ["User", "id"])

    def test_none_entity(self):
        s, (u1, u2, u3, u4) = self._fixture()
        User = self.classes.User

        ua = aliased(User)
        q = s.query(User, ua)

        row = result_tuple(["User", "useralias"])

        def kt(*x):
            return row(x)

        collection = [kt(u1, u2), kt(u1, None), kt(u2, u3)]
        with assertions.expect_deprecated_20(merge_result_dep):
            it = q.merge_result(collection)
        eq_(
            [(x and x.id or None, y and y.id or None) for x, y in it],
            [(u1.id, u2.id), (u1.id, None), (u2.id, u3.id)],
        )


class DefaultStrategyOptionsTest(DefaultStrategyOptionsTestFixtures):
    def test_joined_path_wildcards(self):
        sess = self._upgrade_fixture()
        users = []

        User, Order, Item = self.classes("User", "Order", "Item")

        # test upgrade all to joined: 1 sql
        def go():
            users[:] = (
                sess.query(User)
                .options(joinedload(".*"))
                .options(defaultload(User.addresses).joinedload("*"))
                .options(defaultload(User.orders).joinedload("*"))
                .options(
                    defaultload(User.orders)
                    .defaultload(Order.items)
                    .joinedload("*")
                )
                .order_by(self.classes.User.id)
                .all()
            )

        with assertions.expect_deprecated(dep_exc_wildcard):
            self.assert_sql_count(testing.db, go, 1)
            self._assert_fully_loaded(users)

    def test_subquery_path_wildcards(self):
        sess = self._upgrade_fixture()
        users = []

        User, Order = self.classes("User", "Order")

        # test upgrade all to subquery: 1 sql + 4 relationships = 5
        def go():
            users[:] = (
                sess.query(User)
                .options(subqueryload(".*"))
                .options(defaultload(User.addresses).subqueryload("*"))
                .options(defaultload(User.orders).subqueryload("*"))
                .options(
                    defaultload(User.orders)
                    .defaultload(Order.items)
                    .subqueryload("*")
                )
                .order_by(User.id)
                .all()
            )

        with assertions.expect_deprecated(dep_exc_wildcard):
            self.assert_sql_count(testing.db, go, 5)

            # verify everything loaded, with no additional sql needed
            self._assert_fully_loaded(users)

    def test_noload_with_joinedload(self):
        """Mapper load strategy defaults can be downgraded with
        noload('*') option, while explicit joinedload() option
        is still honored"""
        sess = self._downgrade_fixture()
        users = []

        # test noload('*') shuts off 'orders' subquery, only 1 sql
        def go():
            users[:] = (
                sess.query(self.classes.User)
                .options(sa.orm.noload("*"))
                .options(joinedload(self.classes.User.addresses))
                .order_by(self.classes.User.id)
                .all()
            )

        with expect_noload_deprecation():
            self.assert_sql_count(testing.db, go, 1)

        # verify all the addresses were joined loaded (no more sql)
        self._assert_addresses_loaded(users)

        # User.orders should have loaded "noload" (meaning [])
        def go():
            for u in users:
                assert u.orders == []

        self.assert_sql_count(testing.db, go, 0)

    def test_noload_with_subqueryload(self):
        """Mapper load strategy defaults can be downgraded with
        noload('*') option, while explicit subqueryload() option
        is still honored"""
        sess = self._downgrade_fixture()
        users = []

        # test noload('*') option combined with subqueryload()
        # shuts off 'addresses' load AND orders.items load: 2 sql expected
        def go():
            users[:] = (
                sess.query(self.classes.User)
                .options(sa.orm.noload("*"))
                .options(subqueryload(self.classes.User.orders))
                .order_by(self.classes.User.id)
                .all()
            )

        with expect_noload_deprecation():
            self.assert_sql_count(testing.db, go, 2)

        def go():
            # Verify orders have already been loaded: 0 sql
            for u, static in zip(users, self.static.user_all_result):
                assert len(u.orders) == len(static.orders)
            # Verify noload('*') prevented orders.items load
            # and set 'items' to []
            for u in users:
                for o in u.orders:
                    assert o.items == []

        self.assert_sql_count(testing.db, go, 0)


class Deferred_InheritanceTest(_deferred_InheritanceTest):
    def test_defer_on_wildcard_subclass(self):
        # pretty much the same as load_only except doesn't
        # exclude the primary key

        # what is ".*"?  this is not documented anywhere, how did this
        # get implemented without docs ?  see #4390
        s = fixture_session()
        with assertions.expect_deprecated(dep_exc_wildcard):
            q = (
                s.query(Manager)
                .order_by(Person.person_id)
                .options(defer(".*"), undefer(Manager.status))
            )
        self.assert_compile(
            q,
            "SELECT managers.person_id AS managers_person_id, "
            "people.person_id AS people_person_id, "
            "people.type AS people_type, managers.status AS managers_status "
            "FROM people JOIN managers ON "
            "people.person_id = managers.person_id ORDER BY people.person_id",
        )
        # note this doesn't apply to "bound" loaders since they don't seem
        # to have this ".*" feature.


class NoLoadTest(_fixtures.FixtureTest):
    run_inserts = "once"
    run_deletes = None

    def test_o2m_noload(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        m = self.mapper_registry.map_imperatively(
            User,
            users,
            properties=dict(
                addresses=relationship(
                    self.mapper_registry.map_imperatively(Address, addresses),
                    lazy="noload",
                )
            ),
        )
        q = fixture_session().query(m)
        result = [None]

        def go():
            x = q.filter(User.id == 7).all()
            x[0].addresses
            result[0] = x

        with expect_noload_deprecation():
            self.assert_sql_count(testing.db, go, 1)

        self.assert_result(
            result[0], User, {"id": 7, "addresses": (Address, [])}
        )

    def test_upgrade_o2m_noload_lazyload_option(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        m = self.mapper_registry.map_imperatively(
            User,
            users,
            properties=dict(
                addresses=relationship(
                    self.mapper_registry.map_imperatively(Address, addresses),
                    lazy="noload",
                )
            ),
        )
        with expect_noload_deprecation():
            q = (
                fixture_session()
                .query(m)
                .options(sa.orm.lazyload(User.addresses))
            )
        result = [None]

        def go():
            x = q.filter(User.id == 7).all()
            x[0].addresses
            result[0] = x

        self.sql_count_(2, go)

        self.assert_result(
            result[0], User, {"id": 7, "addresses": (Address, [{"id": 1}])}
        )

    def test_m2o_noload_option(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )
        self.mapper_registry.map_imperatively(
            Address, addresses, properties={"user": relationship(User)}
        )
        self.mapper_registry.map_imperatively(User, users)
        s = fixture_session()
        with expect_noload_deprecation():
            a1 = (
                s.query(Address)
                .filter_by(id=1)
                .options(sa.orm.noload(Address.user))
                .first()
            )

        def go():
            eq_(a1.user, None)

        self.sql_count_(0, go)


class DynamicTest(_DynamicFixture, _fixtures.FixtureTest):

    @testing.combinations(("star",), ("attronly",), argnames="type_")
    def test_noload_issue(self, type_, user_address_fixture):
        """test #6420.   a noload that hits the dynamic loader
        should have no effect.

        """

        User, Address = user_address_fixture()

        s = fixture_session()

        with expect_noload_deprecation():

            if type_ == "star":
                u1 = s.query(User).filter_by(id=7).options(noload("*")).first()
                assert "name" not in u1.__dict__["name"]
            elif type_ == "attronly":
                u1 = (
                    s.query(User)
                    .filter_by(id=7)
                    .options(noload(User.addresses))
                    .first()
                )

                eq_(u1.__dict__["name"], "jack")

        # noload doesn't affect a dynamic loader, because it has no state
        eq_(list(u1.addresses), [Address(id=1)])


class WriteOnlyTest(_WriteOnlyFixture, _fixtures.FixtureTest):

    @testing.combinations(("star",), ("attronly",), argnames="type_")
    def test_noload_issue(self, type_, user_address_fixture):
        """test #6420.   a noload that hits the dynamic loader
        should have no effect.

        """

        User, Address = user_address_fixture()

        s = fixture_session()

        with expect_noload_deprecation():

            if type_ == "star":
                u1 = s.query(User).filter_by(id=7).options(noload("*")).first()
                assert "name" not in u1.__dict__["name"]
            elif type_ == "attronly":
                u1 = (
                    s.query(User)
                    .filter_by(id=7)
                    .options(noload(User.addresses))
                    .first()
                )

                eq_(u1.__dict__["name"], "jack")


class ExpireTest(_fixtures.FixtureTest):
    def test_state_noload_to_lazy(self):
        """Behavioral test to verify the current activity of
        loader callables

        """

        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={"addresses": relationship(Address, lazy="noload")},
        )
        self.mapper_registry.map_imperatively(Address, addresses)

        sess = fixture_session(autoflush=False)
        with expect_noload_deprecation():
            u1 = sess.query(User).options(lazyload(User.addresses)).first()
        assert isinstance(
            attributes.instance_state(u1).callables["addresses"],
            strategies._LoadLazyAttribute,
        )
        # expire, it goes away from callables as of 1.4 and is considered
        # to be expired
        sess.expire(u1)

        assert "addresses" in attributes.instance_state(u1).expired_attributes
        assert "addresses" not in attributes.instance_state(u1).callables

        # load it
        sess.query(User).first()
        assert (
            "addresses" not in attributes.instance_state(u1).expired_attributes
        )
        assert "addresses" not in attributes.instance_state(u1).callables

        sess.expunge_all()
        u1 = sess.query(User).options(lazyload(User.addresses)).first()
        sess.expire(u1, ["addresses"])
        assert (
            "addresses" not in attributes.instance_state(u1).expired_attributes
        )
        assert isinstance(
            attributes.instance_state(u1).callables["addresses"],
            strategies._LoadLazyAttribute,
        )

        # load the attr, goes away
        u1.addresses
        assert (
            "addresses" not in attributes.instance_state(u1).expired_attributes
        )
        assert "addresses" not in attributes.instance_state(u1).callables


class NoLoadBackPopulates(_fixtures.FixtureTest):
    """test the noload stratgegy which unlike others doesn't use
    lazyloader to set up instrumentation"""

    def test_o2m(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address, back_populates="user", lazy="noload"
                )
            },
        )

        self.mapper_registry.map_imperatively(
            Address, addresses, properties={"user": relationship(User)}
        )
        with expect_noload_deprecation():
            u1 = User()
        a1 = Address()
        u1.addresses.append(a1)
        is_(a1.user, u1)

    def test_m2o(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(
            User, users, properties={"addresses": relationship(Address)}
        )

        self.mapper_registry.map_imperatively(
            Address,
            addresses,
            properties={
                "user": relationship(
                    User, back_populates="addresses", lazy="noload"
                )
            },
        )
        with expect_noload_deprecation():
            u1 = User()
        a1 = Address()
        a1.user = u1
        in_(a1, u1.addresses)


class ManyToOneTest(_fixtures.FixtureTest):
    run_inserts = None

    def test_bidirectional_no_load(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address, backref="user", lazy="noload"
                )
            },
        )
        self.mapper_registry.map_imperatively(Address, addresses)

        # try it on unsaved objects
        with expect_noload_deprecation():
            u1 = User(name="u1")
        a1 = Address(email_address="e1")
        a1.user = u1

        session = fixture_session()
        session.add(u1)
        session.flush()
        session.expunge_all()

        a1 = session.get(Address, a1.id)

        a1.user = None
        session.flush()
        session.expunge_all()
        assert session.get(Address, a1.id).user is None
        assert session.get(User, u1.id).addresses == []
