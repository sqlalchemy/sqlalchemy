import random

import sqlalchemy as sa
from sqlalchemy import Column
from sqlalchemy import column
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Identity
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import literal_column
from sqlalchemy import null
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import true
from sqlalchemy import update
from sqlalchemy import util
from sqlalchemy.ext.declarative import ConcreteBase
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import aliased
from sqlalchemy.orm import Bundle
from sqlalchemy.orm import defaultload
from sqlalchemy.orm import defer
from sqlalchemy.orm import join as orm_join
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import lazyload
from sqlalchemy.orm import Load
from sqlalchemy.orm import load_only
from sqlalchemy.orm import Query
from sqlalchemy.orm import query_expression
from sqlalchemy.orm import relationship
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import Session
from sqlalchemy.orm import subqueryload
from sqlalchemy.orm import synonym
from sqlalchemy.orm import with_expression
from sqlalchemy.orm import with_loader_criteria
from sqlalchemy.orm import with_polymorphic
from sqlalchemy.sql.base import CacheableOptions
from sqlalchemy.sql.expression import case
from sqlalchemy.sql.visitors import InternalTraversal
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import int_within_variance
from sqlalchemy.testing import ne_
from sqlalchemy.testing.entities import ComparableMixin
from sqlalchemy.testing.fixtures import DeclarativeMappedTest
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.util import count_cache_key_tuples
from sqlalchemy.testing.util import total_size
from test.orm import _fixtures
from .inheritance import _poly_fixtures
from .test_query import QueryTest


def stmt_20(*elements):
    return tuple(
        elem._statement_20() if isinstance(elem, Query) else elem
        for elem in elements
    )


class CacheKeyTest(fixtures.CacheKeyFixture, _fixtures.FixtureTest):
    run_setup_mappers = "once"
    run_inserts = None
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()

    def test_mapper_and_aliased(self):
        User, Address, Keyword = self.classes("User", "Address", "Keyword")

        addresses_table = self.tables.addresses

        self._run_cache_key_fixture(
            lambda: (
                inspect(User),
                inspect(Address),
                inspect(aliased(User)),
                inspect(aliased(aliased(User, addresses_table))),
                inspect(aliased(aliased(User), addresses_table.select())),
                inspect(aliased(Address)),
                inspect(aliased(Address, addresses_table.select())),
                inspect(aliased(User, addresses_table.select())),
            ),
            compare_values=True,
        )

    def test_attributes(self):
        User, Address, Keyword = self.classes("User", "Address", "Keyword")

        self._run_cache_key_fixture(
            lambda: (
                User.id,
                Address.id,
                aliased(User).id,
                aliased(User, name="foo").id,
                aliased(User, name="bar").id,
                User.name,
                User.addresses,
                Address.email_address,
                aliased(User).addresses,
            ),
            compare_values=True,
        )

    def test_bundles_in_annotations(self):
        User = self.classes.User

        self._run_cache_key_fixture(
            lambda: (
                Bundle("mybundle", User.id).__clause_element__(),
                Bundle("myotherbundle", User.id).__clause_element__(),
                Bundle("mybundle", User.name).__clause_element__(),
                Bundle("mybundle", User.id, User.name).__clause_element__(),
            ),
            compare_values=True,
        )

    def test_bundles_directly(self):
        User = self.classes.User

        self._run_cache_key_fixture(
            lambda: (
                Bundle("mybundle", User.id),
                Bundle("mybundle", User.id).__clause_element__(),
                Bundle("myotherbundle", User.id),
                Bundle("mybundle", User.name),
                Bundle("mybundle", User.id, User.name),
            ),
            compare_values=True,
        )

    def test_query_expr(self):
        (User,) = self.classes("User")

        self._run_cache_key_fixture(
            lambda: (
                with_expression(User.name, true()),
                with_expression(User.name, null()),
                with_expression(User.name, func.foobar()),
                with_expression(User.name, User.name == "test"),
            ),
            compare_values=True,
        )

        self._run_cache_key_fixture(
            lambda: (
                Load(User).with_expression(User.name, true()),
                Load(User).with_expression(User.name, null()),
                Load(User).with_expression(User.name, func.foobar()),
                Load(User).with_expression(User.name, User.name == "test"),
            ),
            compare_values=True,
        )

    def test_loader_criteria(self):
        User, Address = self.classes("User", "Address")

        class Foo:
            id = Column(Integer)
            name = Column(String)

        self._run_cache_key_fixture(
            lambda: (
                with_loader_criteria(User, User.name != "somename"),
                with_loader_criteria(User, User.id != 5),
                with_loader_criteria(User, lambda cls: cls.id == 10),
                with_loader_criteria(Address, Address.id != 5),
                with_loader_criteria(Foo, lambda cls: cls.id == 10),
            ),
            compare_values=True,
        )

    def test_loader_criteria_bound_param_thing(self):
        class Foo:
            id = Column(Integer)

        def go(param):
            return with_loader_criteria(Foo, lambda cls: cls.id == param)

        g1 = go(10)
        g2 = go(20)

        ck1 = g1._generate_cache_key()
        ck2 = g2._generate_cache_key()

        eq_(ck1.key, ck2.key)
        eq_(ck1.bindparams[0].key, ck2.bindparams[0].key)
        eq_(ck1.bindparams[0].value, 10)
        eq_(ck2.bindparams[0].value, 20)

    def test_instrumented_attributes(self):
        User, Address, Keyword, Order, Item = self.classes(
            "User", "Address", "Keyword", "Order", "Item"
        )

        self._run_cache_key_fixture(
            lambda: (
                User.addresses,
                User.addresses.of_type(aliased(Address)),
                User.orders,
                User.orders.and_(Order.id != 5),
                User.orders.and_(Order.description != "somename"),
            ),
            compare_values=True,
        )

    def test_unbound_options(self):
        User, Address, Keyword, Order, Item = self.classes(
            "User", "Address", "Keyword", "Order", "Item"
        )

        self._run_cache_key_fixture(
            lambda: (
                joinedload(User.addresses),
                joinedload(User.addresses.of_type(aliased(Address))),
                joinedload(User.orders),
                joinedload(User.orders.and_(Order.id != 5)),
                joinedload(User.orders.and_(Order.id == 5)),
                joinedload(User.orders.and_(Order.description != "somename")),
                joinedload(User.orders).selectinload(Order.items),
                defer(User.id),
                defer("*"),
                defer(Address.id),
                subqueryload(User.orders),
                selectinload(User.orders),
                joinedload(User.addresses).defer(Address.id),
                joinedload(aliased(User).addresses).defer(Address.id),
                joinedload(User.orders).joinedload(Order.items),
                joinedload(User.orders).subqueryload(Order.items),
                subqueryload(User.orders).subqueryload(Order.items),
                subqueryload(User.orders)
                .subqueryload(Order.items)
                .defer(Item.description),
                defaultload(User.orders).defaultload(Order.items),
                defaultload(User.orders),
            ),
            compare_values=True,
        )

    def test_unbound_sub_options(self):
        """test #6869"""

        User, Address, Keyword, Order, Item = self.classes(
            "User", "Address", "Keyword", "Order", "Item"
        )
        Dingaling = self.classes.Dingaling

        self._run_cache_key_fixture(
            lambda: (
                joinedload(User.addresses).options(
                    joinedload(Address.dingaling)
                ),
                joinedload(User.addresses).options(
                    joinedload(Address.dingaling).options(
                        load_only(Dingaling.id)
                    )
                ),
                joinedload(User.orders).options(
                    joinedload(Order.items).options(joinedload(Item.keywords))
                ),
            ),
            compare_values=True,
        )

    def test_bound_options(self):
        User, Address, Keyword, Order, Item = self.classes(
            "User", "Address", "Keyword", "Order", "Item"
        )

        a1 = aliased(Address)

        self._run_cache_key_fixture(
            lambda: (
                Load(User).joinedload(User.addresses),
                Load(User).joinedload(
                    User.addresses.of_type(aliased(Address))
                ),
                Load(User).joinedload(User.orders),
                Load(User).joinedload(User.orders.and_(Order.id != 5)),
                Load(User).joinedload(
                    User.orders.and_(Order.description != "somename")
                ),
                Load(User).defer(User.id),
                Load(User).subqueryload(User.addresses),
                Load(Address).defer(Address.id),
                Load(Address).defer("*"),
                Load(a1).defer(a1.id),
                Load(User).joinedload(User.addresses).defer(Address.id),
                Load(User).joinedload(User.orders).joinedload(Order.items),
                Load(User).joinedload(User.orders).subqueryload(Order.items),
                Load(User).subqueryload(User.orders).subqueryload(Order.items),
                Load(User)
                .subqueryload(User.orders)
                .subqueryload(Order.items)
                .defer(Item.description),
                Load(User).defaultload(User.orders).defaultload(Order.items),
                Load(User).defaultload(User.orders),
                Load(Address).raiseload("*"),
                Load(Address).raiseload(Address.user),
            ),
            compare_values=True,
        )

    def test_selects_w_orm_joins(self):
        User, Address, Keyword, Order, Item = self.classes(
            "User", "Address", "Keyword", "Order", "Item"
        )

        a1 = aliased(Address)

        self._run_cache_key_fixture(
            lambda: (
                select(User).join(User.addresses),
                select(User).join(User.orders),
                select(User).join(User.addresses).join(User.orders),
                select(User).join(Address, User.addresses),
                select(User).join(a1, User.addresses),
                select(User).join(User.addresses.of_type(a1)),
                select(User).join(
                    User.addresses.and_(Address.email_address == "foo")
                ),
                select(User)
                .join(Address, User.addresses)
                .join_from(User, Order),
                select(User)
                .join(Address, User.addresses)
                .join_from(User, User.orders),
                select(User.id, Order.id).select_from(
                    orm_join(User, Order, User.orders)
                ),
            ),
            compare_values=True,
        )

    def test_orm_query_w_orm_joins(self):
        User, Address, Keyword, Order, Item = self.classes(
            "User", "Address", "Keyword", "Order", "Item"
        )

        a1 = aliased(Address)

        self._run_cache_key_fixture(
            lambda: stmt_20(
                fixture_session().query(User).join(User.addresses),
                fixture_session().query(User).join(User.orders),
                fixture_session()
                .query(User)
                .join(User.addresses)
                .join(User.orders),
                fixture_session()
                .query(User)
                .join(User.addresses)
                .join(Address.dingaling),
                fixture_session().query(User).join(Address, User.addresses),
                fixture_session().query(User).join(a1, User.addresses),
                fixture_session().query(User).join(User.addresses.of_type(a1)),
            ),
            compare_values=True,
        )

    def test_orm_query_using_with_entities(self):
        """test issue #6503"""
        User, Address, Keyword, Order, Item = self.classes(
            "User", "Address", "Keyword", "Order", "Item"
        )

        self._run_cache_key_fixture(
            lambda: stmt_20(
                fixture_session()
                .query(User)
                .join(User.addresses)
                .with_entities(Address.id),
                #
                fixture_session().query(Address.id).join(User.addresses),
                #
                fixture_session()
                .query(User)
                .options(selectinload(User.addresses))
                .with_entities(User.id),
                #
                fixture_session()
                .query(User)
                .options(selectinload(User.addresses)),
                #
                fixture_session().query(User).with_entities(User.id),
                #
                # here, propagate_attr->orm is Address, entity is Address.id,
                # but the join() + with_entities() will log a
                # _MemoizedSelectEntities to differentiate
                fixture_session()
                .query(Address, Order)
                .join(Address.dingaling)
                .with_entities(Address.id),
                #
                # same, propagate_attr->orm is Address, entity is Address.id,
                # but the join() + with_entities() will log a
                # _MemoizedSelectEntities to differentiate
                fixture_session()
                .query(Address, User)
                .join(Address.dingaling)
                .with_entities(Address.id),
            ),
            compare_values=True,
        )

    def test_synonyms(self, registry):
        """test for issue discovered in #7394"""

        @registry.mapped
        class User2:
            __table__ = self.tables.users

            name_syn = synonym("name")

        @registry.mapped
        class Address2:
            __table__ = self.tables.addresses

            name_syn = synonym("email_address")

        self._run_cache_key_fixture(
            lambda: (
                User2.id,
                User2.name,
                User2.name_syn,
                Address2.name_syn,
                Address2.email_address,
                aliased(User2).name_syn,
                aliased(User2, name="foo").name_syn,
                aliased(User2, name="bar").name_syn,
            ),
            compare_values=True,
        )

    def test_more_with_entities_sanity_checks(self):
        """test issue #6503"""
        User, Address, Keyword, Order, Item = self.classes(
            "User", "Address", "Keyword", "Order", "Item"
        )

        sess = fixture_session()

        q1 = (
            sess.query(Address, Order)
            .with_entities(Address.id)
            ._statement_20()
        )
        q2 = (
            sess.query(Address, User).with_entities(Address.id)._statement_20()
        )

        assert not q1._memoized_select_entities
        assert not q2._memoized_select_entities

        # no joins or options, so q1 and q2 have the same cache key as Order/
        # User are discarded.  Note Address is first so propagate_attrs->orm is
        # Address.
        eq_(q1._generate_cache_key(), q2._generate_cache_key())

        q3 = sess.query(Order).with_entities(Address.id)._statement_20()
        q4 = sess.query(User).with_entities(Address.id)._statement_20()

        # with Order/User as lead entity, this affects propagate_attrs->orm
        # so keys are different
        ne_(q3._generate_cache_key(), q4._generate_cache_key())

        # confirm by deleting propagate attrs and memoized key and
        # running again
        q3._propagate_attrs = None
        q4._propagate_attrs = None
        del q3.__dict__["_generate_cache_key"]
        del q4.__dict__["_generate_cache_key"]
        eq_(q3._generate_cache_key(), q4._generate_cache_key())

        # once there's a join() or options() prior to with_entities, now they
        # are not discarded from the key; Order and User are in the
        # _MemoizedSelectEntities
        q5 = (
            sess.query(Address, Order)
            .join(Address.dingaling)
            .with_entities(Address.id)
            ._statement_20()
        )
        q6 = (
            sess.query(Address, User)
            .join(Address.dingaling)
            .with_entities(Address.id)
            ._statement_20()
        )

        assert q5._memoized_select_entities
        assert q6._memoized_select_entities
        ne_(q5._generate_cache_key(), q6._generate_cache_key())

    def test_orm_query_from_statement(self):
        User, Address, Keyword, Order, Item = self.classes(
            "User", "Address", "Keyword", "Order", "Item"
        )

        self._run_cache_key_fixture(
            lambda: stmt_20(
                fixture_session()
                .query(User)
                .from_statement(text("select * from user")),
                select(User).from_statement(text("select * from user")),
                fixture_session()
                .query(User)
                .options(selectinload(User.addresses))
                .from_statement(text("select * from user")),
                fixture_session()
                .query(User)
                .options(subqueryload(User.addresses))
                .from_statement(text("select * from user")),
                fixture_session()
                .query(User)
                .from_statement(text("select * from user order by id")),
                fixture_session()
                .query(User.id)
                .from_statement(text("select * from user")),
            ),
            compare_values=True,
        )

    def test_orm_query_basic(self):
        User, Address, Keyword, Order, Item = self.classes(
            "User", "Address", "Keyword", "Order", "Item"
        )

        a1 = aliased(Address)

        self._run_cache_key_fixture(
            lambda: stmt_20(
                fixture_session().query(User),
                fixture_session().query(User).prefix_with("foo"),
                fixture_session().query(User).filter_by(name="ed"),
                fixture_session()
                .query(User)
                .filter_by(name="ed")
                .order_by(User.id),
                fixture_session()
                .query(User)
                .filter_by(name="ed")
                .order_by(User.name),
                fixture_session()
                .query(User)
                .filter_by(name="ed")
                .group_by(User.id),
                fixture_session()
                .query(User)
                .join(User.addresses)
                .filter(User.name == "ed"),
                fixture_session().query(User).join(User.orders),
                fixture_session()
                .query(User)
                .join(User.orders)
                .filter(Order.description == "adsf"),
                fixture_session()
                .query(User)
                .join(User.addresses)
                .join(User.orders),
                fixture_session().query(User).join(Address, User.addresses),
                fixture_session().query(User).join(a1, User.addresses),
                fixture_session().query(User).join(User.addresses.of_type(a1)),
                fixture_session().query(Address).join(Address.user),
                fixture_session().query(User, Address).filter_by(name="ed"),
                fixture_session().query(User, a1).filter_by(name="ed"),
            ),
            compare_values=True,
        )

    def test_options(self):
        class MyOpt(CacheableOptions):
            _cache_key_traversal = [
                ("x", InternalTraversal.dp_plain_obj),
                ("y", InternalTraversal.dp_plain_obj),
            ]
            x = 5
            y = ()

        self._run_cache_key_fixture(
            lambda: (
                MyOpt,
                MyOpt + {"x": 10},
                MyOpt + {"x": 15, "y": ("foo",)},
                MyOpt + {"x": 15, "y": ("foo",)} + {"y": ("foo", "bar")},
            ),
            compare_values=True,
        )


class PolyCacheKeyTest(fixtures.CacheKeyFixture, _poly_fixtures._Polymorphic):
    run_setup_mappers = "once"
    run_inserts = None
    run_deletes = None

    def test_wp_objects(self):
        Person, Manager, Engineer, Boss = self.classes(
            "Person", "Manager", "Engineer", "Boss"
        )

        self._run_cache_key_fixture(
            lambda: (
                inspect(with_polymorphic(Person, [Manager, Engineer])),
                inspect(with_polymorphic(Person, [Manager])),
                inspect(with_polymorphic(Person, [Manager, Engineer, Boss])),
                inspect(
                    with_polymorphic(Person, [Manager, Engineer], flat=True)
                ),
                inspect(
                    with_polymorphic(
                        Person,
                        [Manager, Engineer],
                        select(Person)
                        .outerjoin(Manager)
                        .outerjoin(Engineer)
                        .subquery(),
                    )
                ),
            ),
            compare_values=True,
        )

    def test_wpoly_cache_keys(self):
        Person, Manager, Engineer, Boss = self.classes(
            "Person", "Manager", "Engineer", "Boss"
        )

        meb_stmt = inspect(
            with_polymorphic(Person, [Manager, Engineer, Boss])
        ).selectable
        me_stmt = inspect(
            with_polymorphic(Person, [Manager, Engineer])
        ).selectable

        self._run_cache_key_fixture(
            lambda: (
                inspect(Person),
                inspect(aliased(Person, me_stmt)),
                inspect(aliased(Person, meb_stmt)),
                inspect(with_polymorphic(Person, [Manager, Engineer])),
                # aliased=True is the same as flat=True for default selectable
                inspect(
                    with_polymorphic(
                        Person, [Manager, Engineer], aliased=True
                    ),
                ),
                inspect(
                    with_polymorphic(Person, [Manager, Engineer], flat=True),
                ),
                inspect(
                    with_polymorphic(
                        Person, [Manager, Engineer], flat=True, innerjoin=True
                    ),
                ),
                inspect(
                    with_polymorphic(
                        Person,
                        [Manager, Engineer],
                        flat=True,
                        _use_mapper_path=True,
                    ),
                ),
                inspect(
                    with_polymorphic(
                        Person,
                        [Manager, Engineer],
                        flat=True,
                        adapt_on_names=True,
                    ),
                ),
                inspect(
                    with_polymorphic(
                        Person, [Manager, Engineer], selectable=meb_stmt
                    ),
                ),
                inspect(
                    with_polymorphic(
                        Person,
                        [Manager, Engineer],
                        selectable=meb_stmt,
                        aliased=True,
                    ),
                ),
                inspect(with_polymorphic(Person, [Manager, Engineer, Boss])),
                inspect(
                    with_polymorphic(
                        Person,
                        [Manager, Engineer, Boss],
                        polymorphic_on=literal_column("foo"),
                    ),
                ),
                inspect(
                    with_polymorphic(
                        Person,
                        [Manager, Engineer, Boss],
                        polymorphic_on=literal_column("bar"),
                    ),
                ),
                inspect(with_polymorphic(Person, "*", name="foo")),
            ),
            compare_values=True,
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
            lambda: stmt_20(two(), three(), three_a(), five()),
            compare_values=True,
        )

    def test_wp_joins(self):
        Company, Person, Manager, Engineer, Boss = self.classes(
            "Company", "Person", "Manager", "Engineer", "Boss"
        )

        def one():
            return (
                fixture_session()
                .query(Company)
                .join(Company.employees)
                .filter(Person.name == "asdf")
            )

        def two():
            wp = with_polymorphic(Person, [Manager, Engineer])
            return (
                fixture_session()
                .query(Company)
                .join(Company.employees.of_type(wp))
                .filter(wp.name == "asdf")
            )

        def three():
            wp = with_polymorphic(Person, [Manager, Engineer])
            return (
                fixture_session()
                .query(Company)
                .join(Company.employees.of_type(wp))
                .filter(wp.Engineer.name == "asdf")
            )

        self._run_cache_key_fixture(
            lambda: stmt_20(one(), two(), three()),
            compare_values=True,
        )

    @testing.variation(
        "exprtype", ["plain_column", "self_standing_case", "case_w_columns"]
    )
    def test_hybrid_w_case_ac(self, decl_base, exprtype):
        """test #9728"""

        class Employees(decl_base):
            __tablename__ = "employees"
            id = Column(String(128), primary_key=True)
            first_name = Column(String(length=64))

            @hybrid_property
            def name(self):
                return self.first_name

            @name.expression
            def name(
                cls,
            ):
                if exprtype.plain_column:
                    return cls.first_name
                elif exprtype.self_standing_case:
                    return case(
                        (column("x") == 1, column("q")),
                        else_=column("q"),
                    )
                elif exprtype.case_w_columns:
                    return case(
                        (column("x") == 1, column("q")),
                        else_=cls.first_name,
                    )
                else:
                    exprtype.fail()

        def go1():
            employees_2 = aliased(Employees, name="employees_2")
            stmt = select(employees_2.name)
            return stmt

        def go2():
            employees_2 = aliased(Employees, name="employees_2")
            stmt = select(employees_2)
            return stmt

        self._run_cache_key_fixture(
            lambda: stmt_20(go1(), go2()),
            compare_values=True,
        )


class RoundTripTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    run_setup_mappers = None

    @testing.fixture
    def plain_fixture(self):
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
                    Address, back_populates="user", order_by=addresses.c.id
                )
            },
        )

        self.mapper_registry.map_imperatively(
            Address,
            addresses,
            properties={
                "user": relationship(User, back_populates="addresses")
            },
        )

        return User, Address

    def test_subqueryload(self, plain_fixture):
        # subqueryload works pretty poorly w/ caching because it has
        # to create a new query.  previously, baked query went through a
        # bunch of hoops to improve upon this and they were found to be
        # broken anyway.   so subqueryload currently pulls out the original
        # query as well as the requested query and works with them at row
        # processing time to create its own query.   all of which is fairly
        # non-performant compared to the selectinloader that has a fixed
        # query.
        User, Address = plain_fixture

        s = Session(testing.db, future=True)

        def query(names):
            stmt = (
                select(User)
                .where(User.name.in_(names))
                .options(subqueryload(User.addresses))
                .order_by(User.id)
            )
            return s.execute(stmt)

        def go1():
            r1 = query(["ed"])
            eq_(
                r1.scalars().all(),
                [User(name="ed", addresses=[Address(), Address(), Address()])],
            )

        def go2():
            r1 = query(["ed", "fred"])
            eq_(
                r1.scalars().all(),
                [
                    User(
                        name="ed", addresses=[Address(), Address(), Address()]
                    ),
                    User(name="fred", addresses=[Address()]),
                ],
            )

        for i in range(5):
            fn = random.choice([go1, go2])
            self.assert_sql_count(testing.db, fn, 2)

    @testing.combinations((True,), (False,), argnames="use_core")
    @testing.combinations((True,), (False,), argnames="arbitrary_element")
    @testing.combinations((True,), (False,), argnames="exercise_caching")
    def test_column_targeting_core_execute(
        self,
        plain_fixture,
        connection,
        use_core,
        arbitrary_element,
        exercise_caching,
    ):
        """test that CursorResultSet will do a column rewrite for any core
        execute even if the ORM compiled the statement.

        This translates the current stmt.selected_columns to the cached
        ResultSetMetaData._keymap.      The ORM skips this because loading.py
        has also cached the selected_columns that are used.   But for
        an outside-facing Core execute, this has to remain turned on.

        Additionally, we want targeting of SQL expressions to work with both
        Core and ORM statement executions. So the ORM still has to do some
        translation here for these elements to be supported.

        """
        User, Address = plain_fixture
        user_table = inspect(User).persist_selectable

        def go():
            my_thing = case((User.id > 9, 1), else_=2)

            # include entities in the statement so that we test that
            # the column indexing from
            # ORM select()._raw_columns -> Core select()._raw_columns is
            # translated appropriately
            stmt = (
                select(User, Address.email_address, my_thing, User.name)
                .join(Address)
                .where(User.name == "ed")
            )

            if arbitrary_element:
                target, exp = (my_thing, 2)
            elif use_core:
                target, exp = (user_table.c.name, "ed")
            else:
                target, exp = (User.name, "ed")

            if use_core:
                row = connection.execute(stmt).first()

            else:
                row = Session(connection).execute(stmt).first()

            eq_(row._mapping[target], exp)

        if exercise_caching:
            for i in range(3):
                go()
        else:
            go()

    @testing.combinations(
        (lazyload, 2),
        (joinedload, 1),
        (selectinload, 2),
        (subqueryload, 2),
        argnames="strat,expected_stmt_cache",
    )
    def test_cache_key_loader_strategies(
        self,
        plain_fixture,
        strat,
        expected_stmt_cache,
        connection,
    ):
        User, Address = plain_fixture

        cache = {}

        connection = connection.execution_options(compiled_cache=cache)
        sess = Session(connection)

        def go():
            stmt = (
                select(User).where(User.id == 7).options(strat(User.addresses))
            )

            u1 = sess.execute(stmt).scalars().first()
            eq_(u1.addresses, [Address(id=1)])

        go()

        lc = len(cache)

        stmt_entries = [k for k in cache]

        eq_(len(stmt_entries), expected_stmt_cache)

        for i in range(3):
            go()

        eq_(len(cache), lc)


class CompositeTest(fixtures.MappedTest):
    __dialect__ = "default"

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "edges",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("x1", Integer),
            Column("y1", Integer),
            Column("x2", Integer),
            Column("y2", Integer),
        )

    @classmethod
    def setup_mappers(cls):
        edges = cls.tables.edges

        class Point(cls.Comparable):
            def __init__(self, x, y):
                self.x = x
                self.y = y

            def __composite_values__(self):
                return [self.x, self.y]

            __hash__ = None

            def __eq__(self, other):
                return (
                    isinstance(other, Point)
                    and other.x == self.x
                    and other.y == self.y
                )

            def __ne__(self, other):
                return not isinstance(other, Point) or not self.__eq__(other)

        class Edge(cls.Comparable):
            def __init__(self, *args):
                if args:
                    self.start, self.end = args

        cls.mapper_registry.map_imperatively(
            Edge,
            edges,
            properties={
                "start": sa.orm.composite(Point, edges.c.x1, edges.c.y1),
                "end": sa.orm.composite(Point, edges.c.x2, edges.c.y2),
            },
        )

    def test_bulk_update_cache_key(self):
        """test secondary issue located as part of #7209"""
        Edge, Point = (self.classes.Edge, self.classes.Point)

        stmt = (
            update(Edge)
            .filter(Edge.start == Point(14, 5))
            .values({Edge.end: Point(16, 10)})
        )
        stmt2 = (
            update(Edge)
            .filter(Edge.start == Point(14, 5))
            .values({Edge.end: Point(17, 8)})
        )

        eq_(stmt._generate_cache_key(), stmt2._generate_cache_key())


class EmbeddedSubqTest(
    fixtures.RemoveORMEventsGlobally, DeclarativeMappedTest
):
    """test #8790.

    it's expected that cache key structures will change, this test is here
    testing something fairly similar to the issue we had (though vastly
    smaller scale) so we mostly want to look for surprise jumps here.

    """

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Employee(ConcreteBase, Base):
            __tablename__ = "employee"
            id = Column(Integer, primary_key=True)
            name = Column(String(50))

            __mapper_args__ = {
                "polymorphic_identity": "employee",
                "concrete": True,
            }

        class Manager(Employee):
            __tablename__ = "manager"
            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            manager_data = Column(String(40))

            __mapper_args__ = {
                "polymorphic_identity": "manager",
                "concrete": True,
            }

        class Engineer(Employee):
            __tablename__ = "engineer"
            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            engineer_info = Column(String(40))

            __mapper_args__ = {
                "polymorphic_identity": "engineer",
                "concrete": True,
            }

        Base.registry.configure()

    @testing.combinations(
        "tuples",
        ("memory", testing.requires.is64bit + testing.requires.cpython),
        argnames="assert_on",
    )
    def test_cache_key_gen(self, assert_on):
        Employee = self.classes.Employee

        e1 = aliased(Employee)
        e2 = aliased(Employee)

        subq = select(e1).union_all(select(e2)).subquery()

        anno = aliased(Employee, subq)

        stmt = select(anno)

        ck = stmt._generate_cache_key()

        if assert_on == "tuples":
            # before the fix for #8790 this was 700
            int_within_variance(142, count_cache_key_tuples(ck), 0.05)

        elif assert_on == "memory":
            # before the fix for #8790 this was 55154

            if util.py312:
                testing.skip_test("python platform not available")
            elif util.py311:
                int_within_variance(39996, total_size(ck), 0.05)
            else:
                int_within_variance(29796, total_size(ck), 0.05)


class WithExpresionLoaderOptTest(DeclarativeMappedTest):
    """test #10570"""

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class A(ComparableMixin, Base):
            __tablename__ = "a"

            id = Column(Integer, Identity(), primary_key=True)
            data = Column(String(30))
            bs = relationship("B")

        class B(ComparableMixin, Base):
            __tablename__ = "b"
            id = Column(Integer, Identity(), primary_key=True)
            a_id = Column(ForeignKey("a.id"))
            boolean = query_expression()
            data = Column(String(30))

    @classmethod
    def insert_data(cls, connection):
        A, B = cls.classes("A", "B")

        with Session(connection) as s:
            s.add(A(bs=[B(data="a"), B(data="b"), B(data="c")]))
            s.commit()

    @testing.combinations(
        joinedload, lazyload, defaultload, selectinload, subqueryload
    )
    @testing.only_on(
        ["sqlite", "postgresql"],
        "in-place boolean not generally available (Oracle, SQL Server)",
    )
    def test_from_opt(self, loadopt):
        A, B = self.classes("A", "B")

        def go(value):
            with Session(testing.db) as sess:
                objects = sess.execute(
                    select(A).options(
                        loadopt(A.bs).options(
                            with_expression(B.boolean, B.data == value)
                        )
                    )
                ).scalars()
                if loadopt is joinedload:
                    objects = objects.unique()
                eq_(
                    objects.all(),
                    [
                        A(
                            bs=[
                                B(data="a", boolean=value == "a"),
                                B(data="b", boolean=value == "b"),
                                B(data="c", boolean=value == "c"),
                            ]
                        )
                    ],
                )

        go("b")
        go("c")
