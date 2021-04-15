import random

from sqlalchemy import func
from sqlalchemy import inspect
from sqlalchemy import null
from sqlalchemy import select
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import true
from sqlalchemy.orm import aliased
from sqlalchemy.orm import defaultload
from sqlalchemy.orm import defer
from sqlalchemy.orm import join as orm_join
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import Load
from sqlalchemy.orm import mapper
from sqlalchemy.orm import Query
from sqlalchemy.orm import relationship
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import Session
from sqlalchemy.orm import subqueryload
from sqlalchemy.orm import with_expression
from sqlalchemy.orm import with_loader_criteria
from sqlalchemy.orm import with_polymorphic
from sqlalchemy.sql.base import CacheableOptions
from sqlalchemy.sql.expression import case
from sqlalchemy.sql.visitors import InternalTraversal
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing.fixtures import fixture_session
from test.orm import _fixtures
from .inheritance import _poly_fixtures
from .test_query import QueryTest
from ..sql.test_compare import CacheKeyFixture


def stmt_20(*elements):
    return tuple(
        elem._statement_20() if isinstance(elem, Query) else elem
        for elem in elements
    )


class CacheKeyTest(CacheKeyFixture, _fixtures.FixtureTest):
    run_setup_mappers = "once"
    run_inserts = None
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()

    def test_mapper_and_aliased(self):
        User, Address, Keyword = self.classes("User", "Address", "Keyword")

        self._run_cache_key_fixture(
            lambda: (inspect(User), inspect(Address), inspect(aliased(User))),
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

    def test_query_expr(self):
        (User,) = self.classes("User")

        self._run_cache_key_fixture(
            lambda: (
                with_expression(User.name, true()),
                with_expression(User.name, null()),
                with_expression(User.name, func.foobar()),
                with_expression(User.name, User.name == "test"),
                Load(User).with_expression(User.name, true()),
                Load(User).with_expression(User.name, null()),
                Load(User).with_expression(User.name, func.foobar()),
                Load(User).with_expression(User.name, User.name == "test"),
            ),
            compare_values=True,
        )

    def test_loader_criteria(self):
        User, Address = self.classes("User", "Address")

        from sqlalchemy import Column, Integer, String

        class Foo(object):
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
        from sqlalchemy import Column, Integer

        class Foo(object):
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
                joinedload("addresses"),
                joinedload(User.orders),
                joinedload(User.orders.and_(Order.id != 5)),
                joinedload(User.orders.and_(Order.id == 5)),
                joinedload(User.orders.and_(Order.description != "somename")),
                joinedload(User.orders).selectinload("items"),
                joinedload(User.orders).selectinload(Order.items),
                defer(User.id),
                defer("id"),
                defer("*"),
                defer(Address.id),
                subqueryload(User.orders),
                selectinload(User.orders),
                joinedload(User.addresses).defer(Address.id),
                joinedload(aliased(User).addresses).defer(Address.id),
                joinedload(User.addresses).defer("id"),
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

    def test_bound_options(self):
        User, Address, Keyword, Order, Item = self.classes(
            "User", "Address", "Keyword", "Order", "Item"
        )

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
                Load(User).subqueryload("addresses"),
                Load(Address).defer("id"),
                Load(Address).defer("*"),
                Load(aliased(Address)).defer("id"),
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
                Load(Address).raiseload("user"),
            ),
            compare_values=True,
        )

    def test_bound_options_equiv_on_strname(self):
        """Bound loader options resolve on string name so test that the cache
        key for the string version matches the resolved version.

        """
        User, Address, Keyword, Order, Item = self.classes(
            "User", "Address", "Keyword", "Order", "Item"
        )

        for left, right in [
            (Load(User).defer(User.id), Load(User).defer("id")),
            (
                Load(User).joinedload(User.addresses),
                Load(User).joinedload("addresses"),
            ),
            (
                Load(User).joinedload(User.orders).joinedload(Order.items),
                Load(User).joinedload("orders").joinedload("items"),
            ),
        ]:
            eq_(left._generate_cache_key(), right._generate_cache_key())

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
                .join("addresses")
                .join("dingalings", from_joinpoint=True),
                fixture_session().query(User).join("addresses"),
                fixture_session().query(User).join("orders"),
                fixture_session().query(User).join("addresses").join("orders"),
                fixture_session().query(User).join(Address, User.addresses),
                fixture_session().query(User).join(a1, "addresses"),
                fixture_session()
                .query(User)
                .join(a1, "addresses", aliased=True),
                fixture_session().query(User).join(User.addresses.of_type(a1)),
            ),
            compare_values=True,
        )

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


class PolyCacheKeyTest(CacheKeyFixture, _poly_fixtures._Polymorphic):
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

    def test_wp_queries(self):
        Person, Manager, Engineer, Boss = self.classes(
            "Person", "Manager", "Engineer", "Boss"
        )

        def one():
            return (
                fixture_session()
                .query(Person)
                .with_polymorphic([Manager, Engineer])
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

        def four():
            return (
                fixture_session()
                .query(Person)
                .with_polymorphic([Manager, Engineer])
                .filter(Person.name == "asdf")
            )

        def five():
            subq = (
                select(Person)
                .outerjoin(Manager)
                .outerjoin(Engineer)
                .subquery()
            )
            wp = with_polymorphic(Person, [Manager, Engineer], subq)

            return fixture_session().query(wp).filter(wp.name == "asdfo")

        def six():
            subq = (
                select(Person)
                .outerjoin(Manager)
                .outerjoin(Engineer)
                .subquery()
            )

            return (
                fixture_session()
                .query(Person)
                .with_polymorphic([Manager, Engineer], subq)
                .filter(Person.name == "asdfo")
            )

        self._run_cache_key_fixture(
            lambda: stmt_20(
                one(), two(), three(), three_a(), four(), five(), six()
            ),
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

        mapper(
            User,
            users,
            properties={
                "addresses": relationship(Address, back_populates="user")
            },
        )

        mapper(
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
