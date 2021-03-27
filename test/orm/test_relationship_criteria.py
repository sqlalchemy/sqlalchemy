import datetime
import random

from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import event
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import orm
from sqlalchemy import select
from sqlalchemy import sql
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.orm import aliased
from sqlalchemy.orm import defer
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import lazyload
from sqlalchemy.orm import mapper
from sqlalchemy.orm import registry
from sqlalchemy.orm import relationship
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import Session
from sqlalchemy.orm import subqueryload
from sqlalchemy.orm import with_loader_criteria
from sqlalchemy.testing import eq_
from sqlalchemy.testing.assertsql import CompiledSQL
from test.orm import _fixtures


class _Fixtures(_fixtures.FixtureTest):
    @testing.fixture
    def user_address_fixture(self):
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
                "addresses": relationship(
                    mapper(Address, addresses), order_by=Address.id
                )
            },
        )
        return User, Address

    @testing.fixture
    def order_item_fixture(self):
        Order, Item = self.classes("Order", "Item")
        orders, items, order_items = self.tables(
            "orders", "items", "order_items"
        )

        mapper(
            Order,
            orders,
            properties={
                # m2m
                "items": relationship(
                    Item, secondary=order_items, order_by=items.c.id
                ),
            },
        )
        mapper(Item, items)

        return Order, Item

    @testing.fixture
    def mixin_fixture(self):
        users = self.tables.users

        class HasFoob(object):
            name = Column(String)

        class UserWFoob(HasFoob, self.Comparable):
            pass

        mapper(
            UserWFoob,
            users,
        )
        return HasFoob, UserWFoob

    @testing.fixture
    def multi_mixin_fixture(self):
        orders, items = self.tables.orders, self.tables.items
        order_items = self.tables.order_items

        class HasFoob(object):
            description = Column(String)

        class HasBat(HasFoob):
            some_nothing = Column(Integer)

        class Order(HasFoob, self.Comparable):
            pass

        class Item(HasBat, self.Comparable):
            pass

        base = registry()
        base.map_imperatively(
            Order,
            orders,
            properties={"items": relationship("Item", secondary=order_items)},
        )
        base.map_imperatively(Item, items)
        return HasFoob, Order, Item


class LoaderCriteriaTest(_Fixtures, testing.AssertsCompiledSQL):
    """
    combinations:


        with_loader_criteria
            # for these we have mapper_criteria

            select(mapper)  # select_mapper
            select(mapper.col, mapper.col)  # select_mapper_col
            select(func.count()).select_from(mapper)  # select_from_mapper
            select(a).join(mapper, a.target)  # select_join_mapper
            select(a).options(joinedload(a.target))  # select_joinedload_mapper


            # for these we have aliased_criteria, inclaliased_criteria

            select(aliased)  # select_aliased
            select(aliased.col, aliased.col)  # select_aliased_col
            select(func.count()).select_from(aliased) # select_from_aliased
            select(a).join(aliased, a.target)  # select_join_aliased
            select(a).options(joinedload(a.target.of_type(aliased))
            # select_joinedload_aliased

    """

    __dialect__ = "default"

    def test_select_mapper_mapper_criteria(self, user_address_fixture):
        User, Address = user_address_fixture

        stmt = select(User).options(
            with_loader_criteria(User, User.name != "name")
        )

        self.assert_compile(
            stmt,
            "SELECT users.id, users.name "
            "FROM users WHERE users.name != :name_1",
        )

    def test_select_from_mapper_mapper_criteria(self, user_address_fixture):
        User, Address = user_address_fixture

        stmt = (
            select(sql.func.count())
            .select_from(User)
            .options(with_loader_criteria(User, User.name != "name"))
        )

        self.assert_compile(
            stmt,
            "SELECT count(*) AS count_1 FROM users "
            "WHERE users.name != :name_1",
        )

    def test_select_mapper_columns_mapper_criteria(self, user_address_fixture):
        User, Address = user_address_fixture

        stmt = select(User.id, User.name).options(
            with_loader_criteria(User, User.name != "name")
        )

        self.assert_compile(
            stmt,
            "SELECT users.id, users.name "
            "FROM users WHERE users.name != :name_1",
        )

    def test_select_join_mapper_mapper_criteria(self, user_address_fixture):
        User, Address = user_address_fixture

        stmt = (
            select(User)
            .join(User.addresses)
            .options(
                with_loader_criteria(Address, Address.email_address != "name")
            )
        )

        self.assert_compile(
            stmt,
            "SELECT users.id, users.name FROM users "
            "JOIN addresses ON users.id = addresses.user_id "
            "AND addresses.email_address != :email_address_1",
        )

    def test_select_joinm2m_mapper_mapper_criteria(self, order_item_fixture):
        Order, Item = order_item_fixture

        stmt = (
            select(Order)
            .join(Order.items)
            .options(
                with_loader_criteria(Item, Item.description != "description")
            )
        )

        self.assert_compile(
            stmt,
            "SELECT orders.id, orders.user_id, orders.address_id, "
            "orders.description, orders.isopen FROM orders "
            "JOIN order_items AS order_items_1 "
            "ON orders.id = order_items_1.order_id "
            "JOIN items ON items.id = order_items_1.item_id "
            "AND items.description != :description_1",
        )

    def test_select_joinedload_mapper_mapper_criteria(
        self, user_address_fixture
    ):
        User, Address = user_address_fixture

        stmt = select(User).options(
            joinedload(User.addresses),
            with_loader_criteria(Address, Address.email_address != "name"),
        )

        self.assert_compile(
            stmt,
            "SELECT users.id, users.name, addresses_1.id AS id_1, "
            "addresses_1.user_id, addresses_1.email_address "
            "FROM users LEFT OUTER JOIN addresses AS addresses_1 "
            "ON users.id = addresses_1.user_id "
            "AND addresses_1.email_address != :email_address_1 "
            "ORDER BY addresses_1.id",
        )

    def test_select_selectinload_mapper_mapper_criteria(
        self, user_address_fixture
    ):
        User, Address = user_address_fixture

        stmt = select(User).options(
            selectinload(User.addresses),
            with_loader_criteria(Address, Address.email_address != "name"),
        )

        s = Session(testing.db, future=True)

        with self.sql_execution_asserter() as asserter:

            s.execute(stmt).all()

        asserter.assert_(
            CompiledSQL(
                "SELECT users.id, users.name FROM users",
                [],
            ),
            CompiledSQL(
                "SELECT addresses.user_id AS addresses_user_id, addresses.id "
                "AS addresses_id, addresses.email_address "
                "AS addresses_email_address FROM addresses "
                "WHERE addresses.user_id IN ([POSTCOMPILE_primary_keys]) "
                "AND addresses.email_address != :email_address_1 "
                "ORDER BY addresses.id",
                [{"primary_keys": [7, 8, 9, 10], "email_address_1": "name"}],
            ),
        )

    def test_select_lazyload_mapper_mapper_criteria(
        self, user_address_fixture
    ):
        User, Address = user_address_fixture

        stmt = (
            select(User)
            .options(
                with_loader_criteria(Address, Address.email_address != "name"),
            )
            .order_by(User.id)
        )

        s = Session(testing.db, future=True)

        with self.sql_execution_asserter() as asserter:
            for u in s.execute(stmt).scalars():
                u.addresses

        asserter.assert_(
            CompiledSQL(
                "SELECT users.id, users.name FROM users ORDER BY users.id",
                [],
            ),
            CompiledSQL(
                "SELECT addresses.id AS addresses_id, "
                "addresses.user_id AS addresses_user_id, "
                "addresses.email_address AS addresses_email_address "
                "FROM addresses WHERE :param_1 = addresses.user_id "
                "AND addresses.email_address != :email_address_1 "
                "ORDER BY addresses.id",
                [{"param_1": 7, "email_address_1": "name"}],
            ),
            CompiledSQL(
                "SELECT addresses.id AS addresses_id, "
                "addresses.user_id AS addresses_user_id, "
                "addresses.email_address AS addresses_email_address "
                "FROM addresses WHERE :param_1 = addresses.user_id "
                "AND addresses.email_address != :email_address_1 "
                "ORDER BY addresses.id",
                [{"param_1": 8, "email_address_1": "name"}],
            ),
            CompiledSQL(
                "SELECT addresses.id AS addresses_id, "
                "addresses.user_id AS addresses_user_id, "
                "addresses.email_address AS addresses_email_address "
                "FROM addresses WHERE :param_1 = addresses.user_id "
                "AND addresses.email_address != :email_address_1 "
                "ORDER BY addresses.id",
                [{"param_1": 9, "email_address_1": "name"}],
            ),
            CompiledSQL(
                "SELECT addresses.id AS addresses_id, "
                "addresses.user_id AS addresses_user_id, "
                "addresses.email_address AS addresses_email_address "
                "FROM addresses WHERE :param_1 = addresses.user_id "
                "AND addresses.email_address != :email_address_1 "
                "ORDER BY addresses.id",
                [{"param_1": 10, "email_address_1": "name"}],
            ),
        )

    def test_select_aliased_inclaliased_criteria(self, user_address_fixture):
        User, Address = user_address_fixture

        u1 = aliased(User)
        stmt = select(u1).options(
            with_loader_criteria(
                User, User.name != "name", include_aliases=True
            )
        )

        self.assert_compile(
            stmt,
            "SELECT users_1.id, users_1.name "
            "FROM users AS users_1 WHERE users_1.name != :name_1",
        )

    def test_select_from_aliased_inclaliased_criteria(
        self, user_address_fixture
    ):
        User, Address = user_address_fixture

        u1 = aliased(User)
        stmt = (
            select(sql.func.count())
            .select_from(u1)
            .options(
                with_loader_criteria(
                    User, User.name != "name", include_aliases=True
                )
            )
        )

        self.assert_compile(
            stmt,
            "SELECT count(*) AS count_1 FROM users AS users_1 "
            "WHERE users_1.name != :name_1",
        )

    def test_select_aliased_columns_inclaliased_criteria(
        self, user_address_fixture
    ):
        User, Address = user_address_fixture

        u1 = aliased(User)
        stmt = select(u1.id, u1.name).options(
            with_loader_criteria(
                User, User.name != "name", include_aliases=True
            )
        )

        self.assert_compile(
            stmt,
            "SELECT users_1.id, users_1.name "
            "FROM users AS users_1 WHERE users_1.name != :name_1",
        )

    def test_select_join_aliased_inclaliased_criteria(
        self, user_address_fixture
    ):
        User, Address = user_address_fixture

        a1 = aliased(Address)
        stmt = (
            select(User)
            .join(User.addresses.of_type(a1))
            .options(
                with_loader_criteria(
                    Address,
                    Address.email_address != "name",
                    include_aliases=True,
                )
            )
        )

        self.assert_compile(
            stmt,
            "SELECT users.id, users.name FROM users "
            "JOIN addresses AS addresses_1 ON users.id = addresses_1.user_id "
            "AND addresses_1.email_address != :email_address_1",
        )

    def test_select_joinm2m_aliased_inclaliased_criteria(
        self, order_item_fixture
    ):
        Order, Item = order_item_fixture

        i1 = aliased(Item)

        stmt = (
            select(Order)
            .join(Order.items.of_type(i1))
            .options(
                with_loader_criteria(
                    Item,
                    Item.description != "description",
                    include_aliases=True,
                )
            )
        )

        self.assert_compile(
            stmt,
            "SELECT orders.id, orders.user_id, orders.address_id, "
            "orders.description, orders.isopen FROM orders "
            "JOIN order_items AS order_items_1 "
            "ON orders.id = order_items_1.order_id "
            "JOIN items AS items_1 ON items_1.id = order_items_1.item_id "
            "AND items_1.description != :description_1",
        )

    def test_select_aliased_aliased_criteria(self, user_address_fixture):
        User, Address = user_address_fixture

        u1 = aliased(User)
        stmt = select(u1).options(with_loader_criteria(u1, u1.name != "name"))

        self.assert_compile(
            stmt,
            "SELECT users_1.id, users_1.name "
            "FROM users AS users_1 WHERE users_1.name != :name_1",
        )

    def test_select_aliased_columns_aliased_criteria(
        self, user_address_fixture
    ):
        User, Address = user_address_fixture

        u1 = aliased(User)
        stmt = select(u1.id, u1.name).options(
            with_loader_criteria(u1, u1.name != "name")
        )

        self.assert_compile(
            stmt,
            "SELECT users_1.id, users_1.name "
            "FROM users AS users_1 WHERE users_1.name != :name_1",
        )

    def test_joinedload_global_criteria(self, user_address_fixture):
        User, Address = user_address_fixture

        s = Session(testing.db, future=True)

        stmt = select(User).options(
            joinedload(User.addresses),
            with_loader_criteria(Address, Address.email_address != "email"),
        )

        with self.sql_execution_asserter() as asserter:

            s.execute(stmt)

        asserter.assert_(
            CompiledSQL(
                "SELECT users.id, users.name, addresses_1.id AS id_1, "
                "addresses_1.user_id, addresses_1.email_address FROM "
                "users LEFT OUTER JOIN addresses AS addresses_1 "
                "ON users.id = addresses_1.user_id "
                "AND addresses_1.email_address != :email_address_1 "
                "ORDER BY addresses_1.id",
                [{"email_address_1": "email"}],
            ),
        )

    def test_query_count_global_criteria(self, user_address_fixture):
        User, Address = user_address_fixture

        s = Session(testing.db)

        q = s.query(User).options(with_loader_criteria(User, User.id != 8))

        with self.sql_execution_asserter() as asserter:
            q.count()

        asserter.assert_(
            CompiledSQL(
                "SELECT count(*) AS count_1 FROM (SELECT "
                "users.id AS users_id, users.name AS users_name "
                "FROM users WHERE users.id != :id_1) AS anon_1",
                [{"id_1": 8}],
            ),
        )

    def test_query_count_after_the_fact_global_criteria(
        self, user_address_fixture
    ):
        User, Address = user_address_fixture

        s = Session(testing.db)

        # this essentially tests that the query.from_self() which takes
        # place in count() is one that can still be affected by
        # the loader criteria, meaning it has to be an ORM query

        q = s.query(User)

        @event.listens_for(s, "do_orm_execute")
        def add_criteria(orm_context):
            orm_context.statement = orm_context.statement.options(
                with_loader_criteria(User, User.id != 8)
            )

        with self.sql_execution_asserter() as asserter:
            q.count()

        asserter.assert_(
            CompiledSQL(
                "SELECT count(*) AS count_1 FROM (SELECT "
                "users.id AS users_id, users.name AS users_name "
                "FROM users WHERE users.id != :id_1) AS anon_1",
                [{"id_1": 8}],
            ),
        )

    def test_select_count_subquery_global_criteria(self, user_address_fixture):
        User, Address = user_address_fixture

        stmt = select(User).subquery()

        stmt = (
            select(sql.func.count())
            .select_from(stmt)
            .options(with_loader_criteria(User, User.id != 8))
        )

        self.assert_compile(
            stmt,
            "SELECT count(*) AS count_1 FROM (SELECT users.id AS id, "
            "users.name AS name FROM users WHERE users.id != :id_1) AS anon_1",
        )

    def test_query_outerjoin_global_criteria(self, user_address_fixture):
        User, Address = user_address_fixture

        s = Session(testing.db)

        q = (
            s.query(User, Address)
            .outerjoin(User.addresses)
            .options(
                with_loader_criteria(
                    Address,
                    ~Address.email_address.like("ed@%"),
                )
            )
            .order_by(User.id)
        )

        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name, "
            "addresses.id AS addresses_id, "
            "addresses.user_id AS addresses_user_id, "
            "addresses.email_address AS addresses_email_address "
            "FROM users LEFT OUTER JOIN addresses "
            "ON users.id = addresses.user_id AND "
            "addresses.email_address NOT LIKE :email_address_1 "
            "ORDER BY users.id",
        )
        eq_(
            q.all(),
            [
                (User(id=7), Address(id=1)),
                (User(id=8), None),  # three addresses not here
                (User(id=9), Address(id=5)),
                (User(id=10), None),
            ],
        )

    def test_caching_and_binds_lambda(self, mixin_fixture):
        HasFoob, UserWFoob = mixin_fixture

        statement = select(UserWFoob).filter(UserWFoob.id < 10)

        def go(value):
            return statement.options(
                with_loader_criteria(
                    HasFoob,
                    lambda cls: cls.name == value,
                    include_aliases=True,
                )
            )

        s = Session(testing.db, future=True)

        for i in range(10):
            name = random.choice(["ed", "fred", "jack"])
            stmt = go(name)

            eq_(s.execute(stmt).scalars().all(), [UserWFoob(name=name)])

    def test_unnamed_param_dont_fail(self, multi_mixin_fixture):
        HasFoob, Order, Item = multi_mixin_fixture

        def go(stmt, value):
            return stmt.options(
                with_loader_criteria(
                    HasFoob,
                    lambda cls: cls.description == "order 3",
                    include_aliases=True,
                )
            )

        with Session(testing.db) as sess:
            for i in range(10):
                name = random.choice(["order 1", "order 3", "order 5"])

                statement = select(Order)
                stmt = go(statement, name)

                eq_(
                    sess.execute(stmt).scalars().all(),
                    [Order(description="order 3")],
                )

    def test_caching_and_binds_lambda_more_mixins(self, multi_mixin_fixture):
        # By including non-mapped mixin HasBat in the middle of the
        # hierarchy, we test issue #5766
        HasFoob, Order, Item = multi_mixin_fixture

        def go(stmt, value):
            return stmt.options(
                with_loader_criteria(
                    HasFoob,
                    lambda cls: cls.description == value,
                    include_aliases=True,
                )
            )

        with Session(testing.db) as sess:
            for i in range(10):
                name = random.choice(["order 1", "order 3", "order 5"])

                statement = select(Order)
                stmt = go(statement, name)

                eq_(
                    sess.execute(stmt).scalars().all(),
                    [Order(description=name)],
                )

                name = random.choice(["item 1", "item 3", "item 5"])

                statement = select(Item)
                stmt = go(statement, name)

                eq_(
                    sess.execute(stmt).scalars().all(),
                    [Item(description=name)],
                )

    def test_never_for_refresh(self, user_address_fixture):
        User, Address = user_address_fixture

        s = Session(testing.db)
        u1 = s.get(User, 8)

        @event.listens_for(s, "do_orm_execute")
        def add_criteria(orm_context):
            orm_context.statement = orm_context.statement.options(
                with_loader_criteria(User, User.id != 8)
            )

        s.refresh(u1)
        eq_(u1.name, "ed")

    def test_never_for_unexpire(self, user_address_fixture):
        User, Address = user_address_fixture

        s = Session(testing.db)
        u1 = s.get(User, 8)

        s.expire(u1)

        @event.listens_for(s, "do_orm_execute")
        def add_criteria(orm_context):
            orm_context.statement = orm_context.statement.options(
                with_loader_criteria(User, User.id != 8)
            )

        eq_(u1.name, "ed")

    def test_never_for_undefer(self, user_address_fixture):
        User, Address = user_address_fixture

        s = Session(testing.db)
        u1 = s.execute(
            select(User).options(defer(User.name)).filter(User.id == 8)
        ).scalar_one()

        @event.listens_for(s, "do_orm_execute")
        def add_criteria(orm_context):
            orm_context.statement = orm_context.statement.options(
                with_loader_criteria(User, User.id != 8)
            )

        eq_(u1.name, "ed")


class TemporalFixtureTest(testing.fixtures.DeclarativeMappedTest):
    @classmethod
    def setup_classes(cls):
        class HasTemporal(object):
            """Mixin that identifies a class as having a timestamp column"""

            timestamp = Column(
                DateTime, default=datetime.datetime.utcnow, nullable=False
            )

        cls.HasTemporal = HasTemporal

        def temporal_range(range_lower, range_upper):
            return with_loader_criteria(
                HasTemporal,
                lambda cls: cls.timestamp.between(range_lower, range_upper),
                include_aliases=True,
            )

        cls.temporal_range = staticmethod(temporal_range)

        class Parent(HasTemporal, cls.DeclarativeBasic):
            __tablename__ = "parent"
            id = Column(Integer, primary_key=True)
            children = relationship("Child", order_by="Child.id")

        class Child(HasTemporal, cls.DeclarativeBasic):
            __tablename__ = "child"
            id = Column(Integer, primary_key=True)
            parent_id = Column(
                Integer, ForeignKey("parent.id"), nullable=False
            )

    @classmethod
    def insert_data(cls, connection):
        Parent, Child = cls.classes("Parent", "Child")

        sess = Session(connection)
        c1, c2, c3, c4, c5 = [
            Child(timestamp=datetime.datetime(2009, 10, 15, 12, 00, 00)),
            Child(timestamp=datetime.datetime(2009, 10, 17, 12, 00, 00)),
            Child(timestamp=datetime.datetime(2009, 10, 20, 12, 00, 00)),
            Child(timestamp=datetime.datetime(2009, 10, 12, 12, 00, 00)),
            Child(timestamp=datetime.datetime(2009, 10, 17, 12, 00, 00)),
        ]

        p1 = Parent(
            timestamp=datetime.datetime(2009, 10, 15, 12, 00, 00),
            children=[c1, c2, c3],
        )
        p2 = Parent(
            timestamp=datetime.datetime(2009, 10, 17, 12, 00, 00),
            children=[c4, c5],
        )

        sess.add_all([p1, p2])
        sess.commit()

    @testing.combinations((True,), (False,), argnames="use_caching")
    @testing.combinations(
        (None,),
        (orm.lazyload,),
        (orm.joinedload,),
        (orm.subqueryload,),
        (orm.selectinload,),
        argnames="loader_strategy",
    )
    def test_same_relatinship_load_different_range(
        self, use_caching, loader_strategy
    ):
        """This is the first test that exercises lazy loading, which uses
        a lambda select, which then needs to transform the select to have
        different bound parameters if it's not cached (or generate a working
        list of parameters if it is), which then calls into a
        with_loader_crieria that itself has another lambda inside of it,
        which means we have to traverse and replace that lambda's expression,
        but we can't evaluate it until compile time, so the inner lambda
        holds onto the "transform" function so it can run it as needed.
        this makes use of a new feature in visitors that exports a
        "run this traversal later" function.

        All of these individual features, cloning lambdaelements,
        running replacement traversals later, are very new and need a lot
        of tests, most likely in test/sql/test_lambdas.py.

        the test is from the "temporal_range" example which is the whole
        use case this feature is designed for and it is a whopper.


        """
        Parent, Child = self.classes("Parent", "Child")
        temporal_range = self.temporal_range

        if use_caching:
            Parent.children.property.bake_queries = True
            eng = testing.db
        else:
            Parent.children.property.bake_queries = False
            eng = testing.db.execution_options(compiled_cache=None)

        sess = Session(eng, future=True)

        if loader_strategy:
            loader_options = (loader_strategy(Parent.children),)
        else:
            loader_options = ()

        is_joined = (
            loader_strategy and loader_strategy.__name__ == "joinedload"
        )
        p1 = sess.execute(
            select(Parent).filter(
                Parent.timestamp == datetime.datetime(2009, 10, 15, 12, 00, 00)
            )
        ).scalar()
        c1, c2 = p1.children[0:2]
        c2_id = c2.id

        p2 = sess.execute(
            select(Parent).filter(
                Parent.timestamp == datetime.datetime(2009, 10, 17, 12, 00, 00)
            )
        ).scalar()
        c5 = p2.children[1]

        result = sess.execute(
            select(Parent)
            .execution_options(populate_existing=True)
            .options(
                temporal_range(
                    datetime.datetime(2009, 10, 16, 12, 00, 00),
                    datetime.datetime(2009, 10, 18, 12, 00, 00),
                ),
                *loader_options
            )
        )
        if is_joined:
            result = result.unique()
        parents = result.scalars().all()

        assert parents[0] == p2
        assert parents[0].children == [c5]

        result = sess.execute(
            select(Parent)
            .execution_options(populate_existing=True)
            .join(Parent.children)
            .filter(Child.id == c2_id)
            .options(
                temporal_range(
                    datetime.datetime(2009, 10, 15, 11, 00, 00),
                    datetime.datetime(2009, 10, 18, 12, 00, 00),
                ),
                *loader_options
            )
        )
        if is_joined:
            result = result.unique()
        parents = result.scalars().all()

        assert parents[0] == p1
        assert parents[0].children == [c1, c2]


class RelationshipCriteriaTest(_Fixtures, testing.AssertsCompiledSQL):
    __dialect__ = "default"

    @testing.fixture
    def user_address_fixture(self):
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
                "addresses": relationship(
                    mapper(Address, addresses), order_by=Address.id
                )
            },
        )
        return User, Address

    def _user_minus_edwood(self, User, Address):
        return [
            User(
                addresses=[
                    Address(email_address="jack@bean.com", id=1, user_id=7)
                ],
                id=7,
                name="jack",
            ),
            User(
                addresses=[
                    Address(
                        email_address="ed@bettyboop.com",
                        id=3,
                        user_id=8,
                    ),
                    Address(email_address="ed@lala.com", id=4, user_id=8),
                ],
                id=8,
                name="ed",
            ),
            User(
                addresses=[
                    Address(email_address="fred@fred.com", id=5, user_id=9)
                ],
                id=9,
                name="fred",
            ),
            User(addresses=[], id=10, name="chuck"),
        ]

    def _user_minus_edlala(self, User, Address):
        return [
            User(
                addresses=[
                    Address(email_address="jack@bean.com", id=1, user_id=7)
                ],
                id=7,
                name="jack",
            ),
            User(
                addresses=[
                    Address(email_address="ed@wood.com", id=2, user_id=8),
                    Address(
                        email_address="ed@bettyboop.com",
                        id=3,
                        user_id=8,
                    ),
                ],
                id=8,
                name="ed",
            ),
            User(
                addresses=[
                    Address(email_address="fred@fred.com", id=5, user_id=9)
                ],
                id=9,
                name="fred",
            ),
            User(addresses=[], id=10, name="chuck"),
        ]

    def test_joinedload_local_criteria(self, user_address_fixture):
        User, Address = user_address_fixture

        s = Session(testing.db, future=True)

        def go(value):
            stmt = (
                select(User)
                .options(
                    joinedload(
                        User.addresses.and_(Address.email_address != value)
                    ),
                )
                .order_by(User.id)
            )
            result = s.execute(stmt)
            return result

        for value in "ed@wood.com", "ed@lala.com":
            s.close()
            with self.sql_execution_asserter() as asserter:

                result = go(value)

                eq_(
                    result.scalars().unique().all(),
                    self._user_minus_edwood(*user_address_fixture)
                    if value == "ed@wood.com"
                    else self._user_minus_edlala(*user_address_fixture),
                )

            asserter.assert_(
                CompiledSQL(
                    "SELECT users.id, users.name, addresses_1.id AS id_1, "
                    "addresses_1.user_id, addresses_1.email_address FROM "
                    "users LEFT OUTER JOIN addresses AS addresses_1 "
                    "ON users.id = addresses_1.user_id "
                    "AND addresses_1.email_address != :email_address_1 "
                    "ORDER BY users.id, addresses_1.id",
                    [{"email_address_1": value}],
                ),
            )

    def test_selectinload_local_criteria(self, user_address_fixture):
        User, Address = user_address_fixture

        s = Session(testing.db, future=True)

        def go(value):
            stmt = (
                select(User)
                .options(
                    selectinload(
                        User.addresses.and_(Address.email_address != value)
                    ),
                )
                .order_by(User.id)
            )
            result = s.execute(stmt)
            return result

        for value in (
            "ed@wood.com",
            "ed@lala.com",
            "ed@wood.com",
            "ed@lala.com",
        ):
            s.close()
            with self.sql_execution_asserter() as asserter:
                result = go(value)

                eq_(
                    result.scalars().unique().all(),
                    self._user_minus_edwood(*user_address_fixture)
                    if value == "ed@wood.com"
                    else self._user_minus_edlala(*user_address_fixture),
                )

            asserter.assert_(
                CompiledSQL(
                    "SELECT users.id, users.name FROM users ORDER BY users.id"
                ),
                CompiledSQL(
                    "SELECT addresses.user_id AS addresses_user_id, "
                    "addresses.id AS addresses_id, addresses.email_address "
                    "AS addresses_email_address FROM addresses "
                    "WHERE addresses.user_id IN ([POSTCOMPILE_primary_keys]) "
                    "AND addresses.email_address != :email_address_1 "
                    "ORDER BY addresses.id",
                    [
                        {
                            "primary_keys": [7, 8, 9, 10],
                            "email_address_1": value,
                        }
                    ],
                ),
            )

    def test_lazyload_local_criteria(self, user_address_fixture):
        User, Address = user_address_fixture

        s = Session(testing.db, future=True)

        def go(value):
            s.close()
            stmt = (
                select(User)
                .options(
                    lazyload(
                        User.addresses.and_(Address.email_address != value)
                    ),
                )
                .order_by(User.id)
            )
            result = s.execute(stmt)
            return result

        for value in "ed@wood.com", "ed@lala.com":
            with self.sql_execution_asserter() as asserter:

                result = go(value)

                eq_(
                    result.scalars().unique().all(),
                    self._user_minus_edwood(*user_address_fixture)
                    if value == "ed@wood.com"
                    else self._user_minus_edlala(*user_address_fixture),
                )

            asserter.assert_(
                CompiledSQL(
                    "SELECT users.id, users.name FROM users ORDER BY users.id"
                ),
                CompiledSQL(
                    "SELECT addresses.id AS addresses_id, "
                    "addresses.user_id AS addresses_user_id, "
                    "addresses.email_address AS addresses_email_address "
                    "FROM addresses WHERE :param_1 = addresses.user_id "
                    "AND addresses.email_address != :email_address_1 "
                    "ORDER BY addresses.id",
                    [{"param_1": 7, "email_address_1": value}],
                ),
                CompiledSQL(
                    "SELECT addresses.id AS addresses_id, "
                    "addresses.user_id AS addresses_user_id, "
                    "addresses.email_address AS addresses_email_address "
                    "FROM addresses WHERE :param_1 = addresses.user_id "
                    "AND addresses.email_address != :email_address_1 "
                    "ORDER BY addresses.id",
                    [{"param_1": 8, "email_address_1": value}],
                ),
                CompiledSQL(
                    "SELECT addresses.id AS addresses_id, "
                    "addresses.user_id AS addresses_user_id, "
                    "addresses.email_address AS addresses_email_address "
                    "FROM addresses WHERE :param_1 = addresses.user_id "
                    "AND addresses.email_address != :email_address_1 "
                    "ORDER BY addresses.id",
                    [{"param_1": 9, "email_address_1": value}],
                ),
                CompiledSQL(
                    "SELECT addresses.id AS addresses_id, "
                    "addresses.user_id AS addresses_user_id, "
                    "addresses.email_address AS addresses_email_address "
                    "FROM addresses WHERE :param_1 = addresses.user_id "
                    "AND addresses.email_address != :email_address_1 "
                    "ORDER BY addresses.id",
                    [{"param_1": 10, "email_address_1": value}],
                ),
            )

    def test_subqueryload_local_criteria(self, user_address_fixture):
        User, Address = user_address_fixture

        s = Session(testing.db, future=True)

        def go(value):
            s.close()
            stmt = (
                select(User)
                .options(
                    subqueryload(
                        User.addresses.and_(Address.email_address != value)
                    ),
                )
                .order_by(User.id)
            )
            result = s.execute(stmt)
            return result

        for value in "ed@wood.com", "ed@lala.com":
            with self.sql_execution_asserter() as asserter:

                result = go(value)

                eq_(
                    result.scalars().unique().all(),
                    self._user_minus_edwood(*user_address_fixture)
                    if value == "ed@wood.com"
                    else self._user_minus_edlala(*user_address_fixture),
                )

            asserter.assert_(
                CompiledSQL(
                    "SELECT users.id, users.name FROM users ORDER BY users.id"
                ),
                CompiledSQL(
                    "SELECT addresses.id AS addresses_id, addresses.user_id "
                    "AS addresses_user_id, addresses.email_address "
                    "AS addresses_email_address, anon_1.users_id "
                    "AS anon_1_users_id FROM (SELECT users.id AS users_id "
                    "FROM users) AS anon_1 "
                    "JOIN addresses ON anon_1.users_id = "
                    "addresses.user_id AND "
                    "addresses.email_address != :email_address_1 "
                    "ORDER BY addresses.id",
                    [{"email_address_1": value}],
                ),
            )

    def test_query_join_local_criteria(self, user_address_fixture):
        User, Address = user_address_fixture

        s = Session(testing.db)

        q = s.query(User).join(
            User.addresses.and_(Address.email_address != "email")
        )

        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN addresses ON users.id = addresses.user_id "
            "AND addresses.email_address != :email_address_1",
        )

    def test_select_join_local_criteria(self, user_address_fixture):
        User, Address = user_address_fixture

        stmt = select(User).join(
            User.addresses.and_(Address.email_address != "email")
        )

        self.assert_compile(
            stmt,
            "SELECT users.id, users.name FROM users JOIN addresses "
            "ON users.id = addresses.user_id "
            "AND addresses.email_address != :email_address_1",
        )

    def test_select_joinm2m_local_criteria(self, order_item_fixture):
        Order, Item = order_item_fixture

        stmt = select(Order).join(
            Order.items.and_(Item.description != "description")
        )

        self.assert_compile(
            stmt,
            "SELECT orders.id, orders.user_id, orders.address_id, "
            "orders.description, orders.isopen "
            "FROM orders JOIN order_items AS order_items_1 "
            "ON orders.id = order_items_1.order_id "
            "JOIN items ON items.id = order_items_1.item_id "
            "AND items.description != :description_1",
        )

    def test_select_joinm2m_aliased_local_criteria(self, order_item_fixture):
        Order, Item = order_item_fixture

        i1 = aliased(Item)
        stmt = select(Order).join(
            Order.items.of_type(i1).and_(i1.description != "description")
        )

        self.assert_compile(
            stmt,
            "SELECT orders.id, orders.user_id, orders.address_id, "
            "orders.description, orders.isopen "
            "FROM orders JOIN order_items AS order_items_1 "
            "ON orders.id = order_items_1.order_id "
            "JOIN items AS items_1 ON items_1.id = order_items_1.item_id "
            "AND items_1.description != :description_1",
        )
