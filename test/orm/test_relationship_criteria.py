from __future__ import annotations

import datetime
from functools import partial
import random
from typing import List

from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import delete
from sqlalchemy import event
from sqlalchemy import exc as sa_exc
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import insert
from sqlalchemy import Integer
from sqlalchemy import literal_column
from sqlalchemy import orm
from sqlalchemy import select
from sqlalchemy import sql
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import union
from sqlalchemy import update
from sqlalchemy.orm import aliased
from sqlalchemy.orm import column_property
from sqlalchemy.orm import contains_eager
from sqlalchemy.orm import defer
from sqlalchemy.orm import join as orm_join
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import lazyload
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import registry
from sqlalchemy.orm import relationship
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import Session
from sqlalchemy.orm import subqueryload
from sqlalchemy.orm import with_loader_criteria
from sqlalchemy.orm.decl_api import declared_attr
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import expect_raises
from sqlalchemy.testing.assertsql import CompiledSQL
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.util import resolve_lambda
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

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "addresses": relationship(
                    self.mapper_registry.map_imperatively(Address, addresses),
                    order_by=Address.id,
                )
            },
        )
        return User, Address

    @testing.fixture
    def user_address_col_property_fixture(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(Address, addresses)

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address,
                    order_by=Address.id,
                ),
                "num_addresses": column_property(
                    select(func.count(Address.id))
                    .where(Address.user_id == users.c.id)
                    .correlate_except(Address)
                    .scalar_subquery()
                ),
            },
        )
        return User, Address

    @testing.fixture
    def user_address_custom_strat_fixture(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        def go(strat):
            self.mapper_registry.map_imperatively(
                User,
                users,
                properties={
                    "addresses": relationship(
                        self.mapper_registry.map_imperatively(
                            Address, addresses
                        ),
                        lazy=strat,
                        order_by=Address.id,
                    )
                },
            )
            return User, Address

        return go

    @testing.fixture
    def order_item_fixture(self):
        Order, Item = self.classes("Order", "Item")
        orders, items, order_items = self.tables(
            "orders", "items", "order_items"
        )

        self.mapper_registry.map_imperatively(
            Order,
            orders,
            properties={
                # m2m
                "items": relationship(
                    Item, secondary=order_items, order_by=items.c.id
                ),
            },
        )
        self.mapper_registry.map_imperatively(Item, items)

        return Order, Item

    @testing.fixture
    def user_order_item_fixture(self):
        User, Order, Item = self.classes("User", "Order", "Item")
        users, orders, items, order_items = self.tables(
            "users", "orders", "items", "order_items"
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={"orders": relationship(Order, order_by=orders.c.id)},
        )
        self.mapper_registry.map_imperatively(
            Order,
            orders,
            properties={
                # m2m
                "items": relationship(
                    Item, secondary=order_items, order_by=items.c.id
                ),
            },
        )
        self.mapper_registry.map_imperatively(Item, items)

        return User, Order, Item

    @testing.fixture
    def mixin_fixture(self):
        users = self.tables.users

        class HasFoob:
            name = Column(String)

        class UserWFoob(HasFoob, self.Comparable):
            pass

        self.mapper_registry.map_imperatively(
            UserWFoob,
            users,
        )
        return HasFoob, UserWFoob

    @testing.fixture
    def declattr_mixin_fixture(self):
        users = self.tables.users

        class HasFoob:
            @declared_attr
            def name(cls):
                return Column(String)

        class UserWFoob(HasFoob, self.Comparable):
            pass

        self.mapper_registry.map_imperatively(
            UserWFoob,
            users,
        )
        return HasFoob, UserWFoob

    @testing.fixture
    def multi_mixin_fixture(self):
        orders, items = self.tables.orders, self.tables.items
        order_items = self.tables.order_items

        class HasFoob:
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

    def test_err_given_in_pathed(self, user_address_fixture):
        User, Address = user_address_fixture

        with expect_raises_message(
            sa_exc.ArgumentError,
            r"Loader option <.*LoaderCriteriaOption.*> is not compatible "
            r"with the Load.options\(\) method.",
        ):
            select(User).options(
                selectinload(User.addresses).options(
                    with_loader_criteria(
                        Address, Address.email_address != "foo"
                    )
                )
            )

    def test_criteria_post_replace(self, user_address_fixture):
        User, Address = user_address_fixture

        stmt = (
            select(User)
            .select_from(User)
            .options(with_loader_criteria(User, User.name != "name"))
            .with_only_columns(func.count())
        )

        self.assert_compile(
            stmt,
            "SELECT count(*) AS count_1 FROM users "
            "WHERE users.name != :name_1",
        )

    @testing.combinations(
        (
            lambda User, Address: select(Address)
            .select_from(User)
            .join(User.addresses)
            .options(with_loader_criteria(User, User.name != "name")),
        ),
        (
            # issue #10365
            lambda User, Address: select(Address)
            .select_from(User)
            .join(Address, User.id == Address.user_id)
            .options(with_loader_criteria(User, User.name != "name")),
        ),
        (
            lambda User, Address: select(Address)
            .select_from(orm_join(User, Address, User.addresses))
            .options(with_loader_criteria(User, User.name != "name")),
        ),
        (
            lambda User, Address: select(Address)
            .join_from(User, Address, User.addresses)
            .options(with_loader_criteria(User, User.name != "name")),
        ),
        argnames="stmt_fn",
    )
    @testing.combinations(True, False, argnames="alias_user")
    def test_criteria_select_from_w_join_left(
        self, user_address_fixture, stmt_fn, alias_user
    ):
        """test #8721"""
        User, Address = user_address_fixture

        if alias_user:
            User = aliased(User)

        stmt = testing.resolve_lambda(stmt_fn, User=User, Address=Address)

        if alias_user:
            self.assert_compile(
                stmt,
                "SELECT addresses.id, addresses.user_id, "
                "addresses.email_address FROM users AS users_1 "
                "JOIN addresses ON users_1.id = addresses.user_id "
                "WHERE users_1.name != :name_1",
            )
        else:
            self.assert_compile(
                stmt,
                "SELECT addresses.id, addresses.user_id, "
                "addresses.email_address "
                "FROM users JOIN addresses ON users.id = addresses.user_id "
                "WHERE users.name != :name_1",
            )

    @testing.combinations(
        (
            lambda User, Address: select(Address.id, User.id)
            .select_from(User)
            .join(User.addresses)
            .options(with_loader_criteria(User, User.name != "name")),
        ),
        (
            # issue #10365 - this seems to have already worked
            lambda User, Address: select(Address.id, User.id)
            .select_from(User)
            .join(Address, User.id == Address.user_id)
            .options(with_loader_criteria(User, User.name != "name")),
        ),
        (
            lambda User, Address: select(Address.id, User.id)
            .select_from(orm_join(User, Address, User.addresses))
            .options(with_loader_criteria(User, User.name != "name")),
        ),
        (
            lambda User, Address: select(Address.id, User.id)
            .join_from(User, Address, User.addresses)
            .options(with_loader_criteria(User, User.name != "name")),
        ),
        argnames="stmt_fn",
    )
    @testing.combinations(True, False, argnames="alias_user")
    def test_criteria_select_from_w_join_left_including_entity(
        self, user_address_fixture, stmt_fn, alias_user
    ):
        """test #8721"""
        User, Address = user_address_fixture

        if alias_user:
            User = aliased(User)

        stmt = testing.resolve_lambda(stmt_fn, User=User, Address=Address)

        if alias_user:
            self.assert_compile(
                stmt,
                "SELECT addresses.id, users_1.id AS id_1 "
                "FROM users AS users_1 JOIN addresses "
                "ON users_1.id = addresses.user_id "
                "WHERE users_1.name != :name_1",
            )
        else:
            self.assert_compile(
                stmt,
                "SELECT addresses.id, users.id AS id_1 "
                "FROM users JOIN addresses ON users.id = addresses.user_id "
                "WHERE users.name != :name_1",
            )

    @testing.combinations(
        (
            lambda User, Address: select(Address)
            .select_from(User)
            .join(User.addresses)
            .options(
                with_loader_criteria(Address, Address.email_address != "email")
            ),
        ),
        (
            # issue #10365
            lambda User, Address: select(Address)
            .select_from(User)
            .join(Address, User.id == Address.user_id)
            .options(
                with_loader_criteria(Address, Address.email_address != "email")
            ),
        ),
        (
            # for orm_join(), this is set up before we have the context
            # available that allows with_loader_criteria to be set up
            # correctly
            lambda User, Address: select(Address)
            .select_from(orm_join(User, Address, User.addresses))
            .options(
                with_loader_criteria(Address, Address.email_address != "email")
            ),
            testing.fails("not implemented right now"),
        ),
        (
            lambda User, Address: select(Address)
            .join_from(User, Address, User.addresses)
            .options(
                with_loader_criteria(Address, Address.email_address != "email")
            ),
        ),
        argnames="stmt_fn",
    )
    def test_criteria_select_from_w_join_right(
        self, user_address_fixture, stmt_fn
    ):
        """test #8721"""
        User, Address = user_address_fixture

        stmt = testing.resolve_lambda(stmt_fn, User=User, Address=Address)
        self.assert_compile(
            stmt,
            "SELECT addresses.id, addresses.user_id, addresses.email_address "
            "FROM users JOIN addresses ON users.id = addresses.user_id "
            "AND addresses.email_address != :email_address_1",
        )

    @testing.combinations(
        "select",
        "joined",
        "subquery",
        "selectin",
        "immediate",
        argnames="loader_strategy",
    )
    def test_loader_strategy_on_refresh(
        self, loader_strategy, user_address_custom_strat_fixture
    ):
        User, Address = user_address_custom_strat_fixture(loader_strategy)

        sess = fixture_session()

        @event.listens_for(sess, "do_orm_execute")
        def add_criteria(orm_context):
            orm_context.statement = orm_context.statement.options(
                with_loader_criteria(
                    Address,
                    ~Address.id.in_([5, 3]),
                )
            )

        u1 = sess.get(User, 7)
        u2 = sess.get(User, 8)
        eq_(u1.addresses, [Address(id=1)])
        eq_(u2.addresses, [Address(id=2), Address(id=4)])

        for i in range(3):
            sess.expire_all()
            eq_(u1.addresses, [Address(id=1)])
            eq_(u2.addresses, [Address(id=2), Address(id=4)])

    def test_criteria_post_replace_legacy(self, user_address_fixture):
        User, Address = user_address_fixture

        s = fixture_session()
        stmt = (
            s.query(User)
            .select_from(User)
            .options(with_loader_criteria(User, User.name != "name"))
            .with_entities(func.count())
        )

        self.assert_compile(
            stmt,
            "SELECT count(*) AS count_1 FROM users "
            "WHERE users.name != :name_1",
        )

    def test_criteria_applies_to_column_property(
        self, user_address_col_property_fixture
    ):
        """test related to #8064, added after discussion #9091 which
        requested this behavior for with_loader_criteria() where it was
        found to be working as of this issue, just not tested"""

        User, Address = user_address_col_property_fixture

        stmt = select(User)

        self.assert_compile(
            stmt,
            "SELECT (SELECT count(addresses.id) AS count_1 FROM addresses "
            "WHERE addresses.user_id = users.id) AS anon_1, "
            "users.id, users.name FROM users",
        )

        stmt = select(User).options(
            with_loader_criteria(
                Address, Address.email_address != "email_address"
            )
        )

        self.assert_compile(
            stmt,
            "SELECT (SELECT count(addresses.id) AS count_1 FROM addresses "
            "WHERE addresses.user_id = users.id AND "
            "addresses.email_address != :email_address_1) AS anon_1, "
            "users.id, users.name FROM users",
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

    def test_with_loader_criteria_recursion_check_scalar_subq(
        self, user_address_fixture
    ):
        """test #7491"""

        User, Address = user_address_fixture
        subq = select(Address).where(Address.id == 8).scalar_subquery()
        stmt = (
            select(User)
            .join(Address)
            .options(with_loader_criteria(Address, Address.id == subq))
        )
        self.assert_compile(
            stmt,
            "SELECT users.id, users.name FROM users JOIN addresses "
            "ON users.id = addresses.user_id AND addresses.id = "
            "(SELECT addresses.id, addresses.user_id, "
            "addresses.email_address FROM addresses "
            "WHERE addresses.id = :id_1)",
        )

    def test_with_loader_criteria_recursion_check_from_subq(
        self, user_address_fixture
    ):
        """test #7491"""

        User, Address = user_address_fixture
        subq = select(Address).where(Address.id == 8).subquery()
        stmt = (
            select(User)
            .join(Address)
            .options(with_loader_criteria(Address, Address.id == subq.c.id))
        )
        # note this query is incorrect SQL right now.   This is a current
        # artifact of how with_loader_criteria() is used and may be considered
        # a bug at some point, in which case if fixed this query can be
        # changed.  the main thing we are testing at the moment is that
        # there is not a recursion overflow.
        self.assert_compile(
            stmt,
            "SELECT users.id, users.name FROM users JOIN addresses "
            "ON users.id = addresses.user_id AND addresses.id = anon_1.id",
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

    @testing.variation("style", ["direct_union", "from_statement"])
    @testing.variation("add_nested_union", [True, False])
    def test_select_mapper_columns_w_union_mapper_criteria(
        self, multi_mixin_fixture, style: testing.Variation, add_nested_union
    ):
        """test #9635"""
        HasFoob, Order, Item = multi_mixin_fixture

        stmt = (
            select(Order.id, Order.description)
            .where(Order.id > 8)
            .union(select(Order.id, Order.description).where(Order.id <= 8))
        )

        if add_nested_union:
            stmt = union(
                stmt,
                union(
                    select(Item.id, Item.description).where(Item.id <= 8),
                    select(Item.id, Item.description).where(Item.id > 8),
                ),
            )

        if style.direct_union:
            stmt = stmt.options(
                with_loader_criteria(
                    HasFoob,
                    lambda cls: cls.description != "name",
                    include_aliases=True,
                )
            )
        elif style.from_statement:
            stmt = (
                select(Order.id, Order.description)
                .from_statement(stmt)
                .options(
                    with_loader_criteria(
                        HasFoob,
                        lambda cls: cls.description != "name",
                        include_aliases=True,
                    )
                )
            )

        else:
            style.fail()

        if add_nested_union:
            # the criteria is embedded into all UNIONS regardless of nesting.
            self.assert_compile(
                stmt,
                "(SELECT orders.id, orders.description FROM orders WHERE "
                "orders.id > :id_1 AND orders.description != :description_1 "
                "UNION SELECT orders.id, orders.description FROM orders WHERE "
                "orders.id <= :id_2 AND orders.description != :description_2) "
                "UNION (SELECT items.id, items.description FROM items WHERE "
                "items.id <= :id_3 AND items.description != :description_3 "
                "UNION SELECT items.id, items.description FROM items WHERE "
                "items.id > :id_4 AND items.description != :description_4)",
                checkparams={
                    "id_1": 8,
                    "description_1": "name",
                    "id_2": 8,
                    "description_2": "name",
                    "id_3": 8,
                    "description_3": "name",
                    "id_4": 8,
                    "description_4": "name",
                },
            )
        else:
            self.assert_compile(
                stmt,
                "SELECT orders.id, orders.description FROM orders WHERE "
                "orders.id > :id_1 AND orders.description != :description_1 "
                "UNION SELECT orders.id, orders.description FROM orders WHERE "
                "orders.id <= :id_2 AND orders.description != :description_2",
                checkparams={
                    "description_1": "name",
                    "description_2": "name",
                    "id_1": 8,
                    "id_2": 8,
                },
            )

    def test_select_mapper_columns_w_core_dml_mapper_criteria(
        self, multi_mixin_fixture
    ):
        """test #9635"""
        HasFoob, Order, Item = multi_mixin_fixture

        stmt = (
            insert(Order)
            .from_select(
                ["id", "description"],
                select(Order.id, Order.description).where(Order.id > 8),
            )
            .options(
                with_loader_criteria(
                    HasFoob,
                    lambda cls: cls.description != "name",
                    include_aliases=True,
                )
            )
        )

        self.assert_compile(
            stmt,
            "INSERT INTO orders (id, description) SELECT orders.id, "
            "orders.description FROM orders WHERE orders.id > :id_1 "
            "AND orders.description != :description_1",
            checkparams={"description_1": "name", "id_1": 8},
        )

    @testing.variation("update_is_orm", [True, False])
    def test_select_mapper_columns_w_core_cte_update_mapper_criteria(
        self, multi_mixin_fixture, update_is_orm
    ):
        """test #9635"""
        HasFoob, Order, Item = multi_mixin_fixture

        cte = select(Order).cte("pd")

        if update_is_orm:
            stmt = (
                update(Order)
                .where(Order.id == cte.c.id)
                .values(description="newname")
            )
        else:
            stmt = (
                update(Order.__table__)
                .where(Order.__table__.c.id == cte.c.id)
                .values(description="newname")
            )

        stmt = stmt.options(
            with_loader_criteria(
                HasFoob,
                lambda cls: cls.description != "name",
                include_aliases=True,
            )
        )

        if update_is_orm:
            self.assert_compile(
                stmt,
                "WITH pd AS (SELECT orders.id AS id, "
                "orders.user_id AS user_id, "
                "orders.address_id AS address_id, "
                "orders.description AS description, orders.isopen AS isopen "
                "FROM orders WHERE orders.description != %(description_1)s) "
                "UPDATE orders SET description=%(description)s "
                "FROM pd WHERE orders.id = pd.id "
                "AND orders.description != %(description_2)s",
                dialect="postgresql",
                checkparams={
                    "description": "newname",
                    "description_1": "name",
                    "description_2": "name",
                },
            )
        else:
            # non ORM update, no criteria, but criteria still gets rendered
            # inside the SELECT
            self.assert_compile(
                stmt,
                "WITH pd AS (SELECT orders.id AS id, "
                "orders.user_id AS user_id, "
                "orders.address_id AS address_id, "
                "orders.description AS description, orders.isopen AS isopen "
                "FROM orders WHERE orders.description != %(description_1)s) "
                "UPDATE orders SET description=%(description)s "
                "FROM pd WHERE orders.id = pd.id",
                dialect="postgresql",
                checkparams={
                    "description": "newname",
                    "description_1": "name",
                },
            )

    @testing.variation("delete_is_orm", [True, False])
    def test_select_mapper_columns_w_core_cte_delete_mapper_criteria(
        self, multi_mixin_fixture, delete_is_orm
    ):
        """test #9635"""
        HasFoob, Order, Item = multi_mixin_fixture

        cte = select(Order).cte("pd")

        if delete_is_orm:
            stmt = delete(Order).where(Order.id == cte.c.id)
        else:
            stmt = delete(Order.__table__).where(
                Order.__table__.c.id == cte.c.id
            )

        stmt = stmt.options(
            with_loader_criteria(
                HasFoob,
                lambda cls: cls.description != "name",
                include_aliases=True,
            )
        )

        if delete_is_orm:
            self.assert_compile(
                stmt,
                "WITH pd AS (SELECT orders.id AS id, orders.user_id AS "
                "user_id, orders.address_id AS address_id, "
                "orders.description AS description, orders.isopen AS isopen "
                "FROM orders WHERE orders.description != %(description_1)s) "
                "DELETE FROM orders USING pd WHERE orders.id = pd.id "
                "AND orders.description != %(description_2)s",
                dialect="postgresql",
                checkparams={"description_1": "name", "description_2": "name"},
            )
        else:
            # non ORM update, no criteria, but criteria still gets rendered
            # inside the SELECT
            self.assert_compile(
                stmt,
                "WITH pd AS (SELECT orders.id AS id, orders.user_id AS "
                "user_id, orders.address_id AS address_id, "
                "orders.description AS description, orders.isopen AS isopen "
                "FROM orders WHERE orders.description != %(description_1)s) "
                "DELETE FROM orders USING pd WHERE orders.id = pd.id",
                dialect="postgresql",
                checkparams={"description_1": "name"},
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

    def test_select_implicit_join_mapper_mapper_criteria(
        self, user_address_fixture
    ):
        User, Address = user_address_fixture

        stmt = (
            select(User)
            .join(Address)
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
                "SELECT addresses.user_id, addresses.id, "
                "addresses.email_address "
                "FROM addresses "
                "WHERE addresses.user_id IN (__[POSTCOMPILE_primary_keys]) "
                "AND addresses.email_address != :email_address_1 "
                "ORDER BY addresses.id",
                [{"primary_keys": [7, 8, 9, 10], "email_address_1": "name"}],
            ),
        )

    def test_select_selectinload_mapper_mapper_closure_criteria(
        self, user_address_fixture
    ):
        User, Address = user_address_fixture

        def get_statement(closure="name"):
            stmt = select(User).options(
                selectinload(User.addresses),
                with_loader_criteria(
                    Address, lambda cls: cls.email_address != closure
                ),
            )
            return stmt

        s = Session(testing.db, future=True)

        stmt = get_statement(closure="name")
        with self.sql_execution_asserter() as asserter:
            s.execute(stmt).all()

        asserter.assert_(
            CompiledSQL(
                "SELECT users.id, users.name FROM users",
                [],
            ),
            CompiledSQL(
                "SELECT addresses.user_id, addresses.id, "
                "addresses.email_address "
                "FROM addresses "
                "WHERE addresses.user_id IN (__[POSTCOMPILE_primary_keys]) "
                "AND addresses.email_address != :closure_1 "
                "ORDER BY addresses.id",
                [{"primary_keys": [7, 8, 9, 10], "closure_1": "name"}],
            ),
        )

        stmt = get_statement(closure="new name")
        with self.sql_execution_asserter() as asserter:
            s.execute(stmt).all()

        asserter.assert_(
            CompiledSQL(
                "SELECT users.id, users.name FROM users",
                [],
            ),
            CompiledSQL(
                "SELECT addresses.user_id, addresses.id, "
                "addresses.email_address "
                "FROM addresses "
                "WHERE addresses.user_id IN (__[POSTCOMPILE_primary_keys]) "
                "AND addresses.email_address != :closure_1 "
                "ORDER BY addresses.id",
                [{"primary_keys": [7, 8, 9, 10], "closure_1": "new name"}],
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
                "SELECT addresses.id, "
                "addresses.user_id, "
                "addresses.email_address "
                "FROM addresses WHERE :param_1 = addresses.user_id "
                "AND addresses.email_address != :email_address_1 "
                "ORDER BY addresses.id",
                [{"param_1": 7, "email_address_1": "name"}],
            ),
            CompiledSQL(
                "SELECT addresses.id, "
                "addresses.user_id, "
                "addresses.email_address "
                "FROM addresses WHERE :param_1 = addresses.user_id "
                "AND addresses.email_address != :email_address_1 "
                "ORDER BY addresses.id",
                [{"param_1": 8, "email_address_1": "name"}],
            ),
            CompiledSQL(
                "SELECT addresses.id, "
                "addresses.user_id, "
                "addresses.email_address "
                "FROM addresses WHERE :param_1 = addresses.user_id "
                "AND addresses.email_address != :email_address_1 "
                "ORDER BY addresses.id",
                [{"param_1": 9, "email_address_1": "name"}],
            ),
            CompiledSQL(
                "SELECT addresses.id, "
                "addresses.user_id, "
                "addresses.email_address "
                "FROM addresses WHERE :param_1 = addresses.user_id "
                "AND addresses.email_address != :email_address_1 "
                "ORDER BY addresses.id",
                [{"param_1": 10, "email_address_1": "name"}],
            ),
        )

    def test_select_lazyload_mapper_mapper_closure_criteria(
        self, user_address_fixture
    ):
        User, Address = user_address_fixture

        def get_statement(closure="name"):
            stmt = (
                select(User)
                .options(
                    lazyload(User.addresses),
                    with_loader_criteria(
                        Address, lambda cls: cls.email_address != closure
                    ),
                )
                .order_by(User.id)
            )
            return stmt

        s = Session(testing.db, future=True)

        stmt = get_statement(closure="name")
        with self.sql_execution_asserter() as asserter:
            for obj in s.scalars(stmt).all():
                obj.addresses

        asserter.assert_(
            CompiledSQL(
                "SELECT users.id, users.name FROM users ORDER BY users.id",
                [],
            ),
            CompiledSQL(
                "SELECT addresses.id, "
                "addresses.user_id, "
                "addresses.email_address "
                "FROM addresses WHERE :param_1 = addresses.user_id "
                "AND addresses.email_address != :closure_1 "
                "ORDER BY addresses.id",
                [{"param_1": 7, "closure_1": "name"}],
            ),
            CompiledSQL(
                "SELECT addresses.id, "
                "addresses.user_id, "
                "addresses.email_address "
                "FROM addresses WHERE :param_1 = addresses.user_id "
                "AND addresses.email_address != :closure_1 "
                "ORDER BY addresses.id",
                [{"param_1": 8, "closure_1": "name"}],
            ),
            CompiledSQL(
                "SELECT addresses.id, "
                "addresses.user_id, "
                "addresses.email_address "
                "FROM addresses WHERE :param_1 = addresses.user_id "
                "AND addresses.email_address != :closure_1 "
                "ORDER BY addresses.id",
                [{"param_1": 9, "closure_1": "name"}],
            ),
            CompiledSQL(
                "SELECT addresses.id, "
                "addresses.user_id, "
                "addresses.email_address "
                "FROM addresses WHERE :param_1 = addresses.user_id "
                "AND addresses.email_address != :closure_1 "
                "ORDER BY addresses.id",
                [{"param_1": 10, "closure_1": "name"}],
            ),
        )

        stmt = get_statement(closure="new name")
        with self.sql_execution_asserter() as asserter:
            for obj in s.scalars(
                stmt, execution_options={"populate_existing": True}
            ).all():
                obj.addresses

        asserter.assert_(
            CompiledSQL(
                "SELECT users.id, users.name FROM users ORDER BY users.id",
                [],
            ),
            CompiledSQL(
                "SELECT addresses.id, "
                "addresses.user_id, "
                "addresses.email_address "
                "FROM addresses WHERE :param_1 = addresses.user_id "
                "AND addresses.email_address != :closure_1 "
                "ORDER BY addresses.id",
                [{"param_1": 7, "closure_1": "new name"}],
            ),
            CompiledSQL(
                "SELECT addresses.id, "
                "addresses.user_id, "
                "addresses.email_address "
                "FROM addresses WHERE :param_1 = addresses.user_id "
                "AND addresses.email_address != :closure_1 "
                "ORDER BY addresses.id",
                [{"param_1": 8, "closure_1": "new name"}],
            ),
            CompiledSQL(
                "SELECT addresses.id, "
                "addresses.user_id, "
                "addresses.email_address "
                "FROM addresses WHERE :param_1 = addresses.user_id "
                "AND addresses.email_address != :closure_1 "
                "ORDER BY addresses.id",
                [{"param_1": 9, "closure_1": "new name"}],
            ),
            CompiledSQL(
                "SELECT addresses.id, "
                "addresses.user_id, "
                "addresses.email_address "
                "FROM addresses WHERE :param_1 = addresses.user_id "
                "AND addresses.email_address != :closure_1 "
                "ORDER BY addresses.id",
                [{"param_1": 10, "closure_1": "new name"}],
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

    @testing.combinations(
        (lambda User: [User.id], "users.id"),
        (lambda User: [User.id.label("foo")], "users.id AS foo"),
        (lambda User: [User.name + "bar"], "users.name || :name_1 AS anon_1"),
        (
            lambda User: [(User.name + "bar").label("foo")],
            "users.name || :name_1 AS foo",
        ),
        (lambda User: [func.count(User.id)], "count(users.id) AS count_1"),
        (
            lambda User: [func.count(User.id).label("foo")],
            "count(users.id) AS foo",
        ),
        argnames="case, expected",
    )
    def test_select_expr_with_criteria(
        self, case, expected, user_address_fixture
    ):
        """test #7205"""
        User, Address = user_address_fixture

        stmt = select(*resolve_lambda(case, User=User)).options(
            # use non-bound value so that we dont have to accommodate for
            # the "anon" counter
            with_loader_criteria(
                User, User.name != literal_column("some_crit")
            )
        )

        self.assert_compile(
            stmt,
            "SELECT %s FROM users WHERE users.name != some_crit" % (expected,),
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

    def test_declared_attr_no_warning(self, declattr_mixin_fixture):
        HasFoob, UserWFoob = declattr_mixin_fixture

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
        class HasTemporal:
            """Mixin that identifies a class as having a timestamp column"""

            timestamp = Column(
                DateTime,
                default=partial(datetime.datetime.now, datetime.timezone.utc),
                nullable=False,
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
                *loader_options,
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
                *loader_options,
            )
        )
        if is_joined:
            result = result.unique()
        parents = result.scalars().all()

        assert parents[0] == p1
        assert parents[0].children == [c1, c2]


class RelationshipCriteriaTest(_Fixtures, testing.AssertsCompiledSQL):
    __dialect__ = "default"

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
                    (
                        self._user_minus_edwood(*user_address_fixture)
                        if value == "ed@wood.com"
                        else self._user_minus_edlala(*user_address_fixture)
                    ),
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

    @testing.combinations(
        lambda r: r.scalar(),
        lambda r: r.scalar_one(),
        lambda r: r.scalar_one_or_none(),
        argnames="get",
    )
    def test_joinedload_scalar(self, user_address_fixture, get):
        User, Address = user_address_fixture

        s = Session(testing.db, future=True)

        stmt = (
            select(User)
            .options(joinedload(User.addresses))
            .where(User.name == "jack")
        )
        r = s.execute(stmt).unique()

        jack = get(r)
        eq_(jack.name, "jack")

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
                    (
                        self._user_minus_edwood(*user_address_fixture)
                        if value == "ed@wood.com"
                        else self._user_minus_edlala(*user_address_fixture)
                    ),
                )

            asserter.assert_(
                CompiledSQL(
                    "SELECT users.id, users.name FROM users ORDER BY users.id"
                ),
                CompiledSQL(
                    "SELECT addresses.user_id, addresses.id, "
                    "addresses.email_address "
                    "FROM addresses "
                    "WHERE addresses.user_id IN "
                    "(__[POSTCOMPILE_primary_keys]) "
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

    def test_selectinload_local_criteria_subquery(self, user_address_fixture):
        """test #7489"""
        User, Address = user_address_fixture

        s = Session(testing.db, future=True)

        def go(value):
            a1 = aliased(Address)
            subq = select(a1.id).where(a1.email_address != value).subquery()
            stmt = (
                select(User)
                .options(
                    selectinload(User.addresses.and_(Address.id == subq.c.id)),
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
                    (
                        self._user_minus_edwood(*user_address_fixture)
                        if value == "ed@wood.com"
                        else self._user_minus_edlala(*user_address_fixture)
                    ),
                )

            asserter.assert_(
                CompiledSQL(
                    "SELECT users.id, users.name FROM users ORDER BY users.id"
                ),
                CompiledSQL(
                    "SELECT addresses.user_id, addresses.id, "
                    "addresses.email_address "
                    # note the comma-separated FROM clause
                    "FROM addresses, (SELECT addresses_1.id AS id FROM "
                    "addresses AS addresses_1 "
                    "WHERE addresses_1.email_address != :email_address_1) "
                    "AS anon_1 WHERE addresses.user_id "
                    "IN (__[POSTCOMPILE_primary_keys]) "
                    "AND addresses.id = anon_1.id ORDER BY addresses.id",
                    [
                        {
                            "primary_keys": [7, 8, 9, 10],
                            "email_address_1": value,
                        }
                    ],
                ),
            )

    @testing.combinations(
        (selectinload,),
        (subqueryload,),
        (lazyload,),
        (joinedload,),
        argnames="opt",
    )
    @testing.variation("use_in", [True, False])
    def test_opts_local_criteria_cachekey(
        self, opt, user_address_fixture, use_in
    ):
        """test #11173"""
        User, Address = user_address_fixture

        s = Session(testing.db, future=True)

        def go(value):
            if use_in:
                expr = ~Address.email_address.in_([value, "some_email"])
            else:
                expr = Address.email_address != value
            stmt = (
                select(User)
                .options(
                    opt(User.addresses.and_(expr)),
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
            result = go(value)

            eq_(
                result.scalars().unique().all(),
                (
                    self._user_minus_edwood(*user_address_fixture)
                    if value == "ed@wood.com"
                    else self._user_minus_edlala(*user_address_fixture)
                ),
            )

    @testing.combinations(
        (joinedload, False),
        (lazyload, True),
        (subqueryload, False),
        (selectinload, True),
        argnames="opt,results_supported",
    )
    def test_loader_criteria_subquery_w_same_entity(
        self, user_address_fixture, opt, results_supported
    ):
        """test #7491.

        note this test also uses the not-quite-supported form of subquery
        criteria introduced by #7489. where we also have to clone
        the subquery linked only from a column criteria.  this required
        additional changes to the _annotate() method that is also
        test here, which is why two of the loader strategies still fail;
        we're just testing that there's no recursion overflow with this
        very particular form.

        """
        User, Address = user_address_fixture

        s = Session(testing.db, future=True)

        def go(value):
            subq = (
                select(Address.id)
                .where(Address.email_address != value)
                .subquery()
            )
            stmt = (
                select(User)
                .options(
                    # subquery here would need to be added to the FROM
                    # clause.  this isn't quite supported and won't work
                    # right now with joinedoad() or subqueryload().
                    opt(User.addresses.and_(Address.id == subq.c.id)),
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

            if not results_supported:
                # for joinedload and subqueryload, the query generated here
                # is invalid right now; this is because it's already not
                # quite a supported pattern to refer to a subquery-bound
                # column in loader criteria.  However, the main thing we want
                # to prevent here is the recursion overflow, so make sure
                # we get a DBAPI error at least indicating compilation
                # succeeded.
                with expect_raises(sa_exc.DBAPIError):
                    go(value).scalars().unique().all()
            else:
                result = go(value).scalars().unique().all()

                eq_(
                    result,
                    (
                        self._user_minus_edwood(*user_address_fixture)
                        if value == "ed@wood.com"
                        else self._user_minus_edlala(*user_address_fixture)
                    ),
                )

    @testing.combinations((True,), (False,), argnames="use_compiled_cache")
    def test_selectinload_nested_criteria(
        self, user_order_item_fixture, use_compiled_cache
    ):
        User, Order, Item = user_order_item_fixture

        if not use_compiled_cache:
            s = Session(
                testing.db.execution_options(compiled_cache=None), future=True
            )
        else:
            s = Session(testing.db, future=True)

        def go(order_description, item_description):
            stmt = (
                select(User)
                .where(User.id == 7)
                .options(
                    selectinload(
                        User.orders.and_(
                            Order.description == order_description
                        )
                    ).joinedload(
                        Order.items.and_(Item.description == item_description)
                    ),
                )
            )
            return s.execute(stmt)

        for order_description, item_description, oid, iid in (
            ("order 3", "item 3", 3, 3),
            ("order 3", "item 4", 3, 4),
            ("order 3", "item 4", 3, 4),
            ("order 5", "item 5", 5, 5),
            ("order 3", "item 3", 3, 3),
            ("order 5", "item 5", 5, 5),
        ):
            s.close()
            with self.sql_execution_asserter() as asserter:
                result = go(order_description, item_description)

                eq_(
                    result.scalars().unique().all(),
                    [User(id=7, orders=[Order(id=oid, items=[Item(id=iid)])])],
                )
            asserter.assert_(
                CompiledSQL(
                    "SELECT users.id, users.name FROM users "
                    "WHERE users.id = :id_1",
                    [{"id_1": 7}],
                ),
                CompiledSQL(
                    "SELECT orders.user_id, "
                    "orders.id, "
                    "orders.address_id, "
                    "orders.description, "
                    "orders.isopen, "
                    "items_1.id, "
                    "items_1.description "
                    "FROM orders LEFT OUTER JOIN "
                    "(order_items AS order_items_1 "
                    "JOIN items AS items_1 "
                    "ON items_1.id = order_items_1.item_id "
                    "AND items_1.description = :description_1) "
                    "ON orders.id = order_items_1.order_id "
                    "WHERE orders.user_id IN (__[POSTCOMPILE_primary_keys]) "
                    "AND orders.description = :description_2 "
                    "ORDER BY orders.id, items_1.id",
                    [
                        {
                            "description_1": item_description,
                            "primary_keys": [7],
                            "description_2": order_description,
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
                    (
                        self._user_minus_edwood(*user_address_fixture)
                        if value == "ed@wood.com"
                        else self._user_minus_edlala(*user_address_fixture)
                    ),
                )

            asserter.assert_(
                CompiledSQL(
                    "SELECT users.id, users.name FROM users ORDER BY users.id"
                ),
                CompiledSQL(
                    "SELECT addresses.id, "
                    "addresses.user_id, "
                    "addresses.email_address "
                    "FROM addresses WHERE :param_1 = addresses.user_id "
                    "AND addresses.email_address != :email_address_1 "
                    "ORDER BY addresses.id",
                    [{"param_1": 7, "email_address_1": value}],
                ),
                CompiledSQL(
                    "SELECT addresses.id, "
                    "addresses.user_id, "
                    "addresses.email_address "
                    "FROM addresses WHERE :param_1 = addresses.user_id "
                    "AND addresses.email_address != :email_address_1 "
                    "ORDER BY addresses.id",
                    [{"param_1": 8, "email_address_1": value}],
                ),
                CompiledSQL(
                    "SELECT addresses.id, "
                    "addresses.user_id, "
                    "addresses.email_address "
                    "FROM addresses WHERE :param_1 = addresses.user_id "
                    "AND addresses.email_address != :email_address_1 "
                    "ORDER BY addresses.id",
                    [{"param_1": 9, "email_address_1": value}],
                ),
                CompiledSQL(
                    "SELECT addresses.id, "
                    "addresses.user_id, "
                    "addresses.email_address "
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
                    (
                        self._user_minus_edwood(*user_address_fixture)
                        if value == "ed@wood.com"
                        else self._user_minus_edlala(*user_address_fixture)
                    ),
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

    def test_use_secondary_table_in_criteria(self, order_item_fixture):
        """test #11010 , regression caused by #9779"""

        Order, Item = order_item_fixture
        order_items = self.tables.order_items

        stmt = select(Order).join(
            Order.items.and_(
                order_items.c.item_id > 1, Item.description != "description"
            )
        )

        self.assert_compile(
            stmt,
            "SELECT orders.id, orders.user_id, orders.address_id, "
            "orders.description, orders.isopen FROM orders JOIN order_items "
            "AS order_items_1 ON orders.id = order_items_1.order_id "
            "JOIN items ON items.id = order_items_1.item_id "
            "AND order_items_1.item_id > :item_id_1 "
            "AND items.description != :description_1",
        )


class SubqueryCriteriaTest(fixtures.DeclarativeMappedTest):
    """test #10223"""

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Temperature(Base):
            __tablename__ = "temperature"
            id: Mapped[int] = mapped_column(primary_key=True)
            pointless_flag: Mapped[bool]

        class Color(Base):
            __tablename__ = "color"
            id: Mapped[int] = mapped_column(primary_key=True)
            name: Mapped[str] = mapped_column(String(50))
            temperature_id: Mapped[int] = mapped_column(
                ForeignKey("temperature.id")
            )
            temperature: Mapped[Temperature] = relationship()

        room_connections = Table(
            "room_connections",
            Base.metadata,
            Column(
                "room_a_id",
                Integer,
                # mariadb does not like this FK constraint
                # ForeignKey("room.id"),
                primary_key=True,
            ),
            Column(
                "room_b_id",
                Integer,
                # mariadb does not like this FK constraint
                # ForeignKey("room.id"),
                primary_key=True,
            ),
        )

        class Room(Base):
            __tablename__ = "room"
            id: Mapped[int] = mapped_column(primary_key=True)
            token: Mapped[str] = mapped_column(String(50))
            color_id: Mapped[int] = mapped_column(ForeignKey("color.id"))
            color: Mapped[Color] = relationship()
            connected_rooms: Mapped[List["Room"]] = relationship(  # noqa: F821
                secondary=room_connections,
                primaryjoin=id == room_connections.c.room_a_id,
                secondaryjoin=id == room_connections.c.room_b_id,
            )

    @classmethod
    def insert_data(cls, connection):
        Room, Temperature, Color = cls.classes("Room", "Temperature", "Color")
        with Session(connection) as session:
            warm = Temperature(pointless_flag=True)
            cool = Temperature(pointless_flag=True)
            session.add_all([warm, cool])

            red = Color(name="red", temperature=warm)
            orange = Color(name="orange", temperature=warm)
            blue = Color(name="blue", temperature=cool)
            green = Color(name="green", temperature=cool)
            session.add_all([red, orange, blue, green])

            red1 = Room(token="Red-1", color=red)
            red2 = Room(token="Red-2", color=red)
            orange2 = Room(token="Orange-2", color=orange)
            blue1 = Room(token="Blue-1", color=blue)
            blue2 = Room(token="Blue-2", color=blue)
            green1 = Room(token="Green-1", color=green)
            red1.connected_rooms = [red2, blue1, green1]
            red2.connected_rooms = [red1, blue2, orange2]
            blue1.connected_rooms = [red1, blue2, green1]
            blue2.connected_rooms = [red2, blue1, orange2]
            session.add_all([red1, red2, blue1, blue2, green1, orange2])

            session.commit()

    @testing.variation(
        "join_on_relationship", ["alone", "with_and", "no", "omit"]
    )
    def test_selectinload(self, join_on_relationship):
        Room, Temperature, Color = self.classes("Room", "Temperature", "Color")
        similar_color = aliased(Color)
        subquery = (
            select(Color.id)
            .join(
                similar_color,
                similar_color.temperature_id == Color.temperature_id,
            )
            .where(similar_color.name == "red")
        )

        if join_on_relationship.alone:
            subquery = subquery.join(Color.temperature).where(
                Temperature.pointless_flag == True
            )
        elif join_on_relationship.with_and:
            subquery = subquery.join(
                Color.temperature.and_(Temperature.pointless_flag == True)
            )
        elif join_on_relationship.no:
            subquery = subquery.join(
                Temperature, Color.temperature_id == Temperature.id
            ).where(Temperature.pointless_flag == True)
        elif join_on_relationship.omit:
            pass
        else:
            join_on_relationship.fail()

        session = fixture_session()
        room_result = session.scalars(
            select(Room)
            .order_by(Room.id)
            .join(Room.color.and_(Color.name == "red"))
            .options(
                selectinload(
                    Room.connected_rooms.and_(Room.color_id.in_(subquery))
                )
            )
        ).unique()

        self._assert_result(room_result)

    def test_contains_eager(self):
        Room, Temperature, Color = self.classes("Room", "Temperature", "Color")
        similar_color = aliased(Color)
        subquery = (
            select(Color.id)
            .join(
                similar_color,
                similar_color.temperature_id == Color.temperature_id,
            )
            .join(Color.temperature.and_(Temperature.pointless_flag == True))
            .where(similar_color.name == "red")
        )

        room_alias = aliased(Room)
        session = fixture_session()

        room_result = session.scalars(
            select(Room)
            .order_by(Room.id, room_alias.id)
            .join(Room.color.and_(Color.name == "red"))
            .join(
                room_alias,
                Room.connected_rooms.of_type(room_alias).and_(
                    room_alias.color_id.in_(subquery)
                ),
            )
            .options(contains_eager(Room.connected_rooms.of_type(room_alias)))
        ).unique()

        self._assert_result(room_result)

    def _assert_result(self, room_result):
        eq_(
            [
                (
                    each_room.token,
                    [room.token for room in each_room.connected_rooms],
                )
                for each_room in room_result
            ],
            [
                ("Red-1", ["Red-2"]),
                ("Red-2", ["Red-1", "Orange-2"]),
            ],
        )
