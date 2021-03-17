import sqlalchemy as sa
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import util
from sqlalchemy.orm import aliased
from sqlalchemy.orm import attributes
from sqlalchemy.orm import contains_eager
from sqlalchemy.orm import defaultload
from sqlalchemy.orm import defer
from sqlalchemy.orm import deferred
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import Load
from sqlalchemy.orm import load_only
from sqlalchemy.orm import mapper
from sqlalchemy.orm import query_expression
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm import subqueryload
from sqlalchemy.orm import synonym
from sqlalchemy.orm import undefer
from sqlalchemy.orm import undefer_group
from sqlalchemy.orm import with_expression
from sqlalchemy.orm import with_polymorphic
from sqlalchemy.sql import literal
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from test.orm import _fixtures
from .inheritance._poly_fixtures import _Polymorphic
from .inheritance._poly_fixtures import Boss
from .inheritance._poly_fixtures import Company
from .inheritance._poly_fixtures import Manager
from .inheritance._poly_fixtures import Person


class DeferredTest(AssertsCompiledSQL, _fixtures.FixtureTest):
    def test_basic(self):
        """A basic deferred load."""

        Order, orders = self.classes.Order, self.tables.orders

        mapper(
            Order,
            orders,
            properties={"description": deferred(orders.c.description)},
        )

        o = Order()
        self.assert_(o.description is None)

        q = fixture_session().query(Order).order_by(Order.id)

        def go():
            result = q.all()
            o2 = result[2]
            o2.description

        self.sql_eq_(
            go,
            [
                (
                    "SELECT orders.id AS orders_id, "
                    "orders.user_id AS orders_user_id, "
                    "orders.address_id AS orders_address_id, "
                    "orders.isopen AS orders_isopen "
                    "FROM orders ORDER BY orders.id",
                    {},
                ),
                (
                    "SELECT orders.description AS orders_description "
                    "FROM orders WHERE orders.id = :pk_1",
                    {"pk_1": 3},
                ),
            ],
        )

    def test_defer_primary_key(self):
        """what happens when we try to defer the primary key?"""

        Order, orders = self.classes.Order, self.tables.orders

        mapper(Order, orders, properties={"id": deferred(orders.c.id)})

        # right now, it's not that graceful :)
        q = fixture_session().query(Order)
        assert_raises_message(
            sa.exc.NoSuchColumnError, "Could not locate", q.first
        )

    def test_unsaved(self):
        """Deferred loading does not kick in when just PK cols are set."""

        Order, orders = self.classes.Order, self.tables.orders

        mapper(
            Order,
            orders,
            properties={"description": deferred(orders.c.description)},
        )

        sess = fixture_session()
        o = Order()
        sess.add(o)
        o.id = 7

        def go():
            o.description = "some description"

        self.sql_count_(0, go)

    def test_synonym_group_bug(self):
        orders, Order = self.tables.orders, self.classes.Order

        mapper(
            Order,
            orders,
            properties={
                "isopen": synonym("_isopen", map_column=True),
                "description": deferred(orders.c.description, group="foo"),
            },
        )

        sess = fixture_session()
        o1 = sess.query(Order).get(1)
        eq_(o1.description, "order 1")

    def test_unsaved_2(self):
        Order, orders = self.classes.Order, self.tables.orders

        mapper(
            Order,
            orders,
            properties={"description": deferred(orders.c.description)},
        )

        sess = fixture_session()
        o = Order()
        sess.add(o)

        def go():
            o.description = "some description"

        self.sql_count_(0, go)

    def test_unsaved_group(self):
        """Deferred loading doesn't kick in when just PK cols are set"""

        orders, Order = self.tables.orders, self.classes.Order

        mapper(
            Order,
            orders,
            properties=dict(
                description=deferred(orders.c.description, group="primary"),
                opened=deferred(orders.c.isopen, group="primary"),
            ),
        )

        sess = fixture_session()
        o = Order()
        sess.add(o)
        o.id = 7

        def go():
            o.description = "some description"

        self.sql_count_(0, go)

    def test_unsaved_group_2(self):
        orders, Order = self.tables.orders, self.classes.Order

        mapper(
            Order,
            orders,
            properties=dict(
                description=deferred(orders.c.description, group="primary"),
                opened=deferred(orders.c.isopen, group="primary"),
            ),
        )

        sess = fixture_session()
        o = Order()
        sess.add(o)

        def go():
            o.description = "some description"

        self.sql_count_(0, go)

    def test_save(self):
        Order, orders = self.classes.Order, self.tables.orders

        mapper(
            Order,
            orders,
            properties={"description": deferred(orders.c.description)},
        )

        sess = fixture_session()
        o2 = sess.query(Order).get(2)
        o2.isopen = 1
        sess.flush()

    def test_group(self):
        """Deferred load with a group"""

        orders, Order = self.tables.orders, self.classes.Order

        mapper(
            Order,
            orders,
            properties=util.OrderedDict(
                [
                    ("userident", deferred(orders.c.user_id, group="primary")),
                    (
                        "addrident",
                        deferred(orders.c.address_id, group="primary"),
                    ),
                    (
                        "description",
                        deferred(orders.c.description, group="primary"),
                    ),
                    ("opened", deferred(orders.c.isopen, group="primary")),
                ]
            ),
        )

        sess = fixture_session()
        q = sess.query(Order).order_by(Order.id)

        def go():
            result = q.all()
            o2 = result[2]
            eq_(o2.opened, 1)
            eq_(o2.userident, 7)
            eq_(o2.description, "order 3")

        self.sql_eq_(
            go,
            [
                (
                    "SELECT orders.id AS orders_id "
                    "FROM orders ORDER BY orders.id",
                    {},
                ),
                (
                    "SELECT orders.user_id AS orders_user_id, "
                    "orders.address_id AS orders_address_id, "
                    "orders.description AS orders_description, "
                    "orders.isopen AS orders_isopen "
                    "FROM orders WHERE orders.id = :pk_1",
                    {"pk_1": 3},
                ),
            ],
        )

        o2 = q.all()[2]
        eq_(o2.description, "order 3")
        assert o2 not in sess.dirty
        o2.description = "order 3"

        def go():
            sess.flush()

        self.sql_count_(0, go)

    def test_preserve_changes(self):
        """A deferred load operation doesn't revert modifications on
        attributes"""

        orders, Order = self.tables.orders, self.classes.Order

        mapper(
            Order,
            orders,
            properties={
                "userident": deferred(orders.c.user_id, group="primary"),
                "description": deferred(orders.c.description, group="primary"),
                "opened": deferred(orders.c.isopen, group="primary"),
            },
        )
        sess = fixture_session(autoflush=False)
        o = sess.query(Order).get(3)
        assert "userident" not in o.__dict__
        o.description = "somenewdescription"
        eq_(o.description, "somenewdescription")

        def go():
            eq_(o.opened, 1)

        self.assert_sql_count(testing.db, go, 1)
        eq_(o.description, "somenewdescription")
        assert o in sess.dirty

    def test_commits_state(self):
        """
        When deferred elements are loaded via a group, they get the proper
        CommittedState and don't result in changes being committed

        """

        orders, Order = self.tables.orders, self.classes.Order

        mapper(
            Order,
            orders,
            properties={
                "userident": deferred(orders.c.user_id, group="primary"),
                "description": deferred(orders.c.description, group="primary"),
                "opened": deferred(orders.c.isopen, group="primary"),
            },
        )

        sess = fixture_session()
        o2 = sess.query(Order).get(3)

        # this will load the group of attributes
        eq_(o2.description, "order 3")
        assert o2 not in sess.dirty
        # this will mark it as 'dirty', but nothing actually changed
        o2.description = "order 3"
        # therefore the flush() shouldn't actually issue any SQL
        self.assert_sql_count(testing.db, sess.flush, 0)

    def test_map_selectable_wo_deferred(self):
        """test mapping to a selectable with deferred cols,
        the selectable doesn't include the deferred col.

        """

        Order, orders = self.classes.Order, self.tables.orders

        order_select = sa.select(
            orders.c.id,
            orders.c.user_id,
            orders.c.address_id,
            orders.c.description,
            orders.c.isopen,
        ).alias()
        mapper(
            Order,
            order_select,
            properties={"description": deferred(order_select.c.description)},
        )

        sess = fixture_session()
        o1 = sess.query(Order).order_by(Order.id).first()
        assert "description" not in o1.__dict__
        eq_(o1.description, "order 1")


class DeferredOptionsTest(AssertsCompiledSQL, _fixtures.FixtureTest):
    __dialect__ = "default"

    def test_options(self):
        """Options on a mapper to create deferred and undeferred columns"""

        orders, Order = self.tables.orders, self.classes.Order

        mapper(Order, orders)

        sess = fixture_session()
        q = sess.query(Order).order_by(Order.id).options(defer("user_id"))

        def go():
            q.all()[0].user_id

        self.sql_eq_(
            go,
            [
                (
                    "SELECT orders.id AS orders_id, "
                    "orders.address_id AS orders_address_id, "
                    "orders.description AS orders_description, "
                    "orders.isopen AS orders_isopen "
                    "FROM orders ORDER BY orders.id",
                    {},
                ),
                (
                    "SELECT orders.user_id AS orders_user_id "
                    "FROM orders WHERE orders.id = :pk_1",
                    {"pk_1": 1},
                ),
            ],
        )
        sess.expunge_all()

        q2 = q.options(undefer("user_id"))
        self.sql_eq_(
            q2.all,
            [
                (
                    "SELECT orders.id AS orders_id, "
                    "orders.user_id AS orders_user_id, "
                    "orders.address_id AS orders_address_id, "
                    "orders.description AS orders_description, "
                    "orders.isopen AS orders_isopen "
                    "FROM orders ORDER BY orders.id",
                    {},
                )
            ],
        )

    def test_undefer_group(self):
        orders, Order = self.tables.orders, self.classes.Order

        mapper(
            Order,
            orders,
            properties=util.OrderedDict(
                [
                    ("userident", deferred(orders.c.user_id, group="primary")),
                    (
                        "description",
                        deferred(orders.c.description, group="primary"),
                    ),
                    ("opened", deferred(orders.c.isopen, group="primary")),
                ]
            ),
        )

        sess = fixture_session()
        q = sess.query(Order).order_by(Order.id)

        def go():
            result = q.options(undefer_group("primary")).all()
            o2 = result[2]
            eq_(o2.opened, 1)
            eq_(o2.userident, 7)
            eq_(o2.description, "order 3")

        self.sql_eq_(
            go,
            [
                (
                    "SELECT orders.user_id AS orders_user_id, "
                    "orders.description AS orders_description, "
                    "orders.isopen AS orders_isopen, "
                    "orders.id AS orders_id, "
                    "orders.address_id AS orders_address_id "
                    "FROM orders ORDER BY orders.id",
                    {},
                )
            ],
        )

    def test_undefer_group_multi(self):
        orders, Order = self.tables.orders, self.classes.Order

        mapper(
            Order,
            orders,
            properties=util.OrderedDict(
                [
                    ("userident", deferred(orders.c.user_id, group="primary")),
                    (
                        "description",
                        deferred(orders.c.description, group="primary"),
                    ),
                    ("opened", deferred(orders.c.isopen, group="secondary")),
                ]
            ),
        )

        sess = fixture_session()
        q = sess.query(Order).order_by(Order.id)

        def go():
            result = q.options(
                undefer_group("primary"), undefer_group("secondary")
            ).all()
            o2 = result[2]
            eq_(o2.opened, 1)
            eq_(o2.userident, 7)
            eq_(o2.description, "order 3")

        self.sql_eq_(
            go,
            [
                (
                    "SELECT orders.user_id AS orders_user_id, "
                    "orders.description AS orders_description, "
                    "orders.isopen AS orders_isopen, "
                    "orders.id AS orders_id, "
                    "orders.address_id AS orders_address_id "
                    "FROM orders ORDER BY orders.id",
                    {},
                )
            ],
        )

    def test_undefer_group_multi_pathed(self):
        orders, Order = self.tables.orders, self.classes.Order

        mapper(
            Order,
            orders,
            properties=util.OrderedDict(
                [
                    ("userident", deferred(orders.c.user_id, group="primary")),
                    (
                        "description",
                        deferred(orders.c.description, group="primary"),
                    ),
                    ("opened", deferred(orders.c.isopen, group="secondary")),
                ]
            ),
        )

        sess = fixture_session()
        q = sess.query(Order).order_by(Order.id)

        def go():
            result = q.options(
                Load(Order).undefer_group("primary").undefer_group("secondary")
            ).all()
            o2 = result[2]
            eq_(o2.opened, 1)
            eq_(o2.userident, 7)
            eq_(o2.description, "order 3")

        self.sql_eq_(
            go,
            [
                (
                    "SELECT orders.user_id AS orders_user_id, "
                    "orders.description AS orders_description, "
                    "orders.isopen AS orders_isopen, "
                    "orders.id AS orders_id, "
                    "orders.address_id AS orders_address_id "
                    "FROM orders ORDER BY orders.id",
                    {},
                )
            ],
        )

    def test_undefer_group_from_relationship_lazyload(self):
        users, Order, User, orders = (
            self.tables.users,
            self.classes.Order,
            self.classes.User,
            self.tables.orders,
        )

        mapper(
            User,
            users,
            properties=dict(orders=relationship(Order, order_by=orders.c.id)),
        )
        mapper(
            Order,
            orders,
            properties=util.OrderedDict(
                [
                    ("userident", deferred(orders.c.user_id, group="primary")),
                    (
                        "description",
                        deferred(orders.c.description, group="primary"),
                    ),
                    ("opened", deferred(orders.c.isopen, group="primary")),
                ]
            ),
        )

        sess = fixture_session()
        q = (
            sess.query(User)
            .filter(User.id == 7)
            .options(defaultload(User.orders).undefer_group("primary"))
        )

        def go():
            result = q.all()
            o2 = result[0].orders[1]
            eq_(o2.opened, 1)
            eq_(o2.userident, 7)
            eq_(o2.description, "order 3")

        self.sql_eq_(
            go,
            [
                (
                    "SELECT users.id AS users_id, users.name AS users_name "
                    "FROM users WHERE users.id = :id_1",
                    {"id_1": 7},
                ),
                (
                    "SELECT orders.user_id AS orders_user_id, "
                    "orders.description "
                    "AS orders_description, orders.isopen AS orders_isopen, "
                    "orders.id AS orders_id, orders.address_id "
                    "AS orders_address_id "
                    "FROM orders WHERE :param_1 = orders.user_id "
                    "ORDER BY orders.id",
                    {"param_1": 7},
                ),
            ],
        )

    def test_undefer_group_from_relationship_subqueryload(self):
        users, Order, User, orders = (
            self.tables.users,
            self.classes.Order,
            self.classes.User,
            self.tables.orders,
        )

        mapper(
            User,
            users,
            properties=dict(orders=relationship(Order, order_by=orders.c.id)),
        )
        mapper(
            Order,
            orders,
            properties=util.OrderedDict(
                [
                    ("userident", deferred(orders.c.user_id, group="primary")),
                    (
                        "description",
                        deferred(orders.c.description, group="primary"),
                    ),
                    ("opened", deferred(orders.c.isopen, group="primary")),
                ]
            ),
        )

        sess = fixture_session()
        q = (
            sess.query(User)
            .filter(User.id == 7)
            .options(subqueryload(User.orders).undefer_group("primary"))
        )

        def go():
            result = q.all()
            o2 = result[0].orders[1]
            eq_(o2.opened, 1)
            eq_(o2.userident, 7)
            eq_(o2.description, "order 3")

        self.sql_eq_(
            go,
            [
                (
                    "SELECT users.id AS users_id, users.name AS users_name "
                    "FROM users WHERE users.id = :id_1",
                    {"id_1": 7},
                ),
                (
                    "SELECT orders.user_id AS orders_user_id, "
                    "orders.description "
                    "AS orders_description, orders.isopen AS orders_isopen, "
                    "orders.id AS orders_id, "
                    "orders.address_id AS orders_address_id, "
                    "anon_1.users_id AS anon_1_users_id "
                    "FROM (SELECT users.id AS "
                    "users_id FROM users WHERE users.id = :id_1) AS anon_1 "
                    "JOIN orders ON anon_1.users_id = orders.user_id ORDER BY "
                    "orders.id",
                    [{"id_1": 7}],
                ),
            ],
        )

    def test_undefer_group_from_relationship_joinedload(self):
        users, Order, User, orders = (
            self.tables.users,
            self.classes.Order,
            self.classes.User,
            self.tables.orders,
        )

        mapper(
            User,
            users,
            properties=dict(orders=relationship(Order, order_by=orders.c.id)),
        )
        mapper(
            Order,
            orders,
            properties=util.OrderedDict(
                [
                    ("userident", deferred(orders.c.user_id, group="primary")),
                    (
                        "description",
                        deferred(orders.c.description, group="primary"),
                    ),
                    ("opened", deferred(orders.c.isopen, group="primary")),
                ]
            ),
        )

        sess = fixture_session()
        q = (
            sess.query(User)
            .filter(User.id == 7)
            .options(joinedload(User.orders).undefer_group("primary"))
        )

        def go():
            result = q.all()
            o2 = result[0].orders[1]
            eq_(o2.opened, 1)
            eq_(o2.userident, 7)
            eq_(o2.description, "order 3")

        self.sql_eq_(
            go,
            [
                (
                    "SELECT users.id AS users_id, users.name AS users_name, "
                    "orders_1.user_id AS orders_1_user_id, "
                    "orders_1.description AS orders_1_description, "
                    "orders_1.isopen AS orders_1_isopen, "
                    "orders_1.id AS orders_1_id, orders_1.address_id AS "
                    "orders_1_address_id FROM users "
                    "LEFT OUTER JOIN orders AS orders_1 ON users.id = "
                    "orders_1.user_id WHERE users.id = :id_1 "
                    "ORDER BY orders_1.id",
                    {"id_1": 7},
                )
            ],
        )

    def test_undefer_group_from_relationship_joinedload_colexpr(self):
        users, Order, User, orders = (
            self.tables.users,
            self.classes.Order,
            self.classes.User,
            self.tables.orders,
        )

        mapper(
            User,
            users,
            properties=dict(orders=relationship(Order, order_by=orders.c.id)),
        )
        mapper(
            Order,
            orders,
            properties=util.OrderedDict(
                [
                    ("userident", deferred(orders.c.user_id, group="primary")),
                    (
                        "lower_desc",
                        deferred(
                            sa.func.lower(orders.c.description).label(None),
                            group="primary",
                        ),
                    ),
                    ("opened", deferred(orders.c.isopen, group="primary")),
                ]
            ),
        )

        sess = fixture_session()
        q = (
            sess.query(User)
            .filter(User.id == 7)
            .options(joinedload(User.orders).undefer_group("primary"))
        )

        def go():
            result = q.all()
            o2 = result[0].orders[1]
            eq_(o2.opened, 1)
            eq_(o2.userident, 7)
            eq_(o2.lower_desc, "order 3")

        self.sql_eq_(
            go,
            [
                (
                    "SELECT users.id AS users_id, users.name AS users_name, "
                    "orders_1.user_id AS orders_1_user_id, "
                    "lower(orders_1.description) AS lower_1, "
                    "orders_1.isopen AS orders_1_isopen, "
                    "orders_1.id AS orders_1_id, "
                    "orders_1.address_id AS orders_1_address_id, "
                    "orders_1.description AS orders_1_description FROM users "
                    "LEFT OUTER JOIN orders AS orders_1 ON users.id = "
                    "orders_1.user_id WHERE users.id = :id_1 "
                    "ORDER BY orders_1.id",
                    {"id_1": 7},
                )
            ],
        )

    def test_undefer_star(self):
        orders, Order = self.tables.orders, self.classes.Order

        mapper(
            Order,
            orders,
            properties=util.OrderedDict(
                [
                    ("userident", deferred(orders.c.user_id)),
                    ("description", deferred(orders.c.description)),
                    ("opened", deferred(orders.c.isopen)),
                ]
            ),
        )

        sess = fixture_session()
        q = sess.query(Order).options(Load(Order).undefer("*"))
        self.assert_compile(
            q,
            "SELECT orders.user_id AS orders_user_id, "
            "orders.description AS orders_description, "
            "orders.isopen AS orders_isopen, "
            "orders.id AS orders_id, "
            "orders.address_id AS orders_address_id "
            "FROM orders",
        )

    def test_locates_col(self):
        """changed in 1.0 - we don't search for deferred cols in the result
        now."""

        orders, Order = self.tables.orders, self.classes.Order

        mapper(
            Order,
            orders,
            properties={"description": deferred(orders.c.description)},
        )

        sess = fixture_session()
        o1 = (
            sess.query(Order)
            .order_by(Order.id)
            .add_columns(orders.c.description)
            .first()
        )[0]

        def go():
            eq_(o1.description, "order 1")

        # prior to 1.0 we'd search in the result for this column
        # self.sql_count_(0, go)
        self.sql_count_(1, go)

    def test_locates_col_rowproc_only(self):
        """changed in 1.0 - we don't search for deferred cols in the result
        now.

        Because the loading for ORM Query and Query from a core select
        is now split off, we test loading from a plain select()
        separately.

        """

        orders, Order = self.tables.orders, self.classes.Order

        mapper(
            Order,
            orders,
            properties={"description": deferred(orders.c.description)},
        )

        sess = fixture_session()
        stmt = sa.select(Order).order_by(Order.id)
        o1 = (sess.query(Order).from_statement(stmt).all())[0]

        def go():
            eq_(o1.description, "order 1")

        # prior to 1.0 we'd search in the result for this column
        # self.sql_count_(0, go)
        self.sql_count_(1, go)

    def test_raise_on_col_rowproc_only(self):
        orders, Order = self.tables.orders, self.classes.Order

        mapper(
            Order,
            orders,
            properties={
                "description": deferred(orders.c.description, raiseload=True)
            },
        )

        sess = fixture_session()
        stmt = sa.select(Order).order_by(Order.id)
        o1 = (sess.query(Order).from_statement(stmt).all())[0]

        assert_raises_message(
            sa.exc.InvalidRequestError,
            "'Order.description' is not available due to raiseload=True",
            getattr,
            o1,
            "description",
        )

    def test_locates_col_w_option_rowproc_only(self):
        orders, Order = self.tables.orders, self.classes.Order

        mapper(Order, orders)

        sess = fixture_session()
        stmt = sa.select(Order).order_by(Order.id)
        o1 = (
            sess.query(Order)
            .from_statement(stmt)
            .options(defer(Order.description))
            .all()
        )[0]

        def go():
            eq_(o1.description, "order 1")

        # prior to 1.0 we'd search in the result for this column
        # self.sql_count_(0, go)
        self.sql_count_(1, go)

    def test_raise_on_col_w_option_rowproc_only(self):
        orders, Order = self.tables.orders, self.classes.Order

        mapper(Order, orders)

        sess = fixture_session()
        stmt = sa.select(Order).order_by(Order.id)
        o1 = (
            sess.query(Order)
            .from_statement(stmt)
            .options(defer(Order.description, raiseload=True))
            .all()
        )[0]

        assert_raises_message(
            sa.exc.InvalidRequestError,
            "'Order.description' is not available due to raiseload=True",
            getattr,
            o1,
            "description",
        )

    def test_deep_options(self):
        users, items, order_items, Order, Item, User, orders = (
            self.tables.users,
            self.tables.items,
            self.tables.order_items,
            self.classes.Order,
            self.classes.Item,
            self.classes.User,
            self.tables.orders,
        )

        mapper(
            Item,
            items,
            properties=dict(description=deferred(items.c.description)),
        )
        mapper(
            Order,
            orders,
            properties=dict(items=relationship(Item, secondary=order_items)),
        )
        mapper(
            User,
            users,
            properties=dict(orders=relationship(Order, order_by=orders.c.id)),
        )

        sess = fixture_session()
        q = sess.query(User).order_by(User.id)
        result = q.all()
        item = result[0].orders[1].items[1]

        def go():
            eq_(item.description, "item 4")

        self.sql_count_(1, go)
        eq_(item.description, "item 4")

        sess.expunge_all()
        result = q.options(undefer("orders.items.description")).all()
        item = result[0].orders[1].items[1]

        def go():
            eq_(item.description, "item 4")

        self.sql_count_(0, go)
        eq_(item.description, "item 4")

    def test_path_entity(self):
        r"""test the legacy \*addl_attrs argument."""

        User = self.classes.User
        Order = self.classes.Order
        Item = self.classes.Item

        users = self.tables.users
        orders = self.tables.orders
        items = self.tables.items
        order_items = self.tables.order_items

        mapper(
            User,
            users,
            properties={"orders": relationship(Order, lazy="joined")},
        )
        mapper(
            Order,
            orders,
            properties={
                "items": relationship(
                    Item, secondary=order_items, lazy="joined"
                )
            },
        )
        mapper(Item, items)

        sess = fixture_session()

        exp = (
            "SELECT users.id AS users_id, users.name AS users_name, "
            "items_1.id AS items_1_id, orders_1.id AS orders_1_id, "
            "orders_1.user_id AS orders_1_user_id, orders_1.address_id "
            "AS orders_1_address_id, orders_1.description AS "
            "orders_1_description, orders_1.isopen AS orders_1_isopen "
            "FROM users LEFT OUTER JOIN orders AS orders_1 "
            "ON users.id = orders_1.user_id LEFT OUTER JOIN "
            "(order_items AS order_items_1 JOIN items AS items_1 "
            "ON items_1.id = order_items_1.item_id) "
            "ON orders_1.id = order_items_1.order_id"
        )

        q = sess.query(User).options(
            defaultload(User.orders)
            .defaultload(Order.items)
            .defer(Item.description)
        )
        self.assert_compile(q, exp)

    def test_chained_multi_col_options(self):
        users, User = self.tables.users, self.classes.User
        orders, Order = self.tables.orders, self.classes.Order

        mapper(User, users, properties={"orders": relationship(Order)})
        mapper(Order, orders)

        sess = fixture_session()
        q = sess.query(User).options(
            joinedload(User.orders).defer("description").defer("isopen")
        )
        self.assert_compile(
            q,
            "SELECT users.id AS users_id, "
            "users.name AS users_name, "
            "orders_1.id AS orders_1_id, "
            "orders_1.user_id AS orders_1_user_id, "
            "orders_1.address_id AS orders_1_address_id "
            "FROM users "
            "LEFT OUTER JOIN orders AS orders_1 "
            "ON users.id = orders_1.user_id",
        )

    def test_load_only_no_pk(self):
        orders, Order = self.tables.orders, self.classes.Order

        mapper(Order, orders)

        sess = fixture_session()
        q = sess.query(Order).options(load_only("isopen", "description"))
        self.assert_compile(
            q,
            "SELECT orders.id AS orders_id, "
            "orders.description AS orders_description, "
            "orders.isopen AS orders_isopen FROM orders",
        )

    def test_load_only_no_pk_rt(self):
        orders, Order = self.tables.orders, self.classes.Order

        mapper(Order, orders)

        sess = fixture_session()
        q = (
            sess.query(Order)
            .order_by(Order.id)
            .options(load_only("isopen", "description"))
        )
        eq_(q.first(), Order(id=1))

    def test_load_only_w_deferred(self):
        orders, Order = self.tables.orders, self.classes.Order

        mapper(
            Order,
            orders,
            properties={"description": deferred(orders.c.description)},
        )

        sess = fixture_session()
        q = sess.query(Order).options(
            load_only("isopen", "description"), undefer("user_id")
        )
        self.assert_compile(
            q,
            "SELECT orders.description AS orders_description, "
            "orders.id AS orders_id, "
            "orders.user_id AS orders_user_id, "
            "orders.isopen AS orders_isopen FROM orders",
        )

    def test_load_only_propagate_unbound(self):
        self._test_load_only_propagate(False)

    def test_load_only_propagate_bound(self):
        self._test_load_only_propagate(True)

    def _test_load_only_propagate(self, use_load):
        User = self.classes.User
        Address = self.classes.Address

        users = self.tables.users
        addresses = self.tables.addresses

        mapper(User, users, properties={"addresses": relationship(Address)})
        mapper(Address, addresses)

        sess = fixture_session()
        expected = [
            (
                "SELECT users.id AS users_id, users.name AS users_name "
                "FROM users WHERE users.id IN ([POSTCOMPILE_id_1])",
                {"id_1": [7, 8]},
            ),
            (
                "SELECT addresses.id AS addresses_id, "
                "addresses.email_address AS addresses_email_address "
                "FROM addresses WHERE :param_1 = addresses.user_id",
                {"param_1": 7},
            ),
            (
                "SELECT addresses.id AS addresses_id, "
                "addresses.email_address AS addresses_email_address "
                "FROM addresses WHERE :param_1 = addresses.user_id",
                {"param_1": 8},
            ),
        ]

        if use_load:
            opt = (
                Load(User)
                .defaultload(User.addresses)
                .load_only("id", "email_address")
            )
        else:
            opt = defaultload(User.addresses).load_only("id", "email_address")
        q = sess.query(User).options(opt).filter(User.id.in_([7, 8]))

        def go():
            for user in q:
                user.addresses

        self.sql_eq_(go, expected)

    def test_load_only_parent_specific(self):
        User = self.classes.User
        Address = self.classes.Address
        Order = self.classes.Order

        users = self.tables.users
        addresses = self.tables.addresses
        orders = self.tables.orders

        mapper(User, users)
        mapper(Address, addresses)
        mapper(Order, orders)

        sess = fixture_session()
        q = sess.query(User, Order, Address).options(
            Load(User).load_only("name"),
            Load(Order).load_only("id"),
            Load(Address).load_only("id", "email_address"),
        )

        self.assert_compile(
            q,
            "SELECT users.id AS users_id, "
            "users.name AS users_name, "
            "orders.id AS orders_id, "
            "addresses.id AS addresses_id, "
            "addresses.email_address AS addresses_email_address "
            "FROM users, orders, addresses",
        )

    def test_load_only_path_specific(self):
        User = self.classes.User
        Address = self.classes.Address
        Order = self.classes.Order

        users = self.tables.users
        addresses = self.tables.addresses
        orders = self.tables.orders

        mapper(
            User,
            users,
            properties=util.OrderedDict(
                [
                    ("addresses", relationship(Address, lazy="joined")),
                    ("orders", relationship(Order, lazy="joined")),
                ]
            ),
        )

        mapper(Address, addresses)
        mapper(Order, orders)

        sess = fixture_session()

        q = sess.query(User).options(
            load_only("name")
            .defaultload("addresses")
            .load_only("id", "email_address"),
            defaultload("orders").load_only("id"),
        )

        # hmmmm joinedload seems to be forcing users.id into here...
        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name, "
            "addresses_1.id AS addresses_1_id, "
            "addresses_1.email_address AS addresses_1_email_address, "
            "orders_1.id AS orders_1_id FROM users "
            "LEFT OUTER JOIN addresses AS addresses_1 "
            "ON users.id = addresses_1.user_id "
            "LEFT OUTER JOIN orders AS orders_1 "
            "ON users.id = orders_1.user_id",
        )


class SelfReferentialMultiPathTest(testing.fixtures.DeclarativeMappedTest):
    """test for [ticket:3822]"""

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Node(Base):
            __tablename__ = "node"

            id = sa.Column(sa.Integer, primary_key=True)
            parent_id = sa.Column(sa.ForeignKey("node.id"))
            parent = relationship("Node", remote_side=[id])
            name = sa.Column(sa.String(10))

    @classmethod
    def insert_data(cls, connection):
        Node = cls.classes.Node

        session = Session(connection)
        session.add_all(
            [
                Node(id=1, name="name"),
                Node(id=2, parent_id=1, name="name"),
                Node(id=3, parent_id=1, name="name"),
            ]
        )
        session.commit()

    def test_present_overrides_deferred(self):
        Node = self.classes.Node

        session = fixture_session()

        q = session.query(Node).options(
            joinedload(Node.parent).load_only(Node.id, Node.parent_id)
        )

        # Node #1 will appear first as Node.parent and have
        # deferred applied to Node.name.  it will then appear
        # as Node in the last row and "name" should be populated.
        nodes = q.order_by(Node.id.desc()).all()

        def go():
            for node in nodes:
                eq_(node.name, "name")

        self.assert_sql_count(testing.db, go, 0)


class InheritanceTest(_Polymorphic):
    __dialect__ = "default"

    @classmethod
    def setup_mappers(cls):
        super(InheritanceTest, cls).setup_mappers()
        from sqlalchemy import inspect

        inspect(Company).add_property(
            "managers", relationship(Manager, viewonly=True)
        )

    def test_load_only_subclass(self):
        s = fixture_session()
        q = (
            s.query(Manager)
            .order_by(Manager.person_id)
            .options(load_only("status", "manager_name"))
        )
        self.assert_compile(
            q,
            "SELECT managers.person_id AS managers_person_id, "
            "people.person_id AS people_person_id, "
            "people.type AS people_type, "
            "managers.status AS managers_status, "
            "managers.manager_name AS managers_manager_name "
            "FROM people JOIN managers "
            "ON people.person_id = managers.person_id "
            "ORDER BY managers.person_id",
        )

    def test_load_only_subclass_bound(self):
        s = fixture_session()
        q = (
            s.query(Manager)
            .order_by(Manager.person_id)
            .options(Load(Manager).load_only("status", "manager_name"))
        )
        self.assert_compile(
            q,
            "SELECT managers.person_id AS managers_person_id, "
            "people.person_id AS people_person_id, "
            "people.type AS people_type, "
            "managers.status AS managers_status, "
            "managers.manager_name AS managers_manager_name "
            "FROM people JOIN managers "
            "ON people.person_id = managers.person_id "
            "ORDER BY managers.person_id",
        )

    def test_load_only_subclass_and_superclass(self):
        s = fixture_session()
        q = (
            s.query(Boss)
            .order_by(Person.person_id)
            .options(load_only("status", "manager_name"))
        )
        self.assert_compile(
            q,
            "SELECT managers.person_id AS managers_person_id, "
            "people.person_id AS people_person_id, "
            "people.type AS people_type, "
            "managers.status AS managers_status, "
            "managers.manager_name AS managers_manager_name "
            "FROM people JOIN managers "
            "ON people.person_id = managers.person_id JOIN boss "
            "ON managers.person_id = boss.boss_id ORDER BY people.person_id",
        )

    def test_load_only_subclass_and_superclass_bound(self):
        s = fixture_session()
        q = (
            s.query(Boss)
            .order_by(Person.person_id)
            .options(Load(Boss).load_only("status", "manager_name"))
        )
        self.assert_compile(
            q,
            "SELECT managers.person_id AS managers_person_id, "
            "people.person_id AS people_person_id, "
            "people.type AS people_type, "
            "managers.status AS managers_status, "
            "managers.manager_name AS managers_manager_name "
            "FROM people JOIN managers "
            "ON people.person_id = managers.person_id JOIN boss "
            "ON managers.person_id = boss.boss_id ORDER BY people.person_id",
        )

    def test_load_only_alias_subclass(self):
        s = fixture_session()
        m1 = aliased(Manager, flat=True)
        q = (
            s.query(m1)
            .order_by(m1.person_id)
            .options(load_only("status", "manager_name"))
        )
        self.assert_compile(
            q,
            "SELECT managers_1.person_id AS managers_1_person_id, "
            "people_1.person_id AS people_1_person_id, "
            "people_1.type AS people_1_type, "
            "managers_1.status AS managers_1_status, "
            "managers_1.manager_name AS managers_1_manager_name "
            "FROM people AS people_1 JOIN managers AS "
            "managers_1 ON people_1.person_id = managers_1.person_id "
            "ORDER BY managers_1.person_id",
        )

    def test_load_only_alias_subclass_bound(self):
        s = fixture_session()
        m1 = aliased(Manager, flat=True)
        q = (
            s.query(m1)
            .order_by(m1.person_id)
            .options(Load(m1).load_only("status", "manager_name"))
        )
        self.assert_compile(
            q,
            "SELECT managers_1.person_id AS managers_1_person_id, "
            "people_1.person_id AS people_1_person_id, "
            "people_1.type AS people_1_type, "
            "managers_1.status AS managers_1_status, "
            "managers_1.manager_name AS managers_1_manager_name "
            "FROM people AS people_1 JOIN managers AS "
            "managers_1 ON people_1.person_id = managers_1.person_id "
            "ORDER BY managers_1.person_id",
        )

    def test_load_only_subclass_from_relationship_polymorphic(self):
        s = fixture_session()
        wp = with_polymorphic(Person, [Manager], flat=True)
        q = (
            s.query(Company)
            .join(Company.employees.of_type(wp))
            .options(
                contains_eager(Company.employees.of_type(wp)).load_only(
                    wp.Manager.status, wp.Manager.manager_name
                )
            )
        )
        self.assert_compile(
            q,
            "SELECT people_1.person_id AS people_1_person_id, "
            "people_1.type AS people_1_type, "
            "managers_1.person_id AS managers_1_person_id, "
            "managers_1.status AS managers_1_status, "
            "managers_1.manager_name AS managers_1_manager_name, "
            "companies.company_id AS companies_company_id, "
            "companies.name AS companies_name "
            "FROM companies JOIN (people AS people_1 LEFT OUTER JOIN "
            "managers AS managers_1 ON people_1.person_id = "
            "managers_1.person_id) ON companies.company_id = "
            "people_1.company_id",
        )

    def test_load_only_subclass_from_relationship_polymorphic_bound(self):
        s = fixture_session()
        wp = with_polymorphic(Person, [Manager], flat=True)
        q = (
            s.query(Company)
            .join(Company.employees.of_type(wp))
            .options(
                Load(Company)
                .contains_eager(Company.employees.of_type(wp))
                .load_only(wp.Manager.status, wp.Manager.manager_name)
            )
        )
        self.assert_compile(
            q,
            "SELECT people_1.person_id AS people_1_person_id, "
            "people_1.type AS people_1_type, "
            "managers_1.person_id AS managers_1_person_id, "
            "managers_1.status AS managers_1_status, "
            "managers_1.manager_name AS managers_1_manager_name, "
            "companies.company_id AS companies_company_id, "
            "companies.name AS companies_name "
            "FROM companies JOIN (people AS people_1 LEFT OUTER JOIN "
            "managers AS managers_1 ON people_1.person_id = "
            "managers_1.person_id) ON companies.company_id = "
            "people_1.company_id",
        )

    def test_load_only_subclass_from_relationship(self):
        s = fixture_session()
        q = (
            s.query(Company)
            .join(Company.managers)
            .options(
                contains_eager(Company.managers).load_only(
                    "status", "manager_name"
                )
            )
        )
        self.assert_compile(
            q,
            "SELECT companies.company_id AS companies_company_id, "
            "companies.name AS companies_name, "
            "managers.person_id AS managers_person_id, "
            "people.person_id AS people_person_id, "
            "people.type AS people_type, "
            "managers.status AS managers_status, "
            "managers.manager_name AS managers_manager_name "
            "FROM companies JOIN (people JOIN managers ON people.person_id = "
            "managers.person_id) ON companies.company_id = people.company_id",
        )

    def test_load_only_subclass_from_relationship_bound(self):
        s = fixture_session()
        q = (
            s.query(Company)
            .join(Company.managers)
            .options(
                Load(Company)
                .contains_eager(Company.managers)
                .load_only("status", "manager_name")
            )
        )
        self.assert_compile(
            q,
            "SELECT companies.company_id AS companies_company_id, "
            "companies.name AS companies_name, "
            "managers.person_id AS managers_person_id, "
            "people.person_id AS people_person_id, "
            "people.type AS people_type, "
            "managers.status AS managers_status, "
            "managers.manager_name AS managers_manager_name "
            "FROM companies JOIN (people JOIN managers ON people.person_id = "
            "managers.person_id) ON companies.company_id = people.company_id",
        )

    def test_defer_on_wildcard_subclass(self):
        # pretty much the same as load_only except doesn't
        # exclude the primary key

        # TODO: what is ".*"?  this is not documented anywhere, how did this
        # get implemented without docs ?  see #4390
        s = fixture_session()
        q = (
            s.query(Manager)
            .order_by(Person.person_id)
            .options(defer(".*"), undefer("status"))
        )
        self.assert_compile(
            q,
            "SELECT managers.status AS managers_status "
            "FROM people JOIN managers ON "
            "people.person_id = managers.person_id ORDER BY people.person_id",
        )

        # note this doesn't apply to "bound" loaders since they don't seem
        # to have this ".*" featue.

    def test_load_only_subclass_of_type(self):
        s = fixture_session()
        q = s.query(Company).options(
            joinedload(Company.employees.of_type(Manager)).load_only("status")
        )
        self.assert_compile(
            q,
            "SELECT companies.company_id AS companies_company_id, "
            "companies.name AS companies_name, "
            "anon_1.people_person_id AS anon_1_people_person_id, "
            "anon_1.people_type AS anon_1_people_type, "
            "anon_1.managers_person_id AS anon_1_managers_person_id, "
            "anon_1.managers_status AS anon_1_managers_status "
            "FROM companies LEFT OUTER JOIN "
            "(SELECT people.person_id AS people_person_id, "
            "people.company_id AS people_company_id, "
            "people.name AS people_name, people.type AS people_type, "
            "managers.person_id AS managers_person_id, "
            "managers.status AS managers_status, "
            "managers.manager_name AS managers_manager_name "
            "FROM people LEFT OUTER JOIN managers "
            "ON people.person_id = managers.person_id) AS anon_1 "
            "ON companies.company_id = anon_1.people_company_id "
            "ORDER BY anon_1.people_person_id",
        )

    def test_wildcard_subclass_of_type(self):
        s = fixture_session()
        q = s.query(Company).options(
            joinedload(Company.employees.of_type(Manager)).defer("*")
        )
        self.assert_compile(
            q,
            "SELECT companies.company_id AS companies_company_id, "
            "companies.name AS companies_name "
            "FROM companies LEFT OUTER JOIN "
            "(SELECT people.person_id AS people_person_id, "
            "people.company_id AS people_company_id, "
            "people.name AS people_name, people.type AS people_type, "
            "managers.person_id AS managers_person_id, "
            "managers.status AS managers_status, "
            "managers.manager_name AS managers_manager_name "
            "FROM people LEFT OUTER JOIN managers "
            "ON people.person_id = managers.person_id) AS anon_1 "
            "ON companies.company_id = anon_1.people_company_id "
            "ORDER BY anon_1.people_person_id",
        )

    def test_defer_super_name_on_subclass(self):
        s = fixture_session()
        q = s.query(Manager).order_by(Person.person_id).options(defer("name"))
        self.assert_compile(
            q,
            "SELECT managers.person_id AS managers_person_id, "
            "people.person_id AS people_person_id, "
            "people.company_id AS people_company_id, "
            "people.type AS people_type, managers.status AS managers_status, "
            "managers.manager_name AS managers_manager_name "
            "FROM people JOIN managers "
            "ON people.person_id = managers.person_id "
            "ORDER BY people.person_id",
        )

    def test_defer_super_name_on_subclass_bound(self):
        s = fixture_session()
        q = (
            s.query(Manager)
            .order_by(Person.person_id)
            .options(Load(Manager).defer("name"))
        )
        self.assert_compile(
            q,
            "SELECT managers.person_id AS managers_person_id, "
            "people.person_id AS people_person_id, "
            "people.company_id AS people_company_id, "
            "people.type AS people_type, managers.status AS managers_status, "
            "managers.manager_name AS managers_manager_name "
            "FROM people JOIN managers "
            "ON people.person_id = managers.person_id "
            "ORDER BY people.person_id",
        )

    def test_load_only_from_with_polymorphic(self):
        s = fixture_session()

        wp = with_polymorphic(Person, [Manager], flat=True)

        assert_raises_message(
            sa.exc.ArgumentError,
            'Mapped attribute "Manager.status" does not apply to any of the '
            "root entities in this query, e.g. "
            r"with_polymorphic\(Person, \[Manager\]\).",
            s.query(wp).options(load_only(Manager.status))._compile_context,
        )

        q = s.query(wp).options(load_only(wp.Manager.status))
        self.assert_compile(
            q,
            "SELECT people_1.person_id AS people_1_person_id, "
            "people_1.type AS people_1_type, "
            "managers_1.person_id AS managers_1_person_id, "
            "managers_1.status AS managers_1_status "
            "FROM people AS people_1 "
            "LEFT OUTER JOIN managers AS managers_1 "
            "ON people_1.person_id = managers_1.person_id",
        )

    def test_load_only_of_type_with_polymorphic(self):
        s = fixture_session()

        wp = with_polymorphic(Person, [Manager], flat=True)

        # needs to be explicit, we don't currently dig onto all the
        # sub-entities in the wp
        assert_raises_message(
            sa.exc.ArgumentError,
            r'Can\'t find property named "status" on '
            r"with_polymorphic\(Person, \[Manager\]\) in this Query.",
            s.query(Company)
            .options(
                joinedload(Company.employees.of_type(wp)).load_only("status")
            )
            ._compile_context,
        )

        assert_raises_message(
            sa.exc.ArgumentError,
            'Attribute "Manager.status" does not link from element '
            r'"with_polymorphic\(Person, \[Manager\]\)"',
            s.query(Company)
            .options(
                joinedload(Company.employees.of_type(wp)).load_only(
                    Manager.status
                )
            )
            ._compile_context,
        )

        self.assert_compile(
            s.query(Company).options(
                joinedload(Company.employees.of_type(wp)).load_only(
                    wp.Manager.status
                )
            ),
            # should at least not have manager_name in it
            "SELECT companies.company_id AS companies_company_id, "
            "companies.name AS companies_name, "
            "people_1.person_id AS people_1_person_id, "
            "people_1.type AS people_1_type, "
            "managers_1.person_id AS managers_1_person_id, "
            "managers_1.status AS managers_1_status "
            "FROM companies LEFT OUTER JOIN "
            "(people AS people_1 LEFT OUTER JOIN managers AS managers_1 "
            "ON people_1.person_id = managers_1.person_id) "
            "ON companies.company_id = people_1.company_id "
            "ORDER BY people_1.person_id",
        )


class WithExpressionTest(fixtures.DeclarativeMappedTest):
    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class A(fixtures.ComparableEntity, Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)
            x = Column(Integer)
            y = Column(Integer)

            my_expr = query_expression()

            bs = relationship("B", order_by="B.id")

        class B(fixtures.ComparableEntity, Base):
            __tablename__ = "b"
            id = Column(Integer, primary_key=True)
            a_id = Column(ForeignKey("a.id"))
            p = Column(Integer)
            q = Column(Integer)

            b_expr = query_expression()

        class C(fixtures.ComparableEntity, Base):
            __tablename__ = "c"
            id = Column(Integer, primary_key=True)
            x = Column(Integer)

            c_expr = query_expression(literal(1))

    @classmethod
    def insert_data(cls, connection):
        A, B, C = cls.classes("A", "B", "C")
        s = Session(connection)

        s.add_all(
            [
                A(id=1, x=1, y=2, bs=[B(id=1, p=1, q=2), B(id=2, p=4, q=8)]),
                A(id=2, x=2, y=3),
                A(id=3, x=5, y=10, bs=[B(id=3, p=5, q=0)]),
                A(id=4, x=2, y=10, bs=[B(id=4, p=19, q=8), B(id=5, p=5, q=5)]),
                C(id=1, x=1),
                C(id=2, x=2),
            ]
        )

        s.commit()

    def test_simple_expr(self):
        A = self.classes.A

        s = fixture_session()
        a1 = (
            s.query(A)
            .options(with_expression(A.my_expr, A.x + A.y))
            .filter(A.x > 1)
            .order_by(A.id)
        )

        eq_(a1.all(), [A(my_expr=5), A(my_expr=15), A(my_expr=12)])

    def test_expr_default_value(self):
        A = self.classes.A
        C = self.classes.C
        s = fixture_session()

        a1 = s.query(A).order_by(A.id).filter(A.x > 1)
        eq_(a1.all(), [A(my_expr=None), A(my_expr=None), A(my_expr=None)])

        c1 = s.query(C).order_by(C.id)
        eq_(c1.all(), [C(c_expr=1), C(c_expr=1)])

        s.expunge_all()

        c2 = (
            s.query(C)
            .options(with_expression(C.c_expr, C.x * 2))
            .filter(C.x > 1)
            .order_by(C.id)
        )
        eq_(c2.all(), [C(c_expr=4)])

    def test_reuse_expr(self):
        A = self.classes.A

        s = fixture_session()

        # so people will obv. want to say, "filter(A.my_expr > 10)".
        # but that means Query or Core has to post-modify the statement
        # after construction.
        expr = A.x + A.y
        a1 = (
            s.query(A)
            .options(with_expression(A.my_expr, expr))
            .filter(expr > 10)
            .order_by(expr)
        )

        eq_(a1.all(), [A(my_expr=12), A(my_expr=15)])

    def test_in_joinedload(self):
        A, B = self.classes("A", "B")

        s = fixture_session()

        q = (
            s.query(A)
            .options(joinedload(A.bs).with_expression(B.b_expr, B.p * A.x))
            .filter(A.id.in_([3, 4]))
            .order_by(A.id)
        )

        eq_(
            q.all(), [A(bs=[B(b_expr=25)]), A(bs=[B(b_expr=38), B(b_expr=10)])]
        )

    def test_no_refresh_unless_populate_existing(self):
        A = self.classes.A

        s = fixture_session()
        a1 = s.query(A).first()

        def go():
            eq_(a1.my_expr, None)

        self.assert_sql_count(testing.db, go, 0)

        a1 = s.query(A).options(with_expression(A.my_expr, A.x + A.y)).first()

        eq_(a1.my_expr, None)

        a1 = (
            s.query(A)
            .populate_existing()
            .options(with_expression(A.my_expr, A.x + A.y))
            .first()
        )

        eq_(a1.my_expr, 3)

        a1 = s.query(A).first()

        eq_(a1.my_expr, 3)

        s.expire(a1)

        eq_(a1.my_expr, None)

    def test_no_sql_not_set_up(self):
        A = self.classes.A

        s = fixture_session()
        a1 = s.query(A).first()

        def go():
            eq_(a1.my_expr, None)

        self.assert_sql_count(testing.db, go, 0)

    def test_dont_explode_on_expire_individual(self):
        A = self.classes.A

        s = fixture_session()
        q = (
            s.query(A)
            .options(with_expression(A.my_expr, A.x + A.y))
            .filter(A.x > 1)
            .order_by(A.id)
        )

        a1 = q.first()

        eq_(a1.my_expr, 5)

        s.expire(a1, ["my_expr"])

        eq_(a1.my_expr, None)

        # comes back
        q = (
            s.query(A)
            .options(with_expression(A.my_expr, A.x + A.y))
            .filter(A.x > 1)
            .order_by(A.id)
        )
        q.first()
        eq_(a1.my_expr, 5)

    def test_dont_explode_on_expire_whole(self):
        A = self.classes.A

        s = fixture_session()
        q = (
            s.query(A)
            .options(with_expression(A.my_expr, A.x + A.y))
            .filter(A.x > 1)
            .order_by(A.id)
        )

        a1 = q.first()

        eq_(a1.my_expr, 5)

        s.expire(a1)

        eq_(a1.my_expr, None)

        # comes back
        q = (
            s.query(A)
            .options(with_expression(A.my_expr, A.x + A.y))
            .filter(A.x > 1)
            .order_by(A.id)
        )
        q.first()
        eq_(a1.my_expr, 5)


class RaiseLoadTest(fixtures.DeclarativeMappedTest):
    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class A(fixtures.ComparableEntity, Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)
            x = Column(Integer)
            y = deferred(Column(Integer))
            z = deferred(Column(Integer), raiseload=True)

    @classmethod
    def insert_data(cls, connection):
        A = cls.classes.A
        s = Session(connection)
        s.add(A(id=1, x=2, y=3, z=4))
        s.commit()

    def test_mapper_raise(self):
        A = self.classes.A
        s = fixture_session()
        a1 = s.query(A).first()
        assert_raises_message(
            sa.exc.InvalidRequestError,
            "'A.z' is not available due to raiseload=True",
            getattr,
            a1,
            "z",
        )
        eq_(a1.x, 2)

    def test_mapper_defer_unraise(self):
        A = self.classes.A
        s = fixture_session()
        a1 = s.query(A).options(defer(A.z)).first()
        assert "z" not in a1.__dict__
        eq_(a1.z, 4)
        eq_(a1.x, 2)

    def test_mapper_undefer_unraise(self):
        A = self.classes.A
        s = fixture_session()
        a1 = s.query(A).options(undefer(A.z)).first()
        assert "z" in a1.__dict__
        eq_(a1.z, 4)
        eq_(a1.x, 2)

    def test_deferred_raise_option_raise_column_plain(self):
        A = self.classes.A
        s = fixture_session()
        a1 = s.query(A).options(defer(A.x)).first()
        a1.x

        s.close()

        a1 = s.query(A).options(defer(A.x, raiseload=True)).first()
        assert_raises_message(
            sa.exc.InvalidRequestError,
            "'A.x' is not available due to raiseload=True",
            getattr,
            a1,
            "x",
        )

    def test_deferred_raise_option_load_column_unexpire(self):
        A = self.classes.A
        s = fixture_session()
        a1 = s.query(A).options(defer(A.x, raiseload=True)).first()
        s.expire(a1, ["x"])

        # after expire(), options are cleared.  relationship w/ raiseload
        # works this way also
        eq_(a1.x, 2)

    def test_mapper_raise_after_expire_attr(self):
        A = self.classes.A
        s = fixture_session()
        a1 = s.query(A).first()

        s.expire(a1, ["z"])

        # raises even after expire()
        assert_raises_message(
            sa.exc.InvalidRequestError,
            "'A.z' is not available due to raiseload=True",
            getattr,
            a1,
            "z",
        )

    def test_mapper_raise_after_expire_obj(self):
        A = self.classes.A
        s = fixture_session()
        a1 = s.query(A).first()

        s.expire(a1)

        # raises even after expire()
        assert_raises_message(
            sa.exc.InvalidRequestError,
            "'A.z' is not available due to raiseload=True",
            getattr,
            a1,
            "z",
        )

    def test_mapper_raise_after_modify_attr_expire_obj(self):
        A = self.classes.A
        s = fixture_session()
        a1 = s.query(A).first()

        a1.z = 10
        s.expire(a1)

        # raises even after expire()
        assert_raises_message(
            sa.exc.InvalidRequestError,
            "'A.z' is not available due to raiseload=True",
            getattr,
            a1,
            "z",
        )

    def test_deferred_raise_option_load_after_expire_obj(self):
        A = self.classes.A
        s = fixture_session()
        a1 = s.query(A).options(defer(A.y, raiseload=True)).first()

        s.expire(a1)

        # after expire(), options are cleared.  relationship w/ raiseload
        # works this way also
        eq_(a1.y, 3)

    def test_option_raiseload_unexpire_modified_obj(self):
        A = self.classes.A
        s = fixture_session()
        a1 = s.query(A).options(defer(A.y, raiseload=True)).first()

        a1.y = 10
        s.expire(a1)

        # after expire(), options are cleared.  relationship w/ raiseload
        # works this way also
        eq_(a1.y, 3)

    def test_option_raise_deferred(self):
        A = self.classes.A
        s = fixture_session()
        a1 = s.query(A).options(defer(A.y, raiseload=True)).first()

        assert_raises_message(
            sa.exc.InvalidRequestError,
            "'A.y' is not available due to raiseload=True",
            getattr,
            a1,
            "y",
        )

    def test_does_expire_cancel_normal_defer_option(self):
        A = self.classes.A
        s = fixture_session()
        a1 = s.query(A).options(defer(A.x)).first()

        # expire object
        s.expire(a1)

        # unexpire object
        eq_(a1.id, 1)

        assert "x" in a1.__dict__


class AutoflushTest(fixtures.DeclarativeMappedTest):
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

        A.b_count = deferred(
            select(func.count(1)).where(A.id == B.a_id).scalar_subquery()
        )

    def test_deferred_autoflushes(self):
        A, B = self.classes("A", "B")

        s = fixture_session()

        a1 = A(id=1, bs=[B()])
        s.add(a1)
        s.commit()

        eq_(a1.b_count, 1)
        s.close()

        a1 = s.query(A).first()
        assert "b_count" not in a1.__dict__

        b1 = B(a_id=1)
        s.add(b1)

        eq_(a1.b_count, 2)

        assert b1 in s


class DeferredPopulationTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "thing",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(20)),
        )

        Table(
            "human",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("thing_id", Integer, ForeignKey("thing.id")),
            Column("name", String(20)),
        )

    @classmethod
    def setup_mappers(cls):
        thing, human = cls.tables.thing, cls.tables.human

        class Human(cls.Basic):
            pass

        class Thing(cls.Basic):
            pass

        mapper(Human, human, properties={"thing": relationship(Thing)})
        mapper(Thing, thing, properties={"name": deferred(thing.c.name)})

    @classmethod
    def insert_data(cls, connection):
        thing, human = cls.tables.thing, cls.tables.human

        connection.execute(thing.insert(), [{"id": 1, "name": "Chair"}])

        connection.execute(
            human.insert(), [{"id": 1, "thing_id": 1, "name": "Clark Kent"}]
        )

    def _test(self, thing):
        assert "name" in attributes.instance_state(thing).dict

    def test_no_previous_query(self):
        Thing = self.classes.Thing

        session = fixture_session()
        thing = session.query(Thing).options(sa.orm.undefer("name")).first()
        self._test(thing)

    def test_query_twice_with_clear(self):
        Thing = self.classes.Thing

        session = fixture_session()
        result = session.query(Thing).first()  # noqa
        session.expunge_all()
        thing = session.query(Thing).options(sa.orm.undefer("name")).first()
        self._test(thing)

    def test_query_twice_no_clear(self):
        Thing = self.classes.Thing

        session = fixture_session()
        result = session.query(Thing).first()  # noqa
        thing = session.query(Thing).options(sa.orm.undefer("name")).first()
        self._test(thing)

    def test_joinedload_with_clear(self):
        Thing, Human = self.classes.Thing, self.classes.Human

        session = fixture_session()
        human = (  # noqa
            session.query(Human).options(sa.orm.joinedload("thing")).first()
        )
        session.expunge_all()
        thing = session.query(Thing).options(sa.orm.undefer("name")).first()
        self._test(thing)

    def test_joinedload_no_clear(self):
        Thing, Human = self.classes.Thing, self.classes.Human

        session = fixture_session()
        human = (  # noqa
            session.query(Human).options(sa.orm.joinedload("thing")).first()
        )
        thing = session.query(Thing).options(sa.orm.undefer("name")).first()
        self._test(thing)

    def test_join_with_clear(self):
        Thing, Human = self.classes.Thing, self.classes.Human

        session = fixture_session()
        result = (  # noqa
            session.query(Human).add_entity(Thing).join("thing").first()
        )
        session.expunge_all()
        thing = session.query(Thing).options(sa.orm.undefer("name")).first()
        self._test(thing)

    def test_join_no_clear(self):
        Thing, Human = self.classes.Thing, self.classes.Human

        session = fixture_session()
        result = (  # noqa
            session.query(Human).add_entity(Thing).join("thing").first()
        )
        thing = session.query(Thing).options(sa.orm.undefer("name")).first()
        self._test(thing)
