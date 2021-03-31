import sqlalchemy as sa
from sqlalchemy import bindparam
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import literal_column
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.orm import aliased
from sqlalchemy.orm import backref
from sqlalchemy.orm import clear_mappers
from sqlalchemy.orm import close_all_sessions
from sqlalchemy.orm import deferred
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import mapper
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm import subqueryload
from sqlalchemy.orm import undefer
from sqlalchemy.orm import with_polymorphic
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_not
from sqlalchemy.testing import is_true
from sqlalchemy.testing.assertsql import CompiledSQL
from sqlalchemy.testing.entities import ComparableEntity
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from test.orm import _fixtures
from .inheritance._poly_fixtures import _Polymorphic
from .inheritance._poly_fixtures import Company
from .inheritance._poly_fixtures import Engineer
from .inheritance._poly_fixtures import Machine
from .inheritance._poly_fixtures import MachineType
from .inheritance._poly_fixtures import Page
from .inheritance._poly_fixtures import Paperwork
from .inheritance._poly_fixtures import Person


class EagerTest(_fixtures.FixtureTest, testing.AssertsCompiledSQL):
    run_inserts = "once"
    run_deletes = None

    def test_basic(self):
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
        sess = fixture_session()

        q = sess.query(User).options(subqueryload(User.addresses))

        def go():
            eq_(
                [
                    User(
                        id=7,
                        addresses=[
                            Address(id=1, email_address="jack@bean.com")
                        ],
                    )
                ],
                q.filter(User.id == 7).all(),
            )

        self.assert_sql_count(testing.db, go, 2)

        def go():
            eq_(self.static.user_address_result, q.order_by(User.id).all())

        self.assert_sql_count(testing.db, go, 2)

    def test_params_arent_cached(self):
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
                    mapper(Address, addresses),
                    lazy="subquery",
                    order_by=Address.id,
                )
            },
        )
        query_cache = {}
        sess = fixture_session()

        u1 = (
            sess.query(User)
            .execution_options(query_cache=query_cache)
            .filter(User.id == 7)
            .one()
        )

        u2 = (
            sess.query(User)
            .execution_options(query_cache=query_cache)
            .filter(User.id == 8)
            .one()
        )
        eq_(len(u1.addresses), 1)
        eq_(len(u2.addresses), 3)

    def user_dingaling_fixture(self):
        users, Dingaling, User, dingalings, Address, addresses = (
            self.tables.users,
            self.classes.Dingaling,
            self.classes.User,
            self.tables.dingalings,
            self.classes.Address,
            self.tables.addresses,
        )

        mapper(Dingaling, dingalings)
        mapper(
            Address,
            addresses,
            properties={
                "dingalings": relationship(Dingaling, order_by=Dingaling.id)
            },
        )
        mapper(
            User,
            users,
            properties={
                "addresses": relationship(Address, order_by=Address.id)
            },
        )
        return User, Dingaling, Address

    def test_from_aliased_w_cache_one(self):
        User, Dingaling, Address = self.user_dingaling_fixture()

        for i in range(3):
            sess = fixture_session()

            u = aliased(User)

            q = sess.query(u).options(subqueryload(u.addresses))

            def go():
                eq_(
                    [
                        User(
                            id=7,
                            addresses=[
                                Address(id=1, email_address="jack@bean.com")
                            ],
                        )
                    ],
                    q.filter(u.id == 7).all(),
                )

            self.assert_sql_count(testing.db, go, 2)

    def test_from_aliased_w_cache_two(self):
        User, Dingaling, Address = self.user_dingaling_fixture()

        for i in range(3):
            sess = fixture_session()

            u = aliased(User)

            q = sess.query(u).options(subqueryload(u.addresses))

            def go():
                eq_(self.static.user_address_result, q.order_by(u.id).all())

            self.assert_sql_count(testing.db, go, 2)

    def test_from_aliased_w_cache_three(self):
        User, Dingaling, Address = self.user_dingaling_fixture()

        for i in range(3):
            sess = fixture_session()

            u = aliased(User)
            q = sess.query(u).options(
                subqueryload(u.addresses).subqueryload(Address.dingalings)
            )

            def go():
                eq_(
                    [
                        User(
                            id=8,
                            addresses=[
                                Address(
                                    id=2,
                                    email_address="ed@wood.com",
                                    dingalings=[Dingaling()],
                                ),
                                Address(
                                    id=3, email_address="ed@bettyboop.com"
                                ),
                                Address(id=4, email_address="ed@lala.com"),
                            ],
                        ),
                        User(
                            id=9,
                            addresses=[
                                Address(id=5, dingalings=[Dingaling()])
                            ],
                        ),
                    ],
                    q.filter(u.id.in_([8, 9])).all(),
                )

            self.assert_sql_count(testing.db, go, 3)

    def test_from_get(self):
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
        sess = fixture_session()

        q = sess.query(User).options(subqueryload(User.addresses))

        def go():
            eq_(
                User(
                    id=7,
                    addresses=[Address(id=1, email_address="jack@bean.com")],
                ),
                q.get(7),
            )

        self.assert_sql_count(testing.db, go, 2)

    def test_from_params(self):
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
        sess = fixture_session()

        q = sess.query(User).options(subqueryload(User.addresses))

        def go():
            eq_(
                User(
                    id=7,
                    addresses=[Address(id=1, email_address="jack@bean.com")],
                ),
                q.filter(User.id == bindparam("foo")).params(foo=7).one(),
            )

        self.assert_sql_count(testing.db, go, 2)

    def test_disable_dynamic(self):
        """test no subquery option on a dynamic."""

        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(
            User,
            users,
            properties={"addresses": relationship(Address, lazy="dynamic")},
        )
        mapper(Address, addresses)
        sess = fixture_session()

        # previously this would not raise, but would emit
        # the query needlessly and put the result nowhere.
        assert_raises_message(
            sa.exc.InvalidRequestError,
            "User.addresses' does not support object population - eager "
            "loading cannot be applied.",
            sess.query(User).options(subqueryload(User.addresses)).first,
        )

    def test_many_to_many_plain(self):
        keywords, items, item_keywords, Keyword, Item = (
            self.tables.keywords,
            self.tables.items,
            self.tables.item_keywords,
            self.classes.Keyword,
            self.classes.Item,
        )

        mapper(Keyword, keywords)
        mapper(
            Item,
            items,
            properties=dict(
                keywords=relationship(
                    Keyword,
                    secondary=item_keywords,
                    lazy="subquery",
                    order_by=keywords.c.id,
                )
            ),
        )

        q = fixture_session().query(Item).order_by(Item.id)

        def go():
            eq_(self.static.item_keyword_result, q.all())

        self.assert_sql_count(testing.db, go, 2)

    def test_many_to_many_with_join(self):
        keywords, items, item_keywords, Keyword, Item = (
            self.tables.keywords,
            self.tables.items,
            self.tables.item_keywords,
            self.classes.Keyword,
            self.classes.Item,
        )

        mapper(Keyword, keywords)
        mapper(
            Item,
            items,
            properties=dict(
                keywords=relationship(
                    Keyword,
                    secondary=item_keywords,
                    lazy="subquery",
                    order_by=keywords.c.id,
                )
            ),
        )

        q = fixture_session().query(Item).order_by(Item.id)

        def go():
            eq_(
                self.static.item_keyword_result[0:2],
                q.join("keywords").filter(Keyword.name == "red").all(),
            )

        self.assert_sql_count(testing.db, go, 2)

    def test_many_to_many_with_join_alias(self):
        keywords, items, item_keywords, Keyword, Item = (
            self.tables.keywords,
            self.tables.items,
            self.tables.item_keywords,
            self.classes.Keyword,
            self.classes.Item,
        )

        mapper(Keyword, keywords)
        mapper(
            Item,
            items,
            properties=dict(
                keywords=relationship(
                    Keyword,
                    secondary=item_keywords,
                    lazy="subquery",
                    order_by=keywords.c.id,
                )
            ),
        )

        q = fixture_session().query(Item).order_by(Item.id)

        def go():
            ka = aliased(Keyword)
            eq_(
                self.static.item_keyword_result[0:2],
                (q.join(ka, "keywords").filter(ka.name == "red")).all(),
            )

        self.assert_sql_count(testing.db, go, 2)

    def test_orderby(self):
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
                    mapper(Address, addresses),
                    lazy="subquery",
                    order_by=addresses.c.email_address,
                )
            },
        )
        q = fixture_session().query(User)
        eq_(
            [
                User(id=7, addresses=[Address(id=1)]),
                User(
                    id=8,
                    addresses=[
                        Address(id=3, email_address="ed@bettyboop.com"),
                        Address(id=4, email_address="ed@lala.com"),
                        Address(id=2, email_address="ed@wood.com"),
                    ],
                ),
                User(id=9, addresses=[Address(id=5)]),
                User(id=10, addresses=[]),
            ],
            q.order_by(User.id).all(),
        )

    def test_orderby_multi(self):
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
                    mapper(Address, addresses),
                    lazy="subquery",
                    order_by=[addresses.c.email_address, addresses.c.id],
                )
            },
        )
        q = fixture_session().query(User)
        eq_(
            [
                User(id=7, addresses=[Address(id=1)]),
                User(
                    id=8,
                    addresses=[
                        Address(id=3, email_address="ed@bettyboop.com"),
                        Address(id=4, email_address="ed@lala.com"),
                        Address(id=2, email_address="ed@wood.com"),
                    ],
                ),
                User(id=9, addresses=[Address(id=5)]),
                User(id=10, addresses=[]),
            ],
            q.order_by(User.id).all(),
        )

    def test_orderby_related(self):
        """A regular mapper select on a single table can
        order by a relationship to a second table"""

        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        mapper(Address, addresses)
        mapper(
            User,
            users,
            properties=dict(
                addresses=relationship(
                    Address, lazy="subquery", order_by=addresses.c.id
                )
            ),
        )

        q = fixture_session().query(User)
        result = (
            q.filter(User.id == Address.user_id)
            .order_by(Address.email_address)
            .all()
        )

        eq_(
            [
                User(
                    id=8,
                    addresses=[
                        Address(id=2, email_address="ed@wood.com"),
                        Address(id=3, email_address="ed@bettyboop.com"),
                        Address(id=4, email_address="ed@lala.com"),
                    ],
                ),
                User(id=9, addresses=[Address(id=5)]),
                User(id=7, addresses=[Address(id=1)]),
            ],
            result,
        )

    def test_orderby_desc(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        mapper(Address, addresses)
        mapper(
            User,
            users,
            properties=dict(
                addresses=relationship(
                    Address,
                    lazy="subquery",
                    order_by=[sa.desc(addresses.c.email_address)],
                )
            ),
        )
        sess = fixture_session()
        eq_(
            [
                User(id=7, addresses=[Address(id=1)]),
                User(
                    id=8,
                    addresses=[
                        Address(id=2, email_address="ed@wood.com"),
                        Address(id=4, email_address="ed@lala.com"),
                        Address(id=3, email_address="ed@bettyboop.com"),
                    ],
                ),
                User(id=9, addresses=[Address(id=5)]),
                User(id=10, addresses=[]),
            ],
            sess.query(User).order_by(User.id).all(),
        )

    _pathing_runs = [
        ("lazyload", "lazyload", "lazyload", 15),
        ("subqueryload", "lazyload", "lazyload", 12),
        ("subqueryload", "subqueryload", "lazyload", 8),
        ("joinedload", "subqueryload", "lazyload", 7),
        ("lazyload", "lazyload", "subqueryload", 12),
        ("subqueryload", "subqueryload", "subqueryload", 4),
        ("subqueryload", "subqueryload", "joinedload", 3),
    ]

    def test_options_pathing(self):
        self._do_options_test(self._pathing_runs)

    def test_mapper_pathing(self):
        self._do_mapper_test(self._pathing_runs)

    def _do_options_test(self, configs):
        (
            users,
            Keyword,
            orders,
            items,
            order_items,
            Order,
            Item,
            User,
            keywords,
            item_keywords,
        ) = (
            self.tables.users,
            self.classes.Keyword,
            self.tables.orders,
            self.tables.items,
            self.tables.order_items,
            self.classes.Order,
            self.classes.Item,
            self.classes.User,
            self.tables.keywords,
            self.tables.item_keywords,
        )

        mapper(
            User,
            users,
            properties={
                "orders": relationship(Order, order_by=orders.c.id)  # o2m, m2o
            },
        )
        mapper(
            Order,
            orders,
            properties={
                "items": relationship(
                    Item, secondary=order_items, order_by=items.c.id
                )  # m2m
            },
        )
        mapper(
            Item,
            items,
            properties={
                "keywords": relationship(
                    Keyword, secondary=item_keywords, order_by=keywords.c.id
                )  # m2m
            },
        )
        mapper(Keyword, keywords)

        callables = {"joinedload": joinedload, "subqueryload": subqueryload}

        for o, i, k, count in configs:
            options = []
            if o in callables:
                options.append(callables[o](User.orders))
            if i in callables:
                options.append(callables[i](User.orders, Order.items))
            if k in callables:
                options.append(
                    callables[k](User.orders, Order.items, Item.keywords)
                )

            self._do_query_tests(options, count)

    def _do_mapper_test(self, configs):
        (
            users,
            Keyword,
            orders,
            items,
            order_items,
            Order,
            Item,
            User,
            keywords,
            item_keywords,
        ) = (
            self.tables.users,
            self.classes.Keyword,
            self.tables.orders,
            self.tables.items,
            self.tables.order_items,
            self.classes.Order,
            self.classes.Item,
            self.classes.User,
            self.tables.keywords,
            self.tables.item_keywords,
        )

        opts = {
            "lazyload": "select",
            "joinedload": "joined",
            "subqueryload": "subquery",
        }

        for o, i, k, count in configs:
            mapper(
                User,
                users,
                properties={
                    "orders": relationship(
                        Order, lazy=opts[o], order_by=orders.c.id
                    )
                },
            )
            mapper(
                Order,
                orders,
                properties={
                    "items": relationship(
                        Item,
                        secondary=order_items,
                        lazy=opts[i],
                        order_by=items.c.id,
                    )
                },
            )
            mapper(
                Item,
                items,
                properties={
                    "keywords": relationship(
                        Keyword,
                        lazy=opts[k],
                        secondary=item_keywords,
                        order_by=keywords.c.id,
                    )
                },
            )
            mapper(Keyword, keywords)

            try:
                self._do_query_tests([], count)
            finally:
                clear_mappers()

    def _do_query_tests(self, opts, count):
        Order, User = self.classes.Order, self.classes.User

        with fixture_session() as sess:

            def go():
                eq_(
                    sess.query(User).options(*opts).order_by(User.id).all(),
                    self.static.user_item_keyword_result,
                )

            self.assert_sql_count(testing.db, go, count)

            eq_(
                sess.query(User)
                .options(*opts)
                .filter(User.name == "fred")
                .order_by(User.id)
                .all(),
                self.static.user_item_keyword_result[2:3],
            )

        with fixture_session() as sess:
            eq_(
                sess.query(User)
                .options(*opts)
                .join(User.orders)
                .filter(Order.id == 3)
                .order_by(User.id)
                .all(),
                self.static.user_item_keyword_result[0:1],
            )

    def test_cyclical(self):
        """A circular eager relationship breaks the cycle with a lazy loader"""

        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        mapper(Address, addresses)
        mapper(
            User,
            users,
            properties=dict(
                addresses=relationship(
                    Address,
                    lazy="subquery",
                    backref=sa.orm.backref("user", lazy="subquery"),
                    order_by=Address.id,
                )
            ),
        )
        is_(
            sa.orm.class_mapper(User).get_property("addresses").lazy,
            "subquery",
        )
        is_(sa.orm.class_mapper(Address).get_property("user").lazy, "subquery")

        sess = fixture_session()
        eq_(
            self.static.user_address_result,
            sess.query(User).order_by(User.id).all(),
        )

    def test_cyclical_explicit_join_depth(self):
        """A circular eager relationship breaks the cycle with a lazy loader"""

        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        mapper(Address, addresses)
        mapper(
            User,
            users,
            properties=dict(
                addresses=relationship(
                    Address,
                    lazy="subquery",
                    join_depth=1,
                    backref=sa.orm.backref(
                        "user", lazy="subquery", join_depth=1
                    ),
                    order_by=Address.id,
                )
            ),
        )
        is_(
            sa.orm.class_mapper(User).get_property("addresses").lazy,
            "subquery",
        )
        is_(sa.orm.class_mapper(Address).get_property("user").lazy, "subquery")

        sess = fixture_session()
        eq_(
            self.static.user_address_result,
            sess.query(User).order_by(User.id).all(),
        )

    def test_add_arbitrary_exprs(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        mapper(Address, addresses)
        mapper(
            User,
            users,
            properties=dict(addresses=relationship(Address, lazy="subquery")),
        )

        sess = fixture_session()

        self.assert_compile(
            sess.query(User, literal_column("1")),
            "SELECT users.id AS users_id, users.name AS users_name, "
            "1 FROM users",
        )

    def test_double_w_ac_against_subquery(self):

        (
            users,
            orders,
            User,
            Address,
            Order,
            addresses,
            Item,
            items,
            order_items,
        ) = (
            self.tables.users,
            self.tables.orders,
            self.classes.User,
            self.classes.Address,
            self.classes.Order,
            self.tables.addresses,
            self.classes.Item,
            self.tables.items,
            self.tables.order_items,
        )

        mapper(Address, addresses)
        mapper(
            Order,
            orders,
            properties={
                "items": relationship(
                    Item,
                    secondary=order_items,
                    lazy="subquery",
                    order_by=items.c.id,
                )
            },
        )
        mapper(Item, items)

        open_mapper = aliased(
            Order, select(orders).where(orders.c.isopen == 1).alias()
        )
        closed_mapper = aliased(
            Order, select(orders).where(orders.c.isopen == 0).alias()
        )

        mapper(
            User,
            users,
            properties=dict(
                addresses=relationship(
                    Address, lazy="subquery", order_by=addresses.c.id
                ),
                open_orders=relationship(
                    open_mapper, lazy="subquery", order_by=open_mapper.id
                ),
                closed_orders=relationship(
                    closed_mapper, lazy="subquery", order_by=closed_mapper.id
                ),
            ),
        )

        self._run_double_test()

    def test_double_w_ac(self):

        (
            users,
            orders,
            User,
            Address,
            Order,
            addresses,
            Item,
            items,
            order_items,
        ) = (
            self.tables.users,
            self.tables.orders,
            self.classes.User,
            self.classes.Address,
            self.classes.Order,
            self.tables.addresses,
            self.classes.Item,
            self.tables.items,
            self.tables.order_items,
        )

        mapper(Address, addresses)
        mapper(
            Order,
            orders,
            properties={
                "items": relationship(
                    Item,
                    secondary=order_items,
                    lazy="subquery",
                    order_by=items.c.id,
                )
            },
        )
        mapper(Item, items)

        open_mapper = aliased(Order, orders)
        closed_mapper = aliased(Order, orders)

        mapper(
            User,
            users,
            properties=dict(
                addresses=relationship(
                    Address, lazy="subquery", order_by=addresses.c.id
                ),
                open_orders=relationship(
                    open_mapper,
                    primaryjoin=sa.and_(
                        open_mapper.isopen == 1,
                        users.c.id == open_mapper.user_id,
                    ),
                    lazy="subquery",
                    order_by=open_mapper.id,
                    overlaps="closed_orders",
                ),
                closed_orders=relationship(
                    closed_mapper,
                    primaryjoin=sa.and_(
                        closed_mapper.isopen == 0,
                        users.c.id == closed_mapper.user_id,
                    ),
                    lazy="subquery",
                    order_by=closed_mapper.id,
                    overlaps="open_orders",
                ),
            ),
        )

        self._run_double_test()

    def test_double_same_mappers(self):
        """Eager loading with two relationships simultaneously,
        from the same table, using aliases."""

        (
            addresses,
            items,
            order_items,
            orders,
            Item,
            User,
            Address,
            Order,
            users,
        ) = (
            self.tables.addresses,
            self.tables.items,
            self.tables.order_items,
            self.tables.orders,
            self.classes.Item,
            self.classes.User,
            self.classes.Address,
            self.classes.Order,
            self.tables.users,
        )

        mapper(Address, addresses)
        mapper(
            Order,
            orders,
            properties={
                "items": relationship(
                    Item,
                    secondary=order_items,
                    lazy="subquery",
                    order_by=items.c.id,
                )
            },
        )
        mapper(Item, items)
        mapper(
            User,
            users,
            properties=dict(
                addresses=relationship(
                    Address, lazy="subquery", order_by=addresses.c.id
                ),
                open_orders=relationship(
                    Order,
                    primaryjoin=sa.and_(
                        orders.c.isopen == 1, users.c.id == orders.c.user_id
                    ),
                    lazy="subquery",
                    order_by=orders.c.id,
                    viewonly=True,
                ),
                closed_orders=relationship(
                    Order,
                    primaryjoin=sa.and_(
                        orders.c.isopen == 0, users.c.id == orders.c.user_id
                    ),
                    lazy="subquery",
                    order_by=orders.c.id,
                    viewonly=True,
                ),
            ),
        )
        self._run_double_test()

    def _run_double_test(self, no_items=False):
        User, Address, Order, Item = self.classes(
            "User", "Address", "Order", "Item"
        )
        q = fixture_session().query(User).order_by(User.id)

        def items(*ids):
            if no_items:
                return {}
            else:
                return {"items": [Item(id=id_) for id_ in ids]}

        def go():
            eq_(
                [
                    User(
                        id=7,
                        addresses=[Address(id=1)],
                        open_orders=[Order(id=3, **items(3, 4, 5))],
                        closed_orders=[
                            Order(id=1, **items(1, 2, 3)),
                            Order(id=5, **items(5)),
                        ],
                    ),
                    User(
                        id=8,
                        addresses=[
                            Address(id=2),
                            Address(id=3),
                            Address(id=4),
                        ],
                        open_orders=[],
                        closed_orders=[],
                    ),
                    User(
                        id=9,
                        addresses=[Address(id=5)],
                        open_orders=[Order(id=4, **items(1, 5))],
                        closed_orders=[Order(id=2, **items(1, 2, 3))],
                    ),
                    User(id=10),
                ],
                q.all(),
            )

        if no_items:
            self.assert_sql_count(testing.db, go, 4)
        else:
            self.assert_sql_count(testing.db, go, 6)

    @testing.combinations(
        ("plain",), ("cte", testing.requires.ctes), ("subquery",), id_="s"
    )
    def test_map_to_cte_subq(self, type_):
        User, Address = self.classes("User", "Address")
        users, addresses = self.tables("users", "addresses")

        if type_ == "plain":
            target = users
        elif type_ == "cte":
            target = select(users).cte()
        elif type_ == "subquery":
            target = select(users).subquery()

        mapper(
            User,
            target,
            properties={"addresses": relationship(Address, backref="user")},
        )
        mapper(Address, addresses)

        sess = fixture_session()

        q = (
            sess.query(Address)
            .options(subqueryload(Address.user))
            .order_by(Address.id)
        )
        eq_(q.all(), self.static.address_user_result)

    def test_limit(self):
        """Limit operations combined with lazy-load relationships."""

        (
            users,
            items,
            order_items,
            orders,
            Item,
            User,
            Address,
            Order,
            addresses,
        ) = (
            self.tables.users,
            self.tables.items,
            self.tables.order_items,
            self.tables.orders,
            self.classes.Item,
            self.classes.User,
            self.classes.Address,
            self.classes.Order,
            self.tables.addresses,
        )

        mapper(Item, items)
        mapper(
            Order,
            orders,
            properties={
                "items": relationship(
                    Item,
                    secondary=order_items,
                    lazy="subquery",
                    order_by=items.c.id,
                )
            },
        )
        mapper(
            User,
            users,
            properties={
                "addresses": relationship(
                    mapper(Address, addresses),
                    lazy="subquery",
                    order_by=addresses.c.id,
                ),
                "orders": relationship(
                    Order, lazy="select", order_by=orders.c.id
                ),
            },
        )

        sess = fixture_session()
        q = sess.query(User)

        result = q.order_by(User.id).limit(2).offset(1).all()
        eq_(self.static.user_all_result[1:3], result)

        result = q.order_by(sa.desc(User.id)).limit(2).offset(2).all()
        eq_(list(reversed(self.static.user_all_result[0:2])), result)

    def test_group_by_only(self):
        # test group_by() not impacting results, similarly to joinedload
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
                    mapper(Address, addresses),
                    lazy="subquery",
                    order_by=addresses.c.email_address,
                )
            },
        )

        q = fixture_session().query(User)
        eq_(
            [
                User(id=7, addresses=[Address(id=1)]),
                User(
                    id=8,
                    addresses=[
                        Address(id=3, email_address="ed@bettyboop.com"),
                        Address(id=4, email_address="ed@lala.com"),
                        Address(id=2, email_address="ed@wood.com"),
                    ],
                ),
                User(id=9, addresses=[Address(id=5)]),
                User(id=10, addresses=[]),
            ],
            q.order_by(User.id).group_by(User).all(),  # group by all columns
        )

    def test_one_to_many_scalar(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        mapper(
            User,
            users,
            properties=dict(
                address=relationship(
                    mapper(Address, addresses), lazy="subquery", uselist=False
                )
            ),
        )
        q = fixture_session().query(User)

        def go():
            result = q.filter(users.c.id == 7).all()
            eq_([User(id=7, address=Address(id=1))], result)

        self.assert_sql_count(testing.db, go, 2)

    def test_many_to_one(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(
            Address,
            addresses,
            properties=dict(
                user=relationship(mapper(User, users), lazy="subquery")
            ),
        )
        sess = fixture_session()
        q = sess.query(Address)

        def go():
            a = q.filter(addresses.c.id == 1).one()
            is_not(a.user, None)
            u1 = sess.query(User).get(7)
            is_(a.user, u1)

        self.assert_sql_count(testing.db, go, 2)

    def test_double_with_aggregate(self):
        User, users, orders, Order = (
            self.classes.User,
            self.tables.users,
            self.tables.orders,
            self.classes.Order,
        )

        max_orders_by_user = (
            sa.select(sa.func.max(orders.c.id).label("order_id"))
            .group_by(orders.c.user_id)
            .alias("max_orders_by_user")
        )

        max_orders = orders.select(
            orders.c.id == max_orders_by_user.c.order_id
        ).alias("max_orders")

        mapper(Order, orders)
        mapper(
            User,
            users,
            properties={
                "orders": relationship(
                    Order,
                    backref="user",
                    lazy="subquery",
                    order_by=orders.c.id,
                ),
                "max_order": relationship(
                    aliased(Order, max_orders), lazy="subquery", uselist=False
                ),
            },
        )

        q = fixture_session().query(User)

        def go():
            eq_(
                [
                    User(
                        id=7,
                        orders=[Order(id=1), Order(id=3), Order(id=5)],
                        max_order=Order(id=5),
                    ),
                    User(id=8, orders=[]),
                    User(
                        id=9,
                        orders=[Order(id=2), Order(id=4)],
                        max_order=Order(id=4),
                    ),
                    User(id=10),
                ],
                q.order_by(User.id).all(),
            )

        self.assert_sql_count(testing.db, go, 3)

    def test_uselist_false_warning(self):
        """test that multiple rows received by a
        uselist=False raises a warning."""

        User, users, orders, Order = (
            self.classes.User,
            self.tables.users,
            self.tables.orders,
            self.classes.Order,
        )

        mapper(
            User,
            users,
            properties={"order": relationship(Order, uselist=False)},
        )
        mapper(Order, orders)
        s = fixture_session()
        assert_raises(
            sa.exc.SAWarning,
            s.query(User).options(subqueryload(User.order)).all,
        )


class LoadOnExistingTest(_fixtures.FixtureTest):
    """test that loaders from a base Query fully populate."""

    run_inserts = "once"
    run_deletes = None

    def _collection_to_scalar_fixture(self):
        User, Address, Dingaling = (
            self.classes.User,
            self.classes.Address,
            self.classes.Dingaling,
        )
        mapper(
            User,
            self.tables.users,
            properties={"addresses": relationship(Address)},
        )
        mapper(
            Address,
            self.tables.addresses,
            properties={"dingaling": relationship(Dingaling)},
        )
        mapper(Dingaling, self.tables.dingalings)

        sess = fixture_session(autoflush=False)
        return User, Address, Dingaling, sess

    def _collection_to_collection_fixture(self):
        User, Order, Item = (
            self.classes.User,
            self.classes.Order,
            self.classes.Item,
        )
        mapper(
            User, self.tables.users, properties={"orders": relationship(Order)}
        )
        mapper(
            Order,
            self.tables.orders,
            properties={
                "items": relationship(Item, secondary=self.tables.order_items)
            },
        )
        mapper(Item, self.tables.items)

        sess = fixture_session(autoflush=False)
        return User, Order, Item, sess

    def _eager_config_fixture(self):
        User, Address = self.classes.User, self.classes.Address
        mapper(
            User,
            self.tables.users,
            properties={"addresses": relationship(Address, lazy="subquery")},
        )
        mapper(Address, self.tables.addresses)
        sess = fixture_session(autoflush=False)
        return User, Address, sess

    def _deferred_config_fixture(self):
        User, Address = self.classes.User, self.classes.Address
        mapper(
            User,
            self.tables.users,
            properties={
                "name": deferred(self.tables.users.c.name),
                "addresses": relationship(Address, lazy="subquery"),
            },
        )
        mapper(Address, self.tables.addresses)
        sess = fixture_session(autoflush=False)
        return User, Address, sess

    def test_runs_query_on_refresh(self):
        User, Address, sess = self._eager_config_fixture()

        u1 = sess.query(User).get(8)
        assert "addresses" in u1.__dict__
        sess.expire(u1)

        def go():
            eq_(u1.id, 8)

        self.assert_sql_count(testing.db, go, 2)
        assert "addresses" in u1.__dict__

    def test_no_query_on_deferred(self):
        User, Address, sess = self._deferred_config_fixture()
        u1 = sess.query(User).get(8)
        assert "addresses" in u1.__dict__
        sess.expire(u1, ["addresses"])

        def go():
            eq_(u1.name, "ed")

        self.assert_sql_count(testing.db, go, 1)
        assert "addresses" not in u1.__dict__

    def test_populate_existing_propagate(self):
        User, Address, sess = self._eager_config_fixture()
        u1 = sess.query(User).get(8)
        u1.addresses[2].email_address = "foofoo"
        del u1.addresses[1]
        u1 = sess.query(User).populate_existing().filter_by(id=8).one()
        # collection is reverted
        eq_(len(u1.addresses), 3)

        # attributes on related items reverted
        eq_(u1.addresses[2].email_address, "ed@lala.com")

    def test_loads_second_level_collection_to_scalar(self):
        User, Address, Dingaling, sess = self._collection_to_scalar_fixture()

        u1 = sess.query(User).get(8)
        a1 = Address()
        u1.addresses.append(a1)
        a2 = u1.addresses[0]
        a2.email_address = "foo"
        sess.query(User).options(
            subqueryload("addresses").subqueryload("dingaling")
        ).filter_by(id=8).all()
        assert u1.addresses[-1] is a1
        for a in u1.addresses:
            if a is not a1:
                assert "dingaling" in a.__dict__
            else:
                assert "dingaling" not in a.__dict__
            if a is a2:
                eq_(a2.email_address, "foo")

    def test_loads_second_level_collection_to_collection(self):
        User, Order, Item, sess = self._collection_to_collection_fixture()

        u1 = sess.query(User).get(7)
        u1.orders
        o1 = Order()
        u1.orders.append(o1)
        sess.query(User).options(
            subqueryload("orders").subqueryload("items")
        ).filter_by(id=7).all()
        for o in u1.orders:
            if o is not o1:
                assert "items" in o.__dict__
            else:
                assert "items" not in o.__dict__

    def test_load_two_levels_collection_to_scalar(self):
        User, Address, Dingaling, sess = self._collection_to_scalar_fixture()

        u1 = (
            sess.query(User)
            .filter_by(id=8)
            .options(subqueryload("addresses"))
            .one()
        )
        sess.query(User).filter_by(id=8).options(
            subqueryload("addresses").subqueryload("dingaling")
        ).first()
        assert "dingaling" in u1.addresses[0].__dict__

    def test_load_two_levels_collection_to_collection(self):
        User, Order, Item, sess = self._collection_to_collection_fixture()

        u1 = (
            sess.query(User)
            .filter_by(id=7)
            .options(subqueryload("orders"))
            .one()
        )
        sess.query(User).filter_by(id=7).options(
            subqueryload("orders").subqueryload("items")
        ).first()
        assert "items" in u1.orders[0].__dict__


class OrderBySecondaryTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "m2m",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("aid", Integer, ForeignKey("a.id")),
            Column("bid", Integer, ForeignKey("b.id")),
        )

        Table(
            "a",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(50)),
        )
        Table(
            "b",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(50)),
        )

    @classmethod
    def fixtures(cls):
        return dict(
            a=(("id", "data"), (1, "a1"), (2, "a2")),
            b=(("id", "data"), (1, "b1"), (2, "b2"), (3, "b3"), (4, "b4")),
            m2m=(
                ("id", "aid", "bid"),
                (2, 1, 1),
                (4, 2, 4),
                (1, 1, 3),
                (6, 2, 2),
                (3, 1, 2),
                (5, 2, 3),
            ),
        )

    def test_ordering(self):
        a, m2m, b = (self.tables.a, self.tables.m2m, self.tables.b)

        class A(fixtures.ComparableEntity):
            pass

        class B(fixtures.ComparableEntity):
            pass

        mapper(
            A,
            a,
            properties={
                "bs": relationship(
                    B, secondary=m2m, lazy="subquery", order_by=m2m.c.id
                )
            },
        )
        mapper(B, b)

        sess = fixture_session()

        def go():
            eq_(
                sess.query(A).all(),
                [
                    A(
                        data="a1",
                        bs=[B(data="b3"), B(data="b1"), B(data="b2")],
                    ),
                    A(bs=[B(data="b4"), B(data="b3"), B(data="b2")]),
                ],
            )

        self.assert_sql_count(testing.db, go, 2)


class BaseRelationFromJoinedSubclassTest(_Polymorphic):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "people",
            metadata,
            Column(
                "person_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("name", String(50)),
            Column("type", String(30)),
        )

        # to test fully, PK of engineers table must be
        # named differently from that of people
        Table(
            "engineers",
            metadata,
            Column(
                "engineer_id",
                Integer,
                ForeignKey("people.person_id"),
                primary_key=True,
            ),
            Column("primary_language", String(50)),
        )

        Table(
            "paperwork",
            metadata,
            Column(
                "paperwork_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("description", String(50)),
            Column("person_id", Integer, ForeignKey("people.person_id")),
        )

        Table(
            "pages",
            metadata,
            Column(
                "page_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("stuff", String(50)),
            Column("paperwork_id", ForeignKey("paperwork.paperwork_id")),
        )

    @classmethod
    def setup_mappers(cls):
        people = cls.tables.people
        engineers = cls.tables.engineers
        paperwork = cls.tables.paperwork
        pages = cls.tables.pages

        mapper(
            Person,
            people,
            polymorphic_on=people.c.type,
            polymorphic_identity="person",
            properties={
                "paperwork": relationship(
                    Paperwork, order_by=paperwork.c.paperwork_id
                )
            },
        )

        mapper(
            Engineer,
            engineers,
            inherits=Person,
            polymorphic_identity="engineer",
        )

        mapper(
            Paperwork,
            paperwork,
            properties={"pages": relationship(Page, order_by=pages.c.page_id)},
        )

        mapper(Page, pages)

    @classmethod
    def insert_data(cls, connection):

        e1 = Engineer(primary_language="java")
        e2 = Engineer(primary_language="c++")
        e1.paperwork = [
            Paperwork(
                description="tps report #1",
                pages=[
                    Page(stuff="report1 page1"),
                    Page(stuff="report1 page2"),
                ],
            ),
            Paperwork(
                description="tps report #2",
                pages=[
                    Page(stuff="report2 page1"),
                    Page(stuff="report2 page2"),
                ],
            ),
        ]
        e2.paperwork = [Paperwork(description="tps report #3")]
        sess = Session(connection)
        sess.add_all([e1, e2])
        sess.flush()

    def test_correct_subquery_nofrom(self):
        sess = fixture_session()
        # use Person.paperwork here just to give the least
        # amount of context
        q = (
            sess.query(Engineer)
            .filter(Engineer.primary_language == "java")
            .options(subqueryload(Person.paperwork))
        )

        def go():
            eq_(
                q.all()[0].paperwork,
                [
                    Paperwork(description="tps report #1"),
                    Paperwork(description="tps report #2"),
                ],
            )

        self.assert_sql_execution(
            testing.db,
            go,
            CompiledSQL(
                "SELECT people.person_id AS people_person_id, "
                "people.name AS people_name, people.type AS people_type, "
                "engineers.engineer_id AS engineers_engineer_id, "
                "engineers.primary_language AS engineers_primary_language "
                "FROM people JOIN engineers ON "
                "people.person_id = engineers.engineer_id "
                "WHERE engineers.primary_language = :primary_language_1",
                {"primary_language_1": "java"},
            ),
            # ensure we get "people JOIN engineer" here, even though
            # primary key "people.person_id" is against "Person"
            # *and* the path comes out as "Person.paperwork", still
            # want to select from "Engineer" entity
            CompiledSQL(
                "SELECT paperwork.paperwork_id AS paperwork_paperwork_id, "
                "paperwork.description AS paperwork_description, "
                "paperwork.person_id AS paperwork_person_id, "
                "anon_1.people_person_id AS anon_1_people_person_id "
                "FROM (SELECT people.person_id AS people_person_id "
                "FROM people JOIN engineers "
                "ON people.person_id = engineers.engineer_id "
                "WHERE engineers.primary_language = "
                ":primary_language_1) AS anon_1 "
                "JOIN paperwork "
                "ON anon_1.people_person_id = paperwork.person_id "
                "ORDER BY paperwork.paperwork_id",
                {"primary_language_1": "java"},
            ),
        )

    def test_correct_subquery_existingfrom(self):
        sess = fixture_session()
        # use Person.paperwork here just to give the least
        # amount of context
        q = (
            sess.query(Engineer)
            .filter(Engineer.primary_language == "java")
            .join(Engineer.paperwork)
            .filter(Paperwork.description == "tps report #2")
            .options(subqueryload(Person.paperwork))
        )

        def go():
            eq_(
                q.one().paperwork,
                [
                    Paperwork(description="tps report #1"),
                    Paperwork(description="tps report #2"),
                ],
            )

        self.assert_sql_execution(
            testing.db,
            go,
            CompiledSQL(
                "SELECT people.person_id AS people_person_id, "
                "people.name AS people_name, people.type AS people_type, "
                "engineers.engineer_id AS engineers_engineer_id, "
                "engineers.primary_language AS engineers_primary_language "
                "FROM people JOIN engineers "
                "ON people.person_id = engineers.engineer_id "
                "JOIN paperwork ON people.person_id = paperwork.person_id "
                "WHERE engineers.primary_language = :primary_language_1 "
                "AND paperwork.description = :description_1",
                {
                    "primary_language_1": "java",
                    "description_1": "tps report #2",
                },
            ),
            CompiledSQL(
                "SELECT paperwork.paperwork_id AS paperwork_paperwork_id, "
                "paperwork.description AS paperwork_description, "
                "paperwork.person_id AS paperwork_person_id, "
                "anon_1.people_person_id AS anon_1_people_person_id "
                "FROM (SELECT people.person_id AS people_person_id "
                "FROM people JOIN engineers ON people.person_id = "
                "engineers.engineer_id JOIN paperwork "
                "ON people.person_id = paperwork.person_id "
                "WHERE engineers.primary_language = :primary_language_1 AND "
                "paperwork.description = :description_1) AS anon_1 "
                "JOIN paperwork ON anon_1.people_person_id = "
                "paperwork.person_id "
                "ORDER BY paperwork.paperwork_id",
                {
                    "primary_language_1": "java",
                    "description_1": "tps report #2",
                },
            ),
        )

    def test_correct_subquery_multilevel(self):
        sess = fixture_session()
        # use Person.paperwork here just to give the least
        # amount of context
        q = (
            sess.query(Engineer)
            .filter(Engineer.primary_language == "java")
            .options(
                subqueryload(Engineer.paperwork).subqueryload(Paperwork.pages)
            )
        )

        def go():
            eq_(
                q.one().paperwork,
                [
                    Paperwork(
                        description="tps report #1",
                        pages=[
                            Page(stuff="report1 page1"),
                            Page(stuff="report1 page2"),
                        ],
                    ),
                    Paperwork(
                        description="tps report #2",
                        pages=[
                            Page(stuff="report2 page1"),
                            Page(stuff="report2 page2"),
                        ],
                    ),
                ],
            )

        self.assert_sql_execution(
            testing.db,
            go,
            CompiledSQL(
                "SELECT people.person_id AS people_person_id, "
                "people.name AS people_name, people.type AS people_type, "
                "engineers.engineer_id AS engineers_engineer_id, "
                "engineers.primary_language AS engineers_primary_language "
                "FROM people JOIN engineers "
                "ON people.person_id = engineers.engineer_id "
                "WHERE engineers.primary_language = :primary_language_1",
                {"primary_language_1": "java"},
            ),
            CompiledSQL(
                "SELECT paperwork.paperwork_id AS paperwork_paperwork_id, "
                "paperwork.description AS paperwork_description, "
                "paperwork.person_id AS paperwork_person_id, "
                "anon_1.people_person_id AS anon_1_people_person_id "
                "FROM (SELECT people.person_id AS people_person_id "
                "FROM people JOIN engineers "
                "ON people.person_id = engineers.engineer_id "
                "WHERE engineers.primary_language = :primary_language_1) "
                "AS anon_1 JOIN paperwork "
                "ON anon_1.people_person_id = paperwork.person_id "
                "ORDER BY paperwork.paperwork_id",
                {"primary_language_1": "java"},
            ),
            CompiledSQL(
                "SELECT pages.page_id AS pages_page_id, "
                "pages.stuff AS pages_stuff, "
                "pages.paperwork_id AS pages_paperwork_id, "
                "paperwork_1.paperwork_id AS paperwork_1_paperwork_id "
                "FROM (SELECT people.person_id AS people_person_id "
                "FROM people JOIN engineers ON people.person_id = "
                "engineers.engineer_id "
                "WHERE engineers.primary_language = :primary_language_1) "
                "AS anon_1 JOIN paperwork AS paperwork_1 "
                "ON anon_1.people_person_id = paperwork_1.person_id "
                "JOIN pages ON paperwork_1.paperwork_id = pages.paperwork_id "
                "ORDER BY pages.page_id",
                {"primary_language_1": "java"},
            ),
        )

    def test_correct_subquery_with_polymorphic_no_alias(self):
        # test #3106
        sess = fixture_session()

        wp = with_polymorphic(Person, [Engineer])
        q = (
            sess.query(wp)
            .options(subqueryload(wp.paperwork))
            .order_by(Engineer.primary_language.desc())
        )

        def go():
            eq_(
                q.first(),
                Engineer(
                    paperwork=[
                        Paperwork(description="tps report #1"),
                        Paperwork(description="tps report #2"),
                    ],
                    primary_language="java",
                ),
            )

        self.assert_sql_execution(
            testing.db,
            go,
            CompiledSQL(
                "SELECT people.person_id AS people_person_id, "
                "people.name AS people_name, people.type AS people_type, "
                "engineers.engineer_id AS engineers_engineer_id, "
                "engineers.primary_language AS engineers_primary_language "
                "FROM people LEFT OUTER JOIN engineers ON people.person_id = "
                "engineers.engineer_id ORDER BY engineers.primary_language "
                "DESC LIMIT :param_1"
            ),
            CompiledSQL(
                "SELECT paperwork.paperwork_id AS paperwork_paperwork_id, "
                "paperwork.description AS paperwork_description, "
                "paperwork.person_id AS paperwork_person_id, "
                "anon_1.people_person_id AS anon_1_people_person_id FROM "
                "(SELECT people.person_id AS people_person_id FROM people "
                "LEFT OUTER JOIN engineers ON people.person_id = "
                "engineers.engineer_id ORDER BY engineers.primary_language "
                "DESC LIMIT :param_1) AS anon_1 JOIN paperwork "
                "ON anon_1.people_person_id = paperwork.person_id "
                "ORDER BY paperwork.paperwork_id"
            ),
        )

    def test_correct_subquery_with_polymorphic_alias(self):
        # test #3106
        sess = fixture_session()

        wp = with_polymorphic(Person, [Engineer], aliased=True)
        q = (
            sess.query(wp)
            .options(subqueryload(wp.paperwork))
            .order_by(wp.Engineer.primary_language.desc())
        )

        def go():
            eq_(
                q.first(),
                Engineer(
                    paperwork=[
                        Paperwork(description="tps report #1"),
                        Paperwork(description="tps report #2"),
                    ],
                    primary_language="java",
                ),
            )

        self.assert_sql_execution(
            testing.db,
            go,
            CompiledSQL(
                "SELECT anon_1.people_person_id AS anon_1_people_person_id, "
                "anon_1.people_name AS anon_1_people_name, "
                "anon_1.people_type AS anon_1_people_type, "
                "anon_1.engineers_engineer_id AS "
                "anon_1_engineers_engineer_id, "
                "anon_1.engineers_primary_language "
                "AS anon_1_engineers_primary_language FROM "
                "(SELECT people.person_id AS people_person_id, "
                "people.name AS people_name, people.type AS people_type, "
                "engineers.engineer_id AS engineers_engineer_id, "
                "engineers.primary_language AS engineers_primary_language "
                "FROM people LEFT OUTER JOIN engineers ON people.person_id = "
                "engineers.engineer_id) AS anon_1 "
                "ORDER BY anon_1.engineers_primary_language DESC "
                "LIMIT :param_1"
            ),
            CompiledSQL(
                "SELECT paperwork.paperwork_id AS paperwork_paperwork_id, "
                "paperwork.description AS paperwork_description, "
                "paperwork.person_id AS paperwork_person_id, "
                "anon_1.anon_2_people_person_id AS "
                "anon_1_anon_2_people_person_id FROM "
                "(SELECT DISTINCT anon_2.people_person_id AS "
                "anon_2_people_person_id, "
                "anon_2.engineers_primary_language AS "
                "anon_2_engineers_primary_language FROM "
                "(SELECT people.person_id AS people_person_id, "
                "people.name AS people_name, people.type AS people_type, "
                "engineers.engineer_id AS engineers_engineer_id, "
                "engineers.primary_language AS engineers_primary_language "
                "FROM people LEFT OUTER JOIN engineers ON people.person_id = "
                "engineers.engineer_id) AS anon_2 "
                "ORDER BY anon_2.engineers_primary_language "
                "DESC LIMIT :param_1) AS anon_1 "
                "JOIN paperwork "
                "ON anon_1.anon_2_people_person_id = paperwork.person_id "
                "ORDER BY paperwork.paperwork_id"
            ),
        )

    def test_correct_subquery_with_polymorphic_flat_alias(self):
        # test #3106
        sess = fixture_session()

        wp = with_polymorphic(Person, [Engineer], aliased=True, flat=True)
        q = (
            sess.query(wp)
            .options(subqueryload(wp.paperwork))
            .order_by(wp.Engineer.primary_language.desc())
        )

        def go():
            eq_(
                q.first(),
                Engineer(
                    paperwork=[
                        Paperwork(description="tps report #1"),
                        Paperwork(description="tps report #2"),
                    ],
                    primary_language="java",
                ),
            )

        self.assert_sql_execution(
            testing.db,
            go,
            CompiledSQL(
                "SELECT people_1.person_id AS people_1_person_id, "
                "people_1.name AS people_1_name, "
                "people_1.type AS people_1_type, "
                "engineers_1.engineer_id AS engineers_1_engineer_id, "
                "engineers_1.primary_language AS engineers_1_primary_language "
                "FROM people AS people_1 "
                "LEFT OUTER JOIN engineers AS engineers_1 "
                "ON people_1.person_id = engineers_1.engineer_id "
                "ORDER BY engineers_1.primary_language DESC LIMIT :param_1"
            ),
            CompiledSQL(
                "SELECT paperwork.paperwork_id AS paperwork_paperwork_id, "
                "paperwork.description AS paperwork_description, "
                "paperwork.person_id AS paperwork_person_id, "
                "anon_1.people_1_person_id AS anon_1_people_1_person_id "
                "FROM (SELECT people_1.person_id AS people_1_person_id "
                "FROM people AS people_1 "
                "LEFT OUTER JOIN engineers AS engineers_1 "
                "ON people_1.person_id = engineers_1.engineer_id "
                "ORDER BY engineers_1.primary_language DESC LIMIT :param_1) "
                "AS anon_1 JOIN paperwork ON anon_1.people_1_person_id = "
                "paperwork.person_id ORDER BY paperwork.paperwork_id"
            ),
        )


class SubRelationFromJoinedSubclassMultiLevelTest(_Polymorphic):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "companies",
            metadata,
            Column(
                "company_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("name", String(50)),
        )

        Table(
            "people",
            metadata,
            Column(
                "person_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("company_id", ForeignKey("companies.company_id")),
            Column("name", String(50)),
            Column("type", String(30)),
        )

        Table(
            "engineers",
            metadata,
            Column(
                "engineer_id", ForeignKey("people.person_id"), primary_key=True
            ),
            Column("primary_language", String(50)),
        )

        Table(
            "machines",
            metadata,
            Column(
                "machine_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("name", String(50)),
            Column("engineer_id", ForeignKey("engineers.engineer_id")),
            Column(
                "machine_type_id", ForeignKey("machine_type.machine_type_id")
            ),
        )

        Table(
            "machine_type",
            metadata,
            Column(
                "machine_type_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("name", String(50)),
        )

    @classmethod
    def setup_mappers(cls):
        companies = cls.tables.companies
        people = cls.tables.people
        engineers = cls.tables.engineers
        machines = cls.tables.machines
        machine_type = cls.tables.machine_type

        mapper(
            Company,
            companies,
            properties={
                "employees": relationship(Person, order_by=people.c.person_id)
            },
        )
        mapper(
            Person,
            people,
            polymorphic_on=people.c.type,
            polymorphic_identity="person",
            with_polymorphic="*",
        )

        mapper(
            Engineer,
            engineers,
            inherits=Person,
            polymorphic_identity="engineer",
            properties={
                "machines": relationship(
                    Machine, order_by=machines.c.machine_id
                )
            },
        )

        mapper(
            Machine, machines, properties={"type": relationship(MachineType)}
        )
        mapper(MachineType, machine_type)

    @classmethod
    def insert_data(cls, connection):
        c1 = cls._fixture()
        sess = Session(connection)
        sess.add(c1)
        sess.flush()

    @classmethod
    def _fixture(cls):
        mt1 = MachineType(name="mt1")
        mt2 = MachineType(name="mt2")
        return Company(
            employees=[
                Engineer(
                    name="e1",
                    machines=[
                        Machine(name="m1", type=mt1),
                        Machine(name="m2", type=mt2),
                    ],
                ),
                Engineer(
                    name="e2",
                    machines=[
                        Machine(name="m3", type=mt1),
                        Machine(name="m4", type=mt1),
                    ],
                ),
            ]
        )

    def test_chained_subq_subclass(self):
        s = fixture_session()
        q = s.query(Company).options(
            subqueryload(Company.employees.of_type(Engineer))
            .subqueryload(Engineer.machines)
            .subqueryload(Machine.type)
        )

        def go():
            eq_(q.all(), [self._fixture()])

        self.assert_sql_count(testing.db, go, 4)


class SelfReferentialTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "nodes",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("parent_id", Integer, ForeignKey("nodes.id")),
            Column("data", String(30)),
        )

    def test_basic(self):
        nodes = self.tables.nodes

        class Node(fixtures.ComparableEntity):
            def append(self, node):
                self.children.append(node)

        mapper(
            Node,
            nodes,
            properties={
                "children": relationship(
                    Node, lazy="subquery", join_depth=3, order_by=nodes.c.id
                )
            },
        )
        sess = fixture_session()
        n1 = Node(data="n1")
        n1.append(Node(data="n11"))
        n1.append(Node(data="n12"))
        n1.append(Node(data="n13"))
        n1.children[1].append(Node(data="n121"))
        n1.children[1].append(Node(data="n122"))
        n1.children[1].append(Node(data="n123"))
        n2 = Node(data="n2")
        n2.append(Node(data="n21"))
        n2.children[0].append(Node(data="n211"))
        n2.children[0].append(Node(data="n212"))

        sess.add(n1)
        sess.add(n2)
        sess.flush()
        sess.expunge_all()

        def go():
            d = (
                sess.query(Node)
                .filter(Node.data.in_(["n1", "n2"]))
                .order_by(Node.data)
                .all()
            )
            eq_(
                [
                    Node(
                        data="n1",
                        children=[
                            Node(data="n11"),
                            Node(
                                data="n12",
                                children=[
                                    Node(data="n121"),
                                    Node(data="n122"),
                                    Node(data="n123"),
                                ],
                            ),
                            Node(data="n13"),
                        ],
                    ),
                    Node(
                        data="n2",
                        children=[
                            Node(
                                data="n21",
                                children=[
                                    Node(data="n211"),
                                    Node(data="n212"),
                                ],
                            )
                        ],
                    ),
                ],
                d,
            )

        self.assert_sql_count(testing.db, go, 4)

    def test_lazy_fallback_doesnt_affect_eager(self):
        nodes = self.tables.nodes

        class Node(fixtures.ComparableEntity):
            def append(self, node):
                self.children.append(node)

        mapper(
            Node,
            nodes,
            properties={
                "children": relationship(
                    Node, lazy="subquery", join_depth=1, order_by=nodes.c.id
                )
            },
        )
        sess = fixture_session()
        n1 = Node(data="n1")
        n1.append(Node(data="n11"))
        n1.append(Node(data="n12"))
        n1.append(Node(data="n13"))
        n1.children[0].append(Node(data="n111"))
        n1.children[0].append(Node(data="n112"))
        n1.children[1].append(Node(data="n121"))
        n1.children[1].append(Node(data="n122"))
        n1.children[1].append(Node(data="n123"))
        sess.add(n1)
        sess.flush()
        sess.expunge_all()

        def go():
            allnodes = sess.query(Node).order_by(Node.data).all()

            n11 = allnodes[1]
            eq_(n11.data, "n11")
            eq_([Node(data="n111"), Node(data="n112")], list(n11.children))

            n12 = allnodes[4]
            eq_(n12.data, "n12")
            eq_(
                [Node(data="n121"), Node(data="n122"), Node(data="n123")],
                list(n12.children),
            )

        self.assert_sql_count(testing.db, go, 2)

    def test_with_deferred(self):
        nodes = self.tables.nodes

        class Node(fixtures.ComparableEntity):
            def append(self, node):
                self.children.append(node)

        mapper(
            Node,
            nodes,
            properties={
                "children": relationship(
                    Node, lazy="subquery", join_depth=3, order_by=nodes.c.id
                ),
                "data": deferred(nodes.c.data),
            },
        )
        sess = fixture_session()
        n1 = Node(data="n1")
        n1.append(Node(data="n11"))
        n1.append(Node(data="n12"))
        sess.add(n1)
        sess.flush()
        sess.expunge_all()

        def go():
            eq_(
                Node(data="n1", children=[Node(data="n11"), Node(data="n12")]),
                sess.query(Node).order_by(Node.id).first(),
            )

        self.assert_sql_count(testing.db, go, 6)

        sess.expunge_all()

        def go():
            eq_(
                Node(data="n1", children=[Node(data="n11"), Node(data="n12")]),
                sess.query(Node)
                .options(undefer("data"))
                .order_by(Node.id)
                .first(),
            )

        self.assert_sql_count(testing.db, go, 5)

        sess.expunge_all()

        def go():
            eq_(
                Node(data="n1", children=[Node(data="n11"), Node(data="n12")]),
                sess.query(Node)
                .options(undefer("data"), undefer("children.data"))
                .first(),
            )

        self.assert_sql_count(testing.db, go, 3)

    def test_options(self):
        nodes = self.tables.nodes

        class Node(fixtures.ComparableEntity):
            def append(self, node):
                self.children.append(node)

        mapper(
            Node,
            nodes,
            properties={"children": relationship(Node, order_by=nodes.c.id)},
        )
        sess = fixture_session()
        n1 = Node(data="n1")
        n1.append(Node(data="n11"))
        n1.append(Node(data="n12"))
        n1.append(Node(data="n13"))
        n1.children[1].append(Node(data="n121"))
        n1.children[1].append(Node(data="n122"))
        n1.children[1].append(Node(data="n123"))
        sess.add(n1)
        sess.flush()
        sess.expunge_all()

        def go():
            d = (
                sess.query(Node)
                .filter_by(data="n1")
                .order_by(Node.id)
                .options(subqueryload("children").subqueryload("children"))
                .first()
            )
            eq_(
                Node(
                    data="n1",
                    children=[
                        Node(data="n11"),
                        Node(
                            data="n12",
                            children=[
                                Node(data="n121"),
                                Node(data="n122"),
                                Node(data="n123"),
                            ],
                        ),
                        Node(data="n13"),
                    ],
                ),
                d,
            )

        self.assert_sql_count(testing.db, go, 3)

    def test_no_depth(self):
        """no join depth is set, so no eager loading occurs."""

        nodes = self.tables.nodes

        class Node(fixtures.ComparableEntity):
            def append(self, node):
                self.children.append(node)

        mapper(
            Node,
            nodes,
            properties={"children": relationship(Node, lazy="subquery")},
        )
        sess = fixture_session()
        n1 = Node(data="n1")
        n1.append(Node(data="n11"))
        n1.append(Node(data="n12"))
        n1.append(Node(data="n13"))
        n1.children[1].append(Node(data="n121"))
        n1.children[1].append(Node(data="n122"))
        n1.children[1].append(Node(data="n123"))
        n2 = Node(data="n2")
        n2.append(Node(data="n21"))
        sess.add(n1)
        sess.add(n2)
        sess.flush()
        sess.expunge_all()

        def go():
            d = (
                sess.query(Node)
                .filter(Node.data.in_(["n1", "n2"]))
                .order_by(Node.data)
                .all()
            )
            eq_(
                [
                    Node(
                        data="n1",
                        children=[
                            Node(data="n11"),
                            Node(
                                data="n12",
                                children=[
                                    Node(data="n121"),
                                    Node(data="n122"),
                                    Node(data="n123"),
                                ],
                            ),
                            Node(data="n13"),
                        ],
                    ),
                    Node(data="n2", children=[Node(data="n21")]),
                ],
                d,
            )

        self.assert_sql_count(testing.db, go, 4)


class InheritanceToRelatedTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "foo",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("type", String(50)),
            Column("related_id", Integer, ForeignKey("related.id")),
        )
        Table(
            "bar",
            metadata,
            Column("id", Integer, ForeignKey("foo.id"), primary_key=True),
        )
        Table(
            "baz",
            metadata,
            Column("id", Integer, ForeignKey("foo.id"), primary_key=True),
        )
        Table("related", metadata, Column("id", Integer, primary_key=True))

    @classmethod
    def setup_classes(cls):
        class Foo(cls.Comparable):
            pass

        class Bar(Foo):
            pass

        class Baz(Foo):
            pass

        class Related(cls.Comparable):
            pass

    @classmethod
    def fixtures(cls):
        return dict(
            foo=[
                ("id", "type", "related_id"),
                (1, "bar", 1),
                (2, "bar", 2),
                (3, "baz", 1),
                (4, "baz", 2),
            ],
            bar=[("id",), (1,), (2,)],
            baz=[("id",), (3,), (4,)],
            related=[("id",), (1,), (2,)],
        )

    @classmethod
    def setup_mappers(cls):
        mapper(
            cls.classes.Foo,
            cls.tables.foo,
            properties={"related": relationship(cls.classes.Related)},
            polymorphic_on=cls.tables.foo.c.type,
        )
        mapper(
            cls.classes.Bar,
            cls.tables.bar,
            polymorphic_identity="bar",
            inherits=cls.classes.Foo,
        )
        mapper(
            cls.classes.Baz,
            cls.tables.baz,
            polymorphic_identity="baz",
            inherits=cls.classes.Foo,
        )
        mapper(cls.classes.Related, cls.tables.related)

    def test_caches_query_per_base_subq(self):
        Foo, Bar, Baz, Related = (
            self.classes.Foo,
            self.classes.Bar,
            self.classes.Baz,
            self.classes.Related,
        )
        s = Session(testing.db)

        def go():
            eq_(
                s.query(Foo)
                .with_polymorphic([Bar, Baz])
                .order_by(Foo.id)
                .options(subqueryload(Foo.related))
                .all(),
                [
                    Bar(id=1, related=Related(id=1)),
                    Bar(id=2, related=Related(id=2)),
                    Baz(id=3, related=Related(id=1)),
                    Baz(id=4, related=Related(id=2)),
                ],
            )

        self.assert_sql_count(testing.db, go, 2)

    def test_caches_query_per_base_joined(self):
        # technically this should be in test_eager_relations
        Foo, Bar, Baz, Related = (
            self.classes.Foo,
            self.classes.Bar,
            self.classes.Baz,
            self.classes.Related,
        )
        s = Session(testing.db)

        def go():
            eq_(
                s.query(Foo)
                .with_polymorphic([Bar, Baz])
                .order_by(Foo.id)
                .options(joinedload(Foo.related))
                .all(),
                [
                    Bar(id=1, related=Related(id=1)),
                    Bar(id=2, related=Related(id=2)),
                    Baz(id=3, related=Related(id=1)),
                    Baz(id=4, related=Related(id=2)),
                ],
            )

        self.assert_sql_count(testing.db, go, 1)


class CyclicalInheritingEagerTestOne(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "t1",
            metadata,
            Column(
                "c1", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("c2", String(30)),
            Column("type", String(30)),
        )

        Table(
            "t2",
            metadata,
            Column(
                "c1", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("c2", String(30)),
            Column("type", String(30)),
            Column("t1.id", Integer, ForeignKey("t1.c1")),
        )

    def test_basic(self):
        t2, t1 = self.tables.t2, self.tables.t1

        class T(object):
            pass

        class SubT(T):
            pass

        class T2(object):
            pass

        class SubT2(T2):
            pass

        mapper(T, t1, polymorphic_on=t1.c.type, polymorphic_identity="t1")
        mapper(
            SubT,
            None,
            inherits=T,
            polymorphic_identity="subt1",
            properties={
                "t2s": relationship(
                    SubT2,
                    lazy="subquery",
                    backref=sa.orm.backref("subt", lazy="subquery"),
                )
            },
        )
        mapper(T2, t2, polymorphic_on=t2.c.type, polymorphic_identity="t2")
        mapper(SubT2, None, inherits=T2, polymorphic_identity="subt2")

        # testing a particular endless loop condition in eager load setup
        fixture_session().query(SubT).all()


class CyclicalInheritingEagerTestTwo(
    fixtures.DeclarativeMappedTest, testing.AssertsCompiledSQL
):
    __dialect__ = "default"

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class PersistentObject(Base):
            __tablename__ = "persistent"
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )

        class Movie(PersistentObject):
            __tablename__ = "movie"
            id = Column(Integer, ForeignKey("persistent.id"), primary_key=True)
            director_id = Column(Integer, ForeignKey("director.id"))
            title = Column(String(50))

        class Director(PersistentObject):
            __tablename__ = "director"
            id = Column(Integer, ForeignKey("persistent.id"), primary_key=True)
            movies = relationship("Movie", foreign_keys=Movie.director_id)
            name = Column(String(50))

    @classmethod
    def insert_data(cls, connection):
        Director, Movie = cls.classes("Director", "Movie")
        s = Session(connection)
        s.add_all([Director(movies=[Movie(title="m1"), Movie(title="m2")])])
        s.commit()

    def test_from_subclass(self):
        Director = self.classes.Director

        s = fixture_session()

        with self.sql_execution_asserter(testing.db) as asserter:
            s.query(Director).options(subqueryload("*")).all()
        asserter.assert_(
            CompiledSQL(
                "SELECT director.id AS director_id, "
                "persistent.id AS persistent_id, director.name "
                "AS director_name FROM persistent JOIN director "
                "ON persistent.id = director.id"
            ),
            CompiledSQL(
                "SELECT movie.id AS movie_id, "
                "persistent.id AS persistent_id, "
                "movie.director_id AS movie_director_id, "
                "movie.title AS movie_title, "
                "anon_1.director_id AS anon_1_director_id "
                "FROM (SELECT director.id AS director_id "
                "FROM persistent JOIN director "
                "ON persistent.id = director.id) AS anon_1 "
                "JOIN (persistent JOIN movie "
                "ON persistent.id = movie.id) "
                "ON anon_1.director_id = movie.director_id",
            ),
        )

    def test_integrate(self):
        Director = self.classes.Director
        Movie = self.classes.Movie

        session = Session(testing.db)
        rscott = Director(name="Ridley Scott")
        alien = Movie(title="Alien")
        brunner = Movie(title="Blade Runner")
        rscott.movies.append(brunner)
        rscott.movies.append(alien)
        session.add_all([rscott, alien, brunner])
        session.commit()

        close_all_sessions()
        d = session.query(Director).options(subqueryload("*")).first()  # noqa
        assert len(list(session)) == 3


class SubqueryloadDistinctTest(
    fixtures.DeclarativeMappedTest, testing.AssertsCompiledSQL
):
    __dialect__ = "default"

    run_inserts = "once"
    run_deletes = None

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Director(Base):
            __tablename__ = "director"
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column(String(50))

        class DirectorPhoto(Base):
            __tablename__ = "director_photo"
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            path = Column(String(255))
            director_id = Column(Integer, ForeignKey("director.id"))
            director = relationship(
                Director, backref=backref("photos", order_by=id)
            )

        class Movie(Base):
            __tablename__ = "movie"
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            director_id = Column(Integer, ForeignKey("director.id"))
            director = relationship(Director, backref="movies")
            title = Column(String(50))
            credits = relationship("Credit", backref="movie")

        class Credit(Base):
            __tablename__ = "credit"
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            movie_id = Column(Integer, ForeignKey("movie.id"))

    @classmethod
    def insert_data(cls, connection):
        Movie = cls.classes.Movie
        Director = cls.classes.Director
        DirectorPhoto = cls.classes.DirectorPhoto
        Credit = cls.classes.Credit

        d = Director(name="Woody Allen")
        d.photos = [DirectorPhoto(path="/1.jpg"), DirectorPhoto(path="/2.jpg")]
        d.movies = [
            Movie(title="Manhattan", credits=[Credit(), Credit()]),
            Movie(title="Sweet and Lowdown", credits=[Credit()]),
        ]
        sess = Session(connection)
        sess.add_all([d])
        sess.flush()

    def test_distinct_strategy_opt_m2o(self):
        self._run_test_m2o(True, None)
        self._run_test_m2o(False, None)

    def test_distinct_unrelated_opt_m2o(self):
        self._run_test_m2o(None, True)
        self._run_test_m2o(None, False)

    def _run_test_m2o(self, director_strategy_level, photo_strategy_level):

        # test where the innermost is m2o, e.g.
        # Movie->director

        Movie = self.classes.Movie
        Director = self.classes.Director

        Movie.director.property.distinct_target_key = director_strategy_level
        Director.photos.property.distinct_target_key = photo_strategy_level

        # the DISTINCT is controlled by
        # only the Movie->director relationship, *not* the
        # Director.photos
        expect_distinct = director_strategy_level in (True, None)

        s = fixture_session()

        with self.sql_execution_asserter(testing.db) as asserter:
            result = (
                s.query(Movie)
                .options(
                    subqueryload(Movie.director).subqueryload(Director.photos)
                )
                .all()
            )
        asserter.assert_(
            CompiledSQL(
                "SELECT movie.id AS movie_id, movie.director_id "
                "AS movie_director_id, movie.title AS movie_title FROM movie"
            ),
            CompiledSQL(
                "SELECT director.id AS director_id, "
                "director.name AS director_name, "
                "anon_1.movie_director_id AS anon_1_movie_director_id "
                "FROM (SELECT%s movie.director_id AS movie_director_id "
                "FROM movie) AS anon_1 "
                "JOIN director ON director.id = anon_1.movie_director_id"
                % (" DISTINCT" if expect_distinct else ""),
            ),
            CompiledSQL(
                "SELECT director_photo.id AS director_photo_id, "
                "director_photo.path AS director_photo_path, "
                "director_photo.director_id AS director_photo_director_id, "
                "director_1.id AS director_1_id "
                "FROM (SELECT%s movie.director_id AS movie_director_id "
                "FROM movie) AS anon_1 "
                "JOIN director AS director_1 "
                "ON director_1.id = anon_1.movie_director_id "
                "JOIN director_photo "
                "ON director_1.id = director_photo.director_id "
                "ORDER BY director_photo.id"
                % (" DISTINCT" if expect_distinct else ""),
            ),
        )

        eq_(
            [
                (
                    movie.title,
                    movie.director.name,
                    [photo.path for photo in movie.director.photos],
                )
                for movie in result
            ],
            [
                ("Manhattan", "Woody Allen", ["/1.jpg", "/2.jpg"]),
                ("Sweet and Lowdown", "Woody Allen", ["/1.jpg", "/2.jpg"]),
            ],
        )
        # check number of persistent objects in session
        eq_(len(list(s)), 5)

    def test_cant_do_distinct_in_joins(self):
        """the DISTINCT feature here works when the m2o is in the innermost
        mapper, but when we are just joining along relationships outside
        of that, we can still have dupes, and there's no solution to that.

        """
        Movie = self.classes.Movie
        Credit = self.classes.Credit

        s = fixture_session()

        with self.sql_execution_asserter(testing.db) as asserter:
            result = (
                s.query(Credit)
                .options(
                    subqueryload(Credit.movie).subqueryload(Movie.director)
                )
                .all()
            )
        asserter.assert_(
            CompiledSQL(
                "SELECT credit.id AS credit_id, credit.movie_id AS "
                "credit_movie_id FROM credit"
            ),
            CompiledSQL(
                "SELECT movie.id AS movie_id, movie.director_id "
                "AS movie_director_id, movie.title AS movie_title, "
                "anon_1.credit_movie_id AS anon_1_credit_movie_id "
                "FROM (SELECT DISTINCT credit.movie_id AS credit_movie_id "
                "FROM credit) AS anon_1 JOIN movie ON movie.id = "
                "anon_1.credit_movie_id"
            ),
            CompiledSQL(
                "SELECT director.id AS director_id, director.name "
                "AS director_name, movie_1.director_id AS movie_1_director_id "
                "FROM (SELECT DISTINCT credit.movie_id AS credit_movie_id "
                "FROM credit) AS anon_1 JOIN movie AS movie_1 ON "
                "movie_1.id = anon_1.credit_movie_id JOIN director "
                "ON director.id = movie_1.director_id"
            ),
        )

        eq_(
            [credit.movie.director.name for credit in result],
            ["Woody Allen", "Woody Allen", "Woody Allen"],
        )


class JoinedNoLoadConflictTest(fixtures.DeclarativeMappedTest):
    """test for [ticket:2887]"""

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Parent(ComparableEntity, Base):
            __tablename__ = "parent"

            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column(String(20))

            children = relationship(
                "Child", back_populates="parent", lazy="noload"
            )

        class Child(ComparableEntity, Base):
            __tablename__ = "child"

            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column(String(20))
            parent_id = Column(Integer, ForeignKey("parent.id"))

            parent = relationship(
                "Parent", back_populates="children", lazy="joined"
            )

    @classmethod
    def insert_data(cls, connection):
        Parent = cls.classes.Parent
        Child = cls.classes.Child

        s = Session(connection)
        s.add(Parent(name="parent", children=[Child(name="c1")]))
        s.commit()

    def test_subqueryload_on_joined_noload(self):
        Parent = self.classes.Parent
        Child = self.classes.Child

        s = fixture_session()

        # here we have
        # Parent->subqueryload->Child->joinedload->parent->noload->children.
        # the actual subqueryload has to emit *after* we've started populating
        # Parent->subqueryload->child.
        parent = s.query(Parent).options([subqueryload("children")]).first()
        eq_(parent.children, [Child(name="c1")])


class SelfRefInheritanceAliasedTest(
    fixtures.DeclarativeMappedTest, testing.AssertsCompiledSQL
):
    __dialect__ = "default"

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Foo(Base):
            __tablename__ = "foo"
            id = Column(Integer, primary_key=True)
            type = Column(String(50))

            foo_id = Column(Integer, ForeignKey("foo.id"))
            foo = relationship(
                lambda: Foo, foreign_keys=foo_id, remote_side=id
            )

            __mapper_args__ = {
                "polymorphic_on": type,
                "polymorphic_identity": "foo",
            }

        class Bar(Foo):
            __mapper_args__ = {"polymorphic_identity": "bar"}

    @classmethod
    def insert_data(cls, connection):
        Foo, Bar = cls.classes("Foo", "Bar")

        session = Session(connection)
        target = Bar(id=1)
        b1 = Bar(id=2, foo=Foo(id=3, foo=target))
        session.add(b1)
        session.commit()

    def test_twolevel_subquery_w_polymorphic(self):
        Foo, Bar = self.classes("Foo", "Bar")

        r = with_polymorphic(Foo, "*", aliased=True)
        attr1 = Foo.foo.of_type(r)
        attr2 = r.foo

        s = fixture_session()
        q = (
            s.query(Foo)
            .filter(Foo.id == 2)
            .options(subqueryload(attr1).subqueryload(attr2))
        )

        self.assert_sql_execution(
            testing.db,
            q.all,
            CompiledSQL(
                "SELECT foo.id AS foo_id_1, foo.type AS foo_type, "
                "foo.foo_id AS foo_foo_id FROM foo WHERE foo.id = :id_1",
                [{"id_1": 2}],
            ),
            CompiledSQL(
                "SELECT foo_1.id AS foo_1_id, foo_1.type AS foo_1_type, "
                "foo_1.foo_id AS foo_1_foo_id, "
                "anon_1.foo_foo_id AS anon_1_foo_foo_id "
                "FROM (SELECT DISTINCT foo.foo_id AS foo_foo_id "
                "FROM foo WHERE foo.id = :id_1) AS anon_1 "
                "JOIN foo AS foo_1 ON foo_1.id = anon_1.foo_foo_id",
                {"id_1": 2},
            ),
            CompiledSQL(
                "SELECT foo.id AS foo_id_1, foo.type AS foo_type, "
                "foo.foo_id AS foo_foo_id, foo_1.foo_id AS foo_1_foo_id "
                "FROM (SELECT DISTINCT foo.foo_id AS foo_foo_id FROM foo "
                "WHERE foo.id = :id_1) AS anon_1 "
                "JOIN foo AS foo_1 ON foo_1.id = anon_1.foo_foo_id "
                "JOIN foo ON foo.id = foo_1.foo_id",
                {"id_1": 2},
            ),
        )


class TestExistingRowPopulation(fixtures.DeclarativeMappedTest):
    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class A(Base):
            __tablename__ = "a"

            id = Column(Integer, primary_key=True)
            b_id = Column(ForeignKey("b.id"))
            a2_id = Column(ForeignKey("a2.id"))
            a2 = relationship("A2")
            b = relationship("B")

        class A2(Base):
            __tablename__ = "a2"

            id = Column(Integer, primary_key=True)
            b_id = Column(ForeignKey("b.id"))
            b = relationship("B")

        class B(Base):
            __tablename__ = "b"

            id = Column(Integer, primary_key=True)

            c1_m2o_id = Column(ForeignKey("c1_m2o.id"))
            c2_m2o_id = Column(ForeignKey("c2_m2o.id"))

            c1_o2m = relationship("C1o2m")
            c2_o2m = relationship("C2o2m")
            c1_m2o = relationship("C1m2o")
            c2_m2o = relationship("C2m2o")

        class C1o2m(Base):
            __tablename__ = "c1_o2m"

            id = Column(Integer, primary_key=True)
            b_id = Column(ForeignKey("b.id"))

        class C2o2m(Base):
            __tablename__ = "c2_o2m"

            id = Column(Integer, primary_key=True)
            b_id = Column(ForeignKey("b.id"))

        class C1m2o(Base):
            __tablename__ = "c1_m2o"

            id = Column(Integer, primary_key=True)

        class C2m2o(Base):
            __tablename__ = "c2_m2o"

            id = Column(Integer, primary_key=True)

    @classmethod
    def insert_data(cls, connection):
        A, A2, B, C1o2m, C2o2m, C1m2o, C2m2o = cls.classes(
            "A", "A2", "B", "C1o2m", "C2o2m", "C1m2o", "C2m2o"
        )

        s = Session(connection)

        b = B(
            c1_o2m=[C1o2m()], c2_o2m=[C2o2m()], c1_m2o=C1m2o(), c2_m2o=C2m2o()
        )

        s.add(A(b=b, a2=A2(b=b)))
        s.commit()

    def test_o2m(self):
        A, A2, B, C1o2m, C2o2m = self.classes("A", "A2", "B", "C1o2m", "C2o2m")

        s = fixture_session()

        # A -J-> B -L-> C1
        # A -J-> B -S-> C2

        # A -J-> A2 -J-> B -S-> C1
        # A -J-> A2 -J-> B -L-> C2

        q = s.query(A).options(
            joinedload(A.b).subqueryload(B.c2_o2m),
            joinedload(A.a2).joinedload(A2.b).subqueryload(B.c1_o2m),
        )

        a1 = q.all()[0]

        is_true("c1_o2m" in a1.b.__dict__)
        is_true("c2_o2m" in a1.b.__dict__)

    def test_m2o(self):
        A, A2, B, C1m2o, C2m2o = self.classes("A", "A2", "B", "C1m2o", "C2m2o")

        s = fixture_session()

        # A -J-> B -L-> C1
        # A -J-> B -S-> C2

        # A -J-> A2 -J-> B -S-> C1
        # A -J-> A2 -J-> B -L-> C2

        q = s.query(A).options(
            joinedload(A.b).subqueryload(B.c2_m2o),
            joinedload(A.a2).joinedload(A2.b).subqueryload(B.c1_m2o),
        )

        a1 = q.all()[0]
        is_true("c1_m2o" in a1.b.__dict__)
        is_true("c2_m2o" in a1.b.__dict__)


class FromSubqTest(fixtures.DeclarativeMappedTest):
    """because subqueryloader relies upon the .subquery() method, this means
    if the original Query has a from_self() present, it needs to create
    .subquery() in terms of the Query class as a from_self() selectable
    doesn't work correctly with the future select.   So it has
    to create a Query object now that it gets only a select.
    neutron is currently dependent on this use case which means others
    are too.

    Additionally tests functionality related to #5836, where we are using the
    non-cached context.query, rather than
    context.compile_state.select_statement to generate the subquery.  this is
    so we get the current parameters from the new statement being run, but it
    also means we have to get a new CompileState from that query in order to
    deal with the correct entities.

    """

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class A(Base, ComparableEntity):
            __tablename__ = "a"

            id = Column(Integer, primary_key=True)
            cs = relationship("C", order_by="C.id")

        class B(Base, ComparableEntity):
            __tablename__ = "b"
            id = Column(Integer, primary_key=True)
            a_id = Column(ForeignKey("a.id"))
            a = relationship("A")
            ds = relationship("D", order_by="D.id")

        class C(Base, ComparableEntity):
            __tablename__ = "c"
            id = Column(Integer, primary_key=True)
            a_id = Column(ForeignKey("a.id"))

        class D(Base, ComparableEntity):
            __tablename__ = "d"
            id = Column(Integer, primary_key=True)
            b_id = Column(ForeignKey("b.id"))

    @classmethod
    def insert_data(cls, connection):
        A, B, C, D = cls.classes("A", "B", "C", "D")

        s = Session(connection)

        as_ = [
            A(
                id=i,
                cs=[C(), C()],
            )
            for i in range(1, 5)
        ]

        s.add_all(
            [
                B(a=as_[0], ds=[D()]),
                B(a=as_[1], ds=[D()]),
                B(a=as_[2]),
                B(a=as_[3]),
            ]
        )

        s.commit()

    def test_subq_w_from_self_one(self):
        A, B, C = self.classes("A", "B", "C")

        s = fixture_session()

        cache = {}

        for i in range(3):

            subq = (
                s.query(B)
                .join(B.a)
                .filter(B.id < 4)
                .filter(A.id > 1)
                .subquery()
            )

            bb = aliased(B, subq)

            subq2 = s.query(bb).subquery()

            bb2 = aliased(bb, subq2)

            q = (
                s.query(bb2)
                .execution_options(compiled_cache=cache)
                .options(subqueryload(bb2.a).subqueryload(A.cs))
            )

            def go():
                results = q.all()
                eq_(
                    results,
                    [
                        B(
                            a=A(cs=[C(a_id=2, id=3), C(a_id=2, id=4)], id=2),
                            a_id=2,
                            id=2,
                        ),
                        B(
                            a=A(cs=[C(a_id=3, id=5), C(a_id=3, id=6)], id=3),
                            a_id=3,
                            id=3,
                        ),
                    ],
                )

            self.assert_sql_execution(
                testing.db,
                go,
                CompiledSQL(
                    "SELECT anon_1.id AS anon_1_id, "
                    "anon_1.a_id AS anon_1_a_id FROM "
                    "(SELECT anon_2.id AS id, anon_2.a_id "
                    "AS a_id FROM (SELECT b.id AS id, b.a_id "
                    "AS a_id FROM b JOIN a ON a.id = b.a_id "
                    "WHERE b.id < :id_1 AND a.id > :id_2) AS anon_2) AS anon_1"
                ),
                CompiledSQL(
                    "SELECT a.id AS a_id, anon_1.anon_2_a_id AS "
                    "anon_1_anon_2_a_id FROM (SELECT DISTINCT "
                    "anon_2.a_id AS anon_2_a_id FROM "
                    "(SELECT anon_3.id AS id, anon_3.a_id "
                    "AS a_id FROM (SELECT b.id AS id, b.a_id "
                    "AS a_id FROM b JOIN a ON a.id = b.a_id "
                    "WHERE b.id < :id_1 AND a.id > :id_2) AS anon_3) "
                    "AS anon_2) AS anon_1 JOIN a "
                    "ON a.id = anon_1.anon_2_a_id"
                ),
                CompiledSQL(
                    "SELECT c.id AS c_id, c.a_id AS c_a_id, a_1.id "
                    "AS a_1_id FROM (SELECT DISTINCT anon_2.a_id AS "
                    "anon_2_a_id FROM "
                    "(SELECT anon_3.id AS id, anon_3.a_id "
                    "AS a_id FROM (SELECT b.id AS id, b.a_id "
                    "AS a_id FROM b JOIN a ON a.id = b.a_id "
                    "WHERE b.id < :id_1 AND a.id > :id_2) AS anon_3) "
                    "AS anon_2) AS anon_1 JOIN a AS a_1 ON a_1.id = "
                    "anon_1.anon_2_a_id JOIN c ON a_1.id = c.a_id "
                    "ORDER BY c.id"
                ),
            )

            s.close()

    def test_subq_w_from_self_two(self):

        A, B, C = self.classes("A", "B", "C")

        s = fixture_session()
        cache = {}

        for i in range(3):

            def go():

                subq = s.query(B).join(B.a).subquery()

                bq = aliased(B, subq)

                q = (
                    s.query(bq)
                    .execution_options(compiled_cache=cache)
                    .options(subqueryload(bq.ds))
                )

                q.all()

            self.assert_sql_execution(
                testing.db,
                go,
                CompiledSQL(
                    "SELECT anon_1.id AS anon_1_id, anon_1.a_id AS "
                    "anon_1_a_id FROM (SELECT b.id AS id, b.a_id "
                    "AS a_id FROM b JOIN a ON a.id = b.a_id) AS anon_1"
                ),
                CompiledSQL(
                    "SELECT d.id AS d_id, d.b_id AS d_b_id, "
                    "anon_1.anon_2_id AS anon_1_anon_2_id "
                    "FROM (SELECT anon_2.id AS anon_2_id FROM "
                    "(SELECT b.id AS id, b.a_id AS a_id FROM b "
                    "JOIN a ON a.id = b.a_id) AS anon_2) AS anon_1 "
                    "JOIN d ON anon_1.anon_2_id = d.b_id ORDER BY d.id"
                ),
            )
            s.close()
