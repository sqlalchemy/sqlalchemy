from sqlalchemy import cast
from sqlalchemy import desc
from sqlalchemy import exc
from sqlalchemy import func
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import testing
from sqlalchemy.orm import attributes
from sqlalchemy.orm import backref
from sqlalchemy.orm import configure_mappers
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy.orm import mapper
from sqlalchemy.orm import Query
from sqlalchemy.orm import relationship
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import is_
from sqlalchemy.testing.assertsql import CompiledSQL
from sqlalchemy.testing.fixtures import fixture_session
from test.orm import _fixtures


class _DynamicFixture(object):
    def _user_address_fixture(self, addresses_args={}):
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
                    Address, lazy="dynamic", **addresses_args
                )
            },
        )
        mapper(Address, addresses)
        return User, Address

    def _order_item_fixture(self, items_args={}):
        items, Order, orders, order_items, Item = (
            self.tables.items,
            self.classes.Order,
            self.tables.orders,
            self.tables.order_items,
            self.classes.Item,
        )

        mapper(
            Order,
            orders,
            properties={
                "items": relationship(
                    Item, secondary=order_items, lazy="dynamic", **items_args
                )
            },
        )
        mapper(Item, items)
        return Order, Item

    def _user_order_item_fixture(self):
        (
            users,
            Keyword,
            items,
            order_items,
            item_keywords,
            Item,
            User,
            keywords,
            Order,
            orders,
        ) = (
            self.tables.users,
            self.classes.Keyword,
            self.tables.items,
            self.tables.order_items,
            self.tables.item_keywords,
            self.classes.Item,
            self.classes.User,
            self.tables.keywords,
            self.classes.Order,
            self.tables.orders,
        )

        mapper(
            User,
            users,
            properties={
                "orders": relationship(
                    Order, order_by=orders.c.id, lazy="dynamic"
                )
            },
        )
        mapper(
            Order,
            orders,
            properties={
                "items": relationship(
                    Item, secondary=order_items, order_by=items.c.id
                ),
            },
        )
        mapper(
            Item,
            items,
            properties={
                "keywords": relationship(
                    Keyword, secondary=item_keywords
                )  # m2m
            },
        )
        mapper(Keyword, keywords)

        return User, Order, Item, Keyword


class DynamicTest(_DynamicFixture, _fixtures.FixtureTest, AssertsCompiledSQL):
    def test_basic(self):
        User, Address = self._user_address_fixture()
        sess = fixture_session()
        q = sess.query(User)

        eq_(
            [
                User(
                    id=7,
                    addresses=[Address(id=1, email_address="jack@bean.com")],
                )
            ],
            q.filter(User.id == 7).all(),
        )
        eq_(self.static.user_address_result, q.all())

        eq_(
            [
                User(
                    id=7,
                    addresses=[Address(id=1, email_address="jack@bean.com")],
                )
            ],
            q.filter_by(id=7).all(),
        )

    def test_slice_access(self):
        User, Address = self._user_address_fixture()
        sess = fixture_session()
        u1 = sess.get(User, 8)

        eq_(u1.addresses.limit(1).one(), Address(id=2))

        eq_(u1.addresses[0], Address(id=2))
        eq_(u1.addresses[0:2], [Address(id=2), Address(id=3)])

    def test_negative_slice_access_raises(self):
        User, Address = self._user_address_fixture()
        sess = fixture_session(future=True)
        u1 = sess.get(User, 8)

        with expect_raises_message(
            IndexError,
            "negative indexes are not accepted by SQL index / slice operators",
        ):
            u1.addresses[-1]

        with expect_raises_message(
            IndexError,
            "negative indexes are not accepted by SQL index / slice operators",
        ):
            u1.addresses[-5:-2]

        with expect_raises_message(
            IndexError,
            "negative indexes are not accepted by SQL index / slice operators",
        ):
            u1.addresses[-2]

        with expect_raises_message(
            IndexError,
            "negative indexes are not accepted by SQL index / slice operators",
        ):
            u1.addresses[:-2]

    def test_statement(self):
        """test that the .statement accessor returns the actual statement that
        would render, without any _clones called."""

        User, Address = self._user_address_fixture()
        sess = fixture_session()
        q = sess.query(User)

        u = q.filter(User.id == 7).first()
        self.assert_compile(
            u.addresses.statement,
            "SELECT addresses.id, addresses.user_id, addresses.email_address "
            "FROM "
            "addresses WHERE :param_1 = addresses.user_id",
            use_default_dialect=True,
        )

    def test_query_class_custom_method(self):
        class MyClass(Query):
            def my_filter(self, arg):
                return self.filter(Address.email_address == arg)

        User, Address = self._user_address_fixture(
            addresses_args=dict(query_class=MyClass)
        )

        sess = fixture_session()
        q = sess.query(User)

        u = q.filter(User.id == 7).first()

        assert isinstance(u.addresses, MyClass)

        self.assert_compile(
            u.addresses.my_filter("x").statement,
            "SELECT addresses.id, addresses.user_id, addresses.email_address "
            "FROM "
            "addresses WHERE :param_1 = addresses.user_id AND "
            "addresses.email_address = :email_address_1",
            use_default_dialect=True,
        )

    def test_detached_raise(self):
        User, Address = self._user_address_fixture()
        sess = fixture_session()
        u = sess.query(User).get(8)
        sess.expunge(u)
        assert_raises(
            orm_exc.DetachedInstanceError,
            u.addresses.filter_by,
            email_address="e",
        )

    def test_no_uselist_false(self):
        User, Address = self._user_address_fixture(
            addresses_args={"uselist": False}
        )
        assert_raises_message(
            exc.InvalidRequestError,
            "On relationship User.addresses, 'dynamic' loaders cannot be "
            "used with many-to-one/one-to-one relationships and/or "
            "uselist=False.",
            configure_mappers,
        )

    def test_no_m2o(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )
        mapper(
            Address,
            addresses,
            properties={"user": relationship(User, lazy="dynamic")},
        )
        mapper(User, users)
        assert_raises_message(
            exc.InvalidRequestError,
            "On relationship Address.user, 'dynamic' loaders cannot be "
            "used with many-to-one/one-to-one relationships and/or "
            "uselist=False.",
            configure_mappers,
        )

    def test_no_m2o_w_uselist(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )
        mapper(
            Address,
            addresses,
            properties={
                "user": relationship(User, uselist=True, lazy="dynamic")
            },
        )
        mapper(User, users)
        assert_raises_message(
            exc.SAWarning,
            "On relationship Address.user, 'dynamic' loaders cannot be "
            "used with many-to-one/one-to-one relationships and/or "
            "uselist=False.",
            configure_mappers,
        )

    def test_order_by(self):
        User, Address = self._user_address_fixture()
        sess = fixture_session()
        u = sess.query(User).get(8)
        eq_(
            list(u.addresses.order_by(desc(Address.email_address))),
            [
                Address(email_address="ed@wood.com"),
                Address(email_address="ed@lala.com"),
                Address(email_address="ed@bettyboop.com"),
            ],
        )

    def test_configured_order_by(self):
        addresses = self.tables.addresses
        User, Address = self._user_address_fixture(
            addresses_args={"order_by": addresses.c.email_address.desc()}
        )

        sess = fixture_session()
        u = sess.query(User).get(8)
        eq_(
            list(u.addresses),
            [
                Address(email_address="ed@wood.com"),
                Address(email_address="ed@lala.com"),
                Address(email_address="ed@bettyboop.com"),
            ],
        )

        # test cancellation of None, replacement with something else
        eq_(
            list(u.addresses.order_by(None).order_by(Address.email_address)),
            [
                Address(email_address="ed@bettyboop.com"),
                Address(email_address="ed@lala.com"),
                Address(email_address="ed@wood.com"),
            ],
        )

        # test cancellation of None, replacement with nothing
        eq_(
            set(u.addresses.order_by(None)),
            set(
                [
                    Address(email_address="ed@bettyboop.com"),
                    Address(email_address="ed@lala.com"),
                    Address(email_address="ed@wood.com"),
                ]
            ),
        )

    def test_count(self):
        User, Address = self._user_address_fixture()
        sess = fixture_session()
        u = sess.query(User).first()
        eq_(u.addresses.count(), 1)

    def test_dynamic_on_backref(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(
            Address,
            addresses,
            properties={
                "user": relationship(
                    User, backref=backref("addresses", lazy="dynamic")
                )
            },
        )
        mapper(User, users)

        sess = fixture_session()
        ad = sess.query(Address).get(1)

        def go():
            ad.user = None

        self.assert_sql_count(testing.db, go, 0)
        sess.flush()
        u = sess.query(User).get(7)
        assert ad not in u.addresses

    def test_no_count(self):
        User, Address = self._user_address_fixture()
        sess = fixture_session()
        q = sess.query(User)

        # dynamic collection cannot implement __len__() (at least one that
        # returns a live database result), else additional count() queries are
        # issued when evaluating in a list context
        def go():
            eq_(
                q.filter(User.id == 7).all(),
                [
                    User(
                        id=7,
                        addresses=[
                            Address(id=1, email_address="jack@bean.com")
                        ],
                    )
                ],
            )

        self.assert_sql_count(testing.db, go, 2)

    def test_no_populate(self):
        User, Address = self._user_address_fixture()
        u1 = User()
        assert_raises_message(
            NotImplementedError,
            "Dynamic attributes don't support collection population.",
            attributes.set_committed_value,
            u1,
            "addresses",
            [],
        )

    def test_m2m(self):
        Order, Item = self._order_item_fixture(
            items_args={"backref": backref("orders", lazy="dynamic")}
        )

        sess = fixture_session()
        o1 = Order(id=15, description="order 10")
        i1 = Item(id=10, description="item 8")
        o1.items.append(i1)
        sess.add(o1)
        sess.flush()

        assert o1 in i1.orders.all()
        assert i1 in o1.items.all()

    @testing.exclude(
        "mysql",
        "between",
        ((5, 1, 49), (5, 1, 52)),
        "https://bugs.launchpad.net/ubuntu/+source/mysql-5.1/+bug/706988",
    )
    def test_association_nonaliased(self):
        items, Order, orders, order_items, Item = (
            self.tables.items,
            self.classes.Order,
            self.tables.orders,
            self.tables.order_items,
            self.classes.Item,
        )

        mapper(
            Order,
            orders,
            properties={
                "items": relationship(
                    Item,
                    secondary=order_items,
                    lazy="dynamic",
                    order_by=order_items.c.item_id,
                )
            },
        )
        mapper(Item, items)

        sess = fixture_session()
        o = sess.query(Order).first()

        self.assert_compile(
            o.items,
            "SELECT items.id AS items_id, items.description AS "
            "items_description FROM items,"
            " order_items WHERE :param_1 = order_items.order_id AND "
            "items.id = order_items.item_id"
            " ORDER BY order_items.item_id",
            use_default_dialect=True,
        )

        # filter criterion against the secondary table
        # works
        eq_(o.items.filter(order_items.c.item_id == 2).all(), [Item(id=2)])

    def test_secondary_as_join(self):
        # test [ticket:4349]
        User, users = self.classes.User, self.tables.users
        items, orders, order_items, Item = (
            self.tables.items,
            self.tables.orders,
            self.tables.order_items,
            self.classes.Item,
        )

        mapper(
            User,
            users,
            properties={
                "items": relationship(
                    Item, secondary=order_items.join(orders), lazy="dynamic"
                )
            },
        )
        mapper(Item, items)

        sess = fixture_session()
        u1 = sess.query(User).first()

        self.assert_compile(
            u1.items,
            "SELECT items.id AS items_id, "
            "items.description AS items_description "
            "FROM items, order_items JOIN orders "
            "ON orders.id = order_items.order_id "
            "WHERE :param_1 = orders.user_id "
            "AND items.id = order_items.item_id",
            use_default_dialect=True,
        )

    def test_secondary_doesnt_interfere_w_join_to_fromlist(self):
        # tests that the "secondary" being added to the FROM
        # as part of [ticket:4349] does not prevent a subsequent join to
        # an entity that does not provide any "left side".  Query right now
        # does not know how to join() like this unambiguously if _from_obj is
        # more than one element long.
        Order, orders = self.classes.Order, self.tables.orders

        items, order_items, Item = (
            self.tables.items,
            self.tables.order_items,
            self.classes.Item,
        )
        item_keywords = self.tables.item_keywords

        class ItemKeyword(object):
            pass

        mapper(
            Order,
            orders,
            properties={
                "items": relationship(
                    Item, secondary=order_items, lazy="dynamic"
                )
            },
        )
        mapper(
            ItemKeyword,
            item_keywords,
            primary_key=[item_keywords.c.item_id, item_keywords.c.keyword_id],
        )
        mapper(
            Item,
            items,
            properties={"item_keywords": relationship(ItemKeyword)},
        )

        sess = fixture_session()
        order = sess.query(Order).first()

        self.assert_compile(
            order.items.join(ItemKeyword),
            "SELECT items.id AS items_id, "
            "items.description AS items_description "
            "FROM order_items, items "
            "JOIN item_keywords ON items.id = item_keywords.item_id "
            "WHERE :param_1 = order_items.order_id "
            "AND items.id = order_items.item_id",
            use_default_dialect=True,
        )

    @testing.combinations(
        # lambda
    )
    def test_join_syntaxes(self, expr):
        User, Order, Item, Keyword = self._user_order_item_fixture()

    def test_transient_count(self):
        User, Address = self._user_address_fixture()
        u1 = User()
        u1.addresses.append(Address())
        eq_(u1.addresses.count(), 1)

    def test_transient_access(self):
        User, Address = self._user_address_fixture()
        u1 = User()
        u1.addresses.append(Address())
        eq_(u1.addresses[0], Address())


class UOWTest(
    _DynamicFixture, _fixtures.FixtureTest, testing.AssertsExecutionResults
):

    run_inserts = None

    def test_persistence(self):
        addresses = self.tables.addresses
        User, Address = self._user_address_fixture()

        sess = fixture_session()
        u1 = User(name="jack")
        a1 = Address(email_address="foo")
        sess.add_all([u1, a1])
        sess.flush()

        eq_(
            sess.connection().scalar(
                select(func.count(cast(1, Integer))).where(
                    addresses.c.user_id != None
                )
            ),  # noqa
            0,
        )
        u1 = sess.query(User).get(u1.id)
        u1.addresses.append(a1)
        sess.flush()

        eq_(
            sess.connection()
            .execute(
                select(addresses).where(addresses.c.user_id != None)  # noqa
            )
            .fetchall(),
            [(a1.id, u1.id, "foo")],
        )

        u1.addresses.remove(a1)
        sess.flush()
        eq_(
            sess.connection().scalar(
                select(func.count(cast(1, Integer))).where(
                    addresses.c.user_id != None
                )
            ),  # noqa
            0,
        )

        u1.addresses.append(a1)
        sess.flush()
        eq_(
            sess.connection()
            .execute(
                select(addresses).where(addresses.c.user_id != None)  # noqa
            )
            .fetchall(),
            [(a1.id, u1.id, "foo")],
        )

        a2 = Address(email_address="bar")
        u1.addresses.remove(a1)
        u1.addresses.append(a2)
        sess.flush()
        eq_(
            sess.connection()
            .execute(
                select(addresses).where(addresses.c.user_id != None)  # noqa
            )
            .fetchall(),
            [(a2.id, u1.id, "bar")],
        )

    def test_merge(self):
        addresses = self.tables.addresses
        User, Address = self._user_address_fixture(
            addresses_args={"order_by": addresses.c.email_address}
        )
        sess = fixture_session(autoflush=False)
        u1 = User(name="jack")
        a1 = Address(email_address="a1")
        a2 = Address(email_address="a2")
        a3 = Address(email_address="a3")

        u1.addresses.append(a2)
        u1.addresses.append(a3)

        sess.add_all([u1, a1])
        sess.flush()

        u1 = User(id=u1.id, name="jack")
        u1.addresses.append(a1)
        u1.addresses.append(a3)
        u1 = sess.merge(u1)
        eq_(attributes.get_history(u1, "addresses"), ([a1], [a3], [a2]))

        sess.flush()

        eq_(list(u1.addresses), [a1, a3])

    def test_hasattr(self):
        User, Address = self._user_address_fixture()

        u1 = User(name="jack")

        assert "addresses" not in u1.__dict__
        u1.addresses = [Address(email_address="test")]
        assert "addresses" in u1.__dict__

    def test_collection_set(self):
        addresses = self.tables.addresses
        User, Address = self._user_address_fixture(
            addresses_args={"order_by": addresses.c.email_address}
        )
        sess = fixture_session(autoflush=True, autocommit=False)
        u1 = User(name="jack")
        a1 = Address(email_address="a1")
        a2 = Address(email_address="a2")
        a3 = Address(email_address="a3")
        a4 = Address(email_address="a4")

        sess.add(u1)
        u1.addresses = [a1, a3]
        eq_(list(u1.addresses), [a1, a3])
        u1.addresses = [a1, a2, a4]
        eq_(list(u1.addresses), [a1, a2, a4])
        u1.addresses = [a2, a3]
        eq_(list(u1.addresses), [a2, a3])
        u1.addresses = []
        eq_(list(u1.addresses), [])

    def test_noload_append(self):
        # test that a load of User.addresses is not emitted
        # when flushing an append
        User, Address = self._user_address_fixture()

        sess = fixture_session()
        u1 = User(name="jack", addresses=[Address(email_address="a1")])
        sess.add(u1)
        sess.commit()

        u1_id = u1.id
        sess.expire_all()

        u1.addresses.append(Address(email_address="a2"))

        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL(
                "SELECT users.id AS users_id, users.name AS users_name "
                "FROM users WHERE users.id = :pk_1",
                lambda ctx: [{"pk_1": u1_id}],
            ),
            CompiledSQL(
                "INSERT INTO addresses (user_id, email_address) "
                "VALUES (:user_id, :email_address)",
                lambda ctx: [{"email_address": "a2", "user_id": u1_id}],
            ),
        )

    def test_noload_remove(self):
        # test that a load of User.addresses is not emitted
        # when flushing a remove
        User, Address = self._user_address_fixture()

        sess = fixture_session()
        u1 = User(name="jack", addresses=[Address(email_address="a1")])
        a2 = Address(email_address="a2")
        u1.addresses.append(a2)
        sess.add(u1)
        sess.commit()

        u1_id = u1.id
        a2_id = a2.id
        sess.expire_all()

        u1.addresses.remove(a2)

        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL(
                "SELECT addresses.id AS addresses_id, addresses.email_address "
                "AS addresses_email_address FROM addresses "
                "WHERE addresses.id = :pk_1",
                lambda ctx: [{"pk_1": a2_id}],
            ),
            CompiledSQL(
                "UPDATE addresses SET user_id=:user_id WHERE addresses.id = "
                ":addresses_id",
                lambda ctx: [{"addresses_id": a2_id, "user_id": None}],
            ),
            CompiledSQL(
                "SELECT users.id AS users_id, users.name AS users_name "
                "FROM users WHERE users.id = :pk_1",
                lambda ctx: [{"pk_1": u1_id}],
            ),
        )

    def test_rollback(self):
        User, Address = self._user_address_fixture()
        sess = fixture_session(
            expire_on_commit=False, autocommit=False, autoflush=True
        )
        u1 = User(name="jack")
        u1.addresses.append(Address(email_address="lala@hoho.com"))
        sess.add(u1)
        sess.flush()
        sess.commit()
        u1.addresses.append(Address(email_address="foo@bar.com"))
        eq_(
            u1.addresses.order_by(Address.id).all(),
            [
                Address(email_address="lala@hoho.com"),
                Address(email_address="foo@bar.com"),
            ],
        )
        sess.rollback()
        eq_(u1.addresses.all(), [Address(email_address="lala@hoho.com")])

    def _test_delete_cascade(self, expected):
        addresses = self.tables.addresses
        User, Address = self._user_address_fixture(
            addresses_args={
                "order_by": addresses.c.id,
                "backref": "user",
                "cascade": "save-update" if expected else "all, delete",
            }
        )

        sess = fixture_session(autoflush=True, autocommit=False)
        u = User(name="ed")
        u.addresses.extend(
            [Address(email_address=letter) for letter in "abcdef"]
        )
        sess.add(u)
        sess.commit()
        eq_(
            testing.db.scalar(
                select(func.count("*")).where(addresses.c.user_id == None)
            ),  # noqa
            0,
        )
        eq_(
            testing.db.scalar(
                select(func.count("*")).where(addresses.c.user_id != None)
            ),  # noqa
            6,
        )

        sess.delete(u)

        sess.commit()

        if expected:
            eq_(
                testing.db.scalar(
                    select(func.count("*")).where(
                        addresses.c.user_id == None
                    )  # noqa
                ),
                6,
            )
            eq_(
                testing.db.scalar(
                    select(func.count("*")).where(
                        addresses.c.user_id != None
                    )  # noqa
                ),
                0,
            )
        else:
            eq_(
                testing.db.scalar(
                    select(func.count("*")).select_from(addresses)
                ),
                0,
            )

    def test_delete_nocascade(self):
        self._test_delete_cascade(True)

    def test_delete_cascade(self):
        self._test_delete_cascade(False)

    def test_self_referential(self):
        Node, nodes = self.classes.Node, self.tables.nodes

        mapper(
            Node,
            nodes,
            properties={
                "children": relationship(
                    Node, lazy="dynamic", order_by=nodes.c.id
                )
            },
        )

        sess = fixture_session()
        n2, n3 = Node(), Node()
        n1 = Node(children=[n2, n3])
        sess.add(n1)
        sess.commit()

        eq_(n1.children.all(), [n2, n3])

    def test_remove_orphans(self):
        addresses = self.tables.addresses
        User, Address = self._user_address_fixture(
            addresses_args={
                "order_by": addresses.c.id,
                "backref": "user",
                "cascade": "all, delete-orphan",
            }
        )

        sess = fixture_session(autoflush=True, autocommit=False)
        u = User(name="ed")
        u.addresses.extend(
            [Address(email_address=letter) for letter in "abcdef"]
        )
        sess.add(u)

        for a in u.addresses.filter(
            Address.email_address.in_(["c", "e", "f"])
        ):
            u.addresses.remove(a)

        eq_(
            set(ad for ad, in sess.query(Address.email_address)),
            set(["a", "b", "d"]),
        )

    def _backref_test(self, autoflush, saveuser):
        User, Address = self._user_address_fixture(
            addresses_args={"backref": "user"}
        )
        sess = fixture_session(autoflush=autoflush, autocommit=False)

        u = User(name="buffy")

        a = Address(email_address="foo@bar.com")
        a.user = u

        if saveuser:
            sess.add(u)
        else:
            sess.add(a)

        if not autoflush:
            sess.flush()

        assert u in sess
        assert a in sess

        eq_(list(u.addresses), [a])

        a.user = None
        if not autoflush:
            eq_(list(u.addresses), [a])

        if not autoflush:
            sess.flush()
        eq_(list(u.addresses), [])

    def test_backref_autoflush_saveuser(self):
        self._backref_test(True, True)

    def test_backref_autoflush_savead(self):
        self._backref_test(True, False)

    def test_backref_saveuser(self):
        self._backref_test(False, True)

    def test_backref_savead(self):
        self._backref_test(False, False)

    def test_backref_events(self):
        User, Address = self._user_address_fixture(
            addresses_args={"backref": "user"}
        )

        u1 = User()
        a1 = Address()
        u1.addresses.append(a1)
        is_(a1.user, u1)

    def test_no_deref(self):
        User, Address = self._user_address_fixture(
            addresses_args={"backref": "user"}
        )

        with fixture_session() as session:
            user = User()
            user.name = "joe"
            user.fullname = "Joe User"
            user.password = "Joe's secret"
            address = Address()
            address.email_address = "joe@joesdomain.example"
            address.user = user
            session.add(user)
            session.commit()

        def query1():
            session = fixture_session()
            user = session.query(User).first()
            return user.addresses.all()

        def query2():
            session = fixture_session()
            return session.query(User).first().addresses.all()

        def query3():
            session = fixture_session()
            return session.query(User).first().addresses.all()

        eq_(query1(), [Address(email_address="joe@joesdomain.example")])
        eq_(query2(), [Address(email_address="joe@joesdomain.example")])
        eq_(query3(), [Address(email_address="joe@joesdomain.example")])


class HistoryTest(_DynamicFixture, _fixtures.FixtureTest):
    run_inserts = None

    def _transient_fixture(self, addresses_args={}):
        User, Address = self._user_address_fixture(
            addresses_args=addresses_args
        )

        u1 = User()
        a1 = Address()
        return u1, a1

    def _persistent_fixture(self, autoflush=True, addresses_args={}):
        User, Address = self._user_address_fixture(
            addresses_args=addresses_args
        )

        u1 = User(name="u1")
        a1 = Address(email_address="a1")
        s = fixture_session(autoflush=autoflush)
        s.add(u1)
        s.flush()
        return u1, a1, s

    def _persistent_m2m_fixture(self, autoflush=True, items_args={}):
        Order, Item = self._order_item_fixture(items_args=items_args)

        o1 = Order()
        i1 = Item(description="i1")
        s = fixture_session(autoflush=autoflush)
        s.add(o1)
        s.flush()
        return o1, i1, s

    def _assert_history(self, obj, compare, compare_passive=None):
        if isinstance(obj, self.classes.User):
            attrname = "addresses"
        elif isinstance(obj, self.classes.Order):
            attrname = "items"

        sess = inspect(obj).session

        if sess:
            sess.autoflush = False
        try:
            eq_(attributes.get_history(obj, attrname), compare)

            if compare_passive is None:
                compare_passive = compare

            eq_(
                attributes.get_history(
                    obj, attrname, attributes.LOAD_AGAINST_COMMITTED
                ),
                compare_passive,
            )
        finally:
            if sess:
                sess.autoflush = True

    def test_append_transient(self):
        u1, a1 = self._transient_fixture()
        u1.addresses.append(a1)

        self._assert_history(u1, ([a1], [], []))

    def test_append_persistent(self):
        u1, a1, s = self._persistent_fixture()
        u1.addresses.append(a1)

        self._assert_history(u1, ([a1], [], []))

    def test_remove_transient(self):
        u1, a1 = self._transient_fixture()
        u1.addresses.append(a1)
        u1.addresses.remove(a1)

        self._assert_history(u1, ([], [], []))

    def test_backref_pop_transient(self):
        u1, a1 = self._transient_fixture(addresses_args={"backref": "user"})
        u1.addresses.append(a1)

        self._assert_history(u1, ([a1], [], []))

        a1.user = None

        # removed from added
        self._assert_history(u1, ([], [], []))

    def test_remove_persistent(self):
        u1, a1, s = self._persistent_fixture()
        u1.addresses.append(a1)
        s.flush()
        s.expire_all()

        u1.addresses.remove(a1)

        self._assert_history(u1, ([], [], [a1]))

    def test_backref_pop_persistent_autoflush_o2m_active_hist(self):
        u1, a1, s = self._persistent_fixture(
            addresses_args={"backref": backref("user", active_history=True)}
        )
        u1.addresses.append(a1)
        s.flush()
        s.expire_all()

        a1.user = None

        self._assert_history(u1, ([], [], [a1]))

    def test_backref_pop_persistent_autoflush_m2m(self):
        o1, i1, s = self._persistent_m2m_fixture(
            items_args={"backref": "orders"}
        )
        o1.items.append(i1)
        s.flush()
        s.expire_all()

        i1.orders.remove(o1)

        self._assert_history(o1, ([], [], [i1]))

    def test_backref_pop_persistent_noflush_m2m(self):
        o1, i1, s = self._persistent_m2m_fixture(
            items_args={"backref": "orders"}, autoflush=False
        )
        o1.items.append(i1)
        s.flush()
        s.expire_all()

        i1.orders.remove(o1)

        self._assert_history(o1, ([], [], [i1]))

    def test_unchanged_persistent(self):
        Address = self.classes.Address

        u1, a1, s = self._persistent_fixture()
        a2, a3 = Address(email_address="a2"), Address(email_address="a3")

        u1.addresses.append(a1)
        u1.addresses.append(a2)
        s.flush()

        u1.addresses.append(a3)
        u1.addresses.remove(a2)

        self._assert_history(
            u1, ([a3], [a1], [a2]), compare_passive=([a3], [], [a2])
        )

    def test_replace_transient(self):
        Address = self.classes.Address

        u1, a1 = self._transient_fixture()
        a2, a3, a4, a5 = (
            Address(email_address="a2"),
            Address(email_address="a3"),
            Address(email_address="a4"),
            Address(email_address="a5"),
        )

        u1.addresses = [a1, a2]
        u1.addresses = [a2, a3, a4, a5]

        self._assert_history(u1, ([a2, a3, a4, a5], [], []))

    def test_replace_persistent_noflush(self):
        Address = self.classes.Address

        u1, a1, s = self._persistent_fixture(autoflush=False)
        a2, a3, a4, a5 = (
            Address(email_address="a2"),
            Address(email_address="a3"),
            Address(email_address="a4"),
            Address(email_address="a5"),
        )

        u1.addresses = [a1, a2]
        u1.addresses = [a2, a3, a4, a5]

        self._assert_history(u1, ([a2, a3, a4, a5], [], []))

    def test_replace_persistent_autoflush(self):
        Address = self.classes.Address

        u1, a1, s = self._persistent_fixture(autoflush=True)
        a2, a3, a4, a5 = (
            Address(email_address="a2"),
            Address(email_address="a3"),
            Address(email_address="a4"),
            Address(email_address="a5"),
        )

        u1.addresses = [a1, a2]
        u1.addresses = [a2, a3, a4, a5]

        self._assert_history(
            u1,
            ([a3, a4, a5], [a2], [a1]),
            compare_passive=([a3, a4, a5], [], [a1]),
        )

    def test_persistent_but_readded_noflush(self):
        u1, a1, s = self._persistent_fixture(autoflush=False)
        u1.addresses.append(a1)
        s.flush()

        u1.addresses.append(a1)

        self._assert_history(
            u1, ([], [a1], []), compare_passive=([a1], [], [])
        )

    def test_persistent_but_readded_autoflush(self):
        u1, a1, s = self._persistent_fixture(autoflush=True)
        u1.addresses.append(a1)
        s.flush()

        u1.addresses.append(a1)

        self._assert_history(
            u1, ([], [a1], []), compare_passive=([a1], [], [])
        )

    def test_missing_but_removed_noflush(self):
        u1, a1, s = self._persistent_fixture(autoflush=False)

        u1.addresses.remove(a1)

        self._assert_history(u1, ([], [], []), compare_passive=([], [], [a1]))
