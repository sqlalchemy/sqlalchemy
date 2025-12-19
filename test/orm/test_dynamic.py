from sqlalchemy import cast
from sqlalchemy import Column
from sqlalchemy import desc
from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Identity
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.orm import attributes
from sqlalchemy.orm import backref
from sqlalchemy.orm import configure_mappers
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import noload
from sqlalchemy.orm import PassiveFlag
from sqlalchemy.orm import Query
from sqlalchemy.orm import relationship
from sqlalchemy.orm import WriteOnlyMapped
from sqlalchemy.orm.session import make_transient_to_detached
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import is_
from sqlalchemy.testing.assertsql import CompiledSQL
from sqlalchemy.testing.assertsql import Conditional
from sqlalchemy.testing.fixtures import fixture_session
from test.orm import _fixtures


class _DynamicFixture:
    lazy = "dynamic"

    @testing.fixture
    def user_address_fixture(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        def _user_address_fixture(addresses_args={}):
            self.mapper_registry.map_imperatively(
                User,
                users,
                properties={
                    "addresses": relationship(
                        Address, lazy=self.lazy, **addresses_args
                    )
                },
            )
            self.mapper_registry.map_imperatively(Address, addresses)
            return User, Address

        yield _user_address_fixture

    @testing.fixture
    def order_item_fixture(self):
        def _order_item_fixture(items_args={}):
            items, Order, orders, order_items, Item = (
                self.tables.items,
                self.classes.Order,
                self.tables.orders,
                self.tables.order_items,
                self.classes.Item,
            )

            self.mapper_registry.map_imperatively(
                Order,
                orders,
                properties={
                    "items": relationship(
                        Item,
                        secondary=order_items,
                        lazy=self.lazy,
                        **items_args,
                    )
                },
            )
            self.mapper_registry.map_imperatively(Item, items)
            return Order, Item

        yield _order_item_fixture

    @testing.fixture
    def user_order_item_fixture(self):
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

        def _user_order_item_fixture():
            self.mapper_registry.map_imperatively(
                User,
                users,
                properties={
                    "orders": relationship(
                        Order, order_by=orders.c.id, lazy=self.lazy
                    )
                },
            )
            self.mapper_registry.map_imperatively(
                Order,
                orders,
                properties={
                    "items": relationship(
                        Item, secondary=order_items, order_by=items.c.id
                    ),
                },
            )
            self.mapper_registry.map_imperatively(
                Item,
                items,
                properties={
                    "keywords": relationship(
                        Keyword, secondary=item_keywords
                    )  # m2m
                },
            )
            self.mapper_registry.map_imperatively(Keyword, keywords)

            return User, Order, Item, Keyword

        yield _user_order_item_fixture

    def _expect_no_iteration(self):
        return expect_raises_message(
            exc.InvalidRequestError,
            'Collection "User.addresses" does not support implicit '
            "iteration",
        )


class _WriteOnlyFixture(_DynamicFixture):
    lazy = "write_only"


class DynamicTest(_DynamicFixture, _fixtures.FixtureTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_basic(self, user_address_fixture):
        User, Address = user_address_fixture()

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

    def test_slice_access(self, user_address_fixture):
        User, Address = user_address_fixture()

        sess = fixture_session()
        u1 = sess.get(User, 8)

        eq_(u1.addresses.limit(1).one(), Address(id=2))

        eq_(u1.addresses[0], Address(id=2))
        eq_(u1.addresses[0:2], [Address(id=2), Address(id=3)])

    def test_negative_slice_access_raises(self, user_address_fixture):
        User, Address = user_address_fixture()

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

    def test_statement(self, user_address_fixture):
        """test that the .statement accessor returns the actual statement that
        would render, without any _clones called."""

        User, Address = user_address_fixture()

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

    def test_query_class_custom_method(self, user_address_fixture):
        class MyClass(Query):
            def my_filter(self, arg):
                return self.filter(Address.email_address == arg)

        User, Address = user_address_fixture(
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

    @testing.combinations(
        ("all", []),
        ("one", exc.NoResultFound),
        ("one_or_none", None),
        argnames="method, expected",
    )
    @testing.variation("add_to_session", [True, False])
    def test_transient_raise(
        self, user_address_fixture, method, expected, add_to_session
    ):
        """test 11562"""
        User, Address = user_address_fixture()

        u1 = User(name="u1")
        if add_to_session:
            sess = fixture_session()
            sess.add(u1)

        meth = getattr(u1.addresses, method)
        if expected is exc.NoResultFound:
            with expect_raises_message(
                exc.NoResultFound, "No row was found when one was required"
            ):
                meth()
        else:
            eq_(meth(), expected)

    def test_detached_raise(self, user_address_fixture):
        """so filtering on a detached dynamic list raises an error..."""

        User, Address = user_address_fixture()

        sess = fixture_session()
        u = sess.get(User, 8)
        sess.expunge(u)
        assert_raises(
            orm_exc.DetachedInstanceError,
            u.addresses.filter_by,
            email_address="e",
        )

    def test_detached_all_empty_list(self, user_address_fixture):
        """test #6426 - but you can call .all() on it and you get an empty
        list.   This is legacy stuff, as this should be raising
        DetachedInstanceError.

        """

        User, Address = user_address_fixture()

        sess = fixture_session()
        u = sess.get(User, 8)
        sess.expunge(u)

        with testing.expect_warnings(
            r"Instance <User .*> is detached, dynamic relationship"
        ):
            eq_(u.addresses.all(), [])

        with testing.expect_warnings(
            r"Instance <User .*> is detached, dynamic relationship"
        ):
            eq_(list(u.addresses), [])

    def test_transient_all_empty_list(self, user_address_fixture):
        User, Address = user_address_fixture()

        u1 = User()
        eq_(u1.addresses.all(), [])

        eq_(list(u1.addresses), [])

    def test_no_uselist_false(self, user_address_fixture):
        User, Address = user_address_fixture(addresses_args={"uselist": False})
        assert_raises_message(
            exc.InvalidRequestError,
            "On relationship User.addresses, 'dynamic' loaders cannot be "
            "used with many-to-one/one-to-one relationships and/or "
            "uselist=False.",
            configure_mappers,
        )

    @testing.combinations(False, True, None, argnames="uselist")
    def test_no_m2o(self, uselist):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        if uselist in (True, False):
            kw = {"uselist": uselist}
        else:
            kw = {}

        self.mapper_registry.map_imperatively(
            Address,
            addresses,
            properties={"user": relationship(User, lazy="dynamic", **kw)},
        )
        self.mapper_registry.map_imperatively(User, users)

        with expect_raises_message(
            exc.InvalidRequestError,
            "On relationship Address.user, 'dynamic' loaders cannot be "
            "used with many-to-one/one-to-one relationships and/or "
            "uselist=False.",
        ):
            configure_mappers()

    def test_order_by(self, user_address_fixture):
        User, Address = user_address_fixture()

        sess = fixture_session()
        u = sess.get(User, 8)
        eq_(
            list(u.addresses.order_by(desc(Address.email_address))),
            [
                Address(email_address="ed@wood.com"),
                Address(email_address="ed@lala.com"),
                Address(email_address="ed@bettyboop.com"),
            ],
        )

    @testing.requires.dupe_order_by_ok
    def test_order_by_composition_uses_immutable_tuple(
        self, user_address_fixture
    ):
        addresses = self.tables.addresses
        User, Address = user_address_fixture(
            addresses_args={"order_by": addresses.c.email_address.desc()}
        )

        sess = fixture_session()
        u = sess.get(User, 8)

        with self.sql_execution_asserter() as asserter:
            for i in range(3):
                eq_(
                    list(u.addresses.order_by(desc(Address.email_address))),
                    [
                        Address(email_address="ed@wood.com"),
                        Address(email_address="ed@lala.com"),
                        Address(email_address="ed@bettyboop.com"),
                    ],
                )
        asserter.assert_(
            *[
                CompiledSQL(
                    "SELECT addresses.id AS addresses_id, addresses.user_id "
                    "AS addresses_user_id, addresses.email_address "
                    "AS addresses_email_address FROM addresses "
                    "WHERE :param_1 = addresses.user_id "
                    "ORDER BY addresses.email_address DESC, "
                    "addresses.email_address DESC",
                    [{"param_1": 8}],
                )
                for i in range(3)
            ]
        )

    def test_configured_order_by(self, user_address_fixture):
        addresses = self.tables.addresses
        User, Address = user_address_fixture(
            addresses_args={"order_by": addresses.c.email_address.desc()}
        )

        sess = fixture_session()
        u = sess.get(User, 8)
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
            {
                Address(email_address="ed@bettyboop.com"),
                Address(email_address="ed@lala.com"),
                Address(email_address="ed@wood.com"),
            },
        )

    def test_count(self, user_address_fixture):
        User, Address = user_address_fixture()

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

        self.mapper_registry.map_imperatively(
            Address,
            addresses,
            properties={
                "user": relationship(
                    User, backref=backref("addresses", lazy="dynamic")
                )
            },
        )
        self.mapper_registry.map_imperatively(User, users)

        sess = fixture_session()
        ad = sess.get(Address, 1)

        def go():
            ad.user = None

        self.assert_sql_count(testing.db, go, 0)
        sess.flush()
        u = sess.get(User, 7)
        assert ad not in u.addresses

    def test_no_count(self, user_address_fixture):
        User, Address = user_address_fixture()

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

    def test_no_populate(self, user_address_fixture):
        User, Address = user_address_fixture()

        u1 = User()
        assert_raises_message(
            NotImplementedError,
            "Dynamic attributes don't support collection population.",
            attributes.set_committed_value,
            u1,
            "addresses",
            [],
        )

    @testing.combinations(("star",), ("attronly",), argnames="type_")
    def test_noload_issue(self, type_, user_address_fixture):
        """test #6420.   a noload that hits the dynamic loader
        should have no effect.

        """

        User, Address = user_address_fixture()

        s = fixture_session()

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

    def test_m2m(self, order_item_fixture):
        Order, Item = order_item_fixture(
            items_args={"backref": backref("orders", lazy="dynamic")}
        )

        sess = fixture_session()
        o1 = Order(id=15, description="order 10")
        i1 = Item(id=10, description="item 8")
        o1.items.add(i1)
        sess.add(o1)
        sess.flush()

        assert o1 in i1.orders.all()
        assert i1 in o1.items.all()

    def test_association_nonaliased(self):
        items, Order, orders, order_items, Item = (
            self.tables.items,
            self.classes.Order,
            self.tables.orders,
            self.tables.order_items,
            self.classes.Item,
        )

        self.mapper_registry.map_imperatively(
            Order,
            orders,
            properties={
                "items": relationship(
                    Item,
                    secondary=order_items,
                    order_by=order_items.c.item_id,
                    lazy="dynamic",
                )
            },
        )
        self.mapper_registry.map_imperatively(Item, items)

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

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "items": relationship(
                    Item, secondary=order_items.join(orders), lazy="dynamic"
                )
            },
        )
        item_mapper = self.mapper_registry.map_imperatively(Item, items)

        sess = fixture_session()

        u1 = sess.query(User).first()

        dyn = u1.items

        # test for #7868
        eq_(dyn._from_obj[0]._annotations["parententity"], item_mapper)

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

    def test_secondary_as_join_complex_entity(self, decl_base):
        """integration test for #7868"""

        class GrandParent(decl_base):
            __tablename__ = "grandparent"
            id = Column(Integer, primary_key=True)

            grand_children = relationship(
                "Child", secondary="parent", lazy="dynamic", viewonly=True
            )

        class Parent(decl_base):
            __tablename__ = "parent"
            id = Column(Integer, primary_key=True)
            grand_parent_id = Column(
                Integer, ForeignKey("grandparent.id"), nullable=False
            )

        class Child(decl_base):
            __tablename__ = "child"
            id = Column(Integer, primary_key=True)
            type = Column(String)
            parent_id = Column(
                Integer, ForeignKey("parent.id"), nullable=False
            )

            __mapper_args__ = {
                "polymorphic_on": type,
                "polymorphic_identity": "unknown",
                "with_polymorphic": "*",
            }

        class SubChild(Child):
            __tablename__ = "subchild"
            id = Column(Integer, ForeignKey("child.id"), primary_key=True)

            __mapper_args__ = {
                "polymorphic_identity": "sub",
            }

        gp = GrandParent(id=1)
        make_transient_to_detached(gp)
        sess = fixture_session()
        sess.add(gp)
        self.assert_compile(
            gp.grand_children.filter_by(id=1),
            "SELECT child.id AS child_id, child.type AS child_type, "
            "child.parent_id AS child_parent_id, subchild.id AS subchild_id "
            "FROM child LEFT OUTER JOIN subchild "
            "ON child.id = subchild.id, parent "
            "WHERE :param_1 = parent.grand_parent_id "
            "AND parent.id = child.parent_id AND child.id = :id_1",
            {"id_1": 1},
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

        class ItemKeyword:
            pass

        self.mapper_registry.map_imperatively(
            Order,
            orders,
            properties={
                "items": relationship(
                    Item, secondary=order_items, lazy="dynamic"
                )
            },
        )
        self.mapper_registry.map_imperatively(
            ItemKeyword,
            item_keywords,
            primary_key=[item_keywords.c.item_id, item_keywords.c.keyword_id],
        )
        self.mapper_registry.map_imperatively(
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
            "FROM items "
            "JOIN item_keywords ON items.id = item_keywords.item_id, "
            "order_items "
            "WHERE :param_1 = order_items.order_id "
            "AND items.id = order_items.item_id",
            use_default_dialect=True,
        )

    def test_transient_count(self, user_address_fixture):
        User, Address = user_address_fixture()

        u1 = User()
        u1.addresses.add(Address())
        eq_(u1.addresses.count(), 1)

    def test_transient_access(self, user_address_fixture):
        User, Address = user_address_fixture()

        u1 = User()
        u1.addresses.add(Address())
        eq_(u1.addresses[0], Address())


class WriteOnlyTest(
    _WriteOnlyFixture, _fixtures.FixtureTest, AssertsCompiledSQL
):
    __dialect__ = "default"

    @testing.combinations(("star",), ("attronly",), argnames="type_")
    def test_noload_issue(self, type_, user_address_fixture):
        """test #6420.   a noload that hits the dynamic loader
        should have no effect.

        """

        User, Address = user_address_fixture()

        s = fixture_session()

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

    def test_iteration_error(self, user_address_fixture):
        User, Address = user_address_fixture()

        sess = fixture_session()
        u = sess.get(User, 8)

        with expect_raises_message(
            TypeError,
            "WriteOnly collections don't support iteration in-place; to "
            "query for collection items",
        ):
            list(u.addresses)

    def test_order_by(self, user_address_fixture):
        User, Address = user_address_fixture()

        sess = fixture_session()
        u = sess.get(User, 8)
        eq_(
            list(
                sess.scalars(
                    u.addresses.select().order_by(desc(Address.email_address))
                )
            ),
            [
                Address(email_address="ed@wood.com"),
                Address(email_address="ed@lala.com"),
                Address(email_address="ed@bettyboop.com"),
            ],
        )

    def test_configured_order_by(self, user_address_fixture):
        addresses = self.tables.addresses
        User, Address = user_address_fixture(
            addresses_args={"order_by": addresses.c.email_address.desc()}
        )

        sess = fixture_session()
        u = sess.get(User, 8)
        eq_(
            list(sess.scalars(u.addresses.select())),
            [
                Address(email_address="ed@wood.com"),
                Address(email_address="ed@lala.com"),
                Address(email_address="ed@bettyboop.com"),
            ],
        )

        # test cancellation of None, replacement with something else
        eq_(
            list(
                sess.scalars(
                    u.addresses.select()
                    .order_by(None)
                    .order_by(Address.email_address)
                )
            ),
            [
                Address(email_address="ed@bettyboop.com"),
                Address(email_address="ed@lala.com"),
                Address(email_address="ed@wood.com"),
            ],
        )

        # test cancellation of None, replacement with nothing
        eq_(
            set(sess.scalars(u.addresses.select().order_by(None))),
            {
                Address(email_address="ed@bettyboop.com"),
                Address(email_address="ed@lala.com"),
                Address(email_address="ed@wood.com"),
            },
        )

    def test_secondary_as_join(self):
        # test [ticket:4349]
        User, users = self.classes.User, self.tables.users
        items, orders, order_items, Item = (
            self.tables.items,
            self.tables.orders,
            self.tables.order_items,
            self.classes.Item,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "items": relationship(
                    Item, secondary=order_items.join(orders), lazy="write_only"
                )
            },
        )
        item_mapper = self.mapper_registry.map_imperatively(Item, items)

        sess = fixture_session()

        u1 = sess.query(User).first()

        dyn = u1.items.select()

        # test for #7868
        eq_(dyn._from_obj[0]._annotations["parententity"], item_mapper)

        self.assert_compile(
            u1.items.select(),
            "SELECT items.id, "
            "items.description "
            "FROM items, order_items JOIN orders "
            "ON orders.id = order_items.order_id "
            "WHERE :param_1 = orders.user_id "
            "AND items.id = order_items.item_id",
            use_default_dialect=True,
        )

    def test_secondary_as_join_complex_entity(self, decl_base):
        """integration test for #7868"""

        class GrandParent(decl_base):
            __tablename__ = "grandparent"
            id = Column(Integer, primary_key=True)

            grand_children = relationship(
                "Child", secondary="parent", viewonly=True, lazy="write_only"
            )

        class Parent(decl_base):
            __tablename__ = "parent"
            id = Column(Integer, primary_key=True)
            grand_parent_id = Column(
                Integer, ForeignKey("grandparent.id"), nullable=False
            )

        class Child(decl_base):
            __tablename__ = "child"
            id = Column(Integer, primary_key=True)
            type = Column(String)
            parent_id = Column(
                Integer, ForeignKey("parent.id"), nullable=False
            )

            __mapper_args__ = {
                "polymorphic_on": type,
                "polymorphic_identity": "unknown",
                "with_polymorphic": "*",
            }

        class SubChild(Child):
            __tablename__ = "subchild"
            id = Column(Integer, ForeignKey("child.id"), primary_key=True)

            __mapper_args__ = {
                "polymorphic_identity": "sub",
            }

        gp = GrandParent(id=1)
        make_transient_to_detached(gp)
        self.assert_compile(
            gp.grand_children.select().filter_by(id=1),
            "SELECT child.id, child.type, "
            "child.parent_id, subchild.id AS id_1 "
            "FROM child LEFT OUTER JOIN subchild "
            "ON child.id = subchild.id, parent "
            "WHERE :param_1 = parent.grand_parent_id "
            "AND parent.id = child.parent_id AND child.id = :id_2",
            {"id_2": 1},
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

        class ItemKeyword:
            pass

        self.mapper_registry.map_imperatively(
            Order,
            orders,
            properties={
                "items": relationship(
                    Item, secondary=order_items, lazy="write_only"
                )
            },
        )
        self.mapper_registry.map_imperatively(
            ItemKeyword,
            item_keywords,
            primary_key=[item_keywords.c.item_id, item_keywords.c.keyword_id],
        )
        self.mapper_registry.map_imperatively(
            Item,
            items,
            properties={"item_keywords": relationship(ItemKeyword)},
        )

        sess = fixture_session()
        order = sess.query(Order).first()

        self.assert_compile(
            order.items.select().join(ItemKeyword),
            "SELECT items.id, "
            "items.description "
            "FROM items "
            "JOIN item_keywords ON items.id = item_keywords.item_id, "
            "order_items "
            "WHERE :param_1 = order_items.order_id "
            "AND items.id = order_items.item_id",
            use_default_dialect=True,
        )


class _UOWTests:
    run_inserts = None

    def _list_collection(self, collection):
        if self.lazy == "dynamic":
            return list(collection)

        sess = inspect(collection.instance).session
        return sess.scalars(collection.select()).all()

    def test_persistence(self, user_address_fixture):
        addresses = self.tables.addresses
        User, Address = user_address_fixture()

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
        u1 = sess.get(User, u1.id)
        u1.addresses.add(a1)
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

        u1.addresses.add(a1)
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
        u1.addresses.add(a2)
        sess.flush()
        eq_(
            sess.connection()
            .execute(
                select(addresses).where(addresses.c.user_id != None)  # noqa
            )
            .fetchall(),
            [(a2.id, u1.id, "bar")],
        )

    def test_hasattr(self, user_address_fixture):
        User, Address = user_address_fixture()

        u1 = User(name="jack")

        assert "addresses" not in u1.__dict__
        u1.addresses = [Address(email_address="test")]
        assert "addresses" in u1.__dict__

    def test_collection_set(self, user_address_fixture):
        addresses = self.tables.addresses
        User, Address = user_address_fixture(
            addresses_args={"order_by": addresses.c.email_address}
        )
        sess = fixture_session(
            autoflush=True,
        )
        u1 = User(name="jack")
        a1 = Address(email_address="a1")
        a2 = Address(email_address="a2")
        a3 = Address(email_address="a3")
        a4 = Address(email_address="a4")

        sess.add(u1)
        u1.addresses = [a1, a3]
        eq_(self._list_collection(u1.addresses), [a1, a3])

        if User.addresses.property.lazy == "write_only":
            with self._expect_no_iteration():
                u1.addresses = [a1, a2, a4]
            return

        u1.addresses = [a1, a2, a4]
        eq_(list(u1.addresses), [a1, a2, a4])
        u1.addresses = [a2, a3]
        eq_(list(u1.addresses), [a2, a3])
        u1.addresses = []
        eq_(list(u1.addresses), [])

    def test_noload_add(self, user_address_fixture):
        # test that a load of User.addresses is not emitted
        # when flushing an add
        User, Address = user_address_fixture()

        sess = fixture_session()
        u1 = User(name="jack", addresses=[Address(email_address="a1")])
        sess.add(u1)
        sess.commit()

        u1_id = u1.id
        sess.expire_all()

        u1.addresses.add(Address(email_address="a2"))

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

    def test_noload_remove(self, user_address_fixture):
        # test that a load of User.addresses is not emitted
        # when flushing a remove
        User, Address = user_address_fixture()

        sess = fixture_session()
        u1 = User(name="jack", addresses=[Address(email_address="a1")])
        a2 = Address(email_address="a2")
        u1.addresses.add(a2)
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

    def test_rollback(self, user_address_fixture):
        User, Address = user_address_fixture()

        sess = fixture_session(expire_on_commit=False, autoflush=True)
        u1 = User(name="jack")
        u1.addresses.add(Address(email_address="lala@hoho.com"))
        sess.add(u1)
        sess.flush()
        sess.commit()
        u1.addresses.add(Address(email_address="foo@bar.com"))

        if self.lazy == "dynamic":
            stmt = u1.addresses.statement
        else:
            stmt = u1.addresses.select()

        eq_(
            sess.scalars(stmt.order_by(Address.id)).all(),
            [
                Address(email_address="lala@hoho.com"),
                Address(email_address="foo@bar.com"),
            ],
        )
        sess.rollback()
        eq_(
            sess.scalars(stmt).all(),
            [Address(email_address="lala@hoho.com")],
        )

    def test_self_referential(self):
        Node, nodes = self.classes.Node, self.tables.nodes

        self.mapper_registry.map_imperatively(
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

    def test_remove_orphans(self, user_address_fixture):
        addresses = self.tables.addresses
        User, Address = user_address_fixture(
            addresses_args={
                "order_by": addresses.c.id,
                "backref": "user",
                "cascade": "all, delete-orphan",
            }
        )

        sess = fixture_session(
            autoflush=True,
        )
        u = User(name="ed")
        u.addresses.add_all(
            [Address(email_address=letter) for letter in "abcdef"]
        )
        sess.add(u)

        if self.lazy == "dynamic":
            stmt = u.addresses.statement
        else:
            stmt = u.addresses.select()

        for a in sess.scalars(
            stmt.filter(Address.email_address.in_(["c", "e", "f"]))
        ):
            u.addresses.remove(a)

        eq_(
            {ad for ad, in sess.query(Address.email_address)},
            {"a", "b", "d"},
        )

    @testing.combinations(True, False, argnames="autoflush")
    @testing.combinations(True, False, argnames="saveuser")
    def test_backref(self, autoflush, saveuser, user_address_fixture):
        User, Address = user_address_fixture(
            addresses_args={"backref": "user"}
        )
        sess = fixture_session(
            autoflush=autoflush,
        )

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

        eq_(self._list_collection(u.addresses), [a])

        a.user = None
        if not autoflush:
            eq_(self._list_collection(u.addresses), [a])

        if not autoflush:
            sess.flush()
        eq_(self._list_collection(u.addresses), [])

    def test_backref_events(self, user_address_fixture):
        User, Address = user_address_fixture(
            addresses_args={"backref": "user"}
        )

        u1 = User()
        a1 = Address()
        u1.addresses.add(a1)
        is_(a1.user, u1)

    def test_no_deref(self, user_address_fixture):
        User, Address = user_address_fixture(
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

            return self._list_collection(user.addresses)

        def query2():
            session = fixture_session()

            return self._list_collection(session.query(User).first().addresses)

        def query3():
            session = fixture_session()

            return self._list_collection(session.query(User).first().addresses)

        eq_(query1(), [Address(email_address="joe@joesdomain.example")])
        eq_(query2(), [Address(email_address="joe@joesdomain.example")])
        eq_(query3(), [Address(email_address="joe@joesdomain.example")])


class DynamicUOWTest(
    _DynamicFixture,
    _UOWTests,
    _fixtures.FixtureTest,
    testing.AssertsExecutionResults,
):
    run_inserts = None

    @testing.combinations(
        "empty", "persistent", "transient", argnames="merge_type"
    )
    def test_merge_persistent(self, merge_type, user_address_fixture):
        addresses = self.tables.addresses
        User, Address = user_address_fixture(
            addresses_args={"order_by": addresses.c.email_address}
        )
        sess = fixture_session(autoflush=False)

        a1 = Address(email_address="a1")
        a2 = Address(email_address="a2")
        a3 = Address(email_address="a3")
        u1 = User(name="jack", addresses=[a2, a3])

        if merge_type == "transient":
            # merge transient.  no collection iteration is implied by this.
            u1 = sess.merge(u1)
            sess.add(a1)
        else:
            sess.add_all([u1, a1])
        sess.flush()

        if merge_type == "persistent":
            u1 = User(id=u1.id, name="jane", addresses=[a1, a3])

            # for Dynamic, the list is iterated.   it's been this way the
            # whole time, which is clearly not very useful for a
            # "collection that's too large to load".  however we maintain
            # legacy behavior here
            u1 = sess.merge(u1)
            eq_(attributes.get_history(u1, "addresses"), ([a1], [a3], [a2]))

            sess.flush()

            if self.lazy == "dynamic":
                stmt = u1.addresses.statement
            else:
                stmt = u1.addresses.select()
            eq_(sess.scalars(stmt).all(), [a1, a3])

        elif merge_type == "empty":
            # merge while omitting the "too large to load" collection
            # works fine.
            u1 = User(id=u1.id, name="jane")
            u1 = sess.merge(u1)

            eq_(attributes.get_history(u1, "addresses"), ([], [a2, a3], []))

            sess.flush()

            if self.lazy == "dynamic":
                stmt = u1.addresses.statement
            else:
                stmt = u1.addresses.select()

            eq_(sess.scalars(stmt).all(), [a2, a3])

    @testing.combinations(True, False, argnames="delete_cascade_configured")
    def test_delete_cascade(
        self, delete_cascade_configured, user_address_fixture
    ):
        addresses = self.tables.addresses
        User, Address = user_address_fixture(
            addresses_args={
                "order_by": addresses.c.id,
                "backref": "user",
                "cascade": (
                    "save-update"
                    if not delete_cascade_configured
                    else "all, delete"
                ),
            }
        )

        sess = fixture_session(
            autoflush=True,
        )
        u = User(name="ed")
        u.addresses.add_all(
            [Address(email_address=letter) for letter in "abcdef"]
        )
        sess.add(u)
        sess.commit()

        from sqlalchemy import case

        # the byzantine syntax here is so the query works on MSSQL
        isnull_stmt = select(
            case((addresses.c.user_id == None, True), else_=False),
            func.count("*"),
        ).group_by(
            case((addresses.c.user_id == None, True), else_=False),
            addresses.c.user_id,
        )

        eq_(
            {isnull: count for isnull, count in sess.execute(isnull_stmt)},
            {False: 6},
        )

        sess.delete(u)

        sess.commit()

        if not delete_cascade_configured:
            eq_(
                {isnull: count for isnull, count in sess.execute(isnull_stmt)},
                {True: 6},
            )
        else:
            eq_(
                sess.connection()
                .execute(select(func.count("*")).select_from(addresses))
                .scalar(),
                0,
            )


class WriteOnlyUOWTest(
    _WriteOnlyFixture,
    _UOWTests,
    _fixtures.FixtureTest,
    testing.AssertsExecutionResults,
):
    __sparse_driver_backend__ = True

    @testing.fixture
    def passive_deletes_fixture(self, decl_base, connection):
        """passive deletes fixture

        this fixture is separate from the FixtureTest setup because we need
        to produce the related Table using ON DELETE cascade for the
        foreign key.

        """

        def go(passive_deletes, cascade_deletes):
            class A(decl_base):
                __tablename__ = "a"
                id: Mapped[int] = mapped_column(Identity(), primary_key=True)
                data: Mapped[str]
                bs: WriteOnlyMapped["B"] = relationship(  # noqa: F821
                    passive_deletes=passive_deletes,
                    cascade=(
                        "all, delete-orphan"
                        if cascade_deletes
                        else "save-update, merge"
                    ),
                    order_by="B.id",
                )

            class B(decl_base):
                __tablename__ = "b"
                id: Mapped[int] = mapped_column(Identity(), primary_key=True)
                a_id: Mapped[int] = mapped_column(
                    ForeignKey(
                        "a.id",
                        ondelete="cascade" if cascade_deletes else "set null",
                    ),
                    nullable=not cascade_deletes,
                )

            decl_base.metadata.create_all(connection)
            return A, B

        yield go

    @testing.combinations(
        "empty", "persistent", "transient", argnames="merge_type"
    )
    def test_merge_persistent(self, merge_type, user_address_fixture):
        addresses = self.tables.addresses
        User, Address = user_address_fixture(
            addresses_args={"order_by": addresses.c.email_address}
        )
        sess = fixture_session(autoflush=False)

        a1 = Address(email_address="a1")
        a2 = Address(email_address="a2")
        a3 = Address(email_address="a3")
        u1 = User(name="jack", addresses=[a2, a3])

        if merge_type == "transient":
            # merge transient.  no collection iteration is implied by this.
            u1 = sess.merge(u1)
            sess.add(a1)
        else:
            sess.add_all([u1, a1])
        sess.flush()

        if merge_type == "persistent":
            u1 = User(id=u1.id, name="jane", addresses=[a1, a3])

            # merge of populated list into persistent not supported with
            # write_only because we would need to iterate the existing list
            with self._expect_no_iteration():
                u1 = sess.merge(u1)

        elif merge_type == "empty":
            # merge while omitting the "too large to load" collection
            # works fine.
            u1 = User(id=u1.id, name="jane")
            u1 = sess.merge(u1)

            eq_(
                attributes.get_history(
                    u1, "addresses", PassiveFlag.PASSIVE_NO_FETCH
                ),
                ([], [], []),
            )

            sess.flush()
            eq_(sess.scalars(u1.addresses.select()).all(), [a2, a3])

    def test_passive_deletes_required(self, user_address_fixture):
        addresses = self.tables.addresses
        User, Address = user_address_fixture(
            addresses_args={
                "order_by": addresses.c.id,
                "backref": "user",
                "cascade": "save-update",
            }
        )

        sess = fixture_session(
            autoflush=True,
        )
        u = User(
            name="ed",
            addresses=[Address(email_address=letter) for letter in "abcdef"],
        )
        sess.add(u)
        sess.commit()

        sess.delete(u)

        with expect_raises_message(
            exc.InvalidRequestError,
            "Attribute User.addresses can't load the existing state from the "
            "database for this operation; full iteration is not permitted.",
        ):
            sess.commit()

    @testing.combinations(True, False, argnames="cascade_deletes")
    def test_passive_deletes_succeed(
        self, passive_deletes_fixture, connection, cascade_deletes
    ):
        A, B = passive_deletes_fixture(True, cascade_deletes)

        sess = fixture_session(bind=connection)

        a1 = A(data="d1", bs=[B(), B(), B()])
        sess.add(a1)
        sess.commit()

        sess.delete(a1)

        sess.commit()

        if testing.requires.foreign_keys.enabled and cascade_deletes:
            eq_(sess.scalar(select(func.count()).select_from(B)), 0)
        else:
            eq_(sess.scalar(select(func.count()).select_from(B)), 3)

    @testing.combinations(True, False, argnames="cascade_deletes")
    def test_remove_orphans(
        self, passive_deletes_fixture, connection, cascade_deletes
    ):
        A, B = passive_deletes_fixture(True, cascade_deletes)

        sess = fixture_session(bind=connection)

        b1, b2, b3 = B(), B(), B()
        a1 = A(data="d1", bs=[b1, b2, b3])
        sess.add(a1)
        sess.commit()

        eq_(sess.scalars(a1.bs.select()).all(), [b1, b2, b3])

        a1.bs.remove(b2)

        sess.commit()

        eq_(sess.scalars(a1.bs.select()).all(), [b1, b3])

        if cascade_deletes:
            eq_(sess.scalar(select(func.count()).select_from(B)), 2)
        else:
            eq_(sess.scalar(select(func.count()).select_from(B)), 3)


class WriteOnlyBulkTest(
    _WriteOnlyFixture,
    _UOWTests,
    _fixtures.FixtureTest,
    testing.AssertsExecutionResults,
):
    run_inserts = None
    __sparse_driver_backend__ = True

    @testing.requires.insert_executemany_returning
    @testing.combinations(True, False, argnames="flush_user_first")
    def test_bulk_insert(self, user_address_fixture, flush_user_first):
        User, Address = user_address_fixture(
            addresses_args={"backref": "user"}
        )
        sess = fixture_session()

        u1 = User(name="x")
        sess.add(u1)

        # ha ha!  u1 is not persistent yet.  autoflush won't happen
        # until sess.scalars() actually runs.  statement has to be
        # created with a pending parameter, not actual parameter
        assert inspect(u1).pending

        if flush_user_first:
            sess.flush()

        with self.sql_execution_asserter() as asserter:
            addresses = sess.scalars(
                u1.addresses.insert().returning(Address),
                [
                    {"email_address": "e1"},
                    {"email_address": "e2"},
                    {"email_address": "e3"},
                ],
            ).all()

        eq_(
            addresses,
            [
                Address(user=User(name="x"), email_address="e1"),
                Address(user=User(name="x"), email_address="e2"),
                Address(user=User(name="x"), email_address="e3"),
            ],
        )

        uid = u1.id

        asserter.assert_(
            Conditional(
                not flush_user_first,
                [
                    CompiledSQL(
                        "INSERT INTO users (name) VALUES (:name)",
                        [{"name": "x"}],
                    )
                ],
                [],
            ),
            CompiledSQL(
                "INSERT INTO addresses (user_id, email_address) "
                "VALUES (:param_1, :email_address) "
                "RETURNING addresses.id, addresses.user_id, "
                "addresses.email_address",
                [
                    {"param_1": uid, "email_address": "e1"},
                    {"param_1": uid, "email_address": "e2"},
                    {"param_1": uid, "email_address": "e3"},
                ],
            ),
        )

    @testing.requires.update_returning
    @testing.combinations(True, False, argnames="flush_user_first")
    def test_bulk_update(self, user_address_fixture, flush_user_first):
        User, Address = user_address_fixture(
            addresses_args={"backref": "user"}
        )
        sess = fixture_session()

        u1 = User(
            name="x",
            addresses=[
                Address(email_address="e1"),
                Address(email_address="e2"),
                Address(email_address="e3"),
            ],
        )
        sess.add(u1)

        # ha ha!  u1 is not persistent yet.  autoflush won't happen
        # until sess.scalars() actually runs.  statement has to be
        # created with a pending parameter, not actual parameter
        assert inspect(u1).pending

        if flush_user_first:
            sess.flush()

        with self.sql_execution_asserter() as asserter:
            addresses = sess.scalars(
                u1.addresses.update()
                .values(email_address=Address.email_address + "@foo.com")
                .returning(Address),
            ).all()

        eq_(
            addresses,
            [
                Address(user=User(name="x"), email_address="e1@foo.com"),
                Address(user=User(name="x"), email_address="e2@foo.com"),
                Address(user=User(name="x"), email_address="e3@foo.com"),
            ],
        )

        uid = u1.id

        asserter.assert_(
            Conditional(
                not flush_user_first,
                [
                    CompiledSQL(
                        "INSERT INTO users (name) VALUES (:name)",
                        [{"name": "x"}],
                    ),
                    Conditional(
                        testing.requires.insert_executemany_returning.enabled,
                        [
                            CompiledSQL(
                                "INSERT INTO addresses "
                                "(user_id, email_address) "
                                "VALUES (:user_id, :email_address) "
                                "RETURNING addresses.id",
                                [
                                    {"user_id": uid, "email_address": "e1"},
                                    {"user_id": uid, "email_address": "e2"},
                                    {"user_id": uid, "email_address": "e3"},
                                ],
                            )
                        ],
                        [
                            CompiledSQL(
                                "INSERT INTO addresses "
                                "(user_id, email_address) "
                                "VALUES (:user_id, :email_address)",
                                param,
                            )
                            for param in [
                                {"user_id": uid, "email_address": "e1"},
                                {"user_id": uid, "email_address": "e2"},
                                {"user_id": uid, "email_address": "e3"},
                            ]
                        ],
                    ),
                ],
                [],
            ),
            CompiledSQL(
                "UPDATE addresses SET email_address=(addresses.email_address "
                "|| :email_address_1) WHERE :param_1 = addresses.user_id "
                "RETURNING addresses.id, addresses.user_id, "
                "addresses.email_address",
                [{"email_address_1": "@foo.com", "param_1": uid}],
            ),
        )

    @testing.requires.delete_returning
    @testing.combinations(True, False, argnames="flush_user_first")
    def test_bulk_delete(self, user_address_fixture, flush_user_first):
        User, Address = user_address_fixture(
            addresses_args={"backref": "user"}
        )
        sess = fixture_session()

        u1 = User(
            name="x",
            addresses=[
                Address(email_address="e1"),
                Address(email_address="e2"),
                Address(email_address="e3"),
            ],
        )
        sess.add(u1)

        # ha ha!  u1 is not persistent yet.  autoflush won't happen
        # until sess.scalars() actually runs.  statement has to be
        # created with a pending parameter, not actual parameter
        assert inspect(u1).pending

        if flush_user_first:
            sess.flush()

        with self.sql_execution_asserter() as asserter:
            addresses = sess.scalars(
                u1.addresses.delete()
                .where(Address.email_address == "e2")
                .returning(Address),
            ).all()

        eq_(
            addresses,
            [
                Address(email_address="e2"),
            ],
        )

        uid = u1.id

        asserter.assert_(
            Conditional(
                not flush_user_first,
                [
                    CompiledSQL(
                        "INSERT INTO users (name) VALUES (:name)",
                        [{"name": "x"}],
                    ),
                    Conditional(
                        testing.requires.insert_executemany_returning.enabled,
                        [
                            CompiledSQL(
                                "INSERT INTO addresses "
                                "(user_id, email_address) "
                                "VALUES (:user_id, :email_address) "
                                "RETURNING addresses.id",
                                [
                                    {"user_id": uid, "email_address": "e1"},
                                    {"user_id": uid, "email_address": "e2"},
                                    {"user_id": uid, "email_address": "e3"},
                                ],
                            )
                        ],
                        [
                            CompiledSQL(
                                "INSERT INTO addresses "
                                "(user_id, email_address) "
                                "VALUES (:user_id, :email_address)",
                                param,
                            )
                            for param in [
                                {"user_id": uid, "email_address": "e1"},
                                {"user_id": uid, "email_address": "e2"},
                                {"user_id": uid, "email_address": "e3"},
                            ]
                        ],
                    ),
                ],
                [],
            ),
            CompiledSQL(
                "DELETE FROM addresses WHERE :param_1 = addresses.user_id "
                "AND addresses.email_address = :email_address_1 "
                "RETURNING addresses.id, addresses.user_id, "
                "addresses.email_address",
                [{"param_1": uid, "email_address_1": "e2"}],
            ),
        )


class _HistoryTest:
    @testing.fixture
    def transient_fixture(self, user_address_fixture):
        def _transient_fixture(addresses_args={}):
            User, Address = user_address_fixture(addresses_args=addresses_args)

            u1 = User()
            a1 = Address()
            return u1, a1

        yield _transient_fixture

    @testing.fixture
    def persistent_fixture(self, user_address_fixture):
        def _persistent_fixture(autoflush=True, addresses_args={}):
            User, Address = user_address_fixture(addresses_args=addresses_args)

            u1 = User(name="u1")
            a1 = Address(email_address="a1")
            s = fixture_session(autoflush=autoflush)
            s.add(u1)
            s.flush()
            return u1, a1, s

        yield _persistent_fixture

    @testing.fixture
    def persistent_m2m_fixture(self, order_item_fixture):
        def _persistent_m2m_fixture(autoflush=True, items_args={}):
            Order, Item = order_item_fixture(items_args=items_args)

            o1 = Order()
            i1 = Item(description="i1")
            s = fixture_session(autoflush=autoflush)
            s.add(o1)
            s.flush()
            return o1, i1, s

        yield _persistent_m2m_fixture

    def _assert_history(self, obj, compare, compare_passive=None):
        if isinstance(obj, self.classes.User):
            attrname = "addresses"
        elif isinstance(obj, self.classes.Order):
            attrname = "items"

        sess = inspect(obj).session

        if sess:
            sess.autoflush = False
        try:
            if self.lazy == "write_only" and compare_passive is not None:
                eq_(
                    attributes.get_history(
                        obj, attrname, PassiveFlag.PASSIVE_NO_FETCH
                    ),
                    compare_passive,
                )
            else:
                eq_(
                    attributes.get_history(
                        obj,
                        attrname,
                        (
                            PassiveFlag.PASSIVE_NO_FETCH
                            if self.lazy == "write_only"
                            else PassiveFlag.PASSIVE_OFF
                        ),
                    ),
                    compare,
                )

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

    def test_add_transient(self, transient_fixture):
        u1, a1 = transient_fixture()
        u1.addresses.add(a1)

        self._assert_history(u1, ([a1], [], []))

    def test_add_persistent(self, persistent_fixture):
        u1, a1, s = persistent_fixture()
        u1.addresses.add(a1)

        self._assert_history(u1, ([a1], [], []))

    def test_remove_transient(self, transient_fixture):
        u1, a1 = transient_fixture()
        u1.addresses.add(a1)
        u1.addresses.remove(a1)

        self._assert_history(u1, ([], [], []))

    def test_backref_pop_transient(self, transient_fixture):
        u1, a1 = transient_fixture(addresses_args={"backref": "user"})
        u1.addresses.add(a1)

        self._assert_history(u1, ([a1], [], []))

        a1.user = None

        # removed from added
        self._assert_history(u1, ([], [], []))

    def test_remove_persistent(self, persistent_fixture):
        u1, a1, s = persistent_fixture()
        u1.addresses.add(a1)
        s.flush()
        s.expire_all()

        u1.addresses.remove(a1)

        self._assert_history(u1, ([], [], [a1]))

    def test_backref_pop_persistent_autoflush_o2m_active_hist(
        self, persistent_fixture
    ):
        u1, a1, s = persistent_fixture(
            addresses_args={"backref": backref("user", active_history=True)}
        )
        u1.addresses.add(a1)
        s.flush()
        s.expire_all()

        a1.user = None

        self._assert_history(u1, ([], [], [a1]))

    def test_backref_pop_persistent_autoflush_m2m(
        self, persistent_m2m_fixture
    ):
        o1, i1, s = persistent_m2m_fixture(items_args={"backref": "orders"})
        o1.items.add(i1)
        s.flush()
        s.expire_all()

        i1.orders.remove(o1)

        self._assert_history(o1, ([], [], [i1]))

    def test_backref_pop_persistent_noflush_m2m(self, persistent_m2m_fixture):
        o1, i1, s = persistent_m2m_fixture(
            items_args={"backref": "orders"}, autoflush=False
        )
        o1.items.add(i1)
        s.flush()
        s.expire_all()

        i1.orders.remove(o1)

        self._assert_history(o1, ([], [], [i1]))

    def test_unchanged_persistent(self, persistent_fixture):
        Address = self.classes.Address

        u1, a1, s = persistent_fixture()
        a2, a3 = Address(email_address="a2"), Address(email_address="a3")

        u1.addresses.add(a1)
        u1.addresses.add(a2)
        s.flush()

        u1.addresses.add(a3)
        u1.addresses.remove(a2)

        self._assert_history(
            u1, ([a3], [a1], [a2]), compare_passive=([a3], [], [a2])
        )

    def test_replace_transient(self, transient_fixture):
        Address = self.classes.Address

        u1, a1 = transient_fixture()
        a2, a3, a4, a5 = (
            Address(email_address="a2"),
            Address(email_address="a3"),
            Address(email_address="a4"),
            Address(email_address="a5"),
        )

        u1.addresses = [a1, a2]
        u1.addresses = [a2, a3, a4, a5]

        self._assert_history(u1, ([a2, a3, a4, a5], [], []))

    @testing.combinations(True, False, argnames="autoflush")
    def test_replace_persistent(self, autoflush, persistent_fixture):
        User = self.classes.User
        Address = self.classes.Address

        u1, a1, s = persistent_fixture(autoflush=autoflush)
        a2, a3, a4, a5 = (
            Address(email_address="a2"),
            Address(email_address="a3"),
            Address(email_address="a4"),
            Address(email_address="a5"),
        )

        if User.addresses.property.lazy == "write_only":
            with self._expect_no_iteration():
                u1.addresses = [a1, a2]
            return

        u1.addresses = [a1, a2]
        u1.addresses = [a2, a3, a4, a5]

        if not autoflush:
            self._assert_history(u1, ([a2, a3, a4, a5], [], []))
        else:
            self._assert_history(
                u1,
                ([a3, a4, a5], [a2], [a1]),
                compare_passive=([a3, a4, a5], [], [a1]),
            )

    @testing.combinations(True, False, argnames="autoflush")
    def test_persistent_but_readded(self, autoflush, persistent_fixture):
        u1, a1, s = persistent_fixture(autoflush=autoflush)
        u1.addresses.add(a1)
        s.flush()

        u1.addresses.add(a1)

        self._assert_history(
            u1, ([], [a1], []), compare_passive=([a1], [], [])
        )

    def test_missing_but_removed_noflush(self, persistent_fixture):
        u1, a1, s = persistent_fixture(autoflush=False)

        u1.addresses.remove(a1)

        self._assert_history(u1, ([], [], []), compare_passive=([], [], [a1]))


class DynamicHistoryTest(_DynamicFixture, _HistoryTest, _fixtures.FixtureTest):
    run_inserts = None


class WriteOnlyHistoryTest(_WriteOnlyFixture, DynamicHistoryTest):
    pass
