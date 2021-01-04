import copy

from sqlalchemy import exc as sa_exc
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import util
from sqlalchemy.orm import attributes
from sqlalchemy.orm import backref
from sqlalchemy.orm import class_mapper
from sqlalchemy.orm import configure_mappers
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy.orm import foreign
from sqlalchemy.orm import mapper
from sqlalchemy.orm import object_mapper
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm import util as orm_util
from sqlalchemy.orm.attributes import instance_state
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import in_
from sqlalchemy.testing import not_in
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from test.orm import _fixtures


class CascadeArgTest(fixtures.MappedTest):
    run_inserts = None
    run_create_tables = None
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(30), nullable=False),
        )
        Table(
            "addresses",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("user_id", Integer, ForeignKey("users.id")),
            Column("email_address", String(50), nullable=False),
        )

    @classmethod
    def setup_classes(cls):
        class User(cls.Basic):
            pass

        class Address(cls.Basic):
            pass

    def test_delete_with_passive_deletes_all(self):
        User, Address = self.classes.User, self.classes.Address
        users, addresses = self.tables.users, self.tables.addresses

        mapper(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address,
                    passive_deletes="all",
                    cascade="all, delete-orphan",
                )
            },
        )
        mapper(Address, addresses)
        assert_raises_message(
            sa_exc.ArgumentError,
            "On User.addresses, can't set passive_deletes='all' "
            "in conjunction with 'delete' or 'delete-orphan' cascade",
            configure_mappers,
        )

    def test_delete_orphan_without_delete(self):
        Address = self.classes.Address

        assert_raises_message(
            sa_exc.SAWarning,
            "The 'delete-orphan' cascade option requires 'delete'.",
            relationship,
            Address,
            cascade="save-update, delete-orphan",
        )

    def test_bad_cascade(self):
        addresses, Address = self.tables.addresses, self.classes.Address

        mapper(Address, addresses)
        assert_raises_message(
            sa_exc.ArgumentError,
            r"Invalid cascade option\(s\): 'fake', 'fake2'",
            relationship,
            Address,
            cascade="fake, all, delete-orphan, fake2",
        )

    def test_cascade_repr(self):
        eq_(
            repr(orm_util.CascadeOptions("all, delete-orphan")),
            "CascadeOptions('delete,delete-orphan,expunge,"
            "merge,refresh-expire,save-update')",
        )

    def test_cascade_immutable(self):
        assert isinstance(
            orm_util.CascadeOptions("all, delete-orphan"), frozenset
        )

    def test_cascade_deepcopy(self):
        old = orm_util.CascadeOptions("all, delete-orphan")
        new = copy.deepcopy(old)
        eq_(old, new)

    def test_cascade_assignable(self):
        User, Address = self.classes.User, self.classes.Address
        users, addresses = self.tables.users, self.tables.addresses

        rel = relationship(Address)
        eq_(rel.cascade, set(["save-update", "merge"]))
        rel.cascade = "save-update, merge, expunge"
        eq_(rel.cascade, set(["save-update", "merge", "expunge"]))

        mapper(User, users, properties={"addresses": rel})
        am = mapper(Address, addresses)
        configure_mappers()

        eq_(rel.cascade, set(["save-update", "merge", "expunge"]))

        assert ("addresses", User) not in am._delete_orphans
        rel.cascade = "all, delete, delete-orphan"
        assert ("addresses", User) in am._delete_orphans

        eq_(
            rel.cascade,
            set(
                [
                    "delete",
                    "delete-orphan",
                    "expunge",
                    "merge",
                    "refresh-expire",
                    "save-update",
                ]
            ),
        )

    def test_cascade_unicode(self):
        Address = self.classes.Address

        rel = relationship(Address)
        rel.cascade = util.u("save-update, merge, expunge")
        eq_(rel.cascade, set(["save-update", "merge", "expunge"]))


class O2MCascadeDeleteOrphanTest(fixtures.MappedTest):
    run_inserts = None

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(30), nullable=False),
        )
        Table(
            "addresses",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("user_id", Integer, ForeignKey("users.id")),
            Column("email_address", String(50), nullable=False),
        )
        Table(
            "orders",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("user_id", Integer, ForeignKey("users.id"), nullable=False),
            Column("description", String(30)),
        )
        Table(
            "dingalings",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("address_id", Integer, ForeignKey("addresses.id")),
            Column("data", String(30)),
        )

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            pass

        class Address(cls.Comparable):
            pass

        class Order(cls.Comparable):
            pass

        class Dingaling(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        (
            users,
            Dingaling,
            Order,
            User,
            dingalings,
            Address,
            orders,
            addresses,
        ) = (
            cls.tables.users,
            cls.classes.Dingaling,
            cls.classes.Order,
            cls.classes.User,
            cls.tables.dingalings,
            cls.classes.Address,
            cls.tables.orders,
            cls.tables.addresses,
        )

        mapper(Address, addresses)
        mapper(Order, orders)
        mapper(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address, cascade="all, delete-orphan", backref="user"
                ),
                "orders": relationship(
                    Order, cascade="all, delete-orphan", order_by=orders.c.id
                ),
            },
        )

        mapper(
            Dingaling,
            dingalings,
            properties={"address": relationship(Address)},
        )

    def test_list_assignment_new(self):
        User, Order = self.classes.User, self.classes.Order

        with fixture_session() as sess:
            u = User(
                name="jack",
                orders=[
                    Order(description="order 1"),
                    Order(description="order 2"),
                ],
            )
            sess.add(u)
            sess.commit()

            eq_(
                u,
                User(
                    name="jack",
                    orders=[
                        Order(description="order 1"),
                        Order(description="order 2"),
                    ],
                ),
            )

    def test_list_assignment_replace(self):
        User, Order = self.classes.User, self.classes.Order

        with fixture_session() as sess:
            u = User(
                name="jack",
                orders=[
                    Order(description="someorder"),
                    Order(description="someotherorder"),
                ],
            )
            sess.add(u)

            u.orders = [
                Order(description="order 3"),
                Order(description="order 4"),
            ]
            sess.commit()

            eq_(
                u,
                User(
                    name="jack",
                    orders=[
                        Order(description="order 3"),
                        Order(description="order 4"),
                    ],
                ),
            )

            # order 1, order 2 have been deleted
            eq_(
                sess.query(Order).order_by(Order.id).all(),
                [Order(description="order 3"), Order(description="order 4")],
            )

    def test_standalone_orphan(self):
        Order = self.classes.Order

        with fixture_session() as sess:
            o5 = Order(description="order 5")
            sess.add(o5)
            assert_raises(sa_exc.DBAPIError, sess.flush)

    def test_save_update_sends_pending(self):
        """test that newly added and deleted collection items are
        cascaded on save-update"""

        Order, User = self.classes.Order, self.classes.User

        sess = fixture_session(expire_on_commit=False)
        o1, o2, o3 = (
            Order(description="o1"),
            Order(description="o2"),
            Order(description="o3"),
        )
        u = User(name="jack", orders=[o1, o2])
        sess.add(u)
        sess.commit()
        sess.close()
        u.orders.append(o3)
        u.orders.remove(o1)
        sess.add(u)
        assert o1 in sess
        assert o2 in sess
        assert o3 in sess
        sess.commit()

    def test_remove_pending_from_collection(self):
        User, Order = self.classes.User, self.classes.Order

        with fixture_session() as sess:

            u = User(name="jack")
            sess.add(u)
            sess.commit()

            o1 = Order()
            u.orders.append(o1)
            assert o1 in sess
            u.orders.remove(o1)
            assert o1 not in sess

    def test_remove_pending_from_pending_parent(self):
        # test issue #4040

        User, Order = self.classes.User, self.classes.Order

        with fixture_session() as sess:

            u = User(name="jack")

            o1 = Order()
            sess.add(o1)

            # object becomes an orphan, but parent is not in session
            u.orders.append(o1)
            u.orders.remove(o1)

            sess.add(u)

            assert o1 in sess

            sess.flush()

            assert o1 not in sess

    def test_delete(self):
        User, users, orders, Order = (
            self.classes.User,
            self.tables.users,
            self.tables.orders,
            self.classes.Order,
        )

        with fixture_session() as sess:
            u = User(
                name="jack",
                orders=[
                    Order(description="someorder"),
                    Order(description="someotherorder"),
                ],
            )
            sess.add(u)
            sess.flush()

            sess.delete(u)
            sess.flush()
            eq_(
                sess.execute(
                    select(func.count("*")).select_from(users)
                ).scalar(),
                0,
            )
            eq_(
                sess.execute(
                    select(func.count("*")).select_from(orders)
                ).scalar(),
                0,
            )

    def test_delete_unloaded_collections(self):
        """Unloaded collections are still included in a delete-cascade
        by default."""

        User, addresses, users, Address = (
            self.classes.User,
            self.tables.addresses,
            self.tables.users,
            self.classes.Address,
        )

        with fixture_session() as sess:
            u = User(
                name="jack",
                addresses=[
                    Address(email_address="address1"),
                    Address(email_address="address2"),
                ],
            )
            sess.add(u)
            sess.flush()
            sess.expunge_all()
            eq_(
                sess.execute(
                    select(func.count("*")).select_from(addresses)
                ).scalar(),
                2,
            )
            eq_(
                sess.execute(
                    select(func.count("*")).select_from(users)
                ).scalar(),
                1,
            )

            u = sess.get(User, u.id)

            assert "addresses" not in u.__dict__
            sess.delete(u)
            sess.flush()
            eq_(
                sess.execute(
                    select(func.count("*")).select_from(addresses)
                ).scalar(),
                0,
            )
            eq_(
                sess.execute(
                    select(func.count("*")).select_from(users)
                ).scalar(),
                0,
            )

    def test_cascades_onlycollection(self):
        """Cascade only reaches instances that are still part of the
        collection, not those that have been removed"""

        User, Order, users, orders = (
            self.classes.User,
            self.classes.Order,
            self.tables.users,
            self.tables.orders,
        )

        with fixture_session(autoflush=False) as sess:
            u = User(
                name="jack",
                orders=[
                    Order(description="someorder"),
                    Order(description="someotherorder"),
                ],
            )
            sess.add(u)
            sess.flush()

            o = u.orders[0]
            del u.orders[0]
            sess.delete(u)
            assert u in sess.deleted
            assert o not in sess.deleted
            assert o in sess

            u2 = User(name="newuser", orders=[o])
            sess.add(u2)
            sess.flush()
            sess.expunge_all()
            eq_(
                sess.execute(
                    select(func.count("*")).select_from(users)
                ).scalar(),
                1,
            )
            eq_(
                sess.execute(
                    select(func.count("*")).select_from(orders)
                ).scalar(),
                1,
            )
            eq_(
                sess.query(User).all(),
                [
                    User(
                        name="newuser", orders=[Order(description="someorder")]
                    )
                ],
            )

    def test_cascade_nosideeffects(self):
        """test that cascade leaves the state of unloaded
        scalars/collections unchanged."""

        Dingaling, User, Address = (
            self.classes.Dingaling,
            self.classes.User,
            self.classes.Address,
        )

        sess = fixture_session()
        u = User(name="jack")
        sess.add(u)
        assert "orders" not in u.__dict__

        sess.flush()

        assert "orders" not in u.__dict__

        a = Address(email_address="foo@bar.com")
        sess.add(a)
        assert "user" not in a.__dict__
        a.user = u
        sess.flush()

        d = Dingaling(data="d1")
        d.address_id = a.id
        sess.add(d)
        assert "address" not in d.__dict__
        sess.flush()
        assert d.address is a

    def test_cascade_delete_plusorphans(self):
        User, users, orders, Order = (
            self.classes.User,
            self.tables.users,
            self.tables.orders,
            self.classes.Order,
        )

        sess = fixture_session()
        u = User(
            name="jack",
            orders=[
                Order(description="someorder"),
                Order(description="someotherorder"),
            ],
        )
        sess.add(u)
        sess.flush()
        eq_(
            sess.execute(select(func.count("*")).select_from(users)).scalar(),
            1,
        )
        eq_(
            sess.execute(select(func.count("*")).select_from(orders)).scalar(),
            2,
        )

        del u.orders[0]
        sess.delete(u)
        sess.flush()
        eq_(
            sess.execute(select(func.count("*")).select_from(users)).scalar(),
            0,
        )
        eq_(
            sess.execute(select(func.count("*")).select_from(orders)).scalar(),
            0,
        )

    def test_collection_orphans(self):
        User, users, orders, Order = (
            self.classes.User,
            self.tables.users,
            self.tables.orders,
            self.classes.Order,
        )

        with fixture_session() as sess:
            u = User(
                name="jack",
                orders=[
                    Order(description="someorder"),
                    Order(description="someotherorder"),
                ],
            )
            sess.add(u)
            sess.flush()

            eq_(
                sess.execute(
                    select(func.count("*")).select_from(users)
                ).scalar(),
                1,
            )
            eq_(
                sess.execute(
                    select(func.count("*")).select_from(orders)
                ).scalar(),
                2,
            )

            u.orders[:] = []

            sess.flush()

            eq_(
                sess.execute(
                    select(func.count("*")).select_from(users)
                ).scalar(),
                1,
            )
            eq_(
                sess.execute(
                    select(func.count("*")).select_from(orders)
                ).scalar(),
                0,
            )


class O2MCascadeTest(fixtures.MappedTest):
    run_inserts = None

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(30), nullable=False),
        )
        Table(
            "addresses",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("user_id", Integer, ForeignKey("users.id")),
            Column("email_address", String(50), nullable=False),
        )

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            pass

        class Address(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        users, User, Address, addresses = (
            cls.tables.users,
            cls.classes.User,
            cls.classes.Address,
            cls.tables.addresses,
        )

        mapper(Address, addresses)
        mapper(
            User,
            users,
            properties={"addresses": relationship(Address, backref="user")},
        )

    def test_none_o2m_collection_assignment(self):
        User = self.classes.User
        s = fixture_session()
        u1 = User(name="u", addresses=[None])
        s.add(u1)
        eq_(u1.addresses, [None])
        assert_raises_message(
            orm_exc.FlushError,
            "Can't flush None value found in collection User.addresses",
            s.commit,
        )
        eq_(u1.addresses, [None])

    def test_none_o2m_collection_append(self):
        User = self.classes.User
        s = fixture_session()

        u1 = User(name="u")
        s.add(u1)
        u1.addresses.append(None)
        eq_(u1.addresses, [None])
        assert_raises_message(
            orm_exc.FlushError,
            "Can't flush None value found in collection User.addresses",
            s.commit,
        )
        eq_(u1.addresses, [None])


class O2MCascadeDeleteNoOrphanTest(fixtures.MappedTest):
    run_inserts = None

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(30)),
        )
        Table(
            "orders",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("user_id", Integer, ForeignKey("users.id")),
            Column("description", String(30)),
        )

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            pass

        class Order(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        User, Order, orders, users = (
            cls.classes.User,
            cls.classes.Order,
            cls.tables.orders,
            cls.tables.users,
        )

        mapper(
            User,
            users,
            properties=dict(
                orders=relationship(mapper(Order, orders), cascade="all")
            ),
        )

    def test_cascade_delete_noorphans(self):
        User, Order, orders, users = (
            self.classes.User,
            self.classes.Order,
            self.tables.orders,
            self.tables.users,
        )

        with fixture_session() as sess:
            u = User(
                name="jack",
                orders=[
                    Order(description="someorder"),
                    Order(description="someotherorder"),
                ],
            )
            sess.add(u)
            sess.flush()
            eq_(
                sess.execute(
                    select(func.count("*")).select_from(users)
                ).scalar(),
                1,
            )
            eq_(
                sess.execute(
                    select(func.count("*")).select_from(orders)
                ).scalar(),
                2,
            )

            del u.orders[0]
            sess.delete(u)
            sess.flush()
            eq_(
                sess.execute(
                    select(func.count("*")).select_from(users)
                ).scalar(),
                0,
            )
            eq_(
                sess.execute(
                    select(func.count("*")).select_from(orders)
                ).scalar(),
                1,
            )


class O2OSingleParentTest(_fixtures.FixtureTest):
    run_inserts = None

    @classmethod
    def setup_mappers(cls):
        Address, addresses, users, User = (
            cls.classes.Address,
            cls.tables.addresses,
            cls.tables.users,
            cls.classes.User,
        )

        mapper(Address, addresses)
        mapper(
            User,
            users,
            properties={
                "address": relationship(
                    Address,
                    backref=backref("user", single_parent=True),
                    uselist=False,
                )
            },
        )

    def test_single_parent_raise(self):
        User, Address = self.classes.User, self.classes.Address

        a1 = Address(email_address="some address")
        u1 = User(name="u1", address=a1)
        assert_raises(
            sa_exc.InvalidRequestError, Address, email_address="asd", user=u1
        )
        a2 = Address(email_address="asd")
        u1.address = a2
        assert u1.address is not a1
        assert a1.user is None


class O2OSingleParentNoFlushTest(fixtures.MappedTest):
    run_inserts = None

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(30), nullable=False),
        )

        Table(
            "addresses",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("user_id", None, ForeignKey("users.id"), nullable=False),
            Column("email_address", String(50), nullable=False),
        )

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            pass

        class Address(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        Address, addresses, users, User = (
            cls.classes.Address,
            cls.tables.addresses,
            cls.tables.users,
            cls.classes.User,
        )

        mapper(Address, addresses)
        mapper(
            User,
            users,
            properties={
                "address": relationship(
                    Address,
                    backref=backref(
                        "user",
                        single_parent=True,
                        cascade="all, delete-orphan",
                    ),
                    uselist=False,
                )
            },
        )

    def test_replace_attribute_no_flush(self):
        # test [ticket:2921]

        User, Address = self.classes.User, self.classes.Address
        a1 = Address(email_address="some address")
        u1 = User(name="u1", address=a1)
        sess = fixture_session()
        sess.add(u1)
        sess.commit()

        # in this case, u1.address has active history set, because
        # this operation necessarily replaces the old object which must be
        # loaded.
        # the set operation requires that "u1" is unexpired, because the
        # replace operation wants to load the
        # previous value.  The original test case for #2921 only included
        # that the lazyload operation passed a no autoflush flag through
        # to the operation, however in #5226 this has been enhanced to pass
        # the no autoflush flag down through to the unexpire of the attributes
        # as well, so that attribute unexpire can otherwise invoke autoflush.
        assert "id" not in u1.__dict__
        a2 = Address(email_address="asdf")
        sess.add(a2)
        u1.address = a2


class NoSaveCascadeFlushTest(_fixtures.FixtureTest):
    """Test related item not present in session, commit proceeds."""

    run_inserts = None

    def _one_to_many_fixture(
        self,
        o2m_cascade=True,
        m2o_cascade=True,
        o2m=False,
        m2o=False,
        o2m_cascade_backrefs=True,
        m2o_cascade_backrefs=True,
    ):

        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        if o2m:
            if m2o:
                addresses_rel = {
                    "addresses": relationship(
                        Address,
                        cascade_backrefs=o2m_cascade_backrefs,
                        cascade=o2m_cascade and "save-update" or "",
                        backref=backref(
                            "user",
                            cascade=m2o_cascade and "save-update" or "",
                            cascade_backrefs=m2o_cascade_backrefs,
                        ),
                    )
                }

            else:
                addresses_rel = {
                    "addresses": relationship(
                        Address,
                        cascade=o2m_cascade and "save-update" or "",
                        cascade_backrefs=o2m_cascade_backrefs,
                    )
                }
            user_rel = {}
        elif m2o:
            user_rel = {
                "user": relationship(
                    User,
                    cascade=m2o_cascade and "save-update" or "",
                    cascade_backrefs=m2o_cascade_backrefs,
                )
            }
            addresses_rel = {}
        else:
            addresses_rel = {}
            user_rel = {}

        mapper(User, users, properties=addresses_rel)
        mapper(Address, addresses, properties=user_rel)

    def _many_to_many_fixture(
        self,
        fwd_cascade=True,
        bkd_cascade=True,
        fwd=False,
        bkd=False,
        fwd_cascade_backrefs=True,
        bkd_cascade_backrefs=True,
    ):

        keywords, items, item_keywords, Keyword, Item = (
            self.tables.keywords,
            self.tables.items,
            self.tables.item_keywords,
            self.classes.Keyword,
            self.classes.Item,
        )

        if fwd:
            if bkd:
                keywords_rel = {
                    "keywords": relationship(
                        Keyword,
                        secondary=item_keywords,
                        cascade_backrefs=fwd_cascade_backrefs,
                        cascade=fwd_cascade and "save-update" or "",
                        backref=backref(
                            "items",
                            cascade=bkd_cascade and "save-update" or "",
                            cascade_backrefs=bkd_cascade_backrefs,
                        ),
                    )
                }

            else:
                keywords_rel = {
                    "keywords": relationship(
                        Keyword,
                        secondary=item_keywords,
                        cascade=fwd_cascade and "save-update" or "",
                        cascade_backrefs=fwd_cascade_backrefs,
                    )
                }
            items_rel = {}
        elif bkd:
            items_rel = {
                "items": relationship(
                    Item,
                    secondary=item_keywords,
                    cascade=bkd_cascade and "save-update" or "",
                    cascade_backrefs=bkd_cascade_backrefs,
                )
            }
            keywords_rel = {}
        else:
            keywords_rel = {}
            items_rel = {}

        mapper(Item, items, properties=keywords_rel)
        mapper(Keyword, keywords, properties=items_rel)

    def test_o2m_only_child_pending(self):
        User, Address = self.classes.User, self.classes.Address

        self._one_to_many_fixture(o2m=True, m2o=False)
        sess = fixture_session()
        u1 = User(name="u1")
        a1 = Address(email_address="a1")
        u1.addresses.append(a1)
        sess.add(u1)
        assert u1 in sess
        assert a1 in sess
        sess.flush()

    def test_o2m_only_child_transient(self):
        User, Address = self.classes.User, self.classes.Address

        self._one_to_many_fixture(o2m=True, m2o=False, o2m_cascade=False)
        sess = fixture_session()
        u1 = User(name="u1")
        a1 = Address(email_address="a1")
        u1.addresses.append(a1)
        sess.add(u1)
        assert u1 in sess
        assert a1 not in sess
        assert_raises_message(sa_exc.SAWarning, "not in session", sess.flush)

    def test_o2m_only_child_persistent(self):
        User, Address = self.classes.User, self.classes.Address

        self._one_to_many_fixture(o2m=True, m2o=False, o2m_cascade=False)
        sess = fixture_session()
        u1 = User(name="u1")
        a1 = Address(email_address="a1")
        sess.add(a1)
        sess.flush()

        sess.expunge_all()

        u1.addresses.append(a1)
        sess.add(u1)
        assert u1 in sess
        assert a1 not in sess
        assert_raises_message(sa_exc.SAWarning, "not in session", sess.flush)

    def test_o2m_backref_child_pending(self):
        User, Address = self.classes.User, self.classes.Address

        self._one_to_many_fixture(o2m=True, m2o=True)
        sess = fixture_session()
        u1 = User(name="u1")
        a1 = Address(email_address="a1")
        u1.addresses.append(a1)
        sess.add(u1)
        assert u1 in sess
        assert a1 in sess
        sess.flush()

    def test_o2m_backref_child_transient(self):
        User, Address = self.classes.User, self.classes.Address

        self._one_to_many_fixture(o2m=True, m2o=True, o2m_cascade=False)
        sess = fixture_session()
        u1 = User(name="u1")
        a1 = Address(email_address="a1")
        u1.addresses.append(a1)
        sess.add(u1)
        assert u1 in sess
        assert a1 not in sess
        assert_raises_message(sa_exc.SAWarning, "not in session", sess.flush)

    def test_o2m_backref_child_transient_nochange(self):
        User, Address = self.classes.User, self.classes.Address

        self._one_to_many_fixture(o2m=True, m2o=True, o2m_cascade=False)
        sess = fixture_session()
        u1 = User(name="u1")
        a1 = Address(email_address="a1")
        u1.addresses.append(a1)
        sess.add(u1)
        assert u1 in sess
        assert a1 not in sess

        @testing.emits_warning(r".*not in session")
        def go():
            sess.commit()

        go()
        eq_(u1.addresses, [])

    def test_o2m_backref_child_expunged(self):
        User, Address = self.classes.User, self.classes.Address

        self._one_to_many_fixture(o2m=True, m2o=True, o2m_cascade=False)
        sess = fixture_session()
        u1 = User(name="u1")
        a1 = Address(email_address="a1")
        sess.add(a1)
        sess.flush()

        sess.add(u1)
        u1.addresses.append(a1)
        sess.expunge(a1)
        assert u1 in sess
        assert a1 not in sess
        assert_raises_message(sa_exc.SAWarning, "not in session", sess.flush)

    def test_o2m_backref_child_expunged_nochange(self):
        User, Address = self.classes.User, self.classes.Address

        self._one_to_many_fixture(o2m=True, m2o=True, o2m_cascade=False)
        sess = fixture_session()
        u1 = User(name="u1")
        a1 = Address(email_address="a1")
        sess.add(a1)
        sess.flush()

        sess.add(u1)
        u1.addresses.append(a1)
        sess.expunge(a1)
        assert u1 in sess
        assert a1 not in sess

        @testing.emits_warning(r".*not in session")
        def go():
            sess.commit()

        go()
        eq_(u1.addresses, [])

    def test_m2o_only_child_pending(self):
        User, Address = self.classes.User, self.classes.Address

        self._one_to_many_fixture(o2m=False, m2o=True)
        sess = fixture_session()
        u1 = User(name="u1")
        a1 = Address(email_address="a1")
        a1.user = u1
        sess.add(a1)
        assert u1 in sess
        assert a1 in sess
        sess.flush()

    def test_m2o_only_child_transient(self):
        User, Address = self.classes.User, self.classes.Address

        self._one_to_many_fixture(o2m=False, m2o=True, m2o_cascade=False)
        sess = fixture_session()
        u1 = User(name="u1")
        a1 = Address(email_address="a1")
        a1.user = u1
        sess.add(a1)
        assert u1 not in sess
        assert a1 in sess
        assert_raises_message(sa_exc.SAWarning, "not in session", sess.flush)

    def test_m2o_only_child_expunged(self):
        User, Address = self.classes.User, self.classes.Address

        self._one_to_many_fixture(o2m=False, m2o=True, m2o_cascade=False)
        sess = fixture_session()
        u1 = User(name="u1")
        sess.add(u1)
        sess.flush()

        a1 = Address(email_address="a1")
        a1.user = u1
        sess.add(a1)
        sess.expunge(u1)
        assert u1 not in sess
        assert a1 in sess
        assert_raises_message(sa_exc.SAWarning, "not in session", sess.flush)

    def test_m2o_backref_child_pending(self):
        User, Address = self.classes.User, self.classes.Address

        self._one_to_many_fixture(o2m=True, m2o=True)
        sess = fixture_session()
        u1 = User(name="u1")
        a1 = Address(email_address="a1")
        a1.user = u1
        sess.add(a1)
        assert u1 in sess
        assert a1 in sess
        sess.flush()

    def test_m2o_backref_child_transient(self):
        User, Address = self.classes.User, self.classes.Address

        self._one_to_many_fixture(o2m=True, m2o=True, m2o_cascade=False)
        sess = fixture_session()
        u1 = User(name="u1")
        a1 = Address(email_address="a1")
        a1.user = u1
        sess.add(a1)
        assert u1 not in sess
        assert a1 in sess
        assert_raises_message(sa_exc.SAWarning, "not in session", sess.flush)

    def test_m2o_backref_child_expunged(self):
        User, Address = self.classes.User, self.classes.Address

        self._one_to_many_fixture(o2m=True, m2o=True, m2o_cascade=False)
        with fixture_session() as sess:
            u1 = User(name="u1")
            sess.add(u1)
            sess.flush()

            a1 = Address(email_address="a1")
            with testing.expect_deprecated(
                '"Address" object is being merged into a Session along '
                'the backref cascade path for relationship "User.addresses"'
            ):
                a1.user = u1
            sess.add(a1)
            sess.expunge(u1)
            assert u1 not in sess
            assert a1 in sess
            assert_raises_message(
                sa_exc.SAWarning, "not in session", sess.flush
            )

    def test_m2o_backref_future_child_expunged(self):
        User, Address = self.classes.User, self.classes.Address

        self._one_to_many_fixture(o2m=True, m2o=True, m2o_cascade=False)
        with Session(testing.db, future=True) as sess:
            u1 = User(name="u1")
            sess.add(u1)
            sess.flush()

            a1 = Address(email_address="a1")
            a1.user = u1
            assert a1 not in sess
            sess.add(a1)
            sess.expunge(u1)
            assert u1 not in sess
            assert a1 in sess
            assert_raises_message(
                sa_exc.SAWarning, "not in session", sess.flush
            )

    def test_m2o_backref_child_pending_nochange(self):
        User, Address = self.classes.User, self.classes.Address

        self._one_to_many_fixture(o2m=True, m2o=True, m2o_cascade=False)
        sess = fixture_session()
        u1 = User(name="u1")

        a1 = Address(email_address="a1")
        a1.user = u1
        sess.add(a1)
        assert u1 not in sess
        assert a1 in sess

        @testing.emits_warning(r".*not in session")
        def go():
            sess.commit()

        go()
        # didn't get flushed
        assert a1.user is None

    def test_m2o_backref_child_expunged_nochange(self):
        User, Address = self.classes.User, self.classes.Address

        self._one_to_many_fixture(o2m=True, m2o=True, m2o_cascade=False)

        with fixture_session() as sess:
            u1 = User(name="u1")
            sess.add(u1)
            sess.flush()

            a1 = Address(email_address="a1")
            with testing.expect_deprecated(
                '"Address" object is being merged into a Session along the '
                'backref cascade path for relationship "User.addresses"'
            ):
                a1.user = u1
            sess.add(a1)
            sess.expunge(u1)
            assert u1 not in sess
            assert a1 in sess

            @testing.emits_warning(r".*not in session")
            def go():
                sess.commit()

            go()
            # didn't get flushed
            assert a1.user is None

    def test_m2o_backref_future_child_expunged_nochange(self):
        User, Address = self.classes.User, self.classes.Address

        self._one_to_many_fixture(o2m=True, m2o=True, m2o_cascade=False)

        with Session(testing.db, future=True) as sess:
            u1 = User(name="u1")
            sess.add(u1)
            sess.flush()

            a1 = Address(email_address="a1")
            a1.user = u1
            assert a1 not in sess
            sess.add(a1)
            sess.expunge(u1)
            assert u1 not in sess
            assert a1 in sess

            @testing.emits_warning(r".*not in session")
            def go():
                sess.commit()

            go()
            # didn't get flushed
            assert a1.user is None

    def test_m2m_only_child_pending(self):
        Item, Keyword = self.classes.Item, self.classes.Keyword

        self._many_to_many_fixture(fwd=True, bkd=False)
        sess = fixture_session()
        i1 = Item(description="i1")
        k1 = Keyword(name="k1")
        i1.keywords.append(k1)
        sess.add(i1)
        assert i1 in sess
        assert k1 in sess
        sess.flush()

    def test_m2m_only_child_transient(self):
        Item, Keyword = self.classes.Item, self.classes.Keyword

        self._many_to_many_fixture(fwd=True, bkd=False, fwd_cascade=False)
        sess = fixture_session()
        i1 = Item(description="i1")
        k1 = Keyword(name="k1")
        i1.keywords.append(k1)
        sess.add(i1)
        assert i1 in sess
        assert k1 not in sess
        assert_raises_message(sa_exc.SAWarning, "not in session", sess.flush)

    def test_m2m_only_child_persistent(self):
        Item, Keyword = self.classes.Item, self.classes.Keyword

        self._many_to_many_fixture(fwd=True, bkd=False, fwd_cascade=False)
        sess = fixture_session()
        i1 = Item(description="i1")
        k1 = Keyword(name="k1")
        sess.add(k1)
        sess.flush()

        sess.expunge_all()

        i1.keywords.append(k1)
        sess.add(i1)
        assert i1 in sess
        assert k1 not in sess
        assert_raises_message(sa_exc.SAWarning, "not in session", sess.flush)

    def test_m2m_backref_child_pending(self):
        Item, Keyword = self.classes.Item, self.classes.Keyword

        self._many_to_many_fixture(fwd=True, bkd=True)
        sess = fixture_session()
        i1 = Item(description="i1")
        k1 = Keyword(name="k1")
        i1.keywords.append(k1)
        sess.add(i1)
        assert i1 in sess
        assert k1 in sess
        sess.flush()

    def test_m2m_backref_child_transient(self):
        Item, Keyword = self.classes.Item, self.classes.Keyword

        self._many_to_many_fixture(fwd=True, bkd=True, fwd_cascade=False)
        sess = fixture_session()
        i1 = Item(description="i1")
        k1 = Keyword(name="k1")
        i1.keywords.append(k1)
        sess.add(i1)
        assert i1 in sess
        assert k1 not in sess
        assert_raises_message(sa_exc.SAWarning, "not in session", sess.flush)

    def test_m2m_backref_child_transient_nochange(self):
        Item, Keyword = self.classes.Item, self.classes.Keyword

        self._many_to_many_fixture(fwd=True, bkd=True, fwd_cascade=False)
        sess = fixture_session()
        i1 = Item(description="i1")
        k1 = Keyword(name="k1")
        i1.keywords.append(k1)
        sess.add(i1)
        assert i1 in sess
        assert k1 not in sess

        @testing.emits_warning(r".*not in session")
        def go():
            sess.commit()

        go()
        eq_(i1.keywords, [])

    def test_m2m_backref_child_expunged(self):
        Item, Keyword = self.classes.Item, self.classes.Keyword

        self._many_to_many_fixture(fwd=True, bkd=True, fwd_cascade=False)
        sess = fixture_session()
        i1 = Item(description="i1")
        k1 = Keyword(name="k1")
        sess.add(k1)
        sess.flush()

        sess.add(i1)
        i1.keywords.append(k1)
        sess.expunge(k1)
        assert i1 in sess
        assert k1 not in sess
        assert_raises_message(sa_exc.SAWarning, "not in session", sess.flush)

    def test_m2m_backref_child_expunged_nochange(self):
        Item, Keyword = self.classes.Item, self.classes.Keyword

        self._many_to_many_fixture(fwd=True, bkd=True, fwd_cascade=False)
        sess = fixture_session()
        i1 = Item(description="i1")
        k1 = Keyword(name="k1")
        sess.add(k1)
        sess.flush()

        sess.add(i1)
        i1.keywords.append(k1)
        sess.expunge(k1)
        assert i1 in sess
        assert k1 not in sess

        @testing.emits_warning(r".*not in session")
        def go():
            sess.commit()

        go()
        eq_(i1.keywords, [])


class NoSaveCascadeBackrefTest(_fixtures.FixtureTest):
    """test that backrefs don't force save-update cascades to occur
    when the cascade initiated from the forwards side."""

    def test_unidirectional_cascade_o2m(self):
        User, Order, users, orders = (
            self.classes.User,
            self.classes.Order,
            self.tables.users,
            self.tables.orders,
        )

        mapper(Order, orders)
        mapper(
            User,
            users,
            properties=dict(
                orders=relationship(
                    Order, backref=backref("user", cascade=None)
                )
            ),
        )

        sess = fixture_session()

        o1 = Order()
        sess.add(o1)
        u1 = User(orders=[o1])
        assert u1 not in sess
        assert o1 in sess

        sess.expunge_all()

        o1 = Order()
        u1 = User(orders=[o1])
        sess.add(o1)
        assert u1 not in sess
        assert o1 in sess

    def test_unidirectional_cascade_m2o(self):
        User, Order, users, orders = (
            self.classes.User,
            self.classes.Order,
            self.tables.users,
            self.tables.orders,
        )

        mapper(
            Order,
            orders,
            properties={
                "user": relationship(
                    User, backref=backref("orders", cascade=None)
                )
            },
        )
        mapper(User, users)

        sess = fixture_session()

        u1 = User()
        sess.add(u1)
        o1 = Order()
        o1.user = u1
        assert o1 not in sess
        assert u1 in sess

        sess.expunge_all()

        u1 = User()
        o1 = Order()
        o1.user = u1
        sess.add(u1)
        assert o1 not in sess
        assert u1 in sess

    def test_unidirectional_cascade_m2m(self):
        keywords, items, item_keywords, Keyword, Item = (
            self.tables.keywords,
            self.tables.items,
            self.tables.item_keywords,
            self.classes.Keyword,
            self.classes.Item,
        )

        mapper(
            Item,
            items,
            properties={
                "keywords": relationship(
                    Keyword,
                    secondary=item_keywords,
                    cascade="none",
                    backref="items",
                )
            },
        )
        mapper(Keyword, keywords)

        sess = fixture_session()

        i1 = Item()
        k1 = Keyword()
        sess.add(i1)
        i1.keywords.append(k1)
        assert i1 in sess
        assert k1 not in sess

        sess.expunge_all()

        i1 = Item()
        k1 = Keyword()
        sess.add(i1)
        k1.items.append(i1)
        assert i1 in sess
        assert k1 not in sess


class M2OCascadeDeleteOrphanTestOne(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "extra",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("prefs_id", Integer, ForeignKey("prefs.id")),
        )
        Table(
            "prefs",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(40)),
        )
        Table(
            "users",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(40)),
            Column("pref_id", Integer, ForeignKey("prefs.id")),
            Column("foo_id", Integer, ForeignKey("foo.id")),
        )
        Table(
            "foo",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(40)),
        )

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            pass

        class Pref(cls.Comparable):
            pass

        class Extra(cls.Comparable):
            pass

        class Foo(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        extra, foo, users, Extra, Pref, User, prefs, Foo = (
            cls.tables.extra,
            cls.tables.foo,
            cls.tables.users,
            cls.classes.Extra,
            cls.classes.Pref,
            cls.classes.User,
            cls.tables.prefs,
            cls.classes.Foo,
        )

        mapper(Extra, extra)
        mapper(
            Pref,
            prefs,
            properties=dict(extra=relationship(Extra, cascade="all, delete")),
        )
        mapper(
            User,
            users,
            properties=dict(
                pref=relationship(
                    Pref,
                    lazy="joined",
                    cascade="all, delete-orphan",
                    single_parent=True,
                ),
                foo=relationship(Foo),
            ),
        )  # straight m2o
        mapper(Foo, foo)

    @classmethod
    def insert_data(cls, connection):
        Pref, User, Extra = (
            cls.classes.Pref,
            cls.classes.User,
            cls.classes.Extra,
        )

        u1 = User(name="ed", pref=Pref(data="pref 1", extra=[Extra()]))
        u2 = User(name="jack", pref=Pref(data="pref 2", extra=[Extra()]))
        u3 = User(name="foo", pref=Pref(data="pref 3", extra=[Extra()]))
        sess = Session(connection)
        sess.add_all((u1, u2, u3))
        sess.flush()
        sess.close()

    def test_orphan(self):
        prefs, User, extra = (
            self.tables.prefs,
            self.classes.User,
            self.tables.extra,
        )

        sess = fixture_session()
        eq_(
            sess.execute(select(func.count("*")).select_from(prefs)).scalar(),
            3,
        )
        eq_(
            sess.execute(select(func.count("*")).select_from(extra)).scalar(),
            3,
        )
        jack = sess.query(User).filter_by(name="jack").one()
        jack.pref = None
        sess.flush()
        eq_(
            sess.execute(select(func.count("*")).select_from(prefs)).scalar(),
            2,
        )
        eq_(
            sess.execute(select(func.count("*")).select_from(extra)).scalar(),
            2,
        )

    def test_cascade_on_deleted(self):
        """test a bug introduced by r6711"""

        Foo, User = self.classes.Foo, self.classes.User

        sess = fixture_session(expire_on_commit=True)

        u1 = User(name="jack", foo=Foo(data="f1"))
        sess.add(u1)
        sess.commit()

        u1.foo = None

        # the error condition relies upon
        # these things being true
        assert User.foo.dispatch._active_history is False
        eq_(attributes.get_history(u1, "foo"), ([None], (), ()))

        sess.add(u1)
        assert u1 in sess
        sess.commit()

    def test_save_update_sends_pending(self):
        """test that newly added and deleted scalar items are cascaded
        on save-update"""

        Pref, User = self.classes.Pref, self.classes.User

        sess = fixture_session(expire_on_commit=False)
        p1, p2 = Pref(data="p1"), Pref(data="p2")

        u = User(name="jack", pref=p1)
        sess.add(u)
        sess.commit()
        sess.close()

        u.pref = p2

        sess.add(u)
        assert p1 in sess
        assert p2 in sess
        sess.commit()

    def test_orphan_on_update(self):
        prefs, User, extra = (
            self.tables.prefs,
            self.classes.User,
            self.tables.extra,
        )

        sess = fixture_session()
        jack = sess.query(User).filter_by(name="jack").one()
        p = jack.pref
        e = jack.pref.extra[0]
        sess.expunge_all()

        jack.pref = None
        sess.add(jack)
        sess.add(p)
        sess.add(e)
        assert p in sess
        assert e in sess
        sess.flush()
        eq_(
            sess.execute(select(func.count("*")).select_from(prefs)).scalar(),
            2,
        )
        eq_(
            sess.execute(select(func.count("*")).select_from(extra)).scalar(),
            2,
        )

    def test_pending_expunge(self):
        Pref, User = self.classes.Pref, self.classes.User

        sess = fixture_session()
        someuser = User(name="someuser")
        sess.add(someuser)
        sess.flush()
        someuser.pref = p1 = Pref(data="somepref")
        assert p1 in sess
        someuser.pref = Pref(data="someotherpref")
        assert p1 not in sess
        sess.flush()
        eq_(
            sess.query(Pref).with_parent(someuser).all(),
            [Pref(data="someotherpref")],
        )

    def test_double_assignment(self):
        """Double assignment will not accidentally reset the 'parent' flag."""

        Pref, User = self.classes.Pref, self.classes.User

        sess = fixture_session()
        jack = sess.query(User).filter_by(name="jack").one()

        newpref = Pref(data="newpref")
        jack.pref = newpref
        jack.pref = newpref
        sess.flush()
        eq_(
            sess.query(Pref).order_by(Pref.id).all(),
            [Pref(data="pref 1"), Pref(data="pref 3"), Pref(data="newpref")],
        )


class M2OCascadeDeleteOrphanTestTwo(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "t1",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(50)),
            Column("t2id", Integer, ForeignKey("t2.id")),
        )

        Table(
            "t2",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(50)),
            Column("t3id", Integer, ForeignKey("t3.id")),
        )

        Table(
            "t3",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(50)),
        )

    @classmethod
    def setup_classes(cls):
        class T1(cls.Comparable):
            pass

        class T2(cls.Comparable):
            pass

        class T3(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        t2, T2, T3, t1, t3, T1 = (
            cls.tables.t2,
            cls.classes.T2,
            cls.classes.T3,
            cls.tables.t1,
            cls.tables.t3,
            cls.classes.T1,
        )

        mapper(
            T1,
            t1,
            properties=dict(
                t2=relationship(
                    T2, cascade="all, delete-orphan", single_parent=True
                )
            ),
        )
        mapper(
            T2,
            t2,
            properties=dict(
                t3=relationship(
                    T3,
                    cascade="all, delete-orphan",
                    single_parent=True,
                    backref=backref("t2", uselist=False),
                )
            ),
        )
        mapper(T3, t3)

    def test_cascade_delete(self):
        T2, T3, T1 = (self.classes.T2, self.classes.T3, self.classes.T1)

        sess = fixture_session()
        x = T1(data="t1a", t2=T2(data="t2a", t3=T3(data="t3a")))
        sess.add(x)
        sess.flush()

        sess.delete(x)
        sess.flush()
        eq_(sess.query(T1).all(), [])
        eq_(sess.query(T2).all(), [])
        eq_(sess.query(T3).all(), [])

    def test_deletes_orphans_onelevel(self):
        T2, T3, T1 = (self.classes.T2, self.classes.T3, self.classes.T1)

        sess = fixture_session()
        x2 = T1(data="t1b", t2=T2(data="t2b", t3=T3(data="t3b")))
        sess.add(x2)
        sess.flush()
        x2.t2 = None

        sess.delete(x2)
        sess.flush()
        eq_(sess.query(T1).all(), [])
        eq_(sess.query(T2).all(), [])
        eq_(sess.query(T3).all(), [])

    def test_deletes_orphans_twolevel(self):
        T2, T3, T1 = (self.classes.T2, self.classes.T3, self.classes.T1)

        sess = fixture_session()
        x = T1(data="t1a", t2=T2(data="t2a", t3=T3(data="t3a")))
        sess.add(x)
        sess.flush()

        x.t2.t3 = None
        sess.delete(x)
        sess.flush()
        eq_(sess.query(T1).all(), [])
        eq_(sess.query(T2).all(), [])
        eq_(sess.query(T3).all(), [])

    def test_finds_orphans_twolevel(self):
        T2, T3, T1 = (self.classes.T2, self.classes.T3, self.classes.T1)

        sess = fixture_session()
        x = T1(data="t1a", t2=T2(data="t2a", t3=T3(data="t3a")))
        sess.add(x)
        sess.flush()

        x.t2.t3 = None
        sess.flush()
        eq_(sess.query(T1).all(), [T1()])
        eq_(sess.query(T2).all(), [T2()])
        eq_(sess.query(T3).all(), [])

    def test_single_parent_raise(self):
        T2, T1 = self.classes.T2, self.classes.T1

        y = T2(data="T2a")
        T1(data="T1a", t2=y)
        assert_raises(sa_exc.InvalidRequestError, T1, data="T1b", t2=y)

    def test_single_parent_backref(self):
        T2, T3 = self.classes.T2, self.classes.T3

        y = T3(data="T3a")
        x = T2(data="T2a", t3=y)

        # cant attach the T3 to another T2
        assert_raises(sa_exc.InvalidRequestError, T2, data="T2b", t3=y)

        # set via backref tho is OK, unsets from previous parent
        # first
        z = T2(data="T2b")
        y.t2 = z

        assert z.t3 is y
        assert x.t3 is None


class M2OCascadeDeleteNoOrphanTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "t1",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(50)),
            Column("t2id", Integer, ForeignKey("t2.id")),
        )

        Table(
            "t2",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(50)),
            Column("t3id", Integer, ForeignKey("t3.id")),
        )

        Table(
            "t3",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(50)),
        )

    @classmethod
    def setup_classes(cls):
        class T1(cls.Comparable):
            pass

        class T2(cls.Comparable):
            pass

        class T3(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        t2, T2, T3, t1, t3, T1 = (
            cls.tables.t2,
            cls.classes.T2,
            cls.classes.T3,
            cls.tables.t1,
            cls.tables.t3,
            cls.classes.T1,
        )

        mapper(T1, t1, properties={"t2": relationship(T2, cascade="all")})
        mapper(T2, t2, properties={"t3": relationship(T3, cascade="all")})
        mapper(T3, t3)

    def test_cascade_delete(self):
        T2, T3, T1 = (self.classes.T2, self.classes.T3, self.classes.T1)

        sess = fixture_session()
        x = T1(data="t1a", t2=T2(data="t2a", t3=T3(data="t3a")))
        sess.add(x)
        sess.flush()

        sess.delete(x)
        sess.flush()
        eq_(sess.query(T1).all(), [])
        eq_(sess.query(T2).all(), [])
        eq_(sess.query(T3).all(), [])

    def test_cascade_delete_postappend_onelevel(self):
        T2, T3, T1 = (self.classes.T2, self.classes.T3, self.classes.T1)

        sess = fixture_session()
        x1 = T1(data="t1")
        x2 = T2(data="t2")
        x3 = T3(data="t3")
        sess.add_all((x1, x2, x3))
        sess.flush()

        sess.delete(x1)
        x1.t2 = x2
        x2.t3 = x3
        sess.flush()
        eq_(sess.query(T1).all(), [])
        eq_(sess.query(T2).all(), [])
        eq_(sess.query(T3).all(), [])

    def test_cascade_delete_postappend_twolevel(self):
        T2, T3, T1 = (self.classes.T2, self.classes.T3, self.classes.T1)

        sess = fixture_session()
        x1 = T1(data="t1", t2=T2(data="t2"))
        x3 = T3(data="t3")
        sess.add_all((x1, x3))
        sess.flush()

        sess.delete(x1)
        x1.t2.t3 = x3
        sess.flush()
        eq_(sess.query(T1).all(), [])
        eq_(sess.query(T2).all(), [])
        eq_(sess.query(T3).all(), [])

    def test_preserves_orphans_onelevel(self):
        T2, T3, T1 = (self.classes.T2, self.classes.T3, self.classes.T1)

        sess = fixture_session()
        x2 = T1(data="t1b", t2=T2(data="t2b", t3=T3(data="t3b")))
        sess.add(x2)
        sess.flush()
        x2.t2 = None

        sess.delete(x2)
        sess.flush()
        eq_(sess.query(T1).all(), [])
        eq_(sess.query(T2).all(), [T2()])
        eq_(sess.query(T3).all(), [T3()])

    @testing.future
    def test_preserves_orphans_onelevel_postremove(self):
        T2, T3, T1 = (self.classes.T2, self.classes.T3, self.classes.T1)

        sess = fixture_session()
        x2 = T1(data="t1b", t2=T2(data="t2b", t3=T3(data="t3b")))
        sess.add(x2)
        sess.flush()

        sess.delete(x2)
        x2.t2 = None
        sess.flush()
        eq_(sess.query(T1).all(), [])
        eq_(sess.query(T2).all(), [T2()])
        eq_(sess.query(T3).all(), [T3()])

    def test_preserves_orphans_twolevel(self):
        T2, T3, T1 = (self.classes.T2, self.classes.T3, self.classes.T1)

        sess = fixture_session()
        x = T1(data="t1a", t2=T2(data="t2a", t3=T3(data="t3a")))
        sess.add(x)
        sess.flush()

        x.t2.t3 = None
        sess.delete(x)
        sess.flush()
        eq_(sess.query(T1).all(), [])
        eq_(sess.query(T2).all(), [])
        eq_(sess.query(T3).all(), [T3()])


class M2MCascadeTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "a",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(30)),
            test_needs_fk=True,
        )
        Table(
            "b",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(30)),
            test_needs_fk=True,
        )
        Table(
            "atob",
            metadata,
            Column("aid", Integer, ForeignKey("a.id")),
            Column("bid", Integer, ForeignKey("b.id")),
            test_needs_fk=True,
        )
        Table(
            "c",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(30)),
            Column("bid", Integer, ForeignKey("b.id")),
            test_needs_fk=True,
        )

    @classmethod
    def setup_classes(cls):
        class A(cls.Comparable):
            pass

        class B(cls.Comparable):
            pass

        class C(cls.Comparable):
            pass

    def test_delete_orphan(self):
        a, A, B, b, atob = (
            self.tables.a,
            self.classes.A,
            self.classes.B,
            self.tables.b,
            self.tables.atob,
        )

        # if no backref here, delete-orphan failed until [ticket:427]
        # was fixed

        mapper(
            A,
            a,
            properties={
                "bs": relationship(
                    B,
                    secondary=atob,
                    cascade="all, delete-orphan",
                    single_parent=True,
                )
            },
        )
        mapper(B, b)

        sess = fixture_session()
        b1 = B(data="b1")
        a1 = A(data="a1", bs=[b1])
        sess.add(a1)
        sess.flush()

        a1.bs.remove(b1)
        sess.flush()
        eq_(
            sess.execute(select(func.count("*")).select_from(atob)).scalar(), 0
        )
        eq_(sess.execute(select(func.count("*")).select_from(b)).scalar(), 0)
        eq_(sess.execute(select(func.count("*")).select_from(a)).scalar(), 1)

    def test_delete_orphan_dynamic(self):
        a, A, B, b, atob = (
            self.tables.a,
            self.classes.A,
            self.classes.B,
            self.tables.b,
            self.tables.atob,
        )

        mapper(
            A,
            a,
            # if no backref here, delete-orphan
            properties={
                "bs": relationship(
                    B,
                    secondary=atob,
                    cascade="all, delete-orphan",
                    single_parent=True,
                    lazy="dynamic",
                )
            },
        )
        # failed until [ticket:427] was fixed
        mapper(B, b)

        sess = fixture_session()
        b1 = B(data="b1")
        a1 = A(data="a1", bs=[b1])
        sess.add(a1)
        sess.flush()

        a1.bs.remove(b1)
        sess.flush()
        eq_(
            sess.execute(select(func.count("*")).select_from(atob)).scalar(), 0
        )
        eq_(sess.execute(select(func.count("*")).select_from(b)).scalar(), 0)
        eq_(sess.execute(select(func.count("*")).select_from(a)).scalar(), 1)

    def test_delete_orphan_cascades(self):
        a, A, c, b, C, B, atob = (
            self.tables.a,
            self.classes.A,
            self.tables.c,
            self.tables.b,
            self.classes.C,
            self.classes.B,
            self.tables.atob,
        )

        mapper(
            A,
            a,
            properties={
                # if no backref here, delete-orphan failed until #
                # [ticket:427] was fixed
                "bs": relationship(
                    B,
                    secondary=atob,
                    cascade="all, delete-orphan",
                    single_parent=True,
                )
            },
        )
        mapper(
            B,
            b,
            properties={"cs": relationship(C, cascade="all, delete-orphan")},
        )
        mapper(C, c)

        sess = fixture_session()
        b1 = B(data="b1", cs=[C(data="c1")])
        a1 = A(data="a1", bs=[b1])
        sess.add(a1)
        sess.flush()

        a1.bs.remove(b1)
        sess.flush()
        eq_(
            sess.execute(select(func.count("*")).select_from(atob)).scalar(), 0
        )
        eq_(sess.execute(select(func.count("*")).select_from(b)).scalar(), 0)
        eq_(sess.execute(select(func.count("*")).select_from(a)).scalar(), 1)
        eq_(sess.execute(select(func.count("*")).select_from(c)).scalar(), 0)

    def test_cascade_delete(self):
        a, A, B, b, atob = (
            self.tables.a,
            self.classes.A,
            self.classes.B,
            self.tables.b,
            self.tables.atob,
        )

        mapper(
            A,
            a,
            properties={
                "bs": relationship(
                    B,
                    secondary=atob,
                    cascade="all, delete-orphan",
                    single_parent=True,
                )
            },
        )
        mapper(B, b)

        sess = fixture_session()
        a1 = A(data="a1", bs=[B(data="b1")])
        sess.add(a1)
        sess.flush()

        sess.delete(a1)
        sess.flush()
        eq_(
            sess.execute(select(func.count("*")).select_from(atob)).scalar(), 0
        )
        eq_(sess.execute(select(func.count("*")).select_from(b)).scalar(), 0)
        eq_(sess.execute(select(func.count("*")).select_from(a)).scalar(), 0)

    def test_single_parent_error(self):
        a, A, B, b, atob = (
            self.tables.a,
            self.classes.A,
            self.classes.B,
            self.tables.b,
            self.tables.atob,
        )

        mapper(
            A,
            a,
            properties={
                "bs": relationship(
                    B, secondary=atob, cascade="all, delete-orphan"
                )
            },
        )
        mapper(B, b)
        assert_raises_message(
            sa_exc.ArgumentError,
            "For many-to-many relationship A.bs, delete-orphan cascade",
            configure_mappers,
        )

    def test_single_parent_raise(self):
        a, A, B, b, atob = (
            self.tables.a,
            self.classes.A,
            self.classes.B,
            self.tables.b,
            self.tables.atob,
        )

        mapper(
            A,
            a,
            properties={
                "bs": relationship(
                    B,
                    secondary=atob,
                    cascade="all, delete-orphan",
                    single_parent=True,
                )
            },
        )
        mapper(B, b)

        b1 = B(data="b1")
        A(data="a1", bs=[b1])

        assert_raises(sa_exc.InvalidRequestError, A, data="a2", bs=[b1])

    def test_single_parent_backref(self):
        """test that setting m2m via a uselist=False backref bypasses the
        single_parent raise"""

        a, A, B, b, atob = (
            self.tables.a,
            self.classes.A,
            self.classes.B,
            self.tables.b,
            self.tables.atob,
        )

        mapper(
            A,
            a,
            properties={
                "bs": relationship(
                    B,
                    secondary=atob,
                    cascade="all, delete-orphan",
                    single_parent=True,
                    backref=backref("a", uselist=False),
                )
            },
        )
        mapper(B, b)

        b1 = B(data="b1")
        a1 = A(data="a1", bs=[b1])

        assert_raises(sa_exc.InvalidRequestError, A, data="a2", bs=[b1])

        a2 = A(data="a2")
        b1.a = a2
        assert b1 not in a1.bs
        assert b1 in a2.bs

    def test_none_m2m_collection_assignment(self):
        a, A, B, b, atob = (
            self.tables.a,
            self.classes.A,
            self.classes.B,
            self.tables.b,
            self.tables.atob,
        )

        mapper(
            A,
            a,
            properties={"bs": relationship(B, secondary=atob, backref="as")},
        )
        mapper(B, b)

        s = fixture_session()
        a1 = A(bs=[None])
        s.add(a1)
        eq_(a1.bs, [None])
        assert_raises_message(
            orm_exc.FlushError,
            "Can't flush None value found in collection A.bs",
            s.commit,
        )
        eq_(a1.bs, [None])

    def test_none_m2m_collection_append(self):
        a, A, B, b, atob = (
            self.tables.a,
            self.classes.A,
            self.classes.B,
            self.tables.b,
            self.tables.atob,
        )

        mapper(
            A,
            a,
            properties={"bs": relationship(B, secondary=atob, backref="as")},
        )
        mapper(B, b)

        s = fixture_session()
        a1 = A()
        a1.bs.append(None)
        s.add(a1)
        eq_(a1.bs, [None])
        assert_raises_message(
            orm_exc.FlushError,
            "Can't flush None value found in collection A.bs",
            s.commit,
        )
        eq_(a1.bs, [None])


class O2MSelfReferentialDetelOrphanTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "node",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("parent_id", Integer, ForeignKey("node.id")),
        )

    @classmethod
    def setup_classes(cls):
        class Node(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        Node = cls.classes.Node
        node = cls.tables.node
        mapper(
            Node,
            node,
            properties={
                "children": relationship(
                    Node,
                    cascade="all, delete-orphan",
                    backref=backref("parent", remote_side=node.c.id),
                )
            },
        )

    def test_self_referential_delete(self):
        Node = self.classes.Node
        s = fixture_session()

        n1, n2, n3, n4 = Node(), Node(), Node(), Node()
        n1.children = [n2, n3]
        n3.children = [n4]
        s.add_all([n1, n2, n3, n4])
        s.commit()
        eq_(s.query(Node).count(), 4)

        n1.children.remove(n3)
        s.commit()
        eq_(s.query(Node).count(), 2)


class NoBackrefCascadeTest(_fixtures.FixtureTest):
    run_inserts = None

    @classmethod
    def setup_mappers(cls):
        addresses, Dingaling, User, dingalings, Address, users = (
            cls.tables.addresses,
            cls.classes.Dingaling,
            cls.classes.User,
            cls.tables.dingalings,
            cls.classes.Address,
            cls.tables.users,
        )

        mapper(Address, addresses)
        mapper(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address, backref="user", cascade_backrefs=False
                )
            },
        )

        mapper(
            Dingaling,
            dingalings,
            properties={
                "address": relationship(
                    Address, backref="dingalings", cascade_backrefs=False
                )
            },
        )

    def test_o2m_basic(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()

        u1 = User(name="u1")
        sess.add(u1)

        a1 = Address(email_address="a1")
        a1.user = u1
        assert a1 not in sess

    def test_o2m_commit_warns(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()

        u1 = User(name="u1")
        sess.add(u1)

        a1 = Address(email_address="a1")
        a1.user = u1

        assert_raises_message(sa_exc.SAWarning, "not in session", sess.commit)

        assert a1 not in sess

    def test_o2m_flag_on_backref(self):
        Dingaling, Address = self.classes.Dingaling, self.classes.Address

        sess = fixture_session()

        a1 = Address(email_address="a1")
        sess.add(a1)

        d1 = Dingaling()
        with testing.expect_deprecated(
            '"Dingaling" object is being merged into a Session along the '
            'backref cascade path for relationship "Address.dingalings"'
        ):
            d1.address = a1
        assert d1 in a1.dingalings
        assert d1 in sess

        sess.commit()

    def test_m2o_basic(self):
        Dingaling, Address = self.classes.Dingaling, self.classes.Address

        sess = fixture_session()

        a1 = Address(email_address="a1")
        d1 = Dingaling()
        sess.add(d1)

        a1.dingalings.append(d1)
        assert a1 not in sess

    def test_m2o_flag_on_backref(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()

        a1 = Address(email_address="a1")
        sess.add(a1)

        u1 = User(name="u1")
        with testing.expect_deprecated(
            '"User" object is being merged into a Session along the backref '
            'cascade path for relationship "Address.user"'
        ):
            u1.addresses.append(a1)
        assert u1 in sess

    def test_m2o_commit_warns(self):
        Dingaling, Address = self.classes.Dingaling, self.classes.Address

        sess = fixture_session()

        a1 = Address(email_address="a1")
        d1 = Dingaling()
        sess.add(d1)

        a1.dingalings.append(d1)
        assert a1 not in sess

        assert_raises_message(sa_exc.SAWarning, "not in session", sess.commit)


class PendingOrphanTestSingleLevel(fixtures.MappedTest):
    """Pending entities that are orphans"""

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column(
                "user_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("name", String(40)),
        )

        Table(
            "addresses",
            metadata,
            Column(
                "address_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("user_id", Integer, ForeignKey("users.user_id")),
            Column("email_address", String(40)),
        )
        Table(
            "orders",
            metadata,
            Column(
                "order_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column(
                "user_id", Integer, ForeignKey("users.user_id"), nullable=False
            ),
        )

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            pass

        class Address(cls.Comparable):
            pass

        class Order(cls.Comparable):
            pass

    def test_pending_standalone_orphan(self):
        """Standalone 'orphan' objects can now be persisted, if the underlying
        constraints of the database allow it.

        This now supports persisting of objects based on foreign key
        values alone.

        """

        users, orders, User, Address, Order, addresses = (
            self.tables.users,
            self.tables.orders,
            self.classes.User,
            self.classes.Address,
            self.classes.Order,
            self.tables.addresses,
        )

        mapper(Order, orders)
        mapper(Address, addresses)
        mapper(
            User,
            users,
            properties=dict(
                addresses=relationship(
                    Address, cascade="all,delete-orphan", backref="user"
                ),
                orders=relationship(Order, cascade="all, delete-orphan"),
            ),
        )
        s = fixture_session()

        # the standalone Address goes in, its foreign key
        # allows NULL
        a = Address()
        s.add(a)
        s.commit()

        # the standalone Order does not.
        o = Order()
        s.add(o)
        assert_raises(sa_exc.DBAPIError, s.commit)
        s.rollback()

        # can assign o.user_id by foreign key,
        # flush succeeds
        u = User()
        s.add(u)
        s.flush()
        o = Order(user_id=u.user_id)
        s.add(o)
        s.commit()
        assert o in s and o not in s.new

    def test_pending_collection_expunge(self):
        """Removing a pending item from a collection expunges it from
        the session."""

        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(Address, addresses)
        mapper(
            User,
            users,
            properties=dict(
                addresses=relationship(
                    Address, cascade="all,delete-orphan", backref="user"
                )
            ),
        )
        s = fixture_session()

        u = User()
        s.add(u)
        s.flush()
        a = Address()

        u.addresses.append(a)
        assert a in s

        u.addresses.remove(a)
        assert a not in s

        s.delete(u)
        s.flush()

        assert a.address_id is None, "Error: address should not be persistent"

    def test_nonorphans_ok(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(Address, addresses)
        mapper(
            User,
            users,
            properties=dict(
                addresses=relationship(
                    Address, cascade="all,delete", backref="user"
                )
            ),
        )
        s = fixture_session()
        u = User(name="u1", addresses=[Address(email_address="ad1")])
        s.add(u)
        a1 = u.addresses[0]
        u.addresses.remove(a1)
        assert a1 in s
        s.flush()
        s.expunge_all()
        eq_(s.query(Address).all(), [Address(email_address="ad1")])


class PendingOrphanTestTwoLevel(fixtures.MappedTest):
    """test usages stated at

    http://article.gmane.org/gmane.comp.python.sqlalchemy.user/3085
    http://article.gmane.org/gmane.comp.python.sqlalchemy.user/3119
    """

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "order",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
        )
        Table(
            "item",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column(
                "order_id", Integer, ForeignKey("order.id"), nullable=False
            ),
        )
        Table(
            "attribute",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("item_id", Integer, ForeignKey("item.id"), nullable=False),
        )

    @classmethod
    def setup_classes(cls):
        class Order(cls.Comparable):
            pass

        class Item(cls.Comparable):
            pass

        class Attribute(cls.Comparable):
            pass

    def test_singlelevel_remove(self):
        item, Order, order, Item = (
            self.tables.item,
            self.classes.Order,
            self.tables.order,
            self.classes.Item,
        )

        mapper(
            Order,
            order,
            properties={
                "items": relationship(Item, cascade="all, delete-orphan")
            },
        )
        mapper(Item, item)
        s = fixture_session()
        o1 = Order()
        s.add(o1)

        i1 = Item()
        o1.items.append(i1)
        o1.items.remove(i1)
        s.commit()
        assert i1 not in o1.items

    def test_multilevel_remove(self):
        Item, Attribute, order, item, attribute, Order = (
            self.classes.Item,
            self.classes.Attribute,
            self.tables.order,
            self.tables.item,
            self.tables.attribute,
            self.classes.Order,
        )

        mapper(
            Order,
            order,
            properties={
                "items": relationship(Item, cascade="all, delete-orphan")
            },
        )
        mapper(
            Item,
            item,
            properties={
                "attributes": relationship(
                    Attribute, cascade="all, delete-orphan"
                )
            },
        )
        mapper(Attribute, attribute)
        s = fixture_session()
        o1 = Order()
        s.add(o1)

        i1 = Item()
        a1 = Attribute()
        i1.attributes.append(a1)

        o1.items.append(i1)

        assert i1 in s
        assert a1 in s

        # i1 is an orphan so the operation
        # removes 'i1'.  The operation
        # cascades down to 'a1'.
        o1.items.remove(i1)

        assert i1 not in s
        assert a1 not in s

        s.commit()
        assert o1 in s
        assert a1 not in s
        assert i1 not in s
        assert a1 not in o1.items


class DoubleParentO2MOrphanTest(fixtures.MappedTest):
    """Test orphan behavior on an entity that requires
    two parents via many-to-one (one-to-many collection.).

    """

    @classmethod
    def define_tables(cls, meta):
        Table(
            "sales_reps",
            meta,
            Column(
                "sales_rep_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("name", String(50)),
        )
        Table(
            "accounts",
            meta,
            Column(
                "account_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("balance", Integer),
        )

        Table(
            "customers",
            meta,
            Column(
                "customer_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("name", String(50)),
            Column(
                "sales_rep_id", Integer, ForeignKey("sales_reps.sales_rep_id")
            ),
            Column("account_id", Integer, ForeignKey("accounts.account_id")),
        )

    def _fixture(self, legacy_is_orphan, uselist):
        sales_reps, customers, accounts = (
            self.tables.sales_reps,
            self.tables.customers,
            self.tables.accounts,
        )

        class Customer(fixtures.ComparableEntity):
            pass

        class Account(fixtures.ComparableEntity):
            pass

        class SalesRep(fixtures.ComparableEntity):
            pass

        mapper(Customer, customers, legacy_is_orphan=legacy_is_orphan)
        mapper(
            Account,
            accounts,
            properties=dict(
                customers=relationship(
                    Customer,
                    cascade="all,delete-orphan",
                    backref="account",
                    uselist=uselist,
                )
            ),
        )
        mapper(
            SalesRep,
            sales_reps,
            properties=dict(
                customers=relationship(
                    Customer,
                    cascade="all,delete-orphan",
                    backref="sales_rep",
                    uselist=uselist,
                )
            ),
        )
        s = fixture_session(expire_on_commit=False, autoflush=False)

        a = Account(balance=0)
        sr = SalesRep(name="John")
        s.add_all((a, sr))
        s.commit()

        c = Customer(name="Jane")

        if uselist:
            a.customers.append(c)
            sr.customers.append(c)
        else:
            a.customers = c
            sr.customers = c

        assert c in s
        return s, c, a, sr

    def test_double_parent_expunge_o2m_legacy(self):
        """test the delete-orphan uow event for multiple delete-orphan
        parent relationships."""

        s, c, a, sr = self._fixture(True, True)

        a.customers.remove(c)
        assert c in s, "Should not expunge customer yet, still has one parent"

        sr.customers.remove(c)
        assert c not in s, "Should expunge customer when both parents are gone"

    def test_double_parent_expunge_o2m_current(self):
        """test the delete-orphan uow event for multiple delete-orphan
        parent relationships."""

        s, c, a, sr = self._fixture(False, True)

        a.customers.remove(c)
        assert c not in s, "Should expunge customer when either parent is gone"

        sr.customers.remove(c)
        assert c not in s, "Should expunge customer when both parents are gone"

    def test_double_parent_expunge_o2o_legacy(self):
        """test the delete-orphan uow event for multiple delete-orphan
        parent relationships."""

        s, c, a, sr = self._fixture(True, False)

        a.customers = None
        assert c in s, "Should not expunge customer yet, still has one parent"

        sr.customers = None
        assert c not in s, "Should expunge customer when both parents are gone"

    def test_double_parent_expunge_o2o_current(self):
        """test the delete-orphan uow event for multiple delete-orphan
        parent relationships."""

        s, c, a, sr = self._fixture(False, False)

        a.customers = None
        assert c not in s, "Should expunge customer when either parent is gone"

        sr.customers = None
        assert c not in s, "Should expunge customer when both parents are gone"


class DoubleParentM2OOrphanTest(fixtures.MappedTest):
    """Test orphan behavior on an entity that requires
    two parents via one-to-many (many-to-one reference to the orphan).

    """

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "addresses",
            metadata,
            Column(
                "address_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("street", String(30)),
        )

        Table(
            "homes",
            metadata,
            Column(
                "home_id",
                Integer,
                primary_key=True,
                key="id",
                test_needs_autoincrement=True,
            ),
            Column("description", String(30)),
            Column(
                "address_id",
                Integer,
                ForeignKey("addresses.address_id"),
                nullable=False,
            ),
        )

        Table(
            "businesses",
            metadata,
            Column(
                "business_id",
                Integer,
                primary_key=True,
                key="id",
                test_needs_autoincrement=True,
            ),
            Column("description", String(30), key="description"),
            Column(
                "address_id",
                Integer,
                ForeignKey("addresses.address_id"),
                nullable=False,
            ),
        )

    def test_non_orphan(self):
        """test that an entity can have two parent delete-orphan
        cascades, and persists normally."""

        homes, businesses, addresses = (
            self.tables.homes,
            self.tables.businesses,
            self.tables.addresses,
        )

        class Address(fixtures.ComparableEntity):
            pass

        class Home(fixtures.ComparableEntity):
            pass

        class Business(fixtures.ComparableEntity):
            pass

        mapper(Address, addresses)
        mapper(
            Home,
            homes,
            properties={
                "address": relationship(
                    Address, cascade="all,delete-orphan", single_parent=True
                )
            },
        )
        mapper(
            Business,
            businesses,
            properties={
                "address": relationship(
                    Address, cascade="all,delete-orphan", single_parent=True
                )
            },
        )

        session = fixture_session()
        h1 = Home(description="home1", address=Address(street="address1"))
        b1 = Business(
            description="business1", address=Address(street="address2")
        )
        session.add_all((h1, b1))
        session.flush()
        session.expunge_all()

        eq_(
            session.get(Home, h1.id),
            Home(description="home1", address=Address(street="address1")),
        )
        eq_(
            session.get(Business, b1.id),
            Business(
                description="business1", address=Address(street="address2")
            ),
        )

    def test_orphan(self):
        """test that an entity can have two parent delete-orphan
        cascades, and is detected as an orphan when saved without a
        parent."""

        homes, businesses, addresses = (
            self.tables.homes,
            self.tables.businesses,
            self.tables.addresses,
        )

        class Address(fixtures.ComparableEntity):
            pass

        class Home(fixtures.ComparableEntity):
            pass

        class Business(fixtures.ComparableEntity):
            pass

        mapper(Address, addresses)
        mapper(
            Home,
            homes,
            properties={
                "address": relationship(
                    Address, cascade="all,delete-orphan", single_parent=True
                )
            },
        )
        mapper(
            Business,
            businesses,
            properties={
                "address": relationship(
                    Address, cascade="all,delete-orphan", single_parent=True
                )
            },
        )
        session = fixture_session()
        a1 = Address()
        session.add(a1)
        session.flush()


class CollectionAssignmentOrphanTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "table_a",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(30)),
        )
        Table(
            "table_b",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(30)),
            Column("a_id", Integer, ForeignKey("table_a.id")),
        )

    def test_basic(self):
        table_b, table_a = self.tables.table_b, self.tables.table_a

        class A(fixtures.ComparableEntity):
            pass

        class B(fixtures.ComparableEntity):
            pass

        mapper(
            A,
            table_a,
            properties={"bs": relationship(B, cascade="all, delete-orphan")},
        )
        mapper(B, table_b)

        a1 = A(name="a1", bs=[B(name="b1"), B(name="b2"), B(name="b3")])

        sess = fixture_session()
        sess.add(a1)
        sess.flush()

        sess.expunge_all()

        eq_(
            sess.get(A, a1.id),
            A(name="a1", bs=[B(name="b1"), B(name="b2"), B(name="b3")]),
        )

        a1 = sess.get(A, a1.id)
        assert not class_mapper(B)._is_orphan(
            attributes.instance_state(a1.bs[0])
        )
        a1.bs[0].foo = "b2modified"
        a1.bs[1].foo = "b3modified"
        sess.flush()

        sess.expunge_all()
        eq_(
            sess.get(A, a1.id),
            A(name="a1", bs=[B(name="b1"), B(name="b2"), B(name="b3")]),
        )


class OrphanCriterionTest(fixtures.MappedTest):
    @classmethod
    def define_tables(self, metadata):
        Table(
            "core",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("related_one_id", Integer, ForeignKey("related_one.id")),
            Column("related_two_id", Integer, ForeignKey("related_two.id")),
        )

        Table(
            "related_one",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
        )

        Table(
            "related_two",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
        )

    def _fixture(
        self,
        legacy_is_orphan,
        persistent,
        r1_present,
        r2_present,
        detach_event=True,
    ):
        class Core(object):
            pass

        class RelatedOne(object):
            def __init__(self, cores):
                self.cores = cores

        class RelatedTwo(object):
            def __init__(self, cores):
                self.cores = cores

        mapper(Core, self.tables.core, legacy_is_orphan=legacy_is_orphan)
        mapper(
            RelatedOne,
            self.tables.related_one,
            properties={
                "cores": relationship(
                    Core, cascade="all, delete-orphan", backref="r1"
                )
            },
        )
        mapper(
            RelatedTwo,
            self.tables.related_two,
            properties={
                "cores": relationship(
                    Core, cascade="all, delete-orphan", backref="r2"
                )
            },
        )
        c1 = Core()
        if detach_event:
            RelatedOne(cores=[c1])
            RelatedTwo(cores=[c1])
        else:
            if r1_present:
                RelatedOne(cores=[c1])
            if r2_present:
                RelatedTwo(cores=[c1])

        if persistent:
            s = fixture_session()
            s.add(c1)
            s.flush()

        if detach_event:
            if not r1_present:
                c1.r1 = None
            if not r2_present:
                c1.r2 = None
        return c1

    def _assert_not_orphan(self, c1):
        mapper = object_mapper(c1)
        state = instance_state(c1)
        assert not mapper._is_orphan(state)

    def _assert_is_orphan(self, c1):
        mapper = object_mapper(c1)
        state = instance_state(c1)
        assert mapper._is_orphan(state)

    def test_leg_pers_r1_r2(self):
        c1 = self._fixture(True, True, True, True)

        self._assert_not_orphan(c1)

    def test_current_pers_r1_r2(self):
        c1 = self._fixture(False, True, True, True)

        self._assert_not_orphan(c1)

    def test_leg_pers_r1_notr2(self):
        c1 = self._fixture(True, True, True, False)

        self._assert_not_orphan(c1)

    def test_current_pers_r1_notr2(self):
        c1 = self._fixture(False, True, True, False)

        self._assert_is_orphan(c1)

    def test_leg_pers_notr1_notr2(self):
        c1 = self._fixture(True, True, False, False)

        self._assert_is_orphan(c1)

    def test_current_pers_notr1_notr2(self):
        c1 = self._fixture(False, True, True, False)

        self._assert_is_orphan(c1)

    def test_leg_transient_r1_r2(self):
        c1 = self._fixture(True, False, True, True)

        self._assert_not_orphan(c1)

    def test_current_transient_r1_r2(self):
        c1 = self._fixture(False, False, True, True)

        self._assert_not_orphan(c1)

    def test_leg_transient_r1_notr2(self):
        c1 = self._fixture(True, False, True, False)

        self._assert_not_orphan(c1)

    def test_current_transient_r1_notr2(self):
        c1 = self._fixture(False, False, True, False)

        self._assert_is_orphan(c1)

    def test_leg_transient_notr1_notr2(self):
        c1 = self._fixture(True, False, False, False)

        self._assert_is_orphan(c1)

    def test_current_transient_notr1_notr2(self):
        c1 = self._fixture(False, False, False, False)

        self._assert_is_orphan(c1)

    def test_leg_transient_notr1_notr2_noevent(self):
        c1 = self._fixture(True, False, False, False, False)

        self._assert_is_orphan(c1)

    def test_current_transient_notr1_notr2_noevent(self):
        c1 = self._fixture(False, False, False, False, False)

        self._assert_is_orphan(c1)

    def test_leg_persistent_notr1_notr2_noevent(self):
        c1 = self._fixture(True, True, False, False, False)

        self._assert_not_orphan(c1)

    def test_current_persistent_notr1_notr2_noevent(self):
        c1 = self._fixture(False, True, False, False, False)

        self._assert_not_orphan(c1)


class O2MConflictTest(fixtures.MappedTest):
    """test that O2M dependency detects a change in parent, does the
    right thing, and updates the collection/attribute.

    """

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "parent",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
        )
        Table(
            "child",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column(
                "parent_id", Integer, ForeignKey("parent.id"), nullable=False
            ),
        )

    @classmethod
    def setup_classes(cls):
        class Parent(cls.Comparable):
            pass

        class Child(cls.Comparable):
            pass

    def _do_move_test(self, delete_old):
        Parent, Child = self.classes.Parent, self.classes.Child

        with fixture_session(autoflush=False) as sess:
            p1, p2, c1 = Parent(), Parent(), Child()
            if Parent.child.property.uselist:
                p1.child.append(c1)
            else:
                p1.child = c1
            sess.add_all([p1, c1])
            sess.flush()

            if delete_old:
                sess.delete(p1)

            if Parent.child.property.uselist:
                p2.child.append(c1)
            else:
                p2.child = c1
            sess.add(p2)

            sess.flush()
            eq_(sess.query(Child).filter(Child.parent_id == p2.id).all(), [c1])

    def test_o2o_delete_old(self):
        Child, Parent, parent, child = (
            self.classes.Child,
            self.classes.Parent,
            self.tables.parent,
            self.tables.child,
        )

        mapper(
            Parent,
            parent,
            properties={"child": relationship(Child, uselist=False)},
        )
        mapper(Child, child)
        self._do_move_test(True)
        self._do_move_test(False)

    def test_o2m_delete_old(self):
        Child, Parent, parent, child = (
            self.classes.Child,
            self.classes.Parent,
            self.tables.parent,
            self.tables.child,
        )

        mapper(
            Parent,
            parent,
            properties={"child": relationship(Child, uselist=True)},
        )
        mapper(Child, child)
        self._do_move_test(True)
        self._do_move_test(False)

    def test_o2o_backref_delete_old(self):
        Child, Parent, parent, child = (
            self.classes.Child,
            self.classes.Parent,
            self.tables.parent,
            self.tables.child,
        )

        mapper(
            Parent,
            parent,
            properties={
                "child": relationship(
                    Child,
                    uselist=False,
                    backref=backref("parent", cascade_backrefs=False),
                )
            },
        )
        mapper(Child, child)
        self._do_move_test(True)
        self._do_move_test(False)

    def test_o2o_delcascade_delete_old(self):
        Child, Parent, parent, child = (
            self.classes.Child,
            self.classes.Parent,
            self.tables.parent,
            self.tables.child,
        )

        mapper(
            Parent,
            parent,
            properties={
                "child": relationship(
                    Child, uselist=False, cascade="all, delete"
                )
            },
        )
        mapper(Child, child)
        self._do_move_test(True)
        self._do_move_test(False)

    def test_o2o_delorphan_delete_old(self):
        Child, Parent, parent, child = (
            self.classes.Child,
            self.classes.Parent,
            self.tables.parent,
            self.tables.child,
        )

        mapper(
            Parent,
            parent,
            properties={
                "child": relationship(
                    Child, uselist=False, cascade="all, delete, delete-orphan"
                )
            },
        )
        mapper(Child, child)
        self._do_move_test(True)
        self._do_move_test(False)

    def test_o2o_delorphan_backref_delete_old(self):
        Child, Parent, parent, child = (
            self.classes.Child,
            self.classes.Parent,
            self.tables.parent,
            self.tables.child,
        )

        mapper(
            Parent,
            parent,
            properties={
                "child": relationship(
                    Child,
                    uselist=False,
                    cascade="all, delete, delete-orphan",
                    backref=backref("parent", cascade_backrefs=False),
                )
            },
        )
        mapper(Child, child)
        self._do_move_test(True)
        self._do_move_test(False)

    def test_o2o_backref_delorphan_delete_old(self):
        Child, Parent, parent, child = (
            self.classes.Child,
            self.classes.Parent,
            self.tables.parent,
            self.tables.child,
        )

        mapper(Parent, parent)
        mapper(
            Child,
            child,
            properties={
                "parent": relationship(
                    Parent,
                    uselist=False,
                    single_parent=True,
                    backref=backref("child", uselist=False),
                    cascade="all,delete,delete-orphan",
                    cascade_backrefs=False,
                )
            },
        )
        self._do_move_test(True)
        self._do_move_test(False)

    def test_o2m_backref_delorphan_delete_old(self):
        Child, Parent, parent, child = (
            self.classes.Child,
            self.classes.Parent,
            self.tables.parent,
            self.tables.child,
        )

        mapper(Parent, parent)
        mapper(
            Child,
            child,
            properties={
                "parent": relationship(
                    Parent,
                    uselist=False,
                    single_parent=True,
                    backref=backref("child", uselist=True),
                    cascade="all,delete,delete-orphan",
                    cascade_backrefs=False,
                )
            },
        )
        self._do_move_test(True)
        self._do_move_test(False)


class PartialFlushTest(fixtures.MappedTest):
    """test cascade behavior as it relates to object lists passed
    to flush().

    """

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "base",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("descr", String(50)),
        )

        Table(
            "noninh_child",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("base_id", Integer, ForeignKey("base.id")),
        )

        Table(
            "parent",
            metadata,
            Column("id", Integer, ForeignKey("base.id"), primary_key=True),
        )
        Table(
            "inh_child",
            metadata,
            Column("id", Integer, ForeignKey("base.id"), primary_key=True),
            Column("parent_id", Integer, ForeignKey("parent.id")),
        )

    def test_o2m_m2o(self):
        base, noninh_child = self.tables.base, self.tables.noninh_child

        class Base(fixtures.ComparableEntity):
            pass

        class Child(fixtures.ComparableEntity):
            pass

        mapper(
            Base,
            base,
            properties={"children": relationship(Child, backref="parent")},
        )
        mapper(Child, noninh_child)

        sess = fixture_session()

        c1, c2 = Child(), Child()
        b1 = Base(descr="b1", children=[c1, c2])
        sess.add(b1)

        assert c1 in sess.new
        assert c2 in sess.new
        sess.flush([b1])

        # c1, c2 get cascaded into the session on o2m.
        # not sure if this is how I like this
        # to work but that's how it works for now.
        assert c1 in sess and c1 not in sess.new
        assert c2 in sess and c2 not in sess.new
        assert b1 in sess and b1 not in sess.new

        sess = fixture_session()
        c1, c2 = Child(), Child()
        b1 = Base(descr="b1", children=[c1, c2])
        sess.add(b1)
        sess.flush([c1])
        # m2o, otoh, doesn't cascade up the other way.
        assert c1 in sess and c1 not in sess.new
        assert c2 in sess and c2 in sess.new
        assert b1 in sess and b1 in sess.new

        sess = fixture_session()
        c1, c2 = Child(), Child()
        b1 = Base(descr="b1", children=[c1, c2])
        sess.add(b1)
        sess.flush([c1, c2])
        # m2o, otoh, doesn't cascade up the other way.
        assert c1 in sess and c1 not in sess.new
        assert c2 in sess and c2 not in sess.new
        assert b1 in sess and b1 in sess.new

    def test_circular_sort(self):
        """test ticket 1306"""

        base, inh_child, parent = (
            self.tables.base,
            self.tables.inh_child,
            self.tables.parent,
        )

        class Base(fixtures.ComparableEntity):
            pass

        class Parent(Base):
            pass

        class Child(Base):
            pass

        mapper(Base, base)

        mapper(
            Child,
            inh_child,
            inherits=Base,
            properties={
                "parent": relationship(
                    Parent,
                    backref="children",
                    primaryjoin=inh_child.c.parent_id == parent.c.id,
                )
            },
        )

        mapper(Parent, parent, inherits=Base)

        sess = fixture_session()
        p1 = Parent()

        c1, c2, c3 = Child(), Child(), Child()
        p1.children = [c1, c2, c3]
        sess.add(p1)

        sess.flush([c1])
        assert p1 in sess.new
        assert c1 not in sess.new
        assert c2 in sess.new


class SubclassCascadeTest(fixtures.DeclarativeMappedTest):
    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Company(Base):
            __tablename__ = "company"
            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            employees = relationship("Employee", cascade="all, delete-orphan")

        class Employee(Base):
            __tablename__ = "employee"
            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            type = Column(String(50))
            company_id = Column(ForeignKey("company.id"))

            __mapper_args__ = {
                "polymorphic_identity": "employee",
                "polymorphic_on": type,
            }

        class Engineer(Employee):
            __tablename__ = "engineer"
            id = Column(Integer, ForeignKey("employee.id"), primary_key=True)
            engineer_name = Column(String(30))
            languages = relationship("Language", cascade="all, delete-orphan")

            __mapper_args__ = {"polymorphic_identity": "engineer"}

        class MavenBuild(Base):
            __tablename__ = "maven_build"
            id = Column(Integer, primary_key=True)
            java_language_id = Column(
                ForeignKey("java_language.id"), nullable=False
            )

        class Manager(Employee):
            __tablename__ = "manager"
            id = Column(Integer, ForeignKey("employee.id"), primary_key=True)
            manager_name = Column(String(30))

            __mapper_args__ = {"polymorphic_identity": "manager"}

        class Language(Base):
            __tablename__ = "language"
            id = Column(Integer, primary_key=True)
            engineer_id = Column(ForeignKey("engineer.id"), nullable=False)
            name = Column(String(50))
            type = Column(String(50))

            __mapper_args__ = {
                "polymorphic_on": type,
                "polymorphic_identity": "language",
            }

        class JavaLanguage(Language):
            __tablename__ = "java_language"
            id = Column(ForeignKey("language.id"), primary_key=True)
            maven_builds = relationship(
                "MavenBuild", cascade="all, delete-orphan"
            )

            __mapper_args__ = {"polymorphic_identity": "java_language"}

    def test_cascade_iterator_polymorphic(self):
        (
            Company,
            Employee,
            Engineer,
            Language,
            JavaLanguage,
            MavenBuild,
        ) = self.classes(
            "Company",
            "Employee",
            "Engineer",
            "Language",
            "JavaLanguage",
            "MavenBuild",
        )

        obj = Company(
            employees=[
                Engineer(
                    languages=[
                        JavaLanguage(name="java", maven_builds=[MavenBuild()])
                    ]
                )
            ]
        )
        eng = obj.employees[0]
        lang = eng.languages[0]
        maven_build = lang.maven_builds[0]

        from sqlalchemy import inspect

        state = inspect(obj)
        it = inspect(Company).cascade_iterator("save-update", state)
        eq_(set([rec[0] for rec in it]), set([eng, maven_build, lang]))

        state = inspect(eng)
        it = inspect(Employee).cascade_iterator("save-update", state)
        eq_(set([rec[0] for rec in it]), set([maven_build, lang]))

    def test_delete_orphan_round_trip(self):
        (
            Company,
            Employee,
            Engineer,
            Language,
            JavaLanguage,
            MavenBuild,
        ) = self.classes(
            "Company",
            "Employee",
            "Engineer",
            "Language",
            "JavaLanguage",
            "MavenBuild",
        )

        obj = Company(
            employees=[
                Engineer(
                    languages=[
                        JavaLanguage(name="java", maven_builds=[MavenBuild()])
                    ]
                )
            ]
        )
        s = fixture_session()
        s.add(obj)
        s.commit()

        obj.employees = []
        s.commit()

        eq_(s.query(Language).count(), 0)


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
        ({"delete"}, {"delete"}),
        (
            {"all, delete-orphan"},
            {"delete", "delete-orphan", "merge", "save-update"},
        ),
        ({"save-update, expunge"}, {"save-update"}),
    )
    def test_write_cascades(self, setting, settings_that_warn):
        Order = self.classes.Order

        assert_raises_message(
            sa_exc.ArgumentError,
            'Cascade settings "%s" apply to persistence '
            "operations" % (", ".join(sorted(settings_that_warn))),
            relationship,
            Order,
            primaryjoin=(
                self.tables.users.c.id == foreign(self.tables.orders.c.user_id)
            ),
            cascade=", ".join(sorted(setting)),
            viewonly=True,
        )

    def test_expunge_cascade(self):
        User, Order, orders, users = (
            self.classes.User,
            self.classes.Order,
            self.tables.orders,
            self.tables.users,
        )

        mapper(Order, orders)
        mapper(
            User,
            users,
            properties={
                "orders": relationship(
                    Order,
                    primaryjoin=(
                        self.tables.users.c.id
                        == foreign(self.tables.orders.c.user_id)
                    ),
                    cascade="expunge",
                    viewonly=True,
                )
            },
        )

        sess = fixture_session()
        u = User(id=1, name="jack")
        sess.add(u)
        sess.add_all(
            [
                Order(id=1, user_id=1, description="someorder"),
                Order(id=2, user_id=1, description="someotherorder"),
            ]
        )
        sess.commit()

        u1 = sess.query(User).first()
        orders = u1.orders
        eq_(len(orders), 2)

        in_(orders[0], sess)
        in_(orders[1], sess)

        sess.expunge(u1)

        not_in(orders[0], sess)
        not_in(orders[1], sess)

    def test_default_none_cascade(self):
        User, Order, orders, users = (
            self.classes.User,
            self.classes.Order,
            self.tables.orders,
            self.tables.users,
        )

        mapper(Order, orders)
        mapper(
            User,
            users,
            properties={
                "orders": relationship(
                    Order,
                    primaryjoin=(
                        self.tables.users.c.id
                        == foreign(self.tables.orders.c.user_id)
                    ),
                    viewonly=True,
                )
            },
        )

        sess = fixture_session()
        u1 = User(id=1, name="jack")
        sess.add(u1)

        o1, o2 = (
            Order(id=1, user_id=1, description="someorder"),
            Order(id=2, user_id=1, description="someotherorder"),
        )

        u1.orders.append(o1)
        u1.orders.append(o2)

        not_in(o1, sess)
        not_in(o2, sess)

    def test_default_merge_cascade(self):
        User, Order, orders, users = (
            self.classes.User,
            self.classes.Order,
            self.tables.orders,
            self.tables.users,
        )

        mapper(Order, orders)
        mapper(
            User,
            users,
            properties={
                "orders": relationship(
                    Order,
                    primaryjoin=(
                        self.tables.users.c.id
                        == foreign(self.tables.orders.c.user_id)
                    ),
                    viewonly=True,
                )
            },
        )

        sess = fixture_session()
        u1 = User(id=1, name="jack")

        o1, o2 = (
            Order(id=1, user_id=1, description="someorder"),
            Order(id=2, user_id=1, description="someotherorder"),
        )

        u1.orders.append(o1)
        u1.orders.append(o2)

        u1 = sess.merge(u1)

        assert not u1.orders

    def test_default_cascade(self):
        User, Order, orders, users = (
            self.classes.User,
            self.classes.Order,
            self.tables.orders,
            self.tables.users,
        )

        mapper(Order, orders)
        umapper = mapper(
            User,
            users,
            properties={
                "orders": relationship(
                    Order,
                    primaryjoin=(
                        self.tables.users.c.id
                        == foreign(self.tables.orders.c.user_id)
                    ),
                    viewonly=True,
                )
            },
        )

        eq_(umapper.attrs["orders"].cascade, set())

    def test_write_cascade_disallowed_w_viewonly(self):

        Order = self.classes.Order

        assert_raises_message(
            sa_exc.ArgumentError,
            'Cascade settings "delete, delete-orphan, merge, save-update" '
            "apply to persistence operations",
            relationship,
            Order,
            primaryjoin=(
                self.tables.users.c.id == foreign(self.tables.orders.c.user_id)
            ),
            cascade="all, delete, delete-orphan",
            viewonly=True,
        )
