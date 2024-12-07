"""Attribute/instance expiration, deferral of attributes, etc."""

import re

import sqlalchemy as sa
from sqlalchemy import exc as sa_exc
from sqlalchemy import FetchedValue
from sqlalchemy import ForeignKey
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.orm import attributes
from sqlalchemy.orm import contains_eager
from sqlalchemy.orm import defer
from sqlalchemy.orm import deferred
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy.orm import immediateload
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import lazyload
from sqlalchemy.orm import make_transient_to_detached
from sqlalchemy.orm import relationship
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import Session
from sqlalchemy.orm import strategies
from sqlalchemy.orm import subqueryload
from sqlalchemy.orm import undefer
from sqlalchemy.sql import select
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import expect_raises_message
from sqlalchemy.testing.assertsql import CountStatements
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from sqlalchemy.testing.util import gc_collect
from test.orm import _fixtures


class ExpireTest(_fixtures.FixtureTest):
    def test_expire(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={"addresses": relationship(Address, backref="user")},
        )
        self.mapper_registry.map_imperatively(Address, addresses)

        sess = fixture_session(autoflush=False)
        u = sess.get(User, 7)
        assert len(u.addresses) == 1
        u.name = "foo"
        del u.addresses[0]
        sess.expire(u)

        assert "name" not in u.__dict__

        def go():
            assert u.name == "jack"

        self.assert_sql_count(testing.db, go, 1)
        assert "name" in u.__dict__

        u.name = "foo"
        sess.flush()
        # change the value in the DB
        sess.execute(
            users.update().values(dict(name="jack")).where(users.c.id == 7)
        )
        sess.expire(u)
        # object isn't refreshed yet, using dict to bypass trigger
        assert u.__dict__.get("name") != "jack"
        assert "name" in attributes.instance_state(u).expired_attributes

        sess.query(User).all()
        # test that it refreshed
        assert u.__dict__["name"] == "jack"
        assert "name" not in attributes.instance_state(u).expired_attributes

        def go():
            assert u.name == "jack"

        self.assert_sql_count(testing.db, go, 0)

    def test_expire_autoflush(self):
        User, users = self.classes.User, self.tables.users
        Address, addresses = self.classes.Address, self.tables.addresses

        self.mapper_registry.map_imperatively(User, users)
        self.mapper_registry.map_imperatively(
            Address, addresses, properties={"user": relationship(User)}
        )

        s = fixture_session()

        a1 = s.get(Address, 2)
        u1 = s.get(User, 7)
        a1.user = u1

        s.expire(a1, ["user_id"])

        # autoflushes
        eq_(a1.user_id, 7)

    def test_persistence_check(self):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)
        s = fixture_session()
        u = s.get(User, 7)
        s.expunge_all()

        assert_raises_message(
            sa_exc.InvalidRequestError,
            r"is not persistent within this Session",
            s.expire,
            u,
        )

    def test_get_refreshes(self):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)
        s = fixture_session()
        u = s.get(User, 10)
        s.expire_all()

        def go():
            s.get(User, 10)  # get() refreshes

        self.assert_sql_count(testing.db, go, 1)

        def go():
            eq_(u.name, "chuck")  # attributes unexpired

        self.assert_sql_count(testing.db, go, 0)

        def go():
            s.get(User, 10)  # expire flag reset, so not expired

        self.assert_sql_count(testing.db, go, 0)

    def test_get_on_deleted_expunges(self):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)
        s = fixture_session()
        u = s.get(User, 10)

        s.expire_all()
        s.execute(users.delete().where(User.id == 10))

        # object is gone, get() returns None, removes u from session
        assert u in s
        assert s.get(User, 10) is None
        assert u not in s  # and expunges

    def test_refresh_on_deleted_raises(self):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)
        s = fixture_session()
        u = s.get(User, 10)
        s.expire_all()

        s.expire_all()
        s.execute(users.delete().where(User.id == 10))

        # raises ObjectDeletedError
        assert_raises_message(
            sa.orm.exc.ObjectDeletedError,
            "Instance '<User at .*?>' has been "
            "deleted, or its row is otherwise not present.",
            getattr,
            u,
            "name",
        )

    def test_rollback_undoes_expunge_from_deleted(self):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)
        s = fixture_session()
        u = s.get(User, 10)
        s.expire_all()
        s.execute(users.delete().where(User.id == 10))

        # do a get()/remove u from session
        assert s.get(User, 10) is None
        assert u not in s

        s.rollback()

        assert u in s
        # but now its back, rollback has occurred, the
        # _remove_newly_deleted is reverted
        eq_(u.name, "chuck")

    def test_deferred(self):
        """test that unloaded, deferred attributes aren't included in the
        expiry list."""

        Order, orders = self.classes.Order, self.tables.orders

        self.mapper_registry.map_imperatively(
            Order,
            orders,
            properties={"description": deferred(orders.c.description)},
        )

        s = fixture_session()
        o1 = s.query(Order).first()
        assert "description" not in o1.__dict__
        s.expire(o1)

        # the deferred attribute is listed as expired (new in 1.4)
        eq_(
            inspect(o1).expired_attributes,
            {"id", "isopen", "address_id", "user_id", "description"},
        )

        # unexpire by accessing isopen
        assert o1.isopen is not None

        # all expired_attributes are cleared
        eq_(inspect(o1).expired_attributes, set())

        # but description wasn't loaded (new in 1.4)
        assert "description" not in o1.__dict__

        # loads using deferred callable
        assert o1.description

    def test_deferred_notfound(self):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(
            User, users, properties={"name": deferred(users.c.name)}
        )
        s = fixture_session()
        u = s.get(User, 10)

        assert "name" not in u.__dict__
        s.execute(users.delete().where(User.id == 10))
        assert_raises_message(
            sa.orm.exc.ObjectDeletedError,
            "Instance '<User at .*?>' has been "
            "deleted, or its row is otherwise not present.",
            getattr,
            u,
            "name",
        )

    def test_lazyload_autoflushes(self):
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
                    Address, order_by=addresses.c.email_address
                )
            },
        )
        self.mapper_registry.map_imperatively(Address, addresses)
        s = fixture_session(
            autoflush=True,
        )
        u = s.get(User, 8)
        adlist = u.addresses
        eq_(
            adlist,
            [
                Address(email_address="ed@bettyboop.com"),
                Address(email_address="ed@lala.com"),
                Address(email_address="ed@wood.com"),
            ],
        )
        a1 = u.addresses[2]
        a1.email_address = "aaaaa"
        s.expire(u, ["addresses"])
        eq_(
            u.addresses,
            [
                Address(email_address="aaaaa"),
                Address(email_address="ed@bettyboop.com"),
                Address(email_address="ed@lala.com"),
            ],
        )

    def test_refresh_cancels_expire(self):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)
        s = fixture_session()
        u = s.get(User, 7)
        s.expire(u)
        s.refresh(u)

        def go():
            u = s.get(User, 7)
            eq_(u.name, "jack")

        self.assert_sql_count(testing.db, go, 0)

    def test_expire_doesntload_on_set(self):
        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(User, users)

        sess = fixture_session(autoflush=False)
        u = sess.get(User, 7)

        sess.expire(u, attribute_names=["name"])

        def go():
            u.name = "somenewname"

        self.assert_sql_count(testing.db, go, 0)
        sess.flush()
        sess.expunge_all()
        assert sess.get(User, 7).name == "somenewname"

    def test_no_session(self):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)
        sess = fixture_session()
        u = sess.get(User, 7)

        sess.expire(u, attribute_names=["name"])
        sess.expunge(u)
        assert_raises(orm_exc.DetachedInstanceError, getattr, u, "name")

    def test_pending_raises(self):
        users, User = self.tables.users, self.classes.User

        # this was the opposite in 0.4, but the reasoning there seemed off.
        # expiring a pending instance makes no sense, so should raise
        self.mapper_registry.map_imperatively(User, users)
        sess = fixture_session()
        u = User(id=15)
        sess.add(u)
        assert_raises(sa_exc.InvalidRequestError, sess.expire, u, ["name"])

    def test_no_instance_key(self):
        User, users = self.classes.User, self.tables.users

        # this tests an artificial condition such that
        # an instance is pending, but has expired attributes.  this
        # is actually part of a larger behavior when postfetch needs to
        # occur during a flush() on an instance that was just inserted
        self.mapper_registry.map_imperatively(User, users)
        sess = fixture_session(autoflush=False)
        u = sess.get(User, 7)

        sess.expire(u, attribute_names=["name"])
        sess.expunge(u)
        attributes.instance_state(u).key = None
        assert "name" not in u.__dict__
        sess.add(u)
        assert u.name == "jack"

    def test_no_instance_key_no_pk(self):
        users, User = self.tables.users, self.classes.User

        # same as test_no_instance_key, but the PK columns
        # are absent.  ensure an error is raised.
        self.mapper_registry.map_imperatively(User, users)
        sess = fixture_session()
        u = sess.get(User, 7)

        sess.expire(u, attribute_names=["name", "id"])
        sess.expunge(u)
        attributes.instance_state(u).key = None
        assert "name" not in u.__dict__
        sess.add(u)
        assert_raises(sa_exc.InvalidRequestError, getattr, u, "name")

    def test_expire_preserves_changes(self):
        """test that the expire load operation doesn't revert post-expire
        changes"""

        Order, orders = self.classes.Order, self.tables.orders

        self.mapper_registry.map_imperatively(Order, orders)
        sess = fixture_session(autoflush=False)
        o = sess.get(Order, 3)
        sess.expire(o)

        o.description = "order 3 modified"

        def go():
            assert o.isopen == 1

        self.assert_sql_count(testing.db, go, 1)
        assert o.description == "order 3 modified"

        del o.description
        assert "description" not in o.__dict__
        sess.expire(o, ["isopen"])
        sess.query(Order).all()
        assert o.isopen == 1
        assert "description" not in o.__dict__

        assert o.description is None

        o.isopen = 15
        sess.expire(o, ["isopen", "description"])
        o.description = "some new description"
        sess.query(Order).all()
        assert o.isopen == 1
        assert o.description == "some new description"

        sess.expire(o, ["isopen", "description"])
        sess.query(Order).all()
        del o.isopen

        def go():
            assert o.isopen is None

        self.assert_sql_count(testing.db, go, 0)

        o.isopen = 14
        sess.expire(o)
        o.description = "another new description"
        sess.query(Order).all()
        assert o.isopen == 1
        assert o.description == "another new description"

    def test_expire_committed(self):
        """test that the committed state of the attribute receives the most
        recent DB data"""

        orders, Order = self.tables.orders, self.classes.Order

        self.mapper_registry.map_imperatively(Order, orders)

        sess = fixture_session(autoflush=False)
        o = sess.get(Order, 3)
        sess.expire(o)

        sess.execute(orders.update(), dict(description="order 3 modified"))
        assert o.isopen == 1
        assert (
            attributes.instance_state(o).dict["description"]
            == "order 3 modified"
        )

        def go():
            sess.flush()

        self.assert_sql_count(testing.db, go, 0)

    def test_expire_cascade(self):
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
                    Address, cascade="all, refresh-expire"
                )
            },
        )
        self.mapper_registry.map_imperatively(Address, addresses)
        s = fixture_session(autoflush=False)
        u = s.get(User, 8)
        assert u.addresses[0].email_address == "ed@wood.com"

        u.addresses[0].email_address = "someotheraddress"
        s.expire(u)
        assert u.addresses[0].email_address == "ed@wood.com"

    def test_refresh_cascade(self):
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
                    Address,
                    cascade="all, refresh-expire",
                    order_by=addresses.c.id,
                )
            },
        )
        self.mapper_registry.map_imperatively(Address, addresses)
        s = fixture_session(autoflush=False)
        u = s.get(User, 8)
        assert u.addresses[0].email_address == "ed@wood.com"

        u.addresses[0].email_address = "someotheraddress"
        s.refresh(u)
        assert u.addresses[0].email_address == "ed@wood.com"

    def test_expire_cascade_pending_orphan(self):
        cascade = "save-update, refresh-expire, delete, delete-orphan"
        self._test_cascade_to_pending(cascade, True)

    def test_refresh_cascade_pending_orphan(self):
        cascade = "save-update, refresh-expire, delete, delete-orphan"
        self._test_cascade_to_pending(cascade, False)

    def test_expire_cascade_pending(self):
        cascade = "save-update, refresh-expire"
        self._test_cascade_to_pending(cascade, True)

    def test_refresh_cascade_pending(self):
        cascade = "save-update, refresh-expire"
        self._test_cascade_to_pending(cascade, False)

    def _test_cascade_to_pending(self, cascade, expire_or_refresh):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={"addresses": relationship(Address, cascade=cascade)},
        )
        self.mapper_registry.map_imperatively(Address, addresses)
        s = fixture_session(autoflush=False)

        u = s.get(User, 8)
        a = Address(id=12, email_address="foobar")

        u.addresses.append(a)
        if expire_or_refresh:
            s.expire(u)
        else:
            s.refresh(u)
        if "delete-orphan" in cascade:
            assert a not in s
        else:
            assert a in s

        assert a not in u.addresses
        s.flush()

    def test_expired_lazy(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={"addresses": relationship(Address, backref="user")},
        )
        self.mapper_registry.map_imperatively(Address, addresses)

        sess = fixture_session()
        u = sess.get(User, 7)

        sess.expire(u)
        assert "name" not in u.__dict__
        assert "addresses" not in u.__dict__

        def go():
            assert u.addresses[0].email_address == "jack@bean.com"
            assert u.name == "jack"

        # two loads
        self.assert_sql_count(testing.db, go, 2)
        assert "name" in u.__dict__
        assert "addresses" in u.__dict__

    def test_expired_eager(self):
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
                    Address, backref="user", lazy="joined"
                )
            },
        )
        self.mapper_registry.map_imperatively(Address, addresses)

        sess = fixture_session()
        u = sess.get(User, 7)

        sess.expire(u)
        assert "name" not in u.__dict__
        assert "addresses" not in u.__dict__

        def go():
            assert u.addresses[0].email_address == "jack@bean.com"
            assert u.name == "jack"

        # one load, due to #1763 allows joinedload to
        # take over
        self.assert_sql_count(testing.db, go, 1)
        assert "name" in u.__dict__
        assert "addresses" in u.__dict__

        sess.expire(u, ["name", "addresses"])
        assert "name" not in u.__dict__
        assert "addresses" not in u.__dict__

        def go():
            sess.query(User).filter_by(id=7).one()
            assert u.addresses[0].email_address == "jack@bean.com"
            assert u.name == "jack"

        # one load, since relationship() + scalar are
        # together when eager load used with Query
        self.assert_sql_count(testing.db, go, 1)

    def test_unexpire_eager_dont_overwrite_related(self):
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
                    Address, backref="user", lazy="joined"
                )
            },
        )
        self.mapper_registry.map_imperatively(Address, addresses)

        sess = fixture_session(autoflush=False)
        u = sess.get(User, 7)

        a1 = u.addresses[0]
        eq_(a1.email_address, "jack@bean.com")

        sess.expire(u)
        a1.email_address = "foo"

        assert a1 in u.addresses
        eq_(a1.email_address, "foo")
        assert a1 in sess.dirty

    @testing.combinations(
        ("contains,joined",),
        ("contains,contains",),
    )
    def test_unexpire_eager_dont_include_contains_eager(self, case):
        """test #6449

        testing that contains_eager is downgraded to lazyload during
        a refresh, including if additional eager loaders are off the
        contains_eager

        """
        orders, Order, users, Address, addresses, User = (
            self.tables.orders,
            self.classes.Order,
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={"orders": relationship(Order, order_by=orders.c.id)},
        )
        self.mapper_registry.map_imperatively(
            Address, addresses, properties={"user": relationship(User)}
        )
        self.mapper_registry.map_imperatively(Order, orders)

        sess = fixture_session(autoflush=False)

        with self.sql_execution_asserter(testing.db) as asserter:
            if case == "contains,joined":
                a1 = (
                    sess.query(Address)
                    .join(Address.user)
                    .options(
                        contains_eager(Address.user).joinedload(User.orders)
                    )
                    .filter(Address.id == 1)
                    .one()
                )
            elif case == "contains,contains":
                # legacy query.first() can't be used here because it sets
                # limit 1 without the correct query wrapping.   1.3 has
                # the same problem though it renders differently
                a1 = (
                    sess.query(Address)
                    .join(Address.user)
                    .join(User.orders)
                    .order_by(Order.id)
                    .options(
                        contains_eager(Address.user).contains_eager(
                            User.orders
                        )
                    )
                    .filter(Address.id == 1)
                    .one()
                )

            eq_(
                a1,
                Address(
                    id=1,
                    user=User(
                        id=7, orders=[Order(id=1), Order(id=3), Order(id=5)]
                    ),
                ),
            )

        # ensure load with either contains_eager().joinedload() or
        # contains_eager().contains_eager() worked as expected
        asserter.assert_(CountStatements(1))

        # expire object, reset the session fully and re-add so that
        # the related User / Order objects are not in the identity map,
        # allows SQL count below to be deterministic
        sess.expire(a1)
        sess.close()
        sess.add(a1)

        # assert behavior on unexpire
        with self.sql_execution_asserter(testing.db) as asserter:
            a1.user
            assert "user" in a1.__dict__

            if case == "contains,joined":
                # joinedload took place
                assert "orders" in a1.user.__dict__
            elif case == "contains,contains":
                # contains eager is downgraded to a lazy load
                assert "orders" not in a1.user.__dict__

            eq_(
                a1,
                Address(
                    id=1,
                    user=User(
                        id=7, orders=[Order(id=1), Order(id=3), Order(id=5)]
                    ),
                ),
            )

        if case == "contains,joined":
            # the joinedloader for Address->User works,
            # so we get refresh(Address).lazyload(Address.user).
            # joinedload(User.order)
            asserter.assert_(CountStatements(2))
        elif case == "contains,contains":
            # both contains_eagers become normal loads so we get
            # refresh(Address).lazyload(Address.user).lazyload(User.order]
            asserter.assert_(CountStatements(3))

    def test_relationship_changes_preserved(self):
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
                    Address, backref="user", lazy="joined"
                )
            },
        )
        self.mapper_registry.map_imperatively(Address, addresses)
        sess = fixture_session(autoflush=False)
        u = sess.get(User, 8)
        sess.expire(u, ["name", "addresses"])
        u.addresses
        assert "name" not in u.__dict__
        del u.addresses[1]
        u.name
        assert "name" in u.__dict__
        assert len(u.addresses) == 2

    @testing.combinations(
        (True, False),
        (False, False),
        (False, True),
    )
    def test_skip_options_that_dont_match(self, test_control_case, do_expire):
        """test #7318"""

        User, Address, Order = self.classes("User", "Address", "Order")
        users, addresses, orders = self.tables("users", "addresses", "orders")

        self.mapper_registry.map_imperatively(Order, orders)

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address, backref="user", lazy="joined"
                ),
                "orders": relationship(Order),
            },
        )
        self.mapper_registry.map_imperatively(Address, addresses)
        sess = fixture_session()

        if test_control_case:
            # this would be the error we are skipping, make sure it happens
            # for up front
            with expect_raises_message(
                sa.exc.ArgumentError,
                r"Mapped class Mapper\[User\(users\)\] does not apply to "
                "any of the root entities in this query",
            ):
                row = sess.execute(
                    select(Order).options(joinedload(User.addresses))
                ).first()
        else:
            stmt = (
                select(User, Order)
                .join_from(User, Order)
                .options(joinedload(User.addresses))
                .order_by(User.id, Order.id)
            )

            row = sess.execute(stmt).first()

            u1, o1 = row
            if do_expire:
                sess.expire(o1)
            eq_(o1.description, "order 1")

    def test_mapper_joinedload_props_load(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        # changed in #1763, eager loaders are run when we unexpire

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address, backref="user", lazy="joined"
                )
            },
        )
        self.mapper_registry.map_imperatively(Address, addresses)
        sess = fixture_session()
        u = sess.get(User, 8)
        sess.expire(u)
        u.id

        assert "addresses" in u.__dict__
        u.addresses
        assert "addresses" in u.__dict__

    def test_options_joinedload_props_load(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        # changed in #1763, eager loaders are run when we unexpire

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={"addresses": relationship(Address, backref="user")},
        )
        self.mapper_registry.map_imperatively(Address, addresses)
        sess = fixture_session()
        u = sess.get(User, 8, options=[joinedload(User.addresses)])
        sess.expire(u)
        u.id
        assert "addresses" in u.__dict__
        u.addresses
        assert "addresses" in u.__dict__

    def test_joinedload_props_load_two(self):
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
                    Address, backref="user", lazy="joined"
                )
            },
        )
        self.mapper_registry.map_imperatively(Address, addresses)
        sess = fixture_session()
        u = sess.get(User, 8)
        sess.expire(u)

        # here, the lazy loader will encounter the attribute already
        # loaded when it goes to get the PK, so the loader itself
        # needs to no longer fire off.
        def go():
            u.addresses
            assert "addresses" in u.__dict__
            assert "id" in u.__dict__

        self.assert_sql_count(testing.db, go, 1)

    @testing.combinations(
        "selectin",
        "joined",
        "subquery",
        "immediate",
        "select",
        argnames="lazy",
    )
    @testing.variation(
        "as_option",
        [True, False],
    )
    @testing.variation(
        "expire_first",
        [
            True,
            False,
            "not_pk",
            (
                "not_pk_plus_pending",
                testing.requires.updateable_autoincrement_pks,
            ),
        ],
    )
    @testing.variation("include_column", [True, False, "no_attrs"])
    @testing.variation("autoflush", [True, False])
    def test_load_only_relationships(
        self, lazy, expire_first, include_column, as_option, autoflush
    ):
        """test #8703, #8997, a regression for #8996, and new feature
        for #9298."""

        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        if expire_first.not_pk_plus_pending:
            # if the test will be flushing a pk change, choose the user_id
            # that has no address rows so that we dont get an FK violation.
            # test only looks for the presence of "addresses" collection,
            # not the contents
            target_id = 10
            target_name = "chuck"
        else:
            # for all the other cases use user_id 8 where the addresses
            # collection has some elements.  this could theoretically catch
            # any odd per-row issues with the collection load
            target_id = 8
            target_name = "ed"

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address,
                    backref="user",
                    lazy=lazy if not as_option else "select",
                )
            },
        )
        self.mapper_registry.map_imperatively(Address, addresses)
        sess = fixture_session(autoflush=bool(autoflush))
        if as_option:
            fn = {
                "joined": joinedload,
                "selectin": selectinload,
                "subquery": subqueryload,
                "immediate": immediateload,
                "select": lazyload,
            }[lazy]

        u = sess.get(
            User,
            target_id,
            options=([fn(User.addresses)] if as_option else []),
        )

        if expire_first.not_pk_plus_pending:
            u.id = 25
            sess.expire(u, ["name", "addresses"])

            assert "addresses" not in u.__dict__
            assert "name" not in u.__dict__
            name_is_expired = True
        elif expire_first.not_pk:
            sess.expire(u, ["name", "addresses"])
            assert "id" in u.__dict__
            assert "addresses" not in u.__dict__
            assert "name" not in u.__dict__
            name_is_expired = True
        elif expire_first:
            sess.expire(u)
            assert "id" not in u.__dict__
            assert "addresses" not in u.__dict__
            assert "name" not in u.__dict__
            name_is_expired = True
        else:
            name_is_expired = False

        if (
            expire_first.not_pk_plus_pending
            and not autoflush
            and not include_column.no_attrs
        ):
            with expect_raises_message(
                sa_exc.InvalidRequestError,
                re.escape(
                    "Please flush pending primary key changes on attributes "
                    "{'id'} for mapper Mapper[User(users)] before proceeding "
                    "with a refresh"
                ),
            ):
                if include_column:
                    sess.refresh(u, ["name", "addresses"])
                else:
                    sess.refresh(u, ["addresses"])

            return

        with self.sql_execution_asserter(testing.db) as asserter:
            if include_column.no_attrs:
                sess.refresh(u)
                name_is_expired = False
                id_was_refreshed = True
            elif include_column:
                sess.refresh(u, ["name", "addresses"])
                name_is_expired = False
                id_was_refreshed = False
            else:
                sess.refresh(u, ["addresses"])
                id_was_refreshed = False

        expect_addresses = lazy != "select" or not include_column.no_attrs

        expected_count = 2 if (lazy != "joined" and expect_addresses) else 1
        if (
            autoflush
            and expire_first.not_pk_plus_pending
            and not id_was_refreshed
        ):
            expected_count += 1

        asserter.assert_(CountStatements(expected_count))

        # pk there for all cases
        assert "id" in u.__dict__

        if name_is_expired:
            assert "name" not in u.__dict__
        else:
            assert "name" in u.__dict__

        if expect_addresses:
            assert "addresses" in u.__dict__
        else:
            assert "addresses" not in u.__dict__

        u.addresses
        assert "addresses" in u.__dict__
        if include_column:
            eq_(u.__dict__["name"], target_name)

        if expire_first.not_pk_plus_pending and not id_was_refreshed:
            eq_(u.__dict__["id"], 25)
        else:
            eq_(u.__dict__["id"], target_id)

    @testing.variation("expire_first", [True, False])
    @testing.variation("autoflush", [True, False])
    @testing.variation("ensure_name_cleared", [True, False])
    @testing.requires.updateable_autoincrement_pks
    def test_no_pending_pks_on_refresh(
        self, expire_first, autoflush, ensure_name_cleared
    ):
        users = self.tables.users
        User = self.classes.User

        self.mapper_registry.map_imperatively(
            User,
            users,
        )
        sess = fixture_session(autoflush=bool(autoflush))

        u = sess.get(User, 10)

        u.id = 25

        if ensure_name_cleared:
            u.name = "newname"

        if expire_first:
            sess.expire(u, ["name"])

        if ensure_name_cleared and not expire_first:
            eq_(inspect(u).attrs.name.history, (["newname"], (), ["chuck"]))

        if not autoflush:
            with expect_raises_message(
                sa_exc.InvalidRequestError,
                re.escape(
                    "Please flush pending primary key changes on attributes "
                    "{'id'} for mapper Mapper[User(users)] before proceeding "
                    "with a refresh"
                ),
            ):
                sess.refresh(u, ["name"])

            # id was not expired
            eq_(inspect(u).attrs.id.history, ([25], (), [10]))

            # name was expired
            eq_(inspect(u).attrs.name.history, ((), (), ()))

        else:
            sess.refresh(u, ["name"])

            # new id value stayed
            eq_(u.__dict__["id"], 25)

            # was autoflushed
            eq_(inspect(u).attrs.id.history, ((), [25], ()))

            # new value for "name" is lost, value was refreshed
            eq_(inspect(u).attrs.name.history, ((), ["chuck"], ()))

    def test_expire_synonym(self):
        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(
            User, users, properties={"uname": sa.orm.synonym("name")}
        )

        sess = fixture_session()
        u = sess.get(User, 7)
        assert "name" in u.__dict__
        assert u.uname == u.name

        sess.expire(u)
        assert "name" not in u.__dict__

        sess.execute(users.update().where(users.c.id == 7), dict(name="jack2"))
        assert u.name == "jack2"
        assert u.uname == "jack2"
        assert "name" in u.__dict__

        # this won't work unless we add API hooks through the attr. system to
        # provide "expire" behavior on a synonym
        #    sess.expire(u, ['uname'])
        #    users.update(users.c.id==7).execute(name='jack3')
        #    assert u.uname == 'jack3'

    def test_partial_expire(self):
        orders, Order = self.tables.orders, self.classes.Order

        self.mapper_registry.map_imperatively(Order, orders)

        sess = fixture_session(autoflush=False)
        o = sess.get(Order, 3)

        sess.expire(o, attribute_names=["description"])
        assert "id" in o.__dict__
        assert "description" not in o.__dict__
        assert attributes.instance_state(o).dict["isopen"] == 1

        sess.execute(
            orders.update().where(orders.c.id == 3),
            dict(description="order 3 modified"),
        )

        def go():
            assert o.description == "order 3 modified"

        self.assert_sql_count(testing.db, go, 1)
        assert (
            attributes.instance_state(o).dict["description"]
            == "order 3 modified"
        )

        o.isopen = 5
        sess.expire(o, attribute_names=["description"])
        assert "id" in o.__dict__
        assert "description" not in o.__dict__
        assert o.__dict__["isopen"] == 5
        assert attributes.instance_state(o).committed_state["isopen"] == 1

        def go():
            assert o.description == "order 3 modified"

        self.assert_sql_count(testing.db, go, 1)
        assert o.__dict__["isopen"] == 5
        assert (
            attributes.instance_state(o).dict["description"]
            == "order 3 modified"
        )
        assert attributes.instance_state(o).committed_state["isopen"] == 1

        sess.flush()

        sess.expire(o, attribute_names=["id", "isopen", "description"])
        assert "id" not in o.__dict__
        assert "isopen" not in o.__dict__
        assert "description" not in o.__dict__

        def go():
            assert o.description == "order 3 modified"
            assert o.id == 3
            assert o.isopen == 5

        self.assert_sql_count(testing.db, go, 1)

    def test_partial_expire_lazy(self):
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
                    Address, backref="user", order_by=addresses.c.id
                )
            },
        )
        self.mapper_registry.map_imperatively(Address, addresses)

        sess = fixture_session(autoflush=False)
        u = sess.get(User, 8)

        sess.expire(u, ["name", "addresses"])
        assert "name" not in u.__dict__
        assert "addresses" not in u.__dict__

        # hit the lazy loader.  just does the lazy load,
        # doesn't do the overall refresh
        def go():
            assert u.addresses[0].email_address == "ed@wood.com"

        self.assert_sql_count(testing.db, go, 1)

        assert "name" not in u.__dict__

        # check that mods to expired lazy-load attributes
        # only do the lazy load
        sess.expire(u, ["name", "addresses"])

        def go():
            u.addresses = [Address(id=10, email_address="foo@bar.com")]

        self.assert_sql_count(testing.db, go, 1)

        sess.flush()

        # flush has occurred, and addresses was modified,
        # so the addresses collection got committed and is
        # longer expired
        def go():
            assert u.addresses[0].email_address == "foo@bar.com"
            assert len(u.addresses) == 1

        self.assert_sql_count(testing.db, go, 0)

        # but the name attribute was never loaded and so
        # still loads
        def go():
            assert u.name == "ed"

        self.assert_sql_count(testing.db, go, 1)

    def test_partial_expire_eager(self):
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
                    Address, backref="user", lazy="joined"
                )
            },
        )
        self.mapper_registry.map_imperatively(Address, addresses)

        sess = fixture_session(autoflush=False)
        u = sess.get(User, 8)

        sess.expire(u, ["name", "addresses"])
        assert "name" not in u.__dict__
        assert "addresses" not in u.__dict__

        def go():
            assert u.addresses[0].email_address == "ed@wood.com"

        self.assert_sql_count(testing.db, go, 1)

        # check that mods to expired eager-load attributes
        # do the refresh
        sess.expire(u, ["name", "addresses"])

        def go():
            u.addresses = [Address(id=10, email_address="foo@bar.com")]

        self.assert_sql_count(testing.db, go, 1)
        sess.flush()

        # this should ideally trigger the whole load
        # but currently it works like the lazy case
        def go():
            assert u.addresses[0].email_address == "foo@bar.com"
            assert len(u.addresses) == 1

        self.assert_sql_count(testing.db, go, 0)

        def go():
            assert u.name == "ed"

        # scalar attributes have their own load
        self.assert_sql_count(testing.db, go, 1)
        # ideally, this was already loaded, but we aren't
        # doing it that way right now
        # self.assert_sql_count(testing.db, go, 0)

    def test_relationships_load_on_query(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={"addresses": relationship(Address, backref="user")},
        )
        self.mapper_registry.map_imperatively(Address, addresses)

        sess = fixture_session(autoflush=False)
        u = sess.get(User, 8)
        assert "name" in u.__dict__
        u.addresses
        assert "addresses" in u.__dict__

        sess.expire(u, ["name", "addresses"])
        assert "name" not in u.__dict__
        assert "addresses" not in u.__dict__
        (
            sess.query(User)
            .options(sa.orm.joinedload(User.addresses))
            .filter_by(id=8)
            .all()
        )
        assert "name" in u.__dict__
        assert "addresses" in u.__dict__

    def test_partial_expire_deferred(self):
        orders, Order = self.tables.orders, self.classes.Order

        self.mapper_registry.map_imperatively(
            Order,
            orders,
            properties={"description": sa.orm.deferred(orders.c.description)},
        )

        sess = fixture_session(autoflush=False)
        o = sess.get(Order, 3)
        sess.expire(o, ["description", "isopen"])
        assert "isopen" not in o.__dict__
        assert "description" not in o.__dict__

        # test that expired attribute access does not refresh
        # the deferred
        def go():
            assert o.isopen == 1
            assert o.description == "order 3"

        # requires two statements
        self.assert_sql_count(testing.db, go, 2)

        sess.expire(o, ["description", "isopen"])
        assert "isopen" not in o.__dict__
        assert "description" not in o.__dict__
        # test that the deferred attribute does not trigger the full
        # reload

        def go():
            assert o.description == "order 3"
            assert o.isopen == 1

        self.assert_sql_count(testing.db, go, 2)

        sa.orm.clear_mappers()

        self.mapper_registry.map_imperatively(Order, orders)
        sess.expunge_all()

        # same tests, using deferred at the options level
        o = sess.get(Order, 3, options=[sa.orm.defer(Order.description)])

        assert "description" not in o.__dict__

        # sanity check
        def go():
            assert o.description == "order 3"

        self.assert_sql_count(testing.db, go, 1)

        assert "description" in o.__dict__
        assert "isopen" in o.__dict__
        sess.expire(o, ["description", "isopen"])
        assert "isopen" not in o.__dict__
        assert "description" not in o.__dict__

        # test that expired attribute access refreshes
        # the deferred
        def go():
            assert o.isopen == 1
            assert o.description == "order 3"

        self.assert_sql_count(testing.db, go, 1)
        sess.expire(o, ["description", "isopen"])

        assert "isopen" not in o.__dict__
        assert "description" not in o.__dict__
        # test that the deferred attribute triggers the full
        # reload

        def go():
            assert o.description == "order 3"
            assert o.isopen == 1

        self.assert_sql_count(testing.db, go, 1)

    def test_joinedload_query_refreshes(self):
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
                    Address, backref="user", lazy="joined"
                )
            },
        )
        self.mapper_registry.map_imperatively(Address, addresses)

        sess = fixture_session(autoflush=False)
        u = sess.get(User, 8)
        assert len(u.addresses) == 3
        sess.expire(u)
        assert "addresses" not in u.__dict__
        sess.query(User).filter_by(id=8).all()
        assert "addresses" in u.__dict__
        assert len(u.addresses) == 3

    @testing.requires.predictable_gc
    def test_expire_all(self):
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
                    Address,
                    backref="user",
                    lazy="joined",
                    order_by=addresses.c.id,
                )
            },
        )
        self.mapper_registry.map_imperatively(Address, addresses)

        sess = fixture_session(autoflush=False)
        userlist = sess.query(User).order_by(User.id).all()
        eq_(self.static.user_address_result, userlist)
        eq_(len(list(sess)), 9)
        sess.expire_all()
        gc_collect()
        eq_(len(list(sess)), 4)  # since addresses were gc'ed

        userlist = sess.query(User).order_by(User.id).all()
        eq_(self.static.user_address_result, userlist)
        eq_(len(list(sess)), 9)

    def test_state_change_col_to_deferred(self):
        """Behavioral test to verify the current activity of loader
        callables

        """

        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)

        sess = fixture_session(autoflush=False)

        # deferred attribute option, gets the LoadDeferredColumns
        # callable
        u1 = sess.query(User).options(defer(User.name)).first()
        assert isinstance(
            attributes.instance_state(u1).callables["name"],
            strategies._LoadDeferredColumns,
        )

        # expire the attr, it gets the InstanceState callable
        sess.expire(u1, ["name"])
        assert "name" in attributes.instance_state(u1).expired_attributes
        assert "name" not in attributes.instance_state(u1).callables

        # load it, callable is gone
        u1.name
        assert "name" not in attributes.instance_state(u1).expired_attributes
        assert "name" not in attributes.instance_state(u1).callables

        # same for expire all
        sess.expunge_all()
        u1 = sess.query(User).options(defer(User.name)).first()
        sess.expire(u1)
        assert "name" in attributes.instance_state(u1).expired_attributes
        assert "name" not in attributes.instance_state(u1).callables

        # load over it.  everything normal.
        sess.query(User).first()
        assert "name" not in attributes.instance_state(u1).expired_attributes
        assert "name" not in attributes.instance_state(u1).callables

        sess.expunge_all()
        u1 = sess.query(User).first()
        # for non present, still expires the same way
        del u1.name
        sess.expire(u1)
        assert "name" in attributes.instance_state(u1).expired_attributes
        assert "name" not in attributes.instance_state(u1).callables

    def test_state_deferred_to_col(self):
        """Behavioral test to verify the current activity of
        loader callables

        """

        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(
            User, users, properties={"name": deferred(users.c.name)}
        )

        sess = fixture_session(autoflush=False)
        u1 = sess.query(User).options(undefer(User.name)).first()
        assert "name" not in attributes.instance_state(u1).callables

        # mass expire, the attribute was loaded,
        # the attribute gets the callable
        sess.expire(u1)
        assert "name" in attributes.instance_state(u1).expired_attributes
        assert "name" not in attributes.instance_state(u1).callables

        # load it
        u1.name
        assert "name" not in attributes.instance_state(u1).expired_attributes
        assert "name" not in attributes.instance_state(u1).callables

        # mass expire, attribute was loaded but then deleted,
        # the callable goes away - the state wants to flip
        # it back to its "deferred" loader.
        sess.expunge_all()
        u1 = sess.query(User).options(undefer(User.name)).first()
        del u1.name
        sess.expire(u1)
        assert "name" in attributes.instance_state(u1).expired_attributes
        assert "name" not in attributes.instance_state(u1).callables

        # single attribute expire, the attribute gets the callable
        sess.expunge_all()
        u1 = sess.query(User).options(undefer(User.name)).first()
        sess.expire(u1, ["name"])

        # the expire cancels the undefer
        assert "name" in attributes.instance_state(u1).expired_attributes
        assert "name" not in attributes.instance_state(u1).callables

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

    def test_deferred_expire_w_transient_to_detached(self):
        orders, Order = self.tables.orders, self.classes.Order
        self.mapper_registry.map_imperatively(
            Order,
            orders,
            properties={"description": deferred(orders.c.description)},
        )

        s = fixture_session()
        item = Order(id=1)

        make_transient_to_detached(item)
        s.add(item)
        item.isopen
        assert "description" not in item.__dict__

    def test_deferred_expire_normally(self):
        orders, Order = self.tables.orders, self.classes.Order
        self.mapper_registry.map_imperatively(
            Order,
            orders,
            properties={"description": deferred(orders.c.description)},
        )

        s = fixture_session()

        item = s.query(Order).first()
        s.expire(item)
        item.isopen
        assert "description" not in item.__dict__

    def test_deferred_expire_explicit_attrs(self):
        orders, Order = self.tables.orders, self.classes.Order
        self.mapper_registry.map_imperatively(
            Order,
            orders,
            properties={"description": deferred(orders.c.description)},
        )

        s = fixture_session()

        item = s.query(Order).first()
        s.expire(item, ["isopen", "description"])
        item.isopen
        assert "description" not in item.__dict__


class PolymorphicExpireTest(fixtures.MappedTest):
    run_inserts = "once"
    run_deletes = None

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

        Table(
            "engineers",
            metadata,
            Column(
                "person_id",
                Integer,
                ForeignKey("people.person_id"),
                primary_key=True,
            ),
            Column("status", String(30)),
        )

    @classmethod
    def setup_classes(cls):
        class Person(cls.Basic):
            pass

        class Engineer(Person):
            pass

    @classmethod
    def insert_data(cls, connection):
        people, engineers = cls.tables.people, cls.tables.engineers

        connection.execute(
            people.insert(),
            [
                {"person_id": 1, "name": "person1", "type": "person"},
                {"person_id": 2, "name": "engineer1", "type": "engineer"},
                {"person_id": 3, "name": "engineer2", "type": "engineer"},
            ],
        )
        connection.execute(
            engineers.insert(),
            [
                {"person_id": 2, "status": "new engineer"},
                {"person_id": 3, "status": "old engineer"},
            ],
        )

    @classmethod
    def setup_mappers(cls):
        Person, people, engineers, Engineer = (
            cls.classes.Person,
            cls.tables.people,
            cls.tables.engineers,
            cls.classes.Engineer,
        )

        cls.mapper_registry.map_imperatively(
            Person,
            people,
            polymorphic_on=people.c.type,
            polymorphic_identity="person",
        )
        cls.mapper_registry.map_imperatively(
            Engineer,
            engineers,
            inherits=Person,
            polymorphic_identity="engineer",
        )

    def test_poly_deferred(self):
        Person, people, Engineer = (
            self.classes.Person,
            self.tables.people,
            self.classes.Engineer,
        )

        sess = fixture_session(autoflush=False)
        [p1, e1, e2] = sess.query(Person).order_by(people.c.person_id).all()

        sess.expire(p1)
        sess.expire(e1, ["status"])
        sess.expire(e2)

        for p in [p1, e2]:
            assert "name" not in p.__dict__

        assert "name" in e1.__dict__
        assert "status" not in e2.__dict__
        assert "status" not in e1.__dict__

        e1.name = "new engineer name"

        def go():
            sess.query(Person).all()

        self.assert_sql_count(testing.db, go, 1)

        for p in [p1, e1, e2]:
            assert "name" in p.__dict__

        assert "status" not in e2.__dict__
        assert "status" not in e1.__dict__

        def go():
            assert e1.name == "new engineer name"
            assert e2.name == "engineer2"
            assert e1.status == "new engineer"
            assert e2.status == "old engineer"

        self.assert_sql_count(testing.db, go, 2)
        eq_(
            Engineer.name.get_history(e1),
            (["new engineer name"], (), ["engineer1"]),
        )

    def test_no_instance_key(self):
        Engineer = self.classes.Engineer

        sess = fixture_session(autoflush=False)
        e1 = sess.get(Engineer, 2)

        sess.expire(e1, attribute_names=["name"])
        sess.expunge(e1)
        attributes.instance_state(e1).key = None
        assert "name" not in e1.__dict__
        sess.add(e1)
        assert e1.name == "engineer1"

    def test_no_instance_key_pk_absent(self):
        Engineer = self.classes.Engineer

        # same as test_no_instance_key, but the PK columns
        # are absent.  ensure an error is raised.
        sess = fixture_session(autoflush=False)
        e1 = sess.get(Engineer, 2)

        sess.expire(e1, attribute_names=["name", "person_id"])
        sess.expunge(e1)
        attributes.instance_state(e1).key = None
        assert "name" not in e1.__dict__
        sess.add(e1)
        assert_raises(sa_exc.InvalidRequestError, getattr, e1, "name")


class ExpiredPendingTest(_fixtures.FixtureTest):
    run_define_tables = "once"
    run_setup_classes = "once"
    run_setup_mappers = None
    run_inserts = None

    def test_expired_pending(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={"addresses": relationship(Address, backref="user")},
        )
        self.mapper_registry.map_imperatively(Address, addresses)

        sess = fixture_session(autoflush=False, future=True)
        a1 = Address(email_address="a1")
        sess.add(a1)
        sess.flush()

        u1 = User(name="u1")
        a1.user = u1
        sess.flush()

        # expire 'addresses'.  backrefs
        # which attach to u1 will expect to be "pending"
        sess.expire(u1, ["addresses"])

        # attach an Address.  now its "pending"
        # in user.addresses
        a2 = Address(email_address="a2")
        a2.user = u1

        # needed now that cascade backrefs is disabled
        sess.add(a2)

        # expire u1.addresses again.  this expires
        # "pending" as well.
        sess.expire(u1, ["addresses"])

        # insert a new row
        sess.execute(
            addresses.insert(), dict(email_address="a3", user_id=u1.id)
        )

        # only two addresses pulled from the DB, no "pending"
        assert len(u1.addresses) == 2

        sess.flush()
        sess.expire_all()
        assert len(u1.addresses) == 3


class LifecycleTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "data",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(30)),
        )
        Table(
            "data_fetched",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(30), FetchedValue()),
        )
        Table(
            "data_defer",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(30)),
            Column("data2", String(30)),
        )

    @classmethod
    def setup_classes(cls):
        class Data(cls.Comparable):
            pass

        class DataFetched(cls.Comparable):
            pass

        class DataDefer(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        cls.mapper_registry.map_imperatively(cls.classes.Data, cls.tables.data)
        cls.mapper_registry.map_imperatively(
            cls.classes.DataFetched,
            cls.tables.data_fetched,
            eager_defaults=False,
        )
        cls.mapper_registry.map_imperatively(
            cls.classes.DataDefer,
            cls.tables.data_defer,
            properties={"data": deferred(cls.tables.data_defer.c.data)},
        )

    def test_attr_not_inserted(self):
        Data = self.classes.Data

        sess = fixture_session()

        d1 = Data()
        sess.add(d1)
        sess.flush()

        # we didn't insert a value for 'data',
        # so its not in dict, but also when we hit it, it isn't
        # expired because there's no column default on it or anything like that
        assert "data" not in d1.__dict__

        def go():
            eq_(d1.data, None)

        self.assert_sql_count(testing.db, go, 0)

    def test_attr_not_inserted_expired(self):
        Data = self.classes.Data

        sess = fixture_session(autoflush=False)

        d1 = Data()
        sess.add(d1)
        sess.flush()

        assert "data" not in d1.__dict__

        # with an expire, we emit
        sess.expire(d1)

        def go():
            eq_(d1.data, None)

        self.assert_sql_count(testing.db, go, 1)

    def test_attr_not_inserted_fetched(self):
        Data = self.classes.DataFetched

        sess = fixture_session()

        d1 = Data()
        sess.add(d1)
        sess.flush()

        assert "data" not in d1.__dict__

        def go():
            eq_(d1.data, None)

        self.assert_sql_count(testing.db, go, 1)

    def test_cols_missing_in_load(self):
        Data = self.classes.Data

        with Session(testing.db) as sess, sess.begin():
            d1 = Data(data="d1")
            sess.add(d1)

        sess = fixture_session()
        d1 = sess.query(Data).from_statement(select(Data.id)).first()

        # cols not present in the row are implicitly expired
        def go():
            eq_(d1.data, "d1")

        self.assert_sql_count(testing.db, go, 1)

    def test_deferred_cols_missing_in_load_state_reset(self):
        Data = self.classes.DataDefer

        with Session(testing.db) as sess, sess.begin():
            d1 = Data(data="d1")
            sess.add(d1)

        with Session(testing.db) as sess:
            d1 = (
                sess.query(Data)
                .from_statement(select(Data.id))
                .options(undefer(Data.data))
                .first()
            )
            d1.data = "d2"

        # the deferred loader has to clear out any state
        # on the col, including that 'd2' here
        d1 = sess.query(Data).populate_existing().first()

        def go():
            eq_(d1.data, "d1")

        self.assert_sql_count(testing.db, go, 1)


class RefreshTest(_fixtures.FixtureTest):
    def test_refresh(self):
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
                    backref="user",
                )
            },
        )
        s = fixture_session(autoflush=False)
        u = s.get(User, 7)
        u.name = "foo"
        a = Address()
        assert sa.orm.object_session(a) is None
        u.addresses.append(a)
        assert a.email_address is None
        assert id(a) in [id(x) for x in u.addresses]

        s.refresh(u)

        # its refreshed, so not dirty
        assert u not in s.dirty

        # username is back to the DB
        assert u.name == "jack"

        assert id(a) not in [id(x) for x in u.addresses]

        u.name = "foo"
        u.addresses.append(a)
        # now its dirty
        assert u in s.dirty
        assert u.name == "foo"
        assert id(a) in [id(x) for x in u.addresses]
        s.expire(u)

        # get the attribute, it refreshes
        assert u.name == "jack"
        assert id(a) not in [id(x) for x in u.addresses]

    def test_persistence_check(self):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)
        s = fixture_session()
        u = s.get(User, 7)
        s.expunge_all()
        assert_raises_message(
            sa_exc.InvalidRequestError,
            r"is not persistent within this Session",
            lambda: s.refresh(u),
        )

    def test_refresh_autoflush(self):
        User, users = self.classes.User, self.tables.users
        Address, addresses = self.classes.Address, self.tables.addresses

        self.mapper_registry.map_imperatively(User, users)
        self.mapper_registry.map_imperatively(
            Address, addresses, properties={"user": relationship(User)}
        )

        s = fixture_session()

        a1 = s.get(Address, 2)
        u1 = s.get(User, 7)
        a1.user = u1

        s.refresh(a1, ["user_id"])

        # autoflushes
        eq_(a1.user_id, 7)

    def test_refresh_expired(self):
        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(User, users)
        s = fixture_session()
        u = s.get(User, 7)
        s.expire(u)
        assert "name" not in u.__dict__
        s.refresh(u)
        assert u.name == "jack"

    def test_refresh_with_lazy(self):
        """test that when a lazy loader is set as a trigger on an object's
        attribute (at the attribute level, not the class level), a refresh()
        operation doesn't fire the lazy loader or create any problems"""

        User, Address, addresses, users = (
            self.classes.User,
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
        )

        s = fixture_session()
        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "addresses": relationship(
                    self.mapper_registry.map_imperatively(Address, addresses)
                )
            },
        )
        q = s.query(User).options(sa.orm.lazyload(User.addresses))
        u = q.filter(users.c.id == 8).first()

        def go():
            s.refresh(u)

        self.assert_sql_count(testing.db, go, 1)

    def test_refresh_with_eager(self):
        """test that a refresh/expire operation loads rows properly and sends
        correct "isnew" state to eager loaders"""

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
                    lazy="joined",
                )
            },
        )

        s = fixture_session()
        u = s.get(User, 8)
        assert len(u.addresses) == 3
        s.refresh(u)
        assert len(u.addresses) == 3

        s = fixture_session()
        u = s.get(User, 8)
        assert len(u.addresses) == 3
        s.expire(u)
        assert len(u.addresses) == 3

    def test_refresh_maintains_deferred_options(self):
        # testing a behavior that may have changed with
        # [ticket:3822]
        User, Address, Dingaling = self.classes("User", "Address", "Dingaling")
        users, addresses, dingalings = self.tables(
            "users", "addresses", "dingalings"
        )

        self.mapper_registry.map_imperatively(
            User, users, properties={"addresses": relationship(Address)}
        )

        self.mapper_registry.map_imperatively(
            Address,
            addresses,
            properties={"dingalings": relationship(Dingaling)},
        )

        self.mapper_registry.map_imperatively(Dingaling, dingalings)

        s = fixture_session()
        q = (
            s.query(User)
            .filter_by(name="fred")
            .options(
                sa.orm.lazyload(User.addresses).joinedload(Address.dingalings)
            )
        )

        u1 = q.one()

        # "addresses" is not present on u1, but when u1.addresses
        # lazy loads, it should also joinedload dingalings.  This is
        # present in state.load_options and state.load_path.   The
        # refresh operation should not reset these attributes.
        s.refresh(u1)

        def go():
            eq_(
                u1.addresses,
                [
                    Address(
                        email_address="fred@fred.com",
                        dingalings=[Dingaling(data="ding 2/5")],
                    )
                ],
            )

        self.assert_sql_count(testing.db, go, 1)

    def test_refresh2(self):
        """test a hang condition that was occurring on expire/refresh"""

        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        s = fixture_session()
        self.mapper_registry.map_imperatively(Address, addresses)

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties=dict(
                addresses=relationship(
                    Address, cascade="all, delete-orphan", lazy="joined"
                )
            ),
        )

        u = User()
        u.name = "Justin"
        a = Address(id=10, email_address="lala")
        u.addresses.append(a)

        s.add(u)
        s.flush()
        s.expunge_all()
        u = s.query(User).filter(User.name == "Justin").one()

        s.expire(u)
        assert u.name == "Justin"

        s.refresh(u)
