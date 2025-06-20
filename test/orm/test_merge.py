import operator

import sqlalchemy as sa
from sqlalchemy import and_
from sqlalchemy import case
from sqlalchemy import event
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import PickleType
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import Text
from sqlalchemy.orm import attributes
from sqlalchemy.orm import backref
from sqlalchemy.orm import configure_mappers
from sqlalchemy.orm import defer
from sqlalchemy.orm import deferred
from sqlalchemy.orm import foreign
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import relationship
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import Session
from sqlalchemy.orm import synonym
from sqlalchemy.orm.collections import attribute_keyed_dict
from sqlalchemy.orm.interfaces import MapperOption
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_warnings
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import in_
from sqlalchemy.testing import not_in
from sqlalchemy.testing.assertsql import CountStatements
from sqlalchemy.testing.entities import ComparableEntity
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from test.orm import _fixtures


class MergeTest(_fixtures.FixtureTest):
    """Session.merge() functionality"""

    run_inserts = None

    def load_tracker(self, cls, canary=None):
        if canary is None:

            def canary(instance, *args):
                canary.called += 1

            canary.called = 0

        event.listen(cls, "load", canary)

        return canary

    def test_loader_options(self):
        User, Address, addresses, users = (
            self.classes.User,
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
        )

        self.mapper(
            User,
            users,
            properties={"addresses": relationship(Address, backref="user")},
        )
        self.mapper(Address, addresses)

        s = fixture_session()
        u = User(
            id=7,
            name="fred",
            addresses=[Address(id=1, email_address="jack@bean.com")],
        )
        s.add(u)
        s.commit()
        s.close()

        u = User(id=7, name="fred")
        u2 = s.merge(u, options=[selectinload(User.addresses)])

        eq_(len(u2.__dict__["addresses"]), 1)

    def test_transient_to_pending(self):
        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(User, users)
        sess = fixture_session()
        load = self.load_tracker(User)

        u = User(id=7, name="fred")
        eq_(load.called, 0)
        u2 = sess.merge(u)
        eq_(load.called, 1)
        assert u2 in sess
        eq_(u2, User(id=7, name="fred"))
        sess.flush()
        sess.expunge_all()
        eq_(sess.query(User).first(), User(id=7, name="fred"))

    def test_transient_to_pending_no_pk(self):
        """test that a transient object with no PK attribute
        doesn't trigger a needless load."""

        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(User, users)
        sess = fixture_session()
        u = User(name="fred")

        def go():
            sess.merge(u)

        self.assert_sql_count(testing.db, go, 0)

    def test_warn_transient_already_pending_nopk(self):
        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(User, users)
        sess = fixture_session(autoflush=False)
        u = User(name="fred")

        sess.add(u)

        with expect_warnings(
            "Instance <User.*> is already pending in this Session yet is "
            "being merged again; this is probably not what you want to do"
        ):
            sess.merge(u)

    def test_warn_transient_already_pending_pk(self):
        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(User, users)
        sess = fixture_session(autoflush=False)
        u = User(id=1, name="fred")

        sess.add(u)

        with expect_warnings(
            "Instance <User.*> is already pending in this Session yet is "
            "being merged again; this is probably not what you want to do"
        ):
            sess.merge(u)

    def test_transient_to_pending_collection(self):
        User, Address, addresses, users = (
            self.classes.User,
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address, backref="user", collection_class=set
                )
            },
        )
        self.mapper_registry.map_imperatively(Address, addresses)
        load = self.load_tracker(User)
        self.load_tracker(Address, load)

        u = User(
            id=7,
            name="fred",
            addresses={
                Address(id=1, email_address="fred1"),
                Address(id=2, email_address="fred2"),
            },
        )
        eq_(load.called, 0)

        sess = fixture_session()
        sess.merge(u)
        eq_(load.called, 3)

        merged_users = [e for e in sess if isinstance(e, User)]
        eq_(len(merged_users), 1)
        assert merged_users[0] is not u

        sess.flush()
        sess.expunge_all()

        eq_(
            sess.query(User).one(),
            User(
                id=7,
                name="fred",
                addresses={
                    Address(id=1, email_address="fred1"),
                    Address(id=2, email_address="fred2"),
                },
            ),
        )

    def test_transient_non_mutated_collection(self):
        User, Address, addresses, users = (
            self.classes.User,
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={"addresses": relationship(Address, backref="user")},
        )
        self.mapper_registry.map_imperatively(Address, addresses)

        s = fixture_session()
        u = User(
            id=7,
            name="fred",
            addresses=[Address(id=1, email_address="jack@bean.com")],
        )
        s.add(u)
        s.commit()
        s.close()

        u = User(id=7, name="fred")
        # access address collection to get implicit blank collection
        eq_(u.addresses, [])
        u2 = s.merge(u)

        # collection wasn't emptied
        eq_(u2.addresses, [Address()])

    def test_transient_to_pending_collection_pk_none(self):
        User, Address, addresses, users = (
            self.classes.User,
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address, backref="user", collection_class=set
                )
            },
        )
        self.mapper_registry.map_imperatively(Address, addresses)
        load = self.load_tracker(User)
        self.load_tracker(Address, load)

        u = User(
            id=None,
            name="fred",
            addresses={
                Address(id=None, email_address="fred1"),
                Address(id=None, email_address="fred2"),
            },
        )
        eq_(load.called, 0)

        sess = fixture_session()
        sess.merge(u)
        eq_(load.called, 3)

        merged_users = [e for e in sess if isinstance(e, User)]
        eq_(len(merged_users), 1)
        assert merged_users[0] is not u

        sess.flush()
        sess.expunge_all()

        eq_(
            sess.query(User).one(),
            User(
                name="fred",
                addresses={
                    Address(email_address="fred1"),
                    Address(email_address="fred2"),
                },
            ),
        )

    def test_transient_to_persistent(self):
        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(User, users)
        load = self.load_tracker(User)

        sess = fixture_session()
        u = User(id=7, name="fred")
        sess.add(u)
        sess.flush()
        sess.expunge_all()

        eq_(load.called, 0)

        _u2 = u2 = User(id=7, name="fred jones")
        eq_(load.called, 0)
        u2 = sess.merge(u2)
        assert u2 is not _u2
        eq_(load.called, 1)
        sess.flush()
        sess.expunge_all()
        eq_(sess.query(User).first(), User(id=7, name="fred jones"))
        eq_(load.called, 2)

    def test_transient_to_persistent_collection(self):
        User, Address, addresses, users = (
            self.classes.User,
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address,
                    backref="user",
                    collection_class=set,
                    cascade="all, delete-orphan",
                )
            },
        )
        self.mapper_registry.map_imperatively(Address, addresses)

        load = self.load_tracker(User)
        self.load_tracker(Address, load)

        u = User(
            id=7,
            name="fred",
            addresses={
                Address(id=1, email_address="fred1"),
                Address(id=2, email_address="fred2"),
            },
        )
        sess = fixture_session()
        sess.add(u)
        sess.flush()
        sess.expunge_all()

        eq_(load.called, 0)

        u = User(
            id=7,
            name="fred",
            addresses={
                Address(id=3, email_address="fred3"),
                Address(id=4, email_address="fred4"),
            },
        )

        u = sess.merge(u)

        # 1. merges User object.  updates into session.
        # 2.,3. merges Address ids 3 & 4, saves into session.
        # 4.,5. loads pre-existing elements in "addresses" collection,
        # marks as deleted, Address ids 1 and 2.
        eq_(load.called, 5)

        eq_(
            u,
            User(
                id=7,
                name="fred",
                addresses={
                    Address(id=3, email_address="fred3"),
                    Address(id=4, email_address="fred4"),
                },
            ),
        )
        sess.flush()
        sess.expunge_all()
        eq_(
            sess.query(User).one(),
            User(
                id=7,
                name="fred",
                addresses={
                    Address(id=3, email_address="fred3"),
                    Address(id=4, email_address="fred4"),
                },
            ),
        )

    def test_detached_to_persistent_collection(self):
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
                    order_by=addresses.c.id,
                    collection_class=set,
                )
            },
        )
        self.mapper_registry.map_imperatively(Address, addresses)
        load = self.load_tracker(User)
        self.load_tracker(Address, load)

        a = Address(id=1, email_address="fred1")
        u = User(
            id=7,
            name="fred",
            addresses={a, Address(id=2, email_address="fred2")},
        )
        sess = fixture_session()
        sess.add(u)
        sess.flush()
        sess.expunge_all()

        u.name = "fred jones"
        u.addresses.add(Address(id=3, email_address="fred3"))
        u.addresses.remove(a)

        eq_(load.called, 0)
        u = sess.merge(u)
        eq_(load.called, 4)
        sess.flush()
        sess.expunge_all()

        eq_(
            sess.query(User).first(),
            User(
                id=7,
                name="fred jones",
                addresses={
                    Address(id=2, email_address="fred2"),
                    Address(id=3, email_address="fred3"),
                },
            ),
        )

    def test_unsaved_cascade(self):
        """Merge of a transient entity with two child transient
        entities, with a bidirectional relationship."""

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
                    cascade="all",
                    backref="user",
                )
            },
        )
        load = self.load_tracker(User)
        self.load_tracker(Address, load)
        sess = fixture_session()

        u = User(id=7, name="fred")
        a1 = Address(email_address="foo@bar.com")
        a2 = Address(email_address="hoho@bar.com")
        u.addresses.append(a1)
        u.addresses.append(a2)

        u2 = sess.merge(u)
        eq_(load.called, 3)

        eq_(
            u,
            User(
                id=7,
                name="fred",
                addresses=[
                    Address(email_address="foo@bar.com"),
                    Address(email_address="hoho@bar.com"),
                ],
            ),
        )
        eq_(
            u2,
            User(
                id=7,
                name="fred",
                addresses=[
                    Address(email_address="foo@bar.com"),
                    Address(email_address="hoho@bar.com"),
                ],
            ),
        )

        sess.flush()
        sess.expunge_all()
        u2 = sess.get(User, 7)

        eq_(
            u2,
            User(
                id=7,
                name="fred",
                addresses=[
                    Address(email_address="foo@bar.com"),
                    Address(email_address="hoho@bar.com"),
                ],
            ),
        )
        eq_(load.called, 6)

    def test_merge_empty_attributes(self):
        User, dingalings = self.classes.User, self.tables.dingalings

        self.mapper_registry.map_imperatively(User, dingalings)

        sess = fixture_session(autoflush=False)

        # merge empty stuff.  goes in as NULL.
        # not sure what this was originally trying to
        # test.
        u1 = sess.merge(User(id=1))
        sess.flush()
        assert u1.data is None

        # save another user with "data"
        u2 = User(id=2, data="foo")
        sess.add(u2)
        sess.flush()

        # merge User on u2's pk with
        # no "data".
        # value isn't whacked from the destination
        # dict.
        u3 = sess.merge(User(id=2))
        eq_(u3.__dict__["data"], "foo")

        # make a change.
        u3.data = "bar"

        # merge another no-"data" user.
        # attribute maintains modified state.
        # (usually autoflush would have happened
        # here anyway).
        u4 = sess.merge(User(id=2))  # noqa
        eq_(u3.__dict__["data"], "bar")

        sess.flush()
        # and after the flush.
        eq_(u3.data, "bar")

        # new row.
        u5 = User(id=3, data="foo")
        sess.add(u5)
        sess.flush()

        # blow it away from u5, but don't
        # mark as expired.  so it would just
        # be blank.
        del u5.data

        # the merge adds expiry to the
        # attribute so that it loads.
        # not sure if I like this - it currently is needed
        # for test_pickled:PickleTest.test_instance_deferred_cols
        u6 = sess.merge(User(id=3))
        assert "data" not in u6.__dict__
        assert u6.data == "foo"

        # set it to None.  this is actually
        # a change so gets preserved.
        u6.data = None
        u7 = sess.merge(User(id=3))  # noqa
        assert u6.__dict__["data"] is None

    def test_merge_irregular_collection(self):
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
                    collection_class=attribute_keyed_dict("email_address"),
                )
            },
        )
        u1 = User(id=7, name="fred")
        u1.addresses["foo@bar.com"] = Address(email_address="foo@bar.com")
        sess = fixture_session()
        sess.merge(u1)
        sess.flush()
        assert list(u1.addresses.keys()) == ["foo@bar.com"]

    def test_attribute_cascade(self):
        """Merge of a persistent entity with two child
        persistent entities."""

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
        load = self.load_tracker(User)
        self.load_tracker(Address, load)

        with fixture_session(expire_on_commit=False) as sess, sess.begin():
            # set up data and save
            u = User(
                id=7,
                name="fred",
                addresses=[
                    Address(email_address="foo@bar.com"),
                    Address(email_address="hoho@la.com"),
                ],
            )
            sess.add(u)

        # assert data was saved
        sess2 = fixture_session()
        u2 = sess2.get(User, 7)
        eq_(
            u2,
            User(
                id=7,
                name="fred",
                addresses=[
                    Address(email_address="foo@bar.com"),
                    Address(email_address="hoho@la.com"),
                ],
            ),
        )

        # make local changes to data
        u.name = "fred2"
        u.addresses[1].email_address = "hoho@lalala.com"

        eq_(load.called, 3)

        # new session, merge modified data into session
        with fixture_session(expire_on_commit=False) as sess3:
            u3 = sess3.merge(u)
            eq_(load.called, 6)

            # ensure local changes are pending
            eq_(
                u3,
                User(
                    id=7,
                    name="fred2",
                    addresses=[
                        Address(email_address="foo@bar.com"),
                        Address(email_address="hoho@lalala.com"),
                    ],
                ),
            )

            # save merged data
            sess3.commit()

        # assert modified/merged data was saved
        with fixture_session() as sess:
            u = sess.get(User, 7)
            eq_(
                u,
                User(
                    id=7,
                    name="fred2",
                    addresses=[
                        Address(email_address="foo@bar.com"),
                        Address(email_address="hoho@lalala.com"),
                    ],
                ),
            )
            eq_(load.called, 9)

        # merge persistent object into another session
        with fixture_session(expire_on_commit=False) as sess4:
            u = sess4.merge(u)
            assert len(u.addresses)
            for a in u.addresses:
                assert a.user is u

            def go():
                sess4.flush()

            # no changes; therefore flush should do nothing
            self.assert_sql_count(testing.db, go, 0)

            sess4.commit()

        eq_(load.called, 12)

        # test with "dontload" merge
        with fixture_session(expire_on_commit=False) as sess5:
            u = sess5.merge(u, load=False)
            assert len(u.addresses)
            for a in u.addresses:
                assert a.user is u

            def go():
                sess5.flush()

            # no changes; therefore flush should do nothing
            # but also, load=False wipes out any difference in committed state,
            # so no flush at all
            self.assert_sql_count(testing.db, go, 0)
        eq_(load.called, 15)

        with fixture_session(expire_on_commit=False) as sess4, sess4.begin():
            u = sess4.merge(u, load=False)
            # post merge change
            u.addresses[1].email_address = "afafds"

            def go():
                sess4.flush()

            # afafds change flushes
            self.assert_sql_count(testing.db, go, 1)
        eq_(load.called, 18)

        with fixture_session(expire_on_commit=False) as sess5:
            u2 = sess5.get(User, u.id)
            eq_(u2.name, "fred2")
            eq_(u2.addresses[1].email_address, "afafds")
        eq_(load.called, 21)

    def test_dont_send_neverset_to_get(self):
        # test issue #3647
        CompositePk, composite_pk_table = (
            self.classes.CompositePk,
            self.tables.composite_pk_table,
        )
        self.mapper_registry.map_imperatively(CompositePk, composite_pk_table)
        cp1 = CompositePk(j=1, k=1)

        sess = fixture_session()

        rec = []

        def go():
            rec.append(sess.merge(cp1))

        self.assert_sql_count(testing.db, go, 0)
        rec[0].i = 5
        sess.commit()
        eq_(rec[0].i, 5)

    def test_dont_send_neverset_to_get_w_relationship(self):
        # test issue #3647
        CompositePk, composite_pk_table = (
            self.classes.CompositePk,
            self.tables.composite_pk_table,
        )
        User, users = (self.classes.User, self.tables.users)
        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "elements": relationship(
                    CompositePk,
                    primaryjoin=users.c.id == foreign(composite_pk_table.c.i),
                )
            },
        )
        self.mapper_registry.map_imperatively(CompositePk, composite_pk_table)

        u1 = User(id=5, name="some user")
        cp1 = CompositePk(j=1, k=1)
        u1.elements.append(cp1)
        sess = fixture_session()

        rec = []

        def go():
            rec.append(sess.merge(u1))

        self.assert_sql_count(testing.db, go, 1)
        u2 = rec[0]
        sess.commit()
        eq_(u2.elements[0].i, 5)
        eq_(u2.id, 5)

    def test_no_relationship_cascade(self):
        """test that merge doesn't interfere with a relationship()
        target that specifically doesn't include 'merge' cascade.
        """

        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(
            Address,
            addresses,
            properties={"user": relationship(User, cascade="save-update")},
        )
        self.mapper_registry.map_imperatively(User, users)
        sess = fixture_session()
        u1 = User(name="fred")
        a1 = Address(email_address="asdf", user=u1)
        sess.add(a1)
        sess.flush()

        a2 = Address(id=a1.id, email_address="bar", user=User(name="hoho"))
        a2 = sess.merge(a2)
        sess.flush()

        # no expire of the attribute

        assert a2.__dict__["user"] is u1

        # merge succeeded
        eq_(
            sess.query(Address).all(), [Address(id=a1.id, email_address="bar")]
        )

        # didn't touch user
        eq_(sess.query(User).all(), [User(name="fred")])

    def test_one_to_many_cascade(self):
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
                    self.mapper_registry.map_imperatively(Address, addresses)
                )
            },
        )

        load = self.load_tracker(User)
        self.load_tracker(Address, load)

        sess = fixture_session(expire_on_commit=False)
        u = User(name="fred")
        a1 = Address(email_address="foo@bar")
        a2 = Address(email_address="foo@quux")
        u.addresses.extend([a1, a2])

        sess.add(u)
        sess.commit()

        eq_(load.called, 0)

        sess2 = fixture_session()
        u2 = sess2.get(User, u.id)
        eq_(load.called, 1)

        u.addresses[1].email_address = "addr 2 modified"
        sess2.merge(u)
        eq_(u2.addresses[1].email_address, "addr 2 modified")
        eq_(load.called, 3)

        sess3 = fixture_session()
        u3 = sess3.get(User, u.id)
        eq_(load.called, 4)

        u.name = "also fred"
        sess3.merge(u)
        eq_(load.called, 6)
        eq_(u3.name, "also fred")

    def test_many_to_one_cascade(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(
            Address, addresses, properties={"user": relationship(User)}
        )
        self.mapper_registry.map_imperatively(User, users)

        u1 = User(id=1, name="u1")
        a1 = Address(id=1, email_address="a1", user=u1)
        u2 = User(id=2, name="u2")

        sess = fixture_session(expire_on_commit=False)
        sess.add_all([a1, u2])
        sess.commit()

        a1.user = u2

        with fixture_session(expire_on_commit=False) as sess2:
            a2 = sess2.merge(a1)
            eq_(attributes.get_history(a2, "user"), ([u2], (), ()))
            assert a2 in sess2.dirty

        sess.refresh(a1)

        with fixture_session(expire_on_commit=False) as sess2:
            a2 = sess2.merge(a1, load=False)
            eq_(attributes.get_history(a2, "user"), ((), [u1], ()))
            assert a2 not in sess2.dirty

    def test_many_to_many_cascade(self):
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
                    self.mapper_registry.map_imperatively(Item, items),
                    secondary=order_items,
                )
            },
        )

        load = self.load_tracker(Order)
        self.load_tracker(Item, load)

        with fixture_session(expire_on_commit=False) as sess:
            i1 = Item()
            i1.description = "item 1"

            i2 = Item()
            i2.description = "item 2"

            o = Order()
            o.description = "order description"
            o.items.append(i1)
            o.items.append(i2)

            sess.add(o)
            sess.commit()

        eq_(load.called, 0)

        with fixture_session(expire_on_commit=False) as sess2:
            o2 = sess2.get(Order, o.id)
            eq_(load.called, 1)

            o.items[1].description = "item 2 modified"
            sess2.merge(o)
            eq_(o2.items[1].description, "item 2 modified")
            eq_(load.called, 3)

        with fixture_session(expire_on_commit=False) as sess3:
            o3 = sess3.get(Order, o.id)
            eq_(load.called, 4)

            o.description = "desc modified"
            sess3.merge(o)
            eq_(load.called, 6)
            eq_(o3.description, "desc modified")

    def test_one_to_one_cascade(self):
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
                "address": relationship(
                    self.mapper_registry.map_imperatively(Address, addresses),
                    uselist=False,
                )
            },
        )
        load = self.load_tracker(User)
        self.load_tracker(Address, load)
        sess = fixture_session(expire_on_commit=False)

        u = User()
        u.id = 7
        u.name = "fred"
        a1 = Address()
        a1.email_address = "foo@bar.com"
        u.address = a1

        sess.add(u)
        sess.commit()

        eq_(load.called, 0)

        sess2 = fixture_session()
        u2 = sess2.get(User, 7)
        eq_(load.called, 1)
        u2.name = "fred2"
        u2.address.email_address = "hoho@lalala.com"
        eq_(load.called, 2)

        u3 = sess.merge(u2)
        eq_(load.called, 2)
        assert u3 is u

    def test_value_to_none(self):
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
                "address": relationship(
                    self.mapper_registry.map_imperatively(Address, addresses),
                    uselist=False,
                    backref="user",
                )
            },
        )
        sess = fixture_session()
        u = User(
            id=7,
            name="fred",
            address=Address(id=1, email_address="foo@bar.com"),
        )
        sess.add(u)
        sess.commit()
        sess.close()

        u2 = User(id=7, name=None, address=None)
        u3 = sess.merge(u2)
        assert u3.name is None
        assert u3.address is None

        sess.close()

        a1 = Address(id=1, user=None)
        a2 = sess.merge(a1)
        assert a2.user is None

    def test_transient_no_load(self):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)

        sess = fixture_session()
        u = User()
        assert_raises_message(
            sa.exc.InvalidRequestError,
            "load=False option does not support",
            sess.merge,
            u,
            load=False,
        )

    def test_no_load_with_backrefs(self):
        """load=False populates relationships in both
        directions without requiring a load"""

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

        u = User(
            id=7,
            name="fred",
            addresses=[
                Address(email_address="ad1"),
                Address(email_address="ad2"),
            ],
        )
        sess = fixture_session()
        sess.add(u)
        sess.flush()
        sess.close()
        assert "user" in u.addresses[1].__dict__

        sess = fixture_session()
        u2 = sess.merge(u, load=False)
        assert "user" in u2.addresses[1].__dict__
        eq_(u2.addresses[1].user, User(id=7, name="fred"))

        sess.expire(u2.addresses[1], ["user"])
        assert "user" not in u2.addresses[1].__dict__
        sess.close()

        sess = fixture_session()
        u = sess.merge(u2, load=False)
        assert "user" not in u.addresses[1].__dict__
        eq_(u.addresses[1].user, User(id=7, name="fred"))

    def test_dontload_with_eager(self):
        """

        This test illustrates that with load=False, we can't just copy
        the committed_state of the merged instance over; since it
        references collection objects which themselves are to be merged.
        This committed_state would instead need to be piecemeal
        'converted' to represent the correct objects.  However, at the
        moment I'd rather not support this use case; if you are merging
        with load=False, you're typically dealing with caching and the
        merged objects shouldn't be 'dirty'.

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
            properties={
                "addresses": relationship(
                    self.mapper_registry.map_imperatively(Address, addresses)
                )
            },
        )
        with fixture_session(expire_on_commit=False) as sess:
            u = User()
            u.id = 7
            u.name = "fred"
            a1 = Address()
            a1.email_address = "foo@bar.com"
            u.addresses.append(a1)

            sess.add(u)
            sess.commit()

        sess2 = fixture_session()
        u2 = sess2.get(User, 7, options=[sa.orm.joinedload(User.addresses)])

        sess3 = fixture_session()
        u3 = sess3.merge(u2, load=False)  # noqa

        def go():
            sess3.flush()

        self.assert_sql_count(testing.db, go, 0)

    def test_no_load_disallows_dirty(self):
        """load=False doesn't support 'dirty' objects right now

        (see test_no_load_with_eager()). Therefore lets assert it.

        """

        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)
        with fixture_session(expire_on_commit=False) as sess:
            u = User()
            u.id = 7
            u.name = "fred"
            sess.add(u)
            sess.commit()

        u.name = "ed"
        sess2 = fixture_session()
        try:
            sess2.merge(u, load=False)
            assert False
        except sa.exc.InvalidRequestError as e:
            assert (
                "merge() with load=False option does not support "
                "objects marked as 'dirty'.  flush() all changes on "
                "mapped instances before merging with load=False." in str(e)
            )

        u2 = sess2.get(User, 7)

        sess3 = fixture_session()
        u3 = sess3.merge(u2, load=False)  # noqa
        assert not sess3.dirty

        def go():
            sess3.flush()

        self.assert_sql_count(testing.db, go, 0)

    def test_no_load_sets_backrefs(self):
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

        sess = fixture_session()
        u = User()
        u.id = 7
        u.name = "fred"
        a1 = Address()
        a1.email_address = "foo@bar.com"
        u.addresses.append(a1)

        sess.add(u)
        sess.flush()

        assert u.addresses[0].user is u

        sess2 = fixture_session()
        u2 = sess2.merge(u, load=False)
        assert not sess2.dirty

        def go():
            assert u2.addresses[0].user is u2

        self.assert_sql_count(testing.db, go, 0)

    def test_no_load_preserves_parents(self):
        """Merge with load=False does not trigger a 'delete-orphan'
        operation.

        merge with load=False sets attributes without using events.
        this means the 'hasparent' flag is not propagated to the newly
        merged instance. in fact this works out OK, because the
        '_state.parents' collection on the newly merged instance is
        empty; since the mapper doesn't see an active 'False' setting in
        this collection when _is_orphan() is called, it does not count
        as an orphan (i.e. this is the 'optimistic' logic in
        mapper._is_orphan().)

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
            properties={
                "addresses": relationship(
                    self.mapper_registry.map_imperatively(Address, addresses),
                    backref="user",
                    cascade="all, delete-orphan",
                )
            },
        )
        with fixture_session(expire_on_commit=False) as sess:
            u = User()
            u.id = 7
            u.name = "fred"
            a1 = Address()
            a1.email_address = "foo@bar.com"
            u.addresses.append(a1)
            sess.add(u)
            sess.commit()

        assert u.addresses[0].user is u

        with fixture_session(expire_on_commit=False) as sess2:
            u2 = sess2.merge(u, load=False)
            assert not sess2.dirty
            a2 = u2.addresses[0]
            a2.email_address = "somenewaddress"
            assert not sa.orm.object_mapper(a2)._is_orphan(
                sa.orm.attributes.instance_state(a2)
            )
            sess2.commit()

        with fixture_session() as sess2:
            eq_(
                sess2.get(User, u2.id).addresses[0].email_address,
                "somenewaddress",
            )

        # this use case is not supported; this is with a pending Address
        # on the pre-merged object, and we currently don't support
        # 'dirty' objects being merged with load=False.  in this case,
        # the empty '_state.parents' collection would be an issue, since
        # the optimistic flag is False in _is_orphan() for pending
        # instances.  so if we start supporting 'dirty' with load=False,
        # this test will need to pass

        sess2 = fixture_session()
        sess = fixture_session()
        u = sess.get(User, 7)
        u.addresses.append(Address())
        sess2 = fixture_session()
        try:
            u2 = sess2.merge(u, load=False)
            assert False

            # if load=False is changed to support dirty objects, this code
            # needs to pass
            a2 = u2.addresses[0]
            a2.email_address = "somenewaddress"
            assert not sa.orm.object_mapper(a2)._is_orphan(
                sa.orm.attributes.instance_state(a2)
            )
            sess2.flush()
            sess2.expunge_all()
            eq_(
                sess2.get(User, u2.id).addresses[0].email_address,
                "somenewaddress",
            )
        except sa.exc.InvalidRequestError as e:
            assert "load=False option does not support" in str(e)

    @testing.variation("viewonly", ["viewonly", "normal"])
    @testing.variation("load", ["load", "noload"])
    @testing.variation("lazy", ["select", "raise", "raise_on_sql"])
    @testing.variation(
        "merge_persistent", ["merge_persistent", "merge_detached"]
    )
    @testing.variation("detach_original", ["detach", "persistent"])
    @testing.variation("direction", ["o2m", "m2o"])
    def test_relationship_population_maintained(
        self,
        viewonly,
        load,
        lazy,
        merge_persistent,
        direction,
        detach_original,
    ):
        """test #8862"""

        User, Address = self.classes("User", "Address")
        users, addresses = self.tables("users", "addresses")

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address,
                    viewonly=viewonly.viewonly,
                    lazy=lazy.name,
                    back_populates="user",
                    order_by=addresses.c.id,
                )
            },
        )

        self.mapper_registry.map_imperatively(
            Address,
            addresses,
            properties={
                "user": relationship(
                    User,
                    viewonly=viewonly.viewonly,
                    lazy=lazy.name,
                    back_populates="addresses",
                )
            },
        )

        s = fixture_session()

        u1 = User(id=1, name="u1")
        s.add(u1)
        s.flush()
        s.add_all(
            [Address(user_id=1, email_address="e%d" % i) for i in range(1, 4)]
        )
        s.commit()

        if direction.o2m:
            cls_to_merge = User
            obj_to_merge = (
                s.scalars(select(User).options(joinedload(User.addresses)))
                .unique()
                .one()
            )
            attrname = "addresses"

        elif direction.m2o:
            cls_to_merge = Address
            obj_to_merge = (
                s.scalars(
                    select(Address)
                    .filter_by(email_address="e1")
                    .options(joinedload(Address.user))
                )
                .unique()
                .one()
            )
            attrname = "user"
        else:
            direction.fail()

        assert attrname in obj_to_merge.__dict__

        s2 = Session(testing.db)

        if merge_persistent.merge_persistent:
            target_persistent = s2.get(cls_to_merge, obj_to_merge.id)  # noqa

        if detach_original.detach:
            s.expunge(obj_to_merge)

        with self.sql_execution_asserter(testing.db) as assert_:
            merged_object = s2.merge(obj_to_merge, load=load.load)

        assert_.assert_(
            CountStatements(
                0
                if load.noload
                else 1 if merge_persistent.merge_persistent else 2
            )
        )

        assert attrname in merged_object.__dict__

        with self.sql_execution_asserter(testing.db) as assert_:
            if direction.o2m:
                eq_(
                    merged_object.addresses,
                    [
                        Address(user_id=1, email_address="e%d" % i)
                        for i in range(1, 4)
                    ],
                )
            elif direction.m2o:
                eq_(merged_object.user, User(id=1, name="u1"))
        assert_.assert_(CountStatements(0))

    def test_synonym(self):
        users = self.tables.users

        class User:
            def _getValue(self):
                return self._value

            def _setValue(self, value):
                setattr(self, "_value", value)

            value = property(_getValue, _setValue)

        self.mapper_registry.map_imperatively(
            User, users, properties={"uid": synonym("id")}
        )

        sess = fixture_session()
        u = User()
        u.name = "ed"
        sess.add(u)
        sess.flush()
        sess.expunge(u)
        sess.merge(u)

    def test_cascade_doesnt_blowaway_manytoone(self):
        """a merge test that was fixed by [ticket:1202]"""

        User, Address, addresses, users = (
            self.classes.User,
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
        )

        s = fixture_session(autoflush=True, future=True)
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

        a1 = Address(user=s.merge(User(id=1, name="ed")), email_address="x")
        s.add(a1)
        before_id = id(a1.user)
        a2 = Address(user=s.merge(User(id=1, name="jack")), email_address="x")
        s.add(a2)
        after_id = id(a1.user)
        other_id = id(a2.user)
        eq_(before_id, other_id)
        eq_(after_id, other_id)
        eq_(before_id, after_id)
        eq_(a1.user, a2.user)

    def test_cascades_dont_autoflush(self):
        User, Address, addresses, users = (
            self.classes.User,
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
        )

        sess = fixture_session(
            autoflush=True,
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
        user = User(
            id=8, name="fred", addresses=[Address(email_address="user")]
        )
        merged_user = sess.merge(user)
        assert merged_user in sess.new
        sess.flush()
        assert merged_user not in sess.new

    def test_cascades_dont_autoflush_2(self):
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
                    Address, backref="user", cascade="all, delete-orphan"
                )
            },
        )
        self.mapper_registry.map_imperatively(Address, addresses)

        u = User(
            id=7, name="fred", addresses=[Address(id=1, email_address="fred1")]
        )
        sess = fixture_session(
            autoflush=True,
        )
        sess.add(u)
        sess.commit()

        sess.expunge_all()

        u = User(
            id=7,
            name="fred",
            addresses=[
                Address(id=1, email_address="fred1"),
                Address(id=2, email_address="fred2"),
            ],
        )
        sess.merge(u)
        assert sess.autoflush
        sess.commit()

    def test_dont_expire_pending(self):
        """test that pending instances aren't expired during a merge."""

        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)
        u = User(id=7)
        sess = fixture_session(
            autoflush=True,
        )
        u = sess.merge(u)
        assert not bool(attributes.instance_state(u).expired_attributes)

        def go():
            eq_(u.name, None)

        self.assert_sql_count(testing.db, go, 0)

    def test_option_state(self):
        """test that the merged takes on the MapperOption characteristics
        of that which is merged.

        """

        users, User = self.tables.users, self.classes.User

        class Option(MapperOption):
            propagate_to_loaders = True

        opt1, opt2 = Option(), Option()

        sess = fixture_session()

        umapper = self.mapper_registry.map_imperatively(User, users)

        sess.add_all([User(id=1, name="u1"), User(id=2, name="u2")])
        sess.commit()

        sess2 = fixture_session()
        s2_users = sess2.query(User).options(opt2).all()

        # test 1.  no options are replaced by merge options
        sess = fixture_session()
        s1_users = sess.query(User).all()

        for u in s1_users:
            ustate = attributes.instance_state(u)
            eq_(ustate.load_path.path, (umapper,))
            eq_(ustate.load_options, ())

        for u in s2_users:
            sess.merge(u)

        for u in s1_users:
            ustate = attributes.instance_state(u)
            eq_(ustate.load_path.path, (umapper,))
            eq_(ustate.load_options, (opt2,))

        # test 2.  present options are replaced by merge options
        sess = fixture_session()
        s1_users = sess.query(User).options(opt1).all()
        for u in s1_users:
            ustate = attributes.instance_state(u)
            eq_(ustate.load_path.path, (umapper,))
            eq_(ustate.load_options, (opt1,))

        for u in s2_users:
            sess.merge(u)

        for u in s1_users:
            ustate = attributes.instance_state(u)
            eq_(ustate.load_path.path, (umapper,))
            eq_(ustate.load_options, (opt2,))

    def test_resolve_conflicts_pending_doesnt_interfere_no_ident(self):
        User, Address, Order = (
            self.classes.User,
            self.classes.Address,
            self.classes.Order,
        )
        users, addresses, orders = (
            self.tables.users,
            self.tables.addresses,
            self.tables.orders,
        )

        self.mapper_registry.map_imperatively(
            User, users, properties={"orders": relationship(Order)}
        )
        self.mapper_registry.map_imperatively(
            Order, orders, properties={"address": relationship(Address)}
        )
        self.mapper_registry.map_imperatively(Address, addresses)

        u1 = User(id=7, name="x")
        u1.orders = [
            Order(description="o1", address=Address(email_address="a")),
            Order(description="o2", address=Address(email_address="b")),
            Order(description="o3", address=Address(email_address="c")),
        ]

        sess = fixture_session()
        sess.merge(u1)
        sess.flush()

        eq_(
            sess.query(Address.email_address)
            .order_by(Address.email_address)
            .all(),
            [("a",), ("b",), ("c",)],
        )

    def test_resolve_conflicts_pending(self):
        User, Address, Order = (
            self.classes.User,
            self.classes.Address,
            self.classes.Order,
        )
        users, addresses, orders = (
            self.tables.users,
            self.tables.addresses,
            self.tables.orders,
        )

        self.mapper_registry.map_imperatively(
            User, users, properties={"orders": relationship(Order)}
        )
        self.mapper_registry.map_imperatively(
            Order, orders, properties={"address": relationship(Address)}
        )
        self.mapper_registry.map_imperatively(Address, addresses)

        u1 = User(id=7, name="x")
        u1.orders = [
            Order(description="o1", address=Address(id=1, email_address="a")),
            Order(description="o2", address=Address(id=1, email_address="b")),
            Order(description="o3", address=Address(id=1, email_address="c")),
        ]

        sess = fixture_session()
        sess.merge(u1)
        sess.flush()

        eq_(sess.query(Address).one(), Address(id=1, email_address="c"))

    def test_resolve_conflicts_persistent(self):
        User, Address, Order = (
            self.classes.User,
            self.classes.Address,
            self.classes.Order,
        )
        users, addresses, orders = (
            self.tables.users,
            self.tables.addresses,
            self.tables.orders,
        )

        self.mapper_registry.map_imperatively(
            User, users, properties={"orders": relationship(Order)}
        )
        self.mapper_registry.map_imperatively(
            Order, orders, properties={"address": relationship(Address)}
        )
        self.mapper_registry.map_imperatively(Address, addresses)

        sess = fixture_session()
        sess.add(Address(id=1, email_address="z"))
        sess.commit()

        u1 = User(id=7, name="x")
        u1.orders = [
            Order(description="o1", address=Address(id=1, email_address="a")),
            Order(description="o2", address=Address(id=1, email_address="b")),
            Order(description="o3", address=Address(id=1, email_address="c")),
        ]

        sess = fixture_session()
        sess.merge(u1)
        sess.flush()

        eq_(sess.query(Address).one(), Address(id=1, email_address="c"))

    def test_merge_all(self):
        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(User, users)
        sess = fixture_session()
        load = self.load_tracker(User)

        ua = User(id=42, name="bob")
        ub = User(id=7, name="fred")
        eq_(load.called, 0)
        uam, ubm = sess.merge_all([ua, ub])
        eq_(load.called, 2)
        assert uam in sess
        assert ubm in sess
        eq_(uam, User(id=42, name="bob"))
        eq_(ubm, User(id=7, name="fred"))
        sess.flush()
        sess.expunge_all()
        eq_(
            sess.query(User).order_by("id").all(),
            [User(id=7, name="fred"), User(id=42, name="bob")],
        )


class M2ONoUseGetLoadingTest(fixtures.MappedTest):
    """Merge a one-to-many.  The many-to-one on the other side is set up
    so that use_get is False.   See if skipping the "m2o" merge
    vs. doing it saves on SQL calls.

    """

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "user",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(50)),
        )
        Table(
            "address",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("user_id", Integer, ForeignKey("user.id")),
            Column("email", String(50)),
        )

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            pass

        class Address(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        User, Address = cls.classes.User, cls.classes.Address
        user, address = cls.tables.user, cls.tables.address
        cls.mapper_registry.map_imperatively(
            User,
            user,
            properties={
                "addresses": relationship(
                    Address,
                    backref=backref(
                        "user",
                        # needlessly complex primaryjoin so
                        # that the use_get flag is False
                        primaryjoin=and_(
                            user.c.id == address.c.user_id,
                            user.c.id == user.c.id,
                        ),
                    ),
                )
            },
        )
        cls.mapper_registry.map_imperatively(Address, address)
        configure_mappers()
        assert Address.user.property._use_get is False

    @classmethod
    def insert_data(cls, connection):
        User, Address = cls.classes.User, cls.classes.Address
        s = Session(connection)
        s.add_all(
            [
                User(
                    id=1,
                    name="u1",
                    addresses=[
                        Address(id=1, email="a1"),
                        Address(id=2, email="a2"),
                    ],
                )
            ]
        )
        s.commit()

    # "persistent" - we get at an Address that was already present.
    # With the "skip bidirectional" check removed, the "set" emits SQL
    # for the "previous" version in any case,
    # address.user_id is 1, you get a load.
    def test_persistent_access_none(self):
        User, Address = self.classes.User, self.classes.Address
        s = fixture_session()

        def go():
            u1 = User(id=1, addresses=[Address(id=1), Address(id=2)])
            s.merge(u1)

        self.assert_sql_count(testing.db, go, 2)

    def test_persistent_access_one(self):
        User, Address = self.classes.User, self.classes.Address
        s = fixture_session()

        def go():
            u1 = User(id=1, addresses=[Address(id=1), Address(id=2)])
            u2 = s.merge(u1)
            a1 = u2.addresses[0]
            assert a1.user is u2

        self.assert_sql_count(testing.db, go, 3)

    def test_persistent_access_two(self):
        User, Address = self.classes.User, self.classes.Address
        s = fixture_session()

        def go():
            u1 = User(id=1, addresses=[Address(id=1), Address(id=2)])
            u2 = s.merge(u1)
            a1 = u2.addresses[0]
            assert a1.user is u2
            a2 = u2.addresses[1]
            assert a2.user is u2

        self.assert_sql_count(testing.db, go, 4)

    # "pending" - we get at an Address that is new- user_id should be
    # None.  But in this case the set attribute on the forward side
    # already sets the backref.  commenting out the "skip bidirectional"
    # check emits SQL again for the other two Address objects already
    # persistent.
    def test_pending_access_one(self):
        User, Address = self.classes.User, self.classes.Address
        s = fixture_session()

        def go():
            u1 = User(
                id=1,
                addresses=[
                    Address(id=1),
                    Address(id=2),
                    Address(id=3, email="a3"),
                ],
            )
            u2 = s.merge(u1)
            a3 = u2.addresses[2]
            assert a3.user is u2

        self.assert_sql_count(testing.db, go, 3)

    def test_pending_access_two(self):
        User, Address = self.classes.User, self.classes.Address
        s = fixture_session()

        def go():
            u1 = User(
                id=1,
                addresses=[
                    Address(id=1),
                    Address(id=2),
                    Address(id=3, email="a3"),
                ],
            )
            u2 = s.merge(u1)
            a3 = u2.addresses[2]
            assert a3.user is u2
            a2 = u2.addresses[1]
            assert a2.user is u2

        self.assert_sql_count(testing.db, go, 5)


class DeferredMergeTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "book",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("title", String(200), nullable=False),
            Column("summary", String(2000)),
            Column("excerpt", Text),
        )

    @classmethod
    def setup_classes(cls):
        class Book(cls.Basic):
            pass

    def test_deferred_column_keyed_dict(self):
        # defer 'excerpt' at mapping level instead of query level
        Book, book = self.classes.Book, self.tables.book
        self.mapper_registry.map_imperatively(
            Book, book, properties={"excerpt": deferred(book.c.excerpt)}
        )
        sess = fixture_session()

        b = Book(
            id=1,
            title="Essential SQLAlchemy",
            summary="some summary",
            excerpt="some excerpt",
        )
        sess.add(b)
        sess.commit()

        b1 = sess.query(Book).first()
        sess.expire(b1, ["summary"])
        sess.close()

        def go():
            b2 = sess.merge(b1, load=False)

            # should not emit load for deferred 'excerpt'
            eq_(b2.summary, "some summary")
            not_in("excerpt", b2.__dict__)

            # now it should emit load for deferred 'excerpt'
            eq_(b2.excerpt, "some excerpt")
            in_("excerpt", b2.__dict__)

        self.sql_eq_(
            go,
            [
                (
                    "SELECT book.summary AS book_summary "
                    "FROM book WHERE book.id = :pk_1",
                    {"pk_1": 1},
                ),
                (
                    "SELECT book.excerpt AS book_excerpt "
                    "FROM book WHERE book.id = :pk_1",
                    {"pk_1": 1},
                ),
            ],
        )

    def test_deferred_column_query(self):
        Book, book = self.classes.Book, self.tables.book
        self.mapper_registry.map_imperatively(Book, book)
        sess = fixture_session()

        b = Book(
            id=1,
            title="Essential SQLAlchemy",
            summary="some summary",
            excerpt="some excerpt",
        )
        sess.add(b)
        sess.commit()

        # defer 'excerpt' at query level instead of mapping level
        b1 = sess.query(Book).options(defer(Book.excerpt)).first()
        sess.expire(b1, ["summary"])
        sess.close()

        def go():
            b2 = sess.merge(b1, load=False)

            # should not emit load for deferred 'excerpt'
            eq_(b2.summary, "some summary")
            not_in("excerpt", b2.__dict__)

            # now it should emit load for deferred 'excerpt'
            eq_(b2.excerpt, "some excerpt")
            in_("excerpt", b2.__dict__)

        self.sql_eq_(
            go,
            [
                (
                    "SELECT book.summary AS book_summary "
                    "FROM book WHERE book.id = :pk_1",
                    {"pk_1": 1},
                ),
                (
                    "SELECT book.excerpt AS book_excerpt "
                    "FROM book WHERE book.id = :pk_1",
                    {"pk_1": 1},
                ),
            ],
        )


class MutableMergeTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "data",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", PickleType(comparator=operator.eq)),
        )

    @classmethod
    def setup_classes(cls):
        class Data(cls.Basic):
            pass

    def test_list(self):
        Data, data = self.classes.Data, self.tables.data

        self.mapper_registry.map_imperatively(Data, data)
        sess = fixture_session()
        d = Data(data=["this", "is", "a", "list"])

        sess.add(d)
        sess.commit()

        d2 = Data(id=d.id, data=["this", "is", "another", "list"])
        d3 = sess.merge(d2)
        eq_(d3.data, ["this", "is", "another", "list"])


class CompositeNullPksTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "data",
            metadata,
            Column("pk1", String(10), primary_key=True),
            Column("pk2", String(10), primary_key=True),
        )

    @classmethod
    def setup_classes(cls):
        class Data(cls.Basic):
            pass

    def test_merge_allow_partial(self):
        Data, data = self.classes.Data, self.tables.data

        self.mapper_registry.map_imperatively(Data, data)
        sess = fixture_session()

        d1 = Data(pk1="someval", pk2=None)

        def go():
            return sess.merge(d1)

        self.assert_sql_count(testing.db, go, 1)

    def test_merge_disallow_partial(self):
        Data, data = self.classes.Data, self.tables.data

        self.mapper_registry.map_imperatively(
            Data, data, allow_partial_pks=False
        )
        sess = fixture_session()

        d1 = Data(pk1="someval", pk2=None)

        def go():
            return sess.merge(d1)

        self.assert_sql_count(testing.db, go, 0)


class LoadOnPendingTest(fixtures.MappedTest):
    """Test interaction of merge() with load_on_pending relationships"""

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "rocks",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("description", String(10)),
        )
        Table(
            "bugs",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("rockid", Integer, ForeignKey("rocks.id")),
        )

    @classmethod
    def setup_classes(cls):
        class Rock(cls.Basic, ComparableEntity):
            pass

        class Bug(cls.Basic, ComparableEntity):
            pass

    def _setup_delete_orphan_o2o(self):
        self.mapper_registry.map_imperatively(
            self.classes.Rock,
            self.tables.rocks,
            properties={
                "bug": relationship(
                    self.classes.Bug,
                    cascade="all,delete-orphan",
                    load_on_pending=True,
                    uselist=False,
                )
            },
        )
        self.mapper_registry.map_imperatively(
            self.classes.Bug, self.tables.bugs
        )
        self.sess = fixture_session()

    def _merge_delete_orphan_o2o_with(self, bug):
        # create a transient rock with passed bug
        r = self.classes.Rock(id=0, description="moldy")
        r.bug = bug
        m = self.sess.merge(r)
        # we've already passed ticket #2374 problem since merge() returned,
        # but for good measure:
        assert m is not r
        eq_(m, r)

    def test_merge_delete_orphan_o2o_none(self):
        """one to one delete_orphan relationships marked load_on_pending
        should be able to merge() with attribute None"""

        self._setup_delete_orphan_o2o()
        self._merge_delete_orphan_o2o_with(None)

    def test_merge_delete_orphan_o2o(self):
        """one to one delete_orphan relationships marked load_on_pending
        should be able to merge()"""

        self._setup_delete_orphan_o2o()
        self._merge_delete_orphan_o2o_with(self.classes.Bug(id=1))


class PolymorphicOnTest(fixtures.MappedTest):
    """Test merge() of polymorphic object when polymorphic_on
    isn't a Column"""

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "employees",
            metadata,
            Column(
                "employee_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("type", String(1), nullable=False),
            Column("data", String(50)),
        )

    @classmethod
    def setup_classes(cls):
        class Employee(cls.Basic, ComparableEntity):
            pass

        class Manager(Employee):
            pass

        class Engineer(Employee):
            pass

    def _setup_polymorphic_on_mappers(self):
        employee_mapper = self.mapper_registry.map_imperatively(
            self.classes.Employee,
            self.tables.employees,
            polymorphic_on=case(
                {
                    "E": "employee",
                    "M": "manager",
                    "G": "engineer",
                    "R": "engineer",
                },
                value=self.tables.employees.c.type,
            ),
            polymorphic_identity="employee",
        )
        self.mapper_registry.map_imperatively(
            self.classes.Manager,
            inherits=employee_mapper,
            polymorphic_identity="manager",
        )
        self.mapper_registry.map_imperatively(
            self.classes.Engineer,
            inherits=employee_mapper,
            polymorphic_identity="engineer",
        )
        self.sess = fixture_session()

    def test_merge_polymorphic_on(self):
        """merge() should succeed with a polymorphic object even when
        polymorphic_on is not a Column
        """
        self._setup_polymorphic_on_mappers()

        m = self.classes.Manager(
            employee_id=55, type="M", data="original data"
        )
        self.sess.add(m)
        self.sess.commit()
        self.sess.expunge_all()

        m = self.classes.Manager(employee_id=55, data="updated data")
        merged = self.sess.merge(m)

        # we've already passed ticket #2449 problem since
        # merge() returned, but for good measure:
        assert m is not merged
        eq_(m, merged)
