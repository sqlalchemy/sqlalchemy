"""
a series of tests which assert the behavior of moving objects between
collections and scalar attributes resulting in the expected state w.r.t.
backrefs, add/remove events, etc.

there's a particular focus on collections that have "uselist=False", since in
these cases the re-assignment of an attribute means the previous owner needs an
UPDATE in the database.

"""

from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy.orm import attributes
from sqlalchemy.orm import backref
from sqlalchemy.orm import mapper
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.testing import eq_
from sqlalchemy.testing import is_
from sqlalchemy.testing.fixtures import fixture_session
from test.orm import _fixtures


class O2MCollectionTest(_fixtures.FixtureTest):
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
            properties=dict(addresses=relationship(Address, backref="user")),
        )

    def test_collection_move_hitslazy(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        a1 = Address(email_address="address1")
        a2 = Address(email_address="address2")
        a3 = Address(email_address="address3")
        u1 = User(name="jack", addresses=[a1, a2, a3])
        u2 = User(name="ed")
        sess.add_all([u1, a1, a2, a3])
        sess.commit()

        # u1.addresses

        def go():
            u2.addresses.append(a1)
            u2.addresses.append(a2)
            u2.addresses.append(a3)

        self.assert_sql_count(testing.db, go, 0)

    def test_collection_move_preloaded(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        a1 = Address(email_address="address1")
        u1 = User(name="jack", addresses=[a1])

        u2 = User(name="ed")
        sess.add_all([u1, u2])
        sess.commit()  # everything is expired

        # load u1.addresses collection
        u1.addresses

        u2.addresses.append(a1)

        # backref fires
        assert a1.user is u2

        # a1 removed from u1.addresses as of [ticket:2789]
        assert a1 not in u1.addresses
        assert a1 in u2.addresses

    def test_collection_move_notloaded(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        a1 = Address(email_address="address1")
        u1 = User(name="jack", addresses=[a1])

        u2 = User(name="ed")
        sess.add_all([u1, u2])
        sess.commit()  # everything is expired

        u2.addresses.append(a1)

        # backref fires
        assert a1.user is u2

        # u1.addresses wasn't loaded,
        # so when it loads its correct
        assert a1 not in u1.addresses
        assert a1 in u2.addresses

    def test_collection_move_commitfirst(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        a1 = Address(email_address="address1")
        u1 = User(name="jack", addresses=[a1])

        u2 = User(name="ed")
        sess.add_all([u1, u2])
        sess.commit()  # everything is expired

        # load u1.addresses collection
        u1.addresses

        u2.addresses.append(a1)

        # backref fires
        assert a1.user is u2

        # everything expires, no changes in
        # u1.addresses, so all is fine
        sess.commit()
        assert a1 not in u1.addresses
        assert a1 in u2.addresses

    def test_scalar_move_preloaded(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()

        u1 = User(name="jack")
        u2 = User(name="ed")
        a1 = Address(email_address="a1")
        a1.user = u1
        sess.add_all([u1, u2, a1])
        sess.commit()

        # u1.addresses is loaded
        u1.addresses

        # direct set - the "old" is "fetched",
        # but only from the local session - not the
        # database, due to the PASSIVE_NO_FETCH flag.
        # this is a more fine grained behavior introduced
        # in 0.6
        a1.user = u2

        assert a1 not in u1.addresses
        assert a1 in u2.addresses

    def test_plain_load_passive(self):
        """test that many-to-one set doesn't load the old value."""

        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        u1 = User(name="jack")
        u2 = User(name="ed")
        a1 = Address(email_address="a1")
        a1.user = u1
        sess.add_all([u1, u2, a1])
        sess.commit()

        # in this case, a lazyload would
        # ordinarily occur except for the
        # PASSIVE_NO_FETCH flag.
        def go():
            a1.user = u2

        self.assert_sql_count(testing.db, go, 0)

        assert a1 not in u1.addresses
        assert a1 in u2.addresses

    def test_set_none(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        u1 = User(name="jack")
        a1 = Address(email_address="a1")
        a1.user = u1
        sess.add_all([u1, a1])
        sess.commit()

        # works for None too
        def go():
            a1.user = None

        self.assert_sql_count(testing.db, go, 0)

        assert a1 not in u1.addresses

    def test_scalar_move_notloaded(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()

        u1 = User(name="jack")
        u2 = User(name="ed")
        a1 = Address(email_address="a1")
        a1.user = u1
        sess.add_all([u1, u2, a1])
        sess.commit()

        # direct set - the fetching of the
        # "old" u1 here allows the backref
        # to remove it from the addresses collection
        a1.user = u2

        assert a1 not in u1.addresses
        assert a1 in u2.addresses

    def test_scalar_move_commitfirst(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()

        u1 = User(name="jack")
        u2 = User(name="ed")
        a1 = Address(email_address="a1")
        a1.user = u1
        sess.add_all([u1, u2, a1])
        sess.commit()

        # u1.addresses is loaded
        u1.addresses

        # direct set - the fetching of the
        # "old" u1 here allows the backref
        # to remove it from the addresses collection
        a1.user = u2

        sess.commit()
        assert a1 not in u1.addresses
        assert a1 in u2.addresses

    def test_collection_assignment_mutates_previous_one(self):
        User, Address = self.classes.User, self.classes.Address

        u1 = User(name="jack")
        u2 = User(name="ed")
        a1 = Address(email_address="a1")
        u1.addresses.append(a1)

        is_(a1.user, u1)

        u2.addresses = [a1]

        eq_(u1.addresses, [])

        is_(a1.user, u2)

    def test_collection_assignment_mutates_previous_two(self):
        User, Address = self.classes.User, self.classes.Address

        u1 = User(name="jack")
        a1 = Address(email_address="a1")

        u1.addresses.append(a1)

        is_(a1.user, u1)

        u1.addresses = []
        is_(a1.user, None)

    def test_del_from_collection(self):
        User, Address = self.classes.User, self.classes.Address

        u1 = User(name="jack")
        a1 = Address(email_address="a1")

        u1.addresses.append(a1)

        is_(a1.user, u1)

        del u1.addresses[0]

        is_(a1.user, None)

    def test_del_from_scalar(self):
        User, Address = self.classes.User, self.classes.Address

        u1 = User(name="jack")
        a1 = Address(email_address="a1")

        u1.addresses.append(a1)

        is_(a1.user, u1)

        del a1.user

        assert a1 not in u1.addresses

    def test_tuple_assignment_w_reverse(self):
        User, Address = self.classes.User, self.classes.Address

        u1 = User()

        a1 = Address(email_address="1")
        a2 = Address(email_address="2")
        a3 = Address(email_address="3")
        u1.addresses.append(a1)
        u1.addresses.append(a2)
        u1.addresses.append(a3)

        u1.addresses[1], u1.addresses[2] = u1.addresses[2], u1.addresses[1]

        assert a3.user is u1

        eq_(u1.addresses, [a1, a3, a2])

    def test_straight_remove(self):
        User, Address = self.classes.User, self.classes.Address

        u1 = User()

        a1 = Address(email_address="1")
        a2 = Address(email_address="2")
        a3 = Address(email_address="3")
        u1.addresses.append(a1)
        u1.addresses.append(a2)
        u1.addresses.append(a3)

        del u1.addresses[2]

        assert a3.user is None
        eq_(u1.addresses, [a1, a2])

    def test_append_del(self):
        User, Address = self.classes.User, self.classes.Address

        u1 = User()

        a1 = Address(email_address="1")
        a2 = Address(email_address="2")
        a3 = Address(email_address="3")
        u1.addresses.append(a1)
        u1.addresses.append(a2)
        u1.addresses.append(a3)

        u1.addresses.append(a2)
        del u1.addresses[1]

        assert a2.user is u1
        eq_(u1.addresses, [a1, a3, a2])

    def test_bulk_replace(self):
        User, Address = self.classes.User, self.classes.Address

        u1 = User()

        a1 = Address(email_address="1")
        a2 = Address(email_address="2")
        a3 = Address(email_address="3")
        u1.addresses.append(a1)
        u1.addresses.append(a2)
        u1.addresses.append(a3)
        u1.addresses.append(a3)

        assert a3.user is u1

        u1.addresses = [a1, a2, a1]

        assert a3.user is None
        eq_(u1.addresses, [a1, a2, a1])


class O2OScalarBackrefMoveTest(_fixtures.FixtureTest):
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
                    Address, backref=backref("user"), uselist=False
                )
            },
        )

    def test_collection_move_preloaded(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        a1 = Address(email_address="address1")
        u1 = User(name="jack", address=a1)

        u2 = User(name="ed")
        sess.add_all([u1, u2])
        sess.commit()  # everything is expired

        # load u1.address
        u1.address

        # reassign
        u2.address = a1
        assert u2.address is a1

        # backref fires
        assert a1.user is u2

        # doesn't extend to the previous attribute tho.
        # flushing at this point means its anyone's guess.
        assert u1.address is a1
        assert u2.address is a1

    def test_scalar_move_preloaded(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        a1 = Address(email_address="address1")
        a2 = Address(email_address="address1")
        u1 = User(name="jack", address=a1)

        sess.add_all([u1, a1, a2])
        sess.commit()  # everything is expired

        # load a1.user
        a1.user

        # reassign
        a2.user = u1

        # backref fires
        assert u1.address is a2

        # stays on both sides
        assert a1.user is u1
        assert a2.user is u1

    def test_collection_move_notloaded(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        a1 = Address(email_address="address1")
        u1 = User(name="jack", address=a1)

        u2 = User(name="ed")
        sess.add_all([u1, u2])
        sess.commit()  # everything is expired

        # reassign
        u2.address = a1
        assert u2.address is a1

        # backref fires
        assert a1.user is u2

        # u1.address loads now after a flush
        assert u1.address is None
        assert u2.address is a1

    def test_scalar_move_notloaded(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        a1 = Address(email_address="address1")
        a2 = Address(email_address="address1")
        u1 = User(name="jack", address=a1)

        sess.add_all([u1, a1, a2])
        sess.commit()  # everything is expired

        # reassign
        a2.user = u1

        # backref fires
        assert u1.address is a2

        # stays on both sides
        assert a1.user is u1
        assert a2.user is u1

    def test_collection_move_commitfirst(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        a1 = Address(email_address="address1")
        u1 = User(name="jack", address=a1)

        u2 = User(name="ed")
        sess.add_all([u1, u2])
        sess.commit()  # everything is expired

        # load u1.address
        u1.address

        # reassign
        u2.address = a1
        assert u2.address is a1

        # backref fires
        assert a1.user is u2

        # the commit cancels out u1.addresses
        # being loaded, on next access its fine.
        sess.commit()
        assert u1.address is None
        assert u2.address is a1

    def test_scalar_move_commitfirst(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        a1 = Address(email_address="address1")
        a2 = Address(email_address="address2")
        u1 = User(name="jack", address=a1)

        sess.add_all([u1, a1, a2])
        sess.commit()  # everything is expired

        # load
        assert a1.user is u1

        # reassign
        a2.user = u1

        # backref fires
        assert u1.address is a2

        # didn't work this way tho
        assert a1.user is u1

        # moves appropriately after commit
        sess.commit()
        assert u1.address is a2
        assert a1.user is None
        assert a2.user is u1


class O2OScalarMoveTest(_fixtures.FixtureTest):
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
            properties={"address": relationship(Address, uselist=False)},
        )

    def test_collection_move_commitfirst(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        a1 = Address(email_address="address1")
        u1 = User(name="jack", address=a1)

        u2 = User(name="ed")
        sess.add_all([u1, u2])
        sess.commit()  # everything is expired

        # load u1.address
        u1.address

        # reassign
        u2.address = a1
        assert u2.address is a1

        # the commit cancels out u1.addresses
        # being loaded, on next access its fine.
        sess.commit()
        assert u1.address is None
        assert u2.address is a1


class O2OScalarOrphanTest(_fixtures.FixtureTest):
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
                    uselist=False,
                    backref=backref(
                        "user",
                        single_parent=True,
                        cascade="all, delete-orphan",
                    ),
                )
            },
        )

    def test_m2o_event(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        a1 = Address(email_address="address1")
        u1 = User(name="jack", address=a1)

        sess.add(u1)
        sess.commit()
        sess.expunge(u1)

        u2 = User(name="ed")
        # the _SingleParent extension sets the backref get to "active" !
        # u1 gets loaded and deleted
        u2.address = a1
        sess.commit()
        assert sess.query(User).count() == 1


class M2MCollectionMoveTest(_fixtures.FixtureTest):
    run_inserts = None

    @classmethod
    def setup_mappers(cls):
        keywords, items, item_keywords, Keyword, Item = (
            cls.tables.keywords,
            cls.tables.items,
            cls.tables.item_keywords,
            cls.classes.Keyword,
            cls.classes.Item,
        )

        mapper(
            Item,
            items,
            properties={
                "keywords": relationship(
                    Keyword, secondary=item_keywords, backref="items"
                )
            },
        )
        mapper(Keyword, keywords)

    def test_add_remove_pending_backref(self):
        """test that pending doesn't add an item that's not a net add."""

        Item, Keyword = (self.classes.Item, self.classes.Keyword)

        session = fixture_session(autoflush=False)

        i1 = Item(description="i1")
        session.add(i1)
        session.commit()

        session.expire(i1, ["keywords"])

        k1 = Keyword(name="k1")
        k1.items.append(i1)
        k1.items.remove(i1)
        eq_(i1.keywords, [])

    def test_remove_add_pending_backref(self):
        """test that pending doesn't remove an item that's not a net remove."""

        Item, Keyword = (self.classes.Item, self.classes.Keyword)

        session = fixture_session(autoflush=False)

        k1 = Keyword(name="k1")
        i1 = Item(description="i1", keywords=[k1])
        session.add(i1)
        session.commit()

        session.expire(i1, ["keywords"])

        k1.items.remove(i1)
        k1.items.append(i1)
        eq_(i1.keywords, [k1])

    def test_pending_combines_with_flushed(self):
        """test the combination of unflushed pending + lazy loaded from DB."""

        Item, Keyword = (self.classes.Item, self.classes.Keyword)

        session = Session(testing.db, autoflush=False)

        k1 = Keyword(name="k1")
        k2 = Keyword(name="k2")
        i1 = Item(description="i1", keywords=[k1])
        session.add(i1)
        session.add(k2)
        session.commit()

        k2.items.append(i1)
        # the pending
        # list is still here.
        eq_(
            set(
                attributes.instance_state(i1)
                ._pending_mutations["keywords"]
                .added_items
            ),
            set([k2]),
        )
        # because autoflush is off, k2 is still
        # coming in from pending
        eq_(i1.keywords, [k1, k2])

        # prove it didn't flush
        eq_(session.scalar(text("select count(*) from item_keywords")), 1)

        # the pending collection was removed
        assert (
            "keywords" not in attributes.instance_state(i1)._pending_mutations
        )

    def test_duplicate_adds(self):
        Item, Keyword = (self.classes.Item, self.classes.Keyword)

        session = Session(testing.db, autoflush=False)

        k1 = Keyword(name="k1")
        i1 = Item(description="i1", keywords=[k1])
        session.add(i1)
        session.commit()

        k1.items.append(i1)
        eq_(i1.keywords, [k1, k1])

        session.expire(i1, ["keywords"])
        k1.items.append(i1)
        eq_(i1.keywords, [k1, k1])

        session.expire(i1, ["keywords"])
        k1.items.append(i1)
        eq_(i1.keywords, [k1, k1])

        eq_(k1.items, [i1, i1, i1, i1])
        session.commit()
        eq_(k1.items, [i1])

    def test_bulk_replace(self):
        Item, Keyword = (self.classes.Item, self.classes.Keyword)

        k1 = Keyword(name="k1")
        k2 = Keyword(name="k2")
        k3 = Keyword(name="k3")
        i1 = Item(description="i1", keywords=[k1, k2])
        i2 = Item(description="i2", keywords=[k3])

        i1.keywords = [k2, k3]
        assert i1 in k3.items
        assert i2 in k3.items
        assert i1 not in k1.items


class M2MScalarMoveTest(_fixtures.FixtureTest):
    run_inserts = None

    @classmethod
    def setup_mappers(cls):
        keywords, items, item_keywords, Keyword, Item = (
            cls.tables.keywords,
            cls.tables.items,
            cls.tables.item_keywords,
            cls.classes.Keyword,
            cls.classes.Item,
        )

        mapper(
            Item,
            items,
            properties={
                "keyword": relationship(
                    Keyword,
                    secondary=item_keywords,
                    uselist=False,
                    backref=backref("item", uselist=False),
                )
            },
        )
        mapper(Keyword, keywords)

    def test_collection_move_preloaded(self):
        Item, Keyword = self.classes.Item, self.classes.Keyword

        sess = fixture_session()

        k1 = Keyword(name="k1")
        i1 = Item(description="i1", keyword=k1)
        i2 = Item(description="i2")

        sess.add_all([i1, i2, k1])
        sess.commit()  # everything is expired

        # load i1.keyword
        assert i1.keyword is k1

        i2.keyword = k1

        assert k1.item is i2

        # nothing happens.
        assert i1.keyword is k1
        assert i2.keyword is k1

    def test_collection_move_notloaded(self):
        Item, Keyword = self.classes.Item, self.classes.Keyword

        sess = fixture_session()

        k1 = Keyword(name="k1")
        i1 = Item(description="i1", keyword=k1)
        i2 = Item(description="i2")

        sess.add_all([i1, i2, k1])
        sess.commit()  # everything is expired

        i2.keyword = k1

        assert k1.item is i2

        assert i1.keyword is None
        assert i2.keyword is k1

    def test_collection_move_commit(self):
        Item, Keyword = self.classes.Item, self.classes.Keyword

        sess = fixture_session()

        k1 = Keyword(name="k1")
        i1 = Item(description="i1", keyword=k1)
        i2 = Item(description="i2")

        sess.add_all([i1, i2, k1])
        sess.commit()  # everything is expired

        # load i1.keyword
        assert i1.keyword is k1

        i2.keyword = k1

        assert k1.item is i2

        sess.commit()
        assert i1.keyword is None
        assert i2.keyword is k1


class O2MStaleBackrefTest(_fixtures.FixtureTest):
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
            properties=dict(addresses=relationship(Address, backref="user")),
        )

    def test_backref_pop_m2o(self):
        User, Address = self.classes.User, self.classes.Address

        u1 = User()
        u2 = User()
        a1 = Address()
        u1.addresses.append(a1)
        u2.addresses.append(a1)

        # a1 removed from u1.addresses as of [ticket:2789]
        assert a1 not in u1.addresses

        assert a1.user is u2
        assert a1 in u2.addresses


class M2MStaleBackrefTest(_fixtures.FixtureTest):
    run_inserts = None

    @classmethod
    def setup_mappers(cls):
        keywords, items, item_keywords, Keyword, Item = (
            cls.tables.keywords,
            cls.tables.items,
            cls.tables.item_keywords,
            cls.classes.Keyword,
            cls.classes.Item,
        )

        mapper(
            Item,
            items,
            properties={
                "keywords": relationship(
                    Keyword, secondary=item_keywords, backref="items"
                )
            },
        )
        mapper(Keyword, keywords)

    def test_backref_pop_m2m(self):
        Keyword, Item = self.classes.Keyword, self.classes.Item

        k1 = Keyword()
        k2 = Keyword()
        i1 = Item()
        k1.items.append(i1)
        k2.items.append(i1)
        k2.items.append(i1)
        i1.keywords = []
        k2.items.remove(i1)
        assert len(k2.items) == 0
