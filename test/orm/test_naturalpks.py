"""
Primary key changing capabilities and passive/non-passive cascading updates.

"""

import itertools

import sqlalchemy as sa
from sqlalchemy import bindparam
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import TypeDecorator
from sqlalchemy.orm import mapper
from sqlalchemy.orm import relationship
from sqlalchemy.orm.session import make_transient
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_warnings
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import ne_
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from test.orm import _fixtures


def _backend_specific_fk_args():
    if (
        testing.requires.deferrable_fks.enabled
        and testing.requires.non_updating_cascade.enabled
    ):
        fk_args = dict(deferrable=True, initially="deferred")
    elif not testing.requires.on_update_cascade.enabled:
        fk_args = dict()
    else:
        fk_args = dict(onupdate="cascade")
    return fk_args


class NaturalPKTest(fixtures.MappedTest):
    # MySQL 5.5 on Windows crashes (the entire server, not the client)
    # if you screw around with ON UPDATE CASCADE type of stuff.
    __requires__ = ("skip_mysql_on_windows",)
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        fk_args = _backend_specific_fk_args()

        Table(
            "users",
            metadata,
            Column("username", String(50), primary_key=True),
            Column("fullname", String(100)),
            test_needs_fk=True,
        )

        Table(
            "addresses",
            metadata,
            Column("email", String(50), primary_key=True),
            Column(
                "username", String(50), ForeignKey("users.username", **fk_args)
            ),
            test_needs_fk=True,
        )

        Table(
            "items",
            metadata,
            Column("itemname", String(50), primary_key=True),
            Column("description", String(100)),
            test_needs_fk=True,
        )

        Table(
            "users_to_items",
            metadata,
            Column(
                "username",
                String(50),
                ForeignKey("users.username", **fk_args),
                primary_key=True,
            ),
            Column(
                "itemname",
                String(50),
                ForeignKey("items.itemname", **fk_args),
                primary_key=True,
            ),
            test_needs_fk=True,
        )

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            pass

        class Address(cls.Comparable):
            pass

        class Item(cls.Comparable):
            pass

    def test_entity(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        sess = fixture_session()
        u1 = User(username="jack", fullname="jack")

        sess.add(u1)
        sess.flush()
        assert sess.query(User).get("jack") is u1

        u1.username = "ed"
        sess.flush()

        def go():
            assert sess.query(User).get("ed") is u1

        self.assert_sql_count(testing.db, go, 0)

        assert sess.query(User).get("jack") is None

        sess.expunge_all()
        u1 = sess.query(User).get("ed")
        eq_(User(username="ed", fullname="jack"), u1)

    def test_load_after_expire(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        sess = fixture_session()
        u1 = User(username="jack", fullname="jack")

        sess.add(u1)
        sess.flush()
        assert sess.query(User).get("jack") is u1

        sess.execute(
            users.update(values={User.username: "jack"}), dict(username="ed")
        )

        # expire/refresh works off of primary key.  the PK is gone
        # in this case so there's no way to look it up.  criterion-
        # based session invalidation could solve this [ticket:911]
        sess.expire(u1)
        assert_raises(sa.orm.exc.ObjectDeletedError, getattr, u1, "username")

        sess.expunge_all()
        assert sess.query(User).get("jack") is None
        assert sess.query(User).get("ed").fullname == "jack"

    @testing.requires.returning
    def test_update_to_sql_expr(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        sess = fixture_session()
        u1 = User(username="jack", fullname="jack")

        sess.add(u1)
        sess.flush()

        u1.username = User.username + " jones"

        sess.flush()

        eq_(u1.username, "jack jones")

    def test_update_to_self_sql_expr(self):
        # SQL expression where the PK won't actually change,
        # such as to bump a server side trigger
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        sess = fixture_session()
        u1 = User(username="jack", fullname="jack")

        sess.add(u1)
        sess.flush()

        u1.username = User.username + ""

        sess.flush()

        eq_(u1.username, "jack")

    def test_flush_new_pk_after_expire(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users)
        sess = fixture_session()
        u1 = User(username="jack", fullname="jack")

        sess.add(u1)
        sess.flush()
        assert sess.query(User).get("jack") is u1

        sess.expire(u1)
        u1.username = "ed"
        sess.flush()
        sess.expunge_all()
        assert sess.query(User).get("ed").fullname == "jack"

    @testing.requires.on_update_cascade
    def test_onetomany_passive(self):
        self._test_onetomany(True)

    def test_onetomany_nonpassive(self):
        self._test_onetomany(False)

    def _test_onetomany(self, passive_updates):
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
                    Address, passive_updates=passive_updates
                )
            },
        )
        mapper(Address, addresses)

        sess = fixture_session()
        u1 = User(username="jack", fullname="jack")
        u1.addresses.append(Address(email="jack1"))
        u1.addresses.append(Address(email="jack2"))
        sess.add(u1)
        sess.flush()

        assert sess.query(Address).get("jack1") is u1.addresses[0]

        u1.username = "ed"
        sess.flush()
        assert u1.addresses[0].username == "ed"

        sess.expunge_all()
        eq_(
            [Address(username="ed"), Address(username="ed")],
            sess.query(Address).all(),
        )

        u1 = sess.query(User).get("ed")
        u1.username = "jack"

        def go():
            sess.flush()

        if not passive_updates:
            # test passive_updates=False;
            # load addresses, update user, update 2 addresses
            self.assert_sql_count(testing.db, go, 3)
        else:
            # test passive_updates=True; update user
            self.assert_sql_count(testing.db, go, 1)
        sess.expunge_all()
        assert (
            User(
                username="jack",
                addresses=[Address(username="jack"), Address(username="jack")],
            )
            == sess.query(User).get("jack")
        )

        u1 = sess.query(User).get("jack")
        u1.addresses = []
        u1.username = "fred"
        sess.flush()
        sess.expunge_all()
        assert sess.query(Address).get("jack1").username is None
        u1 = sess.query(User).get("fred")
        eq_(User(username="fred", fullname="jack"), u1)

    @testing.requires.on_update_cascade
    def test_manytoone_passive(self):
        self._test_manytoone(True)

    def test_manytoone_nonpassive(self):
        self._test_manytoone(False)

    @testing.requires.on_update_cascade
    def test_manytoone_passive_uselist(self):
        self._test_manytoone(True, True)

    def test_manytoone_nonpassive_uselist(self):
        self._test_manytoone(False, True)

    def test_manytoone_nonpassive_cold_mapping(self):
        """test that the mapper-level m2o dependency processor
        is set up even if the opposite side relationship
        hasn't yet been part of a flush.

        """
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        with testing.db.begin() as conn:
            conn.execute(
                users.insert(), dict(username="jack", fullname="jack")
            )
            conn.execute(
                addresses.insert(), dict(email="jack1", username="jack")
            )
            conn.execute(
                addresses.insert(), dict(email="jack2", username="jack")
            )

        mapper(User, users)
        mapper(
            Address,
            addresses,
            properties={"user": relationship(User, passive_updates=False)},
        )

        sess = fixture_session()
        u1 = sess.query(User).first()
        a1, a2 = sess.query(Address).all()
        u1.username = "ed"

        def go():
            sess.flush()

        self.assert_sql_count(testing.db, go, 2)

    def _test_manytoone(self, passive_updates, uselist=False, dynamic=False):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(User, users)
        mapper(
            Address,
            addresses,
            properties={
                "user": relationship(
                    User, uselist=uselist, passive_updates=passive_updates
                )
            },
        )

        sess = fixture_session()
        a1 = Address(email="jack1")
        a2 = Address(email="jack2")
        a3 = Address(email="fred")

        u1 = User(username="jack", fullname="jack")
        if uselist:
            a1.user = [u1]
            a2.user = [u1]
        else:
            a1.user = u1
            a2.user = u1
        sess.add(a1)
        sess.add(a2)
        sess.add(a3)
        sess.flush()

        u1.username = "ed"

        def go():
            sess.flush()

        if passive_updates:
            self.assert_sql_count(testing.db, go, 1)
        else:
            self.assert_sql_count(testing.db, go, 2)

        def go():
            sess.flush()

        self.assert_sql_count(testing.db, go, 0)

        assert a1.username == a2.username == "ed"
        sess.expunge_all()
        if uselist:
            eq_(
                [
                    Address(email="fred", user=[]),
                    Address(username="ed"),
                    Address(username="ed"),
                ],
                sess.query(Address).order_by(Address.email).all(),
            )
        else:
            eq_(
                [
                    Address(email="fred", user=None),
                    Address(username="ed"),
                    Address(username="ed"),
                ],
                sess.query(Address).order_by(Address.email).all(),
            )

    @testing.requires.on_update_cascade
    def test_onetoone_passive(self):
        self._test_onetoone(True)

    def test_onetoone_nonpassive(self):
        self._test_onetoone(False)

    def _test_onetoone(self, passive_updates):
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
                "address": relationship(
                    Address, passive_updates=passive_updates, uselist=False
                )
            },
        )
        mapper(Address, addresses)

        sess = fixture_session()
        u1 = User(username="jack", fullname="jack")
        sess.add(u1)
        sess.flush()

        a1 = Address(email="jack1")
        u1.address = a1
        sess.add(a1)
        sess.flush()

        u1.username = "ed"

        def go():
            sess.flush()

        if passive_updates:
            sess.expire(u1, ["address"])
            self.assert_sql_count(testing.db, go, 1)
        else:
            self.assert_sql_count(testing.db, go, 2)

        def go():
            sess.flush()

        self.assert_sql_count(testing.db, go, 0)

        sess.expunge_all()
        eq_([Address(username="ed")], sess.query(Address).all())

    @testing.requires.on_update_cascade
    def test_bidirectional_passive(self):
        self._test_bidirectional(True)

    def test_bidirectional_nonpassive(self):
        self._test_bidirectional(False)

    def _test_bidirectional(self, passive_updates):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(User, users)
        mapper(
            Address,
            addresses,
            properties={
                "user": relationship(
                    User, passive_updates=passive_updates, backref="addresses"
                )
            },
        )

        sess = fixture_session(autoflush=False)
        a1 = Address(email="jack1")
        a2 = Address(email="jack2")

        u1 = User(username="jack", fullname="jack")
        a1.user = u1
        a2.user = u1
        sess.add(a1)
        sess.add(a2)
        sess.flush()

        u1.username = "ed"
        (ad1, ad2) = sess.query(Address).all()
        eq_([Address(username="jack"), Address(username="jack")], [ad1, ad2])

        def go():
            sess.flush()

        if passive_updates:
            self.assert_sql_count(testing.db, go, 1)
        else:
            # two updates bundled
            self.assert_sql_count(testing.db, go, 2)
        eq_([Address(username="ed"), Address(username="ed")], [ad1, ad2])
        sess.expunge_all()
        eq_(
            [Address(username="ed"), Address(username="ed")],
            sess.query(Address).all(),
        )

        u1 = sess.query(User).get("ed")
        assert len(u1.addresses) == 2  # load addresses
        u1.username = "fred"

        def go():
            sess.flush()

        # check that the passive_updates is on on the other side
        if passive_updates:
            self.assert_sql_count(testing.db, go, 1)
        else:
            # two updates bundled
            self.assert_sql_count(testing.db, go, 2)
        sess.expunge_all()
        eq_(
            [Address(username="fred"), Address(username="fred")],
            sess.query(Address).all(),
        )

    @testing.requires.on_update_cascade
    def test_manytomany_passive(self):
        self._test_manytomany(True)

    @testing.fails_if(
        testing.requires.on_update_cascade
        + testing.requires.sane_multi_rowcount
    )
    def test_manytomany_nonpassive(self):
        self._test_manytomany(False)

    def _test_manytomany(self, passive_updates):
        users, items, Item, User, users_to_items = (
            self.tables.users,
            self.tables.items,
            self.classes.Item,
            self.classes.User,
            self.tables.users_to_items,
        )

        mapper(
            User,
            users,
            properties={
                "items": relationship(
                    Item,
                    secondary=users_to_items,
                    backref="users",
                    passive_updates=passive_updates,
                )
            },
        )
        mapper(Item, items)

        sess = fixture_session()
        u1 = User(username="jack")
        u2 = User(username="fred")
        i1 = Item(itemname="item1")
        i2 = Item(itemname="item2")

        u1.items.append(i1)
        u1.items.append(i2)
        i2.users.append(u2)
        sess.add(u1)
        sess.add(u2)
        sess.flush()

        r = sess.query(Item).all()
        # ComparableEntity can't handle a comparison with the backrefs
        # involved....
        eq_(Item(itemname="item1"), r[0])
        eq_(["jack"], [u.username for u in r[0].users])
        eq_(Item(itemname="item2"), r[1])
        eq_(["jack", "fred"], [u.username for u in r[1].users])

        u2.username = "ed"

        def go():
            sess.flush()

        go()

        def go():
            sess.flush()

        self.assert_sql_count(testing.db, go, 0)

        sess.expunge_all()
        r = sess.query(Item).all()
        eq_(Item(itemname="item1"), r[0])
        eq_(["jack"], [u.username for u in r[0].users])
        eq_(Item(itemname="item2"), r[1])
        eq_(["ed", "jack"], sorted([u.username for u in r[1].users]))

        sess.expunge_all()
        u2 = sess.query(User).get(u2.username)
        u2.username = "wendy"
        sess.flush()
        r = sess.query(Item).with_parent(u2).all()
        eq_(Item(itemname="item2"), r[0])

    def test_manytoone_deferred_relationship_expr(self):
        """for [ticket:4359], test that updates to the columns embedded
        in an object expression are also updated."""
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(User, users)
        mapper(
            Address,
            addresses,
            properties={
                "user": relationship(
                    User,
                    passive_updates=testing.requires.on_update_cascade.enabled,
                )
            },
        )

        s = fixture_session()
        a1 = Address(email="jack1")
        u1 = User(username="jack", fullname="jack")

        a1.user = u1

        # scenario 1.  object is still transient, we get a value.
        expr = Address.user == u1

        eq_(expr.left.callable(), "jack")

        # scenario 2.  value has been changed while we are transient.
        # we get the updated value.
        u1.username = "ed"
        eq_(expr.left.callable(), "ed")

        s.add_all([u1, a1])
        s.commit()

        eq_(a1.username, "ed")

        # scenario 3.  the value is changed and flushed, we get the new value.
        u1.username = "fred"
        s.flush()

        eq_(expr.left.callable(), "fred")

        # scenario 4.  the value is changed, flushed, and expired.
        # the callable goes out to get that value.
        u1.username = "wendy"
        s.commit()
        assert "username" not in u1.__dict__

        eq_(expr.left.callable(), "wendy")

        # scenario 5.  the value is changed flushed, expired,
        # and then when we hit the callable, we are detached.
        u1.username = "jack"
        s.commit()
        assert "username" not in u1.__dict__

        s.expunge(u1)

        # InstanceState has a "last known values" feature we use
        # to pick up on this
        eq_(expr.left.callable(), "jack")

        # doesn't unexpire the attribute
        assert "username" not in u1.__dict__

        # once we are persistent again, we check the DB
        s.add(u1)
        eq_(expr.left.callable(), "jack")
        assert "username" in u1.__dict__

        # scenario 6.  we are using del
        u2 = User(username="jack", fullname="jack")
        expr = Address.user == u2

        eq_(expr.left.callable(), "jack")

        del u2.username

        assert_raises_message(
            sa.exc.InvalidRequestError,
            "Can't resolve value for column users.username",
            expr.left.callable,
        )

        u2.username = "ed"
        eq_(expr.left.callable(), "ed")

        s.add(u2)
        s.commit()

        eq_(expr.left.callable(), "ed")

        del u2.username

        # object is persistent, so since we deleted, we get None
        with expect_warnings("Got None for value of column "):
            eq_(expr.left.callable(), None)

        s.expunge(u2)

        # however that None isn't in the dict, that's just the default
        # attribute value, so after expunge it's gone
        assert "username" not in u2.__dict__

        # detached, we don't have it
        assert_raises_message(
            sa.exc.InvalidRequestError,
            "Can't resolve value for column users.username",
            expr.left.callable,
        )


class TransientExceptionTesst(_fixtures.FixtureTest):
    run_inserts = None
    __backend__ = True

    def test_transient_exception(self):
        """An object that goes from a pk value to transient/pending
        doesn't count as a "pk" switch.

        """

        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(User, users)
        mapper(Address, addresses, properties={"user": relationship(User)})

        sess = fixture_session()
        u1 = User(id=5, name="u1")
        ad1 = Address(email_address="e1", user=u1)
        sess.add_all([u1, ad1])
        sess.flush()

        make_transient(u1)
        u1.id = None
        u1.username = "u2"
        sess.add(u1)
        sess.flush()

        eq_(ad1.user_id, 5)

        sess.expire_all()
        eq_(ad1.user_id, 5)
        ne_(u1.id, 5)
        ne_(u1.id, None)
        eq_(sess.query(User).count(), 2)


class ReversePKsTest(fixtures.MappedTest):
    """reverse the primary keys of two entities and ensure bookkeeping
    succeeds."""

    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "user",
            metadata,
            Column("code", Integer, autoincrement=False, primary_key=True),
            Column("status", Integer, autoincrement=False, primary_key=True),
            Column("username", String(50), nullable=False),
            test_needs_acid=True,
        )

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            def __init__(self, code, status, username):
                self.code = code
                self.status = status
                self.username = username

    def test_reverse(self):
        user, User = self.tables.user, self.classes.User

        PUBLISHED, EDITABLE, ARCHIVED = 1, 2, 3

        mapper(User, user)

        session = fixture_session()

        a_published = User(1, PUBLISHED, "a")
        session.add(a_published)
        session.commit()

        a_editable = User(1, EDITABLE, "a")

        session.add(a_editable)
        session.commit()

        # see also much more recent issue #4890 where we add a warning
        # for almost this same case

        # do the switch in both directions -
        # one or the other should raise the error
        # based on platform dictionary ordering
        a_published.status = ARCHIVED
        a_editable.status = PUBLISHED

        session.commit()
        assert session.query(User).get([1, PUBLISHED]) is a_editable
        assert session.query(User).get([1, ARCHIVED]) is a_published

        a_published.status = PUBLISHED
        a_editable.status = EDITABLE

        session.commit()

        assert session.query(User).get([1, PUBLISHED]) is a_published
        assert session.query(User).get([1, EDITABLE]) is a_editable

    @testing.requires.savepoints
    def test_reverse_savepoint(self):
        user, User = self.tables.user, self.classes.User

        PUBLISHED, EDITABLE, ARCHIVED = 1, 2, 3

        mapper(User, user)

        session = fixture_session()

        a_published = User(1, PUBLISHED, "a")
        session.add(a_published)
        session.commit()

        a_editable = User(1, EDITABLE, "a")

        session.add(a_editable)
        session.commit()

        # testing #3108
        session.begin_nested()

        a_published.status = ARCHIVED
        a_editable.status = PUBLISHED

        session.commit()

        session.rollback()
        eq_(a_published.status, PUBLISHED)
        eq_(a_editable.status, EDITABLE)


class SelfReferentialTest(fixtures.MappedTest):
    # mssql, mysql don't allow
    # ON UPDATE on self-referential keys
    __unsupported_on__ = ("mssql", "mysql", "mariadb")

    __requires__ = ("on_update_or_deferrable_fks",)
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        fk_args = _backend_specific_fk_args()

        Table(
            "nodes",
            metadata,
            Column("name", String(50), primary_key=True),
            Column("parent", String(50), ForeignKey("nodes.name", **fk_args)),
            test_needs_fk=True,
        )

    @classmethod
    def setup_classes(cls):
        class Node(cls.Comparable):
            pass

    def test_one_to_many_on_m2o(self):
        Node, nodes = self.classes.Node, self.tables.nodes

        mapper(
            Node,
            nodes,
            properties={
                "children": relationship(
                    Node,
                    backref=sa.orm.backref(
                        "parentnode",
                        remote_side=nodes.c.name,
                        passive_updates=False,
                    ),
                )
            },
        )

        sess = fixture_session()
        n1 = Node(name="n1")
        sess.add(n1)
        n2 = Node(name="n11", parentnode=n1)
        n3 = Node(name="n12", parentnode=n1)
        n4 = Node(name="n13", parentnode=n1)
        sess.add_all([n2, n3, n4])
        sess.commit()

        n1.name = "new n1"
        sess.commit()
        eq_(
            ["new n1", "new n1", "new n1"],
            [
                n.parent
                for n in sess.query(Node).filter(
                    Node.name.in_(["n11", "n12", "n13"])
                )
            ],
        )

    def test_one_to_many_on_o2m(self):
        Node, nodes = self.classes.Node, self.tables.nodes

        mapper(
            Node,
            nodes,
            properties={
                "children": relationship(
                    Node,
                    backref=sa.orm.backref(
                        "parentnode", remote_side=nodes.c.name
                    ),
                    passive_updates=False,
                )
            },
        )

        sess = fixture_session()
        n1 = Node(name="n1")
        n1.children.append(Node(name="n11"))
        n1.children.append(Node(name="n12"))
        n1.children.append(Node(name="n13"))
        sess.add(n1)
        sess.commit()

        n1.name = "new n1"
        sess.commit()
        eq_(n1.children[1].parent, "new n1")
        eq_(
            ["new n1", "new n1", "new n1"],
            [
                n.parent
                for n in sess.query(Node).filter(
                    Node.name.in_(["n11", "n12", "n13"])
                )
            ],
        )

    @testing.requires.on_update_cascade
    def test_many_to_one_passive(self):
        self._test_many_to_one(True)

    def test_many_to_one_nonpassive(self):
        self._test_many_to_one(False)

    def _test_many_to_one(self, passive):
        Node, nodes = self.classes.Node, self.tables.nodes

        mapper(
            Node,
            nodes,
            properties={
                "parentnode": relationship(
                    Node, remote_side=nodes.c.name, passive_updates=passive
                )
            },
        )

        sess = fixture_session()
        n1 = Node(name="n1")
        n11 = Node(name="n11", parentnode=n1)
        n12 = Node(name="n12", parentnode=n1)
        n13 = Node(name="n13", parentnode=n1)
        sess.add_all([n1, n11, n12, n13])
        sess.commit()

        n1.name = "new n1"
        sess.commit()
        eq_(
            ["new n1", "new n1", "new n1"],
            [
                n.parent
                for n in sess.query(Node).filter(
                    Node.name.in_(["n11", "n12", "n13"])
                )
            ],
        )


class NonPKCascadeTest(fixtures.MappedTest):
    __requires__ = "skip_mysql_on_windows", "on_update_or_deferrable_fks"
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        fk_args = _backend_specific_fk_args()

        Table(
            "users",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("username", String(50), unique=True),
            Column("fullname", String(100)),
            test_needs_fk=True,
        )

        Table(
            "addresses",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("email", String(50)),
            Column(
                "username", String(50), ForeignKey("users.username", **fk_args)
            ),
            test_needs_fk=True,
        )

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            pass

        class Address(cls.Comparable):
            pass

    @testing.requires.on_update_cascade
    def test_onetomany_passive(self):
        self._test_onetomany(True)

    def test_onetomany_nonpassive(self):
        self._test_onetomany(False)

    def _test_onetomany(self, passive_updates):
        User, Address, users, addresses = (
            self.classes.User,
            self.classes.Address,
            self.tables.users,
            self.tables.addresses,
        )

        mapper(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address, passive_updates=passive_updates
                )
            },
        )
        mapper(Address, addresses)

        sess = fixture_session()
        u1 = User(username="jack", fullname="jack")
        u1.addresses.append(Address(email="jack1"))
        u1.addresses.append(Address(email="jack2"))
        sess.add(u1)
        sess.flush()
        a1 = u1.addresses[0]

        eq_(
            sess.execute(sa.select(addresses.c.username)).fetchall(),
            [("jack",), ("jack",)],
        )

        assert sess.query(Address).get(a1.id) is u1.addresses[0]

        u1.username = "ed"
        sess.flush()
        assert u1.addresses[0].username == "ed"
        eq_(
            sess.execute(sa.select(addresses.c.username)).fetchall(),
            [("ed",), ("ed",)],
        )

        sess.expunge_all()
        eq_(
            [Address(username="ed"), Address(username="ed")],
            sess.query(Address).all(),
        )

        u1 = sess.query(User).get(u1.id)
        u1.username = "jack"

        def go():
            sess.flush()

        if not passive_updates:
            # test passive_updates=False; load addresses,
            #  update user, update 2 addresses (in one executemany)
            self.assert_sql_count(testing.db, go, 3)
        else:
            # test passive_updates=True; update user
            self.assert_sql_count(testing.db, go, 1)
        sess.expunge_all()
        assert (
            User(
                username="jack",
                addresses=[Address(username="jack"), Address(username="jack")],
            )
            == sess.query(User).get(u1.id)
        )
        sess.expunge_all()

        u1 = sess.query(User).get(u1.id)
        u1.addresses = []
        u1.username = "fred"
        sess.flush()
        sess.expunge_all()
        a1 = sess.query(Address).get(a1.id)
        eq_(a1.username, None)

        eq_(
            sess.execute(sa.select(addresses.c.username)).fetchall(),
            [(None,), (None,)],
        )

        u1 = sess.query(User).get(u1.id)
        eq_(User(username="fred", fullname="jack"), u1)


class CascadeToFKPKTest(fixtures.MappedTest, testing.AssertsCompiledSQL):
    """A primary key mutation cascades onto a foreign key that is itself a
    primary key."""

    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        fk_args = _backend_specific_fk_args()

        Table(
            "users",
            metadata,
            Column("username", String(50), primary_key=True),
            test_needs_fk=True,
        )

        Table(
            "addresses",
            metadata,
            Column(
                "username",
                String(50),
                ForeignKey("users.username", **fk_args),
                primary_key=True,
            ),
            Column("email", String(50), primary_key=True),
            Column("etc", String(50)),
            test_needs_fk=True,
        )

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            pass

        class Address(cls.Comparable):
            pass

    @testing.requires.on_update_cascade
    def test_onetomany_passive(self):
        self._test_onetomany(True)

    @testing.requires.non_updating_cascade
    def test_onetomany_nonpassive(self):
        self._test_onetomany(False)

    def test_o2m_change_passive(self):
        self._test_o2m_change(True)

    def test_o2m_change_nonpassive(self):
        self._test_o2m_change(False)

    def _test_o2m_change(self, passive_updates):
        """Change the PK of a related entity to another.

        "on update cascade" is not involved here, so the mapper has
        to do the UPDATE itself.

        """

        User, Address, users, addresses = (
            self.classes.User,
            self.classes.Address,
            self.tables.users,
            self.tables.addresses,
        )

        mapper(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address, passive_updates=passive_updates
                )
            },
        )
        mapper(Address, addresses)

        sess = fixture_session()
        a1 = Address(username="ed", email="ed@host1")
        u1 = User(username="ed", addresses=[a1])
        u2 = User(username="jack")

        sess.add_all([a1, u1, u2])
        sess.flush()

        a1.username = "jack"
        sess.flush()

    def test_o2m_move_passive(self):
        self._test_o2m_move(True)

    def test_o2m_move_nonpassive(self):
        self._test_o2m_move(False)

    def _test_o2m_move(self, passive_updates):
        """Move the related entity to a different collection,
        changing its PK.

        """

        User, Address, users, addresses = (
            self.classes.User,
            self.classes.Address,
            self.tables.users,
            self.tables.addresses,
        )

        mapper(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address, passive_updates=passive_updates
                )
            },
        )
        mapper(Address, addresses)

        sess = fixture_session(autoflush=False)
        a1 = Address(username="ed", email="ed@host1")
        u1 = User(username="ed", addresses=[a1])
        u2 = User(username="jack")

        sess.add_all([a1, u1, u2])
        sess.flush()

        u1.addresses.remove(a1)
        u2.addresses.append(a1)
        sess.flush()

    @testing.requires.on_update_cascade
    def test_change_m2o_passive(self):
        self._test_change_m2o(True)

    @testing.requires.non_updating_cascade
    def test_change_m2o_nonpassive(self):
        self._test_change_m2o(False)

    @testing.requires.on_update_cascade
    def test_change_m2o_passive_uselist(self):
        self._test_change_m2o(True, True)

    @testing.requires.non_updating_cascade
    def test_change_m2o_nonpassive_uselist(self):
        self._test_change_m2o(False, True)

    def _test_change_m2o(self, passive_updates, uselist=False):
        User, Address, users, addresses = (
            self.classes.User,
            self.classes.Address,
            self.tables.users,
            self.tables.addresses,
        )

        mapper(User, users)
        mapper(
            Address,
            addresses,
            properties={
                "user": relationship(
                    User, uselist=uselist, passive_updates=passive_updates
                )
            },
        )

        sess = fixture_session()
        u1 = User(username="jack")
        if uselist:
            a1 = Address(user=[u1], email="foo@bar")
        else:
            a1 = Address(user=u1, email="foo@bar")
        sess.add_all([u1, a1])
        sess.flush()

        u1.username = "edmodified"
        sess.flush()
        eq_(a1.username, "edmodified")

        sess.expire_all()
        eq_(a1.username, "edmodified")

    def test_move_m2o_passive(self):
        self._test_move_m2o(True)

    def test_move_m2o_nonpassive(self):
        self._test_move_m2o(False)

    def _test_move_m2o(self, passive_updates):
        User, Address, users, addresses = (
            self.classes.User,
            self.classes.Address,
            self.tables.users,
            self.tables.addresses,
        )

        # tests [ticket:1856]
        mapper(User, users)
        mapper(
            Address,
            addresses,
            properties={
                "user": relationship(User, passive_updates=passive_updates)
            },
        )

        sess = fixture_session()
        u1 = User(username="jack")
        u2 = User(username="ed")
        a1 = Address(user=u1, email="foo@bar")
        sess.add_all([u1, u2, a1])
        sess.flush()

        a1.user = u2
        sess.flush()

    def test_rowswitch_doesntfire(self):
        User, Address, users, addresses = (
            self.classes.User,
            self.classes.Address,
            self.tables.users,
            self.tables.addresses,
        )

        mapper(User, users)
        mapper(
            Address,
            addresses,
            properties={"user": relationship(User, passive_updates=True)},
        )

        sess = fixture_session()
        u1 = User(username="ed")
        a1 = Address(user=u1, email="ed@host1")

        sess.add(u1)
        sess.add(a1)
        sess.flush()

        sess.delete(u1)
        sess.delete(a1)

        u2 = User(username="ed")
        a2 = Address(user=u2, email="ed@host1", etc="foo")
        sess.add(u2)
        sess.add(a2)

        from sqlalchemy.testing.assertsql import CompiledSQL

        # test that the primary key columns of addresses are not
        # being updated as well, since this is a row switch.
        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL(
                "UPDATE addresses SET etc=:etc WHERE "
                "addresses.username = :addresses_username AND"
                " addresses.email = :addresses_email",
                {
                    "etc": "foo",
                    "addresses_username": "ed",
                    "addresses_email": "ed@host1",
                },
            ),
        )

    def _test_onetomany(self, passive_updates):
        """Change the PK of a related entity via foreign key cascade.

        For databases that require "on update cascade", the mapper
        has to identify the row by the new value, not the old, when
        it does the update.

        """

        User, Address, users, addresses = (
            self.classes.User,
            self.classes.Address,
            self.tables.users,
            self.tables.addresses,
        )

        mapper(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address, passive_updates=passive_updates
                )
            },
        )
        mapper(Address, addresses)

        sess = fixture_session()
        a1, a2 = (
            Address(username="ed", email="ed@host1"),
            Address(username="ed", email="ed@host2"),
        )
        u1 = User(username="ed", addresses=[a1, a2])
        sess.add(u1)
        sess.flush()
        eq_(a1.username, "ed")
        eq_(a2.username, "ed")
        eq_(
            sess.execute(sa.select(addresses.c.username)).fetchall(),
            [("ed",), ("ed",)],
        )

        u1.username = "jack"
        a2.email = "ed@host3"
        sess.flush()

        eq_(a1.username, "jack")
        eq_(a2.username, "jack")
        eq_(
            sess.execute(sa.select(addresses.c.username)).fetchall(),
            [("jack",), ("jack",)],
        )


class JoinedInheritanceTest(fixtures.MappedTest):
    """Test cascades of pk->pk/fk on joined table inh."""

    # mssql doesn't allow ON UPDATE on self-referential keys
    __unsupported_on__ = ("mssql",)

    __requires__ = ("skip_mysql_on_windows",)
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        fk_args = _backend_specific_fk_args()

        Table(
            "person",
            metadata,
            Column("name", String(50), primary_key=True),
            Column("type", String(50), nullable=False),
            test_needs_fk=True,
        )

        Table(
            "engineer",
            metadata,
            Column(
                "name",
                String(50),
                ForeignKey("person.name", **fk_args),
                primary_key=True,
            ),
            Column("primary_language", String(50)),
            Column(
                "boss_name", String(50), ForeignKey("manager.name", **fk_args)
            ),
            test_needs_fk=True,
        )

        Table(
            "manager",
            metadata,
            Column(
                "name",
                String(50),
                ForeignKey("person.name", **fk_args),
                primary_key=True,
            ),
            Column("paperwork", String(50)),
            test_needs_fk=True,
        )

        Table(
            "owner",
            metadata,
            Column(
                "name",
                String(50),
                ForeignKey("manager.name", **fk_args),
                primary_key=True,
            ),
            Column("owner_name", String(50)),
            test_needs_fk=True,
        )

    @classmethod
    def setup_classes(cls):
        class Person(cls.Comparable):
            pass

        class Engineer(Person):
            pass

        class Manager(Person):
            pass

        class Owner(Manager):
            pass

    def _mapping_fixture(self, threelevel, passive_updates):
        Person, Manager, Engineer, Owner = self.classes(
            "Person", "Manager", "Engineer", "Owner"
        )
        person, manager, engineer, owner = self.tables(
            "person", "manager", "engineer", "owner"
        )

        mapper(
            Person,
            person,
            polymorphic_on=person.c.type,
            polymorphic_identity="person",
            passive_updates=passive_updates,
        )

        mapper(
            Engineer,
            engineer,
            inherits=Person,
            polymorphic_identity="engineer",
            properties={
                "boss": relationship(
                    Manager,
                    primaryjoin=manager.c.name == engineer.c.boss_name,
                    passive_updates=passive_updates,
                )
            },
        )

        mapper(
            Manager, manager, inherits=Person, polymorphic_identity="manager"
        )

        if threelevel:
            mapper(
                Owner, owner, inherits=Manager, polymorphic_identity="owner"
            )

    @testing.requires.on_update_cascade
    def test_pk_passive(self):
        self._test_pk(True)

    @testing.requires.non_updating_cascade
    def test_pk_nonpassive(self):
        self._test_pk(False)

    @testing.requires.on_update_cascade
    def test_fk_passive(self):
        self._test_fk(True)

    # PG etc. need passive=True to allow PK->PK cascade
    @testing.requires.non_updating_cascade
    def test_fk_nonpassive(self):
        self._test_fk(False)

    @testing.requires.on_update_cascade
    def test_pk_threelevel_passive(self):
        self._test_pk_threelevel(True)

    @testing.requires.non_updating_cascade
    def test_pk_threelevel_nonpassive(self):
        self._test_pk_threelevel(False)

    @testing.requires.on_update_cascade
    def test_fk_threelevel_passive(self):
        self._test_fk_threelevel(True)

    # PG etc. need passive=True to allow PK->PK cascade
    @testing.requires.non_updating_cascade
    def test_fk_threelevel_nonpassive(self):
        self._test_fk_threelevel(False)

    def _test_pk(self, passive_updates):
        (Engineer,) = self.classes("Engineer")
        self._mapping_fixture(False, passive_updates)
        sess = fixture_session()

        e1 = Engineer(name="dilbert", primary_language="java")
        sess.add(e1)
        sess.commit()
        e1.name = "wally"
        e1.primary_language = "c++"

        sess.commit()
        eq_(
            sess.execute(self.tables.engineer.select()).fetchall(),
            [("wally", "c++", None)],
        )

        eq_(e1.name, "wally")

        e1.name = "dogbert"
        sess.commit()
        eq_(e1.name, "dogbert")

        eq_(
            sess.execute(self.tables.engineer.select()).fetchall(),
            [("dogbert", "c++", None)],
        )

    def _test_fk(self, passive_updates):
        Manager, Engineer = self.classes("Manager", "Engineer")

        self._mapping_fixture(False, passive_updates)

        sess = fixture_session()

        m1 = Manager(name="dogbert", paperwork="lots")
        e1, e2 = (
            Engineer(name="dilbert", primary_language="java", boss=m1),
            Engineer(name="wally", primary_language="c++", boss=m1),
        )
        sess.add_all([e1, e2, m1])
        sess.commit()

        eq_(e1.boss_name, "dogbert")
        eq_(e2.boss_name, "dogbert")

        eq_(
            sess.execute(
                self.tables.engineer.select().order_by(Engineer.name)
            ).fetchall(),
            [("dilbert", "java", "dogbert"), ("wally", "c++", "dogbert")],
        )

        sess.expire_all()

        m1.name = "pointy haired"
        e1.primary_language = "scala"
        e2.primary_language = "cobol"
        sess.commit()

        eq_(e1.boss_name, "pointy haired")
        eq_(e2.boss_name, "pointy haired")

        eq_(
            sess.execute(
                self.tables.engineer.select().order_by(Engineer.name)
            ).fetchall(),
            [
                ("dilbert", "scala", "pointy haired"),
                ("wally", "cobol", "pointy haired"),
            ],
        )

    def _test_pk_threelevel(self, passive_updates):
        (Owner,) = self.classes("Owner")

        self._mapping_fixture(True, passive_updates)

        sess = fixture_session()

        o1 = Owner(name="dogbert", owner_name="dog")
        sess.add(o1)
        sess.commit()
        o1.name = "pointy haired"
        o1.owner_name = "pointy"
        sess.commit()

        eq_(
            sess.execute(self.tables.manager.select()).fetchall(),
            [("pointy haired", None)],
        )
        eq_(
            sess.execute(self.tables.owner.select()).fetchall(),
            [("pointy haired", "pointy")],
        )

        eq_(o1.name, "pointy haired")

        o1.name = "catbert"
        sess.commit()

        eq_(o1.name, "catbert")

        eq_(
            sess.execute(self.tables.manager.select()).fetchall(),
            [("catbert", None)],
        )
        eq_(
            sess.execute(self.tables.owner.select()).fetchall(),
            [("catbert", "pointy")],
        )

    def _test_fk_threelevel(self, passive_updates):
        Owner, Engineer = self.classes("Owner", "Engineer")
        self._mapping_fixture(True, passive_updates)

        sess = fixture_session()

        m1 = Owner(name="dogbert", paperwork="lots", owner_name="dog")
        e1, e2 = (
            Engineer(name="dilbert", primary_language="java", boss=m1),
            Engineer(name="wally", primary_language="c++", boss=m1),
        )
        sess.add_all([e1, e2, m1])
        sess.commit()

        eq_(e1.boss_name, "dogbert")
        eq_(e2.boss_name, "dogbert")
        sess.expire_all()

        m1.name = "pointy haired"

        e1.primary_language = "scala"
        e2.primary_language = "cobol"
        sess.commit()

        eq_(e1.boss_name, "pointy haired")
        eq_(e2.boss_name, "pointy haired")

        eq_(
            sess.execute(self.tables.manager.select()).fetchall(),
            [("pointy haired", "lots")],
        )
        eq_(
            sess.execute(self.tables.owner.select()).fetchall(),
            [("pointy haired", "dog")],
        )


class UnsortablePKTest(fixtures.MappedTest):
    """Test integration with TypeEngine.sort_key_function"""

    class HashableDict(dict):
        def __hash__(self):
            return hash((self["x"], self["y"]))

    @classmethod
    def define_tables(cls, metadata):
        class MyUnsortable(TypeDecorator):
            impl = String(10)

            def process_bind_param(self, value, dialect):
                return "%s,%s" % (value["x"], value["y"])

            def process_result_value(self, value, dialect):
                rec = value.split(",")
                return cls.HashableDict({"x": rec[0], "y": rec[1]})

            def sort_key_function(self, value):
                return (value["x"], value["y"])

        Table(
            "data",
            metadata,
            Column("info", MyUnsortable(), primary_key=True),
            Column("int_value", Integer),
        )

    @classmethod
    def setup_classes(cls):
        class Data(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        mapper(cls.classes.Data, cls.tables.data)

    def test_updates_sorted(self):
        Data = self.classes.Data
        s = fixture_session()

        s.add_all(
            [
                Data(info=self.HashableDict(x="a", y="b")),
                Data(info=self.HashableDict(x="a", y="a")),
                Data(info=self.HashableDict(x="b", y="b")),
                Data(info=self.HashableDict(x="b", y="a")),
            ]
        )
        s.commit()

        aa, ab, ba, bb = s.query(Data).order_by(Data.info).all()

        counter = itertools.count()
        ab.int_value = bindparam(key=None, callable_=lambda: next(counter))
        ba.int_value = bindparam(key=None, callable_=lambda: next(counter))
        bb.int_value = bindparam(key=None, callable_=lambda: next(counter))
        aa.int_value = bindparam(key=None, callable_=lambda: next(counter))

        s.commit()

        eq_(
            s.query(Data.int_value).order_by(Data.info).all(),
            [(0,), (1,), (2,), (3,)],
        )


class JoinedInheritancePKOnFKTest(fixtures.MappedTest):
    """Test cascades of pk->non-pk/fk on joined table inh."""

    # mssql doesn't allow ON UPDATE on self-referential keys
    __unsupported_on__ = ("mssql",)

    __requires__ = ("skip_mysql_on_windows",)
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        fk_args = _backend_specific_fk_args()

        Table(
            "person",
            metadata,
            Column("name", String(50), primary_key=True),
            Column("type", String(50), nullable=False),
            test_needs_fk=True,
        )

        Table(
            "engineer",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column(
                "person_name", String(50), ForeignKey("person.name", **fk_args)
            ),
            Column("primary_language", String(50)),
            test_needs_fk=True,
        )

    @classmethod
    def setup_classes(cls):
        class Person(cls.Comparable):
            pass

        class Engineer(Person):
            pass

    def _test_pk(self, passive_updates):
        Person, person, Engineer, engineer = (
            self.classes.Person,
            self.tables.person,
            self.classes.Engineer,
            self.tables.engineer,
        )

        mapper(
            Person,
            person,
            polymorphic_on=person.c.type,
            polymorphic_identity="person",
            passive_updates=passive_updates,
        )
        mapper(
            Engineer,
            engineer,
            inherits=Person,
            polymorphic_identity="engineer",
        )

        sess = fixture_session()

        e1 = Engineer(name="dilbert", primary_language="java")
        sess.add(e1)
        sess.commit()
        e1.name = "wally"
        e1.primary_language = "c++"

        sess.flush()

        eq_(e1.person_name, "wally")

        sess.expire_all()
        eq_(e1.primary_language, "c++")

    @testing.requires.on_update_cascade
    def test_pk_passive(self):
        self._test_pk(True)

    # @testing.requires.non_updating_cascade
    def test_pk_nonpassive(self):
        self._test_pk(False)
