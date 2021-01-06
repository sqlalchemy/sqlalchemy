"""test the current state of the hasparent() flag."""

from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import testing
from sqlalchemy.orm import attributes
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy.orm import mapper
from sqlalchemy.orm import relationship
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from sqlalchemy.testing.util import gc_collect


class ParentRemovalTest(fixtures.MappedTest):
    """Test that the 'hasparent' flag gets flipped to False
    only if we're sure this object is the real parent.

    In ambiguous cases a stale data exception is
    raised.

    """

    run_inserts = None

    @classmethod
    def define_tables(cls, metadata):
        if testing.against("oracle"):
            fk_args = dict(deferrable=True, initially="deferred")
        elif testing.against("mysql"):
            fk_args = {}
        else:
            fk_args = dict(onupdate="cascade")

        Table(
            "users",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
        )
        Table(
            "addresses",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("user_id", Integer, ForeignKey("users.id", **fk_args)),
        )

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            pass

        class Address(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        mapper(cls.classes.Address, cls.tables.addresses)
        mapper(
            cls.classes.User,
            cls.tables.users,
            properties={
                "addresses": relationship(
                    cls.classes.Address, cascade="all, delete-orphan"
                )
            },
        )

    def _assert_hasparent(self, a1):
        assert attributes.has_parent(self.classes.User, a1, "addresses")

    def _assert_not_hasparent(self, a1):
        assert not attributes.has_parent(self.classes.User, a1, "addresses")

    def _fixture(self):
        User, Address = self.classes.User, self.classes.Address

        s = fixture_session()

        u1 = User()
        a1 = Address()
        u1.addresses.append(a1)
        s.add(u1)
        s.flush()
        return s, u1, a1

    def test_stale_state_positive(self):
        User = self.classes.User
        s, u1, a1 = self._fixture()

        s.expunge(u1)

        u1 = s.query(User).first()
        u1.addresses.remove(a1)

        self._assert_not_hasparent(a1)

    @testing.requires.predictable_gc
    def test_stale_state_positive_gc(self):
        User = self.classes.User
        s, u1, a1 = self._fixture()

        s.expunge(u1)
        del u1
        gc_collect()

        u1 = s.query(User).first()
        u1.addresses.remove(a1)

        self._assert_not_hasparent(a1)

    @testing.requires.updateable_autoincrement_pks
    @testing.requires.predictable_gc
    def test_stale_state_positive_pk_change(self):
        """Illustrate that we can't easily link a
        stale state to a fresh one if the fresh one has
        a PK change  (unless we a. tracked all the previous PKs,
        wasteful, or b. recycled states - time consuming,
        breaks lots of edge cases, destabilizes the code)

        """

        User = self.classes.User
        s, u1, a1 = self._fixture()

        s._expunge_states([attributes.instance_state(u1)])

        del u1
        gc_collect()

        u1 = s.query(User).first()

        # primary key change.  now we
        # can't rely on state.key as the
        # identifier.
        new_id = u1.id + 10
        u1.id = new_id
        a1.user_id = new_id
        s.flush()

        assert_raises_message(
            orm_exc.StaleDataError,
            "can't be sure this is the most recent parent.",
            u1.addresses.remove,
            a1,
        )

        # u1.addresses wasn't actually impacted, because the event was
        # caught before collection mutation
        eq_(u1.addresses, [a1])

        # expire all and we can continue
        s.expire_all()
        u1.addresses.remove(a1)

        self._assert_not_hasparent(a1)

    def test_stale_state_negative_child_expired(self):
        """illustrate the current behavior of
        expiration on the child.

        there's some uncertainty here in how
        this use case should work.

        """
        User = self.classes.User
        s, u1, a1 = self._fixture()

        u2 = User(addresses=[a1])  # noqa

        s.expire(a1)
        u1.addresses.remove(a1)

        # controversy here.  The action is
        # to expire one object, not the other, and remove;
        # this is pretty abusive in any case. for now
        # we are expiring away the 'parents' collection
        # so the remove will unset the hasparent flag.
        # this is what has occurred historically in any case.
        self._assert_not_hasparent(a1)
        # self._assert_hasparent(a1)

    @testing.requires.predictable_gc
    def test_stale_state_negative(self):
        User = self.classes.User
        s, u1, a1 = self._fixture()

        u2 = User(addresses=[a1])
        s.add(u2)
        s.flush()
        s._expunge_states([attributes.instance_state(u2)])
        del u2
        gc_collect()

        assert_raises_message(
            orm_exc.StaleDataError,
            "can't be sure this is the most recent parent.",
            u1.addresses.remove,
            a1,
        )

        s.flush()
        self._assert_hasparent(a1)

    def test_fresh_state_positive(self):
        s, u1, a1 = self._fixture()

        self._assert_hasparent(a1)

    def test_fresh_state_negative(self):
        s, u1, a1 = self._fixture()

        u1.addresses.remove(a1)

        self._assert_not_hasparent(a1)
