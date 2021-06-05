"""basic tests of lazy loaded attributes"""

from sqlalchemy import testing
from sqlalchemy.orm import immediateload
from sqlalchemy.orm import mapper
from sqlalchemy.orm import relationship
from sqlalchemy.testing import eq_
from sqlalchemy.testing.fixtures import fixture_session
from test.orm import _fixtures


class ImmediateTest(_fixtures.FixtureTest):
    run_inserts = "once"
    run_deletes = None

    @testing.combinations(
        ("raise",),
        ("raise_on_sql",),
        ("select",),
        ("immediate"),
    )
    def test_basic_option(self, default_lazy):
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
            properties={"addresses": relationship(Address, lazy=default_lazy)},
        )
        sess = fixture_session()

        result = (
            sess.query(User)
            .options(immediateload(User.addresses))
            .filter(users.c.id == 7)
            .all()
        )
        eq_(len(sess.identity_map), 2)

        sess.close()

        eq_(
            [
                User(
                    id=7,
                    addresses=[Address(id=1, email_address="jack@bean.com")],
                )
            ],
            result,
        )

    @testing.combinations(
        ("raise",),
        ("raise_on_sql",),
        ("select",),
        ("immediate"),
    )
    def test_basic_option_m2o(self, default_lazy):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        mapper(
            Address,
            addresses,
            properties={"user": relationship(User, lazy=default_lazy)},
        )
        mapper(User, users)
        sess = fixture_session()

        result = (
            sess.query(Address)
            .options(immediateload(Address.user))
            .filter(Address.id == 1)
            .all()
        )
        eq_(len(sess.identity_map), 2)

        sess.close()

        eq_(
            [Address(id=1, email_address="jack@bean.com", user=User(id=7))],
            result,
        )

    def test_basic(self):
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
            properties={"addresses": relationship(Address, lazy="immediate")},
        )
        sess = fixture_session()

        result = sess.query(User).filter(users.c.id == 7).all()
        eq_(len(sess.identity_map), 2)
        sess.close()

        eq_(
            [
                User(
                    id=7,
                    addresses=[Address(id=1, email_address="jack@bean.com")],
                )
            ],
            result,
        )

    @testing.combinations(
        ("joined",),
        ("selectin",),
        ("subquery",),
    )
    def test_m2one_side(self, o2m_lazy):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        mapper(
            Address,
            addresses,
            properties={
                "user": relationship(
                    User, lazy="immediate", back_populates="addresses"
                )
            },
        )
        mapper(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address, lazy=o2m_lazy, back_populates="user"
                )
            },
        )
        sess = fixture_session()
        u1 = sess.query(User).filter(users.c.id == 7).one()
        sess.close()

        assert "addresses" in u1.__dict__
        assert "user" in u1.addresses[0].__dict__

    @testing.combinations(
        ("immediate",),
        ("joined",),
        ("selectin",),
        ("subquery",),
    )
    def test_o2mone_side(self, m2o_lazy):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        mapper(
            Address,
            addresses,
            properties={
                "user": relationship(
                    User, lazy=m2o_lazy, back_populates="addresses"
                )
            },
        )
        mapper(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address, lazy="immediate", back_populates="user"
                )
            },
        )
        sess = fixture_session()
        u1 = sess.query(User).filter(users.c.id == 7).one()
        sess.close()

        assert "addresses" in u1.__dict__

        # current behavior of "immediate" is that subsequent eager loaders
        # aren't fired off.  This is because the "lazyload" strategy
        # does not invoke eager loaders.
        assert "user" not in u1.addresses[0].__dict__
