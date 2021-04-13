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
