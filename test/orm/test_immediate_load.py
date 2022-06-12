"""basic tests of lazy loaded attributes"""

import sqlalchemy as sa
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.orm import immediateload
from sqlalchemy.orm import relationship
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
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

        self.mapper_registry.map_imperatively(Address, addresses)
        self.mapper_registry.map_imperatively(
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

    def test_no_auto_recurse_non_self_referential(self):
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
                    order_by=Address.id,
                )
            },
        )
        sess = fixture_session()

        stmt = select(User).options(
            immediateload(User.addresses, auto_recurse=True)
        )
        with expect_raises_message(
            sa.exc.InvalidRequestError,
            "auto_recurse option on relationship User.addresses not valid",
        ):
            sess.execute(stmt).all()

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

        self.mapper_registry.map_imperatively(
            Address,
            addresses,
            properties={"user": relationship(User, lazy=default_lazy)},
        )
        self.mapper_registry.map_imperatively(User, users)
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

        self.mapper_registry.map_imperatively(Address, addresses)
        self.mapper_registry.map_imperatively(
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

        self.mapper_registry.map_imperatively(
            Address,
            addresses,
            properties={
                "user": relationship(
                    User, lazy="immediate", back_populates="addresses"
                )
            },
        )
        self.mapper_registry.map_imperatively(
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

        self.mapper_registry.map_imperatively(
            Address,
            addresses,
            properties={
                "user": relationship(
                    User, lazy=m2o_lazy, back_populates="addresses"
                )
            },
        )
        self.mapper_registry.map_imperatively(
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


class SelfReferentialTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "nodes",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("parent_id", Integer, ForeignKey("nodes.id")),
            Column("data", String(30)),
        )

    @classmethod
    def setup_classes(cls):
        class Node(cls.Comparable):
            def append(self, node):
                self.children.append(node)

    @testing.fixture
    def data_fixture(self):
        def go(sess):
            Node = self.classes.Node
            n1 = Node(data="n1")
            n1.append(Node(data="n11"))
            n1.append(Node(data="n12"))
            n1.append(Node(data="n13"))

            n1.children[0].children = [Node(data="n111"), Node(data="n112")]

            n1.children[1].append(Node(data="n121"))
            n1.children[1].append(Node(data="n122"))
            n1.children[1].append(Node(data="n123"))
            n2 = Node(data="n2")
            n2.append(Node(data="n21"))
            n2.children[0].append(Node(data="n211"))
            n2.children[0].append(Node(data="n212"))
            sess.add(n1)
            sess.add(n2)
            sess.flush()
            sess.expunge_all()
            return n1, n2

        return go

    def _full_structure(self):
        Node = self.classes.Node
        return [
            Node(
                data="n1",
                children=[
                    Node(data="n11"),
                    Node(
                        data="n12",
                        children=[
                            Node(data="n121"),
                            Node(data="n122"),
                            Node(data="n123"),
                        ],
                    ),
                    Node(data="n13"),
                ],
            ),
            Node(
                data="n2",
                children=[
                    Node(
                        data="n21",
                        children=[
                            Node(data="n211"),
                            Node(data="n212"),
                        ],
                    )
                ],
            ),
        ]

    def test_auto_recurse_opt(self, data_fixture):
        nodes = self.tables.nodes
        Node = self.classes.Node

        self.mapper_registry.map_imperatively(
            Node,
            nodes,
            properties={"children": relationship(Node)},
        )
        sess = fixture_session()
        n1, n2 = data_fixture(sess)

        def go():
            return (
                sess.query(Node)
                .filter(Node.data.in_(["n1", "n2"]))
                .options(immediateload(Node.children, auto_recurse=True))
                .order_by(Node.data)
                .all()
            )

        result = self.assert_sql_count(testing.db, go, 14)
        sess.close()

        eq_(result, self._full_structure())
