from sqlalchemy import cast
from sqlalchemy import DateTime
from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy import FetchedValue
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Identity
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import JSON
from sqlalchemy import literal
from sqlalchemy import select
from sqlalchemy import Sequence
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import util
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import attributes
from sqlalchemy.orm import backref
from sqlalchemy.orm import clear_mappers
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy.orm import mapper
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm import unitofwork
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import config
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing.assertsql import AllOf
from sqlalchemy.testing.assertsql import CompiledSQL
from sqlalchemy.testing.assertsql import Conditional
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.mock import Mock
from sqlalchemy.testing.mock import patch
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from test.orm import _fixtures


class AssertsUOW(object):
    def _get_test_uow(self, session):
        uow = unitofwork.UOWTransaction(session)
        deleted = set(session._deleted)
        new = set(session._new)
        dirty = set(session._dirty_states).difference(deleted)
        for s in new.union(dirty):
            uow.register_object(s)
        for d in deleted:
            uow.register_object(d, isdelete=True)
        return uow

    def _assert_uow_size(self, session, expected):
        uow = self._get_test_uow(session)
        postsort_actions = uow._generate_actions()
        print(postsort_actions)
        eq_(len(postsort_actions), expected, postsort_actions)


class UOWTest(
    _fixtures.FixtureTest, testing.AssertsExecutionResults, AssertsUOW
):
    run_inserts = None


class RudimentaryFlushTest(UOWTest):
    def test_one_to_many_save(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(User, users, properties={"addresses": relationship(Address)})
        mapper(Address, addresses)
        sess = fixture_session()

        a1, a2 = Address(email_address="a1"), Address(email_address="a2")
        u1 = User(name="u1", addresses=[a1, a2])
        sess.add(u1)

        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL(
                "INSERT INTO users (name) VALUES (:name)", {"name": "u1"}
            ),
            Conditional(
                testing.db.dialect.insert_executemany_returning,
                [
                    CompiledSQL(
                        "INSERT INTO addresses (user_id, email_address) "
                        "VALUES (:user_id, :email_address)",
                        lambda ctx: [
                            {"email_address": "a1", "user_id": u1.id},
                            {"email_address": "a2", "user_id": u1.id},
                        ],
                    ),
                ],
                [
                    CompiledSQL(
                        "INSERT INTO addresses (user_id, email_address) "
                        "VALUES (:user_id, :email_address)",
                        lambda ctx: {"email_address": "a1", "user_id": u1.id},
                    ),
                    CompiledSQL(
                        "INSERT INTO addresses (user_id, email_address) "
                        "VALUES (:user_id, :email_address)",
                        lambda ctx: {"email_address": "a2", "user_id": u1.id},
                    ),
                ],
            ),
        )

    def test_one_to_many_delete_all(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(User, users, properties={"addresses": relationship(Address)})
        mapper(Address, addresses)
        sess = fixture_session()
        a1, a2 = Address(email_address="a1"), Address(email_address="a2")
        u1 = User(name="u1", addresses=[a1, a2])
        sess.add(u1)
        sess.flush()

        sess.delete(u1)
        sess.delete(a1)
        sess.delete(a2)
        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL(
                "DELETE FROM addresses WHERE addresses.id = :id",
                [{"id": a1.id}, {"id": a2.id}],
            ),
            CompiledSQL(
                "DELETE FROM users WHERE users.id = :id", {"id": u1.id}
            ),
        )

    def test_one_to_many_delete_parent(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(User, users, properties={"addresses": relationship(Address)})
        mapper(Address, addresses)
        sess = fixture_session()
        a1, a2 = Address(email_address="a1"), Address(email_address="a2")
        u1 = User(name="u1", addresses=[a1, a2])
        sess.add(u1)
        sess.flush()

        sess.delete(u1)
        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL(
                "UPDATE addresses SET user_id=:user_id WHERE "
                "addresses.id = :addresses_id",
                lambda ctx: [
                    {"addresses_id": a1.id, "user_id": None},
                    {"addresses_id": a2.id, "user_id": None},
                ],
            ),
            CompiledSQL(
                "DELETE FROM users WHERE users.id = :id", {"id": u1.id}
            ),
        )

    def test_many_to_one_save(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(User, users)
        mapper(Address, addresses, properties={"user": relationship(User)})
        sess = fixture_session()

        u1 = User(name="u1")
        a1, a2 = (
            Address(email_address="a1", user=u1),
            Address(email_address="a2", user=u1),
        )
        sess.add_all([a1, a2])

        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL(
                "INSERT INTO users (name) VALUES (:name)", {"name": "u1"}
            ),
            Conditional(
                testing.db.dialect.insert_executemany_returning,
                [
                    CompiledSQL(
                        "INSERT INTO addresses (user_id, email_address) "
                        "VALUES (:user_id, :email_address)",
                        lambda ctx: [
                            {"email_address": "a1", "user_id": u1.id},
                            {"email_address": "a2", "user_id": u1.id},
                        ],
                    ),
                ],
                [
                    CompiledSQL(
                        "INSERT INTO addresses (user_id, email_address) "
                        "VALUES (:user_id, :email_address)",
                        lambda ctx: {"email_address": "a1", "user_id": u1.id},
                    ),
                    CompiledSQL(
                        "INSERT INTO addresses (user_id, email_address) "
                        "VALUES (:user_id, :email_address)",
                        lambda ctx: {"email_address": "a2", "user_id": u1.id},
                    ),
                ],
            ),
        )

    def test_many_to_one_delete_all(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(User, users)
        mapper(Address, addresses, properties={"user": relationship(User)})
        sess = fixture_session()

        u1 = User(name="u1")
        a1, a2 = (
            Address(email_address="a1", user=u1),
            Address(email_address="a2", user=u1),
        )
        sess.add_all([a1, a2])
        sess.flush()

        sess.delete(u1)
        sess.delete(a1)
        sess.delete(a2)
        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL(
                "DELETE FROM addresses WHERE addresses.id = :id",
                [{"id": a1.id}, {"id": a2.id}],
            ),
            CompiledSQL(
                "DELETE FROM users WHERE users.id = :id", {"id": u1.id}
            ),
        )

    def test_many_to_one_delete_target(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(User, users)
        mapper(Address, addresses, properties={"user": relationship(User)})
        sess = fixture_session()

        u1 = User(name="u1")
        a1, a2 = (
            Address(email_address="a1", user=u1),
            Address(email_address="a2", user=u1),
        )
        sess.add_all([a1, a2])
        sess.flush()

        sess.delete(u1)
        a1.user = a2.user = None
        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL(
                "UPDATE addresses SET user_id=:user_id WHERE "
                "addresses.id = :addresses_id",
                lambda ctx: [
                    {"addresses_id": a1.id, "user_id": None},
                    {"addresses_id": a2.id, "user_id": None},
                ],
            ),
            CompiledSQL(
                "DELETE FROM users WHERE users.id = :id", {"id": u1.id}
            ),
        )

    def test_many_to_one_delete_unloaded(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(User, users)
        mapper(Address, addresses, properties={"parent": relationship(User)})

        parent = User(name="p1")
        c1, c2 = (
            Address(email_address="c1", parent=parent),
            Address(email_address="c2", parent=parent),
        )

        session = fixture_session()
        session.add_all([c1, c2])
        session.add(parent)

        session.flush()

        pid = parent.id
        c1id = c1.id
        c2id = c2.id

        session.expire(parent)
        session.expire(c1)
        session.expire(c2)

        session.delete(c1)
        session.delete(c2)
        session.delete(parent)

        # testing that relationships
        # are loaded even if all ids/references are
        # expired
        self.assert_sql_execution(
            testing.db,
            session.flush,
            AllOf(
                # [ticket:2002] - ensure the m2os are loaded.
                # the selects here are in fact unexpiring
                # each row - the m2o comes from the identity map.
                # the User row might be handled before or the addresses
                # are loaded so need to use AllOf
                CompiledSQL(
                    "SELECT addresses.id AS addresses_id, "
                    "addresses.user_id AS "
                    "addresses_user_id, addresses.email_address AS "
                    "addresses_email_address FROM addresses "
                    "WHERE addresses.id = "
                    ":pk_1",
                    lambda ctx: {"pk_1": c1id},
                ),
                CompiledSQL(
                    "SELECT addresses.id AS addresses_id, "
                    "addresses.user_id AS "
                    "addresses_user_id, addresses.email_address AS "
                    "addresses_email_address FROM addresses "
                    "WHERE addresses.id = "
                    ":pk_1",
                    lambda ctx: {"pk_1": c2id},
                ),
                CompiledSQL(
                    "SELECT users.id AS users_id, users.name AS users_name "
                    "FROM users WHERE users.id = :pk_1",
                    lambda ctx: {"pk_1": pid},
                ),
                CompiledSQL(
                    "DELETE FROM addresses WHERE addresses.id = :id",
                    lambda ctx: [{"id": c1id}, {"id": c2id}],
                ),
                CompiledSQL(
                    "DELETE FROM users WHERE users.id = :id",
                    lambda ctx: {"id": pid},
                ),
            ),
        )

    def test_many_to_one_delete_childonly_unloaded(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(User, users)
        mapper(Address, addresses, properties={"parent": relationship(User)})

        parent = User(name="p1")
        c1, c2 = (
            Address(email_address="c1", parent=parent),
            Address(email_address="c2", parent=parent),
        )

        session = fixture_session()
        session.add_all([c1, c2])
        session.add(parent)

        session.flush()

        # pid = parent.id
        c1id = c1.id
        c2id = c2.id

        session.expire(c1)
        session.expire(c2)

        session.delete(c1)
        session.delete(c2)

        self.assert_sql_execution(
            testing.db,
            session.flush,
            AllOf(
                # [ticket:2049] - we aren't deleting User,
                # relationship is simple m2o, no SELECT should be emitted for
                # it.
                CompiledSQL(
                    "SELECT addresses.id AS addresses_id, "
                    "addresses.user_id AS "
                    "addresses_user_id, addresses.email_address AS "
                    "addresses_email_address FROM addresses "
                    "WHERE addresses.id = "
                    ":pk_1",
                    lambda ctx: {"pk_1": c1id},
                ),
                CompiledSQL(
                    "SELECT addresses.id AS addresses_id, "
                    "addresses.user_id AS "
                    "addresses_user_id, addresses.email_address AS "
                    "addresses_email_address FROM addresses "
                    "WHERE addresses.id = "
                    ":pk_1",
                    lambda ctx: {"pk_1": c2id},
                ),
            ),
            CompiledSQL(
                "DELETE FROM addresses WHERE addresses.id = :id",
                lambda ctx: [{"id": c1id}, {"id": c2id}],
            ),
        )

    def test_many_to_one_delete_childonly_unloaded_expired(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(User, users)
        mapper(Address, addresses, properties={"parent": relationship(User)})

        parent = User(name="p1")
        c1, c2 = (
            Address(email_address="c1", parent=parent),
            Address(email_address="c2", parent=parent),
        )

        session = fixture_session()
        session.add_all([c1, c2])
        session.add(parent)

        session.flush()

        # pid = parent.id
        c1id = c1.id
        c2id = c2.id

        session.expire(parent)
        session.expire(c1)
        session.expire(c2)

        session.delete(c1)
        session.delete(c2)

        self.assert_sql_execution(
            testing.db,
            session.flush,
            AllOf(
                # the parent User is expired, so it gets loaded here.
                CompiledSQL(
                    "SELECT addresses.id AS addresses_id, "
                    "addresses.user_id AS "
                    "addresses_user_id, addresses.email_address AS "
                    "addresses_email_address FROM addresses "
                    "WHERE addresses.id = "
                    ":pk_1",
                    lambda ctx: {"pk_1": c1id},
                ),
                CompiledSQL(
                    "SELECT addresses.id AS addresses_id, "
                    "addresses.user_id AS "
                    "addresses_user_id, addresses.email_address AS "
                    "addresses_email_address FROM addresses "
                    "WHERE addresses.id = "
                    ":pk_1",
                    lambda ctx: {"pk_1": c2id},
                ),
            ),
            CompiledSQL(
                "DELETE FROM addresses WHERE addresses.id = :id",
                lambda ctx: [{"id": c1id}, {"id": c2id}],
            ),
        )

    def test_many_to_one_del_attr(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(User, users)
        mapper(Address, addresses, properties={"user": relationship(User)})
        sess = fixture_session()

        u1 = User(name="u1")
        a1, a2 = (
            Address(email_address="a1", user=u1),
            Address(email_address="a2", user=u1),
        )
        sess.add_all([a1, a2])
        sess.flush()

        del a1.user
        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL(
                "UPDATE addresses SET user_id=:user_id WHERE "
                "addresses.id = :addresses_id",
                lambda ctx: [{"addresses_id": a1.id, "user_id": None}],
            ),
        )

    def test_many_to_one_del_attr_unloaded(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(User, users)
        mapper(Address, addresses, properties={"user": relationship(User)})
        sess = fixture_session()

        u1 = User(name="u1")
        a1, a2 = (
            Address(email_address="a1", user=u1),
            Address(email_address="a2", user=u1),
        )
        sess.add_all([a1, a2])
        sess.flush()

        # trying to guarantee that the history only includes
        # PASSIVE_NO_RESULT for "deleted" and nothing else
        sess.expunge(u1)
        sess.expire(a1, ["user"])
        del a1.user

        sess.add(a1)
        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL(
                "UPDATE addresses SET user_id=:user_id WHERE "
                "addresses.id = :addresses_id",
                lambda ctx: [{"addresses_id": a1.id, "user_id": None}],
            ),
        )

    def test_natural_ordering(self):
        """test that unconnected items take relationship()
        into account regardless."""

        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(User, users)
        mapper(Address, addresses, properties={"parent": relationship(User)})

        sess = fixture_session()

        u1 = User(id=1, name="u1")
        a1 = Address(id=1, user_id=1, email_address="a2")

        sess.add_all([u1, a1])
        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL(
                "INSERT INTO users (id, name) VALUES (:id, :name)",
                {"id": 1, "name": "u1"},
            ),
            CompiledSQL(
                "INSERT INTO addresses (id, user_id, email_address) "
                "VALUES (:id, :user_id, :email_address)",
                {"email_address": "a2", "user_id": 1, "id": 1},
            ),
        )

        sess.delete(u1)
        sess.delete(a1)
        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL(
                "DELETE FROM addresses WHERE addresses.id = :id", [{"id": 1}]
            ),
            CompiledSQL("DELETE FROM users WHERE users.id = :id", [{"id": 1}]),
        )

    def test_natural_selfref(self):
        """test that unconnected items take relationship()
        into account regardless."""

        Node, nodes = self.classes.Node, self.tables.nodes

        mapper(Node, nodes, properties={"children": relationship(Node)})

        sess = fixture_session()

        n1 = Node(id=1)
        n2 = Node(id=2, parent_id=1)
        n3 = Node(id=3, parent_id=2)

        # insert order is determined from add order since they
        # are the same class
        sess.add_all([n1, n2, n3])

        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL(
                "INSERT INTO nodes (id, parent_id, data) VALUES "
                "(:id, :parent_id, :data)",
                [
                    {"parent_id": None, "data": None, "id": 1},
                    {"parent_id": 1, "data": None, "id": 2},
                    {"parent_id": 2, "data": None, "id": 3},
                ],
            ),
        )

    def test_many_to_many(self):
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
                "keywords": relationship(Keyword, secondary=item_keywords)
            },
        )
        mapper(Keyword, keywords)

        sess = fixture_session()
        k1 = Keyword(name="k1")
        i1 = Item(description="i1", keywords=[k1])
        sess.add(i1)
        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL(
                "INSERT INTO items (description) VALUES (:description)",
                {"description": "i1"},
            ),
            CompiledSQL(
                "INSERT INTO keywords (name) VALUES (:name)",
                {"name": "k1"},
            ),
            CompiledSQL(
                "INSERT INTO item_keywords (item_id, keyword_id) "
                "VALUES (:item_id, :keyword_id)",
                lambda ctx: {"item_id": i1.id, "keyword_id": k1.id},
            ),
        )

        # test that keywords collection isn't loaded
        sess.expire(i1, ["keywords"])
        i1.description = "i2"
        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL(
                "UPDATE items SET description=:description "
                "WHERE items.id = :items_id",
                lambda ctx: {"description": "i2", "items_id": i1.id},
            ),
        )

    def test_m2o_flush_size(self):
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
            properties={"user": relationship(User, passive_updates=True)},
        )
        sess = fixture_session()
        u1 = User(name="ed")
        sess.add(u1)
        self._assert_uow_size(sess, 2)

    def test_o2m_flush_size(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(User, users, properties={"addresses": relationship(Address)})
        mapper(Address, addresses)

        with fixture_session(autoflush=False) as sess:
            u1 = User(name="ed")
            sess.add(u1)
            self._assert_uow_size(sess, 2)

            sess.flush()

            u1.name = "jack"

            self._assert_uow_size(sess, 2)
            sess.flush()

            a1 = Address(email_address="foo")
            sess.add(a1)
            sess.flush()

            u1.addresses.append(a1)

            self._assert_uow_size(sess, 6)

            sess.commit()

        with fixture_session(autoflush=False) as sess:
            u1 = sess.query(User).first()
            u1.name = "ed"
            self._assert_uow_size(sess, 2)

            u1.addresses
            self._assert_uow_size(sess, 6)


class RaiseLoadIgnoredTest(
    fixtures.DeclarativeMappedTest,
    testing.AssertsExecutionResults,
):
    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class A(Base):
            __tablename__ = "a"

            id = Column(Integer, primary_key=True)

            bs = relationship("B", back_populates="user", lazy="raise")

        class B(Base):
            __tablename__ = "b"
            id = Column(Integer, primary_key=True)
            a_id = Column(ForeignKey("a.id"))
            user = relationship("A", back_populates="bs", lazy="raise")

    def test_delete_head(self):
        A, B = self.classes("A", "B")

        sess = fixture_session()

        sess.add(A(bs=[B(), B()]))
        sess.commit()

        a1 = sess.execute(select(A)).scalars().first()

        sess.delete(a1)

        self.assert_sql_execution(
            testing.db,
            sess.flush,
            # for the flush process, lazy="raise" is ignored
            CompiledSQL(
                "SELECT b.id AS b_id, b.a_id AS b_a_id FROM b "
                "WHERE :param_1 = b.a_id",
                [{"param_1": 1}],
            ),
            CompiledSQL(
                "UPDATE b SET a_id=:a_id WHERE b.id = :b_id",
                [{"a_id": None, "b_id": 1}, {"a_id": None, "b_id": 2}],
            ),
            CompiledSQL("DELETE FROM a WHERE a.id = :id", [{"id": 1}]),
        )


class SingleCycleTest(UOWTest):
    def teardown_test(self):
        engines.testing_reaper.rollback_all()
        # mysql can't handle delete from nodes
        # since it doesn't deal with the FKs correctly,
        # so wipe out the parent_id first
        with testing.db.begin() as conn:
            conn.execute(self.tables.nodes.update().values(parent_id=None))

    def test_one_to_many_save(self):
        Node, nodes = self.classes.Node, self.tables.nodes

        mapper(Node, nodes, properties={"children": relationship(Node)})
        sess = fixture_session()

        n2, n3 = Node(data="n2"), Node(data="n3")
        n1 = Node(data="n1", children=[n2, n3])

        sess.add(n1)

        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL(
                "INSERT INTO nodes (parent_id, data) VALUES "
                "(:parent_id, :data)",
                {"parent_id": None, "data": "n1"},
            ),
            Conditional(
                testing.db.dialect.insert_executemany_returning,
                [
                    CompiledSQL(
                        "INSERT INTO nodes (parent_id, data) VALUES "
                        "(:parent_id, :data)",
                        lambda ctx: [
                            {"parent_id": n1.id, "data": "n2"},
                            {"parent_id": n1.id, "data": "n3"},
                        ],
                    ),
                ],
                [
                    AllOf(
                        CompiledSQL(
                            "INSERT INTO nodes (parent_id, data) VALUES "
                            "(:parent_id, :data)",
                            lambda ctx: {"parent_id": n1.id, "data": "n2"},
                        ),
                        CompiledSQL(
                            "INSERT INTO nodes (parent_id, data) VALUES "
                            "(:parent_id, :data)",
                            lambda ctx: {"parent_id": n1.id, "data": "n3"},
                        ),
                    ),
                ],
            ),
        )

    def test_one_to_many_delete_all(self):
        Node, nodes = self.classes.Node, self.tables.nodes

        mapper(Node, nodes, properties={"children": relationship(Node)})
        sess = fixture_session()

        n2, n3 = Node(data="n2", children=[]), Node(data="n3", children=[])
        n1 = Node(data="n1", children=[n2, n3])

        sess.add(n1)
        sess.flush()

        sess.delete(n1)
        sess.delete(n2)
        sess.delete(n3)
        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL(
                "DELETE FROM nodes WHERE nodes.id = :id",
                lambda ctx: [{"id": n2.id}, {"id": n3.id}],
            ),
            CompiledSQL(
                "DELETE FROM nodes WHERE nodes.id = :id",
                lambda ctx: {"id": n1.id},
            ),
        )

    def test_one_to_many_delete_parent(self):
        Node, nodes = self.classes.Node, self.tables.nodes

        mapper(Node, nodes, properties={"children": relationship(Node)})
        sess = fixture_session()

        n2, n3 = Node(data="n2", children=[]), Node(data="n3", children=[])
        n1 = Node(data="n1", children=[n2, n3])

        sess.add(n1)
        sess.flush()

        sess.delete(n1)
        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL(
                "UPDATE nodes SET parent_id=:parent_id "
                "WHERE nodes.id = :nodes_id",
                lambda ctx: [
                    {"nodes_id": n3.id, "parent_id": None},
                    {"nodes_id": n2.id, "parent_id": None},
                ],
            ),
            CompiledSQL(
                "DELETE FROM nodes WHERE nodes.id = :id",
                lambda ctx: {"id": n1.id},
            ),
        )

    def test_many_to_one_save(self):
        Node, nodes = self.classes.Node, self.tables.nodes

        mapper(
            Node,
            nodes,
            properties={"parent": relationship(Node, remote_side=nodes.c.id)},
        )
        sess = fixture_session()

        n1 = Node(data="n1")
        n2, n3 = Node(data="n2", parent=n1), Node(data="n3", parent=n1)

        sess.add_all([n2, n3])

        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL(
                "INSERT INTO nodes (parent_id, data) VALUES "
                "(:parent_id, :data)",
                {"parent_id": None, "data": "n1"},
            ),
            Conditional(
                testing.db.dialect.insert_executemany_returning,
                [
                    CompiledSQL(
                        "INSERT INTO nodes (parent_id, data) VALUES "
                        "(:parent_id, :data)",
                        lambda ctx: [
                            {"parent_id": n1.id, "data": "n2"},
                            {"parent_id": n1.id, "data": "n3"},
                        ],
                    ),
                ],
                [
                    AllOf(
                        CompiledSQL(
                            "INSERT INTO nodes (parent_id, data) VALUES "
                            "(:parent_id, :data)",
                            lambda ctx: {"parent_id": n1.id, "data": "n2"},
                        ),
                        CompiledSQL(
                            "INSERT INTO nodes (parent_id, data) VALUES "
                            "(:parent_id, :data)",
                            lambda ctx: {"parent_id": n1.id, "data": "n3"},
                        ),
                    ),
                ],
            ),
        )

    def test_many_to_one_delete_all(self):
        Node, nodes = self.classes.Node, self.tables.nodes

        mapper(
            Node,
            nodes,
            properties={"parent": relationship(Node, remote_side=nodes.c.id)},
        )
        sess = fixture_session()

        n1 = Node(data="n1")
        n2, n3 = Node(data="n2", parent=n1), Node(data="n3", parent=n1)

        sess.add_all([n2, n3])
        sess.flush()

        sess.delete(n1)
        sess.delete(n2)
        sess.delete(n3)
        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL(
                "DELETE FROM nodes WHERE nodes.id = :id",
                lambda ctx: [{"id": n2.id}, {"id": n3.id}],
            ),
            CompiledSQL(
                "DELETE FROM nodes WHERE nodes.id = :id",
                lambda ctx: {"id": n1.id},
            ),
        )

    def test_many_to_one_set_null_unloaded(self):
        Node, nodes = self.classes.Node, self.tables.nodes

        mapper(
            Node,
            nodes,
            properties={"parent": relationship(Node, remote_side=nodes.c.id)},
        )
        with fixture_session() as sess:
            n1 = Node(data="n1")
            n2 = Node(data="n2", parent=n1)
            sess.add_all([n1, n2])
            sess.commit()

        with fixture_session() as sess:
            n2 = sess.query(Node).filter_by(data="n2").one()
            n2.parent = None
            self.assert_sql_execution(
                testing.db,
                sess.flush,
                CompiledSQL(
                    "UPDATE nodes SET parent_id=:parent_id WHERE "
                    "nodes.id = :nodes_id",
                    lambda ctx: {"parent_id": None, "nodes_id": n2.id},
                ),
            )

    def test_cycle_rowswitch(self):
        Node, nodes = self.classes.Node, self.tables.nodes

        mapper(Node, nodes, properties={"children": relationship(Node)})
        sess = fixture_session()

        n2, n3 = Node(data="n2", children=[]), Node(data="n3", children=[])
        n1 = Node(data="n1", children=[n2])

        sess.add(n1)
        sess.flush()
        sess.delete(n2)
        n3.id = n2.id
        n1.children.append(n3)
        sess.flush()

    def test_bidirectional_mutations_one(self):
        Node, nodes = self.classes.Node, self.tables.nodes

        mapper(
            Node,
            nodes,
            properties={
                "children": relationship(
                    Node, backref=backref("parent", remote_side=nodes.c.id)
                )
            },
        )
        sess = fixture_session()

        n2, n3 = Node(data="n2", children=[]), Node(data="n3", children=[])
        n1 = Node(data="n1", children=[n2])
        sess.add(n1)
        sess.flush()
        sess.delete(n2)
        n1.children.append(n3)
        sess.flush()

        sess.delete(n1)
        sess.delete(n3)
        sess.flush()

    def test_bidirectional_multilevel_save(self):
        Node, nodes = self.classes.Node, self.tables.nodes

        mapper(
            Node,
            nodes,
            properties={
                "children": relationship(
                    Node, backref=backref("parent", remote_side=nodes.c.id)
                )
            },
        )
        sess = fixture_session()
        n1 = Node(data="n1")
        n1.children.append(Node(data="n11"))
        n12 = Node(data="n12")
        n1.children.append(n12)
        n1.children.append(Node(data="n13"))
        n1.children[1].children.append(Node(data="n121"))
        n1.children[1].children.append(Node(data="n122"))
        n1.children[1].children.append(Node(data="n123"))
        sess.add(n1)
        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL(
                "INSERT INTO nodes (parent_id, data) VALUES "
                "(:parent_id, :data)",
                lambda ctx: {"parent_id": None, "data": "n1"},
            ),
            Conditional(
                testing.db.dialect.insert_executemany_returning,
                [
                    CompiledSQL(
                        "INSERT INTO nodes (parent_id, data) VALUES "
                        "(:parent_id, :data)",
                        lambda ctx: [
                            {"parent_id": n1.id, "data": "n11"},
                            {"parent_id": n1.id, "data": "n12"},
                            {"parent_id": n1.id, "data": "n13"},
                        ],
                    ),
                ],
                [
                    CompiledSQL(
                        "INSERT INTO nodes (parent_id, data) VALUES "
                        "(:parent_id, :data)",
                        lambda ctx: {"parent_id": n1.id, "data": "n11"},
                    ),
                    CompiledSQL(
                        "INSERT INTO nodes (parent_id, data) VALUES "
                        "(:parent_id, :data)",
                        lambda ctx: {"parent_id": n1.id, "data": "n12"},
                    ),
                    CompiledSQL(
                        "INSERT INTO nodes (parent_id, data) VALUES "
                        "(:parent_id, :data)",
                        lambda ctx: {"parent_id": n1.id, "data": "n13"},
                    ),
                ],
            ),
            Conditional(
                testing.db.dialect.insert_executemany_returning,
                [
                    CompiledSQL(
                        "INSERT INTO nodes (parent_id, data) VALUES "
                        "(:parent_id, :data)",
                        lambda ctx: [
                            {"parent_id": n12.id, "data": "n121"},
                            {"parent_id": n12.id, "data": "n122"},
                            {"parent_id": n12.id, "data": "n123"},
                        ],
                    ),
                ],
                [
                    CompiledSQL(
                        "INSERT INTO nodes (parent_id, data) VALUES "
                        "(:parent_id, :data)",
                        lambda ctx: {"parent_id": n12.id, "data": "n121"},
                    ),
                    CompiledSQL(
                        "INSERT INTO nodes (parent_id, data) VALUES "
                        "(:parent_id, :data)",
                        lambda ctx: {"parent_id": n12.id, "data": "n122"},
                    ),
                    CompiledSQL(
                        "INSERT INTO nodes (parent_id, data) VALUES "
                        "(:parent_id, :data)",
                        lambda ctx: {"parent_id": n12.id, "data": "n123"},
                    ),
                ],
            ),
        )

    def test_singlecycle_flush_size(self):
        Node, nodes = self.classes.Node, self.tables.nodes

        mapper(Node, nodes, properties={"children": relationship(Node)})
        with fixture_session() as sess:
            n1 = Node(data="ed")
            sess.add(n1)
            self._assert_uow_size(sess, 2)

            sess.flush()

            n1.data = "jack"

            self._assert_uow_size(sess, 2)
            sess.flush()

            n2 = Node(data="foo")
            sess.add(n2)
            sess.flush()

            n1.children.append(n2)

            self._assert_uow_size(sess, 3)

            sess.commit()

        sess = fixture_session(autoflush=False)
        n1 = sess.query(Node).first()
        n1.data = "ed"
        self._assert_uow_size(sess, 2)

        n1.children
        self._assert_uow_size(sess, 2)

    def test_delete_unloaded_m2o(self):
        Node, nodes = self.classes.Node, self.tables.nodes

        mapper(
            Node,
            nodes,
            properties={"parent": relationship(Node, remote_side=nodes.c.id)},
        )

        parent = Node()
        c1, c2 = Node(parent=parent), Node(parent=parent)

        session = fixture_session()
        session.add_all([c1, c2])
        session.add(parent)

        session.flush()

        pid = parent.id
        c1id = c1.id
        c2id = c2.id

        session.expire(parent)
        session.expire(c1)
        session.expire(c2)

        session.delete(c1)
        session.delete(c2)
        session.delete(parent)

        # testing that relationships
        # are loaded even if all ids/references are
        # expired
        self.assert_sql_execution(
            testing.db,
            session.flush,
            AllOf(
                # ensure all three m2os are loaded.
                # the selects here are in fact unexpiring
                # each row - the m2o comes from the identity map.
                CompiledSQL(
                    "SELECT nodes.id AS nodes_id, nodes.parent_id AS "
                    "nodes_parent_id, "
                    "nodes.data AS nodes_data FROM nodes "
                    "WHERE nodes.id = :pk_1",
                    lambda ctx: {"pk_1": pid},
                ),
                CompiledSQL(
                    "SELECT nodes.id AS nodes_id, nodes.parent_id AS "
                    "nodes_parent_id, "
                    "nodes.data AS nodes_data FROM nodes "
                    "WHERE nodes.id = :pk_1",
                    lambda ctx: {"pk_1": c1id},
                ),
                CompiledSQL(
                    "SELECT nodes.id AS nodes_id, nodes.parent_id AS "
                    "nodes_parent_id, "
                    "nodes.data AS nodes_data FROM nodes "
                    "WHERE nodes.id = :pk_1",
                    lambda ctx: {"pk_1": c2id},
                ),
                AllOf(
                    CompiledSQL(
                        "DELETE FROM nodes WHERE nodes.id = :id",
                        lambda ctx: [{"id": c1id}, {"id": c2id}],
                    ),
                    CompiledSQL(
                        "DELETE FROM nodes WHERE nodes.id = :id",
                        lambda ctx: {"id": pid},
                    ),
                ),
            ),
        )


class SingleCyclePlusAttributeTest(
    fixtures.MappedTest, testing.AssertsExecutionResults, AssertsUOW
):
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

        Table(
            "foobars",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("parent_id", Integer, ForeignKey("nodes.id")),
        )

    def test_flush_size(self):
        foobars, nodes = self.tables.foobars, self.tables.nodes

        class Node(fixtures.ComparableEntity):
            pass

        class FooBar(fixtures.ComparableEntity):
            pass

        mapper(
            Node,
            nodes,
            properties={
                "children": relationship(Node),
                "foobars": relationship(FooBar),
            },
        )
        mapper(FooBar, foobars)

        sess = fixture_session()
        n1 = Node(data="n1")
        n2 = Node(data="n2")
        n1.children.append(n2)
        sess.add(n1)
        # ensure "foobars" doesn't get yanked in here
        self._assert_uow_size(sess, 3)

        n1.foobars.append(FooBar())
        # saveupdateall/deleteall for FooBar added here,
        # plus processstate node.foobars
        # currently the "all" procs stay in pairs
        self._assert_uow_size(sess, 6)

        sess.flush()


class SingleCycleM2MTest(
    fixtures.MappedTest, testing.AssertsExecutionResults, AssertsUOW
):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "nodes",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(30)),
            Column("favorite_node_id", Integer, ForeignKey("nodes.id")),
        )

        Table(
            "node_to_nodes",
            metadata,
            Column(
                "left_node_id",
                Integer,
                ForeignKey("nodes.id"),
                primary_key=True,
            ),
            Column(
                "right_node_id",
                Integer,
                ForeignKey("nodes.id"),
                primary_key=True,
            ),
        )

    def test_many_to_many_one(self):
        nodes, node_to_nodes = self.tables.nodes, self.tables.node_to_nodes

        class Node(fixtures.ComparableEntity):
            pass

        mapper(
            Node,
            nodes,
            properties={
                "children": relationship(
                    Node,
                    secondary=node_to_nodes,
                    primaryjoin=nodes.c.id == node_to_nodes.c.left_node_id,
                    secondaryjoin=nodes.c.id == node_to_nodes.c.right_node_id,
                    backref="parents",
                ),
                "favorite": relationship(Node, remote_side=nodes.c.id),
            },
        )

        with fixture_session(autoflush=False) as sess:
            n1 = Node(data="n1")
            n2 = Node(data="n2")
            n3 = Node(data="n3")
            n4 = Node(data="n4")
            n5 = Node(data="n5")

            n4.favorite = n3
            n1.favorite = n5
            n5.favorite = n2

            n1.children = [n2, n3, n4]
            n2.children = [n3, n5]
            n3.children = [n5, n4]

            sess.add_all([n1, n2, n3, n4, n5])

            # can't really assert the SQL on this easily
            # since there's too many ways to insert the rows.
            # so check the end result
            sess.flush()
            eq_(
                sess.query(
                    node_to_nodes.c.left_node_id, node_to_nodes.c.right_node_id
                )
                .order_by(
                    node_to_nodes.c.left_node_id, node_to_nodes.c.right_node_id
                )
                .all(),
                sorted(
                    [
                        (n1.id, n2.id),
                        (n1.id, n3.id),
                        (n1.id, n4.id),
                        (n2.id, n3.id),
                        (n2.id, n5.id),
                        (n3.id, n5.id),
                        (n3.id, n4.id),
                    ]
                ),
            )

            sess.delete(n1)

            self.assert_sql_execution(
                testing.db,
                sess.flush,
                # this is n1.parents firing off, as it should, since
                # passive_deletes is False for n1.parents
                CompiledSQL(
                    "SELECT nodes.id AS nodes_id, nodes.data AS nodes_data, "
                    "nodes.favorite_node_id AS nodes_favorite_node_id FROM "
                    "nodes, node_to_nodes WHERE :param_1 = "
                    "node_to_nodes.right_node_id AND nodes.id = "
                    "node_to_nodes.left_node_id",
                    lambda ctx: {"param_1": n1.id},
                ),
                CompiledSQL(
                    "DELETE FROM node_to_nodes WHERE "
                    "node_to_nodes.left_node_id = :left_node_id AND "
                    "node_to_nodes.right_node_id = :right_node_id",
                    lambda ctx: [
                        {"right_node_id": n2.id, "left_node_id": n1.id},
                        {"right_node_id": n3.id, "left_node_id": n1.id},
                        {"right_node_id": n4.id, "left_node_id": n1.id},
                    ],
                ),
                CompiledSQL(
                    "DELETE FROM nodes WHERE nodes.id = :id",
                    lambda ctx: {"id": n1.id},
                ),
            )

            for n in [n2, n3, n4, n5]:
                sess.delete(n)

            # load these collections
            # outside of the flush() below
            n4.children
            n5.children

            self.assert_sql_execution(
                testing.db,
                sess.flush,
                CompiledSQL(
                    "DELETE FROM node_to_nodes "
                    "WHERE node_to_nodes.left_node_id "
                    "= :left_node_id AND node_to_nodes.right_node_id = "
                    ":right_node_id",
                    lambda ctx: [
                        {"right_node_id": n5.id, "left_node_id": n3.id},
                        {"right_node_id": n4.id, "left_node_id": n3.id},
                        {"right_node_id": n3.id, "left_node_id": n2.id},
                        {"right_node_id": n5.id, "left_node_id": n2.id},
                    ],
                ),
                CompiledSQL(
                    "DELETE FROM nodes WHERE nodes.id = :id",
                    lambda ctx: [{"id": n4.id}, {"id": n5.id}],
                ),
                CompiledSQL(
                    "DELETE FROM nodes WHERE nodes.id = :id",
                    lambda ctx: [{"id": n2.id}, {"id": n3.id}],
                ),
            )


class RowswitchAccountingTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "parent",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", Integer),
        )
        Table(
            "child",
            metadata,
            Column("id", Integer, ForeignKey("parent.id"), primary_key=True),
            Column("data", Integer),
        )

    def _fixture(self):
        parent, child = self.tables.parent, self.tables.child

        class Parent(fixtures.BasicEntity):
            pass

        class Child(fixtures.BasicEntity):
            pass

        mapper(
            Parent,
            parent,
            properties={
                "child": relationship(
                    Child,
                    uselist=False,
                    cascade="all, delete-orphan",
                    backref="parent",
                )
            },
        )
        mapper(Child, child)
        return Parent, Child

    def test_switch_on_update(self):
        Parent, Child = self._fixture()

        sess = fixture_session(autocommit=False)

        p1 = Parent(id=1, child=Child())
        sess.add(p1)
        sess.commit()

        sess.close()
        p2 = Parent(id=1, child=Child())
        p3 = sess.merge(p2)

        old = attributes.get_history(p3, "child")[2][0]
        assert old in sess

        # essentially no SQL should emit here,
        # because we've replaced the row with another identical one
        sess.flush()

        assert p3.child._sa_instance_state.session_id == sess.hash_key
        assert p3.child in sess

        p4 = Parent(id=1, child=Child())
        p5 = sess.merge(p4)

        old = attributes.get_history(p5, "child")[2][0]
        assert old in sess

        sess.flush()

    def test_switch_on_delete(self):
        Parent, Child = self._fixture()

        sess = fixture_session()
        p1 = Parent(id=1, data=2, child=None)
        sess.add(p1)
        sess.flush()

        p1.id = 5
        sess.delete(p1)
        eq_(p1.id, 5)
        sess.flush()

        eq_(
            sess.scalar(
                select(func.count("*")).select_from(self.tables.parent)
            ),
            0,
        )

        sess.close()


class RowswitchM2OTest(fixtures.MappedTest):
    # tests for #3060 and related issues

    @classmethod
    def define_tables(cls, metadata):
        Table("a", metadata, Column("id", Integer, primary_key=True))
        Table(
            "b",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("aid", ForeignKey("a.id")),
            Column("cid", ForeignKey("c.id")),
            Column("data", String(50)),
        )
        Table("c", metadata, Column("id", Integer, primary_key=True))

    def _fixture(self):
        a, b, c = self.tables.a, self.tables.b, self.tables.c

        class A(fixtures.BasicEntity):
            pass

        class B(fixtures.BasicEntity):
            pass

        class C(fixtures.BasicEntity):
            pass

        mapper(
            A,
            a,
            properties={"bs": relationship(B, cascade="all, delete-orphan")},
        )
        mapper(B, b, properties={"c": relationship(C)})
        mapper(C, c)
        return A, B, C

    def test_set_none_replaces_m2o(self):
        # we have to deal here with the fact that a
        # get of an unset attribute implicitly sets it to None
        # with no history.  So while we'd like "b.x = None" to
        # record that "None" was added and we can then actively set it,
        # a simple read of "b.x" ruins that; we'd have to dramatically
        # alter the semantics of get() such that it creates history, which
        # would incur extra work within the flush process to deal with
        # change that previously showed up as nothing.

        A, B, C = self._fixture()
        sess = fixture_session()

        sess.add(A(id=1, bs=[B(id=1, c=C(id=1))]))
        sess.commit()

        a1 = sess.query(A).first()
        a1.bs = [B(id=1, c=None)]
        sess.commit()
        assert a1.bs[0].c is None

    def test_set_none_w_get_replaces_m2o(self):
        A, B, C = self._fixture()
        sess = fixture_session()

        sess.add(A(id=1, bs=[B(id=1, c=C(id=1))]))
        sess.commit()

        a1 = sess.query(A).first()
        b2 = B(id=1)
        assert b2.c is None
        b2.c = None
        a1.bs = [b2]
        sess.commit()
        assert a1.bs[0].c is None

    def test_set_none_replaces_scalar(self):
        # this case worked before #3060, because a straight scalar
        # set of None shows up.  However, as test_set_none_w_get
        # shows, we can't rely on this - the get of None will blow
        # away the history.
        A, B, C = self._fixture()
        sess = fixture_session()

        sess.add(A(id=1, bs=[B(id=1, data="somedata")]))
        sess.commit()

        a1 = sess.query(A).first()
        a1.bs = [B(id=1, data=None)]
        sess.commit()
        assert a1.bs[0].data is None

    def test_set_none_w_get_replaces_scalar(self):
        A, B, C = self._fixture()
        sess = fixture_session()

        sess.add(A(id=1, bs=[B(id=1, data="somedata")]))
        sess.commit()

        a1 = sess.query(A).first()
        b2 = B(id=1)
        assert b2.data is None
        b2.data = None
        a1.bs = [b2]
        sess.commit()
        assert a1.bs[0].data is None


class BasicStaleChecksTest(fixtures.MappedTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "parent",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", Integer),
        )
        Table(
            "child",
            metadata,
            Column("id", Integer, ForeignKey("parent.id"), primary_key=True),
            Column("data", Integer),
        )

    def _fixture(self, confirm_deleted_rows=True):
        parent, child = self.tables.parent, self.tables.child

        class Parent(fixtures.BasicEntity):
            pass

        class Child(fixtures.BasicEntity):
            pass

        mapper(
            Parent,
            parent,
            properties={
                "child": relationship(
                    Child,
                    uselist=False,
                    cascade="all, delete-orphan",
                    backref="parent",
                )
            },
            confirm_deleted_rows=confirm_deleted_rows,
        )
        mapper(Child, child)
        return Parent, Child

    @testing.requires.sane_rowcount
    def test_update_single_missing(self):
        Parent, Child = self._fixture()
        sess = fixture_session()
        p1 = Parent(id=1, data=2)
        sess.add(p1)
        sess.flush()

        sess.execute(self.tables.parent.delete())

        p1.data = 3
        assert_raises_message(
            orm_exc.StaleDataError,
            r"UPDATE statement on table 'parent' expected to "
            r"update 1 row\(s\); 0 were matched.",
            sess.flush,
        )

    @testing.requires.sane_rowcount
    def test_update_single_missing_broken_multi_rowcount(self):
        @util.memoized_property
        def rowcount(self):
            if len(self.context.compiled_parameters) > 1:
                return -1
            else:
                return self.context.rowcount

        with patch.object(
            config.db.dialect, "supports_sane_multi_rowcount", False
        ):
            with patch(
                "sqlalchemy.engine.cursor.CursorResult.rowcount", rowcount
            ):
                Parent, Child = self._fixture()
                sess = fixture_session()
                p1 = Parent(id=1, data=2)
                sess.add(p1)
                sess.flush()

                sess.execute(self.tables.parent.delete())

                p1.data = 3
                assert_raises_message(
                    orm_exc.StaleDataError,
                    r"UPDATE statement on table 'parent' expected to "
                    r"update 1 row\(s\); 0 were matched.",
                    sess.flush,
                )

    def test_update_multi_missing_broken_multi_rowcount(self):
        @util.memoized_property
        def rowcount(self):
            if len(self.context.compiled_parameters) > 1:
                return -1
            else:
                return self.context.rowcount

        with patch.object(
            config.db.dialect, "supports_sane_multi_rowcount", False
        ):
            with patch(
                "sqlalchemy.engine.cursor.CursorResult.rowcount", rowcount
            ):
                Parent, Child = self._fixture()
                sess = fixture_session()
                p1 = Parent(id=1, data=2)
                p2 = Parent(id=2, data=3)
                sess.add_all([p1, p2])
                sess.flush()

                sess.execute(self.tables.parent.delete().where(Parent.id == 1))

                p1.data = 3
                p2.data = 4
                sess.flush()  # no exception

                # update occurred for remaining row
                eq_(sess.query(Parent.id, Parent.data).all(), [(2, 4)])

    def test_update_value_missing_broken_multi_rowcount(self):
        @util.memoized_property
        def rowcount(self):
            if len(self.context.compiled_parameters) > 1:
                return -1
            else:
                return self.context.rowcount

        with patch.object(
            config.db.dialect, "supports_sane_multi_rowcount", False
        ):
            with patch(
                "sqlalchemy.engine.cursor.CursorResult.rowcount", rowcount
            ):
                Parent, Child = self._fixture()
                sess = fixture_session()
                p1 = Parent(id=1, data=1)
                sess.add(p1)
                sess.flush()

                sess.execute(self.tables.parent.delete())

                p1.data = literal(1)
                assert_raises_message(
                    orm_exc.StaleDataError,
                    r"UPDATE statement on table 'parent' expected to "
                    r"update 1 row\(s\); 0 were matched.",
                    sess.flush,
                )

    @testing.requires.sane_rowcount
    def test_delete_twice(self):
        Parent, Child = self._fixture()
        sess = fixture_session()
        p1 = Parent(id=1, data=2, child=None)
        sess.add(p1)
        sess.commit()

        sess.delete(p1)
        sess.flush()

        sess.delete(p1)

        assert_raises_message(
            exc.SAWarning,
            r"DELETE statement on table 'parent' expected to "
            r"delete 1 row\(s\); 0 were matched.",
            sess.commit,
        )

    @testing.requires.sane_multi_rowcount
    def test_delete_multi_missing_warning(self):
        Parent, Child = self._fixture()
        sess = fixture_session()
        p1 = Parent(id=1, data=2, child=None)
        p2 = Parent(id=2, data=3, child=None)
        sess.add_all([p1, p2])
        sess.flush()

        sess.execute(self.tables.parent.delete())
        sess.delete(p1)
        sess.delete(p2)

        assert_raises_message(
            exc.SAWarning,
            r"DELETE statement on table 'parent' expected to "
            r"delete 2 row\(s\); 0 were matched.",
            sess.flush,
        )

    def test_update_single_broken_multi_rowcount_still_raises(self):
        # raise occurs for single row UPDATE that misses even if
        # supports_sane_multi_rowcount is False
        Parent, Child = self._fixture()
        sess = fixture_session()
        p1 = Parent(id=1, data=2, child=None)
        sess.add(p1)
        sess.flush()

        sess.execute(self.tables.parent.delete())
        p1.data = 3

        with patch.object(
            config.db.dialect, "supports_sane_multi_rowcount", False
        ):
            assert_raises_message(
                orm_exc.StaleDataError,
                r"UPDATE statement on table 'parent' expected to "
                r"update 1 row\(s\); 0 were matched.",
                sess.flush,
            )

    def test_update_multi_broken_multi_rowcount_doesnt_raise(self):
        # raise does not occur for multirow UPDATE that misses if
        # supports_sane_multi_rowcount is False, even if rowcount is still
        # correct
        Parent, Child = self._fixture()
        sess = fixture_session()
        p1 = Parent(id=1, data=2, child=None)
        p2 = Parent(id=2, data=3, child=None)
        sess.add_all([p1, p2])
        sess.flush()

        sess.execute(self.tables.parent.delete())
        p1.data = 3
        p2.data = 4

        with patch.object(
            config.db.dialect, "supports_sane_multi_rowcount", False
        ):
            # no raise
            sess.flush()

    def test_delete_single_broken_multi_rowcount_still_warns(self):
        Parent, Child = self._fixture()
        sess = fixture_session()
        p1 = Parent(id=1, data=2, child=None)
        sess.add(p1)
        sess.flush()
        sess.flush()

        sess.execute(self.tables.parent.delete())
        sess.delete(p1)

        # only one row, so it warns
        with patch.object(
            config.db.dialect, "supports_sane_multi_rowcount", False
        ):
            assert_raises_message(
                exc.SAWarning,
                r"DELETE statement on table 'parent' expected to "
                r"delete 1 row\(s\); 0 were matched.",
                sess.flush,
            )

    def test_delete_multi_broken_multi_rowcount_doesnt_warn(self):
        Parent, Child = self._fixture()
        sess = fixture_session()
        p1 = Parent(id=1, data=2, child=None)
        p2 = Parent(id=2, data=3, child=None)
        sess.add_all([p1, p2])
        sess.flush()

        sess.execute(self.tables.parent.delete())
        sess.delete(p1)
        sess.delete(p2)

        # if the dialect reports supports_sane_multi_rowcount as false,
        # if there were more than one row deleted, need to ensure the
        # rowcount result is ignored.  psycopg2 + batch mode reports the
        # wrong number, not -1. see issue #4661
        with patch.object(
            config.db.dialect, "supports_sane_multi_rowcount", False
        ):
            # no warning
            sess.flush()

    def test_delete_multi_missing_allow(self):
        Parent, Child = self._fixture(confirm_deleted_rows=False)
        sess = fixture_session()
        p1 = Parent(id=1, data=2, child=None)
        p2 = Parent(id=2, data=3, child=None)
        sess.add_all([p1, p2])
        sess.flush()

        sess.execute(self.tables.parent.delete())
        sess.delete(p1)
        sess.delete(p2)

        sess.flush()


class BatchInsertsTest(fixtures.MappedTest, testing.AssertsExecutionResults):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "t",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(50)),
            Column("def_", String(50), server_default="def1"),
        )

    def test_batch_interaction(self):
        """test batching groups same-structured, primary
        key present statements together.

        """

        t = self.tables.t

        class T(fixtures.ComparableEntity):
            pass

        mapper(T, t)
        sess = fixture_session()
        sess.add_all(
            [
                T(data="t1"),
                T(data="t2"),
                T(id=3, data="t3"),
                T(id=4, data="t4"),
                T(id=5, data="t5"),
                T(id=6, data=func.lower("t6")),
                T(id=7, data="t7"),
                T(id=8, data="t8"),
                T(id=9, data="t9", def_="def2"),
                T(id=10, data="t10", def_="def3"),
                T(id=11, data="t11"),
            ]
        )

        self.assert_sql_execution(
            testing.db,
            sess.flush,
            Conditional(
                testing.db.dialect.insert_executemany_returning,
                [
                    CompiledSQL(
                        "INSERT INTO t (data) VALUES (:data)",
                        [{"data": "t1"}, {"data": "t2"}],
                    ),
                ],
                [
                    CompiledSQL(
                        "INSERT INTO t (data) VALUES (:data)", {"data": "t1"}
                    ),
                    CompiledSQL(
                        "INSERT INTO t (data) VALUES (:data)", {"data": "t2"}
                    ),
                ],
            ),
            CompiledSQL(
                "INSERT INTO t (id, data) VALUES (:id, :data)",
                [
                    {"data": "t3", "id": 3},
                    {"data": "t4", "id": 4},
                    {"data": "t5", "id": 5},
                ],
            ),
            CompiledSQL(
                "INSERT INTO t (id, data) VALUES (:id, lower(:lower_1))",
                {"lower_1": "t6", "id": 6},
            ),
            CompiledSQL(
                "INSERT INTO t (id, data) VALUES (:id, :data)",
                [{"data": "t7", "id": 7}, {"data": "t8", "id": 8}],
            ),
            CompiledSQL(
                "INSERT INTO t (id, data, def_) VALUES (:id, :data, :def_)",
                [
                    {"data": "t9", "id": 9, "def_": "def2"},
                    {"data": "t10", "id": 10, "def_": "def3"},
                ],
            ),
            CompiledSQL(
                "INSERT INTO t (id, data) VALUES (:id, :data)",
                {"data": "t11", "id": 11},
            ),
        )


class LoadersUsingCommittedTest(UOWTest):

    """Test that events which occur within a flush()
    get the same attribute loading behavior as on the outside
    of the flush, and that the unit of work itself uses the
    "committed" version of primary/foreign key attributes
    when loading a collection for historical purposes (this typically
    has importance for when primary key values change).

    """

    def _mapper_setup(self, passive_updates=True):
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
                    Address,
                    order_by=addresses.c.email_address,
                    passive_updates=passive_updates,
                    backref="user",
                )
            },
        )
        mapper(Address, addresses)
        return fixture_session(expire_on_commit=False)

    def test_before_update_m2o(self):
        """Expect normal many to one attribute load behavior
        (should not get committed value)
        from within public 'before_update' event"""
        sess = self._mapper_setup()

        Address, User = self.classes.Address, self.classes.User

        def before_update(mapper, connection, target):
            # if get committed is used to find target.user, then
            # it will be still be u1 instead of u2
            assert target.user.id == target.user_id == u2.id

        from sqlalchemy import event

        event.listen(Address, "before_update", before_update)

        a1 = Address(email_address="a1")
        u1 = User(name="u1", addresses=[a1])
        sess.add(u1)

        u2 = User(name="u2")
        sess.add(u2)
        sess.commit()

        sess.expunge_all()
        # lookup an address and move it to the other user
        a1 = sess.query(Address).get(a1.id)

        # move address to another user's fk
        assert a1.user_id == u1.id
        a1.user_id = u2.id

        sess.flush()

    def test_before_update_o2m_passive(self):
        """Expect normal one to many attribute load behavior
        (should not get committed value)
        from within public 'before_update' event"""
        self._test_before_update_o2m(True)

    def test_before_update_o2m_notpassive(self):
        """Expect normal one to many attribute load behavior
        (should not get committed value)
        from within public 'before_update' event with
        passive_updates=False

        """
        self._test_before_update_o2m(False)

    def _test_before_update_o2m(self, passive_updates):
        sess = self._mapper_setup(passive_updates=passive_updates)

        Address, User = self.classes.Address, self.classes.User

        class AvoidReferencialError(Exception):

            """the test here would require ON UPDATE CASCADE on FKs
            for the flush to fully succeed; this exception is used
            to cancel the flush before we get that far.

            """

        def before_update(mapper, connection, target):
            if passive_updates:
                # we shouldn't be using committed value.
                # so, having switched target's primary key,
                # we expect no related items in the collection
                # since we are using passive_updates
                # this is a behavior change since #2350
                assert "addresses" not in target.__dict__
                eq_(target.addresses, [])
            else:
                # in contrast with passive_updates=True,
                # here we expect the orm to have looked up the addresses
                # with the committed value (it needs to in order to
                # update the foreign keys).  So we expect addresses
                # collection to move with the user,
                # (just like they will be after the update)

                # collection is already loaded
                assert "addresses" in target.__dict__
                eq_([a.id for a in target.addresses], [a.id for a in [a1, a2]])
            raise AvoidReferencialError()

        from sqlalchemy import event

        event.listen(User, "before_update", before_update)

        a1 = Address(email_address="jack1")
        a2 = Address(email_address="jack2")
        u1 = User(id=1, name="jack", addresses=[a1, a2])
        sess.add(u1)
        sess.commit()

        sess.expunge_all()
        u1 = sess.query(User).get(u1.id)
        u1.id = 2
        try:
            sess.flush()
        except AvoidReferencialError:
            pass


class NoAttrEventInFlushTest(fixtures.MappedTest):
    """test [ticket:3167].

    See also RefreshFlushInReturningTest in test/orm/test_events.py which
    tests the positive case for the refresh_flush event, added in
    [ticket:3427].

    """

    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "test",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("prefetch_val", Integer, default=5),
            Column("returning_val", Integer, server_default="5"),
        )

    @classmethod
    def setup_classes(cls):
        class Thing(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        Thing = cls.classes.Thing

        mapper(Thing, cls.tables.test, eager_defaults=True)

    def test_no_attr_events_flush(self):
        Thing = self.classes.Thing
        mock = Mock()
        event.listen(Thing.id, "set", mock.id)
        event.listen(Thing.prefetch_val, "set", mock.prefetch_val)
        event.listen(Thing.returning_val, "set", mock.prefetch_val)
        t1 = Thing()
        s = fixture_session()
        s.add(t1)
        s.flush()

        eq_(len(mock.mock_calls), 0)
        eq_(t1.id, 1)
        eq_(t1.prefetch_val, 5)
        eq_(t1.returning_val, 5)


class EagerDefaultsTest(fixtures.MappedTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "test",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("foo", Integer, server_default="3"),
        )

        Table(
            "test2",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("foo", Integer),
            Column("bar", Integer, server_onupdate=FetchedValue()),
        )

    @classmethod
    def setup_classes(cls):
        class Thing(cls.Basic):
            pass

        class Thing2(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        Thing = cls.classes.Thing

        mapper(Thing, cls.tables.test, eager_defaults=True)

        Thing2 = cls.classes.Thing2

        mapper(Thing2, cls.tables.test2, eager_defaults=True)

    def test_insert_defaults_present(self):
        Thing = self.classes.Thing
        s = fixture_session()

        t1, t2 = (Thing(id=1, foo=5), Thing(id=2, foo=10))

        s.add_all([t1, t2])

        self.assert_sql_execution(
            testing.db,
            s.flush,
            CompiledSQL(
                "INSERT INTO test (id, foo) VALUES (:id, :foo)",
                [{"foo": 5, "id": 1}, {"foo": 10, "id": 2}],
            ),
        )

        def go():
            eq_(t1.foo, 5)
            eq_(t2.foo, 10)

        self.assert_sql_count(testing.db, go, 0)

    def test_insert_defaults_present_as_expr(self):
        Thing = self.classes.Thing
        s = fixture_session()

        t1, t2 = (
            Thing(id=1, foo=text("2 + 5")),
            Thing(id=2, foo=text("5 + 5")),
        )

        s.add_all([t1, t2])

        if testing.db.dialect.implicit_returning:
            self.assert_sql_execution(
                testing.db,
                s.flush,
                CompiledSQL(
                    "INSERT INTO test (id, foo) VALUES (%(id)s, 2 + 5) "
                    "RETURNING test.foo",
                    [{"id": 1}],
                    dialect="postgresql",
                ),
                CompiledSQL(
                    "INSERT INTO test (id, foo) VALUES (%(id)s, 5 + 5) "
                    "RETURNING test.foo",
                    [{"id": 2}],
                    dialect="postgresql",
                ),
            )

        else:
            self.assert_sql_execution(
                testing.db,
                s.flush,
                CompiledSQL(
                    "INSERT INTO test (id, foo) VALUES (:id, 2 + 5)",
                    [{"id": 1}],
                ),
                CompiledSQL(
                    "INSERT INTO test (id, foo) VALUES (:id, 5 + 5)",
                    [{"id": 2}],
                ),
                CompiledSQL(
                    "SELECT test.foo AS test_foo FROM test "
                    "WHERE test.id = :pk_1",
                    [{"pk_1": 1}],
                ),
                CompiledSQL(
                    "SELECT test.foo AS test_foo FROM test "
                    "WHERE test.id = :pk_1",
                    [{"pk_1": 2}],
                ),
            )

        def go():
            eq_(t1.foo, 7)
            eq_(t2.foo, 10)

        self.assert_sql_count(testing.db, go, 0)

    def test_insert_defaults_nonpresent(self):
        Thing = self.classes.Thing
        s = fixture_session()

        t1, t2 = (Thing(id=1), Thing(id=2))

        s.add_all([t1, t2])

        self.assert_sql_execution(
            testing.db,
            s.commit,
            Conditional(
                testing.db.dialect.implicit_returning,
                [
                    Conditional(
                        testing.db.dialect.insert_executemany_returning,
                        [
                            CompiledSQL(
                                "INSERT INTO test (id) VALUES (%(id)s) "
                                "RETURNING test.foo",
                                [{"id": 1}, {"id": 2}],
                                dialect="postgresql",
                            ),
                        ],
                        [
                            CompiledSQL(
                                "INSERT INTO test (id) VALUES (%(id)s) "
                                "RETURNING test.foo",
                                [{"id": 1}],
                                dialect="postgresql",
                            ),
                            CompiledSQL(
                                "INSERT INTO test (id) VALUES (%(id)s) "
                                "RETURNING test.foo",
                                [{"id": 2}],
                                dialect="postgresql",
                            ),
                        ],
                    ),
                ],
                [
                    CompiledSQL(
                        "INSERT INTO test (id) VALUES (:id)",
                        [{"id": 1}, {"id": 2}],
                    ),
                    CompiledSQL(
                        "SELECT test.foo AS test_foo FROM test "
                        "WHERE test.id = :pk_1",
                        [{"pk_1": 1}],
                    ),
                    CompiledSQL(
                        "SELECT test.foo AS test_foo FROM test "
                        "WHERE test.id = :pk_1",
                        [{"pk_1": 2}],
                    ),
                ],
            ),
        )

    def test_update_defaults_nonpresent(self):
        Thing2 = self.classes.Thing2
        s = fixture_session()

        t1, t2, t3, t4 = (
            Thing2(id=1, foo=1, bar=2),
            Thing2(id=2, foo=2, bar=3),
            Thing2(id=3, foo=3, bar=4),
            Thing2(id=4, foo=4, bar=5),
        )

        s.add_all([t1, t2, t3, t4])
        s.flush()

        t1.foo = 5
        t2.foo = 6
        t2.bar = 10
        t3.foo = 7
        t4.foo = 8
        t4.bar = 12

        self.assert_sql_execution(
            testing.db,
            s.flush,
            Conditional(
                testing.db.dialect.implicit_returning,
                [
                    CompiledSQL(
                        "UPDATE test2 SET foo=%(foo)s "
                        "WHERE test2.id = %(test2_id)s "
                        "RETURNING test2.bar",
                        [{"foo": 5, "test2_id": 1}],
                        dialect="postgresql",
                    ),
                    CompiledSQL(
                        "UPDATE test2 SET foo=%(foo)s, bar=%(bar)s "
                        "WHERE test2.id = %(test2_id)s",
                        [{"foo": 6, "bar": 10, "test2_id": 2}],
                        dialect="postgresql",
                    ),
                    CompiledSQL(
                        "UPDATE test2 SET foo=%(foo)s "
                        "WHERE test2.id = %(test2_id)s "
                        "RETURNING test2.bar",
                        [{"foo": 7, "test2_id": 3}],
                        dialect="postgresql",
                    ),
                    CompiledSQL(
                        "UPDATE test2 SET foo=%(foo)s, bar=%(bar)s "
                        "WHERE test2.id = %(test2_id)s",
                        [{"foo": 8, "bar": 12, "test2_id": 4}],
                        dialect="postgresql",
                    ),
                ],
                [
                    CompiledSQL(
                        "UPDATE test2 SET foo=:foo WHERE test2.id = :test2_id",
                        [{"foo": 5, "test2_id": 1}],
                    ),
                    CompiledSQL(
                        "UPDATE test2 SET foo=:foo, bar=:bar "
                        "WHERE test2.id = :test2_id",
                        [{"foo": 6, "bar": 10, "test2_id": 2}],
                    ),
                    CompiledSQL(
                        "UPDATE test2 SET foo=:foo WHERE test2.id = :test2_id",
                        [{"foo": 7, "test2_id": 3}],
                    ),
                    CompiledSQL(
                        "UPDATE test2 SET foo=:foo, bar=:bar "
                        "WHERE test2.id = :test2_id",
                        [{"foo": 8, "bar": 12, "test2_id": 4}],
                    ),
                    CompiledSQL(
                        "SELECT test2.bar AS test2_bar FROM test2 "
                        "WHERE test2.id = :pk_1",
                        [{"pk_1": 1}],
                    ),
                    CompiledSQL(
                        "SELECT test2.bar AS test2_bar FROM test2 "
                        "WHERE test2.id = :pk_1",
                        [{"pk_1": 3}],
                    ),
                ],
            ),
        )

        def go():
            eq_(t1.bar, 2)
            eq_(t2.bar, 10)
            eq_(t3.bar, 4)
            eq_(t4.bar, 12)

        self.assert_sql_count(testing.db, go, 0)

    def test_update_defaults_present_as_expr(self):
        Thing2 = self.classes.Thing2
        s = fixture_session()

        t1, t2, t3, t4 = (
            Thing2(id=1, foo=1, bar=2),
            Thing2(id=2, foo=2, bar=3),
            Thing2(id=3, foo=3, bar=4),
            Thing2(id=4, foo=4, bar=5),
        )

        s.add_all([t1, t2, t3, t4])
        s.flush()

        t1.foo = 5
        t1.bar = text("1 + 1")
        t2.foo = 6
        t2.bar = 10
        t3.foo = 7
        t4.foo = 8
        t4.bar = text("5 + 7")

        if testing.db.dialect.implicit_returning:
            self.assert_sql_execution(
                testing.db,
                s.flush,
                CompiledSQL(
                    "UPDATE test2 SET foo=%(foo)s, bar=1 + 1 "
                    "WHERE test2.id = %(test2_id)s "
                    "RETURNING test2.bar",
                    [{"foo": 5, "test2_id": 1}],
                    dialect="postgresql",
                ),
                CompiledSQL(
                    "UPDATE test2 SET foo=%(foo)s, bar=%(bar)s "
                    "WHERE test2.id = %(test2_id)s",
                    [{"foo": 6, "bar": 10, "test2_id": 2}],
                    dialect="postgresql",
                ),
                CompiledSQL(
                    "UPDATE test2 SET foo=%(foo)s "
                    "WHERE test2.id = %(test2_id)s "
                    "RETURNING test2.bar",
                    [{"foo": 7, "test2_id": 3}],
                    dialect="postgresql",
                ),
                CompiledSQL(
                    "UPDATE test2 SET foo=%(foo)s, bar=5 + 7 "
                    "WHERE test2.id = %(test2_id)s RETURNING test2.bar",
                    [{"foo": 8, "test2_id": 4}],
                    dialect="postgresql",
                ),
            )
        else:
            self.assert_sql_execution(
                testing.db,
                s.flush,
                CompiledSQL(
                    "UPDATE test2 SET foo=:foo, bar=1 + 1 "
                    "WHERE test2.id = :test2_id",
                    [{"foo": 5, "test2_id": 1}],
                ),
                CompiledSQL(
                    "UPDATE test2 SET foo=:foo, bar=:bar "
                    "WHERE test2.id = :test2_id",
                    [{"foo": 6, "bar": 10, "test2_id": 2}],
                ),
                CompiledSQL(
                    "UPDATE test2 SET foo=:foo WHERE test2.id = :test2_id",
                    [{"foo": 7, "test2_id": 3}],
                ),
                CompiledSQL(
                    "UPDATE test2 SET foo=:foo, bar=5 + 7 "
                    "WHERE test2.id = :test2_id",
                    [{"foo": 8, "test2_id": 4}],
                ),
                CompiledSQL(
                    "SELECT test2.bar AS test2_bar FROM test2 "
                    "WHERE test2.id = :pk_1",
                    [{"pk_1": 1}],
                ),
                CompiledSQL(
                    "SELECT test2.bar AS test2_bar FROM test2 "
                    "WHERE test2.id = :pk_1",
                    [{"pk_1": 3}],
                ),
                CompiledSQL(
                    "SELECT test2.bar AS test2_bar FROM test2 "
                    "WHERE test2.id = :pk_1",
                    [{"pk_1": 4}],
                ),
            )

        def go():
            eq_(t1.bar, 2)
            eq_(t2.bar, 10)
            eq_(t3.bar, 4)
            eq_(t4.bar, 12)

        self.assert_sql_count(testing.db, go, 0)

    def test_insert_defaults_bulk_insert(self):
        Thing = self.classes.Thing
        s = fixture_session()

        mappings = [{"id": 1}, {"id": 2}]

        self.assert_sql_execution(
            testing.db,
            lambda: s.bulk_insert_mappings(Thing, mappings),
            CompiledSQL(
                "INSERT INTO test (id) VALUES (:id)", [{"id": 1}, {"id": 2}]
            ),
        )

    def test_update_defaults_bulk_update(self):
        Thing2 = self.classes.Thing2
        s = fixture_session()

        t1, t2, t3, t4 = (
            Thing2(id=1, foo=1, bar=2),
            Thing2(id=2, foo=2, bar=3),
            Thing2(id=3, foo=3, bar=4),
            Thing2(id=4, foo=4, bar=5),
        )

        s.add_all([t1, t2, t3, t4])
        s.flush()

        mappings = [
            {"id": 1, "foo": 5},
            {"id": 2, "foo": 6, "bar": 10},
            {"id": 3, "foo": 7},
            {"id": 4, "foo": 8},
        ]

        self.assert_sql_execution(
            testing.db,
            lambda: s.bulk_update_mappings(Thing2, mappings),
            CompiledSQL(
                "UPDATE test2 SET foo=:foo WHERE test2.id = :test2_id",
                [{"foo": 5, "test2_id": 1}],
            ),
            CompiledSQL(
                "UPDATE test2 SET foo=:foo, bar=:bar "
                "WHERE test2.id = :test2_id",
                [{"foo": 6, "bar": 10, "test2_id": 2}],
            ),
            CompiledSQL(
                "UPDATE test2 SET foo=:foo WHERE test2.id = :test2_id",
                [{"foo": 7, "test2_id": 3}, {"foo": 8, "test2_id": 4}],
            ),
        )

    def test_update_defaults_present(self):
        Thing2 = self.classes.Thing2
        s = fixture_session()

        t1, t2 = (Thing2(id=1, foo=1, bar=2), Thing2(id=2, foo=2, bar=3))

        s.add_all([t1, t2])
        s.flush()

        t1.bar = 5
        t2.bar = 10

        self.assert_sql_execution(
            testing.db,
            s.commit,
            CompiledSQL(
                "UPDATE test2 SET bar=%(bar)s WHERE test2.id = %(test2_id)s",
                [{"bar": 5, "test2_id": 1}, {"bar": 10, "test2_id": 2}],
                dialect="postgresql",
            ),
        )

    def test_insert_dont_fetch_nondefaults(self):
        Thing2 = self.classes.Thing2
        s = fixture_session()

        t1 = Thing2(id=1, bar=2)

        s.add(t1)

        self.assert_sql_execution(
            testing.db,
            s.flush,
            CompiledSQL(
                "INSERT INTO test2 (id, foo, bar) " "VALUES (:id, :foo, :bar)",
                [{"id": 1, "foo": None, "bar": 2}],
            ),
        )

    def test_update_dont_fetch_nondefaults(self):
        Thing2 = self.classes.Thing2
        s = fixture_session()

        t1 = Thing2(id=1, bar=2)

        s.add(t1)
        s.flush()

        s.expire(t1, ["foo"])

        t1.bar = 3

        self.assert_sql_execution(
            testing.db,
            s.flush,
            CompiledSQL(
                "UPDATE test2 SET bar=:bar WHERE test2.id = :test2_id",
                [{"bar": 3, "test2_id": 1}],
            ),
        )


class TypeWoBoolTest(fixtures.MappedTest, testing.AssertsExecutionResults):
    """test support for custom datatypes that return a non-__bool__ value
    when compared via __eq__(), eg. ticket 3469"""

    @classmethod
    def define_tables(cls, metadata):
        from sqlalchemy import TypeDecorator

        class NoBool(object):
            def __nonzero__(self):
                raise NotImplementedError("not supported")

        class MyWidget(object):
            def __init__(self, text):
                self.text = text

            def __eq__(self, other):
                return NoBool()

        cls.MyWidget = MyWidget

        class MyType(TypeDecorator):
            impl = String(50)

            def process_bind_param(self, value, dialect):
                if value is not None:
                    value = value.text
                return value

            def process_result_value(self, value, dialect):
                if value is not None:
                    value = MyWidget(value)
                return value

        Table(
            "test",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("value", MyType),
            Column("unrelated", String(50)),
        )

    @classmethod
    def setup_classes(cls):
        class Thing(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        Thing = cls.classes.Thing

        mapper(Thing, cls.tables.test)

    def test_update_against_none(self):
        Thing = self.classes.Thing

        s = fixture_session()
        s.add(Thing(value=self.MyWidget("foo")))
        s.commit()

        t1 = s.query(Thing).first()
        t1.value = None
        s.commit()

        eq_(s.query(Thing.value).scalar(), None)

    def test_update_against_something_else(self):
        Thing = self.classes.Thing

        s = fixture_session()
        s.add(Thing(value=self.MyWidget("foo")))
        s.commit()

        t1 = s.query(Thing).first()
        t1.value = self.MyWidget("bar")
        s.commit()

        eq_(s.query(Thing.value).scalar().text, "bar")

    def test_no_update_no_change(self):
        Thing = self.classes.Thing

        s = fixture_session()
        s.add(Thing(value=self.MyWidget("foo"), unrelated="unrelated"))
        s.commit()

        t1 = s.query(Thing).first()
        t1.unrelated = "something else"

        self.assert_sql_execution(
            testing.db,
            s.commit,
            CompiledSQL(
                "UPDATE test SET unrelated=:unrelated "
                "WHERE test.id = :test_id",
                [{"test_id": 1, "unrelated": "something else"}],
            ),
        )

        eq_(s.query(Thing.value).scalar().text, "foo")


class NullEvaluatingTest(fixtures.MappedTest, testing.AssertsExecutionResults):
    @classmethod
    def define_tables(cls, metadata):
        from sqlalchemy import TypeDecorator

        class EvalsNull(TypeDecorator):
            impl = String(50)

            should_evaluate_none = True

            def process_bind_param(self, value, dialect):
                if value is None:
                    value = "nothing"
                return value

        Table(
            "test",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("evals_null_no_default", EvalsNull()),
            Column("evals_null_default", EvalsNull(), default="default_val"),
            Column("no_eval_null_no_default", String(50)),
            Column("no_eval_null_default", String(50), default="default_val"),
            Column(
                "builtin_evals_null_no_default", String(50).evaluates_none()
            ),
            Column(
                "builtin_evals_null_default",
                String(50).evaluates_none(),
                default="default_val",
            ),
        )

        Table(
            "test_w_renames",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("evals_null_no_default", EvalsNull()),
            Column("evals_null_default", EvalsNull(), default="default_val"),
            Column("no_eval_null_no_default", String(50)),
            Column("no_eval_null_default", String(50), default="default_val"),
            Column(
                "builtin_evals_null_no_default", String(50).evaluates_none()
            ),
            Column(
                "builtin_evals_null_default",
                String(50).evaluates_none(),
                default="default_val",
            ),
        )

        if testing.requires.json_type.enabled:
            Table(
                "test_has_json",
                metadata,
                Column(
                    "id",
                    Integer,
                    primary_key=True,
                    test_needs_autoincrement=True,
                ),
                Column("data", JSON(none_as_null=True).evaluates_none()),
                Column("data_null", JSON(none_as_null=True)),
            )

    @classmethod
    def setup_classes(cls):
        class Thing(cls.Basic):
            pass

        class AltNameThing(cls.Basic):
            pass

        class JSONThing(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        Thing = cls.classes.Thing
        AltNameThing = cls.classes.AltNameThing

        mapper(Thing, cls.tables.test)

        mapper(AltNameThing, cls.tables.test_w_renames, column_prefix="_foo_")

        if testing.requires.json_type.enabled:
            mapper(cls.classes.JSONThing, cls.tables.test_has_json)

    def _assert_col(self, name, value):
        Thing, AltNameThing = self.classes.Thing, self.classes.AltNameThing
        s = fixture_session()

        col = getattr(Thing, name)
        obj = s.query(col).filter(col == value).one()
        eq_(obj[0], value)

        col = getattr(AltNameThing, "_foo_" + name)
        obj = s.query(col).filter(col == value).one()
        eq_(obj[0], value)

    def _test_insert(self, attr, expected):
        Thing, AltNameThing = self.classes.Thing, self.classes.AltNameThing

        s = fixture_session()
        t1 = Thing(**{attr: None})
        s.add(t1)

        t2 = AltNameThing(**{"_foo_" + attr: None})
        s.add(t2)

        s.commit()

        self._assert_col(attr, expected)

    def _test_bulk_insert(self, attr, expected):
        Thing, AltNameThing = self.classes.Thing, self.classes.AltNameThing

        s = fixture_session()
        s.bulk_insert_mappings(Thing, [{attr: None}])
        s.bulk_insert_mappings(AltNameThing, [{"_foo_" + attr: None}])
        s.commit()

        self._assert_col(attr, expected)

    def _test_insert_novalue(self, attr, expected):
        Thing, AltNameThing = self.classes.Thing, self.classes.AltNameThing

        s = fixture_session()
        t1 = Thing()
        s.add(t1)

        t2 = AltNameThing()
        s.add(t2)

        s.commit()

        self._assert_col(attr, expected)

    def _test_bulk_insert_novalue(self, attr, expected):
        Thing, AltNameThing = self.classes.Thing, self.classes.AltNameThing

        s = fixture_session()
        s.bulk_insert_mappings(Thing, [{}])
        s.bulk_insert_mappings(AltNameThing, [{}])
        s.commit()

        self._assert_col(attr, expected)

    def test_evalnull_nodefault_insert(self):
        self._test_insert("evals_null_no_default", "nothing")

    def test_evalnull_nodefault_bulk_insert(self):
        self._test_bulk_insert("evals_null_no_default", "nothing")

    def test_evalnull_nodefault_insert_novalue(self):
        self._test_insert_novalue("evals_null_no_default", None)

    def test_evalnull_nodefault_bulk_insert_novalue(self):
        self._test_bulk_insert_novalue("evals_null_no_default", None)

    def test_evalnull_default_insert(self):
        self._test_insert("evals_null_default", "nothing")

    def test_evalnull_default_bulk_insert(self):
        self._test_bulk_insert("evals_null_default", "nothing")

    def test_evalnull_default_insert_novalue(self):
        self._test_insert_novalue("evals_null_default", "default_val")

    def test_evalnull_default_bulk_insert_novalue(self):
        self._test_bulk_insert_novalue("evals_null_default", "default_val")

    def test_no_evalnull_nodefault_insert(self):
        self._test_insert("no_eval_null_no_default", None)

    def test_no_evalnull_nodefault_bulk_insert(self):
        self._test_bulk_insert("no_eval_null_no_default", None)

    def test_no_evalnull_nodefault_insert_novalue(self):
        self._test_insert_novalue("no_eval_null_no_default", None)

    def test_no_evalnull_nodefault_bulk_insert_novalue(self):
        self._test_bulk_insert_novalue("no_eval_null_no_default", None)

    def test_no_evalnull_default_insert(self):
        self._test_insert("no_eval_null_default", "default_val")

    def test_no_evalnull_default_bulk_insert(self):
        self._test_bulk_insert("no_eval_null_default", "default_val")

    def test_no_evalnull_default_insert_novalue(self):
        self._test_insert_novalue("no_eval_null_default", "default_val")

    def test_no_evalnull_default_bulk_insert_novalue(self):
        self._test_bulk_insert_novalue("no_eval_null_default", "default_val")

    def test_builtin_evalnull_nodefault_insert(self):
        self._test_insert("builtin_evals_null_no_default", None)

    def test_builtin_evalnull_nodefault_bulk_insert(self):
        self._test_bulk_insert("builtin_evals_null_no_default", None)

    def test_builtin_evalnull_nodefault_insert_novalue(self):
        self._test_insert_novalue("builtin_evals_null_no_default", None)

    def test_builtin_evalnull_nodefault_bulk_insert_novalue(self):
        self._test_bulk_insert_novalue("builtin_evals_null_no_default", None)

    def test_builtin_evalnull_default_insert(self):
        self._test_insert("builtin_evals_null_default", None)

    def test_builtin_evalnull_default_bulk_insert(self):
        self._test_bulk_insert("builtin_evals_null_default", None)

    def test_builtin_evalnull_default_insert_novalue(self):
        self._test_insert_novalue("builtin_evals_null_default", "default_val")

    def test_builtin_evalnull_default_bulk_insert_novalue(self):
        self._test_bulk_insert_novalue(
            "builtin_evals_null_default", "default_val"
        )

    @testing.requires.json_type
    def test_json_none_as_null(self):
        JSONThing = self.classes.JSONThing

        s = fixture_session()
        f1 = JSONThing(data=None, data_null=None)
        s.add(f1)
        s.commit()
        eq_(s.query(cast(JSONThing.data, String)).scalar(), "null")
        eq_(s.query(cast(JSONThing.data_null, String)).scalar(), None)


class EnsureCacheTest(fixtures.FutureEngineMixin, UOWTest):
    def test_ensure_cache(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        cache = {}
        eq_(len(inspect(User)._compiled_cache), 0)

        with testing.db.connect().execution_options(
            compiled_cache=cache
        ) as conn:
            s = Session(conn)
            u1 = User(name="adf")
            s.add(u1)
            s.flush()

            is_(conn._execution_options["compiled_cache"], cache)
            eq_(len(inspect(User)._compiled_cache), 1)

            u1.name = "newname"
            s.flush()

            is_(conn._execution_options["compiled_cache"], cache)
            eq_(len(inspect(User)._compiled_cache), 2)

            s.delete(u1)
            s.flush()

            is_(conn._execution_options["compiled_cache"], cache)
            eq_(len(inspect(User)._compiled_cache), 3)


class ORMOnlyPrimaryKeyTest(fixtures.TestBase):
    @testing.requires.identity_columns
    @testing.requires.returning
    def test_a(self, base, run_test):
        class A(base):
            __tablename__ = "a"

            id = Column(Integer, Identity())
            included_col = Column(Integer)

            __mapper_args__ = {"primary_key": [id], "eager_defaults": True}

        run_test(A, A())

    @testing.requires.sequences_as_server_defaults
    @testing.requires.returning
    def test_b(self, base, run_test):

        seq = Sequence("x_seq")

        class A(base):
            __tablename__ = "a"

            id = Column(Integer, seq, server_default=seq.next_value())
            included_col = Column(Integer)

            __mapper_args__ = {"primary_key": [id], "eager_defaults": True}

        run_test(A, A())

    def test_c(self, base, run_test):
        class A(base):
            __tablename__ = "a"

            id = Column(Integer, nullable=False)
            included_col = Column(Integer)

            __mapper_args__ = {"primary_key": [id]}

        a1 = A(id=1, included_col=select(1).scalar_subquery())
        run_test(A, a1)

    def test_d(self, base, run_test):
        class A(base):
            __tablename__ = "a"

            id = Column(Integer, nullable=False)
            updated_at = Column(DateTime, server_default=func.now())

            __mapper_args__ = {"primary_key": [id], "eager_defaults": True}

        a1 = A(id=1)
        run_test(A, a1)

    @testing.fixture
    def base(self, metadata):
        yield declarative_base(metadata=metadata)
        clear_mappers()

    @testing.fixture
    def run_test(self, metadata, connection):
        def go(A, a1):
            metadata.create_all(connection)

            with Session(connection) as s:
                s.add(a1)

                s.flush()
                eq_(a1.id, 1)
                s.commit()

                aa = s.query(A).first()
                is_(a1, aa)

        return go
