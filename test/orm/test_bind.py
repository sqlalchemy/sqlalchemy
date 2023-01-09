from unittest.mock import Mock

import sqlalchemy as sa
from sqlalchemy import delete
from sqlalchemy import ForeignKey
from sqlalchemy import insert
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import table
from sqlalchemy import testing
from sqlalchemy import true
from sqlalchemy import update
from sqlalchemy.orm import aliased
from sqlalchemy.orm import backref
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.query import Query
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import mock
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from test.orm import _fixtures


class BindIntegrationTest(_fixtures.FixtureTest):
    run_inserts = None

    def test_mapped_binds(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        # ensure tables are unbound
        m2 = sa.MetaData()
        users_unbound = users.to_metadata(m2)
        addresses_unbound = addresses.to_metadata(m2)

        self.mapper_registry.map_imperatively(Address, addresses_unbound)
        self.mapper_registry.map_imperatively(
            User,
            users_unbound,
            properties={
                "addresses": relationship(
                    Address,
                    backref=backref("user", cascade="all"),
                    cascade="all",
                )
            },
        )

        sess = Session(binds={User: testing.db, Address: testing.db})

        u1 = User(id=1, name="ed")
        sess.add(u1)
        eq_(
            sess.query(User).filter(User.id == 1).all(),
            [User(id=1, name="ed")],
        )

        # test expression binding

        sess.execute(users_unbound.insert(), params=dict(id=2, name="jack"))
        eq_(
            sess.execute(
                users_unbound.select().where(users_unbound.c.id == 2)
            ).fetchall(),
            [(2, "jack")],
        )

        eq_(
            sess.execute(
                users_unbound.select().where(User.id == 2)
            ).fetchall(),
            [(2, "jack")],
        )

        sess.execute(users_unbound.delete())
        eq_(sess.execute(users_unbound.select()).fetchall(), [])

        sess.close()

    def test_table_binds(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        # ensure tables are unbound
        m2 = sa.MetaData()
        users_unbound = users.to_metadata(m2)
        addresses_unbound = addresses.to_metadata(m2)

        self.mapper_registry.map_imperatively(Address, addresses_unbound)
        self.mapper_registry.map_imperatively(
            User,
            users_unbound,
            properties={
                "addresses": relationship(
                    Address,
                    backref=backref("user", cascade="all"),
                    cascade="all",
                )
            },
        )

        maker = sessionmaker(
            binds={
                users_unbound: testing.db,
                addresses_unbound: testing.db,
            }
        )
        sess = maker()

        u1 = User(id=1, name="ed")
        sess.add(u1)
        eq_(
            sess.query(User).filter(User.id == 1).all(),
            [User(id=1, name="ed")],
        )

        sess.execute(users_unbound.insert(), params=dict(id=2, name="jack"))

        eq_(
            sess.execute(
                users_unbound.select().where(users_unbound.c.id == 2)
            ).fetchall(),
            [(2, "jack")],
        )

        eq_(
            sess.execute(
                users_unbound.select().where(User.id == 2)
            ).fetchall(),
            [(2, "jack")],
        )

        sess.execute(users_unbound.delete())
        eq_(sess.execute(users_unbound.select()).fetchall(), [])

        sess.close()

    def test_bind_from_metadata(self):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)

        session = fixture_session()

        session.execute(users.insert(), dict(name="Johnny"))

        assert len(session.query(User).filter_by(name="Johnny").all()) == 1

        session.execute(users.delete())

        assert len(session.query(User).filter_by(name="Johnny").all()) == 0
        session.close()

    @testing.combinations(
        (lambda: {}, "e3"),
        (lambda e1: {"bind": e1}, "e1"),
        (lambda e1, Address: {"bind": e1, "mapper": Address}, "e1"),
        (
            lambda e1, Address: {
                "bind": e1,
                "clause": Query([Address])._statement_20(),
            },
            "e1",
        ),
        (
            lambda users, User: {"clause": select(users).join(User.addresses)},
            "e1",
        ),
        (lambda Address: {"mapper": Address}, "e2"),
        (lambda Address: {"clause": Query([Address])._statement_20()}, "e2"),
        (lambda addresses: {"clause": select(addresses)}, "e2"),
        (lambda Dingaling: {"mapper": Dingaling}, "e4"),
        (lambda addresses_view: {"clause": addresses_view}, "e4"),
        (lambda addresses_view: {"clause": select(addresses_view)}, "e4"),
        (lambda users_view: {"clause": select(users_view)}, "e2"),
        (
            lambda User, addresses: {
                "mapper": User,
                "clause": select(addresses),
            },
            "e1",
        ),
        (
            lambda e2, User, addresses: {
                "mapper": User,
                "clause": select(addresses),
                "bind": e2,
            },
            "e2",
        ),
        (
            lambda User, Address: {
                "clause": select(1).join_from(User, Address)
            },
            "e1",
        ),
        (
            lambda User, Address: {
                "clause": select(1).join_from(Address, User)
            },
            "e2",
        ),
        (
            lambda User: {"clause": select(1).where(User.name == "ed")},
            "e1",
        ),
        (lambda: {"clause": select(1)}, "e3"),
        (lambda User: {"clause": Query([User])._statement_20()}, "e1"),
        (lambda: {"clause": Query([1])._statement_20()}, "e3"),
        (
            lambda User: {
                "clause": Query([1]).select_from(User)._statement_20()
            },
            "e1",
        ),
        (
            lambda User: {
                "clause": Query([1])
                .select_from(User)
                .join(User.addresses)
                ._statement_20()
            },
            "e1",
        ),
        (
            # forcing the "onclause" argument to be considered
            # in visitors.iterate()
            lambda User: {
                "clause": Query([1])
                .select_from(User)
                .join(table("foo"), User.addresses)
                ._statement_20()
            },
            "e1",
        ),
        (
            lambda User: {
                "clause": select(1).select_from(User).join(User.addresses)
            },
            "e1",
        ),
    )
    def test_get_bind(self, testcase, expected):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )
        Dingaling = self.classes.Dingaling

        self.mapper_registry.map_imperatively(
            User, users, properties={"addresses": relationship(Address)}
        )
        self.mapper_registry.map_imperatively(Address, addresses)

        users_view = table("users", Column("id", Integer, primary_key=True))
        addresses_view = table(
            "addresses",
            Column("id", Integer, primary_key=True),
            Column("user_id", Integer),
            Column("email_address", String),
        )
        j = users_view.join(
            addresses_view, users_view.c.id == addresses_view.c.user_id
        )
        self.mapper_registry.map_imperatively(
            Dingaling,
            j,
            properties={
                "user_t_id": users_view.c.id,
                "address_id": addresses_view.c.id,
            },
        )

        e1 = engines.testing_engine()
        e2 = engines.testing_engine()
        e3 = engines.testing_engine()
        e4 = engines.testing_engine()

        testcase = testing.resolve_lambda(
            testcase,
            User=User,
            Address=Address,
            Dingaling=Dingaling,
            e1=e1,
            e2=e2,
            e3=e3,
            e4=e4,
            users_view=users_view,
            addresses_view=addresses_view,
            addresses=addresses,
            users=users,
        )

        sess = Session(e3)
        sess.bind_mapper(User, e1)
        sess.bind_mapper(Address, e2)
        sess.bind_mapper(Dingaling, e4)
        sess.bind_table(users_view, e2)

        engine = {"e1": e1, "e2": e2, "e3": e3, "e4": e4}[expected]
        conn = sess.connection(bind_arguments=testcase)
        is_(conn.engine, engine)

        sess.close()

    @testing.combinations(True, False)
    def test_dont_mutate_binds(self, empty_dict):
        users, User = (
            self.tables.users,
            self.classes.User,
        )

        mp = self.mapper_registry.map_imperatively(User, users)

        sess = fixture_session()

        if empty_dict:
            bind_arguments = {}
        else:
            bind_arguments = {"mapper": mp}
        sess.execute(select(1), bind_arguments=bind_arguments)

        if empty_dict:
            eq_(bind_arguments, {})
        else:
            eq_(bind_arguments, {"mapper": mp})

    @testing.combinations(
        (
            lambda session, Address: session.query(Address).statement,
            lambda Address: {"mapper": inspect(Address), "clause": mock.ANY},
            "e2",
        ),
        (lambda: select(1), lambda: {"clause": mock.ANY}, "e3"),
        (
            lambda User, Address: select(1).join_from(User, Address),
            lambda User: {"clause": mock.ANY, "mapper": inspect(User)},
            "e1",
        ),
        (
            lambda User, Address: select(1).join_from(Address, User),
            lambda Address: {"clause": mock.ANY, "mapper": inspect(Address)},
            "e2",
        ),
        (
            lambda User: select(1).where(User.name == "ed"),
            # no mapper for this one because the plugin is not "orm"
            lambda User: {"clause": mock.ANY},
            "e1",
        ),
        (
            lambda User: select(1).select_from(User).where(User.name == "ed"),
            lambda User: {"clause": mock.ANY, "mapper": inspect(User)},
            "e1",
        ),
        (
            lambda User: select(User.id),
            lambda User: {"clause": mock.ANY, "mapper": inspect(User)},
            "e1",
        ),
        (
            lambda User: update(User)
            .execution_options(synchronize_session=False)
            .values(name="not ed")
            .where(User.name == "ed"),
            lambda User: {"clause": mock.ANY, "mapper": inspect(User)},
            "e1",
        ),
        (
            lambda User: insert(User).values(name="not ed"),
            lambda User: {
                "clause": mock.ANY,
                "mapper": inspect(User),
            },
            "e1",
        ),
    )
    def test_bind_through_execute(
        self, statement, expected_get_bind_args, expected_engine_name
    ):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(
            User, users, properties={"addresses": relationship(Address)}
        )
        self.mapper_registry.map_imperatively(Address, addresses)

        e1 = engines.testing_engine()
        e2 = engines.testing_engine()
        e3 = engines.testing_engine()

        canary = mock.Mock()

        class GetBindSession(Session):
            def _connection_for_bind(self, bind, **kw):
                canary._connection_for_bind(bind, **kw)
                return mock.Mock()

            def get_bind(self, **kw):
                canary.get_bind(**kw)
                return Session.get_bind(self, **kw)

        sess = GetBindSession(e3)
        sess.bind_mapper(User, e1)
        sess.bind_mapper(Address, e2)

        lambda_args = dict(
            session=sess,
            User=User,
            Address=Address,
            e1=e1,
            e2=e2,
            e3=e3,
            addresses=addresses,
        )
        statement = testing.resolve_lambda(statement, **lambda_args)

        expected_get_bind_args = testing.resolve_lambda(
            expected_get_bind_args, **lambda_args
        )

        engine = {"e1": e1, "e2": e2, "e3": e3}[expected_engine_name]

        with mock.patch(
            "sqlalchemy.orm.context." "ORMCompileState.orm_setup_cursor_result"
        ), mock.patch(
            "sqlalchemy.orm.context.ORMCompileState.orm_execute_statement"
        ), mock.patch(
            "sqlalchemy.orm.bulk_persistence."
            "BulkORMInsert.orm_execute_statement"
        ), mock.patch(
            "sqlalchemy.orm.bulk_persistence."
            "BulkUDCompileState.orm_setup_cursor_result"
        ):
            sess.execute(statement)

        eq_(
            canary.mock_calls,
            [
                mock.call.get_bind(**expected_get_bind_args),
                mock.call._connection_for_bind(engine),
            ],
        )
        sess.close()

    def test_bind_arg(self):
        sess = fixture_session()

        assert_raises_message(
            sa.exc.ArgumentError,
            "Not an acceptable bind target: foobar",
            sess.bind_mapper,
            "foobar",
            testing.db,
        )

        self.mapper_registry.map_imperatively(
            self.classes.User, self.tables.users
        )
        u_object = self.classes.User()

        assert_raises_message(
            sa.exc.ArgumentError,
            "Not an acceptable bind target: User()",
            sess.bind_mapper,
            u_object,
            testing.db,
        )

    @engines.close_open_connections
    def test_bound_connection(self):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)
        c = testing.db.connect()
        sess = Session(bind=c)
        sess.begin()
        transaction = sess.get_transaction()
        u = User(name="u1")
        sess.add(u)
        sess.flush()
        assert (
            transaction._connection_for_bind(testing.db, None)
            is transaction._connection_for_bind(c, None)
            is c
        )

        assert_raises_message(
            sa.exc.InvalidRequestError,
            "Session already has a Connection " "associated",
            transaction._connection_for_bind,
            testing.db.connect(),
            None,
        )
        transaction.rollback()
        assert len(sess.query(User).all()) == 0
        sess.close()

    def test_bound_connection_transactional(self):
        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(User, users)
        with testing.db.connect() as c:

            sess = Session(bind=c)

            u = User(name="u1")
            sess.add(u)
            sess.flush()

            # new in 2.0:
            # autobegin occurred, so c is in a transaction.

            assert c.in_transaction()
            sess.close()

            # .close() does a rollback, so that will end the
            # transaction on the connection.  This is how it was
            # working before also even if transaction was started.
            # is this what we really want?
            assert not c.in_transaction()

            assert (
                c.exec_driver_sql("select count(1) from users").scalar() == 0
            )

            sess = Session(bind=c)
            u = User(name="u2")
            sess.add(u)
            sess.flush()
            sess.commit()

            # new in 2.0:
            # commit OTOH doesn't actually do a commit.
            # so still in transaction due to autobegin
            assert c.in_transaction()

            sess = Session(bind=c)
            u = User(name="u3")
            sess.add(u)
            sess.flush()
            sess.rollback()

            # like .close(), rollback() also ends the transaction
            assert not c.in_transaction()

            assert (
                c.exec_driver_sql("select count(1) from users").scalar() == 0
            )


class SessionBindTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "test_table",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", Integer),
        )

    @classmethod
    def setup_classes(cls):
        class Foo(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        test_table, Foo = cls.tables.test_table, cls.classes.Foo

        meta = MetaData()
        test_table.to_metadata(meta)

        cls.mapper_registry.map_imperatively(Foo, meta.tables["test_table"])

    def test_session_bind(self):
        Foo = self.classes.Foo

        engine = testing.db

        for bind in (engine, engine.connect()):
            try:
                sess = Session(bind=bind)
                assert sess.bind is bind
                f = Foo()
                sess.add(f)
                sess.flush()
                assert sess.get(Foo, f.id) is f
            finally:
                if hasattr(bind, "close"):
                    bind.close()
                sess.close()

    def test_session_unbound(self):
        Foo = self.classes.Foo

        sess = Session()
        sess.add(Foo())
        assert_raises_message(
            sa.exc.UnboundExecutionError,
            (
                "Could not locate a bind configured on Mapper|Foo|test_table "
                "or this Session"
            ),
            sess.flush,
        )


class GetBindTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table("base_table", metadata, Column("id", Integer, primary_key=True))
        Table(
            "w_mixin_table", metadata, Column("id", Integer, primary_key=True)
        )
        Table(
            "joined_sub_table",
            metadata,
            Column("id", ForeignKey("base_table.id"), primary_key=True),
        )
        Table(
            "concrete_sub_table",
            metadata,
            Column("id", Integer, primary_key=True),
        )

    @classmethod
    def setup_classes(cls):
        class MixinOne(cls.Basic):
            pass

        class BaseClass(cls.Basic):
            pass

        class ClassWMixin(MixinOne, cls.Basic):
            pass

        class JoinedSubClass(BaseClass):
            pass

        class ConcreteSubClass(BaseClass):
            pass

    @classmethod
    def setup_mappers(cls):
        cls.mapper_registry.map_imperatively(
            cls.classes.ClassWMixin, cls.tables.w_mixin_table
        )
        cls.mapper_registry.map_imperatively(
            cls.classes.BaseClass, cls.tables.base_table
        )
        cls.mapper_registry.map_imperatively(
            cls.classes.JoinedSubClass,
            cls.tables.joined_sub_table,
            inherits=cls.classes.BaseClass,
        )
        cls.mapper_registry.map_imperatively(
            cls.classes.ConcreteSubClass,
            cls.tables.concrete_sub_table,
            inherits=cls.classes.BaseClass,
            concrete=True,
        )

    def _fixture(self, binds):
        return Session(binds=binds)

    def test_bind_base_table_base_class(self):
        base_class_bind = Mock()
        session = self._fixture({self.tables.base_table: base_class_bind})

        is_(session.get_bind(self.classes.BaseClass), base_class_bind)

    def test_bind_base_table_joined_sub_class(self):
        base_class_bind = Mock()
        session = self._fixture({self.tables.base_table: base_class_bind})

        is_(session.get_bind(self.classes.BaseClass), base_class_bind)
        is_(session.get_bind(self.classes.JoinedSubClass), base_class_bind)

    def test_bind_joined_sub_table_joined_sub_class(self):
        base_class_bind = Mock(name="base")
        joined_class_bind = Mock(name="joined")
        session = self._fixture(
            {
                self.tables.base_table: base_class_bind,
                self.tables.joined_sub_table: joined_class_bind,
            }
        )

        is_(session.get_bind(self.classes.BaseClass), base_class_bind)
        # joined table inheritance has to query based on the base
        # table, so this is what we expect
        is_(session.get_bind(self.classes.JoinedSubClass), base_class_bind)

    def test_fallback_table_metadata(self):
        session = self._fixture({})
        assert_raises_message(
            sa.exc.UnboundExecutionError,
            "Could not locate a bind configured on mapper ",
            session.get_bind,
            self.classes.BaseClass,
        )

    def test_bind_base_table_concrete_sub_class(self):
        base_class_bind = Mock()
        session = self._fixture({self.tables.base_table: base_class_bind})

        assert_raises_message(
            sa.exc.UnboundExecutionError,
            "Could not locate a bind configured on mapper ",
            session.get_bind,
            self.classes.ConcreteSubClass,
        )

    def test_bind_sub_table_concrete_sub_class(self):
        base_class_bind = Mock(name="base")
        concrete_sub_bind = Mock(name="concrete")

        session = self._fixture(
            {
                self.tables.base_table: base_class_bind,
                self.tables.concrete_sub_table: concrete_sub_bind,
            }
        )

        is_(session.get_bind(self.classes.BaseClass), base_class_bind)
        is_(session.get_bind(self.classes.ConcreteSubClass), concrete_sub_bind)

    def test_bind_base_class_base_class(self):
        base_class_bind = Mock()
        session = self._fixture({self.classes.BaseClass: base_class_bind})

        is_(session.get_bind(self.classes.BaseClass), base_class_bind)

    def test_bind_mixin_class_simple_class(self):
        base_class_bind = Mock()
        session = self._fixture({self.classes.MixinOne: base_class_bind})

        is_(session.get_bind(self.classes.ClassWMixin), base_class_bind)

    def test_bind_base_class_joined_sub_class(self):
        base_class_bind = Mock()
        session = self._fixture({self.classes.BaseClass: base_class_bind})

        is_(session.get_bind(self.classes.JoinedSubClass), base_class_bind)

    def test_bind_joined_sub_class_joined_sub_class(self):
        base_class_bind = Mock(name="base")
        joined_class_bind = Mock(name="joined")
        session = self._fixture(
            {
                self.classes.BaseClass: base_class_bind,
                self.classes.JoinedSubClass: joined_class_bind,
            }
        )

        is_(session.get_bind(self.classes.BaseClass), base_class_bind)
        is_(session.get_bind(self.classes.JoinedSubClass), joined_class_bind)

    def test_bind_base_class_concrete_sub_class(self):
        base_class_bind = Mock()
        session = self._fixture({self.classes.BaseClass: base_class_bind})

        is_(session.get_bind(self.classes.ConcreteSubClass), base_class_bind)

    def test_bind_sub_class_concrete_sub_class(self):
        base_class_bind = Mock(name="base")
        concrete_sub_bind = Mock(name="concrete")

        session = self._fixture(
            {
                self.classes.BaseClass: base_class_bind,
                self.classes.ConcreteSubClass: concrete_sub_bind,
            }
        )

        is_(session.get_bind(self.classes.BaseClass), base_class_bind)
        is_(session.get_bind(self.classes.ConcreteSubClass), concrete_sub_bind)

    @testing.fixture
    def two_table_fixture(self):
        base_class_bind = Mock(name="base")
        concrete_sub_bind = Mock(name="concrete")

        session = self._fixture(
            {
                self.tables.base_table: base_class_bind,
                self.tables.concrete_sub_table: concrete_sub_bind,
            }
        )
        return session, base_class_bind, concrete_sub_bind

    def test_bind_selectable_table(self, two_table_fixture):
        session, base_class_bind, concrete_sub_bind = two_table_fixture

        is_(session.get_bind(clause=self.tables.base_table), base_class_bind)
        is_(
            session.get_bind(clause=self.tables.concrete_sub_table),
            concrete_sub_bind,
        )

    def test_bind_selectable_join(self, two_table_fixture):
        session, base_class_bind, concrete_sub_bind = two_table_fixture

        stmt = self.tables.base_table.join(
            self.tables.concrete_sub_table, true()
        )
        is_(session.get_bind(clause=stmt), base_class_bind)

    def test_bind_selectable_union(self, two_table_fixture):
        session, base_class_bind, concrete_sub_bind = two_table_fixture

        stmt = select(self.tables.base_table).union(
            select(self.tables.concrete_sub_table)
        )
        is_(session.get_bind(clause=stmt), base_class_bind)

    @testing.combinations(
        (insert,),
        (update,),
        (delete,),
        (select,),
    )
    def test_clause_extracts_orm_plugin_subject(self, sql_elem):
        ClassWMixin = self.classes.ClassWMixin
        MixinOne = self.classes.MixinOne
        base_class_bind = Mock()

        session = self._fixture({MixinOne: base_class_bind})

        stmt = sql_elem(ClassWMixin)
        is_(session.get_bind(clause=stmt), base_class_bind)

        cwm_alias = aliased(ClassWMixin)
        stmt = sql_elem(cwm_alias)
        is_(session.get_bind(clause=stmt), base_class_bind)
