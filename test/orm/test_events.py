import sqlalchemy as sa
from sqlalchemy import delete
from sqlalchemy import event
from sqlalchemy import ForeignKey
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import literal_column
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import update
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import attributes
from sqlalchemy.orm import class_mapper
from sqlalchemy.orm import configure_mappers
from sqlalchemy.orm import deferred
from sqlalchemy.orm import events
from sqlalchemy.orm import EXT_SKIP
from sqlalchemy.orm import instrumentation
from sqlalchemy.orm import Mapper
from sqlalchemy.orm import mapper
from sqlalchemy.orm import mapperlib
from sqlalchemy.orm import query
from sqlalchemy.orm import relationship
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import subqueryload
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_warnings
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_not
from sqlalchemy.testing.assertsql import CompiledSQL
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.mock import ANY
from sqlalchemy.testing.mock import call
from sqlalchemy.testing.mock import Mock
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from sqlalchemy.testing.util import gc_collect
from test.orm import _fixtures


class _RemoveListeners(object):
    @testing.fixture(autouse=True)
    def _remove_listeners(self):
        yield
        events.MapperEvents._clear()
        events.InstanceEvents._clear()
        events.SessionEvents._clear()
        events.InstrumentationEvents._clear()
        events.QueryEvents._clear()


class ORMExecuteTest(_RemoveListeners, _fixtures.FixtureTest):
    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()

    def _caching_session_fixture(self):

        cache = {}

        maker = sessionmaker(testing.db, future=True)

        def get_value(cache_key, cache, createfunc):
            if cache_key in cache:
                return cache[cache_key]()
            else:
                cache[cache_key] = retval = createfunc().freeze()
                return retval()

        @event.listens_for(maker, "do_orm_execute", retval=True)
        def do_orm_execute(orm_context):
            ckey = None
            for opt in orm_context.user_defined_options:
                ckey = opt.get_cache_key(orm_context)
                if ckey:
                    break
            else:
                if "cache_key" in orm_context.execution_options:
                    ckey = orm_context.execution_options["cache_key"]

            if ckey is not None:
                return get_value(
                    ckey,
                    cache,
                    orm_context.invoke_statement,
                )

        return maker()

    def test_cache_option(self):
        User, Address = self.classes("User", "Address")

        with self.sql_execution_asserter(testing.db) as asserter:

            with self._caching_session_fixture() as session:
                stmt = (
                    select(User)
                    .where(User.id == 7)
                    .execution_options(cache_key="user7")
                )

                result = session.execute(stmt)

                eq_(
                    result.scalars().all(),
                    [User(id=7, addresses=[Address(id=1)])],
                )

                result = session.execute(stmt)

                eq_(
                    result.scalars().all(),
                    [User(id=7, addresses=[Address(id=1)])],
                )

        asserter.assert_(
            CompiledSQL(
                "SELECT users.id, users.name FROM users "
                "WHERE users.id = :id_1",
                [{"id_1": 7}],
            ),
            CompiledSQL(
                "SELECT addresses.id AS addresses_id, addresses.user_id AS "
                "addresses_user_id, "
                "addresses.email_address AS addresses_email_address "
                "FROM addresses WHERE :param_1 = addresses.user_id "
                "ORDER BY addresses.id",
                [{"param_1": 7}],
            ),
        )

    def test_chained_events_one(self):

        sess = Session(testing.db, future=True)

        @event.listens_for(sess, "do_orm_execute")
        def one(ctx):
            ctx.update_execution_options(one=True)

        @event.listens_for(sess, "do_orm_execute")
        def two(ctx):
            ctx.update_execution_options(two=True)

        @event.listens_for(sess, "do_orm_execute")
        def three(ctx):
            ctx.update_execution_options(three=True)

        @event.listens_for(sess, "do_orm_execute")
        def four(ctx):
            ctx.update_execution_options(four=True)

        result = sess.execute(select(literal_column("1")))

        eq_(
            result.context.execution_options,
            {
                "four": True,
                "future_result": True,
                "one": True,
                "three": True,
                "two": True,
            },
        )

    def _flag_fixture(self, session):
        canary = Mock()

        @event.listens_for(session, "do_orm_execute")
        def do_orm_execute(ctx):

            if not ctx.is_select:
                assert_raises_message(
                    sa.exc.InvalidRequestError,
                    "This ORM execution is not against a SELECT statement",
                    lambda: ctx.lazy_loaded_from,
                )

            canary.options(
                bind_mapper=ctx.bind_mapper,
                all_mappers=ctx.all_mappers,
                is_select=ctx.is_select,
                is_update=ctx.is_update,
                is_delete=ctx.is_delete,
                is_orm_statement=ctx.is_orm_statement,
                is_relationship_load=ctx.is_relationship_load,
                is_column_load=ctx.is_column_load,
                lazy_loaded_from=ctx.lazy_loaded_from
                if ctx.is_select
                else None,
            )

        return canary

    def test_all_mappers_accessor_one(self):
        User, Address = self.classes("User", "Address")

        sess = Session(testing.db, future=True)

        canary = self._flag_fixture(sess)

        sess.execute(
            select(User.id, Address.email_address, User.name)
            .join(Address)
            .filter_by(id=7)
        )

        eq_(
            canary.mock_calls,
            [
                call.options(
                    bind_mapper=inspect(User),
                    all_mappers=[inspect(User), inspect(Address)],
                    is_select=True,
                    is_update=False,
                    is_delete=False,
                    is_orm_statement=True,
                    is_relationship_load=False,
                    is_column_load=False,
                    lazy_loaded_from=None,
                )
            ],
        )

    def test_all_mappers_accessor_two(self):

        sess = Session(testing.db, future=True)

        canary = self._flag_fixture(sess)

        sess.execute(select(self.tables.users).filter_by(id=7))

        eq_(
            canary.mock_calls,
            [
                call.options(
                    bind_mapper=None,
                    all_mappers=[],
                    is_select=True,
                    is_update=False,
                    is_delete=False,
                    is_orm_statement=False,
                    is_relationship_load=False,
                    is_column_load=False,
                    lazy_loaded_from=None,
                )
            ],
        )

    def test_all_mappers_accessor_three(self):
        User, Address = self.classes("User", "Address")

        sess = Session(testing.db, future=True)

        canary = self._flag_fixture(sess)

        sess.execute(select(User).join(Address).filter_by(id=7))

        eq_(
            canary.mock_calls,
            [
                call.options(
                    bind_mapper=inspect(User),
                    all_mappers=[inspect(User)],  # Address not in results
                    is_select=True,
                    is_update=False,
                    is_delete=False,
                    is_orm_statement=True,
                    is_relationship_load=False,
                    is_column_load=False,
                    lazy_loaded_from=None,
                )
            ],
        )

    def test_select_flags(self):
        User, Address = self.classes("User", "Address")

        sess = Session(testing.db, future=True)

        canary = self._flag_fixture(sess)

        u1 = sess.execute(select(User).filter_by(id=7)).scalar_one()

        sess.expire(u1)

        eq_(u1.name, "jack")

        eq_(
            canary.mock_calls,
            [
                call.options(
                    bind_mapper=inspect(User),
                    all_mappers=[inspect(User)],
                    is_select=True,
                    is_update=False,
                    is_delete=False,
                    is_orm_statement=True,
                    is_relationship_load=False,
                    is_column_load=False,
                    lazy_loaded_from=None,
                ),
                call.options(
                    bind_mapper=inspect(User),
                    all_mappers=[inspect(User)],
                    is_select=True,
                    is_update=False,
                    is_delete=False,
                    is_orm_statement=True,
                    is_relationship_load=False,
                    is_column_load=True,
                    lazy_loaded_from=None,
                ),
            ],
        )

    def test_lazyload_flags(self):
        User, Address = self.classes("User", "Address")

        sess = Session(testing.db, future=True)

        canary = self._flag_fixture(sess)

        u1 = sess.execute(select(User).filter_by(id=7)).scalar_one()

        u1.addresses

        eq_(
            canary.mock_calls,
            [
                call.options(
                    bind_mapper=inspect(User),
                    all_mappers=[inspect(User)],
                    is_select=True,
                    is_update=False,
                    is_delete=False,
                    is_orm_statement=True,
                    is_relationship_load=False,
                    is_column_load=False,
                    lazy_loaded_from=None,
                ),
                call.options(
                    bind_mapper=inspect(Address),
                    all_mappers=[inspect(Address)],
                    is_select=True,
                    is_update=False,
                    is_delete=False,
                    is_orm_statement=True,
                    is_relationship_load=True,
                    is_column_load=False,
                    lazy_loaded_from=u1._sa_instance_state,
                ),
            ],
        )

    def test_selectinload_flags(self):
        User, Address = self.classes("User", "Address")

        sess = Session(testing.db, future=True)

        canary = self._flag_fixture(sess)

        u1 = sess.execute(
            select(User).filter_by(id=7).options(selectinload(User.addresses))
        ).scalar_one()

        assert "addresses" in u1.__dict__

        eq_(
            canary.mock_calls,
            [
                call.options(
                    bind_mapper=inspect(User),
                    all_mappers=[inspect(User)],
                    is_select=True,
                    is_update=False,
                    is_delete=False,
                    is_orm_statement=True,
                    is_relationship_load=False,
                    is_column_load=False,
                    lazy_loaded_from=None,
                ),
                call.options(
                    bind_mapper=inspect(Address),
                    all_mappers=[inspect(Address)],
                    is_select=True,
                    is_update=False,
                    is_delete=False,
                    is_orm_statement=True,
                    is_relationship_load=True,
                    is_column_load=False,
                    lazy_loaded_from=None,
                ),
            ],
        )

    def test_subqueryload_flags(self):
        User, Address = self.classes("User", "Address")

        sess = Session(testing.db, future=True)

        canary = self._flag_fixture(sess)

        u1 = sess.execute(
            select(User).filter_by(id=7).options(subqueryload(User.addresses))
        ).scalar_one()

        assert "addresses" in u1.__dict__

        eq_(
            canary.mock_calls,
            [
                call.options(
                    bind_mapper=inspect(User),
                    all_mappers=[inspect(User)],
                    is_select=True,
                    is_update=False,
                    is_delete=False,
                    is_orm_statement=True,
                    is_relationship_load=False,
                    is_column_load=False,
                    lazy_loaded_from=None,
                ),
                call.options(
                    bind_mapper=inspect(Address),
                    all_mappers=[inspect(Address), inspect(User)],
                    is_select=True,
                    is_update=False,
                    is_delete=False,
                    is_orm_statement=True,
                    is_relationship_load=True,
                    is_column_load=False,
                    lazy_loaded_from=None,
                ),
            ],
        )

    def test_update_delete_flags(self):
        User, Address = self.classes("User", "Address")

        sess = Session(testing.db, future=True)

        canary = self._flag_fixture(sess)

        sess.execute(delete(User).filter_by(id=18))
        sess.execute(update(User).filter_by(id=18).values(name="eighteen"))

        eq_(
            canary.mock_calls,
            [
                call.options(
                    bind_mapper=inspect(User),
                    all_mappers=[inspect(User)],
                    is_select=False,
                    is_update=False,
                    is_delete=True,
                    is_orm_statement=True,
                    is_relationship_load=False,
                    is_column_load=False,
                    lazy_loaded_from=None,
                ),
                call.options(
                    bind_mapper=inspect(User),
                    all_mappers=[inspect(User)],
                    is_select=False,
                    is_update=True,
                    is_delete=False,
                    is_orm_statement=True,
                    is_relationship_load=False,
                    is_column_load=False,
                    lazy_loaded_from=None,
                ),
            ],
        )

    def test_chained_events_two(self):

        sess = Session(testing.db, future=True)

        def added(ctx):
            ctx.update_execution_options(added_evt=True)

        @event.listens_for(sess, "do_orm_execute")
        def one(ctx):
            ctx.update_execution_options(one=True)

        @event.listens_for(sess, "do_orm_execute", retval=True)
        def two(ctx):
            ctx.update_execution_options(two=True)
            return ctx.invoke_statement(
                statement=ctx.statement.execution_options(statement_two=True)
            )

        @event.listens_for(sess, "do_orm_execute")
        def three(ctx):
            ctx.update_execution_options(three=True)

        @event.listens_for(sess, "do_orm_execute")
        def four(ctx):
            ctx.update_execution_options(four=True)
            return ctx.invoke_statement(
                statement=ctx.statement.execution_options(statement_four=True)
            )

        @event.listens_for(sess, "do_orm_execute")
        def five(ctx):
            ctx.update_execution_options(five=True)

        result = sess.execute(select(literal_column("1")), _add_event=added)

        eq_(
            result.context.execution_options,
            {
                "statement_two": True,
                "statement_four": True,
                "future_result": True,
                "one": True,
                "two": True,
                "three": True,
                "four": True,
                "five": True,
                "added_evt": True,
            },
        )


class MapperEventsTest(_RemoveListeners, _fixtures.FixtureTest):
    run_inserts = None

    @classmethod
    def define_tables(cls, metadata):
        super(MapperEventsTest, cls).define_tables(metadata)
        metadata.tables["users"].append_column(
            Column("extra", Integer, default=5, onupdate=10)
        )

    def test_instance_event_listen(self):
        """test listen targets for instance events"""

        users, addresses = self.tables.users, self.tables.addresses

        canary = []

        class A(object):
            pass

        class B(A):
            pass

        mapper(A, users)
        mapper(
            B, addresses, inherits=A, properties={"address_id": addresses.c.id}
        )

        def init_a(target, args, kwargs):
            canary.append(("init_a", target))

        def init_b(target, args, kwargs):
            canary.append(("init_b", target))

        def init_c(target, args, kwargs):
            canary.append(("init_c", target))

        def init_d(target, args, kwargs):
            canary.append(("init_d", target))

        def init_e(target, args, kwargs):
            canary.append(("init_e", target))

        event.listen(mapper, "init", init_a)
        event.listen(Mapper, "init", init_b)
        event.listen(class_mapper(A), "init", init_c)
        event.listen(A, "init", init_d)
        event.listen(A, "init", init_e, propagate=True)

        a = A()
        eq_(
            canary,
            [
                ("init_a", a),
                ("init_b", a),
                ("init_c", a),
                ("init_d", a),
                ("init_e", a),
            ],
        )

        # test propagate flag
        canary[:] = []
        b = B()
        eq_(canary, [("init_a", b), ("init_b", b), ("init_e", b)])

    def listen_all(self, mapper, **kw):
        canary = []

        def evt(meth):
            def go(*args, **kwargs):
                canary.append(meth)

            return go

        for meth in [
            "init",
            "init_failure",
            "load",
            "refresh",
            "refresh_flush",
            "expire",
            "before_insert",
            "after_insert",
            "before_update",
            "after_update",
            "before_delete",
            "after_delete",
        ]:
            event.listen(mapper, meth, evt(meth), **kw)
        return canary

    def test_init_allow_kw_modify(self):
        User, users = self.classes.User, self.tables.users
        mapper(User, users)

        @event.listens_for(User, "init")
        def add_name(obj, args, kwargs):
            kwargs["name"] = "ed"

        u1 = User()
        eq_(u1.name, "ed")

    def test_init_failure_hook(self):
        users = self.tables.users

        class Thing(object):
            def __init__(self, **kw):
                if kw.get("fail"):
                    raise Exception("failure")

        mapper(Thing, users)

        canary = Mock()
        event.listen(Thing, "init_failure", canary)

        Thing()
        eq_(canary.mock_calls, [])

        assert_raises_message(Exception, "failure", Thing, fail=True)
        eq_(canary.mock_calls, [call(ANY, (), {"fail": True})])

    def test_listen_doesnt_force_compile(self):
        User, users = self.classes.User, self.tables.users
        m = mapper(
            User,
            users,
            properties={
                # intentionally non-existent class to ensure
                # the lambda is not called, simulates a class from
                # a not-yet-imported module
                "addresses": relationship(lambda: ImNotAClass)  # noqa
            },
        )
        event.listen(User, "before_insert", lambda *a, **kw: None)
        assert not m.configured

    def test_basic(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users)
        canary = self.listen_all(User)
        named_canary = self.listen_all(User, named=True)

        sess = fixture_session()
        u = User(name="u1")
        sess.add(u)
        sess.flush()
        sess.expire(u)
        u = sess.query(User).get(u.id)
        sess.expunge_all()
        u = sess.query(User).get(u.id)
        u.name = "u1 changed"
        sess.flush()
        sess.delete(u)
        sess.flush()
        expected = [
            "init",
            "before_insert",
            "refresh_flush",
            "after_insert",
            "expire",
            "refresh",
            "load",
            "before_update",
            "refresh_flush",
            "after_update",
            "before_delete",
            "after_delete",
        ]
        eq_(canary, expected)
        eq_(named_canary, expected)

    def test_insert_before_configured(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        canary = Mock()

        event.listen(mapper, "before_configured", canary.listen1)
        event.listen(mapper, "before_configured", canary.listen2, insert=True)
        event.listen(mapper, "before_configured", canary.listen3)
        event.listen(mapper, "before_configured", canary.listen4, insert=True)

        configure_mappers()

        eq_(
            canary.mock_calls,
            [call.listen4(), call.listen2(), call.listen1(), call.listen3()],
        )

    def test_insert_flags(self):
        users, User = self.tables.users, self.classes.User

        m = mapper(User, users)

        canary = Mock()

        arg = Mock()

        event.listen(m, "before_insert", canary.listen1)
        event.listen(m, "before_insert", canary.listen2, insert=True)
        event.listen(
            m, "before_insert", canary.listen3, propagate=True, insert=True
        )
        event.listen(m, "load", canary.listen4)
        event.listen(m, "load", canary.listen5, insert=True)
        event.listen(m, "load", canary.listen6, propagate=True, insert=True)

        User()
        m.dispatch.before_insert(arg, arg, arg)
        m.class_manager.dispatch.load(arg, arg)
        eq_(
            canary.mock_calls,
            [
                call.listen3(arg, arg, arg.obj()),
                call.listen2(arg, arg, arg.obj()),
                call.listen1(arg, arg, arg.obj()),
                call.listen6(arg.obj(), arg),
                call.listen5(arg.obj(), arg),
                call.listen4(arg.obj(), arg),
            ],
        )

    def test_merge(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        canary = []

        def load(obj, ctx):
            canary.append("load")

        event.listen(mapper, "load", load)

        s = fixture_session()
        u = User(name="u1")
        s.add(u)
        s.commit()
        s = fixture_session()
        u2 = s.merge(u)
        s = fixture_session()
        u2 = s.merge(User(name="u2"))  # noqa
        s.commit()
        s.query(User).order_by(User.id).first()
        eq_(canary, ["load", "load", "load"])

    def test_inheritance(self):
        users, addresses, User = (
            self.tables.users,
            self.tables.addresses,
            self.classes.User,
        )

        class AdminUser(User):
            pass

        mapper(User, users)
        mapper(
            AdminUser,
            addresses,
            inherits=User,
            properties={"address_id": addresses.c.id},
        )

        canary1 = self.listen_all(User, propagate=True)
        canary2 = self.listen_all(User)
        canary3 = self.listen_all(AdminUser)

        sess = fixture_session()
        am = AdminUser(name="au1", email_address="au1@e1")
        sess.add(am)
        sess.flush()
        am = sess.query(AdminUser).populate_existing().get(am.id)
        sess.expunge_all()
        am = sess.query(AdminUser).get(am.id)
        am.name = "au1 changed"
        sess.flush()
        sess.delete(am)
        sess.flush()
        eq_(
            canary1,
            [
                "init",
                "before_insert",
                "refresh_flush",
                "after_insert",
                "refresh",
                "load",
                "before_update",
                "refresh_flush",
                "after_update",
                "before_delete",
                "after_delete",
            ],
        )
        eq_(canary2, [])
        eq_(
            canary3,
            [
                "init",
                "before_insert",
                "refresh_flush",
                "after_insert",
                "refresh",
                "load",
                "before_update",
                "refresh_flush",
                "after_update",
                "before_delete",
                "after_delete",
            ],
        )

    def test_inheritance_subclass_deferred(self):
        users, addresses, User = (
            self.tables.users,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(User, users)

        canary1 = self.listen_all(User, propagate=True)
        canary2 = self.listen_all(User)

        class AdminUser(User):
            pass

        mapper(
            AdminUser,
            addresses,
            inherits=User,
            properties={"address_id": addresses.c.id},
        )
        canary3 = self.listen_all(AdminUser)

        sess = fixture_session()
        am = AdminUser(name="au1", email_address="au1@e1")
        sess.add(am)
        sess.flush()
        am = sess.query(AdminUser).populate_existing().get(am.id)
        sess.expunge_all()
        am = sess.query(AdminUser).get(am.id)
        am.name = "au1 changed"
        sess.flush()
        sess.delete(am)
        sess.flush()
        eq_(
            canary1,
            [
                "init",
                "before_insert",
                "refresh_flush",
                "after_insert",
                "refresh",
                "load",
                "before_update",
                "refresh_flush",
                "after_update",
                "before_delete",
                "after_delete",
            ],
        )
        eq_(canary2, [])
        eq_(
            canary3,
            [
                "init",
                "before_insert",
                "refresh_flush",
                "after_insert",
                "refresh",
                "load",
                "before_update",
                "refresh_flush",
                "after_update",
                "before_delete",
                "after_delete",
            ],
        )

    def test_before_after_only_collection(self):
        """before_update is called on parent for collection modifications,
        after_update is called even if no columns were updated.

        """

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

        canary1 = self.listen_all(Item)
        canary2 = self.listen_all(Keyword)

        sess = fixture_session()
        i1 = Item(description="i1")
        k1 = Keyword(name="k1")
        sess.add(i1)
        sess.add(k1)
        sess.flush()
        eq_(canary1, ["init", "before_insert", "after_insert"])
        eq_(canary2, ["init", "before_insert", "after_insert"])

        canary1[:] = []
        canary2[:] = []

        i1.keywords.append(k1)
        sess.flush()
        eq_(canary1, ["before_update", "after_update"])
        eq_(canary2, [])

    def test_before_after_configured_warn_on_non_mapper(self):
        User, users = self.classes.User, self.tables.users

        m1 = Mock()

        mapper(User, users)
        assert_raises_message(
            sa.exc.SAWarning,
            r"before_configured' and 'after_configured' ORM events only "
            r"invoke with the mapper\(\) function or Mapper class as "
            r"the target.",
            event.listen,
            User,
            "before_configured",
            m1,
        )

        assert_raises_message(
            sa.exc.SAWarning,
            r"before_configured' and 'after_configured' ORM events only "
            r"invoke with the mapper\(\) function or Mapper class as "
            r"the target.",
            event.listen,
            User,
            "after_configured",
            m1,
        )

    def test_before_after_configured(self):
        User, users = self.classes.User, self.tables.users

        m1 = Mock()
        m2 = Mock()

        mapper(User, users)

        event.listen(mapper, "before_configured", m1)
        event.listen(mapper, "after_configured", m2)

        s = fixture_session()
        s.query(User)

        eq_(m1.mock_calls, [call()])
        eq_(m2.mock_calls, [call()])

    def test_instrument_event(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        canary = []

        def instrument_class(mapper, cls):
            canary.append(cls)

        event.listen(Mapper, "instrument_class", instrument_class)

        mapper(User, users)
        eq_(canary, [User])
        mapper(Address, addresses)
        eq_(canary, [User, Address])

    def test_instrument_class_precedes_class_instrumentation(self):
        users = self.tables.users

        class MyClass(object):
            pass

        class MySubClass(MyClass):
            pass

        canary = Mock()

        def my_init(self):
            canary.init()

        # mapper level event
        @event.listens_for(mapper, "instrument_class")
        def instrument_class(mp, class_):
            canary.instrument_class(class_)

            class_.__init__ = my_init

        # instrumentationmanager event
        @event.listens_for(object, "class_instrument")
        def class_instrument(class_):
            canary.class_instrument(class_)

        mapper(MyClass, users)

        mapper(MySubClass, inherits=MyClass)

        m1 = MyClass()
        assert attributes.instance_state(m1)

        m2 = MySubClass()
        assert attributes.instance_state(m2)

        eq_(
            [
                call.instrument_class(MyClass),
                call.class_instrument(MyClass),
                call.instrument_class(MySubClass),
                call.class_instrument(MySubClass),
                call.init(),
                call.init(),
            ],
            canary.mock_calls,
        )

    @testing.combinations((True,), (False,), argnames="create_dependency")
    @testing.combinations((True,), (False,), argnames="configure_at_once")
    def test_before_mapper_configured_event(
        self, create_dependency, configure_at_once
    ):
        """Test [ticket:4397].

        This event is intended to allow a specific mapper to be skipped during
        the configure step, by returning a value of
        :attr:`.orm.interfaces.EXT_SKIP` which means the mapper will be skipped
        within this configure run.    The "new mappers" flag will remain set in
        this case and the configure operation will occur again.

        This event, and its return value, make it possible to query one base
        while a different one still needs configuration, which cannot be
        completed at this time.
        """

        User, users = self.classes.User, self.tables.users
        ump = mapper(User, users)

        AnotherBase = declarative_base()

        class Animal(AnotherBase):
            __tablename__ = "animal"
            species = Column(String(30), primary_key=True)
            __mapper_args__ = dict(
                polymorphic_on="species", polymorphic_identity="Animal"
            )
            if create_dependency:
                user_id = Column("user_id", ForeignKey(users.c.id))

        if not configure_at_once:
            # Register the first classes and create their Mappers:
            configure_mappers()

            unconfigured = list(mapperlib._unconfigured_mappers())
            eq_(0, len(unconfigured))

        if create_dependency:
            ump.add_property("animal", relationship(Animal))

        # Declare a subclass, table and mapper, which refers to one that has
        # not been loaded yet (Employer), and therefore cannot be configured:
        class Mammal(Animal):
            nonexistent = relationship("Nonexistent")

        # These new classes should not be configured at this point:
        unconfigured = list(mapperlib._unconfigured_mappers())

        if configure_at_once:
            eq_(3, len(unconfigured))
        else:
            eq_(1, len(unconfigured))

        # Now try to query User, which is internally consistent. This query
        # fails by default because Mammal needs to be configured, and cannot
        # be:
        def probe():
            s = fixture_session()
            s.query(User)

        if create_dependency:
            assert_raises(sa.exc.InvalidRequestError, probe)
        else:
            probe()

        # If we disable configuring mappers while querying, then it succeeds:
        @event.listens_for(
            AnotherBase,
            "before_mapper_configured",
            propagate=True,
            retval=True,
        )
        def disable_configure_mappers(mapper, cls):
            return EXT_SKIP

        probe()


class RestoreLoadContextTest(fixtures.DeclarativeMappedTest):
    @classmethod
    def setup_classes(cls):
        class A(cls.DeclarativeBasic):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)
            unloaded = deferred(Column(String(50)))
            bs = relationship("B", lazy="joined")

        class B(cls.DeclarativeBasic):
            __tablename__ = "b"
            id = Column(Integer, primary_key=True)
            a_id = Column(ForeignKey("a.id"))

    @classmethod
    def insert_data(cls, connection):
        A, B = cls.classes("A", "B")
        s = Session(connection)
        s.add(A(bs=[B(), B(), B()]))
        s.commit()

    def _combinations(fn):
        return testing.combinations(
            (lambda A: A, "load", lambda instance, context: instance.unloaded),
            (
                lambda A: A,
                "refresh",
                lambda instance, context, attrs: instance.unloaded,
            ),
            (
                lambda session: session,
                "loaded_as_persistent",
                lambda session, instance: instance.unloaded
                if instance.__class__.__name__ == "A"
                else None,
            ),
            argnames="target, event_name, fn",
        )(fn)

    def teardown_test(self):
        A = self.classes.A
        A._sa_class_manager.dispatch._clear()

    @_combinations
    def test_warning(self, target, event_name, fn):
        A = self.classes.A
        s = fixture_session()
        target = testing.util.resolve_lambda(target, A=A, session=s)
        event.listen(target, event_name, fn)

        with expect_warnings(
            r"Loading context for \<A at .*\> has changed within a "
            r"load/refresh handler, suggesting a row refresh operation "
            r"took place. "
            r"If this event handler is expected to be emitting row refresh "
            r"operations within an existing load or refresh operation, set "
            r"restore_load_context=True when establishing the listener to "
            r"ensure the context remains unchanged when the event handler "
            r"completes."
        ):
            a1 = s.query(A).all()[0]
            if event_name == "refresh":
                s.refresh(a1)
        # joined eager load didn't continue
        eq_(len(a1.bs), 1)
        s.close()

    @_combinations
    def test_flag_resolves_existing(self, target, event_name, fn):
        A = self.classes.A
        s = fixture_session()
        target = testing.util.resolve_lambda(target, A=A, session=s)

        a1 = s.query(A).all()[0]

        s.expire(a1)
        event.listen(target, event_name, fn, restore_load_context=True)
        s.query(A).all()
        s.close()

    @testing.combinations(
        ("load", lambda instance, context: instance.unloaded),
        (
            "refresh",
            lambda instance, context, attrs: instance.unloaded,
        ),
    )
    def test_flag_resolves_existing_for_subclass(self, event_name, fn):
        Base = declarative_base()

        event.listen(
            Base, event_name, fn, propagate=True, restore_load_context=True
        )

        class A(Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)
            unloaded = deferred(Column(String(50)))

        s = Session(testing.db)

        a1 = s.query(A).all()[0]
        if event_name == "refresh":
            s.refresh(a1)
        s.close()

    @_combinations
    def test_flag_resolves(self, target, event_name, fn):
        A = self.classes.A
        s = fixture_session()
        target = testing.util.resolve_lambda(target, A=A, session=s)
        event.listen(target, event_name, fn, restore_load_context=True)

        a1 = s.query(A).all()[0]
        if event_name == "refresh":
            s.refresh(a1)
        # joined eager load continued
        eq_(len(a1.bs), 3)
        s.close()


class DeclarativeEventListenTest(
    _RemoveListeners, fixtures.DeclarativeMappedTest
):
    run_setup_classes = "each"
    run_deletes = None

    def test_inheritance_propagate_after_config(self):
        # test [ticket:2949]

        class A(self.DeclarativeBasic):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)

        class B(A):
            pass

        listen = Mock()
        event.listen(self.DeclarativeBasic, "load", listen, propagate=True)

        class C(B):
            pass

        m1 = A.__mapper__.class_manager
        m2 = B.__mapper__.class_manager
        m3 = C.__mapper__.class_manager
        a1 = A()
        b1 = B()
        c1 = C()
        m3.dispatch.load(c1._sa_instance_state, "c")
        m2.dispatch.load(b1._sa_instance_state, "b")
        m1.dispatch.load(a1._sa_instance_state, "a")
        eq_(listen.mock_calls, [call(c1, "c"), call(b1, "b"), call(a1, "a")])


class DeferredMapperEventsTest(_RemoveListeners, _fixtures.FixtureTest):

    """ "test event listeners against unmapped classes.

    This incurs special logic.  Note if we ever do the "remove" case,
    it has to get all of these, too.

    """

    run_inserts = None

    def test_deferred_map_event(self):
        """
        1. mapper event listen on class
        2. map class
        3. event fire should receive event

        """
        users, User = (self.tables.users, self.classes.User)

        canary = []

        def evt(x, y, z):
            canary.append(x)

        event.listen(User, "before_insert", evt, raw=True)

        m = mapper(User, users)
        m.dispatch.before_insert(5, 6, 7)
        eq_(canary, [5])

    def test_deferred_map_event_subclass_propagate(self):
        """
        1. mapper event listen on class, w propagate
        2. map only subclass of class
        3. event fire should receive event

        """
        users, User = (self.tables.users, self.classes.User)

        class SubUser(User):
            pass

        class SubSubUser(SubUser):
            pass

        canary = Mock()

        def evt(x, y, z):
            canary.append(x)

        event.listen(User, "before_insert", canary, propagate=True, raw=True)

        m = mapper(SubUser, users)
        m.dispatch.before_insert(5, 6, 7)
        eq_(canary.mock_calls, [call(5, 6, 7)])

        m2 = mapper(SubSubUser, users)

        m2.dispatch.before_insert(8, 9, 10)
        eq_(canary.mock_calls, [call(5, 6, 7), call(8, 9, 10)])

    def test_deferred_map_event_subclass_no_propagate(self):
        """
        1. mapper event listen on class, w/o propagate
        2. map only subclass of class
        3. event fire should not receive event

        """
        users, User = (self.tables.users, self.classes.User)

        class SubUser(User):
            pass

        canary = []

        def evt(x, y, z):
            canary.append(x)

        event.listen(User, "before_insert", evt, propagate=False)

        m = mapper(SubUser, users)
        m.dispatch.before_insert(5, 6, 7)
        eq_(canary, [])

    def test_deferred_map_event_subclass_post_mapping_propagate(self):
        """
        1. map only subclass of class
        2. mapper event listen on class, w propagate
        3. event fire should receive event

        """
        users, User = (self.tables.users, self.classes.User)

        class SubUser(User):
            pass

        m = mapper(SubUser, users)

        canary = []

        def evt(x, y, z):
            canary.append(x)

        event.listen(User, "before_insert", evt, propagate=True, raw=True)

        m.dispatch.before_insert(5, 6, 7)
        eq_(canary, [5])

    def test_deferred_map_event_subclass_post_mapping_propagate_two(self):
        """
        1. map only subclass of class
        2. mapper event listen on class, w propagate
        3. event fire should receive event

        """
        users, User = (self.tables.users, self.classes.User)

        class SubUser(User):
            pass

        class SubSubUser(SubUser):
            pass

        m = mapper(SubUser, users)

        canary = Mock()
        event.listen(User, "before_insert", canary, propagate=True, raw=True)

        m2 = mapper(SubSubUser, users)

        m.dispatch.before_insert(5, 6, 7)
        eq_(canary.mock_calls, [call(5, 6, 7)])

        m2.dispatch.before_insert(8, 9, 10)
        eq_(canary.mock_calls, [call(5, 6, 7), call(8, 9, 10)])

    def test_deferred_instance_event_subclass_post_mapping_propagate(self):
        """
        1. map only subclass of class
        2. instance event listen on class, w propagate
        3. event fire should receive event

        """
        users, User = (self.tables.users, self.classes.User)

        class SubUser(User):
            pass

        m = mapper(SubUser, users)

        canary = []

        def evt(x):
            canary.append(x)

        event.listen(User, "load", evt, propagate=True, raw=True)

        m.class_manager.dispatch.load(5)
        eq_(canary, [5])

    def test_deferred_instance_event_plain(self):
        """
        1. instance event listen on class, w/o propagate
        2. map class
        3. event fire should receive event

        """
        users, User = (self.tables.users, self.classes.User)

        canary = []

        def evt(x):
            canary.append(x)

        event.listen(User, "load", evt, raw=True)

        m = mapper(User, users)
        m.class_manager.dispatch.load(5)
        eq_(canary, [5])

    def test_deferred_instance_event_subclass_propagate_subclass_only(self):
        """
        1. instance event listen on class, w propagate
        2. map two subclasses of class
        3. event fire on each class should receive one and only one event

        """
        users, User = (self.tables.users, self.classes.User)

        class SubUser(User):
            pass

        class SubUser2(User):
            pass

        canary = []

        def evt(x):
            canary.append(x)

        event.listen(User, "load", evt, propagate=True, raw=True)

        m = mapper(SubUser, users)
        m2 = mapper(SubUser2, users)

        m.class_manager.dispatch.load(5)
        eq_(canary, [5])

        m2.class_manager.dispatch.load(5)
        eq_(canary, [5, 5])

    def test_deferred_instance_event_subclass_propagate_baseclass(self):
        """
        1. instance event listen on class, w propagate
        2. map one subclass of class, map base class, leave 2nd subclass
           unmapped
        3. event fire on sub should receive one and only one event
        4. event fire on base should receive one and only one event
        5. map 2nd subclass
        6. event fire on 2nd subclass should receive one and only one event
        """
        users, User = (self.tables.users, self.classes.User)

        class SubUser(User):
            pass

        class SubUser2(User):
            pass

        canary = Mock()
        event.listen(User, "load", canary, propagate=True, raw=False)

        # reversing these fixes....
        m = mapper(SubUser, users)
        m2 = mapper(User, users)

        instance = Mock()
        m.class_manager.dispatch.load(instance)

        eq_(canary.mock_calls, [call(instance.obj())])

        m2.class_manager.dispatch.load(instance)
        eq_(canary.mock_calls, [call(instance.obj()), call(instance.obj())])

        m3 = mapper(SubUser2, users)
        m3.class_manager.dispatch.load(instance)
        eq_(
            canary.mock_calls,
            [call(instance.obj()), call(instance.obj()), call(instance.obj())],
        )

    def test_deferred_instance_event_subclass_no_propagate(self):
        """
        1. instance event listen on class, w/o propagate
        2. map subclass
        3. event fire on subclass should not receive event
        """
        users, User = (self.tables.users, self.classes.User)

        class SubUser(User):
            pass

        canary = []

        def evt(x):
            canary.append(x)

        event.listen(User, "load", evt, propagate=False)

        m = mapper(SubUser, users)
        m.class_manager.dispatch.load(5)
        eq_(canary, [])

    def test_deferred_instrument_event(self):
        User = self.classes.User

        canary = []

        def evt(x):
            canary.append(x)

        event.listen(User, "attribute_instrument", evt)

        instrumentation._instrumentation_factory.dispatch.attribute_instrument(
            User
        )
        eq_(canary, [User])

    def test_isolation_instrument_event(self):
        User = self.classes.User

        class Bar(object):
            pass

        canary = []

        def evt(x):
            canary.append(x)

        event.listen(Bar, "attribute_instrument", evt)

        instrumentation._instrumentation_factory.dispatch.attribute_instrument(
            User
        )
        eq_(canary, [])

    @testing.requires.predictable_gc
    def test_instrument_event_auto_remove(self):
        class Bar(object):
            pass

        dispatch = instrumentation._instrumentation_factory.dispatch
        assert not dispatch.attribute_instrument

        event.listen(Bar, "attribute_instrument", lambda: None)

        eq_(len(dispatch.attribute_instrument), 1)

        del Bar
        gc_collect()

        assert not dispatch.attribute_instrument

    def test_deferred_instrument_event_subclass_propagate(self):
        User = self.classes.User

        class SubUser(User):
            pass

        canary = []

        def evt(x):
            canary.append(x)

        event.listen(User, "attribute_instrument", evt, propagate=True)

        instrumentation._instrumentation_factory.dispatch.attribute_instrument(
            SubUser
        )
        eq_(canary, [SubUser])

    def test_deferred_instrument_event_subclass_no_propagate(self):
        users, User = (self.tables.users, self.classes.User)

        class SubUser(User):
            pass

        canary = []

        def evt(x):
            canary.append(x)

        event.listen(User, "attribute_instrument", evt, propagate=False)

        mapper(SubUser, users)
        instrumentation._instrumentation_factory.dispatch.attribute_instrument(
            5
        )
        eq_(canary, [])


class LoadTest(_fixtures.FixtureTest):
    run_inserts = None

    @classmethod
    def setup_mappers(cls):
        User, users = cls.classes.User, cls.tables.users

        mapper(User, users)

    def _fixture(self):
        User = self.classes.User

        canary = []

        def load(target, ctx):
            canary.append("load")

        def refresh(target, ctx, attrs):
            canary.append(("refresh", attrs))

        event.listen(User, "load", load)
        event.listen(User, "refresh", refresh)
        return canary

    def test_just_loaded(self):
        User = self.classes.User

        canary = self._fixture()

        sess = fixture_session()

        u1 = User(name="u1")
        sess.add(u1)
        sess.commit()
        sess.close()

        sess.query(User).first()
        eq_(canary, ["load"])

    def test_repeated_rows(self):
        User = self.classes.User

        canary = self._fixture()

        sess = fixture_session()

        u1 = User(name="u1")
        sess.add(u1)
        sess.commit()
        sess.close()

        sess.query(User).union_all(sess.query(User)).all()
        eq_(canary, ["load"])


class RemovalTest(_fixtures.FixtureTest):
    run_inserts = None

    def test_attr_propagated(self):
        User = self.classes.User

        users, addresses, User = (
            self.tables.users,
            self.tables.addresses,
            self.classes.User,
        )

        class AdminUser(User):
            pass

        mapper(User, users)
        mapper(
            AdminUser,
            addresses,
            inherits=User,
            properties={"address_id": addresses.c.id},
        )

        fn = Mock()
        event.listen(User.name, "set", fn, propagate=True)

        au = AdminUser()
        au.name = "ed"

        eq_(fn.call_count, 1)

        event.remove(User.name, "set", fn)

        au.name = "jack"

        eq_(fn.call_count, 1)

    def test_unmapped_listen(self):
        users = self.tables.users

        class Foo(object):
            pass

        fn = Mock()

        event.listen(Foo, "before_insert", fn, propagate=True)

        class User(Foo):
            pass

        m = mapper(User, users)

        u1 = User()
        m.dispatch.before_insert(m, None, attributes.instance_state(u1))
        eq_(fn.call_count, 1)

        event.remove(Foo, "before_insert", fn)

        # existing event is removed
        m.dispatch.before_insert(m, None, attributes.instance_state(u1))
        eq_(fn.call_count, 1)

        # the _HoldEvents is also cleaned out
        class Bar(Foo):
            pass

        m = mapper(Bar, users)
        b1 = Bar()
        m.dispatch.before_insert(m, None, attributes.instance_state(b1))
        eq_(fn.call_count, 1)

    def test_instance_event_listen_on_cls_before_map(self):
        users = self.tables.users

        fn = Mock()

        class User(object):
            pass

        event.listen(User, "load", fn)
        m = mapper(User, users)

        u1 = User()
        m.class_manager.dispatch.load(u1._sa_instance_state, "u1")

        event.remove(User, "load", fn)

        m.class_manager.dispatch.load(u1._sa_instance_state, "u2")

        eq_(fn.mock_calls, [call(u1, "u1")])


class RefreshTest(_fixtures.FixtureTest):
    run_inserts = None

    @classmethod
    def setup_mappers(cls):
        User, users = cls.classes.User, cls.tables.users

        mapper(User, users)

    def _fixture(self):
        User = self.classes.User

        canary = []

        def load(target, ctx):
            canary.append("load")

        def refresh(target, ctx, attrs):
            canary.append(("refresh", attrs))

        event.listen(User, "load", load)
        event.listen(User, "refresh", refresh)
        return canary

    def test_already_present(self):
        User = self.classes.User

        canary = self._fixture()

        sess = fixture_session()

        u1 = User(name="u1")
        sess.add(u1)
        sess.flush()

        sess.query(User).first()
        eq_(canary, [])

    def test_changes_reset(self):
        """test the contract of load/refresh such that history is reset.

        This has never been an official contract but we are testing it
        here to ensure it is maintained given the loading performance
        enhancements.

        """
        User = self.classes.User

        @event.listens_for(User, "load")
        def canary1(obj, context):
            obj.name = "new name!"

        @event.listens_for(User, "refresh")
        def canary2(obj, context, props):
            obj.name = "refreshed name!"

        sess = fixture_session()
        u1 = User(name="u1")
        sess.add(u1)
        sess.commit()
        sess.close()

        u1 = sess.query(User).first()
        eq_(attributes.get_history(u1, "name"), ((), ["new name!"], ()))
        assert "name" not in attributes.instance_state(u1).committed_state
        assert u1 not in sess.dirty

        sess.expire(u1)
        u1.id
        eq_(attributes.get_history(u1, "name"), ((), ["refreshed name!"], ()))
        assert "name" not in attributes.instance_state(u1).committed_state
        assert u1 in sess.dirty

    def test_repeated_rows(self):
        User = self.classes.User

        canary = self._fixture()

        sess = fixture_session()

        u1 = User(name="u1")
        sess.add(u1)
        sess.commit()

        sess.query(User).union_all(sess.query(User)).all()
        eq_(canary, [("refresh", set(["id", "name"]))])

    def test_via_refresh_state(self):
        User = self.classes.User

        canary = self._fixture()

        sess = fixture_session()

        u1 = User(name="u1")
        sess.add(u1)
        sess.commit()

        u1.name
        eq_(canary, [("refresh", set(["id", "name"]))])

    def test_was_expired(self):
        User = self.classes.User

        canary = self._fixture()

        sess = fixture_session()

        u1 = User(name="u1")
        sess.add(u1)
        sess.flush()
        sess.expire(u1)

        sess.query(User).first()
        eq_(canary, [("refresh", set(["id", "name"]))])

    def test_was_expired_via_commit(self):
        User = self.classes.User

        canary = self._fixture()

        sess = fixture_session()

        u1 = User(name="u1")
        sess.add(u1)
        sess.commit()

        sess.query(User).first()
        eq_(canary, [("refresh", set(["id", "name"]))])

    def test_was_expired_attrs(self):
        User = self.classes.User

        canary = self._fixture()

        sess = fixture_session()

        u1 = User(name="u1")
        sess.add(u1)
        sess.flush()
        sess.expire(u1, ["name"])

        sess.query(User).first()
        eq_(canary, [("refresh", set(["name"]))])

    def test_populate_existing(self):
        User = self.classes.User

        canary = self._fixture()

        sess = fixture_session()

        u1 = User(name="u1")
        sess.add(u1)
        sess.commit()

        sess.query(User).populate_existing().first()
        eq_(canary, [("refresh", None)])


class SessionEventsTest(_RemoveListeners, _fixtures.FixtureTest):
    run_inserts = None

    def test_class_listen(self):
        def my_listener(*arg, **kw):
            pass

        event.listen(Session, "before_flush", my_listener)

        s = fixture_session()
        assert my_listener in s.dispatch.before_flush

    def test_sessionmaker_listen(self):
        """test that listen can be applied to individual
        scoped_session() classes."""

        def my_listener_one(*arg, **kw):
            pass

        def my_listener_two(*arg, **kw):
            pass

        S1 = sessionmaker()
        S2 = sessionmaker()

        event.listen(Session, "before_flush", my_listener_one)
        event.listen(S1, "before_flush", my_listener_two)

        s1 = S1()
        assert my_listener_one in s1.dispatch.before_flush
        assert my_listener_two in s1.dispatch.before_flush

        s2 = S2()
        assert my_listener_one in s2.dispatch.before_flush
        assert my_listener_two not in s2.dispatch.before_flush

    def test_scoped_session_invalid_callable(self):
        from sqlalchemy.orm import scoped_session

        def my_listener_one(*arg, **kw):
            pass

        scope = scoped_session(lambda: fixture_session())

        assert_raises_message(
            sa.exc.ArgumentError,
            "Session event listen on a scoped_session requires that its "
            "creation callable is associated with the Session class.",
            event.listen,
            scope,
            "before_flush",
            my_listener_one,
        )

    def test_scoped_session_invalid_class(self):
        from sqlalchemy.orm import scoped_session

        def my_listener_one(*arg, **kw):
            pass

        class NotASession(object):
            def __call__(self):
                return fixture_session()

        scope = scoped_session(NotASession)

        assert_raises_message(
            sa.exc.ArgumentError,
            "Session event listen on a scoped_session requires that its "
            "creation callable is associated with the Session class.",
            event.listen,
            scope,
            "before_flush",
            my_listener_one,
        )

    def test_scoped_session_listen(self):
        from sqlalchemy.orm import scoped_session

        def my_listener_one(*arg, **kw):
            pass

        scope = scoped_session(sessionmaker())
        event.listen(scope, "before_flush", my_listener_one)

        assert my_listener_one in scope().dispatch.before_flush

    def _listener_fixture(self, **kw):
        canary = []

        def listener(name):
            def go(*arg, **kw):
                canary.append(name)

            return go

        sess = fixture_session(**kw)

        for evt in [
            "after_transaction_create",
            "after_transaction_end",
            "before_commit",
            "after_commit",
            "after_rollback",
            "after_soft_rollback",
            "before_flush",
            "after_flush",
            "after_flush_postexec",
            "after_begin",
            "before_attach",
            "after_attach",
            "after_bulk_update",
            "after_bulk_delete",
        ]:
            event.listen(sess, evt, listener(evt))

        return sess, canary

    def test_flush_autocommit_hook(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users)

        sess, canary = self._listener_fixture(
            autoflush=False, autocommit=True, expire_on_commit=False
        )

        u = User(name="u1")
        sess.add(u)
        sess.flush()
        eq_(
            canary,
            [
                "before_attach",
                "after_attach",
                "before_flush",
                "after_transaction_create",
                "after_begin",
                "after_flush",
                "after_flush_postexec",
                "before_commit",
                "after_commit",
                "after_transaction_end",
            ],
        )

    def test_rollback_hook(self):
        User, users = self.classes.User, self.tables.users
        sess, canary = self._listener_fixture()
        mapper(User, users)

        u = User(name="u1", id=1)
        sess.add(u)
        sess.commit()

        u2 = User(name="u1", id=1)
        sess.add(u2)
        assert_raises(sa.exc.SAWarning, sess.commit)
        sess.rollback()
        eq_(
            canary,
            [
                "after_transaction_create",  # changed in #5074
                "before_attach",
                "after_attach",
                "before_commit",
                "before_flush",
                "after_transaction_create",
                "after_begin",
                "after_flush",
                "after_flush_postexec",
                "after_transaction_end",
                "after_commit",
                "after_transaction_end",
                "after_transaction_create",
                "before_attach",
                "after_attach",
                "before_commit",
                "before_flush",
                "after_transaction_create",
                "after_begin",
                "after_rollback",
                "after_transaction_end",
                "after_soft_rollback",
                "after_transaction_end",
                "after_soft_rollback",
            ],
        )

    def test_can_use_session_in_outer_rollback_hook(self):
        User, users = self.classes.User, self.tables.users
        mapper(User, users)

        sess = fixture_session()

        assertions = []

        @event.listens_for(sess, "after_soft_rollback")
        def do_something(session, previous_transaction):
            if session.is_active:
                assertions.append("name" not in u.__dict__)
                assertions.append(u.name == "u1")

        u = User(name="u1", id=1)
        sess.add(u)
        sess.commit()

        u2 = User(name="u1", id=1)
        sess.add(u2)
        assert_raises(sa.exc.SAWarning, sess.commit)
        sess.rollback()
        eq_(assertions, [True, True])

    @testing.combinations((True,), (False,))
    def test_autobegin_no_reentrant(self, future):
        s1 = fixture_session(future=future)

        canary = Mock()

        @event.listens_for(s1, "after_transaction_create")
        def after_transaction_create(session, transaction):
            result = session.execute(select(1)).scalar()
            canary.got_result(result)

        with s1.begin():
            pass

        eq_(canary.mock_calls, [call.got_result(1)])

    def test_flush_noautocommit_hook(self):
        User, users = self.classes.User, self.tables.users

        sess, canary = self._listener_fixture()

        mapper(User, users)

        u = User(name="u1")
        sess.add(u)
        sess.flush()
        eq_(
            canary,
            [
                "after_transaction_create",  # changed due to #5074
                "before_attach",
                "after_attach",
                "before_flush",
                "after_transaction_create",
                "after_begin",
                "after_flush",
                "after_flush_postexec",
                "after_transaction_end",
            ],
        )

    def test_flush_in_commit_hook(self):
        User, users = self.classes.User, self.tables.users

        sess, canary = self._listener_fixture()

        mapper(User, users)
        u = User(name="u1")
        sess.add(u)
        sess.flush()
        canary[:] = []

        u.name = "ed"
        sess.commit()
        eq_(
            canary,
            [
                "before_commit",
                "before_flush",
                "after_transaction_create",
                "after_flush",
                "after_flush_postexec",
                "after_transaction_end",
                "after_commit",
                "after_transaction_end",
                # no longer autocreates after #5074
            ],
        )

    def test_state_before_attach(self):
        User, users = self.classes.User, self.tables.users
        sess = fixture_session()

        @event.listens_for(sess, "before_attach")
        def listener(session, inst):
            state = attributes.instance_state(inst)
            if state.key:
                assert state.key not in session.identity_map
            else:
                assert inst not in session.new

        mapper(User, users)
        u = User(name="u1")
        sess.add(u)
        sess.flush()
        sess.expunge(u)
        sess.add(u)

    def test_state_after_attach(self):
        User, users = self.classes.User, self.tables.users
        sess = fixture_session()

        @event.listens_for(sess, "after_attach")
        def listener(session, inst):
            state = attributes.instance_state(inst)
            if state.key:
                assert session.identity_map[state.key] is inst
            else:
                assert inst in session.new

        mapper(User, users)
        u = User(name="u1")
        sess.add(u)
        sess.flush()
        sess.expunge(u)
        sess.add(u)

    def test_standalone_on_commit_hook(self):
        sess, canary = self._listener_fixture()
        sess.commit()
        eq_(
            canary,
            [
                "after_transaction_create",  # moved to top due to #5074
                "before_commit",
                "after_commit",
                "after_transaction_end",
            ],
        )

    def test_on_bulk_update_hook(self):
        User, users = self.classes.User, self.tables.users

        sess = fixture_session()
        canary = Mock()

        event.listen(sess, "after_begin", canary.after_begin)
        event.listen(sess, "after_bulk_update", canary.after_bulk_update)

        mapper(User, users)

        sess.query(User).update({"name": "foo"})

        eq_(canary.after_begin.call_count, 1)
        eq_(canary.after_bulk_update.call_count, 1)

        upd = canary.after_bulk_update.mock_calls[0][1][0]
        eq_(upd.session, sess)
        eq_(upd.result.rowcount, 0)

    def test_on_bulk_delete_hook(self):
        User, users = self.classes.User, self.tables.users

        sess = fixture_session()
        canary = Mock()

        event.listen(sess, "after_begin", canary.after_begin)
        event.listen(sess, "after_bulk_delete", canary.after_bulk_delete)

        mapper(User, users)

        sess.query(User).delete()

        eq_(canary.after_begin.call_count, 1)
        eq_(canary.after_bulk_delete.call_count, 1)

        upd = canary.after_bulk_delete.mock_calls[0][1][0]
        eq_(upd.session, sess)
        eq_(upd.result.rowcount, 0)

    def test_connection_emits_after_begin(self):
        sess, canary = self._listener_fixture()
        sess.connection()
        # changed due to #5074
        eq_(canary, ["after_transaction_create", "after_begin"])
        sess.close()

    def test_reentrant_flush(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        def before_flush(session, flush_context, objects):
            session.flush()

        sess = fixture_session()
        event.listen(sess, "before_flush", before_flush)
        sess.add(User(name="foo"))
        assert_raises_message(
            sa.exc.InvalidRequestError, "already flushing", sess.flush
        )

    def test_before_flush_affects_flush_plan(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        def before_flush(session, flush_context, objects):
            for obj in list(session.new) + list(session.dirty):
                if isinstance(obj, User):
                    session.add(User(name="another %s" % obj.name))
            for obj in list(session.deleted):
                if isinstance(obj, User):
                    x = (
                        session.query(User)
                        .filter(User.name == "another %s" % obj.name)
                        .one()
                    )
                    session.delete(x)

        sess = fixture_session()
        event.listen(sess, "before_flush", before_flush)

        u = User(name="u1")
        sess.add(u)
        sess.flush()
        eq_(
            sess.query(User).order_by(User.name).all(),
            [User(name="another u1"), User(name="u1")],
        )

        sess.flush()
        eq_(
            sess.query(User).order_by(User.name).all(),
            [User(name="another u1"), User(name="u1")],
        )

        u.name = "u2"
        sess.flush()
        eq_(
            sess.query(User).order_by(User.name).all(),
            [
                User(name="another u1"),
                User(name="another u2"),
                User(name="u2"),
            ],
        )

        sess.delete(u)
        sess.flush()
        eq_(
            sess.query(User).order_by(User.name).all(),
            [User(name="another u1")],
        )

    def test_before_flush_affects_dirty(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        def before_flush(session, flush_context, objects):
            for obj in list(session.identity_map.values()):
                obj.name += " modified"

        sess = fixture_session(autoflush=True)
        event.listen(sess, "before_flush", before_flush)

        u = User(name="u1")
        sess.add(u)
        sess.flush()
        eq_(sess.query(User).order_by(User.name).all(), [User(name="u1")])

        sess.add(User(name="u2"))
        sess.flush()
        sess.expunge_all()
        eq_(
            sess.query(User).order_by(User.name).all(),
            [User(name="u1 modified"), User(name="u2")],
        )

    def test_snapshot_still_present_after_commit(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        sess = fixture_session()

        u1 = User(name="u1")

        sess.add(u1)
        sess.commit()

        u1 = sess.query(User).first()

        @event.listens_for(sess, "after_commit")
        def assert_state(session):
            assert "name" in u1.__dict__
            eq_(u1.name, "u1")

        sess.commit()
        assert "name" not in u1.__dict__

    def test_snapshot_still_present_after_rollback(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        sess = fixture_session()

        u1 = User(name="u1")

        sess.add(u1)
        sess.commit()

        u1 = sess.query(User).first()

        @event.listens_for(sess, "after_rollback")
        def assert_state(session):
            assert "name" in u1.__dict__
            eq_(u1.name, "u1")

        sess.rollback()
        assert "name" not in u1.__dict__


class SessionLifecycleEventsTest(_RemoveListeners, _fixtures.FixtureTest):
    run_inserts = None

    def _fixture(self, include_address=False):
        users, User = self.tables.users, self.classes.User

        if include_address:
            addresses, Address = self.tables.addresses, self.classes.Address
            mapper(
                User,
                users,
                properties={
                    "addresses": relationship(
                        Address, cascade="all, delete-orphan"
                    )
                },
            )
            mapper(Address, addresses)
        else:
            mapper(User, users)

        listener = Mock()

        sess = fixture_session()

        def start_events():
            event.listen(
                sess, "transient_to_pending", listener.transient_to_pending
            )
            event.listen(
                sess, "pending_to_transient", listener.pending_to_transient
            )
            event.listen(
                sess,
                "persistent_to_transient",
                listener.persistent_to_transient,
            )
            event.listen(
                sess, "pending_to_persistent", listener.pending_to_persistent
            )
            event.listen(
                sess, "detached_to_persistent", listener.detached_to_persistent
            )
            event.listen(
                sess, "loaded_as_persistent", listener.loaded_as_persistent
            )

            event.listen(
                sess, "persistent_to_detached", listener.persistent_to_detached
            )
            event.listen(
                sess, "deleted_to_detached", listener.deleted_to_detached
            )

            event.listen(
                sess, "persistent_to_deleted", listener.persistent_to_deleted
            )
            event.listen(
                sess, "deleted_to_persistent", listener.deleted_to_persistent
            )
            return listener

        if include_address:
            return sess, User, Address, start_events
        else:
            return sess, User, start_events

    def test_transient_to_pending(self):
        sess, User, start_events = self._fixture()

        listener = start_events()

        @event.listens_for(sess, "transient_to_pending")
        def trans_to_pending(session, instance):
            assert instance in session
            listener.flag_checked(instance)

        u1 = User(name="u1")
        sess.add(u1)

        eq_(
            listener.mock_calls,
            [call.transient_to_pending(sess, u1), call.flag_checked(u1)],
        )

    def test_pending_to_transient_via_rollback(self):
        sess, User, start_events = self._fixture()

        u1 = User(name="u1")
        sess.add(u1)

        listener = start_events()

        @event.listens_for(sess, "pending_to_transient")
        def test_deleted_flag(session, instance):
            assert instance not in session
            listener.flag_checked(instance)

        sess.rollback()
        assert u1 not in sess

        eq_(
            listener.mock_calls,
            [call.pending_to_transient(sess, u1), call.flag_checked(u1)],
        )

    def test_pending_to_transient_via_expunge(self):
        sess, User, start_events = self._fixture()

        u1 = User(name="u1")
        sess.add(u1)

        listener = start_events()

        @event.listens_for(sess, "pending_to_transient")
        def test_deleted_flag(session, instance):
            assert instance not in session
            listener.flag_checked(instance)

        sess.expunge(u1)
        assert u1 not in sess

        eq_(
            listener.mock_calls,
            [call.pending_to_transient(sess, u1), call.flag_checked(u1)],
        )

    def test_pending_to_persistent(self):
        sess, User, start_events = self._fixture()

        u1 = User(name="u1")
        sess.add(u1)

        listener = start_events()

        @event.listens_for(sess, "pending_to_persistent")
        def test_flag(session, instance):
            assert instance in session
            assert instance._sa_instance_state.persistent
            assert instance._sa_instance_state.key in session.identity_map
            listener.flag_checked(instance)

        sess.flush()

        eq_(
            listener.mock_calls,
            [call.pending_to_persistent(sess, u1), call.flag_checked(u1)],
        )

        u1.name = "u2"
        sess.flush()

        # event was not called again
        eq_(
            listener.mock_calls,
            [call.pending_to_persistent(sess, u1), call.flag_checked(u1)],
        )

    def test_pending_to_persistent_del(self):
        sess, User, start_events = self._fixture()

        @event.listens_for(sess, "pending_to_persistent")
        def pending_to_persistent(session, instance):
            listener.flag_checked(instance)
            # this is actually u1, because
            # we have a strong ref internally
            is_not(None, instance)

        u1 = User(name="u1")
        sess.add(u1)

        u1_inst_state = u1._sa_instance_state
        del u1

        gc_collect()

        listener = start_events()

        sess.flush()

        eq_(
            listener.mock_calls,
            [
                call.flag_checked(u1_inst_state.obj()),
                call.pending_to_persistent(sess, u1_inst_state.obj()),
            ],
        )

    def test_persistent_to_deleted_del(self):
        sess, User, start_events = self._fixture()

        u1 = User(name="u1")
        sess.add(u1)
        sess.flush()

        listener = start_events()

        @event.listens_for(sess, "persistent_to_deleted")
        def persistent_to_deleted(session, instance):
            is_not(None, instance)
            listener.flag_checked(instance)

        sess.delete(u1)
        u1_inst_state = u1._sa_instance_state

        del u1
        gc_collect()

        sess.flush()

        eq_(
            listener.mock_calls,
            [
                call.persistent_to_deleted(sess, u1_inst_state.obj()),
                call.flag_checked(u1_inst_state.obj()),
            ],
        )

    def test_detached_to_persistent(self):
        sess, User, start_events = self._fixture()

        u1 = User(name="u1")
        sess.add(u1)
        sess.flush()

        sess.expunge(u1)

        listener = start_events()

        @event.listens_for(sess, "detached_to_persistent")
        def test_deleted_flag(session, instance):
            assert instance not in session.deleted
            assert instance in session
            listener.flag_checked()

        sess.add(u1)

        eq_(
            listener.mock_calls,
            [call.detached_to_persistent(sess, u1), call.flag_checked()],
        )

    def test_loaded_as_persistent(self):
        sess, User, start_events = self._fixture()

        u1 = User(name="u1")
        sess.add(u1)
        sess.commit()
        sess.close()

        listener = start_events()

        @event.listens_for(sess, "loaded_as_persistent")
        def test_identity_flag(session, instance):
            assert instance in session
            assert instance._sa_instance_state.persistent
            assert instance._sa_instance_state.key in session.identity_map
            assert not instance._sa_instance_state.deleted
            assert not instance._sa_instance_state.detached
            assert instance._sa_instance_state.persistent
            listener.flag_checked(instance)

        u1 = sess.query(User).filter_by(name="u1").one()

        eq_(
            listener.mock_calls,
            [call.loaded_as_persistent(sess, u1), call.flag_checked(u1)],
        )

    def test_detached_to_persistent_via_deleted(self):
        sess, User, start_events = self._fixture()

        u1 = User(name="u1")
        sess.add(u1)
        sess.commit()
        sess.close()

        listener = start_events()

        @event.listens_for(sess, "detached_to_persistent")
        def test_deleted_flag_persistent(session, instance):
            assert instance not in session.deleted
            assert instance in session
            assert not instance._sa_instance_state.deleted
            assert not instance._sa_instance_state.detached
            assert instance._sa_instance_state.persistent
            listener.dtp_flag_checked(instance)

        @event.listens_for(sess, "persistent_to_deleted")
        def test_deleted_flag_detached(session, instance):
            assert instance not in session.deleted
            assert instance not in session
            assert not instance._sa_instance_state.persistent
            assert instance._sa_instance_state.deleted
            assert not instance._sa_instance_state.detached
            listener.ptd_flag_checked(instance)

        sess.delete(u1)
        assert u1 in sess.deleted

        eq_(
            listener.mock_calls,
            [call.detached_to_persistent(sess, u1), call.dtp_flag_checked(u1)],
        )

        sess.flush()

        eq_(
            listener.mock_calls,
            [
                call.detached_to_persistent(sess, u1),
                call.dtp_flag_checked(u1),
                call.persistent_to_deleted(sess, u1),
                call.ptd_flag_checked(u1),
            ],
        )

    def test_detached_to_persistent_via_cascaded_delete(self):
        sess, User, Address, start_events = self._fixture(include_address=True)

        u1 = User(name="u1")
        sess.add(u1)
        a1 = Address(email_address="e1")
        u1.addresses.append(a1)
        sess.commit()
        u1.addresses  # ensure u1.addresses refers to a1 before detachment
        sess.close()

        listener = start_events()

        @event.listens_for(sess, "detached_to_persistent")
        def test_deleted_flag(session, instance):
            assert instance not in session.deleted
            assert instance in session
            assert not instance._sa_instance_state.deleted
            assert not instance._sa_instance_state.detached
            assert instance._sa_instance_state.persistent
            listener.flag_checked(instance)

        sess.delete(u1)
        assert u1 in sess.deleted
        assert a1 in sess.deleted

        eq_(
            listener.mock_calls,
            [
                call.detached_to_persistent(sess, u1),
                call.flag_checked(u1),
                call.detached_to_persistent(sess, a1),
                call.flag_checked(a1),
            ],
        )

        sess.flush()

    def test_persistent_to_deleted(self):
        sess, User, start_events = self._fixture()

        u1 = User(name="u1")
        sess.add(u1)
        sess.commit()

        listener = start_events()

        @event.listens_for(sess, "persistent_to_deleted")
        def test_deleted_flag(session, instance):
            assert instance not in session.deleted
            assert instance not in session
            assert instance._sa_instance_state.deleted
            assert not instance._sa_instance_state.detached
            assert not instance._sa_instance_state.persistent
            listener.flag_checked(instance)

        sess.delete(u1)
        assert u1 in sess.deleted

        eq_(listener.mock_calls, [])

        sess.flush()
        assert u1 not in sess

        eq_(
            listener.mock_calls,
            [call.persistent_to_deleted(sess, u1), call.flag_checked(u1)],
        )

    def test_persistent_to_detached_via_expunge(self):
        sess, User, start_events = self._fixture()

        u1 = User(name="u1")
        sess.add(u1)
        sess.flush()

        listener = start_events()

        @event.listens_for(sess, "persistent_to_detached")
        def test_deleted_flag(session, instance):
            assert instance not in session.deleted
            assert instance not in session
            assert not instance._sa_instance_state.deleted
            assert instance._sa_instance_state.detached
            assert not instance._sa_instance_state.persistent
            listener.flag_checked(instance)

        assert u1 in sess
        sess.expunge(u1)
        assert u1 not in sess

        eq_(
            listener.mock_calls,
            [call.persistent_to_detached(sess, u1), call.flag_checked(u1)],
        )

    def test_persistent_to_detached_via_expunge_all(self):
        sess, User, start_events = self._fixture()

        u1 = User(name="u1")
        sess.add(u1)
        sess.flush()

        listener = start_events()

        @event.listens_for(sess, "persistent_to_detached")
        def test_deleted_flag(session, instance):
            assert instance not in session.deleted
            assert instance not in session
            assert not instance._sa_instance_state.deleted
            assert instance._sa_instance_state.detached
            assert not instance._sa_instance_state.persistent
            listener.flag_checked(instance)

        assert u1 in sess
        sess.expunge_all()
        assert u1 not in sess

        eq_(
            listener.mock_calls,
            [call.persistent_to_detached(sess, u1), call.flag_checked(u1)],
        )

    def test_persistent_to_transient_via_rollback(self):
        sess, User, start_events = self._fixture()

        u1 = User(name="u1")
        sess.add(u1)
        sess.flush()

        listener = start_events()

        @event.listens_for(sess, "persistent_to_transient")
        def test_deleted_flag(session, instance):
            assert instance not in session.deleted
            assert instance not in session
            assert not instance._sa_instance_state.deleted
            assert not instance._sa_instance_state.detached
            assert not instance._sa_instance_state.persistent
            assert instance._sa_instance_state.transient
            listener.flag_checked(instance)

        sess.rollback()

        eq_(
            listener.mock_calls,
            [call.persistent_to_transient(sess, u1), call.flag_checked(u1)],
        )

    def test_deleted_to_persistent_via_rollback(self):
        sess, User, start_events = self._fixture()

        u1 = User(name="u1")
        sess.add(u1)
        sess.commit()

        sess.delete(u1)
        sess.flush()

        listener = start_events()

        @event.listens_for(sess, "deleted_to_persistent")
        def test_deleted_flag(session, instance):
            assert instance not in session.deleted
            assert instance in session
            assert not instance._sa_instance_state.deleted
            assert not instance._sa_instance_state.detached
            assert instance._sa_instance_state.persistent
            listener.flag_checked(instance)

        assert u1 not in sess
        assert u1._sa_instance_state.deleted
        assert not u1._sa_instance_state.persistent
        assert not u1._sa_instance_state.detached

        sess.rollback()

        assert u1 in sess
        assert u1._sa_instance_state.persistent
        assert not u1._sa_instance_state.deleted
        assert not u1._sa_instance_state.detached

        eq_(
            listener.mock_calls,
            [call.deleted_to_persistent(sess, u1), call.flag_checked(u1)],
        )

    def test_deleted_to_detached_via_commit(self):
        sess, User, start_events = self._fixture()

        u1 = User(name="u1")
        sess.add(u1)
        sess.commit()

        sess.delete(u1)
        sess.flush()

        listener = start_events()

        @event.listens_for(sess, "deleted_to_detached")
        def test_detached_flag(session, instance):
            assert instance not in session.deleted
            assert instance not in session
            assert not instance._sa_instance_state.deleted
            assert instance._sa_instance_state.detached
            listener.flag_checked(instance)

        assert u1 not in sess
        assert u1._sa_instance_state.deleted
        assert not u1._sa_instance_state.persistent
        assert not u1._sa_instance_state.detached

        sess.commit()

        assert u1 not in sess
        assert not u1._sa_instance_state.deleted
        assert u1._sa_instance_state.detached

        eq_(
            listener.mock_calls,
            [call.deleted_to_detached(sess, u1), call.flag_checked(u1)],
        )


class QueryEventsTest(
    _RemoveListeners,
    _fixtures.FixtureTest,
    AssertsCompiledSQL,
    testing.AssertsExecutionResults,
):
    __dialect__ = "default"

    @classmethod
    def setup_mappers(cls):
        User = cls.classes.User
        users = cls.tables.users

        mapper(User, users)

    def test_before_compile(self):
        @event.listens_for(query.Query, "before_compile", retval=True)
        def no_deleted(query):
            for desc in query.column_descriptions:
                if desc["type"] is User:
                    entity = desc["expr"]
                    query = query.filter(entity.id != 10)
            return query

        User = self.classes.User
        s = fixture_session()

        q = s.query(User).filter_by(id=7)
        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users "
            "WHERE users.id = :id_1 AND users.id != :id_2",
            checkparams={"id_2": 10, "id_1": 7},
        )

    def test_before_compile_no_retval(self):
        counter = [0]

        @event.listens_for(query.Query, "before_compile")
        def count(query):
            counter[0] += 1

        User = self.classes.User
        s = fixture_session()

        q = s.query(User).filter_by(id=7)
        str(q)
        str(q)
        eq_(counter, [2])

    def test_alters_entities(self):
        User = self.classes.User

        @event.listens_for(query.Query, "before_compile", retval=True)
        def fn(query):
            return query.add_columns(User.name)

        s = fixture_session()

        q = s.query(User.id).filter_by(id=7)
        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users "
            "WHERE users.id = :id_1",
            checkparams={"id_1": 7},
        )
        eq_(q.all(), [(7, "jack")])

    def test_before_compile_update(self):
        @event.listens_for(query.Query, "before_compile_update", retval=True)
        def no_deleted(query, update_context):
            assert update_context.query is query

            for desc in query.column_descriptions:
                if desc["type"] is User:
                    entity = desc["expr"]
                    query = query.filter(entity.id != 10)

                    update_context.values["name"] = (
                        update_context.values["name"] + "_modified"
                    )
            return query

        User = self.classes.User
        s = fixture_session()

        with self.sql_execution_asserter() as asserter:
            s.query(User).filter_by(id=7).update({"name": "ed"})
        asserter.assert_(
            CompiledSQL(
                "UPDATE users SET name=:name WHERE "
                "users.id = :id_1 AND users.id != :id_2",
                [{"name": "ed_modified", "id_1": 7, "id_2": 10}],
            )
        )

    def test_before_compile_delete(self):
        @event.listens_for(query.Query, "before_compile_delete", retval=True)
        def no_deleted(query, delete_context):
            assert delete_context.query is query

            for desc in query.column_descriptions:
                if desc["type"] is User:
                    entity = desc["expr"]
                    query = query.filter(entity.id != 10)
            return query

        User = self.classes.User
        s = fixture_session()

        # note this deletes no rows
        with self.sql_execution_asserter() as asserter:
            s.query(User).filter_by(id=10).delete()
        asserter.assert_(
            CompiledSQL(
                "DELETE FROM users WHERE "
                "users.id = :id_1 AND users.id != :id_2",
                [{"id_1": 10, "id_2": 10}],
            )
        )

    def test_before_compile_execution_options(self):
        User = self.classes.User

        @event.listens_for(query.Query, "before_compile", retval=True)
        def _before_compile(query):
            return query.execution_options(my_option=True)

        opts = {}

        @event.listens_for(testing.db, "before_cursor_execute")
        def _before_cursor_execute(
            conn, cursor, statement, parameters, context, executemany
        ):
            opts.update(context.execution_options)

        sess = fixture_session(autocommit=False)
        sess.query(User).first()
        eq_(opts["my_option"], True)


class RefreshFlushInReturningTest(fixtures.MappedTest):
    """test [ticket:3427].

    this is a rework of the test for [ticket:3167] stated
    in test_unitofworkv2, which tests that returning doesn't trigger
    attribute events; the test here is *reversed* so that we test that
    it *does* trigger the new refresh_flush event.

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
        event.listen(Thing, "refresh_flush", mock)
        t1 = Thing()
        s = fixture_session()
        s.add(t1)
        s.flush()

        if testing.requires.returning.enabled:
            # ordering is deterministic in this test b.c. the routine
            # appends the "returning" params before the "prefetch"
            # ones.  if there were more than one attribute in each category,
            # then we'd have hash order issues.
            eq_(
                mock.mock_calls,
                [call(t1, ANY, ["returning_val", "prefetch_val"])],
            )
        else:
            eq_(mock.mock_calls, [call(t1, ANY, ["prefetch_val"])])

        eq_(t1.id, 1)
        eq_(t1.prefetch_val, 5)
        eq_(t1.returning_val, 5)
