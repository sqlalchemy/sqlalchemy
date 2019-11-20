import sqlalchemy as sa
from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.ext.declarative import comparable_using
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import AttributeExtension
from sqlalchemy.orm import attributes
from sqlalchemy.orm import collections
from sqlalchemy.orm import column_property
from sqlalchemy.orm import comparable_property
from sqlalchemy.orm import composite
from sqlalchemy.orm import configure_mappers
from sqlalchemy.orm import create_session
from sqlalchemy.orm import defer
from sqlalchemy.orm import deferred
from sqlalchemy.orm import EXT_CONTINUE
from sqlalchemy.orm import foreign
from sqlalchemy.orm import identity
from sqlalchemy.orm import instrumentation
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import joinedload_all
from sqlalchemy.orm import mapper
from sqlalchemy.orm import MapperExtension
from sqlalchemy.orm import PropComparator
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm import SessionExtension
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import synonym
from sqlalchemy.orm import undefer
from sqlalchemy.orm.collections import collection
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import assertions
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import in_
from sqlalchemy.testing import is_
from sqlalchemy.testing import not_in_
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from sqlalchemy.testing.util import gc_collect
from sqlalchemy.util.compat import pypy
from . import _fixtures
from .test_options import PathTest as OptionsPathTest
from .test_query import QueryTest
from .test_transaction import _LocalFixture


class DeprecationWarningsTest(fixtures.DeclarativeMappedTest):
    run_setup_classes = "each"
    run_setup_mappers = "each"
    run_define_tables = "each"
    run_create_tables = None

    def test_attribute_extension(self):
        class SomeExtension(AttributeExtension):
            def append(self, obj, value, initiator):
                pass

            def remove(self, obj, value, initiator):
                pass

            def set(self, obj, value, oldvalue, initiator):
                pass

        with assertions.expect_deprecated(
            ".*The column_property.extension parameter will be removed in a "
            "future release."
        ):

            class Foo(self.DeclarativeBasic):
                __tablename__ = "foo"

                id = Column(Integer, primary_key=True)
                foo = column_property(
                    Column("q", Integer), extension=SomeExtension()
                )

        with assertions.expect_deprecated(
            "AttributeExtension.append is deprecated.  The "
            "AttributeExtension class will be removed in a future release.",
            "AttributeExtension.remove is deprecated.  The "
            "AttributeExtension class will be removed in a future release.",
            "AttributeExtension.set is deprecated.  The "
            "AttributeExtension class will be removed in a future release.",
        ):
            configure_mappers()

    def test_attribute_extension_parameter(self):
        class SomeExtension(AttributeExtension):
            def append(self, obj, value, initiator):
                pass

        with assertions.expect_deprecated(
            ".*The relationship.extension parameter will be removed in a "
            "future release."
        ):
            relationship("Bar", extension=SomeExtension)

        with assertions.expect_deprecated(
            ".*The column_property.extension parameter will be removed in a "
            "future release."
        ):
            column_property(Column("q", Integer), extension=SomeExtension)

        with assertions.expect_deprecated(
            ".*The composite.extension parameter will be removed in a "
            "future release."
        ):
            composite("foo", extension=SomeExtension)

    def test_session_extension(self):
        class SomeExtension(SessionExtension):
            def after_commit(self, session):
                pass

            def after_rollback(self, session):
                pass

            def before_flush(self, session, flush_context, instances):
                pass

        with assertions.expect_deprecated(
            ".*The Session.extension parameter will be removed",
            "SessionExtension.after_commit is deprecated.  "
            "The SessionExtension class",
            "SessionExtension.before_flush is deprecated.  "
            "The SessionExtension class",
            "SessionExtension.after_rollback is deprecated.  "
            "The SessionExtension class",
        ):
            Session(extension=SomeExtension())

    def test_mapper_extension(self):
        class SomeExtension(MapperExtension):
            def init_instance(
                self, mapper, class_, oldinit, instance, args, kwargs
            ):
                pass

            def init_failed(
                self, mapper, class_, oldinit, instance, args, kwargs
            ):
                pass

        with assertions.expect_deprecated(
            "MapperExtension.init_instance is deprecated.  "
            "The MapperExtension class",
            "MapperExtension.init_failed is deprecated.  "
            "The MapperExtension class",
            ".*The mapper.extension parameter will be removed",
        ):

            class Foo(self.DeclarativeBasic):
                __tablename__ = "foo"

                id = Column(Integer, primary_key=True)

                __mapper_args__ = {"extension": SomeExtension()}

    def test_session_weak_identity_map(self):
        with testing.expect_deprecated(
            ".*Session.weak_identity_map parameter as well as the"
        ):
            s = Session(weak_identity_map=True)

        is_(s._identity_cls, identity.WeakInstanceDict)

        with assertions.expect_deprecated(
            "The Session.weak_identity_map parameter as well as"
        ):
            s = Session(weak_identity_map=False)

            is_(s._identity_cls, identity.StrongInstanceDict)

        s = Session()
        is_(s._identity_cls, identity.WeakInstanceDict)

    def test_session_prune(self):
        s = Session()

        with assertions.expect_deprecated(
            r"The Session.prune\(\) method is deprecated along with "
            "Session.weak_identity_map"
        ):
            s.prune()

    def test_session_enable_transaction_accounting(self):
        with assertions.expect_deprecated(
            "the Session._enable_transaction_accounting parameter is "
            "deprecated"
        ):
            Session(_enable_transaction_accounting=False)

    def test_session_is_modified(self):
        class Foo(self.DeclarativeBasic):
            __tablename__ = "foo"

            id = Column(Integer, primary_key=True)

        f1 = Foo()
        s = Session()
        with assertions.expect_deprecated(
            "The Session.is_modified.passive flag is deprecated"
        ):
            # this flag was for a long time documented as requiring
            # that it be set to True, so we've changed the default here
            # so that the warning emits
            s.is_modified(f1, passive=True)


class DeprecationQueryTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_textual_query_column(self):
        s = Session()

        with assertions.expect_deprecated(
            r"Plain string expression passed to Query\(\) should be "
            "explicitly "
        ):
            self.assert_compile(s.query("1"), "SELECT 1")

    def test_as_column(self):
        User = self.classes.User

        s = Session()
        with assertions.expect_deprecated(
            r"Plain string expression passed to Query\(\) should be "
            "explicitly "
        ):
            eq_(
                s.query(User.id, "name").order_by(User.id).all(),
                [(7, "jack"), (8, "ed"), (9, "fred"), (10, "chuck")],
            )

    def test_raw_columns(self):
        addresses, users, User = (
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        sess = create_session()
        (user7, user8, user9, user10) = sess.query(User).all()
        expected = [
            (user7, 1, "Name:jack"),
            (user8, 3, "Name:ed"),
            (user9, 1, "Name:fred"),
            (user10, 0, "Name:chuck"),
        ]

        # test with a straight statement
        s = select(
            [
                users,
                func.count(addresses.c.id).label("count"),
                ("Name:" + users.c.name).label("concat"),
            ],
            from_obj=[users.outerjoin(addresses)],
            group_by=[c for c in users.c],
            order_by=[users.c.id],
        )
        q = create_session().query(User)

        with assertions.expect_deprecated(
            r"Plain string expression passed to Query\(\) should be "
            "explicitly "
        ):
            result = (
                q.add_column("count")
                .add_column("concat")
                .from_statement(s)
                .all()
            )
        assert result == expected


class DeprecatedAccountingFlagsTest(_LocalFixture):
    def test_rollback_no_accounting(self):
        User, users = self.classes.User, self.tables.users

        with testing.expect_deprecated(
            "The Session._enable_transaction_accounting parameter"
        ):
            sess = sessionmaker(_enable_transaction_accounting=False)()
        u1 = User(name="ed")
        sess.add(u1)
        sess.commit()

        u1.name = "edwardo"
        sess.rollback()

        testing.db.execute(
            users.update(users.c.name == "ed").values(name="edward")
        )

        assert u1.name == "edwardo"
        sess.expire_all()
        assert u1.name == "edward"

    def test_commit_no_accounting(self):
        User, users = self.classes.User, self.tables.users

        with testing.expect_deprecated(
            "The Session._enable_transaction_accounting parameter"
        ):
            sess = sessionmaker(_enable_transaction_accounting=False)()
        u1 = User(name="ed")
        sess.add(u1)
        sess.commit()

        u1.name = "edwardo"
        sess.rollback()

        testing.db.execute(
            users.update(users.c.name == "ed").values(name="edward")
        )

        assert u1.name == "edwardo"
        sess.commit()

        assert testing.db.execute(select([users.c.name])).fetchall() == [
            ("edwardo",)
        ]
        assert u1.name == "edwardo"

        sess.delete(u1)
        sess.commit()

    def test_preflush_no_accounting(self):
        User, users = self.classes.User, self.tables.users

        with testing.expect_deprecated(
            "The Session._enable_transaction_accounting parameter"
        ):
            sess = Session(
                _enable_transaction_accounting=False,
                autocommit=True,
                autoflush=False,
            )
        u1 = User(name="ed")
        sess.add(u1)
        sess.flush()

        sess.begin()
        u1.name = "edwardo"
        u2 = User(name="some other user")
        sess.add(u2)

        sess.rollback()

        sess.begin()
        assert testing.db.execute(select([users.c.name])).fetchall() == [
            ("ed",)
        ]


class TLTransactionTest(fixtures.MappedTest):
    run_dispose_bind = "once"
    __backend__ = True

    @classmethod
    def setup_bind(cls):
        with testing.expect_deprecated(
            ".*'threadlocal' engine strategy is deprecated"
        ):
            return engines.testing_engine(options=dict(strategy="threadlocal"))

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(20)),
            test_needs_acid=True,
        )

    @classmethod
    def setup_classes(cls):
        class User(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        users, User = cls.tables.users, cls.classes.User

        mapper(User, users)

    @testing.exclude("mysql", "<", (5, 0, 3), "FIXME: unknown")
    def test_session_nesting(self):
        User = self.classes.User

        sess = create_session(bind=self.bind)
        self.bind.begin()
        u = User(name="ed")
        sess.add(u)
        sess.flush()
        self.bind.commit()


class DeprecatedSessionFeatureTest(_fixtures.FixtureTest):
    run_inserts = None

    def test_fast_discard_race(self):
        # test issue #4068
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        with testing.expect_deprecated(".*identity map are deprecated"):
            sess = Session(weak_identity_map=False)

        u1 = User(name="u1")
        sess.add(u1)
        sess.commit()

        u1_state = u1._sa_instance_state
        sess.identity_map._dict.pop(u1_state.key)
        ref = u1_state.obj
        u1_state.obj = lambda: None

        u2 = sess.query(User).first()
        u1_state._cleanup(ref)

        u3 = sess.query(User).first()

        is_(u2, u3)

        u2_state = u2._sa_instance_state
        assert sess.identity_map.contains_state(u2._sa_instance_state)
        ref = u2_state.obj
        u2_state.obj = lambda: None
        u2_state._cleanup(ref)
        assert not sess.identity_map.contains_state(u2._sa_instance_state)

    def test_is_modified_passive_on(self):
        User, Address = self.classes.User, self.classes.Address
        users, addresses = self.tables.users, self.tables.addresses
        mapper(User, users, properties={"addresses": relationship(Address)})
        mapper(Address, addresses)

        s = Session()
        u = User(name="fred", addresses=[Address(email_address="foo")])
        s.add(u)
        s.commit()

        u.id

        def go():
            assert not s.is_modified(u, passive=True)

        with testing.expect_deprecated(
            ".*Session.is_modified.passive flag is deprecated "
        ):
            self.assert_sql_count(testing.db, go, 0)

        u.name = "newname"

        def go():
            assert s.is_modified(u, passive=True)

        with testing.expect_deprecated(
            ".*Session.is_modified.passive flag is deprecated "
        ):
            self.assert_sql_count(testing.db, go, 0)


class StrongIdentityMapTest(_fixtures.FixtureTest):
    run_inserts = None

    def _strong_ident_fixture(self):
        with testing.expect_deprecated(
            ".*Session.weak_identity_map parameter as well as the"
        ):
            sess = create_session(weak_identity_map=False)

        def prune():
            with testing.expect_deprecated(".*Session.prune"):
                return sess.prune()

        return sess, prune

    def _event_fixture(self):
        session = create_session()

        @event.listens_for(session, "pending_to_persistent")
        @event.listens_for(session, "deleted_to_persistent")
        @event.listens_for(session, "detached_to_persistent")
        @event.listens_for(session, "loaded_as_persistent")
        def strong_ref_object(sess, instance):
            if "refs" not in sess.info:
                sess.info["refs"] = refs = set()
            else:
                refs = sess.info["refs"]

            refs.add(instance)

        @event.listens_for(session, "persistent_to_detached")
        @event.listens_for(session, "persistent_to_deleted")
        @event.listens_for(session, "persistent_to_transient")
        def deref_object(sess, instance):
            sess.info["refs"].discard(instance)

        def prune():
            if "refs" not in session.info:
                return 0

            sess_size = len(session.identity_map)
            session.info["refs"].clear()
            gc_collect()
            session.info["refs"] = set(
                s.obj() for s in session.identity_map.all_states()
            )
            return sess_size - len(session.identity_map)

        return session, prune

    def test_strong_ref_imap(self):
        self._test_strong_ref(self._strong_ident_fixture)

    def test_strong_ref_events(self):
        self._test_strong_ref(self._event_fixture)

    def _test_strong_ref(self, fixture):
        s, prune = fixture()

        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        # save user
        s.add(User(name="u1"))
        s.flush()
        user = s.query(User).one()
        user = None
        print(s.identity_map)
        gc_collect()
        assert len(s.identity_map) == 1

        user = s.query(User).one()
        assert not s.identity_map._modified
        user.name = "u2"
        assert s.identity_map._modified
        s.flush()
        eq_(users.select().execute().fetchall(), [(user.id, "u2")])

    def test_prune_imap(self):
        self._test_prune(self._strong_ident_fixture)

    def test_prune_events(self):
        self._test_prune(self._event_fixture)

    @testing.fails_if(lambda: pypy, "pypy has a real GC")
    @testing.fails_on("+zxjdbc", "http://www.sqlalchemy.org/trac/ticket/1473")
    def _test_prune(self, fixture):
        s, prune = fixture()

        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        for o in [User(name="u%s" % x) for x in range(10)]:
            s.add(o)
        # o is still live after this loop...

        self.assert_(len(s.identity_map) == 0)
        eq_(prune(), 0)
        s.flush()
        gc_collect()
        eq_(prune(), 9)
        # o is still in local scope here, so still present
        self.assert_(len(s.identity_map) == 1)

        id_ = o.id
        del o
        eq_(prune(), 1)
        self.assert_(len(s.identity_map) == 0)

        u = s.query(User).get(id_)
        eq_(prune(), 0)
        self.assert_(len(s.identity_map) == 1)
        u.name = "squiznart"
        del u
        eq_(prune(), 0)
        self.assert_(len(s.identity_map) == 1)
        s.flush()
        eq_(prune(), 1)
        self.assert_(len(s.identity_map) == 0)

        s.add(User(name="x"))
        eq_(prune(), 0)
        self.assert_(len(s.identity_map) == 0)
        s.flush()
        self.assert_(len(s.identity_map) == 1)
        eq_(prune(), 1)
        self.assert_(len(s.identity_map) == 0)

        u = s.query(User).get(id_)
        s.delete(u)
        del u
        eq_(prune(), 0)
        self.assert_(len(s.identity_map) == 1)
        s.flush()
        eq_(prune(), 0)
        self.assert_(len(s.identity_map) == 0)


class DeprecatedMapperTest(_fixtures.FixtureTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_cancel_order_by(self):
        users, User = self.tables.users, self.classes.User

        with testing.expect_deprecated(
            "The Mapper.order_by parameter is deprecated, and will be "
            "removed in a future release."
        ):
            mapper(User, users, order_by=users.c.name.desc())

        assert (
            "order by users.name desc"
            in str(create_session().query(User).statement).lower()
        )
        assert (
            "order by"
            not in str(
                create_session().query(User).order_by(None).statement
            ).lower()
        )
        assert (
            "order by users.name asc"
            in str(
                create_session()
                .query(User)
                .order_by(User.name.asc())
                .statement
            ).lower()
        )

        eq_(
            create_session().query(User).all(),
            [
                User(id=7, name="jack"),
                User(id=9, name="fred"),
                User(id=8, name="ed"),
                User(id=10, name="chuck"),
            ],
        )

        eq_(
            create_session().query(User).order_by(User.name).all(),
            [
                User(id=10, name="chuck"),
                User(id=8, name="ed"),
                User(id=9, name="fred"),
                User(id=7, name="jack"),
            ],
        )

    def test_comparable(self):
        users = self.tables.users

        class extendedproperty(property):
            attribute = 123

            def method1(self):
                return "method1"

        from sqlalchemy.orm.properties import ColumnProperty

        class UCComparator(ColumnProperty.Comparator):
            __hash__ = None

            def method1(self):
                return "uccmethod1"

            def method2(self, other):
                return "method2"

            def __eq__(self, other):
                cls = self.prop.parent.class_
                col = getattr(cls, "name")
                if other is None:
                    return col is None
                else:
                    return sa.func.upper(col) == sa.func.upper(other)

        def map_(with_explicit_property):
            class User(object):
                @extendedproperty
                def uc_name(self):
                    if self.name is None:
                        return None
                    return self.name.upper()

            if with_explicit_property:
                args = (UCComparator, User.uc_name)
            else:
                args = (UCComparator,)

            with assertions.expect_deprecated(
                r"comparable_property\(\) is deprecated and will be "
                "removed in a future release."
            ):
                mapper(
                    User,
                    users,
                    properties=dict(uc_name=sa.orm.comparable_property(*args)),
                )
                return User

        for User in (map_(True), map_(False)):
            sess = create_session()
            sess.begin()
            q = sess.query(User)

            assert hasattr(User, "name")
            assert hasattr(User, "uc_name")

            eq_(User.uc_name.method1(), "method1")
            eq_(User.uc_name.method2("x"), "method2")

            assert_raises_message(
                AttributeError,
                "Neither 'extendedproperty' object nor 'UCComparator' "
                "object associated with User.uc_name has an attribute "
                "'nonexistent'",
                getattr,
                User.uc_name,
                "nonexistent",
            )

            # test compile
            assert not isinstance(User.uc_name == "jack", bool)
            u = q.filter(User.uc_name == "JACK").one()

            assert u.uc_name == "JACK"
            assert u not in sess.dirty

            u.name = "some user name"
            eq_(u.name, "some user name")
            assert u in sess.dirty
            eq_(u.uc_name, "SOME USER NAME")

            sess.flush()
            sess.expunge_all()

            q = sess.query(User)
            u2 = q.filter(User.name == "some user name").one()
            u3 = q.filter(User.uc_name == "SOME USER NAME").one()

            assert u2 is u3

            eq_(User.uc_name.attribute, 123)
            sess.rollback()

    def test_comparable_column(self):
        users, User = self.tables.users, self.classes.User

        class MyComparator(sa.orm.properties.ColumnProperty.Comparator):
            __hash__ = None

            def __eq__(self, other):
                # lower case comparison
                return func.lower(self.__clause_element__()) == func.lower(
                    other
                )

            def intersects(self, other):
                # non-standard comparator
                return self.__clause_element__().op("&=")(other)

        mapper(
            User,
            users,
            properties={
                "name": sa.orm.column_property(
                    users.c.name, comparator_factory=MyComparator
                )
            },
        )

        assert_raises_message(
            AttributeError,
            "Neither 'InstrumentedAttribute' object nor "
            "'MyComparator' object associated with User.name has "
            "an attribute 'nonexistent'",
            getattr,
            User.name,
            "nonexistent",
        )

        eq_(
            str(
                (User.name == "ed").compile(
                    dialect=sa.engine.default.DefaultDialect()
                )
            ),
            "lower(users.name) = lower(:lower_1)",
        )
        eq_(
            str(
                (User.name.intersects("ed")).compile(
                    dialect=sa.engine.default.DefaultDialect()
                )
            ),
            "users.name &= :name_1",
        )

    def test_info(self):
        class MyComposite(object):
            pass

        with assertions.expect_deprecated(
            r"comparable_property\(\) is deprecated and will be "
            "removed in a future release."
        ):
            for constructor, args in [(comparable_property, "foo")]:
                obj = constructor(info={"x": "y"}, *args)
                eq_(obj.info, {"x": "y"})
                obj.info["q"] = "p"
                eq_(obj.info, {"x": "y", "q": "p"})

                obj = constructor(*args)
                eq_(obj.info, {})
                obj.info["q"] = "p"
                eq_(obj.info, {"q": "p"})

    def test_add_property(self):
        users = self.tables.users

        assert_col = []

        class User(fixtures.ComparableEntity):
            def _get_name(self):
                assert_col.append(("get", self._name))
                return self._name

            def _set_name(self, name):
                assert_col.append(("set", name))
                self._name = name

            name = property(_get_name, _set_name)

            def _uc_name(self):
                if self._name is None:
                    return None
                return self._name.upper()

            uc_name = property(_uc_name)
            uc_name2 = property(_uc_name)

        m = mapper(User, users)

        class UCComparator(PropComparator):
            __hash__ = None

            def __eq__(self, other):
                cls = self.prop.parent.class_
                col = getattr(cls, "name")
                if other is None:
                    return col is None
                else:
                    return func.upper(col) == func.upper(other)

        m.add_property("_name", deferred(users.c.name))
        m.add_property("name", synonym("_name"))
        with assertions.expect_deprecated(
            r"comparable_property\(\) is deprecated and will be "
            "removed in a future release."
        ):
            m.add_property("uc_name", comparable_property(UCComparator))
            m.add_property(
                "uc_name2", comparable_property(UCComparator, User.uc_name2)
            )

        sess = create_session(autocommit=False)
        assert sess.query(User).get(7)

        u = sess.query(User).filter_by(name="jack").one()

        def go():
            eq_(u.name, "jack")
            eq_(u.uc_name, "JACK")
            eq_(u.uc_name2, "JACK")
            eq_(assert_col, [("get", "jack")], str(assert_col))

        self.sql_count_(1, go)

    def test_kwarg_accepted(self):
        class DummyComposite(object):
            def __init__(self, x, y):
                pass

        class MyFactory(PropComparator):
            pass

        with assertions.expect_deprecated(
            r"comparable_property\(\) is deprecated and will be "
            "removed in a future release."
        ):
            for args in ((comparable_property,),):
                fn = args[0]
                args = args[1:]
                fn(comparator_factory=MyFactory, *args)

    def test_merge_synonym_comparable(self):
        users = self.tables.users

        class User(object):
            class Comparator(PropComparator):
                pass

            def _getValue(self):
                return self._value

            def _setValue(self, value):
                setattr(self, "_value", value)

            value = property(_getValue, _setValue)

        with assertions.expect_deprecated(
            r"comparable_property\(\) is deprecated and will be "
            "removed in a future release."
        ):
            mapper(
                User,
                users,
                properties={
                    "uid": synonym("id"),
                    "foobar": comparable_property(User.Comparator, User.value),
                },
            )

        sess = create_session()
        u = User()
        u.name = "ed"
        sess.add(u)
        sess.flush()
        sess.expunge(u)
        sess.merge(u)


class DeprecatedDeclTest(fixtures.TestBase):
    @testing.provide_metadata
    def test_comparable_using(self):
        class NameComparator(sa.orm.PropComparator):
            @property
            def upperself(self):
                cls = self.prop.parent.class_
                col = getattr(cls, "name")
                return sa.func.upper(col)

            def operate(self, op, other, **kw):
                return op(self.upperself, other, **kw)

        Base = declarative_base(metadata=self.metadata)

        with testing.expect_deprecated(
            r"comparable_property\(\) is deprecated and will be "
            "removed in a future release."
        ):

            class User(Base, fixtures.ComparableEntity):

                __tablename__ = "users"
                id = Column(
                    "id",
                    Integer,
                    primary_key=True,
                    test_needs_autoincrement=True,
                )
                name = Column("name", String(50))

                @comparable_using(NameComparator)
                @property
                def uc_name(self):
                    return self.name is not None and self.name.upper() or None

        Base.metadata.create_all()
        sess = create_session()
        u1 = User(name="someuser")
        eq_(u1.name, "someuser", u1.name)
        eq_(u1.uc_name, "SOMEUSER", u1.uc_name)
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        rt = sess.query(User).filter(User.uc_name == "SOMEUSER").one()
        eq_(rt, u1)
        sess.expunge_all()
        rt = sess.query(User).filter(User.uc_name.startswith("SOMEUSE")).one()
        eq_(rt, u1)


class DeprecatedMapperExtensionTest(_fixtures.FixtureTest):

    """Superseded by MapperEventsTest - test backwards
    compatibility of MapperExtension."""

    run_inserts = None

    def extension(self):
        methods = []

        class Ext(MapperExtension):
            def instrument_class(self, mapper, cls):
                methods.append("instrument_class")
                return EXT_CONTINUE

            def init_instance(
                self, mapper, class_, oldinit, instance, args, kwargs
            ):
                methods.append("init_instance")
                return EXT_CONTINUE

            def init_failed(
                self, mapper, class_, oldinit, instance, args, kwargs
            ):
                methods.append("init_failed")
                return EXT_CONTINUE

            def reconstruct_instance(self, mapper, instance):
                methods.append("reconstruct_instance")
                return EXT_CONTINUE

            def before_insert(self, mapper, connection, instance):
                methods.append("before_insert")
                return EXT_CONTINUE

            def after_insert(self, mapper, connection, instance):
                methods.append("after_insert")
                return EXT_CONTINUE

            def before_update(self, mapper, connection, instance):
                methods.append("before_update")
                return EXT_CONTINUE

            def after_update(self, mapper, connection, instance):
                methods.append("after_update")
                return EXT_CONTINUE

            def before_delete(self, mapper, connection, instance):
                methods.append("before_delete")
                return EXT_CONTINUE

            def after_delete(self, mapper, connection, instance):
                methods.append("after_delete")
                return EXT_CONTINUE

        return Ext, methods

    def test_basic(self):
        """test that common user-defined methods get called."""

        User, users = self.classes.User, self.tables.users

        Ext, methods = self.extension()

        with testing.expect_deprecated(
            "MapperExtension is deprecated in favor of the MapperEvents",
            "MapperExtension.before_insert is deprecated",
            "MapperExtension.instrument_class is deprecated",
            "MapperExtension.init_instance is deprecated",
            "MapperExtension.after_insert is deprecated",
            "MapperExtension.reconstruct_instance is deprecated",
            "MapperExtension.before_delete is deprecated",
            "MapperExtension.after_delete is deprecated",
            "MapperExtension.before_update is deprecated",
            "MapperExtension.after_update is deprecated",
            "MapperExtension.init_failed is deprecated",
        ):
            mapper(User, users, extension=Ext())
        sess = create_session()
        u = User(name="u1")
        sess.add(u)
        sess.flush()
        u = sess.query(User).populate_existing().get(u.id)
        sess.expunge_all()
        u = sess.query(User).get(u.id)
        u.name = "u1 changed"
        sess.flush()
        sess.delete(u)
        sess.flush()
        eq_(
            methods,
            [
                "instrument_class",
                "init_instance",
                "before_insert",
                "after_insert",
                "reconstruct_instance",
                "before_update",
                "after_update",
                "before_delete",
                "after_delete",
            ],
        )

    def test_inheritance(self):
        users, addresses, User = (
            self.tables.users,
            self.tables.addresses,
            self.classes.User,
        )

        Ext, methods = self.extension()

        class AdminUser(User):
            pass

        with testing.expect_deprecated(
            "MapperExtension is deprecated in favor of the MapperEvents",
            "MapperExtension.before_insert is deprecated",
            "MapperExtension.instrument_class is deprecated",
            "MapperExtension.init_instance is deprecated",
            "MapperExtension.after_insert is deprecated",
            "MapperExtension.reconstruct_instance is deprecated",
            "MapperExtension.before_delete is deprecated",
            "MapperExtension.after_delete is deprecated",
            "MapperExtension.before_update is deprecated",
            "MapperExtension.after_update is deprecated",
            "MapperExtension.init_failed is deprecated",
        ):
            mapper(User, users, extension=Ext())
        mapper(
            AdminUser,
            addresses,
            inherits=User,
            properties={"address_id": addresses.c.id},
        )

        sess = create_session()
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
            methods,
            [
                "instrument_class",
                "instrument_class",
                "init_instance",
                "before_insert",
                "after_insert",
                "reconstruct_instance",
                "before_update",
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

        Ext1, methods1 = self.extension()
        Ext2, methods2 = self.extension()

        with testing.expect_deprecated(
            "MapperExtension is deprecated in favor of the MapperEvents",
            "MapperExtension.before_insert is deprecated",
            "MapperExtension.instrument_class is deprecated",
            "MapperExtension.init_instance is deprecated",
            "MapperExtension.after_insert is deprecated",
            "MapperExtension.reconstruct_instance is deprecated",
            "MapperExtension.before_delete is deprecated",
            "MapperExtension.after_delete is deprecated",
            "MapperExtension.before_update is deprecated",
            "MapperExtension.after_update is deprecated",
            "MapperExtension.init_failed is deprecated",
        ):
            mapper(
                Item,
                items,
                extension=Ext1(),
                properties={
                    "keywords": relationship(Keyword, secondary=item_keywords)
                },
            )
        with testing.expect_deprecated(
            "MapperExtension is deprecated in favor of the MapperEvents",
            "MapperExtension.before_insert is deprecated",
            "MapperExtension.instrument_class is deprecated",
            "MapperExtension.init_instance is deprecated",
            "MapperExtension.after_insert is deprecated",
            "MapperExtension.reconstruct_instance is deprecated",
            "MapperExtension.before_delete is deprecated",
            "MapperExtension.after_delete is deprecated",
            "MapperExtension.before_update is deprecated",
            "MapperExtension.after_update is deprecated",
            "MapperExtension.init_failed is deprecated",
        ):
            mapper(Keyword, keywords, extension=Ext2())

        sess = create_session()
        i1 = Item(description="i1")
        k1 = Keyword(name="k1")
        sess.add(i1)
        sess.add(k1)
        sess.flush()
        eq_(
            methods1,
            [
                "instrument_class",
                "init_instance",
                "before_insert",
                "after_insert",
            ],
        )
        eq_(
            methods2,
            [
                "instrument_class",
                "init_instance",
                "before_insert",
                "after_insert",
            ],
        )

        del methods1[:]
        del methods2[:]
        i1.keywords.append(k1)
        sess.flush()
        eq_(methods1, ["before_update", "after_update"])
        eq_(methods2, [])

    def test_inheritance_with_dupes(self):
        """Inheritance with the same extension instance on both mappers."""

        users, addresses, User = (
            self.tables.users,
            self.tables.addresses,
            self.classes.User,
        )

        Ext, methods = self.extension()

        class AdminUser(User):
            pass

        ext = Ext()
        with testing.expect_deprecated(
            "MapperExtension is deprecated in favor of the MapperEvents",
            "MapperExtension.before_insert is deprecated",
            "MapperExtension.instrument_class is deprecated",
            "MapperExtension.init_instance is deprecated",
            "MapperExtension.after_insert is deprecated",
            "MapperExtension.reconstruct_instance is deprecated",
            "MapperExtension.before_delete is deprecated",
            "MapperExtension.after_delete is deprecated",
            "MapperExtension.before_update is deprecated",
            "MapperExtension.after_update is deprecated",
            "MapperExtension.init_failed is deprecated",
        ):
            mapper(User, users, extension=ext)

        with testing.expect_deprecated(
            "MapperExtension is deprecated in favor of the MapperEvents"
        ):
            mapper(
                AdminUser,
                addresses,
                inherits=User,
                extension=ext,
                properties={"address_id": addresses.c.id},
            )

        sess = create_session()
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
            methods,
            [
                "instrument_class",
                "instrument_class",
                "init_instance",
                "before_insert",
                "after_insert",
                "reconstruct_instance",
                "before_update",
                "after_update",
                "before_delete",
                "after_delete",
            ],
        )

    def test_unnecessary_methods_not_evented(self):
        users = self.tables.users

        class MyExtension(MapperExtension):
            def before_insert(self, mapper, connection, instance):
                pass

        class Foo(object):
            pass

        with testing.expect_deprecated(
            "MapperExtension is deprecated in favor of the MapperEvents",
            "MapperExtension.before_insert is deprecated",
        ):
            m = mapper(Foo, users, extension=MyExtension())
        assert not m.class_manager.dispatch.load
        assert not m.dispatch.before_update
        assert len(m.dispatch.before_insert) == 1


class DeprecatedSessionExtensionTest(_fixtures.FixtureTest):
    run_inserts = None

    def test_extension(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users)
        log = []

        class MyExt(SessionExtension):
            def before_commit(self, session):
                log.append("before_commit")

            def after_commit(self, session):
                log.append("after_commit")

            def after_rollback(self, session):
                log.append("after_rollback")

            def before_flush(self, session, flush_context, objects):
                log.append("before_flush")

            def after_flush(self, session, flush_context):
                log.append("after_flush")

            def after_flush_postexec(self, session, flush_context):
                log.append("after_flush_postexec")

            def after_begin(self, session, transaction, connection):
                log.append("after_begin")

            def after_attach(self, session, instance):
                log.append("after_attach")

            def after_bulk_update(self, session, query, query_context, result):
                log.append("after_bulk_update")

            def after_bulk_delete(self, session, query, query_context, result):
                log.append("after_bulk_delete")

        with testing.expect_deprecated(
            "SessionExtension is deprecated in favor of " "the SessionEvents",
            "SessionExtension.before_commit is deprecated",
            "SessionExtension.after_commit is deprecated",
            "SessionExtension.after_begin is deprecated",
            "SessionExtension.after_attach is deprecated",
            "SessionExtension.before_flush is deprecated",
            "SessionExtension.after_flush is deprecated",
            "SessionExtension.after_flush_postexec is deprecated",
            "SessionExtension.after_rollback is deprecated",
            "SessionExtension.after_bulk_update is deprecated",
            "SessionExtension.after_bulk_delete is deprecated",
        ):
            sess = create_session(extension=MyExt())
        u = User(name="u1")
        sess.add(u)
        sess.flush()
        assert log == [
            "after_attach",
            "before_flush",
            "after_begin",
            "after_flush",
            "after_flush_postexec",
            "before_commit",
            "after_commit",
        ]
        log = []
        with testing.expect_deprecated(
            "SessionExtension is deprecated in favor of " "the SessionEvents",
            "SessionExtension.before_commit is deprecated",
            "SessionExtension.after_commit is deprecated",
            "SessionExtension.after_begin is deprecated",
            "SessionExtension.after_attach is deprecated",
            "SessionExtension.before_flush is deprecated",
            "SessionExtension.after_flush is deprecated",
            "SessionExtension.after_flush_postexec is deprecated",
            "SessionExtension.after_rollback is deprecated",
            "SessionExtension.after_bulk_update is deprecated",
            "SessionExtension.after_bulk_delete is deprecated",
        ):
            sess = create_session(autocommit=False, extension=MyExt())
        u = User(name="u1")
        sess.add(u)
        sess.flush()
        assert log == [
            "after_attach",
            "before_flush",
            "after_begin",
            "after_flush",
            "after_flush_postexec",
        ]
        log = []
        u.name = "ed"
        sess.commit()
        assert log == [
            "before_commit",
            "before_flush",
            "after_flush",
            "after_flush_postexec",
            "after_commit",
        ]
        log = []
        sess.commit()
        assert log == ["before_commit", "after_commit"]
        log = []
        sess.query(User).delete()
        assert log == ["after_begin", "after_bulk_delete"]
        log = []
        sess.query(User).update({"name": "foo"})
        assert log == ["after_bulk_update"]
        log = []
        with testing.expect_deprecated(
            "SessionExtension is deprecated in favor of " "the SessionEvents",
            "SessionExtension.before_commit is deprecated",
            "SessionExtension.after_commit is deprecated",
            "SessionExtension.after_begin is deprecated",
            "SessionExtension.after_attach is deprecated",
            "SessionExtension.before_flush is deprecated",
            "SessionExtension.after_flush is deprecated",
            "SessionExtension.after_flush_postexec is deprecated",
            "SessionExtension.after_rollback is deprecated",
            "SessionExtension.after_bulk_update is deprecated",
            "SessionExtension.after_bulk_delete is deprecated",
        ):
            sess = create_session(
                autocommit=False, extension=MyExt(), bind=testing.db
            )
        sess.connection()
        assert log == ["after_begin"]
        sess.close()

    def test_multiple_extensions(self):
        User, users = self.classes.User, self.tables.users

        log = []

        class MyExt1(SessionExtension):
            def before_commit(self, session):
                log.append("before_commit_one")

        class MyExt2(SessionExtension):
            def before_commit(self, session):
                log.append("before_commit_two")

        mapper(User, users)
        with testing.expect_deprecated(
            "SessionExtension is deprecated in favor of " "the SessionEvents",
            "SessionExtension.before_commit is deprecated",
        ):
            sess = create_session(extension=[MyExt1(), MyExt2()])
        u = User(name="u1")
        sess.add(u)
        sess.flush()
        assert log == ["before_commit_one", "before_commit_two"]

    def test_unnecessary_methods_not_evented(self):
        class MyExtension(SessionExtension):
            def before_commit(self, session):
                pass

        with testing.expect_deprecated(
            "SessionExtension is deprecated in favor of " "the SessionEvents",
            "SessionExtension.before_commit is deprecated.",
        ):
            s = Session(extension=MyExtension())
        assert not s.dispatch.after_commit
        assert len(s.dispatch.before_commit) == 1


class DeprecatedAttributeExtensionTest1(fixtures.ORMTest):
    def test_extension_commit_attr(self):
        """test that an extension which commits attribute history
        maintains the end-result history.

        This won't work in conjunction with some unitofwork extensions.

        """

        class Foo(fixtures.BasicEntity):
            pass

        class Bar(fixtures.BasicEntity):
            pass

        class ReceiveEvents(AttributeExtension):
            def __init__(self, key):
                self.key = key

            def append(self, state, child, initiator):
                if commit:
                    state._commit_all(state.dict)
                return child

            def remove(self, state, child, initiator):
                if commit:
                    state._commit_all(state.dict)
                return child

            def set(self, state, child, oldchild, initiator):
                if commit:
                    state._commit_all(state.dict)
                return child

        instrumentation.register_class(Foo)
        instrumentation.register_class(Bar)

        b1, b2, b3, b4 = Bar(id="b1"), Bar(id="b2"), Bar(id="b3"), Bar(id="b4")

        def loadcollection(state, passive):
            if passive is attributes.PASSIVE_NO_FETCH:
                return attributes.PASSIVE_NO_RESULT
            return [b1, b2]

        def loadscalar(state, passive):
            if passive is attributes.PASSIVE_NO_FETCH:
                return attributes.PASSIVE_NO_RESULT
            return b2

        with testing.expect_deprecated(
            "AttributeExtension.append is deprecated.",
            "AttributeExtension.remove is deprecated.",
            "AttributeExtension.set is deprecated.",
        ):
            attributes.register_attribute(
                Foo,
                "bars",
                uselist=True,
                useobject=True,
                callable_=loadcollection,
                extension=[ReceiveEvents("bars")],
            )

        with testing.expect_deprecated(
            "AttributeExtension.append is deprecated.",
            "AttributeExtension.remove is deprecated.",
            "AttributeExtension.set is deprecated.",
        ):
            attributes.register_attribute(
                Foo,
                "bar",
                uselist=False,
                useobject=True,
                callable_=loadscalar,
                extension=[ReceiveEvents("bar")],
            )

        with testing.expect_deprecated(
            "AttributeExtension.append is deprecated.",
            "AttributeExtension.remove is deprecated.",
            "AttributeExtension.set is deprecated.",
        ):
            attributes.register_attribute(
                Foo,
                "scalar",
                uselist=False,
                useobject=False,
                extension=[ReceiveEvents("scalar")],
            )

        def create_hist():
            def hist(key, fn, *arg):
                attributes.instance_state(f1)._commit_all(
                    attributes.instance_dict(f1)
                )
                fn(*arg)
                histories.append(attributes.get_history(f1, key))

            f1 = Foo()
            hist("bars", f1.bars.append, b3)
            hist("bars", f1.bars.append, b4)
            hist("bars", f1.bars.remove, b2)
            hist("bar", setattr, f1, "bar", b3)
            hist("bar", setattr, f1, "bar", None)
            hist("bar", setattr, f1, "bar", b4)
            hist("scalar", setattr, f1, "scalar", 5)
            hist("scalar", setattr, f1, "scalar", None)
            hist("scalar", setattr, f1, "scalar", 4)

        histories = []
        commit = False
        create_hist()
        without_commit = list(histories)
        histories[:] = []
        commit = True
        create_hist()
        with_commit = histories
        for without, with_ in zip(without_commit, with_commit):
            woc = without
            wic = with_
            eq_(woc, wic)

    def test_extension_lazyload_assertion(self):
        class Foo(fixtures.BasicEntity):
            pass

        class Bar(fixtures.BasicEntity):
            pass

        class ReceiveEvents(AttributeExtension):
            def append(self, state, child, initiator):
                state.obj().bars
                return child

            def remove(self, state, child, initiator):
                state.obj().bars
                return child

            def set(self, state, child, oldchild, initiator):
                return child

        instrumentation.register_class(Foo)
        instrumentation.register_class(Bar)

        bar1, bar2, bar3 = [Bar(id=1), Bar(id=2), Bar(id=3)]

        def func1(state, passive):
            if passive is attributes.PASSIVE_NO_FETCH:
                return attributes.PASSIVE_NO_RESULT

            return [bar1, bar2, bar3]

        with testing.expect_deprecated(
            "AttributeExtension.append is deprecated.",
            "AttributeExtension.remove is deprecated.",
            "AttributeExtension.set is deprecated.",
        ):
            attributes.register_attribute(
                Foo,
                "bars",
                uselist=True,
                callable_=func1,
                useobject=True,
                extension=[ReceiveEvents()],
            )
        attributes.register_attribute(
            Bar, "foos", uselist=True, useobject=True, backref="bars"
        )

        x = Foo()
        assert_raises(AssertionError, Bar(id=4).foos.append, x)

        x.bars
        b = Bar(id=4)
        b.foos.append(x)
        attributes.instance_state(x)._expire_attributes(
            attributes.instance_dict(x), ["bars"]
        )
        assert_raises(AssertionError, b.foos.remove, x)

    def test_scalar_listener(self):

        # listeners on ScalarAttributeImpl aren't used normally. test that
        # they work for the benefit of user extensions

        class Foo(object):

            pass

        results = []

        class ReceiveEvents(AttributeExtension):
            def append(self, state, child, initiator):
                assert False

            def remove(self, state, child, initiator):
                results.append(("remove", state.obj(), child))

            def set(self, state, child, oldchild, initiator):
                results.append(("set", state.obj(), child, oldchild))
                return child

        instrumentation.register_class(Foo)
        with testing.expect_deprecated(
            "AttributeExtension.append is deprecated.",
            "AttributeExtension.remove is deprecated.",
            "AttributeExtension.set is deprecated.",
        ):
            attributes.register_attribute(
                Foo,
                "x",
                uselist=False,
                useobject=False,
                extension=ReceiveEvents(),
            )

        f = Foo()
        f.x = 5
        f.x = 17
        del f.x

        eq_(
            results,
            [
                ("set", f, 5, attributes.NEVER_SET),
                ("set", f, 17, 5),
                ("remove", f, 17),
            ],
        )

    def test_cascading_extensions(self):
        t1 = Table(
            "t1",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("type", String(40)),
            Column("data", String(50)),
        )

        ext_msg = []

        class Ex1(AttributeExtension):
            def set(self, state, value, oldvalue, initiator):
                ext_msg.append("Ex1 %r" % value)
                return "ex1" + value

        class Ex2(AttributeExtension):
            def set(self, state, value, oldvalue, initiator):
                ext_msg.append("Ex2 %r" % value)
                return "ex2" + value

        class A(fixtures.BasicEntity):
            pass

        class B(A):
            pass

        class C(B):
            pass

        with testing.expect_deprecated(
            "AttributeExtension is deprecated in favor of the "
            "AttributeEvents listener interface.  "
            "The column_property.extension parameter"
        ):
            mapper(
                A,
                t1,
                polymorphic_on=t1.c.type,
                polymorphic_identity="a",
                properties={
                    "data": column_property(t1.c.data, extension=Ex1())
                },
            )
        mapper(B, polymorphic_identity="b", inherits=A)
        with testing.expect_deprecated(
            "AttributeExtension is deprecated in favor of the "
            "AttributeEvents listener interface.  "
            "The column_property.extension parameter"
        ):
            mapper(
                C,
                polymorphic_identity="c",
                inherits=B,
                properties={
                    "data": column_property(t1.c.data, extension=Ex2())
                },
            )

        with testing.expect_deprecated(
            "AttributeExtension.set is deprecated. "
        ):
            configure_mappers()

        a1 = A(data="a1")
        b1 = B(data="b1")
        c1 = C(data="c1")

        eq_(a1.data, "ex1a1")
        eq_(b1.data, "ex1b1")
        eq_(c1.data, "ex2c1")

        a1.data = "a2"
        b1.data = "b2"
        c1.data = "c2"
        eq_(a1.data, "ex1a2")
        eq_(b1.data, "ex1b2")
        eq_(c1.data, "ex2c2")

        eq_(
            ext_msg,
            [
                "Ex1 'a1'",
                "Ex1 'b1'",
                "Ex2 'c1'",
                "Ex1 'a2'",
                "Ex1 'b2'",
                "Ex2 'c2'",
            ],
        )


class DeprecatedOptionAllTest(OptionsPathTest, _fixtures.FixtureTest):
    run_inserts = "once"
    run_deletes = None

    def _mapper_fixture_one(self):
        users, User, addresses, Address, orders, Order = (
            self.tables.users,
            self.classes.User,
            self.tables.addresses,
            self.classes.Address,
            self.tables.orders,
            self.classes.Order,
        )
        keywords, items, item_keywords, Keyword, Item = (
            self.tables.keywords,
            self.tables.items,
            self.tables.item_keywords,
            self.classes.Keyword,
            self.classes.Item,
        )
        mapper(
            User,
            users,
            properties={
                "addresses": relationship(Address),
                "orders": relationship(Order),
            },
        )
        mapper(Address, addresses)
        mapper(
            Order,
            orders,
            properties={
                "items": relationship(Item, secondary=self.tables.order_items)
            },
        )
        mapper(
            Keyword,
            keywords,
            properties={
                "keywords": column_property(keywords.c.name + "some keyword")
            },
        )
        mapper(
            Item,
            items,
            properties=dict(
                keywords=relationship(Keyword, secondary=item_keywords)
            ),
        )

    def _assert_eager_with_entity_exception(
        self, entity_list, options, message
    ):
        assert_raises_message(
            sa.exc.ArgumentError,
            message,
            create_session().query(*entity_list).options,
            *options
        )

    def test_option_against_nonexistent_twolevel_all(self):
        self._mapper_fixture_one()
        Item = self.classes.Item
        with testing.expect_deprecated(
            r"The joinedload_all\(\) function is deprecated, and "
            "will be removed in a future release.  "
            r"Please use method chaining with joinedload\(\)"
        ):
            self._assert_eager_with_entity_exception(
                [Item],
                (joinedload_all("keywords.foo"),),
                'Can\'t find property named \\"foo\\" on mapped class '
                "Keyword->keywords in this Query.",
            )

    def test_all_path_vs_chained(self):
        self._mapper_fixture_one()
        User = self.classes.User
        Order = self.classes.Order
        Item = self.classes.Item

        with testing.expect_deprecated(
            r"The joinedload_all\(\) function is deprecated, and "
            "will be removed in a future release.  "
            r"Please use method chaining with joinedload\(\)"
        ):
            l1 = joinedload_all("orders.items.keywords")

        sess = Session()
        q = sess.query(User)
        self._assert_path_result(
            l1,
            q,
            [
                (User, "orders"),
                (User, "orders", Order, "items"),
                (User, "orders", Order, "items", Item, "keywords"),
            ],
        )

        l2 = joinedload("orders").joinedload("items").joinedload("keywords")
        self._assert_path_result(
            l2,
            q,
            [
                (User, "orders"),
                (User, "orders", Order, "items"),
                (User, "orders", Order, "items", Item, "keywords"),
            ],
        )

    def test_subqueryload_mapper_order_by(self):
        users, User, Address, addresses = (
            self.tables.users,
            self.classes.User,
            self.classes.Address,
            self.tables.addresses,
        )

        mapper(Address, addresses)

        with testing.expect_deprecated(
            ".*Mapper.order_by parameter is deprecated"
        ):
            mapper(
                User,
                users,
                properties={
                    "addresses": relationship(
                        Address, lazy="subquery", order_by=addresses.c.id
                    )
                },
                order_by=users.c.id.desc(),
            )

        sess = create_session()
        q = sess.query(User)

        result = q.limit(2).all()
        eq_(result, list(reversed(self.static.user_address_result[2:4])))

    def test_selectinload_mapper_order_by(self):
        users, User, Address, addresses = (
            self.tables.users,
            self.classes.User,
            self.classes.Address,
            self.tables.addresses,
        )

        mapper(Address, addresses)
        with testing.expect_deprecated(
            ".*Mapper.order_by parameter is deprecated"
        ):
            mapper(
                User,
                users,
                properties={
                    "addresses": relationship(
                        Address, lazy="selectin", order_by=addresses.c.id
                    )
                },
                order_by=users.c.id.desc(),
            )

        sess = create_session()
        q = sess.query(User)

        result = q.limit(2).all()
        eq_(result, list(reversed(self.static.user_address_result[2:4])))

    def test_join_mapper_order_by(self):
        """test that mapper-level order_by is adapted to a selectable."""

        User, users = self.classes.User, self.tables.users

        with testing.expect_deprecated(
            ".*Mapper.order_by parameter is deprecated"
        ):
            mapper(User, users, order_by=users.c.id)

        sel = users.select(users.c.id.in_([7, 8]))
        sess = create_session()

        eq_(
            sess.query(User).select_entity_from(sel).all(),
            [User(name="jack", id=7), User(name="ed", id=8)],
        )

    def test_defer_addtl_attrs(self):
        users, User, Address, addresses = (
            self.tables.users,
            self.classes.User,
            self.classes.Address,
            self.tables.addresses,
        )

        mapper(Address, addresses)
        mapper(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address, lazy="selectin", order_by=addresses.c.id
                )
            },
        )

        sess = create_session()

        with testing.expect_deprecated(
            r"The \*addl_attrs on orm.defer is deprecated.  "
            "Please use method chaining"
        ):
            sess.query(User).options(defer("addresses", "email_address"))

        with testing.expect_deprecated(
            r"The \*addl_attrs on orm.undefer is deprecated.  "
            "Please use method chaining"
        ):
            sess.query(User).options(undefer("addresses", "email_address"))


class LegacyLockModeTest(_fixtures.FixtureTest):
    run_inserts = None

    @classmethod
    def setup_mappers(cls):
        User, users = cls.classes.User, cls.tables.users
        mapper(User, users)

    def _assert_legacy(self, arg, read=False, nowait=False):
        User = self.classes.User
        s = Session()

        with testing.expect_deprecated(
            r"The Query.with_lockmode\(\) method is deprecated"
        ):
            q = s.query(User).with_lockmode(arg)
        sel = q._compile_context().statement

        if arg is None:
            assert q._for_update_arg is None
            assert sel._for_update_arg is None
            return

        assert q._for_update_arg.read is read
        assert q._for_update_arg.nowait is nowait

        assert sel._for_update_arg.read is read
        assert sel._for_update_arg.nowait is nowait

    def test_false_legacy(self):
        self._assert_legacy(None)

    def test_plain_legacy(self):
        self._assert_legacy("update")

    def test_nowait_legacy(self):
        self._assert_legacy("update_nowait", nowait=True)

    def test_read_legacy(self):
        self._assert_legacy("read", read=True)

    def test_unknown_legacy_lock_mode(self):
        User = self.classes.User
        sess = Session()
        with testing.expect_deprecated(
            r"The Query.with_lockmode\(\) method is deprecated"
        ):
            assert_raises_message(
                exc.ArgumentError,
                "Unknown with_lockmode argument: 'unknown_mode'",
                sess.query(User.id).with_lockmode,
                "unknown_mode",
            )


class InstrumentationTest(fixtures.ORMTest):
    def test_dict_subclass4(self):
        # tests #2654
        with testing.expect_deprecated(
            r"The collection.converter\(\) handler is deprecated and will "
            "be removed in a future release.  Please refer to the "
            "AttributeEvents"
        ):

            class MyDict(collections.MappedCollection):
                def __init__(self):
                    super(MyDict, self).__init__(lambda value: "k%d" % value)

                @collection.converter
                def _convert(self, dictlike):
                    for key, value in dictlike.items():
                        yield value + 5

        class Foo(object):
            pass

        instrumentation.register_class(Foo)
        attributes.register_attribute(
            Foo, "attr", uselist=True, typecallable=MyDict, useobject=True
        )

        f = Foo()
        f.attr = {"k1": 1, "k2": 2}

        eq_(f.attr, {"k7": 7, "k6": 6})

    def test_name_setup(self):
        with testing.expect_deprecated(
            r"The collection.converter\(\) handler is deprecated and will "
            "be removed in a future release.  Please refer to the "
            "AttributeEvents"
        ):

            class Base(object):
                @collection.iterator
                def base_iterate(self, x):
                    return "base_iterate"

                @collection.appender
                def base_append(self, x):
                    return "base_append"

                @collection.converter
                def base_convert(self, x):
                    return "base_convert"

                @collection.remover
                def base_remove(self, x):
                    return "base_remove"

        from sqlalchemy.orm.collections import _instrument_class

        _instrument_class(Base)

        eq_(Base._sa_remover(Base(), 5), "base_remove")
        eq_(Base._sa_appender(Base(), 5), "base_append")
        eq_(Base._sa_iterator(Base(), 5), "base_iterate")
        eq_(Base._sa_converter(Base(), 5), "base_convert")

        with testing.expect_deprecated(
            r"The collection.converter\(\) handler is deprecated and will "
            "be removed in a future release.  Please refer to the "
            "AttributeEvents"
        ):

            class Sub(Base):
                @collection.converter
                def base_convert(self, x):
                    return "sub_convert"

                @collection.remover
                def sub_remove(self, x):
                    return "sub_remove"

        _instrument_class(Sub)

        eq_(Sub._sa_appender(Sub(), 5), "base_append")
        eq_(Sub._sa_remover(Sub(), 5), "sub_remove")
        eq_(Sub._sa_iterator(Sub(), 5), "base_iterate")
        eq_(Sub._sa_converter(Sub(), 5), "sub_convert")

    def test_link_event(self):
        canary = []

        with testing.expect_deprecated(
            r"The collection.linker\(\) handler is deprecated and will "
            "be removed in a future release.  Please refer to the "
            "AttributeEvents"
        ):

            class Collection(list):
                @collection.linker
                def _on_link(self, obj):
                    canary.append(obj)

        class Foo(object):
            pass

        instrumentation.register_class(Foo)
        attributes.register_attribute(
            Foo, "attr", uselist=True, typecallable=Collection, useobject=True
        )

        f1 = Foo()
        f1.attr.append(3)

        eq_(canary, [f1.attr._sa_adapter])
        adapter_1 = f1.attr._sa_adapter

        l2 = Collection()
        f1.attr = l2
        eq_(canary, [adapter_1, f1.attr._sa_adapter, None])


class NonPrimaryRelationshipLoaderTest(_fixtures.FixtureTest):
    run_inserts = "once"
    run_deletes = None

    def test_selectload(self):
        """tests lazy loading with two relationships simultaneously,
        from the same table, using aliases.  """

        users, orders, User, Address, Order, addresses = (
            self.tables.users,
            self.tables.orders,
            self.classes.User,
            self.classes.Address,
            self.classes.Order,
            self.tables.addresses,
        )

        openorders = sa.alias(orders, "openorders")
        closedorders = sa.alias(orders, "closedorders")

        mapper(Address, addresses)

        mapper(Order, orders)

        with testing.expect_deprecated(
            "The mapper.non_primary parameter is deprecated"
        ):
            open_mapper = mapper(Order, openorders, non_primary=True)
            closed_mapper = mapper(Order, closedorders, non_primary=True)
        mapper(
            User,
            users,
            properties=dict(
                addresses=relationship(Address, lazy=True),
                open_orders=relationship(
                    open_mapper,
                    primaryjoin=sa.and_(
                        openorders.c.isopen == 1,
                        users.c.id == openorders.c.user_id,
                    ),
                    lazy="select",
                ),
                closed_orders=relationship(
                    closed_mapper,
                    primaryjoin=sa.and_(
                        closedorders.c.isopen == 0,
                        users.c.id == closedorders.c.user_id,
                    ),
                    lazy="select",
                ),
            ),
        )

        self._run_double_test(10)

    def test_joinedload(self):
        """Eager loading with two relationships simultaneously,
            from the same table, using aliases."""

        users, orders, User, Address, Order, addresses = (
            self.tables.users,
            self.tables.orders,
            self.classes.User,
            self.classes.Address,
            self.classes.Order,
            self.tables.addresses,
        )

        openorders = sa.alias(orders, "openorders")
        closedorders = sa.alias(orders, "closedorders")

        mapper(Address, addresses)
        mapper(Order, orders)

        with testing.expect_deprecated(
            "The mapper.non_primary parameter is deprecated"
        ):
            open_mapper = mapper(Order, openorders, non_primary=True)
            closed_mapper = mapper(Order, closedorders, non_primary=True)

        mapper(
            User,
            users,
            properties=dict(
                addresses=relationship(
                    Address, lazy="joined", order_by=addresses.c.id
                ),
                open_orders=relationship(
                    open_mapper,
                    primaryjoin=sa.and_(
                        openorders.c.isopen == 1,
                        users.c.id == openorders.c.user_id,
                    ),
                    lazy="joined",
                    order_by=openorders.c.id,
                ),
                closed_orders=relationship(
                    closed_mapper,
                    primaryjoin=sa.and_(
                        closedorders.c.isopen == 0,
                        users.c.id == closedorders.c.user_id,
                    ),
                    lazy="joined",
                    order_by=closedorders.c.id,
                ),
            ),
        )
        self._run_double_test(1)

    def test_selectin(self):

        users, orders, User, Address, Order, addresses = (
            self.tables.users,
            self.tables.orders,
            self.classes.User,
            self.classes.Address,
            self.classes.Order,
            self.tables.addresses,
        )

        openorders = sa.alias(orders, "openorders")
        closedorders = sa.alias(orders, "closedorders")

        mapper(Address, addresses)
        mapper(Order, orders)

        with testing.expect_deprecated(
            "The mapper.non_primary parameter is deprecated"
        ):
            open_mapper = mapper(Order, openorders, non_primary=True)
            closed_mapper = mapper(Order, closedorders, non_primary=True)

        mapper(
            User,
            users,
            properties=dict(
                addresses=relationship(
                    Address, lazy="selectin", order_by=addresses.c.id
                ),
                open_orders=relationship(
                    open_mapper,
                    primaryjoin=sa.and_(
                        openorders.c.isopen == 1,
                        users.c.id == openorders.c.user_id,
                    ),
                    lazy="selectin",
                    order_by=openorders.c.id,
                ),
                closed_orders=relationship(
                    closed_mapper,
                    primaryjoin=sa.and_(
                        closedorders.c.isopen == 0,
                        users.c.id == closedorders.c.user_id,
                    ),
                    lazy="selectin",
                    order_by=closedorders.c.id,
                ),
            ),
        )

        self._run_double_test(4)

    def test_subqueryload(self):

        users, orders, User, Address, Order, addresses = (
            self.tables.users,
            self.tables.orders,
            self.classes.User,
            self.classes.Address,
            self.classes.Order,
            self.tables.addresses,
        )

        openorders = sa.alias(orders, "openorders")
        closedorders = sa.alias(orders, "closedorders")

        mapper(Address, addresses)
        mapper(Order, orders)

        with testing.expect_deprecated(
            "The mapper.non_primary parameter is deprecated"
        ):
            open_mapper = mapper(Order, openorders, non_primary=True)
            closed_mapper = mapper(Order, closedorders, non_primary=True)

        mapper(
            User,
            users,
            properties=dict(
                addresses=relationship(
                    Address, lazy="subquery", order_by=addresses.c.id
                ),
                open_orders=relationship(
                    open_mapper,
                    primaryjoin=sa.and_(
                        openorders.c.isopen == 1,
                        users.c.id == openorders.c.user_id,
                    ),
                    lazy="subquery",
                    order_by=openorders.c.id,
                ),
                closed_orders=relationship(
                    closed_mapper,
                    primaryjoin=sa.and_(
                        closedorders.c.isopen == 0,
                        users.c.id == closedorders.c.user_id,
                    ),
                    lazy="subquery",
                    order_by=closedorders.c.id,
                ),
            ),
        )

        self._run_double_test(4)

    def _run_double_test(self, count):
        User, Address, Order, Item = self.classes(
            "User", "Address", "Order", "Item"
        )
        q = create_session().query(User).order_by(User.id)

        def go():
            eq_(
                [
                    User(
                        id=7,
                        addresses=[Address(id=1)],
                        open_orders=[Order(id=3)],
                        closed_orders=[Order(id=1), Order(id=5)],
                    ),
                    User(
                        id=8,
                        addresses=[
                            Address(id=2),
                            Address(id=3),
                            Address(id=4),
                        ],
                        open_orders=[],
                        closed_orders=[],
                    ),
                    User(
                        id=9,
                        addresses=[Address(id=5)],
                        open_orders=[Order(id=4)],
                        closed_orders=[Order(id=2)],
                    ),
                    User(id=10),
                ],
                q.all(),
            )

        self.assert_sql_count(testing.db, go, count)

        sess = create_session()
        user = sess.query(User).get(7)

        closed_mapper = User.closed_orders.entity
        open_mapper = User.open_orders.entity
        eq_(
            [Order(id=1), Order(id=5)],
            create_session()
            .query(closed_mapper)
            .with_parent(user, property="closed_orders")
            .all(),
        )
        eq_(
            [Order(id=3)],
            create_session()
            .query(open_mapper)
            .with_parent(user, property="open_orders")
            .all(),
        )


class ViewonlyFlagWarningTest(fixtures.MappedTest):
    """test for #4993.

    In 1.4, this moves to test/orm/test_cascade, deprecation warnings
    become errors, will then be for #4994.

    """

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(30)),
        )
        Table(
            "orders",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("user_id", Integer),
            Column("description", String(30)),
        )

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            pass

        class Order(cls.Comparable):
            pass

    @testing.combinations(
        ("passive_deletes", True),
        ("passive_updates", False),
        ("enable_typechecks", False),
        ("active_history", True),
        ("cascade_backrefs", False),
    )
    def test_viewonly_warning(self, flag, value):
        Order = self.classes.Order

        with testing.expect_warnings(
            r"Setting %s on relationship\(\) while also setting "
            "viewonly=True does not make sense" % flag
        ):
            kw = {
                "viewonly": True,
                "primaryjoin": self.tables.users.c.id
                == foreign(self.tables.orders.c.user_id),
            }
            kw[flag] = value
            rel = relationship(Order, **kw)

            if flag == "cascade":
                eq_(set(rel.cascade), {"delete", "delete-orphan"})
            else:
                eq_(getattr(rel, flag), value)

    @testing.combinations(
        ({"delete"}, {"delete"}),
        (
            {"all, delete-orphan"},
            {"delete", "delete-orphan", "merge", "save-update"},
        ),
        ({"save-update, expunge"}, {"save-update"}),
    )
    def test_write_cascades(self, setting, settings_that_warn):
        Order = self.classes.Order

        with testing.expect_warnings(
            r"Cascade settings \"%s\" should not be combined"
            % (", ".join(sorted(settings_that_warn)))
        ):
            relationship(
                Order,
                primaryjoin=(
                    self.tables.users.c.id
                    == foreign(self.tables.orders.c.user_id)
                ),
                cascade=", ".join(sorted(setting)),
                viewonly=True,
            )

    def test_expunge_cascade(self):
        User, Order, orders, users = (
            self.classes.User,
            self.classes.Order,
            self.tables.orders,
            self.tables.users,
        )

        mapper(Order, orders)
        mapper(
            User,
            users,
            properties={
                "orders": relationship(
                    Order,
                    primaryjoin=(
                        self.tables.users.c.id
                        == foreign(self.tables.orders.c.user_id)
                    ),
                    cascade="expunge",
                    viewonly=True,
                )
            },
        )

        sess = Session()
        u = User(id=1, name="jack")
        sess.add(u)
        sess.add_all(
            [
                Order(id=1, user_id=1, description="someorder"),
                Order(id=2, user_id=1, description="someotherorder"),
            ]
        )
        sess.commit()

        u1 = sess.query(User).first()
        orders = u1.orders
        eq_(len(orders), 2)

        in_(orders[0], sess)
        in_(orders[1], sess)

        sess.expunge(u1)

        not_in_(orders[0], sess)
        not_in_(orders[1], sess)

    def test_default_save_update_cascade(self):
        User, Order, orders, users = (
            self.classes.User,
            self.classes.Order,
            self.tables.orders,
            self.tables.users,
        )

        mapper(Order, orders)
        mapper(
            User,
            users,
            properties={
                "orders": relationship(
                    Order,
                    primaryjoin=(
                        self.tables.users.c.id
                        == foreign(self.tables.orders.c.user_id)
                    ),
                    viewonly=True,
                )
            },
        )

        sess = Session()
        u1 = User(id=1, name="jack")
        sess.add(u1)

        o1, o2 = (
            Order(id=1, user_id=1, description="someorder"),
            Order(id=2, user_id=1, description="someotherorder"),
        )

        u1.orders.append(o1)
        u1.orders.append(o2)

        # in 1.4, this becomes "not_in_"
        in_(o1, sess)
        in_(o2, sess)

    def test_default_merge_cascade(self):
        User, Order, orders, users = (
            self.classes.User,
            self.classes.Order,
            self.tables.orders,
            self.tables.users,
        )

        mapper(Order, orders)
        mapper(
            User,
            users,
            properties={
                "orders": relationship(
                    Order,
                    primaryjoin=(
                        self.tables.users.c.id
                        == foreign(self.tables.orders.c.user_id)
                    ),
                    viewonly=True,
                )
            },
        )

        sess = Session()
        u1 = User(id=1, name="jack")

        o1, o2 = (
            Order(id=1, user_id=1, description="someorder"),
            Order(id=2, user_id=1, description="someotherorder"),
        )

        u1.orders.append(o1)
        u1.orders.append(o2)

        u1 = sess.merge(u1)

        # in 1.4, this becomes "assert not u1.orders", merge does not occur
        o1, o2 = u1.orders

    def test_default_cascade_didnt_change_yet(self):
        User, Order, orders, users = (
            self.classes.User,
            self.classes.Order,
            self.tables.orders,
            self.tables.users,
        )

        mapper(Order, orders)
        umapper = mapper(
            User,
            users,
            properties={
                "orders": relationship(
                    Order,
                    primaryjoin=(
                        self.tables.users.c.id
                        == foreign(self.tables.orders.c.user_id)
                    ),
                    viewonly=True,
                )
            },
        )

        # in 1.4 this becomes {}
        eq_(umapper.attrs["orders"].cascade, {"save-update", "merge"})

    def test_write_cascade_still_works_w_viewonly(self):
        """should be no longer possible in 1.4"""

        User, Order, orders, users = (
            self.classes.User,
            self.classes.Order,
            self.tables.orders,
            self.tables.users,
        )

        mapper(Order, orders)
        with testing.expect_warnings(r"Cascade settings"):
            mapper(
                User,
                users,
                properties={
                    "orders": relationship(
                        Order,
                        primaryjoin=(
                            self.tables.users.c.id
                            == foreign(self.tables.orders.c.user_id)
                        ),
                        cascade="all, delete, delete-orphan",
                        viewonly=True,
                    )
                },
            )

        sess = Session()
        u = User(id=1, name="jack")
        sess.add(u)
        sess.add_all(
            [
                Order(id=1, user_id=1, description="someorder"),
                Order(id=2, user_id=1, description="someotherorder"),
            ]
        )
        sess.commit()
        eq_(sess.query(Order).count(), 2)

        sess.delete(u)
        sess.commit()

        eq_(sess.query(Order).count(), 0)


class NonPrimaryMapperTest(_fixtures.FixtureTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_non_primary_identity_class(self):
        User = self.classes.User
        users, addresses = self.tables.users, self.tables.addresses

        class AddressUser(User):
            pass

        mapper(User, users, polymorphic_identity="user")
        m2 = mapper(
            AddressUser,
            addresses,
            inherits=User,
            polymorphic_identity="address",
            properties={"address_id": addresses.c.id},
        )
        with testing.expect_deprecated(
            "The mapper.non_primary parameter is deprecated"
        ):
            m3 = mapper(AddressUser, addresses, non_primary=True)
        assert m3._identity_class is m2._identity_class
        eq_(
            m2.identity_key_from_instance(AddressUser()),
            m3.identity_key_from_instance(AddressUser()),
        )

    def test_illegal_non_primary(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(User, users)
        mapper(Address, addresses)
        with testing.expect_deprecated(
            "The mapper.non_primary parameter is deprecated"
        ):
            mapper(
                User,
                users,
                non_primary=True,
                properties={"addresses": relationship(Address)},
            )
        assert_raises_message(
            sa.exc.ArgumentError,
            "Attempting to assign a new relationship 'addresses' "
            "to a non-primary mapper on class 'User'",
            configure_mappers,
        )

    def test_illegal_non_primary_2(self):
        User, users = self.classes.User, self.tables.users

        with testing.expect_deprecated(
            "The mapper.non_primary parameter is deprecated"
        ):
            assert_raises_message(
                sa.exc.InvalidRequestError,
                "Configure a primary mapper first",
                mapper,
                User,
                users,
                non_primary=True,
            )

    def test_illegal_non_primary_3(self):
        users, addresses = self.tables.users, self.tables.addresses

        class Base(object):
            pass

        class Sub(Base):
            pass

        mapper(Base, users)
        with testing.expect_deprecated(
            "The mapper.non_primary parameter is deprecated"
        ):
            assert_raises_message(
                sa.exc.InvalidRequestError,
                "Configure a primary mapper first",
                mapper,
                Sub,
                addresses,
                non_primary=True,
            )
