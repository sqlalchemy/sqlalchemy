# coding: utf-8
"""Tests unitofwork operations."""

import datetime

import sqlalchemy as sa
from sqlalchemy import Boolean
from sqlalchemy import Enum
from sqlalchemy import event
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import literal
from sqlalchemy import literal_column
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import column_property
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy.orm import mapper
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm.persistence import _sort_states
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertsql import AllOf
from sqlalchemy.testing.assertsql import CompiledSQL
from sqlalchemy.testing.assertsql import Conditional
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from sqlalchemy.util import OrderedDict
from sqlalchemy.util import u
from sqlalchemy.util import ue
from test.orm import _fixtures


class UnitOfWorkTest(object):
    pass


class HistoryTest(_fixtures.FixtureTest):
    run_inserts = None

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            pass

        class Address(cls.Comparable):
            pass

    def test_backref(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        am = mapper(Address, addresses)
        m = mapper(
            User,
            users,
            properties=dict(
                addresses=relationship(am, backref="user", lazy="joined")
            ),
        )

        session = fixture_session(autocommit=False)

        u = User(name="u1")
        a = Address(email_address="u1@e")
        a.user = u
        session.add(u)

        eq_(u.addresses, [a])
        session.commit()
        session.expunge_all()

        u = session.query(m).one()
        assert u.addresses[0].user == u
        session.close()


class UnicodeTest(fixtures.MappedTest):
    __requires__ = ("unicode_connections",)

    @classmethod
    def define_tables(cls, metadata):
        uni_type = sa.Unicode(50).with_variant(
            sa.Unicode(50, collation="utf8_unicode_ci"), "mysql"
        )

        Table(
            "uni_t1",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("txt", uni_type, unique=True),
        )
        Table(
            "uni_t2",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("txt", uni_type, ForeignKey("uni_t1")),
        )

    @classmethod
    def setup_classes(cls):
        class Test(cls.Basic):
            pass

        class Test2(cls.Basic):
            pass

    def test_basic(self):
        Test, uni_t1 = self.classes.Test, self.tables.uni_t1

        mapper(Test, uni_t1)

        txt = ue("\u0160\u0110\u0106\u010c\u017d")
        t1 = Test(id=1, txt=txt)
        self.assert_(t1.txt == txt)

        session = fixture_session(autocommit=False)
        session.add(t1)
        session.commit()

        self.assert_(t1.txt == txt)

    def test_relationship(self):
        Test, uni_t2, uni_t1, Test2 = (
            self.classes.Test,
            self.tables.uni_t2,
            self.tables.uni_t1,
            self.classes.Test2,
        )

        mapper(Test, uni_t1, properties={"t2s": relationship(Test2)})
        mapper(Test2, uni_t2)

        txt = ue("\u0160\u0110\u0106\u010c\u017d")
        t1 = Test(txt=txt)
        t1.t2s.append(Test2())
        t1.t2s.append(Test2())
        session = fixture_session(autocommit=False, expire_on_commit=False)
        session.add(t1)
        session.commit()
        session.close()

        session = fixture_session()
        t1 = session.query(Test).filter_by(id=t1.id).one()
        assert len(t1.t2s) == 2


class UnicodeSchemaTest(fixtures.MappedTest):
    __requires__ = ("unicode_connections", "unicode_ddl")

    run_dispose_bind = "once"

    @classmethod
    def define_tables(cls, metadata):
        t1 = Table(
            "unitable1",
            metadata,
            Column(
                u("méil"),
                Integer,
                primary_key=True,
                key="a",
                test_needs_autoincrement=True,
            ),
            Column(ue("\u6e2c\u8a66"), Integer, key="b"),
            Column("type", String(20)),
            test_needs_fk=True,
            test_needs_autoincrement=True,
        )
        t2 = Table(
            u("Unitéble2"),
            metadata,
            Column(
                u("méil"),
                Integer,
                primary_key=True,
                key="cc",
                test_needs_autoincrement=True,
            ),
            Column(
                ue("\u6e2c\u8a66"), Integer, ForeignKey("unitable1.a"), key="d"
            ),
            Column(ue("\u6e2c\u8a66_2"), Integer, key="e"),
            test_needs_fk=True,
            test_needs_autoincrement=True,
        )

        cls.tables["t1"] = t1
        cls.tables["t2"] = t2

    def test_mapping(self):
        t2, t1 = self.tables.t2, self.tables.t1

        class A(fixtures.ComparableEntity):
            pass

        class B(fixtures.ComparableEntity):
            pass

        mapper(A, t1, properties={"t2s": relationship(B)})
        mapper(B, t2)

        a1 = A()
        b1 = B()
        a1.t2s.append(b1)

        session = fixture_session()
        session.add(a1)
        session.flush()
        session.expunge_all()

        new_a1 = session.query(A).filter(t1.c.a == a1.a).one()
        assert new_a1.a == a1.a
        assert new_a1.t2s[0].d == b1.d
        session.expunge_all()

        new_a1 = (
            session.query(A)
            .options(sa.orm.joinedload("t2s"))
            .filter(t1.c.a == a1.a)
        ).one()
        assert new_a1.a == a1.a
        assert new_a1.t2s[0].d == b1.d
        session.expunge_all()

        new_a1 = session.query(A).filter(A.a == a1.a).one()
        assert new_a1.a == a1.a
        assert new_a1.t2s[0].d == b1.d
        session.expunge_all()

    def test_inheritance_mapping(self):
        t2, t1 = self.tables.t2, self.tables.t1

        class A(fixtures.ComparableEntity):
            pass

        class B(A):
            pass

        mapper(A, t1, polymorphic_on=t1.c.type, polymorphic_identity="a")
        mapper(B, t2, inherits=A, polymorphic_identity="b")
        a1 = A(b=5)
        b1 = B(e=7)

        session = fixture_session()
        session.add_all((a1, b1))
        session.flush()
        session.expunge_all()

        eq_([A(b=5), B(e=7)], session.query(A).all())


class BinaryHistTest(fixtures.MappedTest, testing.AssertsExecutionResults):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "t1",
            metadata,
            Column(
                "id",
                sa.Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("data", sa.LargeBinary),
        )

    @classmethod
    def setup_classes(cls):
        class Foo(cls.Basic):
            pass

    @testing.requires.non_broken_binary
    def test_binary_equality(self):
        Foo, t1 = self.classes.Foo, self.tables.t1

        # data = b("this is some data")
        data = b"m\x18"  # m\xf2\r\n\x7f\x10'

        mapper(Foo, t1)

        s = fixture_session()

        f1 = Foo(data=data)
        s.add(f1)
        s.flush()
        s.expire_all()
        f1 = s.query(Foo).first()
        assert f1.data == data
        f1.data = data
        eq_(sa.orm.attributes.get_history(f1, "data"), ((), [data], ()))

        def go():
            s.flush()

        self.assert_sql_count(testing.db, go, 0)


class PKTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "multipk1",
            metadata,
            Column(
                "multi_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=not testing.against("sqlite"),
            ),
            Column("multi_rev", Integer, primary_key=True),
            Column("name", String(50), nullable=False),
            Column("value", String(100)),
        )

        Table(
            "multipk2",
            metadata,
            Column("pk_col_1", String(30), primary_key=True),
            Column("pk_col_2", String(30), primary_key=True),
            Column("data", String(30)),
        )
        Table(
            "multipk3",
            metadata,
            Column("pri_code", String(30), key="primary", primary_key=True),
            Column("sec_code", String(30), key="secondary", primary_key=True),
            Column("date_assigned", sa.Date, key="assigned", primary_key=True),
            Column("data", String(30)),
        )

    @classmethod
    def setup_classes(cls):
        class Entry(cls.Basic):
            pass

    # not supported on sqlite since sqlite's auto-pk generation only works with
    # single column primary keys
    @testing.fails_on("sqlite", "FIXME: unknown")
    def test_primary_key(self):
        Entry, multipk1 = self.classes.Entry, self.tables.multipk1

        mapper(Entry, multipk1)

        e = Entry(name="entry1", value="this is entry 1", multi_rev=2)

        session = fixture_session()
        session.add(e)
        session.flush()
        session.expunge_all()

        e2 = session.query(Entry).get((e.multi_id, 2))
        self.assert_(e is not e2)
        state = sa.orm.attributes.instance_state(e)
        state2 = sa.orm.attributes.instance_state(e2)
        eq_(state.key, state2.key)

    # this one works with sqlite since we are manually setting up pk values
    def test_manual_pk(self):
        Entry, multipk2 = self.classes.Entry, self.tables.multipk2

        mapper(Entry, multipk2)

        e = Entry(pk_col_1="pk1", pk_col_2="pk1_related", data="im the data")

        session = fixture_session()
        session.add(e)
        session.flush()

    def test_key_pks(self):
        Entry, multipk3 = self.classes.Entry, self.tables.multipk3

        mapper(Entry, multipk3)

        e = Entry(
            primary="pk1",
            secondary="pk2",
            assigned=datetime.date.today(),
            data="some more data",
        )

        session = fixture_session()
        session.add(e)
        session.flush()


class ForeignPKTest(fixtures.MappedTest):
    """Detection of the relationship direction on PK joins."""

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "people",
            metadata,
            Column("person", String(10), primary_key=True),
            Column("firstname", String(10)),
            Column("lastname", String(10)),
        )

        Table(
            "peoplesites",
            metadata,
            Column(
                "person",
                String(10),
                ForeignKey("people.person"),
                primary_key=True,
            ),
            Column("site", String(10)),
        )

    @classmethod
    def setup_classes(cls):
        class Person(cls.Basic):
            pass

        class PersonSite(cls.Basic):
            pass

    def test_basic(self):
        peoplesites, PersonSite, Person, people = (
            self.tables.peoplesites,
            self.classes.PersonSite,
            self.classes.Person,
            self.tables.people,
        )

        mapper(PersonSite, peoplesites)
        m2 = mapper(
            Person, people, properties={"sites": relationship(PersonSite)}
        )

        sa.orm.configure_mappers()
        eq_(
            list(m2.get_property("sites").synchronize_pairs),
            [(people.c.person, peoplesites.c.person)],
        )

        p = Person(person="im the key", firstname="asdf")
        ps = PersonSite(site="asdf")
        p.sites.append(ps)

        session = fixture_session()
        session.add(p)
        session.flush()

        conn = session.connection()
        p_count = conn.scalar(
            select(func.count("*")).where(people.c.person == "im the key")
        )
        eq_(p_count, 1)
        eq_(
            conn.scalar(
                select(func.count("*")).where(
                    peoplesites.c.person == "im the key"
                )
            ),
            1,
        )


class ClauseAttributesTest(fixtures.MappedTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users_t",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(30)),
            Column("counter", Integer, default=1),
        )

        Table(
            "boolean_t",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("value", Boolean),
        )

        Table(
            "pk_t",
            metadata,
            Column(
                "p_id", Integer, key="id", autoincrement=True, primary_key=True
            ),
            Column("data", String(30)),
        )

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            pass

        class HasBoolean(cls.Comparable):
            pass

        class PkDefault(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        User, users_t = cls.classes.User, cls.tables.users_t
        HasBoolean, boolean_t = cls.classes.HasBoolean, cls.tables.boolean_t
        PkDefault, pk_t = cls.classes.PkDefault, cls.tables.pk_t
        mapper(User, users_t)
        mapper(HasBoolean, boolean_t)
        mapper(PkDefault, pk_t)

    def test_update(self):
        User = self.classes.User

        u = User(name="test")

        session = fixture_session()
        session.add(u)
        session.flush()

        eq_(u.counter, 1)
        u.counter = User.counter + 1
        session.flush()

        def go():
            assert (u.counter == 2) is True  # ensure its not a ClauseElement

        self.sql_count_(1, go)

    def test_multi_update(self):
        User = self.classes.User

        u = User(name="test")

        session = fixture_session()
        session.add(u)
        session.flush()

        eq_(u.counter, 1)
        u.name = "test2"
        u.counter = User.counter + 1
        session.flush()

        def go():
            eq_(u.name, "test2")
            assert (u.counter == 2) is True

        self.sql_count_(1, go)

        session.expunge_all()
        u = session.query(User).get(u.id)
        eq_(u.name, "test2")
        eq_(u.counter, 2)

    def test_insert(self):
        User = self.classes.User

        u = User(name="test", counter=sa.select(5).scalar_subquery())

        session = fixture_session()
        session.add(u)
        session.flush()

        assert (u.counter == 5) is True

    @testing.requires.sql_expressions_inserted_as_primary_key
    def test_insert_pk_expression(self):
        PkDefault = self.classes.PkDefault

        pk = PkDefault(id=literal(5) + 10, data="some data")
        session = fixture_session()
        session.add(pk)
        session.flush()

        eq_(pk.id, 15)
        session.commit()
        eq_(pk.id, 15)

    def test_update_special_comparator(self):
        HasBoolean = self.classes.HasBoolean

        # make sure the comparison we're shooting
        # for is invalid, otherwise we need to
        # test something else here
        assert_raises_message(
            TypeError,
            "Boolean value of this clause is not defined",
            bool,
            None == sa.false(),  # noqa
        )
        s = fixture_session()
        hb = HasBoolean(value=None)
        s.add(hb)
        s.flush()

        hb.value = sa.false()

        s.flush()

        # needs to be refreshed
        assert "value" not in hb.__dict__
        eq_(hb.value, False)

    def test_clauseelement_accessor(self):
        class Thing(object):
            def __init__(self, value):
                self.value = value

            def __clause_element__(self):
                return literal_column(str(self.value))

        User = self.classes.User

        u = User(id=5, name="test", counter=Thing(3))

        session = fixture_session()
        session.add(u)
        session.flush()

        u.counter = Thing(5)
        session.flush()

        def go():
            eq_(u.counter, 5)

        self.sql_count_(1, go)


class PassiveDeletesTest(fixtures.MappedTest):
    __requires__ = ("foreign_keys",)

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "mytable",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(30)),
            test_needs_fk=True,
        )

        Table(
            "myothertable",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("parent_id", Integer),
            Column("data", String(30)),
            sa.ForeignKeyConstraint(
                ["parent_id"], ["mytable.id"], ondelete="CASCADE"
            ),
            test_needs_fk=True,
        )

    @classmethod
    def setup_classes(cls):
        class MyClass(cls.Basic):
            pass

        class MyOtherClass(cls.Basic):
            pass

    def test_basic(self):
        myothertable, MyClass, MyOtherClass, mytable = (
            self.tables.myothertable,
            self.classes.MyClass,
            self.classes.MyOtherClass,
            self.tables.mytable,
        )

        mapper(MyOtherClass, myothertable)
        mapper(
            MyClass,
            mytable,
            properties={
                "children": relationship(
                    MyOtherClass, passive_deletes=True, cascade="all"
                )
            },
        )
        with fixture_session() as session:
            mc = MyClass()
            mc.children.append(MyOtherClass())
            mc.children.append(MyOtherClass())
            mc.children.append(MyOtherClass())
            mc.children.append(MyOtherClass())

            session.add(mc)
            session.flush()
            session.expunge_all()

            conn = session.connection()

            eq_(
                conn.scalar(select(func.count("*")).select_from(myothertable)),
                4,
            )
            mc = session.query(MyClass).get(mc.id)
            session.delete(mc)
            session.flush()

            eq_(conn.scalar(select(func.count("*")).select_from(mytable)), 0)
            eq_(
                conn.scalar(select(func.count("*")).select_from(myothertable)),
                0,
            )

    @testing.emits_warning(
        r".*'passive_deletes' is normally configured on one-to-many"
    )
    def test_backwards_pd(self):
        """Test that passive_deletes=True disables a delete from an m2o.

        This is not the usual usage and it now raises a warning, but test
        that it works nonetheless.

        """

        myothertable, MyClass, MyOtherClass, mytable = (
            self.tables.myothertable,
            self.classes.MyClass,
            self.classes.MyOtherClass,
            self.tables.mytable,
        )

        mapper(
            MyOtherClass,
            myothertable,
            properties={
                "myclass": relationship(
                    MyClass, cascade="all, delete", passive_deletes=True
                )
            },
        )
        mapper(MyClass, mytable)

        session = fixture_session()
        mc = MyClass()
        mco = MyOtherClass()
        mco.myclass = mc
        session.add(mco)
        session.commit()

        eq_(session.scalar(select(func.count("*")).select_from(mytable)), 1)
        eq_(
            session.scalar(select(func.count("*")).select_from(myothertable)),
            1,
        )

        session.expire(mco, ["myclass"])
        session.delete(mco)
        session.commit()

        # mytable wasn't deleted, is the point.
        eq_(session.scalar(select(func.count("*")).select_from(mytable)), 1)
        eq_(
            session.scalar(select(func.count("*")).select_from(myothertable)),
            0,
        )

    def test_aaa_m2o_emits_warning(self):
        myothertable, MyClass, MyOtherClass, mytable = (
            self.tables.myothertable,
            self.classes.MyClass,
            self.classes.MyOtherClass,
            self.tables.mytable,
        )

        mapper(
            MyOtherClass,
            myothertable,
            properties={
                "myclass": relationship(
                    MyClass, cascade="all, delete", passive_deletes=True
                )
            },
        )
        mapper(MyClass, mytable)
        assert_raises(sa.exc.SAWarning, sa.orm.configure_mappers)


class BatchDeleteIgnoresRowcountTest(fixtures.DeclarativeMappedTest):
    __requires__ = ("foreign_keys", "recursive_fk_cascade")

    @classmethod
    def setup_classes(cls):
        class A(cls.DeclarativeBasic):
            __tablename__ = "A"
            __table_args__ = dict(test_needs_fk=True)
            __mapper_args__ = {"confirm_deleted_rows": False}
            id = Column(Integer, primary_key=True)
            parent_id = Column(Integer, ForeignKey("A.id", ondelete="CASCADE"))

    def test_delete_both(self):
        A = self.classes.A
        session = Session(testing.db)

        a1, a2 = A(id=1), A(id=2, parent_id=1)

        session.add_all([a1, a2])
        session.flush()

        session.delete(a1)
        session.delete(a2)

        # no issue with multi-row count here
        session.flush()


class ExtraPassiveDeletesTest(fixtures.MappedTest):
    __requires__ = ("foreign_keys",)

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "mytable",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(30)),
            test_needs_fk=True,
        )

        Table(
            "myothertable",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("parent_id", Integer),
            Column("data", String(30)),
            # no CASCADE, the same as ON DELETE RESTRICT
            sa.ForeignKeyConstraint(["parent_id"], ["mytable.id"]),
            test_needs_fk=True,
        )

    @classmethod
    def setup_classes(cls):
        class MyClass(cls.Basic):
            pass

        class MyOtherClass(cls.Basic):
            pass

    def test_extra_passive(self):
        myothertable, MyClass, MyOtherClass, mytable = (
            self.tables.myothertable,
            self.classes.MyClass,
            self.classes.MyOtherClass,
            self.tables.mytable,
        )

        mapper(MyOtherClass, myothertable)
        mapper(
            MyClass,
            mytable,
            properties={
                "children": relationship(
                    MyOtherClass, passive_deletes="all", cascade="save-update"
                )
            },
        )

        with fixture_session(expire_on_commit=False) as session:
            mc = MyClass()
            mc.children.append(MyOtherClass())
            mc.children.append(MyOtherClass())
            mc.children.append(MyOtherClass())
            mc.children.append(MyOtherClass())
            session.add(mc)
            session.commit()

        with fixture_session(expire_on_commit=False) as session:
            conn = session.connection()
            eq_(
                conn.scalar(select(func.count("*")).select_from(myothertable)),
                4,
            )
            mc = session.query(MyClass).get(mc.id)
            session.delete(mc)
            assert_raises(sa.exc.DBAPIError, session.flush)

    def test_extra_passive_2(self):
        myothertable, MyClass, MyOtherClass, mytable = (
            self.tables.myothertable,
            self.classes.MyClass,
            self.classes.MyOtherClass,
            self.tables.mytable,
        )

        mapper(MyOtherClass, myothertable)
        mapper(
            MyClass,
            mytable,
            properties={
                "children": relationship(
                    MyOtherClass, passive_deletes="all", cascade="save-update"
                )
            },
        )

        with fixture_session(expire_on_commit=False) as session:
            mc = MyClass()
            mc.children.append(MyOtherClass())
            session.add(mc)
            session.commit()

        with fixture_session(autoflush=False) as session:
            conn = session.connection()
            eq_(
                conn.scalar(select(func.count("*")).select_from(myothertable)),
                1,
            )

            mc = session.query(MyClass).get(mc.id)
            session.delete(mc)
            mc.children[0].data = "some new data"
            assert_raises(sa.exc.DBAPIError, session.flush)

    def test_extra_passive_obj_removed_o2m(self):
        myothertable, MyClass, MyOtherClass, mytable = (
            self.tables.myothertable,
            self.classes.MyClass,
            self.classes.MyOtherClass,
            self.tables.mytable,
        )

        mapper(MyOtherClass, myothertable)
        mapper(
            MyClass,
            mytable,
            properties={
                "children": relationship(MyOtherClass, passive_deletes="all")
            },
        )

        session = fixture_session()
        mc = MyClass()
        moc1 = MyOtherClass()
        moc2 = MyOtherClass()
        mc.children.append(moc1)
        mc.children.append(moc2)
        session.add_all([mc, moc1, moc2])
        session.flush()

        mc.children.remove(moc1)
        mc.children.remove(moc2)
        moc1.data = "foo"
        session.flush()

        eq_(moc1.parent_id, mc.id)
        eq_(moc2.parent_id, mc.id)

    def test_dont_emit(self):
        myothertable, MyClass, MyOtherClass, mytable = (
            self.tables.myothertable,
            self.classes.MyClass,
            self.classes.MyOtherClass,
            self.tables.mytable,
        )

        mapper(MyOtherClass, myothertable)
        mapper(
            MyClass,
            mytable,
            properties={
                "children": relationship(
                    MyOtherClass, passive_deletes="all", cascade="save-update"
                )
            },
        )
        session = fixture_session()
        mc = MyClass()
        session.add(mc)
        session.commit()
        mc.id

        session.delete(mc)

        # no load for "children" should occur
        self.assert_sql_count(testing.db, session.flush, 1)


class ColumnCollisionTest(fixtures.MappedTest):
    """Ensure the mapper doesn't break bind param naming rules on flush."""

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "book",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("book_id", String(50)),
            Column("title", String(50)),
        )

    def test_naming(self):
        book = self.tables.book

        class Book(fixtures.ComparableEntity):
            pass

        mapper(Book, book)
        with fixture_session() as sess:

            b1 = Book(book_id="abc", title="def")
            sess.add(b1)
            sess.flush()

            b1.title = "ghi"
            sess.flush()
            sess.commit()

        with fixture_session() as sess:
            eq_(sess.query(Book).first(), Book(book_id="abc", title="ghi"))


class DefaultTest(fixtures.MappedTest):
    """Exercise mappings on columns with DefaultGenerators.

    Tests that when saving objects whose table contains DefaultGenerators,
    either python-side, preexec or database-side, the newly saved instances
    receive all the default values either through a post-fetch or getting the
    pre-exec'ed defaults back from the engine.

    """

    @classmethod
    def define_tables(cls, metadata):
        use_string_defaults = testing.against(
            "postgresql", "oracle", "sqlite", "mssql"
        )

        if use_string_defaults:
            hohotype = String(30)
            hohoval = "im hoho"
            althohoval = "im different hoho"
        else:
            hohotype = Integer
            hohoval = 9
            althohoval = 15

        cls.other["hohoval"] = hohoval
        cls.other["althohoval"] = althohoval

        dt = Table(
            "default_t",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("hoho", hohotype, server_default=str(hohoval)),
            Column(
                "counter",
                Integer,
                default=sa.func.char_length("1234567", type_=Integer),
            ),
            Column(
                "foober",
                String(30),
                default="im foober",
                onupdate="im the update",
            ),
            mysql_engine="MyISAM",
        )

        st = Table(
            "secondary_table",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(50)),
            mysql_engine="MyISAM",
        )

        if testing.against("postgresql", "oracle"):
            dt.append_column(
                Column(
                    "secondary_id",
                    Integer,
                    sa.Sequence("sec_id_seq"),
                    unique=True,
                )
            )
            st.append_column(
                Column("fk_val", Integer, ForeignKey("default_t.secondary_id"))
            )
        elif testing.against("mssql"):
            st.append_column(
                Column("fk_val", Integer, ForeignKey("default_t.id"))
            )
        else:
            st.append_column(
                Column("hoho", hohotype, ForeignKey("default_t.hoho"))
            )

    @classmethod
    def setup_classes(cls):
        class Hoho(cls.Comparable):
            pass

        class Secondary(cls.Comparable):
            pass

    @testing.fails_on("firebird", "Data type unknown on the parameter")
    def test_insert(self):
        althohoval, hohoval, default_t, Hoho = (
            self.other.althohoval,
            self.other.hohoval,
            self.tables.default_t,
            self.classes.Hoho,
        )

        mapper(Hoho, default_t)

        h1 = Hoho(hoho=althohoval)
        h2 = Hoho(counter=12)
        h3 = Hoho(hoho=althohoval, counter=12)
        h4 = Hoho()
        h5 = Hoho(foober="im the new foober")

        session = fixture_session(autocommit=False, expire_on_commit=False)
        session.add_all((h1, h2, h3, h4, h5))
        session.commit()

        eq_(h1.hoho, althohoval)
        eq_(h3.hoho, althohoval)

        def go():
            # test deferred load of attributes, one select per instance
            self.assert_(h2.hoho == h4.hoho == h5.hoho == hohoval)

        self.sql_count_(3, go)

        def go():
            self.assert_(h1.counter == h4.counter == h5.counter == 7)

        self.sql_count_(1, go)

        def go():
            self.assert_(h3.counter == h2.counter == 12)
            self.assert_(h2.foober == h3.foober == h4.foober == "im foober")
            self.assert_(h5.foober == "im the new foober")

        self.sql_count_(0, go)

        session.expunge_all()

        (h1, h2, h3, h4, h5) = session.query(Hoho).order_by(Hoho.id).all()

        eq_(h1.hoho, althohoval)
        eq_(h3.hoho, althohoval)
        self.assert_(h2.hoho == h4.hoho == h5.hoho == hohoval)
        self.assert_(h3.counter == h2.counter == 12)
        self.assert_(h1.counter == h4.counter == h5.counter == 7)
        self.assert_(h2.foober == h3.foober == h4.foober == "im foober")
        eq_(h5.foober, "im the new foober")

    @testing.fails_on("firebird", "Data type unknown on the parameter")
    @testing.fails_on("oracle+cx_oracle", "seems like a cx_oracle bug")
    def test_eager_defaults(self):
        hohoval, default_t, Hoho = (
            self.other.hohoval,
            self.tables.default_t,
            self.classes.Hoho,
        )
        Secondary = self.classes.Secondary

        mapper(
            Hoho,
            default_t,
            eager_defaults=True,
            properties={
                "sec": relationship(Secondary),
                "syn": sa.orm.synonym(default_t.c.counter),
            },
        )

        mapper(Secondary, self.tables.secondary_table)
        h1 = Hoho()

        session = fixture_session()
        session.add(h1)

        if testing.db.dialect.implicit_returning:
            self.sql_count_(1, session.flush)
        else:
            self.sql_count_(2, session.flush)

        self.sql_count_(0, lambda: eq_(h1.hoho, hohoval))

        # no actual eager defaults, make sure error isn't raised
        h2 = Hoho(hoho=hohoval, counter=5)
        session.add(h2)
        session.flush()
        eq_(h2.hoho, hohoval)
        eq_(h2.counter, 5)

    def test_insert_nopostfetch(self):
        default_t, Hoho = self.tables.default_t, self.classes.Hoho

        # populates from the FetchValues explicitly so there is no
        # "post-update"
        mapper(Hoho, default_t)

        h1 = Hoho(hoho="15", counter=15)
        session = fixture_session()
        session.add(h1)
        session.flush()

        def go():
            eq_(h1.hoho, "15")
            eq_(h1.counter, 15)
            eq_(h1.foober, "im foober")

        self.sql_count_(0, go)

    @testing.fails_on("firebird", "Data type unknown on the parameter")
    def test_update(self):
        default_t, Hoho = self.tables.default_t, self.classes.Hoho

        mapper(Hoho, default_t)

        h1 = Hoho()
        session = fixture_session()
        session.add(h1)
        session.flush()

        eq_(h1.foober, "im foober")
        h1.counter = 19
        session.flush()
        eq_(h1.foober, "im the update")

    @testing.fails_on("firebird", "Data type unknown on the parameter")
    def test_used_in_relationship(self):
        """A server-side default can be used as the target of a foreign key"""

        Hoho, hohoval, default_t, secondary_table, Secondary = (
            self.classes.Hoho,
            self.other.hohoval,
            self.tables.default_t,
            self.tables.secondary_table,
            self.classes.Secondary,
        )

        mapper(
            Hoho,
            default_t,
            properties={
                "secondaries": relationship(
                    Secondary, order_by=secondary_table.c.id
                )
            },
        )
        mapper(Secondary, secondary_table)

        h1 = Hoho()
        s1 = Secondary(data="s1")
        h1.secondaries.append(s1)

        session = fixture_session()
        session.add(h1)
        session.flush()
        session.expunge_all()

        eq_(
            session.query(Hoho).get(h1.id),
            Hoho(hoho=hohoval, secondaries=[Secondary(data="s1")]),
        )

        h1 = session.query(Hoho).get(h1.id)
        h1.secondaries.append(Secondary(data="s2"))
        session.flush()
        session.expunge_all()

        eq_(
            session.query(Hoho).get(h1.id),
            Hoho(
                hoho=hohoval,
                secondaries=[Secondary(data="s1"), Secondary(data="s2")],
            ),
        )


class ColumnPropertyTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "data",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("a", String(50)),
            Column("b", String(50)),
        )

        Table(
            "subdata",
            metadata,
            Column("id", Integer, ForeignKey("data.id"), primary_key=True),
            Column("c", String(50)),
        )

    @classmethod
    def setup_mappers(cls):
        class Data(cls.Basic):
            pass

    def test_refreshes(self):
        Data, data = self.classes.Data, self.tables.data

        mapper(
            Data,
            data,
            properties={
                "aplusb": column_property(
                    data.c.a + literal_column("' '") + data.c.b
                )
            },
        )
        self._test(True)

    def test_no_refresh_ro_column_property_no_expire_on_flush(self):
        Data, data = self.classes.Data, self.tables.data

        mapper(
            Data,
            data,
            properties={
                "aplusb": column_property(
                    data.c.a + literal_column("' '") + data.c.b,
                    expire_on_flush=False,
                )
            },
        )
        self._test(False)

    def test_no_refresh_ro_column_property_expire_on_flush(self):
        Data, data = self.classes.Data, self.tables.data

        mapper(
            Data,
            data,
            properties={
                "aplusb": column_property(
                    data.c.a + literal_column("' '") + data.c.b,
                    expire_on_flush=True,
                )
            },
        )
        self._test(True)

    def test_no_refresh_ro_deferred_no_expire_on_flush(self):
        Data, data = self.classes.Data, self.tables.data

        mapper(
            Data,
            data,
            properties={
                "aplusb": column_property(
                    data.c.a + literal_column("' '") + data.c.b,
                    expire_on_flush=False,
                    deferred=True,
                )
            },
        )
        self._test(False, expect_deferred_load=True)

    def test_no_refresh_ro_deferred_expire_on_flush(self):
        Data, data = self.classes.Data, self.tables.data

        mapper(
            Data,
            data,
            properties={
                "aplusb": column_property(
                    data.c.a + literal_column("' '") + data.c.b,
                    expire_on_flush=True,
                    deferred=True,
                )
            },
        )
        self._test(True, expect_deferred_load=True)

    def test_refreshes_post_init(self):
        Data, data = self.classes.Data, self.tables.data

        m = mapper(Data, data)
        m.add_property(
            "aplusb",
            column_property(data.c.a + literal_column("' '") + data.c.b),
        )
        self._test(True)

    def test_with_inheritance(self):
        subdata, data, Data = (
            self.tables.subdata,
            self.tables.data,
            self.classes.Data,
        )

        class SubData(Data):
            pass

        mapper(
            Data,
            data,
            properties={
                "aplusb": column_property(
                    data.c.a + literal_column("' '") + data.c.b
                )
            },
        )
        mapper(SubData, subdata, inherits=Data)

        sess = fixture_session()
        sd1 = SubData(a="hello", b="there", c="hi")
        sess.add(sd1)
        sess.flush()
        eq_(sd1.aplusb, "hello there")

    def _test(self, expect_expiry, expect_deferred_load=False):
        Data = self.classes.Data

        with fixture_session() as sess:

            d1 = Data(a="hello", b="there")
            sess.add(d1)
            sess.flush()

            eq_(d1.aplusb, "hello there")

            d1.b = "bye"
            sess.flush()
            if expect_expiry:
                eq_(d1.aplusb, "hello bye")
            else:
                eq_(d1.aplusb, "hello there")

            d1.b = "foobar"
            d1.aplusb = "im setting this explicitly"
            sess.flush()
            eq_(d1.aplusb, "im setting this explicitly")

            sess.commit()

        # test issue #3984.
        # NOTE: if we only expire_all() here rather than start with brand new
        # 'd1', d1.aplusb since it was loaded moves into "expired" and stays
        # "undeferred".  this is questionable but not as severe as the never-
        # loaded attribute being loaded during an unexpire.

        with fixture_session() as sess:
            d1 = sess.query(Data).first()

            d1.b = "so long"
            sess.flush()
            sess.expire_all()
            eq_(d1.b, "so long")
            if expect_deferred_load:
                eq_("aplusb" in d1.__dict__, False)
            else:
                eq_("aplusb" in d1.__dict__, True)
            eq_(d1.aplusb, "hello so long")


class OneToManyTest(_fixtures.FixtureTest):
    run_inserts = None

    def test_one_to_many_1(self):
        """Basic save of one to many."""

        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        mapper(
            User,
            users,
            properties=dict(
                addresses=relationship(
                    mapper(Address, addresses), lazy="select"
                )
            ),
        )
        u = User(name="one2manytester")
        a = Address(email_address="one2many@test.org")
        u.addresses.append(a)

        a2 = Address(email_address="lala@test.org")
        u.addresses.append(a2)

        session = fixture_session()
        session.add(u)
        session.flush()

        conn = session.connection()
        user_rows = conn.execute(
            users.select(users.c.id.in_([u.id]))
        ).fetchall()
        eq_(list(user_rows[0]), [u.id, "one2manytester"])

        address_rows = conn.execute(
            addresses.select(
                addresses.c.id.in_([a.id, a2.id]),
                order_by=[addresses.c.email_address],
            )
        ).fetchall()
        eq_(list(address_rows[0]), [a2.id, u.id, "lala@test.org"])
        eq_(list(address_rows[1]), [a.id, u.id, "one2many@test.org"])

        userid = u.id
        addressid = a2.id

        a2.email_address = "somethingnew@foo.com"

        session.flush()

        address_rows = conn.execute(
            addresses.select(addresses.c.id == addressid)
        ).fetchall()
        eq_(list(address_rows[0]), [addressid, userid, "somethingnew@foo.com"])
        self.assert_(u.id == userid and a2.id == addressid)

    def test_one_to_many_2(self):
        """Modifying the child items of an object."""

        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        mapper(
            User,
            users,
            properties=dict(
                addresses=relationship(
                    mapper(Address, addresses), lazy="select"
                )
            ),
        )

        u1 = User(name="user1")
        u1.addresses = []
        a1 = Address(email_address="emailaddress1")
        u1.addresses.append(a1)

        u2 = User(name="user2")
        u2.addresses = []
        a2 = Address(email_address="emailaddress2")
        u2.addresses.append(a2)

        a3 = Address(email_address="emailaddress3")

        session = fixture_session()
        session.add_all((u1, u2, a3))
        session.flush()

        # modify user2 directly, append an address to user1.
        # upon commit, user2 should be updated, user1 should not
        # both address1 and address3 should be updated
        u2.name = "user2modified"
        u1.addresses.append(a3)
        del u1.addresses[0]

        self.assert_sql(
            testing.db,
            session.flush,
            [
                (
                    "UPDATE users SET name=:name "
                    "WHERE users.id = :users_id",
                    {"users_id": u2.id, "name": "user2modified"},
                ),
                (
                    "UPDATE addresses SET user_id=:user_id "
                    "WHERE addresses.id = :addresses_id",
                    [
                        {"user_id": None, "addresses_id": a1.id},
                        {"user_id": u1.id, "addresses_id": a3.id},
                    ],
                ),
            ],
        )

    def test_child_move(self):
        """Moving a child from one parent to another, with a delete.

        Tests that deleting the first parent properly updates the child with
        the new parent.  This tests the 'trackparent' option in the attributes
        module.

        """

        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        mapper(
            User,
            users,
            properties=dict(
                addresses=relationship(
                    mapper(Address, addresses), lazy="select"
                )
            ),
        )

        u1 = User(name="user1")
        u2 = User(name="user2")
        a = Address(email_address="address1")
        u1.addresses.append(a)

        session = fixture_session()
        session.add_all((u1, u2))
        session.flush()

        del u1.addresses[0]
        u2.addresses.append(a)
        session.delete(u1)

        session.flush()
        session.expunge_all()

        u2 = session.query(User).get(u2.id)
        eq_(len(u2.addresses), 1)

    def test_child_move_2(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        mapper(
            User,
            users,
            properties=dict(
                addresses=relationship(
                    mapper(Address, addresses), lazy="select"
                )
            ),
        )

        u1 = User(name="user1")
        u2 = User(name="user2")
        a = Address(email_address="address1")
        u1.addresses.append(a)

        session = fixture_session()
        session.add_all((u1, u2))
        session.flush()

        del u1.addresses[0]
        u2.addresses.append(a)

        session.flush()
        session.expunge_all()

        u2 = session.query(User).get(u2.id)
        eq_(len(u2.addresses), 1)

    def test_o2m_delete_parent(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        mapper(
            User,
            users,
            properties=dict(
                address=relationship(
                    mapper(Address, addresses), lazy="select", uselist=False
                )
            ),
        )

        u = User(name="one2onetester")
        a = Address(email_address="myonlyaddress@foo.com")
        u.address = a

        session = fixture_session()
        session.add(u)
        session.flush()

        session.delete(u)
        session.flush()

        assert a.id is not None
        assert a.user_id is None
        assert sa.orm.attributes.instance_state(a).key in session.identity_map
        assert (
            sa.orm.attributes.instance_state(u).key not in session.identity_map
        )

    def test_one_to_one(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        mapper(
            User,
            users,
            properties=dict(
                address=relationship(
                    mapper(Address, addresses), lazy="select", uselist=False
                )
            ),
        )

        u = User(name="one2onetester")
        u.address = Address(email_address="myonlyaddress@foo.com")

        session = fixture_session()
        session.add(u)
        session.flush()

        u.name = "imnew"
        session.flush()

        u.address.email_address = "imnew@foo.com"
        session.flush()

    def test_bidirectional(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        m1 = mapper(User, users)
        mapper(
            Address,
            addresses,
            properties=dict(
                user=relationship(m1, lazy="joined", backref="addresses")
            ),
        )

        u = User(name="test")
        Address(email_address="testaddress", user=u)

        session = fixture_session()
        session.add(u)
        session.flush()
        session.delete(u)
        session.flush()

    def test_double_relationship(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        m2 = mapper(Address, addresses)
        mapper(
            User,
            users,
            properties={
                "boston_addresses": relationship(
                    m2,
                    primaryjoin=sa.and_(
                        users.c.id == addresses.c.user_id,
                        addresses.c.email_address.like("%boston%"),
                    ),
                    overlaps="newyork_addresses",
                ),
                "newyork_addresses": relationship(
                    m2,
                    primaryjoin=sa.and_(
                        users.c.id == addresses.c.user_id,
                        addresses.c.email_address.like("%newyork%"),
                    ),
                    overlaps="boston_addresses",
                ),
            },
        )

        u = User(name="u1")
        a = Address(email_address="foo@boston.com")
        b = Address(email_address="bar@newyork.com")
        u.boston_addresses.append(a)
        u.newyork_addresses.append(b)

        session = fixture_session()
        session.add(u)
        session.flush()


class SaveTest(_fixtures.FixtureTest):
    run_inserts = None

    def test_basic(self):
        User, users = self.classes.User, self.tables.users

        m = mapper(User, users)

        # save two users
        u = User(name="savetester")
        u2 = User(name="savetester2")

        with fixture_session() as session:
            session.add_all((u, u2))
            session.flush()

            # assert the first one retrieves the same from the identity map
            nu = session.query(m).get(u.id)
            assert u is nu

            # clear out the identity map, so next get forces a SELECT
            session.expunge_all()

            # check it again, identity should be different but ids the same
            nu = session.query(m).get(u.id)
            assert u is not nu and u.id == nu.id and nu.name == "savetester"

            session.commit()

        # change first users name and save
        with fixture_session() as session:
            session.add(u)
            u.name = "modifiedname"
            assert u in session.dirty
            session.flush()

            # select both
            userlist = (
                session.query(User)
                .filter(users.c.id.in_([u.id, u2.id]))
                .order_by(users.c.name)
                .all()
            )

            eq_(u.id, userlist[0].id)
            eq_(userlist[0].name, "modifiedname")
            eq_(u2.id, userlist[1].id)
            eq_(userlist[1].name, "savetester2")

    def test_synonym(self):
        users = self.tables.users

        class SUser(fixtures.BasicEntity):
            def _get_name(self):
                return "User:" + self.name

            def _set_name(self, name):
                self.name = name + ":User"

            syn_name = property(_get_name, _set_name)

        mapper(SUser, users, properties={"syn_name": sa.orm.synonym("name")})

        u = SUser(syn_name="some name")
        eq_(u.syn_name, "User:some name:User")

        session = fixture_session()
        session.add(u)
        session.flush()
        session.expunge_all()

        u = session.query(SUser).first()
        eq_(u.syn_name, "User:some name:User")

    def test_lazyattr_commit(self):
        """Lazily loaded relationships.

        When a lazy-loaded list is unloaded, and a commit occurs, that the
        'passive' call on that list does not blow away its value

        """

        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(
            User,
            users,
            properties={"addresses": relationship(mapper(Address, addresses))},
        )

        u = User(name="u1")
        u.addresses.append(Address(email_address="u1@e1"))
        u.addresses.append(Address(email_address="u1@e2"))
        u.addresses.append(Address(email_address="u1@e3"))
        u.addresses.append(Address(email_address="u1@e4"))

        session = fixture_session()
        session.add(u)
        session.flush()
        session.expunge_all()

        u = session.query(User).one()
        u.name = "newname"
        session.flush()
        eq_(len(u.addresses), 4)

    def test_inherits(self):
        """a user object that also has the users mailing address."""

        users, addresses, User = (
            self.tables.users,
            self.tables.addresses,
            self.classes.User,
        )

        m1 = mapper(User, users)

        class AddressUser(User):
            pass

        # define a mapper for AddressUser that inherits the User.mapper, and
        # joins on the id column
        mapper(
            AddressUser,
            addresses,
            inherits=m1,
            properties={"address_id": addresses.c.id},
        )

        au = AddressUser(name="u", email_address="u@e")

        session = fixture_session()
        session.add(au)
        session.flush()
        session.expunge_all()

        rt = session.query(AddressUser).one()
        eq_(au.user_id, rt.user_id)
        eq_(rt.id, rt.id)

    def test_deferred(self):
        """Deferred column operations"""

        orders, Order = self.tables.orders, self.classes.Order

        mapper(
            Order,
            orders,
            properties={"description": sa.orm.deferred(orders.c.description)},
        )

        # don't set deferred attribute, commit session
        o = Order(id=42)
        session = fixture_session(autocommit=False)
        session.add(o)
        session.commit()

        # assert that changes get picked up
        o.description = "foo"
        session.commit()

        eq_(
            list(session.execute(orders.select(), mapper=Order)),
            [(42, None, None, "foo", None)],
        )
        session.expunge_all()

        # assert that a set operation doesn't trigger a load operation
        o = session.query(Order).filter(Order.description == "foo").one()

        def go():
            o.description = "hoho"

        self.sql_count_(0, go)
        session.flush()

        eq_(
            list(session.execute(orders.select(), mapper=Order)),
            [(42, None, None, "hoho", None)],
        )

        session.expunge_all()

        # test assigning None to an unloaded deferred also works
        o = session.query(Order).filter(Order.description == "hoho").one()
        o.description = None
        session.flush()
        eq_(
            list(session.execute(orders.select(), mapper=Order)),
            [(42, None, None, None, None)],
        )
        session.close()

    # why no support on oracle ?  because oracle doesn't save
    # "blank" strings; it saves a single space character.
    @testing.fails_on("oracle", "FIXME: unknown")
    def test_dont_update_blanks(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users)

        u = User(name="")
        session = fixture_session()
        session.add(u)
        session.flush()
        session.expunge_all()

        u = session.query(User).get(u.id)
        u.name = ""
        self.sql_count_(0, session.flush)

    def test_multi_table_selectable(self):
        """Mapped selectables that span tables.

        Also tests redefinition of the keynames for the column properties.

        """

        addresses, users, User = (
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        usersaddresses = sa.join(
            users, addresses, users.c.id == addresses.c.user_id
        )

        m = mapper(
            User,
            usersaddresses,
            properties=dict(
                email=addresses.c.email_address,
                foo_id=[users.c.id, addresses.c.user_id],
            ),
        )

        u = User(name="multitester", email="multi@test.org")
        session = fixture_session()
        session.add(u)
        session.flush()
        session.expunge_all()

        id_ = m.primary_key_from_instance(u)

        u = session.query(User).get(id_)
        assert u.name == "multitester"

        conn = session.connection()
        user_rows = conn.execute(
            users.select(users.c.id.in_([u.foo_id]))
        ).fetchall()
        eq_(list(user_rows[0]), [u.foo_id, "multitester"])
        address_rows = conn.execute(
            addresses.select(addresses.c.id.in_([u.id]))
        ).fetchall()
        eq_(list(address_rows[0]), [u.id, u.foo_id, "multi@test.org"])

        u.email = "lala@hey.com"
        u.name = "imnew"
        session.flush()

        user_rows = conn.execute(
            users.select(users.c.id.in_([u.foo_id]))
        ).fetchall()
        eq_(list(user_rows[0]), [u.foo_id, "imnew"])
        address_rows = conn.execute(
            addresses.select(addresses.c.id.in_([u.id]))
        ).fetchall()
        eq_(list(address_rows[0]), [u.id, u.foo_id, "lala@hey.com"])

        session.expunge_all()
        u = session.query(User).get(id_)
        assert u.name == "imnew"

    def test_history_get(self):
        """The history lazy-fetches data when it wasn't otherwise loaded."""

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
                    Address, cascade="all, delete-orphan"
                )
            },
        )
        mapper(Address, addresses)

        u = User(name="u1")
        u.addresses.append(Address(email_address="u1@e1"))
        u.addresses.append(Address(email_address="u1@e2"))
        session = fixture_session()
        session.add(u)
        session.flush()
        session.expunge_all()

        u = session.query(User).get(u.id)
        session.delete(u)
        session.flush()
        eq_(
            session.connection().scalar(
                select(func.count("*")).select_from(users)
            ),
            0,
        )
        eq_(
            session.connection().scalar(
                select(func.count("*")).select_from(addresses)
            ),
            0,
        )

    def test_batch_mode(self):
        """The 'batch=False' flag on mapper()"""

        users, User = self.tables.users, self.classes.User

        names = []

        class Events(object):
            def before_insert(self, mapper, connection, instance):
                self.current_instance = instance
                names.append(instance.name)

            def after_insert(self, mapper, connection, instance):
                assert instance is self.current_instance

        mapper(User, users, batch=False)

        evt = Events()
        event.listen(User, "before_insert", evt.before_insert)
        event.listen(User, "after_insert", evt.after_insert)

        u1 = User(name="user1")
        u2 = User(name="user2")

        session = fixture_session()
        session.add_all((u1, u2))
        session.flush()

        u3 = User(name="user3")
        u4 = User(name="user4")
        u5 = User(name="user5")

        session.add_all([u4, u5, u3])
        session.flush()

        # test insert ordering is maintained
        assert names == ["user1", "user2", "user4", "user5", "user3"]
        session.expunge_all()

        sa.orm.clear_mappers()

        mapper(User, users)
        evt = Events()
        event.listen(User, "before_insert", evt.before_insert)
        event.listen(User, "after_insert", evt.after_insert)

        u1 = User(name="user1")
        u2 = User(name="user2")
        session.add_all((u1, u2))
        assert_raises(AssertionError, session.flush)


class ManyToOneTest(_fixtures.FixtureTest):
    run_inserts = None

    def test_m2o_one_to_one(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(
            Address,
            addresses,
            properties=dict(
                user=relationship(
                    mapper(User, users), lazy="select", uselist=False
                )
            ),
        )

        session = fixture_session()

        data = [
            {"name": "thesub", "email_address": "bar@foo.com"},
            {"name": "assdkfj", "email_address": "thesdf@asdf.com"},
            {"name": "n4knd", "email_address": "asf3@bar.org"},
            {"name": "v88f4", "email_address": "adsd5@llala.net"},
            {"name": "asdf8d", "email_address": "theater@foo.com"},
        ]
        objects = []
        for elem in data:
            a = Address()
            a.email_address = elem["email_address"]
            a.user = User()
            a.user.name = elem["name"]
            objects.append(a)
            session.add(a)

        session.flush()
        objects[2].email_address = "imnew@foo.bar"
        objects[3].user = User()
        objects[3].user.name = "imnewlyadded"
        self.assert_sql_execution(
            testing.db,
            session.flush,
            CompiledSQL(
                "INSERT INTO users (name) " "VALUES (:name)",
                {"name": "imnewlyadded"},
            ),
            AllOf(
                CompiledSQL(
                    "UPDATE addresses "
                    "SET email_address=:email_address "
                    "WHERE addresses.id = :addresses_id",
                    lambda ctx: {
                        "email_address": "imnew@foo.bar",
                        "addresses_id": objects[2].id,
                    },
                ),
                CompiledSQL(
                    "UPDATE addresses "
                    "SET user_id=:user_id "
                    "WHERE addresses.id = :addresses_id",
                    lambda ctx: {
                        "user_id": objects[3].user.id,
                        "addresses_id": objects[3].id,
                    },
                ),
            ),
        )

        conn = session.connection()
        result = conn.execute(
            sa.select(users, addresses).where(
                sa.and_(
                    users.c.id == addresses.c.user_id, addresses.c.id == a.id
                ),
            )
        )
        eq_(
            list(result.first()),
            [a.user.id, "asdf8d", a.id, a.user_id, "theater@foo.com"],
        )

    def test_many_to_one_1(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(
            Address,
            addresses,
            properties=dict(
                user=relationship(mapper(User, users), lazy="select")
            ),
        )

        a1 = Address(email_address="emailaddress1")
        u1 = User(name="user1")
        a1.user = u1

        session = fixture_session()
        session.add(a1)
        session.flush()
        session.expunge_all()

        a1 = session.query(Address).get(a1.id)
        u1 = session.query(User).get(u1.id)
        assert a1.user is u1

        a1.user = None
        session.flush()
        session.expunge_all()
        a1 = session.query(Address).get(a1.id)
        u1 = session.query(User).get(u1.id)
        assert a1.user is None

    def test_many_to_one_2(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(
            Address,
            addresses,
            properties=dict(
                user=relationship(mapper(User, users), lazy="select")
            ),
        )

        a1 = Address(email_address="emailaddress1")
        a2 = Address(email_address="emailaddress2")
        u1 = User(name="user1")
        a1.user = u1

        session = fixture_session()
        session.add_all((a1, a2))
        session.flush()
        session.expunge_all()

        a1 = session.query(Address).get(a1.id)
        a2 = session.query(Address).get(a2.id)
        u1 = session.query(User).get(u1.id)
        assert a1.user is u1

        a1.user = None
        a2.user = u1
        session.flush()
        session.expunge_all()

        a1 = session.query(Address).get(a1.id)
        a2 = session.query(Address).get(a2.id)
        u1 = session.query(User).get(u1.id)
        assert a1.user is None
        assert a2.user is u1

    def test_many_to_one_3(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(
            Address,
            addresses,
            properties=dict(
                user=relationship(mapper(User, users), lazy="select")
            ),
        )

        a1 = Address(email_address="emailaddress1")
        u1 = User(name="user1")
        u2 = User(name="user2")
        a1.user = u1

        session = fixture_session()
        session.add_all((a1, u1, u2))
        session.flush()
        session.expunge_all()

        a1 = session.query(Address).get(a1.id)
        u1 = session.query(User).get(u1.id)
        u2 = session.query(User).get(u2.id)
        assert a1.user is u1

        a1.user = u2
        session.flush()
        session.expunge_all()
        a1 = session.query(Address).get(a1.id)
        u1 = session.query(User).get(u1.id)
        u2 = session.query(User).get(u2.id)
        assert a1.user is u2

    def test_bidirectional_no_load(self):
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
                    Address, backref="user", lazy="noload"
                )
            },
        )
        mapper(Address, addresses)

        # try it on unsaved objects
        u1 = User(name="u1")
        a1 = Address(email_address="e1")
        a1.user = u1

        session = fixture_session()
        session.add(u1)
        session.flush()
        session.expunge_all()

        a1 = session.query(Address).get(a1.id)

        a1.user = None
        session.flush()
        session.expunge_all()
        assert session.query(Address).get(a1.id).user is None
        assert session.query(User).get(u1.id).addresses == []


class ManyToManyTest(_fixtures.FixtureTest):
    run_inserts = None

    def test_many_to_many(self):
        keywords, items, item_keywords, Keyword, Item = (
            self.tables.keywords,
            self.tables.items,
            self.tables.item_keywords,
            self.classes.Keyword,
            self.classes.Item,
        )

        mapper(Keyword, keywords)

        mapper(
            Item,
            items,
            properties=dict(
                keywords=relationship(
                    Keyword,
                    item_keywords,
                    lazy="joined",
                    order_by=keywords.c.name,
                )
            ),
        )

        data = [
            Item,
            {
                "description": "mm_item1",
                "keywords": (
                    Keyword,
                    [
                        {"name": "big"},
                        {"name": "green"},
                        {"name": "purple"},
                        {"name": "round"},
                    ],
                ),
            },
            {
                "description": "mm_item2",
                "keywords": (
                    Keyword,
                    [
                        {"name": "blue"},
                        {"name": "imnew"},
                        {"name": "round"},
                        {"name": "small"},
                    ],
                ),
            },
            {"description": "mm_item3", "keywords": (Keyword, [])},
            {
                "description": "mm_item4",
                "keywords": (Keyword, [{"name": "big"}, {"name": "blue"}]),
            },
            {
                "description": "mm_item5",
                "keywords": (
                    Keyword,
                    [{"name": "big"}, {"name": "exacting"}, {"name": "green"}],
                ),
            },
            {
                "description": "mm_item6",
                "keywords": (
                    Keyword,
                    [{"name": "red"}, {"name": "round"}, {"name": "small"}],
                ),
            },
        ]

        session = fixture_session()

        objects = []
        _keywords = dict([(k.name, k) for k in session.query(Keyword)])

        for elem in data[1:]:
            item = Item(description=elem["description"])
            objects.append(item)

            for spec in elem["keywords"][1]:
                keyword_name = spec["name"]
                try:
                    kw = _keywords[keyword_name]
                except KeyError:
                    _keywords[keyword_name] = kw = Keyword(name=keyword_name)
                item.keywords.append(kw)

        session.add_all(objects)
        session.flush()

        result = (
            session.query(Item)
            .filter(Item.description.in_([e["description"] for e in data[1:]]))
            .order_by(Item.description)
            .all()
        )
        self.assert_result(result, *data)

        objects[4].description = "item4updated"
        k = Keyword()
        k.name = "yellow"
        objects[5].keywords.append(k)
        self.assert_sql_execution(
            testing.db,
            session.flush,
            CompiledSQL(
                "UPDATE items SET description=:description "
                "WHERE items.id = :items_id",
                {"description": "item4updated", "items_id": objects[4].id},
            ),
            CompiledSQL(
                "INSERT INTO keywords (name) " "VALUES (:name)",
                {"name": "yellow"},
            ),
            CompiledSQL(
                "INSERT INTO item_keywords (item_id, keyword_id) "
                "VALUES (:item_id, :keyword_id)",
                lambda ctx: [{"item_id": objects[5].id, "keyword_id": k.id}],
            ),
        )

        objects[2].keywords.append(k)
        dkid = objects[5].keywords[1].id
        del objects[5].keywords[1]
        self.assert_sql_execution(
            testing.db,
            session.flush,
            CompiledSQL(
                "DELETE FROM item_keywords "
                "WHERE item_keywords.item_id = :item_id AND "
                "item_keywords.keyword_id = :keyword_id",
                [{"item_id": objects[5].id, "keyword_id": dkid}],
            ),
            CompiledSQL(
                "INSERT INTO item_keywords (item_id, keyword_id) "
                "VALUES (:item_id, :keyword_id)",
                lambda ctx: [{"item_id": objects[2].id, "keyword_id": k.id}],
            ),
        )

        session.delete(objects[3])
        session.flush()

    def test_many_to_many_remove(self):
        """Setting a collection to empty deletes many-to-many rows.

        Tests that setting a list-based attribute to '[]' properly affects the
        history and allows the many-to-many rows to be deleted

        """

        keywords, items, item_keywords, Keyword, Item = (
            self.tables.keywords,
            self.tables.items,
            self.tables.item_keywords,
            self.classes.Keyword,
            self.classes.Item,
        )

        mapper(Keyword, keywords)
        mapper(
            Item,
            items,
            properties=dict(
                keywords=relationship(Keyword, item_keywords, lazy="joined")
            ),
        )

        i = Item(description="i1")
        k1 = Keyword(name="k1")
        k2 = Keyword(name="k2")
        i.keywords.append(k1)
        i.keywords.append(k2)

        session = fixture_session()
        session.add(i)
        session.flush()

        conn = session.connection()
        eq_(conn.scalar(select(func.count("*")).select_from(item_keywords)), 2)
        i.keywords = []
        session.flush()
        eq_(conn.scalar(select(func.count("*")).select_from(item_keywords)), 0)

    def test_scalar(self):
        """sa.dependency won't delete an m2m relationship referencing None."""

        keywords, items, item_keywords, Keyword, Item = (
            self.tables.keywords,
            self.tables.items,
            self.tables.item_keywords,
            self.classes.Keyword,
            self.classes.Item,
        )

        mapper(Keyword, keywords)

        mapper(
            Item,
            items,
            properties=dict(
                keyword=relationship(
                    Keyword, secondary=item_keywords, uselist=False
                )
            ),
        )

        i = Item(description="x")
        session = fixture_session()
        session.add(i)
        session.flush()
        session.delete(i)
        session.flush()

    def test_many_to_many_update(self):
        """Assorted history operations on a many to many"""

        keywords, items, item_keywords, Keyword, Item = (
            self.tables.keywords,
            self.tables.items,
            self.tables.item_keywords,
            self.classes.Keyword,
            self.classes.Item,
        )

        mapper(Keyword, keywords)
        mapper(
            Item,
            items,
            properties=dict(
                keywords=relationship(
                    Keyword,
                    secondary=item_keywords,
                    lazy="joined",
                    order_by=keywords.c.name,
                )
            ),
        )

        k1 = Keyword(name="keyword 1")
        k2 = Keyword(name="keyword 2")
        k3 = Keyword(name="keyword 3")

        item = Item(description="item 1")
        item.keywords.extend([k1, k2, k3])

        session = fixture_session()
        session.add(item)
        session.flush()

        item.keywords = []
        item.keywords.append(k1)
        item.keywords.append(k2)
        session.flush()

        session.expunge_all()
        item = session.query(Item).get(item.id)
        assert item.keywords == [k1, k2]

    def test_association(self):
        """Basic test of an association object"""

        keywords, items, item_keywords, Keyword, Item = (
            self.tables.keywords,
            self.tables.items,
            self.tables.item_keywords,
            self.classes.Keyword,
            self.classes.Item,
        )

        class IKAssociation(fixtures.ComparableEntity):
            pass

        mapper(Keyword, keywords)

        mapper(
            IKAssociation,
            item_keywords,
            primary_key=[item_keywords.c.item_id, item_keywords.c.keyword_id],
            properties=dict(
                keyword=relationship(
                    Keyword,
                    lazy="joined",
                    uselist=False,
                    # note here is a valid place where
                    # order_by can be used on a scalar
                    # relationship(); to determine eager
                    # ordering of the parent object within
                    # its collection.
                    order_by=keywords.c.name,
                )
            ),
        )

        mapper(
            Item,
            items,
            properties=dict(
                keywords=relationship(IKAssociation, lazy="joined")
            ),
        )

        session = fixture_session()

        def fixture():
            _kw = dict([(k.name, k) for k in session.query(Keyword)])
            for n in (
                "big",
                "green",
                "purple",
                "round",
                "huge",
                "violet",
                "yellow",
                "blue",
            ):
                if n not in _kw:
                    _kw[n] = Keyword(name=n)

            def assocs(*names):
                return [
                    IKAssociation(keyword=kw) for kw in [_kw[n] for n in names]
                ]

            return [
                Item(
                    description="a_item1",
                    keywords=assocs("big", "green", "purple", "round"),
                ),
                Item(
                    description="a_item2",
                    keywords=assocs("huge", "violet", "yellow"),
                ),
                Item(description="a_item3", keywords=assocs("big", "blue")),
            ]

        session.add_all(fixture())
        session.flush()
        eq_(fixture(), session.query(Item).order_by(Item.description).all())


class SaveTest2(_fixtures.FixtureTest):
    run_inserts = None

    def test_m2o_nonmatch(self):
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
            properties=dict(
                user=relationship(User, lazy="select", uselist=False)
            ),
        )

        session = fixture_session()

        def fixture():
            return [
                Address(email_address="a1", user=User(name="u1")),
                Address(email_address="a2", user=User(name="u2")),
            ]

        session.add_all(fixture())

        self.assert_sql_execution(
            testing.db,
            session.flush,
            Conditional(
                testing.db.dialect.insert_executemany_returning,
                [
                    CompiledSQL(
                        "INSERT INTO users (name) VALUES (:name)",
                        [{"name": "u1"}, {"name": "u2"}],
                    ),
                    CompiledSQL(
                        "INSERT INTO addresses (user_id, email_address) "
                        "VALUES (:user_id, :email_address)",
                        [
                            {"user_id": 1, "email_address": "a1"},
                            {"user_id": 2, "email_address": "a2"},
                        ],
                    ),
                ],
                [
                    CompiledSQL(
                        "INSERT INTO users (name) VALUES (:name)",
                        {"name": "u1"},
                    ),
                    CompiledSQL(
                        "INSERT INTO users (name) VALUES (:name)",
                        {"name": "u2"},
                    ),
                    CompiledSQL(
                        "INSERT INTO addresses (user_id, email_address) "
                        "VALUES (:user_id, :email_address)",
                        {"user_id": 1, "email_address": "a1"},
                    ),
                    CompiledSQL(
                        "INSERT INTO addresses (user_id, email_address) "
                        "VALUES (:user_id, :email_address)",
                        {"user_id": 2, "email_address": "a2"},
                    ),
                ],
            ),
        )


class SaveTest3(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "items",
            metadata,
            Column(
                "item_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("item_name", String(50)),
        )

        Table(
            "keywords",
            metadata,
            Column(
                "keyword_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("name", String(50)),
        )

        Table(
            "assoc",
            metadata,
            Column("item_id", Integer, ForeignKey("items")),
            Column("keyword_id", Integer, ForeignKey("keywords")),
            Column("foo", sa.Boolean, default=True),
        )

    @classmethod
    def setup_classes(cls):
        class Keyword(cls.Basic):
            pass

        class Item(cls.Basic):
            pass

    def test_manytomany_xtracol_delete(self):
        """A many-to-many on a table that has an extra column can properly
        delete rows from the table without referencing the extra column"""

        keywords, items, assoc, Keyword, Item = (
            self.tables.keywords,
            self.tables.items,
            self.tables.assoc,
            self.classes.Keyword,
            self.classes.Item,
        )

        mapper(Keyword, keywords)
        mapper(
            Item,
            items,
            properties=dict(
                keywords=relationship(Keyword, secondary=assoc, lazy="joined")
            ),
        )

        i = Item()
        k1 = Keyword()
        k2 = Keyword()
        i.keywords.append(k1)
        i.keywords.append(k2)

        session = fixture_session()
        session.add(i)
        session.flush()

        eq_(
            session.connection().scalar(
                select(func.count("*")).select_from(assoc)
            ),
            2,
        )
        i.keywords = []
        session.flush()
        eq_(
            session.connection().scalar(
                select(func.count("*")).select_from(assoc)
            ),
            0,
        )


class BooleanColTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "t1_t",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(30)),
            Column("value", sa.Boolean),
        )

    def test_boolean(self):
        t1_t = self.tables.t1_t

        # use the regular mapper
        class T(fixtures.ComparableEntity):
            pass

        mapper(T, t1_t)

        sess = fixture_session()
        t1 = T(value=True, name="t1")
        t2 = T(value=False, name="t2")
        t3 = T(value=True, name="t3")
        sess.add_all((t1, t2, t3))

        sess.flush()

        for clear in (False, True):
            if clear:
                sess.expunge_all()
            eq_(
                sess.query(T).order_by(T.id).all(),
                [
                    T(value=True, name="t1"),
                    T(value=False, name="t2"),
                    T(value=True, name="t3"),
                ],
            )
            if clear:
                sess.expunge_all()
            eq_(
                sess.query(T)
                .filter(T.value == True)  # noqa
                .order_by(T.id)
                .all(),
                [T(value=True, name="t1"), T(value=True, name="t3")],
            )
            if clear:
                sess.expunge_all()
            eq_(
                sess.query(T)
                .filter(T.value == False)  # noqa
                .order_by(T.id)
                .all(),
                [T(value=False, name="t2")],
            )

        t2 = sess.query(T).get(t2.id)
        t2.value = True
        sess.flush()
        eq_(
            sess.query(T).filter(T.value == True).order_by(T.id).all(),  # noqa
            [
                T(value=True, name="t1"),
                T(value=True, name="t2"),
                T(value=True, name="t3"),
            ],
        )
        t2.value = False
        sess.flush()
        eq_(
            sess.query(T).filter(T.value == True).order_by(T.id).all(),  # noqa
            [T(value=True, name="t1"), T(value=True, name="t3")],
        )


class RowSwitchTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        # parent
        Table(
            "t5",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String(30), nullable=False),
        )

        # onetomany
        Table(
            "t6",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String(30), nullable=False),
            Column("t5id", Integer, ForeignKey("t5.id"), nullable=False),
        )

        # associated
        Table(
            "t7",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String(30), nullable=False),
        )

        # manytomany
        Table(
            "t5t7",
            metadata,
            Column("t5id", Integer, ForeignKey("t5.id"), nullable=False),
            Column("t7id", Integer, ForeignKey("t7.id"), nullable=False),
        )

    @classmethod
    def setup_classes(cls):
        class T5(cls.Comparable):
            pass

        class T6(cls.Comparable):
            pass

        class T7(cls.Comparable):
            pass

    def test_onetomany(self):
        t6, T6, t5, T5 = (
            self.tables.t6,
            self.classes.T6,
            self.tables.t5,
            self.classes.T5,
        )

        mapper(
            T5,
            t5,
            properties={"t6s": relationship(T6, cascade="all, delete-orphan")},
        )
        mapper(T6, t6)

        sess = fixture_session()

        o5 = T5(data="some t5", id=1)
        o5.t6s.append(T6(data="some t6", id=1))
        o5.t6s.append(T6(data="some other t6", id=2))

        sess.add(o5)
        sess.flush()

        eq_(list(sess.execute(t5.select(), mapper=T5)), [(1, "some t5")])
        eq_(
            list(sess.execute(t6.select().order_by(t6.c.id), mapper=T5)),
            [(1, "some t6", 1), (2, "some other t6", 1)],
        )

        o6 = T5(
            data="some other t5",
            id=o5.id,
            t6s=[T6(data="third t6", id=3), T6(data="fourth t6", id=4)],
        )
        sess.delete(o5)
        sess.add(o6)
        sess.flush()

        eq_(list(sess.execute(t5.select(), mapper=T5)), [(1, "some other t5")])
        eq_(
            list(sess.execute(t6.select().order_by(t6.c.id), mapper=T5)),
            [(3, "third t6", 1), (4, "fourth t6", 1)],
        )

    def test_manytomany(self):
        t7, t5, t5t7, T5, T7 = (
            self.tables.t7,
            self.tables.t5,
            self.tables.t5t7,
            self.classes.T5,
            self.classes.T7,
        )

        mapper(
            T5,
            t5,
            properties={
                "t7s": relationship(T7, secondary=t5t7, cascade="all")
            },
        )
        mapper(T7, t7)

        sess = fixture_session()

        o5 = T5(data="some t5", id=1)
        o5.t7s.append(T7(data="some t7", id=1))
        o5.t7s.append(T7(data="some other t7", id=2))

        sess.add(o5)
        sess.flush()

        assert list(sess.execute(t5.select(), mapper=T5)) == [(1, "some t5")]
        assert testing.rowset(sess.execute(t5t7.select(), mapper=T5)) == set(
            [(1, 1), (1, 2)]
        )
        assert list(sess.execute(t7.select(), mapper=T5)) == [
            (1, "some t7"),
            (2, "some other t7"),
        ]

        o6 = T5(
            data="some other t5",
            id=1,
            t7s=[T7(data="third t7", id=3), T7(data="fourth t7", id=4)],
        )

        sess.delete(o5)
        assert o5 in sess.deleted
        assert o5.t7s[0] in sess.deleted
        assert o5.t7s[1] in sess.deleted

        sess.add(o6)
        sess.flush()

        assert list(sess.execute(t5.select(), mapper=T5)) == [
            (1, "some other t5")
        ]
        assert list(sess.execute(t7.select(), mapper=T5)) == [
            (3, "third t7"),
            (4, "fourth t7"),
        ]

    def test_manytoone(self):
        t6, T6, t5, T5 = (
            self.tables.t6,
            self.classes.T6,
            self.tables.t5,
            self.classes.T5,
        )

        mapper(T6, t6, properties={"t5": relationship(T5)})
        mapper(T5, t5)

        sess = fixture_session()

        o5 = T6(data="some t6", id=1)
        o5.t5 = T5(data="some t5", id=1)

        sess.add(o5)
        sess.flush()

        assert list(sess.execute(t5.select(), mapper=T5)) == [(1, "some t5")]
        assert list(sess.execute(t6.select(), mapper=T5)) == [
            (1, "some t6", 1)
        ]

        o6 = T6(data="some other t6", id=1, t5=T5(data="some other t5", id=2))
        sess.delete(o5)
        sess.delete(o5.t5)
        sess.add(o6)
        sess.flush()

        assert list(sess.execute(t5.select(), mapper=T5)) == [
            (2, "some other t5")
        ]
        assert list(sess.execute(t6.select(), mapper=T5)) == [
            (1, "some other t6", 2)
        ]


class InheritingRowSwitchTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "parent",
            metadata,
            Column("pid", Integer, primary_key=True),
            Column("pdata", String(30)),
        )
        Table(
            "child",
            metadata,
            Column("cid", Integer, primary_key=True),
            Column("pid", Integer, ForeignKey("parent.pid")),
            Column("cdata", String(30)),
        )

    @classmethod
    def setup_classes(cls):
        class P(cls.Comparable):
            pass

        class C(P):
            pass

    def test_row_switch_no_child_table(self):
        P, C, parent, child = (
            self.classes.P,
            self.classes.C,
            self.tables.parent,
            self.tables.child,
        )

        mapper(P, parent)
        mapper(C, child, inherits=P)

        sess = fixture_session()
        c1 = C(pid=1, cid=1, pdata="c1", cdata="c1")
        sess.add(c1)
        sess.flush()

        # establish a row switch between c1 and c2.
        # c2 has no value for the "child" table
        c2 = C(pid=1, cid=1, pdata="c2")
        sess.add(c2)
        sess.delete(c1)

        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL(
                "UPDATE parent SET pdata=:pdata "
                "WHERE parent.pid = :parent_pid",
                {"pdata": "c2", "parent_pid": 1},
            ),
            # this fires as of [ticket:1362], since we synchronzize
            # PK/FKs on UPDATES.  c2 is new so the history shows up as
            # pure added, update occurs.  If a future change limits the
            # sync operation during _save_obj().update, this is safe to remove
            # again.
            CompiledSQL(
                "UPDATE child SET pid=:pid " "WHERE child.cid = :child_cid",
                {"pid": 1, "child_cid": 1},
            ),
        )


class PartialNullPKTest(fixtures.MappedTest):
    # sqlite totally fine with NULLs in pk columns.
    # no other DB is like this.
    __only_on__ = ("sqlite",)

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "t1",
            metadata,
            Column("col1", String(10), primary_key=True, nullable=True),
            Column("col2", String(10), primary_key=True, nullable=True),
            Column("col3", String(50)),
        )

    @classmethod
    def setup_classes(cls):
        class T1(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        mapper(cls.classes.T1, cls.tables.t1)

    def test_key_switch(self):
        T1 = self.classes.T1
        s = fixture_session()
        s.add(T1(col1="1", col2=None))

        t1 = s.query(T1).first()
        t1.col2 = 5
        assert_raises_message(
            orm_exc.FlushError,
            "Can't update table t1 using NULL for primary "
            "key value on column t1.col2",
            s.commit,
        )

    def test_plain_update(self):
        T1 = self.classes.T1
        s = fixture_session()
        s.add(T1(col1="1", col2=None))

        t1 = s.query(T1).first()
        t1.col3 = "hi"
        assert_raises_message(
            orm_exc.FlushError,
            "Can't update table t1 using NULL for primary "
            "key value on column t1.col2",
            s.commit,
        )

    def test_delete(self):
        T1 = self.classes.T1
        s = fixture_session()
        s.add(T1(col1="1", col2=None))

        t1 = s.query(T1).first()
        s.delete(t1)
        assert_raises_message(
            orm_exc.FlushError,
            "Can't delete from table t1 using NULL "
            "for primary key value on column t1.col2",
            s.commit,
        )

    def test_total_null(self):
        T1 = self.classes.T1
        s = fixture_session()
        s.add(T1(col1=None, col2=None))
        assert_raises_message(
            orm_exc.FlushError,
            r"Instance \<T1 at .+?\> has a NULL "
            "identity key.  If this is an auto-generated value, "
            "check that the database table allows generation ",
            s.commit,
        )

    def test_dont_complain_if_no_update(self):
        T1 = self.classes.T1
        s = fixture_session()
        t = T1(col1="1", col2=None)
        s.add(t)
        s.commit()

        t.col1 = "1"
        s.commit()


class EnsurePKSortableTest(fixtures.MappedTest):
    class SomeEnum(object):
        # Implements PEP 435 in the minimal fashion needed by SQLAlchemy
        __members__ = OrderedDict()

        def __init__(self, name, value, alias=None):
            self.name = name
            self.value = value
            self.__members__[name] = self
            setattr(self.__class__, name, self)
            if alias:
                self.__members__[alias] = self
                setattr(self.__class__, alias, self)

    class MySortableEnum(SomeEnum):
        __members__ = OrderedDict()

        def __lt__(self, other):
            return self.value < other.value

    class MyNotSortableEnum(SomeEnum):
        __members__ = OrderedDict()

    one = MySortableEnum("one", 1)
    two = MySortableEnum("two", 2)
    three = MyNotSortableEnum("three", 3)
    four = MyNotSortableEnum("four", 4)
    five = MyNotSortableEnum("five", 5)

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "t1",
            metadata,
            Column(
                "id",
                Enum(cls.MySortableEnum, create_constraint=False),
                primary_key=True,
            ),
            Column("data", String(10)),
        )

        Table(
            "t2",
            metadata,
            Column(
                "id",
                Enum(
                    cls.MyNotSortableEnum,
                    sort_key_function=None,
                    create_constraint=False,
                ),
                primary_key=True,
            ),
            Column("data", String(10)),
        )

        Table(
            "t3",
            metadata,
            Column(
                "id",
                Enum(cls.MyNotSortableEnum, create_constraint=False),
                primary_key=True,
            ),
            Column("value", Integer),
        )

    @staticmethod
    def sort_enum_key_value(value):
        return value.value

    @classmethod
    def setup_classes(cls):
        class T1(cls.Basic):
            pass

        class T2(cls.Basic):
            pass

        class T3(cls.Basic):
            def __str__(self):
                return "T3(id={})".format(self.id)

    @classmethod
    def setup_mappers(cls):
        mapper(cls.classes.T1, cls.tables.t1)
        mapper(cls.classes.T2, cls.tables.t2)
        mapper(cls.classes.T3, cls.tables.t3)

    def test_exception_persistent_flush_py3k(self):
        s = fixture_session()

        a, b = self.classes.T2(id=self.three), self.classes.T2(id=self.four)
        s.add_all([a, b])
        s.commit()

        a.data = "bar"
        b.data = "foo"
        if sa.util.py3k:
            message = (
                r"Could not sort objects by primary key; primary key "
                r"values must be sortable in Python \(was: '<' not "
                r"supported between instances of 'MyNotSortableEnum'"
                r" and 'MyNotSortableEnum'\)"
            )

            assert_raises_message(
                sa.exc.InvalidRequestError,
                message,
                s.flush,
            )
        else:
            s.flush()
        s.close()

    def test_persistent_flush_sortable(self):
        s = fixture_session()

        a, b = self.classes.T1(id=self.one), self.classes.T1(id=self.two)
        s.add_all([a, b])
        s.commit()

        a.data = "bar"
        b.data = "foo"
        s.commit()

    def test_pep435_custom_sort_key(self):
        s = fixture_session()

        a = self.classes.T3(id=self.three, value=1)
        b = self.classes.T3(id=self.four, value=2)
        s.add_all([a, b])
        s.commit()

        c = self.classes.T3(id=self.five, value=0)
        s.add(c)

        states = [o._sa_instance_state for o in [b, a, c]]
        eq_(
            _sort_states(inspect(self.classes.T3), states),
            # pending come first, then "four" < "three"
            [o._sa_instance_state for o in [c, b, a]],
        )
