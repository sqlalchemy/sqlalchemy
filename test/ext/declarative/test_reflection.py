from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.ext.declarative import DeferredReflection
from sqlalchemy.orm import clear_mappers
from sqlalchemy.orm import decl_api as decl
from sqlalchemy.orm import declared_attr
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm.decl_base import _DeferredMapperConfig
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from sqlalchemy.testing.util import gc_collect


class DeclarativeReflectionBase(fixtures.TablesTest):
    __requires__ = ("reflectable_autoincrement",)

    def setup_test(self):
        global Base, registry

        _DeferredMapperConfig._configs.clear()

        registry = decl.registry()
        Base = registry.generate_base()

    def teardown_test(self):
        clear_mappers()


class DeferredReflectBase(DeclarativeReflectionBase):
    def teardown_test(self):
        super(DeferredReflectBase, self).teardown_test()
        _DeferredMapperConfig._configs.clear()


Base = None


class DeferredReflectPKFKTest(DeferredReflectBase):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "a",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
        )
        Table(
            "b",
            metadata,
            Column("id", Integer, ForeignKey("a.id"), primary_key=True),
            Column("x", Integer, primary_key=True),
        )

    def test_pk_fk(self):
        class B(DeferredReflection, fixtures.ComparableEntity, Base):
            __tablename__ = "b"
            a = relationship("A")

        class A(DeferredReflection, fixtures.ComparableEntity, Base):
            __tablename__ = "a"

        DeferredReflection.prepare(testing.db)


class DeferredReflectionTest(DeferredReflectBase):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(50)),
            test_needs_fk=True,
        )
        Table(
            "addresses",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("email", String(50)),
            Column("user_id", Integer, ForeignKey("users.id")),
            test_needs_fk=True,
        )

    def _roundtrip(self):

        User = Base.registry._class_registry["User"]
        Address = Base.registry._class_registry["Address"]

        u1 = User(
            name="u1", addresses=[Address(email="one"), Address(email="two")]
        )
        with fixture_session() as sess:
            sess.add(u1)
            sess.commit()

        with fixture_session() as sess:
            eq_(
                sess.query(User).all(),
                [
                    User(
                        name="u1",
                        addresses=[Address(email="one"), Address(email="two")],
                    )
                ],
            )
            a1 = sess.query(Address).filter(Address.email == "two").one()
            eq_(a1, Address(email="two"))
            eq_(a1.user, User(name="u1"))

    def test_exception_prepare_not_called(self):
        class User(DeferredReflection, fixtures.ComparableEntity, Base):
            __tablename__ = "users"
            addresses = relationship("Address", backref="user")

        class Address(DeferredReflection, fixtures.ComparableEntity, Base):
            __tablename__ = "addresses"

        assert_raises_message(
            orm_exc.UnmappedClassError,
            "Class test.ext.declarative.test_reflection.User is a "
            "subclass of DeferredReflection.  Mappings are not produced "
            r"until the .prepare\(\) method is called on the class "
            "hierarchy.",
            Session().query,
            User,
        )

    def test_basic_deferred(self):
        class User(DeferredReflection, fixtures.ComparableEntity, Base):
            __tablename__ = "users"
            addresses = relationship("Address", backref="user")

        class Address(DeferredReflection, fixtures.ComparableEntity, Base):
            __tablename__ = "addresses"

        DeferredReflection.prepare(testing.db)
        self._roundtrip()

    def test_abstract_base(self):
        class DefBase(DeferredReflection, Base):
            __abstract__ = True

        class OtherDefBase(DeferredReflection, Base):
            __abstract__ = True

        class User(fixtures.ComparableEntity, DefBase):
            __tablename__ = "users"
            addresses = relationship("Address", backref="user")

        class Address(fixtures.ComparableEntity, DefBase):
            __tablename__ = "addresses"

        class Fake(OtherDefBase):
            __tablename__ = "nonexistent"

        DefBase.prepare(testing.db)
        self._roundtrip()

    def test_redefine_fk_double(self):
        class User(DeferredReflection, fixtures.ComparableEntity, Base):
            __tablename__ = "users"
            addresses = relationship("Address", backref="user")

        class Address(DeferredReflection, fixtures.ComparableEntity, Base):
            __tablename__ = "addresses"
            user_id = Column(Integer, ForeignKey("users.id"))

        DeferredReflection.prepare(testing.db)
        self._roundtrip()

    def test_mapper_args_deferred(self):
        """test that __mapper_args__ is not called until *after*
        table reflection"""

        class User(DeferredReflection, fixtures.ComparableEntity, Base):
            __tablename__ = "users"

            @declared_attr
            def __mapper_args__(cls):
                return {"primary_key": cls.__table__.c.id}

        DeferredReflection.prepare(testing.db)
        with fixture_session() as sess:
            sess.add_all(
                [
                    User(name="G"),
                    User(name="Q"),
                    User(name="A"),
                    User(name="C"),
                ]
            )
            sess.commit()
            eq_(
                sess.query(User).order_by(User.name).all(),
                [
                    User(name="A"),
                    User(name="C"),
                    User(name="G"),
                    User(name="Q"),
                ],
            )

    @testing.requires.predictable_gc
    def test_cls_not_strong_ref(self):
        class User(DeferredReflection, fixtures.ComparableEntity, Base):
            __tablename__ = "users"

        class Address(DeferredReflection, fixtures.ComparableEntity, Base):
            __tablename__ = "addresses"

        eq_(len(_DeferredMapperConfig._configs), 2)
        del Address
        gc_collect()
        eq_(len(_DeferredMapperConfig._configs), 1)
        DeferredReflection.prepare(testing.db)
        assert not _DeferredMapperConfig._configs


class DeferredSecondaryReflectionTest(DeferredReflectBase):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(50)),
            test_needs_fk=True,
        )

        Table(
            "user_items",
            metadata,
            Column("user_id", ForeignKey("users.id"), primary_key=True),
            Column("item_id", ForeignKey("items.id"), primary_key=True),
            test_needs_fk=True,
        )

        Table(
            "items",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(50)),
            test_needs_fk=True,
        )

    def _roundtrip(self):

        User = Base.registry._class_registry["User"]
        Item = Base.registry._class_registry["Item"]

        u1 = User(name="u1", items=[Item(name="i1"), Item(name="i2")])

        with fixture_session() as sess:
            sess.add(u1)
            sess.commit()

            eq_(
                sess.query(User).all(),
                [User(name="u1", items=[Item(name="i1"), Item(name="i2")])],
            )

    def test_string_resolution(self):
        class User(DeferredReflection, fixtures.ComparableEntity, Base):
            __tablename__ = "users"

            items = relationship("Item", secondary="user_items")

        class Item(DeferredReflection, fixtures.ComparableEntity, Base):
            __tablename__ = "items"

        DeferredReflection.prepare(testing.db)
        self._roundtrip()

    def test_table_resolution(self):
        class User(DeferredReflection, fixtures.ComparableEntity, Base):
            __tablename__ = "users"

            items = relationship(
                "Item", secondary=Table("user_items", Base.metadata)
            )

        class Item(DeferredReflection, fixtures.ComparableEntity, Base):
            __tablename__ = "items"

        DeferredReflection.prepare(testing.db)
        self._roundtrip()


class DeferredInhReflectBase(DeferredReflectBase):
    def _roundtrip(self):
        Foo = Base.registry._class_registry["Foo"]
        Bar = Base.registry._class_registry["Bar"]

        with fixture_session() as s:
            s.add_all(
                [
                    Bar(data="d1", bar_data="b1"),
                    Bar(data="d2", bar_data="b2"),
                    Bar(data="d3", bar_data="b3"),
                    Foo(data="d4"),
                ]
            )
            s.commit()

            eq_(
                s.query(Foo).order_by(Foo.id).all(),
                [
                    Bar(data="d1", bar_data="b1"),
                    Bar(data="d2", bar_data="b2"),
                    Bar(data="d3", bar_data="b3"),
                    Foo(data="d4"),
                ],
            )


class DeferredSingleInhReflectionTest(DeferredInhReflectBase):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "foo",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("type", String(32)),
            Column("data", String(30)),
            Column("bar_data", String(30)),
        )

    def test_basic(self):
        class Foo(DeferredReflection, fixtures.ComparableEntity, Base):
            __tablename__ = "foo"
            __mapper_args__ = {
                "polymorphic_on": "type",
                "polymorphic_identity": "foo",
            }

        class Bar(Foo):
            __mapper_args__ = {"polymorphic_identity": "bar"}

        DeferredReflection.prepare(testing.db)
        self._roundtrip()

    def test_add_subclass_column(self):
        class Foo(DeferredReflection, fixtures.ComparableEntity, Base):
            __tablename__ = "foo"
            __mapper_args__ = {
                "polymorphic_on": "type",
                "polymorphic_identity": "foo",
            }

        class Bar(Foo):
            __mapper_args__ = {"polymorphic_identity": "bar"}
            bar_data = Column(String(30))

        DeferredReflection.prepare(testing.db)
        self._roundtrip()

    def test_add_pk_column(self):
        class Foo(DeferredReflection, fixtures.ComparableEntity, Base):
            __tablename__ = "foo"
            __mapper_args__ = {
                "polymorphic_on": "type",
                "polymorphic_identity": "foo",
            }
            id = Column(Integer, primary_key=True)

        class Bar(Foo):
            __mapper_args__ = {"polymorphic_identity": "bar"}

        DeferredReflection.prepare(testing.db)
        self._roundtrip()


class DeferredJoinedInhReflectionTest(DeferredInhReflectBase):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "foo",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("type", String(32)),
            Column("data", String(30)),
            test_needs_fk=True,
        )
        Table(
            "bar",
            metadata,
            Column("id", Integer, ForeignKey("foo.id"), primary_key=True),
            Column("bar_data", String(30)),
            test_needs_fk=True,
        )

    def test_basic(self):
        class Foo(DeferredReflection, fixtures.ComparableEntity, Base):
            __tablename__ = "foo"
            __mapper_args__ = {
                "polymorphic_on": "type",
                "polymorphic_identity": "foo",
            }

        class Bar(Foo):
            __tablename__ = "bar"
            __mapper_args__ = {"polymorphic_identity": "bar"}

        DeferredReflection.prepare(testing.db)
        self._roundtrip()

    def test_add_subclass_column(self):
        class Foo(DeferredReflection, fixtures.ComparableEntity, Base):
            __tablename__ = "foo"
            __mapper_args__ = {
                "polymorphic_on": "type",
                "polymorphic_identity": "foo",
            }

        class Bar(Foo):
            __tablename__ = "bar"
            __mapper_args__ = {"polymorphic_identity": "bar"}
            bar_data = Column(String(30))

        DeferredReflection.prepare(testing.db)
        self._roundtrip()

    def test_add_pk_column(self):
        class Foo(DeferredReflection, fixtures.ComparableEntity, Base):
            __tablename__ = "foo"
            __mapper_args__ = {
                "polymorphic_on": "type",
                "polymorphic_identity": "foo",
            }
            id = Column(Integer, primary_key=True)

        class Bar(Foo):
            __tablename__ = "bar"
            __mapper_args__ = {"polymorphic_identity": "bar"}

        DeferredReflection.prepare(testing.db)
        self._roundtrip()

    def test_add_fk_pk_column(self):
        class Foo(DeferredReflection, fixtures.ComparableEntity, Base):
            __tablename__ = "foo"
            __mapper_args__ = {
                "polymorphic_on": "type",
                "polymorphic_identity": "foo",
            }

        class Bar(Foo):
            __tablename__ = "bar"
            __mapper_args__ = {"polymorphic_identity": "bar"}
            id = Column(Integer, ForeignKey("foo.id"), primary_key=True)

        DeferredReflection.prepare(testing.db)
        self._roundtrip()
