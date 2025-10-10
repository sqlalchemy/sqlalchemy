import random
import uuid

import sqlalchemy as sa
from sqlalchemy import CheckConstraint
from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy import Index
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import join
from sqlalchemy import literal
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import UniqueConstraint
from sqlalchemy import Uuid
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import as_declarative
from sqlalchemy.orm import backref
from sqlalchemy.orm import class_mapper
from sqlalchemy.orm import clear_mappers
from sqlalchemy.orm import close_all_sessions
from sqlalchemy.orm import column_property
from sqlalchemy.orm import composite
from sqlalchemy.orm import configure_mappers
from sqlalchemy.orm import decl_base
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import declared_attr
from sqlalchemy.orm import deferred
from sqlalchemy.orm import descriptor_props
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import MappedAsDataclass
from sqlalchemy.orm import MappedColumn
from sqlalchemy.orm import Mapper
from sqlalchemy.orm import registry
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm import synonym
from sqlalchemy.orm import synonym_for
from sqlalchemy.orm.decl_api import add_mapped_attribute
from sqlalchemy.orm.decl_api import DeclarativeBaseNoMeta
from sqlalchemy.orm.decl_api import DeclarativeMeta
from sqlalchemy.orm.decl_base import _DeferredDeclarativeConfig
from sqlalchemy.orm.events import InstrumentationEvents
from sqlalchemy.orm.events import MapperEvents
from sqlalchemy.schema import PrimaryKeyConstraint
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import assertions
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import expect_warnings
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import mock
from sqlalchemy.testing.entities import ComparableEntity
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table


Base = None

User = Address = None


class DeclarativeBaseSetupsTest(fixtures.TestBase):
    def test_unbound_declarative_base(self):
        Base = declarative_base()

        class User(Base):
            __tablename__ = "user"
            id = Column(Integer, primary_key=True)

        s = Session()

        with testing.expect_raises(exc.UnboundExecutionError):
            s.get_bind(User)

    def test_unbound_cls_registry(self):
        reg = registry()

        Base = reg.generate_base()

        class User(Base):
            __tablename__ = "user"
            id = Column(Integer, primary_key=True)

        s = Session()

        with testing.expect_raises(exc.UnboundExecutionError):
            s.get_bind(User)

    @testing.variation(
        "base_type",
        ["declbase", "declbasenometa", "declbasefn", "asdeclarative"],
    )
    def test_reg_constructor_is_present(self, base_type):
        """test #9171"""

        if base_type.declbase:

            class Base(DeclarativeBase):
                pass

        elif base_type.declbasenometa:

            class Base(DeclarativeBaseNoMeta):
                pass

        elif base_type.declbasefn:
            Base = declarative_base()
        elif base_type.asdeclarative:

            @as_declarative()
            class Base:
                pass

        else:
            base_type.fail()

        # check for direct assignment
        is_(Base.registry.constructor, Base.__init__)
        is_(Base.__dict__["__init__"], Base.__init__)

        class fakeself:
            foo = None
            bar = None

        fs = fakeself()
        Base.__init__(fs, foo="bar", bar="bat")
        eq_(fs.foo, "bar")
        eq_(fs.bar, "bat")

    @testing.variation(
        "base_type",
        [
            "declbase",
            "declbasenometa",
            "declbasefn",
            "asdeclarative",
            "mixinonbase",
        ],
    )
    def test_reg_constructor_custom_init(self, base_type):
        """test for #9171 testing what an explicit __init__ does.

        Here we decide that newer DeclarativeBase superclasses should
        honor the ``__init__`` that's given.

        """

        m1 = mock.Mock()

        if base_type.declbase:

            class Base(DeclarativeBase):
                def __init__(self, x=None):
                    m1.init(x)

        elif base_type.declbasenometa:

            class Base(DeclarativeBaseNoMeta):
                def __init__(self, x=None):
                    m1.init(x)

        elif base_type.declbasefn:

            class _B:
                def __init__(self, x=None):
                    m1.init(x)

            Base = declarative_base(cls=_B)
        elif base_type.mixinonbase:

            class Mixin:
                def __init__(self, x=None):
                    m1.init(x)

            class Base(Mixin, DeclarativeBase):
                pass

        elif base_type.asdeclarative:

            @as_declarative()
            class Base:
                def __init__(self, x=None):
                    m1.init(x)

        else:
            base_type.fail()

        class fakeself:
            pass

        fs = fakeself()

        if (
            base_type.declbase
            or base_type.declbasenometa
            or base_type.mixinonbase
        ):
            Base.__init__(fs, x=5)
            eq_(m1.mock_calls, [mock.call.init(5)])
        else:
            with expect_raises_message(
                TypeError, "'x' is an invalid keyword argument for fakeself"
            ):
                Base.__init__(fs, x=5)

    def test_insert_sentinel_param_custom_type_maintained(self, decl_base):
        class A(decl_base):
            __tablename__ = "a"
            id: Mapped[uuid.UUID] = mapped_column(
                default=uuid.uuid4, primary_key=True, insert_sentinel=True
            )
            data: Mapped[str]

        is_(A.id.expression.type._type_affinity, Uuid)

    def test_insert_sentinel_param_default_type(self, decl_base):
        class A(decl_base):
            __tablename__ = "a"
            id: Mapped[int] = mapped_column(
                primary_key=True, insert_sentinel=True
            )
            data: Mapped[str]

        is_(A.id.expression.type._type_affinity, Integer)

    @testing.variation("argument", ["version_id_col", "polymorphic_on"])
    @testing.variation("column_type", ["anno", "non_anno", "plain_column"])
    def test_mapped_column_version_poly_arg(
        self, decl_base, column_type, argument
    ):
        """test #9240"""

        if column_type.anno:

            class A(decl_base):
                __tablename__ = "a"

                a: Mapped[int] = mapped_column(primary_key=True)
                b: Mapped[int] = mapped_column()
                c: Mapped[str] = mapped_column()

                if argument.version_id_col:
                    __mapper_args__ = {"version_id_col": b}
                elif argument.polymorphic_on:
                    __mapper_args__ = {"polymorphic_on": c}
                else:
                    argument.fail()

        elif column_type.non_anno:

            class A(decl_base):
                __tablename__ = "a"

                a = mapped_column(Integer, primary_key=True)
                b = mapped_column(Integer)
                c = mapped_column(String)

                if argument.version_id_col:
                    __mapper_args__ = {"version_id_col": b}
                elif argument.polymorphic_on:
                    __mapper_args__ = {"polymorphic_on": c}
                else:
                    argument.fail()

        elif column_type.plain_column:

            class A(decl_base):
                __tablename__ = "a"

                a = Column(Integer, primary_key=True)
                b = Column(Integer)
                c = Column(String)

                if argument.version_id_col:
                    __mapper_args__ = {"version_id_col": b}
                elif argument.polymorphic_on:
                    __mapper_args__ = {"polymorphic_on": c}
                else:
                    argument.fail()

        else:
            column_type.fail()

        if argument.version_id_col:
            assert A.__mapper__.version_id_col is A.__table__.c.b
        elif argument.polymorphic_on:
            assert A.__mapper__.polymorphic_on is A.__table__.c.c
        else:
            argument.fail()

    @testing.variation(
        "pk_type", ["single", "tuple", "list", "single_str", "list_str"]
    )
    @testing.variation("column_type", ["anno", "non_anno", "plain_column"])
    def test_mapped_column_pk_arg(self, decl_base, column_type, pk_type):
        """test #9240"""

        if column_type.anno:

            class A(decl_base):
                __tablename__ = "a"

                a: Mapped[int] = mapped_column()
                b: Mapped[int] = mapped_column()

                if pk_type.single:
                    __mapper_args__ = {"primary_key": a}
                elif pk_type.tuple:
                    __mapper_args__ = {"primary_key": (a, b)}
                elif pk_type.list:
                    __mapper_args__ = {"primary_key": [a, b]}
                elif pk_type.single_str:
                    __mapper_args__ = {"primary_key": "a"}
                elif pk_type.list_str:
                    __mapper_args__ = {"primary_key": ["a", "b"]}
                else:
                    pk_type.fail()

        elif column_type.non_anno:

            class A(decl_base):
                __tablename__ = "a"

                a = mapped_column(Integer)
                b = mapped_column(Integer)

                if pk_type.single:
                    __mapper_args__ = {"primary_key": a}
                elif pk_type.tuple:
                    __mapper_args__ = {"primary_key": (a, b)}
                elif pk_type.list:
                    __mapper_args__ = {"primary_key": [a, b]}
                elif pk_type.single_str:
                    __mapper_args__ = {"primary_key": "a"}
                elif pk_type.list_str:
                    __mapper_args__ = {"primary_key": ["a", "b"]}
                else:
                    pk_type.fail()

        elif column_type.plain_column:

            class A(decl_base):
                __tablename__ = "a"

                a = Column(Integer)
                b = Column(Integer)

                if pk_type.single:
                    __mapper_args__ = {"primary_key": a}
                elif pk_type.tuple:
                    __mapper_args__ = {"primary_key": (a, b)}
                elif pk_type.list:
                    __mapper_args__ = {"primary_key": [a, b]}
                elif pk_type.single_str:
                    __mapper_args__ = {"primary_key": "a"}
                elif pk_type.list_str:
                    __mapper_args__ = {"primary_key": ["a", "b"]}
                else:
                    pk_type.fail()

        else:
            column_type.fail()

        if pk_type.single or pk_type.single_str:
            assert A.__mapper__.primary_key[0] is A.__table__.c.a
        else:
            assert A.__mapper__.primary_key[0] is A.__table__.c.a
            assert A.__mapper__.primary_key[1] is A.__table__.c.b

    def test_mapper_pk_arg_degradation_no_col(self, decl_base):
        with expect_raises_message(
            exc.ArgumentError,
            "Can't determine primary_key column 'q' - no attribute is "
            "mapped to this name.",
        ):

            class A(decl_base):
                __tablename__ = "a"

                a: Mapped[int] = mapped_column()
                b: Mapped[int] = mapped_column()

                __mapper_args__ = {"primary_key": "q"}

    @testing.variation("proptype", ["relationship", "colprop"])
    def test_mapper_pk_arg_degradation_is_not_a_col(self, decl_base, proptype):
        with expect_raises_message(
            exc.ArgumentError,
            "Can't determine primary_key column 'b'; property does "
            "not refer to a single mapped Column",
        ):

            class A(decl_base):
                __tablename__ = "a"

                a: Mapped[int] = mapped_column(Integer)

                if proptype.colprop:
                    b: Mapped[int] = column_property(a + 5)
                elif proptype.relationship:
                    b = relationship("B")
                else:
                    proptype.fail()

                __mapper_args__ = {"primary_key": "b"}

    @testing.variation(
        "argument", ["version_id_col", "polymorphic_on", "primary_key"]
    )
    @testing.variation("argtype", ["callable", "fixed"])
    @testing.variation("column_type", ["mapped_column", "plain_column"])
    def test_mapped_column_pk_arg_via_mixin(
        self, decl_base, argtype, column_type, argument
    ):
        """test #9240"""

        class Mixin:
            if column_type.mapped_column:
                a: Mapped[int] = mapped_column()
                b: Mapped[int] = mapped_column()
                c: Mapped[str] = mapped_column()
            elif column_type.plain_column:
                a = Column(Integer)
                b = Column(Integer)
                c = Column(String)
            else:
                column_type.fail()

            if argtype.callable:

                @declared_attr.directive
                @classmethod
                def __mapper_args__(cls):
                    if argument.primary_key:
                        return {"primary_key": [cls.a, cls.b]}
                    elif argument.version_id_col:
                        return {"version_id_col": cls.b, "primary_key": cls.a}
                    elif argument.polymorphic_on:
                        return {"polymorphic_on": cls.c, "primary_key": cls.a}
                    else:
                        argument.fail()

            elif argtype.fixed:
                if argument.primary_key:
                    __mapper_args__ = {"primary_key": [a, b]}
                elif argument.version_id_col:
                    __mapper_args__ = {"primary_key": a, "version_id_col": b}
                elif argument.polymorphic_on:
                    __mapper_args__ = {"primary_key": a, "polymorphic_on": c}
                else:
                    argument.fail()

            else:
                argtype.fail()

        class A(Mixin, decl_base):
            __tablename__ = "a"

        if argument.primary_key:
            assert A.__mapper__.primary_key[0] is A.__table__.c.a
            assert A.__mapper__.primary_key[1] is A.__table__.c.b
        elif argument.version_id_col:
            assert A.__mapper__.version_id_col is A.__table__.c.b
        elif argument.polymorphic_on:
            assert A.__mapper__.polymorphic_on is A.__table__.c.c
        else:
            argtype.fail()

    def test_dispose_attrs(self):
        reg = registry()

        class Foo:
            __tablename__ = "some_table"

            id = Column(Integer, primary_key=True)

        reg.mapped(Foo)

        is_(Foo.__mapper__, class_mapper(Foo))
        is_(Foo.__table__, class_mapper(Foo).local_table)

        clear_mappers()

        assert not hasattr(Foo, "__mapper__")
        assert not hasattr(Foo, "__table__")

        from sqlalchemy.orm import clsregistry

        assert clsregistry._key_is_empty(
            "Foo", reg._class_registry, lambda cls: cls is Foo
        )

    def test_difficult_class(self, metadata):
        """test no getattr() errors with a customized class"""

        # metaclass to mock the way zope.interface breaks getattr()
        class BrokenMeta(type):
            def __getattribute__(self, attr):
                if attr == "xyzzy":
                    raise AttributeError("xyzzy")
                else:
                    return object.__getattribute__(self, attr)

        # even though this class has an xyzzy attribute, getattr(cls,"xyzzy")
        # fails
        class BrokenParent(metaclass=BrokenMeta):
            xyzzy = "magic"

        # _as_declarative() inspects obj.__class__.__bases__
        class User(BrokenParent, ComparableEntity):
            __tablename__ = "users"
            id = Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column("name", String(50))

        reg = registry(metadata=metadata)

        mp = reg.map_declaratively(User)
        assert mp is inspect(User)
        assert mp is User.__mapper__

    def test_undefer_column_name(self):
        # TODO: not sure if there was an explicit
        # test for this elsewhere
        foo = Column(Integer)
        eq_(str(foo), "(no name)")
        eq_(foo.key, None)
        eq_(foo.name, None)
        decl_base._undefer_column_name("foo", foo)
        eq_(str(foo), "foo")
        eq_(foo.key, "foo")
        eq_(foo.name, "foo")

    def test_string_dependency_resolution_schemas(self):
        Base = declarative_base()

        class User(Base):
            __tablename__ = "users"
            __table_args__ = {"schema": "fooschema"}

            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            props = relationship(
                "Prop",
                secondary="fooschema.user_to_prop",
                primaryjoin="User.id==fooschema.user_to_prop.c.user_id",
                secondaryjoin="fooschema.user_to_prop.c.prop_id==Prop.id",
                backref="users",
            )

        class Prop(Base):
            __tablename__ = "props"
            __table_args__ = {"schema": "fooschema"}

            id = Column(Integer, primary_key=True)
            name = Column(String(50))

        user_to_prop = Table(
            "user_to_prop",
            Base.metadata,
            Column("user_id", Integer, ForeignKey("fooschema.users.id")),
            Column("prop_id", Integer, ForeignKey("fooschema.props.id")),
            schema="fooschema",
        )
        configure_mappers()

        assert (
            class_mapper(User).get_property("props").secondary is user_to_prop
        )

    def test_string_dependency_resolution_schemas_no_base(self):
        """

        found_during_type_annotation

        """

        reg = registry()

        @reg.mapped
        class User:
            __tablename__ = "users"
            __table_args__ = {"schema": "fooschema"}

            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            props = relationship(
                "Prop",
                secondary="fooschema.user_to_prop",
                primaryjoin="User.id==fooschema.user_to_prop.c.user_id",
                secondaryjoin="fooschema.user_to_prop.c.prop_id==Prop.id",
                backref="users",
            )

        @reg.mapped
        class Prop:
            __tablename__ = "props"
            __table_args__ = {"schema": "fooschema"}

            id = Column(Integer, primary_key=True)
            name = Column(String(50))

        user_to_prop = Table(
            "user_to_prop",
            reg.metadata,
            Column("user_id", Integer, ForeignKey("fooschema.users.id")),
            Column("prop_id", Integer, ForeignKey("fooschema.props.id")),
            schema="fooschema",
        )
        configure_mappers()

        assert (
            class_mapper(User).get_property("props").secondary is user_to_prop
        )

    def test_string_dependency_resolution_annotations(self):
        Base = declarative_base()

        class Parent(Base):
            __tablename__ = "parent"
            id = Column(Integer, primary_key=True)
            name = Column(String)
            children = relationship(
                "Child",
                primaryjoin="Parent.name=="
                "remote(foreign(func.lower(Child.name_upper)))",
            )

        class Child(Base):
            __tablename__ = "child"
            id = Column(Integer, primary_key=True)
            name_upper = Column(String)

        configure_mappers()
        eq_(
            Parent.children.property._calculated_foreign_keys,
            {Child.name_upper.property.columns[0]},
        )

    def test_class_has_registry_attr(self, registry):
        existing_registry = registry

        Base = registry.generate_base()

        class A(Base):
            __tablename__ = "a"

            registry = {"foo": "bar"}
            id = Column(Integer, primary_key=True)
            data = Column(String)

        class SubA(A):
            pass

        is_(Base.registry, existing_registry)
        is_(inspect(A).registry, existing_registry)
        eq_(A.registry, {"foo": "bar"})

        is_(inspect(SubA).registry, existing_registry)
        eq_(SubA.registry, {"foo": "bar"})

    def test_class_does_not_have_registry_attr(self):
        with assertions.expect_raises_message(
            exc.InvalidRequestError,
            r"Declarative base class has no 'registry' attribute, or "
            r"registry is not a sqlalchemy.orm.registry\(\) object",
        ):

            class Base(metaclass=DeclarativeMeta):
                metadata = sa.MetaData()

    def test_shared_class_registry(self):
        reg = {}
        Base1 = declarative_base(class_registry=reg)
        Base2 = declarative_base(class_registry=reg)

        class A(Base1):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)

        class B(Base2):
            __tablename__ = "b"
            id = Column(Integer, primary_key=True)
            aid = Column(Integer, ForeignKey(A.id))
            as_ = relationship("A")

        assert B.as_.property.mapper.class_ is A

    def test_custom_base(self):
        class MyBase:
            def foobar(self):
                return "foobar"

        Base = declarative_base(cls=MyBase)
        assert hasattr(Base, "metadata")
        assert Base().foobar() == "foobar"

    def test_as_declarative(self, metadata):
        class User(ComparableEntity):
            __tablename__ = "users"
            id = Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column("name", String(50))
            addresses = relationship("Address", backref="user")

        class Address(ComparableEntity):
            __tablename__ = "addresses"
            id = Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            )
            email = Column("email", String(50))
            user_id = Column("user_id", Integer, ForeignKey("users.id"))

        reg = registry(metadata=metadata)
        reg.mapped(User)
        reg.mapped(Address)

        reg.metadata.create_all(testing.db)
        u1 = User(
            name="u1", addresses=[Address(email="one"), Address(email="two")]
        )
        with Session(testing.db) as sess:
            sess.add(u1)
            sess.commit()
        with Session(testing.db) as sess:
            eq_(
                sess.query(User).all(),
                [
                    User(
                        name="u1",
                        addresses=[Address(email="one"), Address(email="two")],
                    )
                ],
            )

    def test_map_declaratively(self, metadata):
        class User(ComparableEntity):
            __tablename__ = "users"
            id = Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column("name", String(50))
            addresses = relationship("Address", backref="user")

        class Address(ComparableEntity):
            __tablename__ = "addresses"
            id = Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            )
            email = Column("email", String(50))
            user_id = Column("user_id", Integer, ForeignKey("users.id"))

        reg = registry(metadata=metadata)
        um = reg.map_declaratively(User)
        am = reg.map_declaratively(Address)

        is_(User.__mapper__, um)
        is_(Address.__mapper__, am)

        reg.metadata.create_all(testing.db)
        u1 = User(
            name="u1", addresses=[Address(email="one"), Address(email="two")]
        )
        with Session(testing.db) as sess:
            sess.add(u1)
            sess.commit()
        with Session(testing.db) as sess:
            eq_(
                sess.query(User).all(),
                [
                    User(
                        name="u1",
                        addresses=[Address(email="one"), Address(email="two")],
                    )
                ],
            )

    def test_custom_mapper_attribute(self):
        def mymapper(cls, tbl, **kwargs):
            m = sa.orm.Mapper(cls, tbl, **kwargs)
            m.CHECK = True
            return m

        base = declarative_base()

        class Foo(base):
            __tablename__ = "foo"
            __mapper_cls__ = mymapper
            id = Column(Integer, primary_key=True)

        eq_(Foo.__mapper__.CHECK, True)

    def test_custom_mapper_argument(self):
        def mymapper(cls, tbl, **kwargs):
            m = sa.orm.Mapper(cls, tbl, **kwargs)
            m.CHECK = True
            return m

        base = declarative_base(mapper=mymapper)

        class Foo(base):
            __tablename__ = "foo"
            id = Column(Integer, primary_key=True)

        eq_(Foo.__mapper__.CHECK, True)

    def test_no_change_to_all_descriptors(self):
        base = declarative_base()

        class Foo(base):
            __tablename__ = "foo"
            id = Column(Integer, primary_key=True)

        eq_(Foo.__mapper__.all_orm_descriptors.keys(), ["id"])

    def test_cls_docstring(self):
        class MyBase:
            """MyBase Docstring"""

        Base = declarative_base(cls=MyBase)

        eq_(Base.__doc__, MyBase.__doc__)

    def test_delattr_mapped_raises(self):
        Base = declarative_base()

        class Foo(Base):
            __tablename__ = "foo"

            id = Column(Integer, primary_key=True)
            data = Column(String)

        def go():
            del Foo.data

        assert_raises_message(
            NotImplementedError,
            "Can't un-map individual mapped attributes on a mapped class.",
            go,
        )

    def test_delattr_hybrid_fine(self):
        Base = declarative_base()

        class Foo(Base):
            __tablename__ = "foo"

            id = Column(Integer, primary_key=True)
            data = Column(String)

            @hybrid_property
            def data_hybrid(self):
                return self.data

        assert "data_hybrid" in Foo.__mapper__.all_orm_descriptors.keys()

        del Foo.data_hybrid

        assert "data_hybrid" not in Foo.__mapper__.all_orm_descriptors.keys()

        assert not hasattr(Foo, "data_hybrid")

    def test_setattr_hybrid_updates_descriptors(self):
        Base = declarative_base()

        class Foo(Base):
            __tablename__ = "foo"

            id = Column(Integer, primary_key=True)
            data = Column(String)

        assert "data_hybrid" not in Foo.__mapper__.all_orm_descriptors.keys()

        @hybrid_property
        def data_hybrid(self):
            return self.data

        Foo.data_hybrid = data_hybrid
        assert "data_hybrid" in Foo.__mapper__.all_orm_descriptors.keys()

        del Foo.data_hybrid

        assert "data_hybrid" not in Foo.__mapper__.all_orm_descriptors.keys()

        assert not hasattr(Foo, "data_hybrid")

    def test_kw_support_in_declarative_meta_init(self, registry):
        # This will not fail if DeclarativeMeta __init__ supports **kw

        reg = registry

        class Base(metaclass=DeclarativeMeta):
            __abstract__ = True
            registry = reg
            metadata = reg.metadata

        class BaseUser(Base):
            __tablename__ = "base"
            id_ = Column(Integer, primary_key=True)

            @classmethod
            def __init_subclass__(cls, random_keyword=False, **kw):
                super().__init_subclass__(**kw)
                cls._set_random_keyword_used_here = random_keyword

        class User(BaseUser):
            __tablename__ = "user"
            id_ = Column(Integer, ForeignKey("base.id_"), primary_key=True)

        # Check the default option
        eq_(User._set_random_keyword_used_here, False)

        # Build the metaclass with a keyword!
        bases = (BaseUser,)
        UserType = DeclarativeMeta("UserType", bases, {}, random_keyword=True)

        # Check to see if __init_subclass__ works in supported versions
        eq_(UserType._set_random_keyword_used_here, True)

    @testing.variation(
        "basetype",
        ["DeclarativeBase", "DeclarativeBaseNoMeta", "MappedAsDataclass"],
    )
    def test_kw_support_in_declarative_base(self, basetype):
        """test #10732"""

        if basetype.DeclarativeBase:

            class Base(DeclarativeBase):
                pass

        elif basetype.DeclarativeBaseNoMeta:

            class Base(DeclarativeBaseNoMeta):
                pass

        elif basetype.MappedAsDataclass:

            class Base(MappedAsDataclass):
                pass

        else:
            basetype.fail()

        class Mixin:
            def __init_subclass__(cls, random_keyword: bool, **kw) -> None:
                super().__init_subclass__(**kw)
                cls._set_random_keyword_used_here = random_keyword

        class User(Base, Mixin, random_keyword=True):
            __tablename__ = "user"
            id_ = Column(Integer, primary_key=True)

        eq_(User._set_random_keyword_used_here, True)

    def test_declarative_base_bad_registry(self):
        with assertions.expect_raises_message(
            exc.InvalidRequestError,
            "Declarative base class has a 'registry' attribute "
            "that is not an instance",
        ):

            class MyClass(DeclarativeBase):
                registry = {"foo": "bar"}

    def test_declarative_base_registry_and_type_map(self):
        with assertions.expect_raises_message(
            exc.InvalidRequestError,
            "Declarative base class has both a 'registry' attribute and a "
            "type_annotation_map entry.  Per-base type_annotation_maps",
        ):

            class MyClass(DeclarativeBase):
                registry = registry()
                type_annotation_map = {int: Integer}

    @testing.combinations(DeclarativeBase, DeclarativeBaseNoMeta)
    def test_declarative_base_used_directly(self, base):
        with assertions.expect_raises_message(
            exc.InvalidRequestError,
            f"Cannot use {base.__name__!r} directly as a declarative base",
        ):

            class MyClass(base):
                __tablename__ = "foobar"
                id: int = mapped_column(primary_key=True)

        with assertions.expect_raises_message(
            exc.InvalidRequestError,
            f"Cannot use {base.__name__!r} directly as a declarative base",
        ):

            class MyClass2(base):
                __table__ = sa.Table(
                    "foobar",
                    sa.MetaData(),
                    sa.Column("id", Integer, primary_key=True),
                )

    @testing.combinations(DeclarativeBase, DeclarativeBaseNoMeta)
    def test_declarative_base_fn_ok(self, base):
        # __tablename__ or __table__ as declared_attr are ok in the base
        class MyBase1(base):
            @declared_attr
            def __tablename__(cls):
                return cls.__name__

        class MyBase2(base):
            @declared_attr
            def __table__(cls):
                return sa.Table(
                    "foobar",
                    sa.MetaData(),
                    sa.Column("id", Integer, primary_key=True),
                )

    def test_cannot_add_to_selectable(self):
        class Base(DeclarativeBase):
            pass

        class Foo(Base):
            __table__ = (
                select(sa.Column("x", sa.Integer, primary_key=True))
                .select_from(sa.table("foo"))
                .subquery("foo")
            )

        with assertions.expect_raises_message(
            exc.InvalidRequestError,
            "Cannot add a new attribute to mapped class 'Foo' "
            "because it's not mapped against a table",
        ):
            Foo.y = mapped_column(sa.Text)

    def test_default_column_order(self, decl_base):
        class M1:
            a: Mapped[int]
            b: Mapped[int] = mapped_column(primary_key=True)

        class M2(decl_base):
            __abstract__ = True
            c: Mapped[int]
            d: Mapped[int]

        class M(M1, M2, decl_base):
            e: Mapped[int]
            f: Mapped[int]
            g: Mapped[int]

            __tablename__ = "m"

        actual = list(M.__table__.c.keys())
        expected = ["e", "f", "g", "a", "b", "c", "d"]
        eq_(actual, expected)

    def test_custom_column_sort_order(self, decl_base):
        class M1:
            a: Mapped[int] = mapped_column(sort_order=-42)
            b: Mapped[int] = mapped_column(primary_key=True)

        class M2(decl_base):
            __abstract__ = True
            c: Mapped[int] = mapped_column(sort_order=-1)
            d: Mapped[int]

        class M(M1, M2, decl_base):
            e: Mapped[int]
            f: Mapped[int] = mapped_column(sort_order=10)
            g: Mapped[int] = mapped_column(sort_order=-10)

            __tablename__ = "m"

        actual = list(M.__table__.c.keys())
        expected = ["a", "g", "c", "e", "b", "d", "f"]
        eq_(actual, expected)


@testing.combinations(
    ("declarative_base_nometa_superclass",),
    ("declarative_base_superclass",),
    ("dynamic",),
    ("explicit",),
    argnames="base_style",
    id_="s",
)
class DeclarativeMultiBaseTest(
    fixtures.TestBase,
    testing.AssertsExecutionResults,
    testing.AssertsCompiledSQL,
):
    __dialect__ = "default"

    base_style = "dynamic"

    def setup_test(self):
        global Base

        if self.base_style == "dynamic":
            Base = declarative_base()
        elif self.base_style == "explicit":
            mapper_registry = registry()

            class Base(metaclass=DeclarativeMeta):
                __abstract__ = True
                registry = mapper_registry
                metadata = mapper_registry.metadata

        elif self.base_style == "declarative_base_superclass":

            class Base(DeclarativeBase):
                pass

        elif self.base_style == "declarative_base_nometa_superclass":

            class Base(DeclarativeBaseNoMeta):
                pass

        else:
            assert False

    def teardown_test(self):
        close_all_sessions()
        clear_mappers()
        Base.metadata.drop_all(testing.db)

    @testing.fixture
    def require_metaclass(self):
        if type(Base) is type:
            testing.config.skip_test("current base has no metaclass")

    def test_basic(self):
        class User(Base, ComparableEntity):
            __tablename__ = "users"

            id = Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column("name", String(50))
            addresses = relationship("Address", backref="user")

        class Address(Base, ComparableEntity):
            __tablename__ = "addresses"

            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            email = Column(String(50), key="_email")
            user_id = Column(
                "user_id", Integer, ForeignKey("users.id"), key="_user_id"
            )

        Base.metadata.create_all(testing.db)

        eq_(Address.__table__.c["id"].name, "id")
        eq_(Address.__table__.c["_email"].name, "email")
        eq_(Address.__table__.c["_user_id"].name, "user_id")

        u1 = User(
            name="u1", addresses=[Address(email="one"), Address(email="two")]
        )
        sess = fixture_session()
        sess.add(u1)
        sess.flush()
        sess.expunge_all()

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

    @testing.variation("mora", ["mixin", "abstract"])
    def test_abstract_and_or_mixin(self, mora):
        if mora.abstract:

            class Employee(Base):
                __abstract__ = True

                id = mapped_column(Integer, primary_key=True, sort_order=-1)

            class Manager(Employee):
                __tablename__ = "manager"
                name = mapped_column(String(50))
                manager_data = mapped_column(String(40))

            class Engineer(Employee):
                __tablename__ = "engineer"

                name = mapped_column(String(50))
                engineer_info = mapped_column(String(40))

        elif mora.mixin:

            class Mixin:
                pass

            class EmployeeMixin:
                id = mapped_column(Integer, primary_key=True, sort_order=-1)

            class Manager(EmployeeMixin, Base):
                __tablename__ = "manager"
                name = mapped_column(String(50))
                manager_data = mapped_column(String(40))

            class Engineer(EmployeeMixin, Base):
                __tablename__ = "engineer"

                name = mapped_column(String(50))
                engineer_info = mapped_column(String(40))

        else:
            mora.fail()

        self.assert_compile(
            select(Engineer),
            "SELECT engineer.id, engineer.name, engineer.engineer_info "
            "FROM engineer",
        )

    def test_back_populates_setup(self):
        class User(Base):
            __tablename__ = "users"

            id = Column("id", Integer, primary_key=True)
            addresses = relationship("Address", back_populates="user")

        class Address(Base):
            __tablename__ = "addresses"

            id = Column(Integer, primary_key=True)
            user_id = Column(
                "user_id", Integer, ForeignKey("users.id"), key="_user_id"
            )
            user = relationship("User", back_populates="addresses")

        configure_mappers()

        assert (
            Address.__mapper__.attrs.user
            in User.__mapper__.attrs.addresses._reverse_property
        )
        assert (
            User.__mapper__.attrs.addresses
            in Address.__mapper__.attrs.user._reverse_property
        )

    def test_deferred_reflection_default_error(self, require_metaclass):
        class MyExt:
            @classmethod
            def prepare(cls):
                "sample prepare method"
                to_map = _DeferredDeclarativeConfig.classes_for_base(cls)
                for thingy in to_map:
                    thingy.map({})

            @classmethod
            def _sa_decl_prepare(cls):
                pass

        class User(MyExt, Base):
            __tablename__ = "user"
            id = Column(Integer, primary_key=True)

        # TODO; this use case only works when using
        # DeclarativeMeta or DeclarativeAttributeIntercept
        assert_raises_message(
            orm_exc.UnmappedClassError,
            "Class .*User has a deferred "
            "mapping on it.  It is not yet usable as a mapped class.",
            fixture_session().query,
            User,
        )

        User.prepare()

        self.assert_compile(
            fixture_session().query(User),
            'SELECT "user".id AS user_id FROM "user"',
        )

    def test_unicode_string_resolve(self):
        class User(Base, ComparableEntity):
            __tablename__ = "users"

            id = Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column("name", String(50))
            addresses = relationship("Address", backref="user")

        class Address(Base, ComparableEntity):
            __tablename__ = "addresses"

            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            email = Column(String(50), key="_email")
            user_id = Column(
                "user_id", Integer, ForeignKey("users.id"), key="_user_id"
            )

        assert User.addresses.property.mapper.class_ is Address

    def test_unicode_string_resolve_backref(self):
        class User(Base, ComparableEntity):
            __tablename__ = "users"

            id = Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column("name", String(50))

        class Address(Base, ComparableEntity):
            __tablename__ = "addresses"

            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            email = Column(String(50), key="_email")
            user_id = Column(
                "user_id", Integer, ForeignKey("users.id"), key="_user_id"
            )
            user = relationship(
                User,
                backref=backref("addresses", order_by="Address.email"),
            )

        assert Address.user.property.mapper.class_ is User

    def test_no_table(self):
        def go():
            class User(Base):
                id = Column("id", Integer, primary_key=True)

        assert_raises_message(
            sa.exc.InvalidRequestError, "does not have a __table__", go
        )

    def test_table_args_empty_dict(self):
        class MyModel(Base):
            __tablename__ = "test"
            id = Column(Integer, primary_key=True)
            __table_args__ = {}

    def test_table_args_empty_tuple(self):
        class MyModel(Base):
            __tablename__ = "test"
            id = Column(Integer, primary_key=True)
            __table_args__ = ()

    def test_cant_add_columns(self):
        t = Table(
            "t",
            Base.metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String),
        )

        def go():
            class User(Base):
                __table__ = t
                foo = Column(Integer, primary_key=True)

        # can't specify new columns not already in the table

        assert_raises_message(
            sa.exc.ArgumentError,
            "Can't add additional column 'foo' when specifying __table__",
            go,
        )

        # regular re-mapping works tho

        class Bar(Base):
            __table__ = t
            some_data = t.c.data

        assert (
            class_mapper(Bar).get_property("some_data").columns[0] is t.c.data
        )

    def test_non_sql_expression_warning_one(self):
        with assertions.expect_warnings(
            r"Attribute 'x' on class <class .*Foo.* appears to be a "
            r"non-schema SQLAlchemy expression object; "
        ):

            class Foo(Base):
                __tablename__ = "foo"

                id = Column(Integer, primary_key=True)
                x = sa.sql.expression.column(Integer)
                y = Column(Integer)

    def test_non_sql_expression_warning_two(self):
        class MyMixin:
            x = sa.sql.expression.column(Integer)
            y = Column(Integer)

        with assertions.expect_warnings(
            r"Attribute 'x' on class <class .*MyMixin.* appears to be a "
            r"non-schema SQLAlchemy expression object; "
        ):

            class Foo2(MyMixin, Base):
                __tablename__ = "foo2"

                id = Column(Integer, primary_key=True)

    def test_non_sql_expression_warning_three(self):
        with assertions.expect_warnings(
            r"Attribute 'x' on class <class .*Foo3.* appears to be a "
            r"non-schema SQLAlchemy expression object; "
        ):

            class Foo3(Base):
                __tablename__ = "foo3"

                id = Column(Integer, primary_key=True)

                @declared_attr
                def x(cls):
                    return sa.sql.expression.column(Integer)

                y = Column(Integer)

    def test_non_sql_expression_warning_four(self):
        with assertions.expect_warnings(
            r"Attribute 'x' on class <class .*Foo4.* appears to be a "
            r"non-schema SQLAlchemy expression object; "
        ):

            class MyMixin2:
                @declared_attr
                def x(cls):
                    return sa.sql.expression.column(Integer)

                y = Column(Integer)

            class Foo4(MyMixin2, Base):
                __tablename__ = "foo4"

                id = Column(Integer, primary_key=True)

    def test_non_sql_expression_warning_five(self):
        # test for #9537
        with assertions.expect_warnings(
            r"Attribute 'x' on class <class .*Foo5.* appears to be a "
            r"non-schema SQLAlchemy expression object; ",
            r"Attribute 'y' on class <class .*Foo5.* appears to be a "
            r"non-schema SQLAlchemy expression object; ",
        ):

            class Foo5(Base):
                __tablename__ = "foo5"

                id = Column(Integer, primary_key=True)
                x = Column("x", String()).collate("some collation")
                y = Column("y", Integer) + 5
                z = "im not a sqlalchemy thing"

    @testing.variation(
        "attr_type",
        [
            "column",
            "mapped_column",
            "relationship",
            "synonym",
            "column_property",
        ],
    )
    def test_attr_assigned_to_multiple_keys(self, attr_type, decl_base):
        """test #3532"""

        column_warning = expect_warnings(
            "On class 'A', Column object 'a' named directly multiple "
            "times, only one will be used: a, b. Consider using "
            "orm.synonym instead"
        )

        other_warning = expect_warnings(
            "ORM mapped property A.a being assigned to attribute 'b' is "
            "already associated with attribute 'a'. The attribute will be "
            "de-associated from 'a'."
        )
        if attr_type.column:
            with column_warning:

                class A(decl_base):
                    __tablename__ = "a"

                    id = Column(Integer, primary_key=True)

                    a = Column(Integer)

                    b = a

        elif attr_type.mapped_column:
            with column_warning:

                class A(decl_base):
                    __tablename__ = "a"

                    id = mapped_column(Integer, primary_key=True)

                    a = mapped_column(Integer)

                    b = a

        elif attr_type.relationship:
            with other_warning:

                class B(decl_base):
                    __tablename__ = "b"

                    id = mapped_column(Integer, primary_key=True)
                    aid = mapped_column(ForeignKey("a.id"))

                class A(decl_base):
                    __tablename__ = "a"

                    id = mapped_column(Integer, primary_key=True)

                    a = relationship("B")

                    b = a

                decl_base.registry.configure()
        elif attr_type.column_property:
            with other_warning:

                class A(decl_base):
                    __tablename__ = "a"

                    id = mapped_column(Integer, primary_key=True)

                    a = column_property(literal("foo") + literal("bar"))

                    b = a

        elif attr_type.synonym:
            with other_warning:

                class A(decl_base):
                    __tablename__ = "a"

                    id = mapped_column(Integer, primary_key=True)
                    g = mapped_column(Integer)
                    a = synonym("g")

                    b = a

        else:
            attr_type.fail()

    def test_column_named_twice(self):
        with (
            expect_warnings(
                "On class 'Foo', Column object 'x' named directly multiple "
                "times, only one will be used: x, y. Consider using "
                "orm.synonym instead"
            ),
            expect_raises(exc.DuplicateColumnError),
        ):

            class Foo(Base):
                __tablename__ = "foo"

                id = Column(Integer, primary_key=True)
                x = Column("x", Integer)
                y = Column("x", Integer)

    @testing.variation("style", ["old", "new"])
    def test_column_repeated_under_prop(self, style):
        with (
            expect_warnings(
                "On class 'Foo', Column object 'x' named directly multiple "
                "times, only one will be used: x, y, z. Consider using "
                "orm.synonym instead"
            ),
            expect_raises(exc.DuplicateColumnError),
        ):
            if style.old:

                class Foo(Base):
                    __tablename__ = "foo"

                    id = Column(Integer, primary_key=True)
                    x = Column("x", Integer)
                    y = column_property(x)
                    z = Column("x", Integer)

            elif style.new:

                class Foo(Base):
                    __tablename__ = "foo"

                    id = mapped_column(Integer, primary_key=True)
                    x = mapped_column("x", Integer)
                    y = column_property(x)
                    z = mapped_column("x", Integer)

            else:
                style.fail()

    def test_using_explicit_prop_in_schema_objects(self):
        class Foo(Base):
            __tablename__ = "foo"

            id = Column(Integer, primary_key=True)
            cprop = column_property(Column(Integer))

            __table_args__ = (UniqueConstraint(cprop),)

        uq = [
            c
            for c in Foo.__table__.constraints
            if isinstance(c, UniqueConstraint)
        ][0]
        is_(uq.columns.cprop, Foo.__table__.c.cprop)

        class Bar(Base):
            __tablename__ = "bar"

            id = Column(Integer, primary_key=True)
            cprop = deferred(Column(Integer))

            __table_args__ = (CheckConstraint(cprop > sa.func.foo()),)

        ck = [
            c
            for c in Bar.__table__.constraints
            if isinstance(c, CheckConstraint)
        ][0]
        is_(ck.columns.cprop, Bar.__table__.c.cprop)

        # test the existing failure case in case something changes
        def go():
            class Bat(Base):
                __tablename__ = "bat"

                id = Column(Integer, primary_key=True)
                cprop = deferred(Column(Integer))

                # we still can't do an expression like
                # "cprop > 5" because the column property isn't
                # a full blown column

                __table_args__ = (CheckConstraint(cprop > 5),)

        assert_raises(TypeError, go)

    def test_relationship_level_msg_for_invalid_callable(self):
        class A(Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)

        class B(Base):
            __tablename__ = "b"
            id = Column(Integer, primary_key=True)
            a_id = Column(Integer, ForeignKey("a.id"))
            a = relationship("a")

        assert_raises_message(
            sa.exc.ArgumentError,
            "relationship 'a' expects a class or a mapper "
            "argument .received: .*Table",
            configure_mappers,
        )

    def test_relationship_level_msg_for_invalid_object(self):
        class A(Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)

        class B(Base):
            __tablename__ = "b"
            id = Column(Integer, primary_key=True)
            a_id = Column(Integer, ForeignKey("a.id"))
            a = relationship(A.__table__)

        assert_raises_message(
            sa.exc.ArgumentError,
            "relationship 'a' expects a class or a mapper "
            "argument .received: .*Table",
            configure_mappers,
        )

    # currently "registry" is allowed, "metadata" is not.
    @testing.combinations(
        ("metadata", True), ("registry", False), argnames="name, expect_raise"
    )
    @testing.variation("attrtype", ["column", "relationship"])
    def test_reserved_identifiers(
        self, decl_base, name, expect_raise, attrtype
    ):
        if attrtype.column:
            clsdict = {
                "__tablename__": "user",
                "id": Column(Integer, primary_key=True),
                name: Column(Integer),
            }
        elif attrtype.relationship:
            clsdict = {
                "__tablename__": "user",
                "id": Column(Integer, primary_key=True),
                name: relationship("Address"),
            }

            class Address(decl_base):
                __tablename__ = "address"
                id = Column(Integer, primary_key=True)
                user_id = Column(ForeignKey("user.id"))

        else:
            assert False

        if expect_raise:
            with expect_raises_message(
                exc.InvalidRequestError,
                f"Attribute name '{name}' is reserved "
                "when using the Declarative API.",
            ):
                type("User", (decl_base,), clsdict)
        else:
            User = type("User", (decl_base,), clsdict)
            assert getattr(User, name).property

    def test_recompile_on_othermapper(self):
        """declarative version of the same test in mappers.py"""

        class User(Base):
            __tablename__ = "users"

            id = Column("id", Integer, primary_key=True)
            name = Column("name", String(50))

        class Address(Base):
            __tablename__ = "addresses"

            id = Column("id", Integer, primary_key=True)
            email = Column("email", String(50))
            user_id = Column("user_id", Integer, ForeignKey("users.id"))
            user = relationship(
                "User", primaryjoin=user_id == User.id, backref="addresses"
            )

        assert User.__mapper__.registry._new_mappers is True
        u = User()  # noqa
        assert User.addresses
        assert User.__mapper__.registry._new_mappers is False

    def test_string_dependency_resolution(self):
        class User(Base, ComparableEntity):
            __tablename__ = "users"
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column(String(50))
            addresses = relationship(
                "Address",
                order_by="desc(Address.email)",
                primaryjoin="User.id==Address.user_id",
                foreign_keys="[Address.user_id]",
                backref=backref(
                    "user",
                    primaryjoin="User.id==Address.user_id",
                    foreign_keys="[Address.user_id]",
                ),
            )

        class Address(Base, ComparableEntity):
            __tablename__ = "addresses"
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            email = Column(String(50))
            user_id = Column(Integer)  # note no foreign key

        Base.metadata.create_all(testing.db)
        sess = fixture_session()
        u1 = User(
            name="ed",
            addresses=[
                Address(email="abc"),
                Address(email="def"),
                Address(email="xyz"),
            ],
        )
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        eq_(
            sess.query(User).filter(User.name == "ed").one(),
            User(
                name="ed",
                addresses=[
                    Address(email="xyz"),
                    Address(email="def"),
                    Address(email="abc"),
                ],
            ),
        )

        class Foo(Base, ComparableEntity):
            __tablename__ = "foo"
            id = Column(Integer, primary_key=True)
            rel = relationship("User", primaryjoin="User.addresses==Foo.id")

        assert_raises_message(
            exc.InvalidRequestError,
            "'addresses' is not an instance of ColumnProperty",
            configure_mappers,
        )

    def test_string_dependency_resolution_synonym(self):
        class User(Base, ComparableEntity):
            __tablename__ = "users"
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column(String(50))

        Base.metadata.create_all(testing.db)
        sess = fixture_session()
        u1 = User(name="ed")
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        eq_(sess.query(User).filter(User.name == "ed").one(), User(name="ed"))

        class Foo(Base, ComparableEntity):
            __tablename__ = "foo"
            id = Column(Integer, primary_key=True)
            _user_id = Column(Integer)
            rel = relationship(
                "User",
                uselist=False,
                foreign_keys=[User.id],
                primaryjoin="Foo.user_id==User.id",
            )

            @synonym_for("_user_id")
            @property
            def user_id(self):
                return self._user_id

        foo = Foo()
        foo.rel = u1
        assert foo.rel == u1

    def test_string_dependency_resolution_orm_descriptor(self):
        from sqlalchemy.ext.hybrid import hybrid_property

        class User(Base):
            __tablename__ = "user"
            id = Column(Integer, primary_key=True)
            firstname = Column(String(50))
            lastname = Column(String(50))
            game_id = Column(Integer, ForeignKey("game.id"))

            @hybrid_property
            def fullname(self):
                return self.firstname + " " + self.lastname

        class Game(Base):
            __tablename__ = "game"
            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            users = relationship("User", order_by="User.fullname")

        s = fixture_session()
        self.assert_compile(
            s.query(Game).options(joinedload(Game.users)),
            "SELECT game.id AS game_id, game.name AS game_name, "
            "user_1.id AS user_1_id, user_1.firstname AS user_1_firstname, "
            "user_1.lastname AS user_1_lastname, "
            "user_1.game_id AS user_1_game_id "
            'FROM game LEFT OUTER JOIN "user" AS user_1 ON game.id = '
            "user_1.game_id ORDER BY "
            "user_1.firstname || :firstname_1 || user_1.lastname",
        )

    def test_string_dependency_resolution_asselectable(self):
        class A(Base):
            __tablename__ = "a"

            id = Column(Integer, primary_key=True)
            b_id = Column(ForeignKey("b.id"))

            d = relationship(
                "D",
                secondary=lambda: join(B, D, B.d_id == D.id).join(
                    C, C.d_id == D.id
                ),
                primaryjoin="and_(A.b_id == B.id, A.id == C.a_id)",
                secondaryjoin="D.id == B.d_id",
            )

        class B(Base):
            __tablename__ = "b"

            id = Column(Integer, primary_key=True)
            d_id = Column(ForeignKey("d.id"))

        class C(Base):
            __tablename__ = "c"

            id = Column(Integer, primary_key=True)
            a_id = Column(ForeignKey("a.id"))
            d_id = Column(ForeignKey("d.id"))

        class D(Base):
            __tablename__ = "d"

            id = Column(Integer, primary_key=True)

        s = fixture_session()
        self.assert_compile(
            s.query(A).join(A.d),
            "SELECT a.id AS a_id, a.b_id AS a_b_id FROM a JOIN "
            "(b AS b_1 JOIN d AS d_1 ON b_1.d_id = d_1.id "
            "JOIN c AS c_1 ON c_1.d_id = d_1.id) ON a.b_id = b_1.id "
            "AND a.id = c_1.a_id JOIN d ON d.id = b_1.d_id",
        )

    def test_string_dependency_resolution_no_table(self):
        class User(Base, ComparableEntity):
            __tablename__ = "users"
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column(String(50))

        class Bar(Base, ComparableEntity):
            __tablename__ = "bar"
            id = Column(Integer, primary_key=True)
            rel = relationship("User", primaryjoin="User.id==Bar.__table__.id")

        assert_raises_message(
            AttributeError,
            "does not have a mapped column named '__table__'",
            configure_mappers,
        )

    def test_string_w_pj_annotations(self):
        class User(Base, ComparableEntity):
            __tablename__ = "users"
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column(String(50))

        class Address(Base, ComparableEntity):
            __tablename__ = "addresses"
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            email = Column(String(50))
            user_id = Column(Integer)
            user = relationship(
                "User", primaryjoin="remote(User.id)==foreign(Address.user_id)"
            )

        eq_(
            Address.user.property._join_condition.local_remote_pairs,
            [(Address.__table__.c.user_id, User.__table__.c.id)],
        )

    def test_string_dependency_resolution_no_magic(self):
        """test that full tinkery expressions work as written"""

        class User(Base, ComparableEntity):
            __tablename__ = "users"
            id = Column(Integer, primary_key=True)
            addresses = relationship(
                "Address",
                primaryjoin="User.id==Address.user_id.prop.columns[0]",
            )

        class Address(Base, ComparableEntity):
            __tablename__ = "addresses"
            id = Column(Integer, primary_key=True)
            user_id = Column(Integer, ForeignKey("users.id"))

        configure_mappers()
        eq_(
            str(User.addresses.prop.primaryjoin),
            "users.id = addresses.user_id",
        )

    def test_string_dependency_resolution_module_qualified(self):
        class User(Base, ComparableEntity):
            __tablename__ = "users"
            id = Column(Integer, primary_key=True)
            addresses = relationship(
                "%s.Address" % __name__,
                primaryjoin="%s.User.id==%s.Address.user_id.prop.columns[0]"
                % (__name__, __name__),
            )

        class Address(Base, ComparableEntity):
            __tablename__ = "addresses"
            id = Column(Integer, primary_key=True)
            user_id = Column(Integer, ForeignKey("users.id"))

        configure_mappers()
        eq_(
            str(User.addresses.prop.primaryjoin),
            "users.id = addresses.user_id",
        )

    def test_string_dependency_resolution_in_backref(self):
        class User(Base, ComparableEntity):
            __tablename__ = "users"
            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            addresses = relationship(
                "Address",
                primaryjoin="User.id==Address.user_id",
                backref="user",
            )

        class Address(Base, ComparableEntity):
            __tablename__ = "addresses"
            id = Column(Integer, primary_key=True)
            email = Column(String(50))
            user_id = Column(Integer, ForeignKey("users.id"))

        configure_mappers()
        eq_(
            str(User.addresses.property.primaryjoin),
            str(Address.user.property.primaryjoin),
        )

    def test_string_dependency_resolution_tables(self):
        class User(Base, ComparableEntity):
            __tablename__ = "users"
            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            props = relationship(
                "Prop",
                secondary="user_to_prop",
                primaryjoin="User.id==user_to_prop.c.user_id",
                secondaryjoin="user_to_prop.c.prop_id==Prop.id",
                backref="users",
            )

        class Prop(Base, ComparableEntity):
            __tablename__ = "props"
            id = Column(Integer, primary_key=True)
            name = Column(String(50))

        user_to_prop = Table(
            "user_to_prop",
            Base.metadata,
            Column("user_id", Integer, ForeignKey("users.id")),
            Column("prop_id", Integer, ForeignKey("props.id")),
        )

        configure_mappers()
        assert (
            class_mapper(User).get_property("props").secondary is user_to_prop
        )

    def test_string_dependency_resolution_table_over_class(self):
        # test for second half of #5774
        class User(Base, ComparableEntity):
            __tablename__ = "users"
            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            props = relationship(
                "Prop",
                secondary="Secondary",
                backref="users",
            )

        class Prop(Base, ComparableEntity):
            __tablename__ = "props"
            id = Column(Integer, primary_key=True)
            name = Column(String(50))

        # class name and table name match
        class Secondary(Base):
            __tablename__ = "Secondary"
            user_id = Column(Integer, ForeignKey("users.id"), primary_key=True)
            prop_id = Column(Integer, ForeignKey("props.id"), primary_key=True)

        configure_mappers()
        assert (
            class_mapper(User).get_property("props").secondary
            is Secondary.__table__
        )

    def test_string_dependency_resolution_class_over_table(self):
        # test for second half of #5774
        class User(Base, ComparableEntity):
            __tablename__ = "users"
            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            secondary = relationship(
                "Secondary",
            )

        # class name and table name match
        class Secondary(Base):
            __tablename__ = "Secondary"
            user_id = Column(Integer, ForeignKey("users.id"), primary_key=True)

        configure_mappers()
        assert (
            class_mapper(User).get_property("secondary").mapper
            is Secondary.__mapper__
        )

    def test_uncompiled_attributes_in_relationship(self):
        class Address(Base, ComparableEntity):
            __tablename__ = "addresses"
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            email = Column(String(50))
            user_id = Column(Integer, ForeignKey("users.id"))

        class User(Base, ComparableEntity):
            __tablename__ = "users"
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column(String(50))
            addresses = relationship(
                "Address",
                order_by=Address.email,
                foreign_keys=Address.user_id,
                remote_side=Address.user_id,
            )

        # get the mapper for User.   User mapper will compile,
        # "addresses" relationship will call upon Address.user_id for
        # its clause element.  Address.user_id is a _CompileOnAttr,
        # which then calls class_mapper(Address).  But !  We're already
        # "in compilation", but class_mapper(Address) needs to
        # initialize regardless, or COA's assertion fails and things
        # generally go downhill from there.

        class_mapper(User)
        Base.metadata.create_all(testing.db)
        sess = fixture_session()
        u1 = User(
            name="ed",
            addresses=[
                Address(email="abc"),
                Address(email="xyz"),
                Address(email="def"),
            ],
        )
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        eq_(
            sess.query(User).filter(User.name == "ed").one(),
            User(
                name="ed",
                addresses=[
                    Address(email="abc"),
                    Address(email="def"),
                    Address(email="xyz"),
                ],
            ),
        )

    def test_nice_dependency_error(self):
        class User(Base):
            __tablename__ = "users"
            id = Column("id", Integer, primary_key=True)
            addresses = relationship("Address")

        class Address(Base):
            __tablename__ = "addresses"
            id = Column(Integer, primary_key=True)
            foo = sa.orm.column_property(User.id == 5)

        # this used to raise an error when accessing User.id but that's
        # no longer the case since we got rid of _CompileOnAttr.

        assert_raises(sa.exc.ArgumentError, configure_mappers)

    def test_nice_dependency_error_works_with_hasattr(self):
        class User(Base):
            __tablename__ = "users"
            id = Column("id", Integer, primary_key=True)
            addresses = relationship("Address")

        # hasattr() on a compile-loaded attribute
        try:
            hasattr(User.addresses, "property")
        except exc.InvalidRequestError:
            assert True

        # the exception is preserved.  Remains the
        # same through repeated calls.
        for i in range(3):
            assert_raises_message(
                sa.exc.InvalidRequestError,
                "^One or more mappers failed to initialize"
                " - can't proceed with initialization of other mappers. "
                r"Triggering mapper: 'Mapper\[User\(users\)\]'. "
                "Original exception was: When initializing.*",
                configure_mappers,
            )

    def test_uses_get_on_class_col_fk(self):
        # test [ticket:1492]

        class Topic(Base):
            __tablename__ = "topic"
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )

        class Detail(Base):
            __tablename__ = "detail"
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            topic_id = Column(None, ForeignKey(Topic.id))
            topic = relationship(Topic)

        Base.metadata.create_all(testing.db)
        configure_mappers()
        assert class_mapper(Detail).get_property("topic").strategy.use_get
        t1 = Topic()
        d1 = Detail(topic=t1)
        sess = fixture_session()
        sess.add(d1)
        sess.flush()
        sess.expunge_all()
        d1 = sess.query(Detail).first()
        t1 = sess.query(Topic).first()

        def go():
            assert d1.topic

        self.assert_sql_count(testing.db, go, 0)

    def test_index_doesnt_compile(self):
        class User(Base):
            __tablename__ = "users"
            id = Column("id", Integer, primary_key=True)
            name = Column("name", String(50))
            error = relationship("Address")

        i = Index("my_index", User.name)

        # compile fails due to the nonexistent Addresses relationship
        assert_raises(sa.exc.InvalidRequestError, configure_mappers)

        # index configured
        assert i in User.__table__.indexes
        assert User.__table__.c.id not in set(i.columns)
        assert User.__table__.c.name in set(i.columns)

        # tables create fine
        Base.metadata.create_all(testing.db)

    @testing.fixture
    def assert_user_address_mapping(self, connection):
        def go(User, Address):
            User.metadata.create_all(connection)

            u1 = User(
                name="u1",
                addresses=[Address(email="one"), Address(email="two")],
            )

            with Session(connection) as sess:
                sess.add(u1)
                sess.flush()
                sess.expunge_all()
                eq_(
                    sess.query(User).all(),
                    [
                        User(
                            name="u1",
                            addresses=[
                                Address(email="one"),
                                Address(email="two"),
                            ],
                        )
                    ],
                )
                a1 = sess.query(Address).filter(Address.email == "two").one()
                eq_(a1, Address(email="two"))
                eq_(a1.user, User(name="u1"))

        yield go

    @testing.combinations(Column, mapped_column, argnames="_column")
    def test_add_prop_auto(
        self, require_metaclass, assert_user_address_mapping, _column
    ):
        class User(Base, ComparableEntity):
            __tablename__ = "users"
            id = Column("id", Integer, primary_key=True)

        User.name = _column("name", String(50))
        User.addresses = relationship("Address", backref="user")

        class Address(Base, ComparableEntity):
            __tablename__ = "addresses"
            id = _column(Integer, primary_key=True)

        Address.email = _column(String(50), key="_email")
        Address.user_id = _column(
            "user_id", Integer, ForeignKey("users.id"), key="_user_id"
        )

        eq_(Address.__table__.c["id"].name, "id")
        eq_(Address.__table__.c["_email"].name, "email")
        eq_(Address.__table__.c["_user_id"].name, "user_id")

        assert_user_address_mapping(User, Address)

    @testing.combinations(Column, mapped_column, argnames="_column")
    def test_add_prop_manual(self, assert_user_address_mapping, _column):
        class User(Base, ComparableEntity):
            __tablename__ = "users"
            id = _column("id", Integer, primary_key=True)

        add_mapped_attribute(User, "name", _column("name", String(50)))
        add_mapped_attribute(
            User, "addresses", relationship("Address", backref="user")
        )

        class Address(Base, ComparableEntity):
            __tablename__ = "addresses"
            id = _column(Integer, primary_key=True)

        add_mapped_attribute(
            Address, "email", _column(String(50), key="_email")
        )
        add_mapped_attribute(
            Address,
            "user_id",
            _column(
                "user_id", Integer, ForeignKey("users.id"), key="_user_id"
            ),
        )

        eq_(Address.__table__.c["id"].name, "id")
        eq_(Address.__table__.c["_email"].name, "email")
        eq_(Address.__table__.c["_user_id"].name, "user_id")

        assert_user_address_mapping(User, Address)

    def test_alt_name_attr_subclass_column_inline(self):
        # [ticket:2900]
        class A(Base):
            __tablename__ = "a"
            id = Column("id", Integer, primary_key=True)
            data = Column("data")

        class ASub(A):
            brap = A.data

        assert ASub.brap.property is A.data.property
        assert isinstance(
            ASub.brap.original_property, descriptor_props.SynonymProperty
        )

    def test_alt_name_attr_subclass_relationship_inline(self):
        # [ticket:2900]
        class A(Base):
            __tablename__ = "a"
            id = Column("id", Integer, primary_key=True)
            b_id = Column(Integer, ForeignKey("b.id"))
            b = relationship("B", backref="as_")

        class B(Base):
            __tablename__ = "b"
            id = Column("id", Integer, primary_key=True)

        configure_mappers()

        class ASub(A):
            brap = A.b

        assert ASub.brap.property is A.b.property
        assert isinstance(
            ASub.brap.original_property, descriptor_props.SynonymProperty
        )
        ASub(brap=B())

    def test_alt_name_attr_subclass_column_attrset(self, require_metaclass):
        # [ticket:2900]
        class A(Base):
            __tablename__ = "a"
            id = Column("id", Integer, primary_key=True)
            data = Column("data")

        A.brap = A.data
        assert A.brap.property is A.data.property
        assert isinstance(
            A.brap.original_property, descriptor_props.SynonymProperty
        )

    def test_alt_name_attr_subclass_relationship_attrset(
        self, require_metaclass
    ):
        # [ticket:2900]
        class A(Base):
            __tablename__ = "a"
            id = Column("id", Integer, primary_key=True)
            b_id = Column(Integer, ForeignKey("b.id"))
            b = relationship("B", backref="as_")

        A.brap = A.b

        class B(Base):
            __tablename__ = "b"
            id = Column("id", Integer, primary_key=True)

        assert A.brap.property is A.b.property
        assert isinstance(
            A.brap.original_property, descriptor_props.SynonymProperty
        )
        A(brap=B())

    def test_eager_order_by(self):
        class Address(Base, ComparableEntity):
            __tablename__ = "addresses"
            id = Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            )
            email = Column("email", String(50))
            user_id = Column("user_id", Integer, ForeignKey("users.id"))

        class User(Base, ComparableEntity):
            __tablename__ = "users"
            id = Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column("name", String(50))
            addresses = relationship("Address", order_by=Address.email)

        Base.metadata.create_all(testing.db)
        u1 = User(
            name="u1", addresses=[Address(email="two"), Address(email="one")]
        )
        sess = fixture_session()
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        eq_(
            sess.query(User).options(joinedload(User.addresses)).all(),
            [
                User(
                    name="u1",
                    addresses=[Address(email="one"), Address(email="two")],
                )
            ],
        )

    def test_order_by_multi(self):
        class Address(Base, ComparableEntity):
            __tablename__ = "addresses"
            id = Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            )
            email = Column("email", String(50))
            user_id = Column("user_id", Integer, ForeignKey("users.id"))

        class User(Base, ComparableEntity):
            __tablename__ = "users"
            id = Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column("name", String(50))
            addresses = relationship(
                "Address", order_by=(Address.email, Address.id)
            )

        Base.metadata.create_all(testing.db)
        u1 = User(
            name="u1", addresses=[Address(email="two"), Address(email="one")]
        )
        sess = fixture_session()
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        u = sess.query(User).filter(User.name == "u1").one()
        u.addresses

    def test_oops(self):
        with testing.expect_warnings(
            "Ignoring declarative-like tuple value of attribute 'name'"
        ):

            class User(Base, ComparableEntity):
                __tablename__ = "users"
                id = Column("id", Integer, primary_key=True)
                name = (Column("name", String(50)),)

    def test_table_args_no_dict(self):
        class Foo1(Base):
            __tablename__ = "foo"
            __table_args__ = (ForeignKeyConstraint(["id"], ["foo.bar"]),)
            id = Column("id", Integer, primary_key=True)
            bar = Column("bar", Integer)

        assert Foo1.__table__.c.id.references(Foo1.__table__.c.bar)

    def test_table_args_type(self):
        def err():
            class Foo1(Base):
                __tablename__ = "foo"
                __table_args__ = ForeignKeyConstraint(["id"], ["foo.id"])
                id = Column("id", Integer, primary_key=True)

        assert_raises_message(
            sa.exc.ArgumentError, "__table_args__ value must be a tuple, ", err
        )

    def test_table_args_none(self):
        class Foo2(Base):
            __tablename__ = "foo"
            __table_args__ = None
            id = Column("id", Integer, primary_key=True)

        assert Foo2.__table__.kwargs == {}

    def test_table_args_dict_format(self):
        class Foo2(Base):
            __tablename__ = "foo"
            __table_args__ = {"mysql_engine": "InnoDB"}
            id = Column("id", Integer, primary_key=True)

        assert Foo2.__table__.kwargs["mysql_engine"] == "InnoDB"

    def test_table_args_tuple_format(self):
        class Foo2(Base):
            __tablename__ = "foo"
            __table_args__ = {"mysql_engine": "InnoDB"}
            id = Column("id", Integer, primary_key=True)

        class Bar(Base):
            __tablename__ = "bar"
            __table_args__ = (
                ForeignKeyConstraint(["id"], ["foo.id"]),
                {"mysql_engine": "InnoDB"},
            )
            id = Column("id", Integer, primary_key=True)

        assert Bar.__table__.c.id.references(Foo2.__table__.c.id)
        assert Bar.__table__.kwargs["mysql_engine"] == "InnoDB"

    def test_table_cls_attribute(self):
        class Foo(Base):
            __tablename__ = "foo"

            @classmethod
            def __table_cls__(cls, *arg, **kw):
                name = arg[0]
                return Table(name + "bat", *arg[1:], **kw)

            id = Column(Integer, primary_key=True)

        eq_(Foo.__table__.name, "foobat")

    def test_table_cls_attribute_return_none(self):
        # this is separate from the "fixture" version of Column used in the
        # rest of this suite
        from sqlalchemy.schema import Column

        class AutoTable:
            @declared_attr
            def __tablename__(cls):
                return cls.__name__

            @classmethod
            def __table_cls__(cls, *arg, **kw):
                for obj in arg[1:]:
                    if (
                        isinstance(obj, Column) and obj.primary_key
                    ) or isinstance(obj, PrimaryKeyConstraint):
                        return Table(*arg, **kw)

                return None

        class Person(AutoTable, Base):
            id = Column(Integer, primary_key=True)

        class Employee(Person):
            employee_name = Column(String)

        is_(inspect(Employee).local_table, Person.__table__)

    def test_expression(self, require_metaclass):
        class User(Base, ComparableEntity):
            __tablename__ = "users"
            id = Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column("name", String(50))
            addresses = relationship("Address", backref="user")

        class Address(Base, ComparableEntity):
            __tablename__ = "addresses"
            id = Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            )
            email = Column("email", String(50))
            user_id = Column("user_id", Integer, ForeignKey("users.id"))

        User.address_count = sa.orm.column_property(
            sa.select(sa.func.count(Address.id))
            .where(Address.user_id == User.id)
            .scalar_subquery()
        )
        Base.metadata.create_all(testing.db)
        u1 = User(
            name="u1", addresses=[Address(email="one"), Address(email="two")]
        )
        sess = fixture_session()
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        eq_(
            sess.query(User).all(),
            [
                User(
                    name="u1",
                    address_count=2,
                    addresses=[Address(email="one"), Address(email="two")],
                )
            ],
        )

    def test_useless_declared_attr(self):
        class Address(Base, ComparableEntity):
            __tablename__ = "addresses"
            id = Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            )
            email = Column("email", String(50))
            user_id = Column("user_id", Integer, ForeignKey("users.id"))

        class User(Base, ComparableEntity):
            __tablename__ = "users"
            id = Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column("name", String(50))
            addresses = relationship("Address", backref="user")

            @declared_attr
            def address_count(cls):
                # this doesn't really gain us anything.  but if
                # one is used, lets have it function as expected...
                return sa.orm.column_property(
                    sa.select(sa.func.count(Address.id))
                    .where(Address.user_id == cls.id)
                    .scalar_subquery()
                )

        Base.metadata.create_all(testing.db)
        u1 = User(
            name="u1", addresses=[Address(email="one"), Address(email="two")]
        )
        sess = fixture_session()
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        eq_(
            sess.query(User).all(),
            [
                User(
                    name="u1",
                    address_count=2,
                    addresses=[Address(email="one"), Address(email="two")],
                )
            ],
        )

    def test_declared_on_base_class(self):
        class MyBase(Base):
            __tablename__ = "foo"
            id = Column(Integer, primary_key=True)

            @declared_attr
            def somecol(cls):
                return Column(Integer)

        class MyClass(MyBase):
            __tablename__ = "bar"
            id = Column(Integer, ForeignKey("foo.id"), primary_key=True)

        # previously, the 'somecol' declared_attr would be ignored
        # by the mapping and would remain unused.  now we take
        # it as part of MyBase.

        assert "somecol" in MyBase.__table__.c
        assert "somecol" not in MyClass.__table__.c

    def test_decl_cascading_warns_non_mixin(self):
        with expect_warnings(
            "Use of @declared_attr.cascading only applies to "
            "Declarative 'mixin' and 'abstract' classes.  "
            "Currently, this flag is ignored on mapped class "
            "<class '.*.MyBase'>"
        ):

            class MyBase(Base):
                __tablename__ = "foo"
                id = Column(Integer, primary_key=True)

                @declared_attr.cascading
                def somecol(cls):
                    return Column(Integer)

    def test_column(self, require_metaclass):
        class User(Base, ComparableEntity):
            __tablename__ = "users"
            id = Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column("name", String(50))

        User.a = Column("a", String(10))
        User.b = Column(String(10))
        Base.metadata.create_all(testing.db)
        u1 = User(name="u1", a="a", b="b")
        eq_(u1.a, "a")
        eq_(User.a.get_history(u1), (["a"], (), ()))
        sess = fixture_session()
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        eq_(sess.query(User).all(), [User(name="u1", a="a", b="b")])

    def test_active_history_columns(self):
        class Foo(Base):
            __tablename__ = "foo"

            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            a = column_property(Column(String), active_history=True)
            b = mapped_column(String, active_history=True)
            c = column_property(Column(String))
            d = mapped_column(String)

        self.assert_compile(
            select(Foo), "SELECT foo.id, foo.a, foo.b, foo.c, foo.d FROM foo"
        )
        eq_(Foo.a.impl.active_history, True)
        eq_(Foo.b.impl.active_history, True)
        eq_(Foo.c.impl.active_history, False)
        eq_(Foo.d.impl.active_history, False)

    def test_column_properties(self):
        class Address(Base, ComparableEntity):
            __tablename__ = "addresses"
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            email = Column(String(50))
            user_id = Column(Integer, ForeignKey("users.id"))

        class User(Base, ComparableEntity):
            __tablename__ = "users"
            id = Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column("name", String(50))

            adr_count = sa.orm.column_property(
                sa.select(sa.func.count(Address.id))
                .where(Address.user_id == id)
                .scalar_subquery()
            )
            addresses = relationship(Address)

        Base.metadata.create_all(testing.db)
        u1 = User(
            name="u1", addresses=[Address(email="one"), Address(email="two")]
        )
        sess = fixture_session()
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        eq_(
            sess.query(User).all(),
            [
                User(
                    name="u1",
                    adr_count=2,
                    addresses=[Address(email="one"), Address(email="two")],
                )
            ],
        )

    def test_column_properties_2(self):
        class Address(Base, ComparableEntity):
            __tablename__ = "addresses"
            id = Column(Integer, primary_key=True)
            email = Column(String(50))
            user_id = Column(Integer, ForeignKey("users.id"))

        class User(Base, ComparableEntity):
            __tablename__ = "users"
            id = Column("id", Integer, primary_key=True)
            name = Column("name", String(50))

            # this is not "valid" but we want to test that Address.id
            # doesn't get stuck into user's table

            adr_count = Address.id

        eq_(set(User.__table__.c.keys()), {"id", "name"})
        eq_(set(Address.__table__.c.keys()), {"id", "email", "user_id"})

    def test_deferred(self):
        class User(Base, ComparableEntity):
            __tablename__ = "users"
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = sa.orm.deferred(Column(String(50)))

        Base.metadata.create_all(testing.db)
        sess = fixture_session()
        sess.add(User(name="u1"))
        sess.flush()
        sess.expunge_all()
        u1 = sess.query(User).filter(User.name == "u1").one()
        assert "name" not in u1.__dict__

        def go():
            eq_(u1.name, "u1")

        self.assert_sql_count(testing.db, go, 1)

    def test_composite_inline(self):
        class AddressComposite(ComparableEntity):
            def __init__(self, street, state):
                self.street = street
                self.state = state

            def __composite_values__(self):
                return [self.street, self.state]

        class User(Base, ComparableEntity):
            __tablename__ = "user"
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            address = composite(
                AddressComposite,
                Column("street", String(50)),
                Column("state", String(2)),
            )

        Base.metadata.create_all(testing.db)
        sess = fixture_session()
        sess.add(User(address=AddressComposite("123 anywhere street", "MD")))
        sess.commit()
        eq_(
            sess.query(User).all(),
            [User(address=AddressComposite("123 anywhere street", "MD"))],
        )

    def test_composite_separate(self):
        class AddressComposite(ComparableEntity):
            def __init__(self, street, state):
                self.street = street
                self.state = state

            def __composite_values__(self):
                return [self.street, self.state]

        class User(Base, ComparableEntity):
            __tablename__ = "user"
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            street = Column(String(50))
            state = Column(String(2))
            address = composite(AddressComposite, street, state)

        Base.metadata.create_all(testing.db)
        sess = fixture_session()
        sess.add(User(address=AddressComposite("123 anywhere street", "MD")))
        sess.commit()
        eq_(
            sess.query(User).all(),
            [User(address=AddressComposite("123 anywhere street", "MD"))],
        )

    def test_mapping_to_join(self):
        users = Table(
            "users", Base.metadata, Column("id", Integer, primary_key=True)
        )
        addresses = Table(
            "addresses",
            Base.metadata,
            Column("id", Integer, primary_key=True),
            Column("user_id", Integer, ForeignKey("users.id")),
        )
        usersaddresses = sa.join(
            users, addresses, users.c.id == addresses.c.user_id
        )

        class User(Base):
            __table__ = usersaddresses
            __table_args__ = {"primary_key": [users.c.id]}

            # need to use column_property for now
            user_id = column_property(users.c.id, addresses.c.user_id)
            address_id = addresses.c.id

        assert User.__mapper__.get_property("user_id").columns[0] is users.c.id
        assert (
            User.__mapper__.get_property("user_id").columns[1]
            is addresses.c.user_id
        )

    def test_synonym_inline(self):
        class User(Base, ComparableEntity):
            __tablename__ = "users"
            id = Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            )
            _name = Column("name", String(50))

            def _set_name(self, name):
                self._name = "SOMENAME " + name

            def _get_name(self):
                return self._name

            name = sa.orm.synonym(
                "_name", descriptor=property(_get_name, _set_name)
            )

        Base.metadata.create_all(testing.db)
        sess = fixture_session()
        u1 = User(name="someuser")
        eq_(u1.name, "SOMENAME someuser")
        sess.add(u1)
        sess.flush()
        eq_(
            sess.query(User).filter(User.name == "SOMENAME someuser").one(), u1
        )

    def test_synonym_no_descriptor(self):
        from sqlalchemy.orm.properties import ColumnProperty

        class CustomCompare(ColumnProperty.Comparator):
            __hash__ = None

            def __eq__(self, other):
                return self.__clause_element__() == other + " FOO"

        class User(Base, ComparableEntity):
            __tablename__ = "users"
            id = Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            )
            _name = Column("name", String(50))
            name = sa.orm.synonym("_name", comparator_factory=CustomCompare)

        Base.metadata.create_all(testing.db)
        sess = fixture_session()
        u1 = User(name="someuser FOO")
        sess.add(u1)
        sess.flush()
        eq_(sess.query(User).filter(User.name == "someuser").one(), u1)

    def test_synonym_added(self, require_metaclass):
        class User(Base, ComparableEntity):
            __tablename__ = "users"
            id = Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            )
            _name = Column("name", String(50))

            def _set_name(self, name):
                self._name = "SOMENAME " + name

            def _get_name(self):
                return self._name

            name = property(_get_name, _set_name)

        User.name = sa.orm.synonym("_name", descriptor=User.name)
        Base.metadata.create_all(testing.db)
        sess = fixture_session()
        u1 = User(name="someuser")
        eq_(u1.name, "SOMENAME someuser")
        sess.add(u1)
        sess.flush()
        eq_(
            sess.query(User).filter(User.name == "SOMENAME someuser").one(), u1
        )

    def test_reentrant_compile_via_foreignkey(self):
        class User(Base, ComparableEntity):
            __tablename__ = "users"
            id = Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column("name", String(50))
            addresses = relationship("Address", backref="user")

        class Address(Base, ComparableEntity):
            __tablename__ = "addresses"
            id = Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            )
            email = Column("email", String(50))
            user_id = Column("user_id", Integer, ForeignKey(User.id))

        # previous versions would force a re-entrant mapper compile via
        # the User.id inside the ForeignKey but this is no longer the
        # case

        sa.orm.configure_mappers()
        eq_(
            list(Address.user_id.property.columns[0].foreign_keys)[0].column,
            User.__table__.c.id,
        )
        Base.metadata.create_all(testing.db)
        u1 = User(
            name="u1", addresses=[Address(email="one"), Address(email="two")]
        )
        sess = fixture_session()
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        eq_(
            sess.query(User).all(),
            [
                User(
                    name="u1",
                    addresses=[Address(email="one"), Address(email="two")],
                )
            ],
        )

    def test_relationship_reference(self, require_metaclass):
        class Address(Base, ComparableEntity):
            __tablename__ = "addresses"
            id = Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            )
            email = Column("email", String(50))
            user_id = Column("user_id", Integer, ForeignKey("users.id"))

        class User(Base, ComparableEntity):
            __tablename__ = "users"
            id = Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column("name", String(50))
            addresses = relationship(
                "Address", backref="user", primaryjoin=id == Address.user_id
            )

        User.address_count = sa.orm.column_property(
            sa.select(sa.func.count(Address.id))
            .where(Address.user_id == User.id)
            .scalar_subquery()
        )
        Base.metadata.create_all(testing.db)
        u1 = User(
            name="u1", addresses=[Address(email="one"), Address(email="two")]
        )
        sess = fixture_session()
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        eq_(
            sess.query(User).all(),
            [
                User(
                    name="u1",
                    address_count=2,
                    addresses=[Address(email="one"), Address(email="two")],
                )
            ],
        )

    def test_pk_with_fk_init(self):
        class Bar(Base):
            __tablename__ = "bar"
            id = sa.Column(
                sa.Integer, sa.ForeignKey("foo.id"), primary_key=True
            )
            ex = sa.Column(sa.Integer, primary_key=True)

        class Foo(Base):
            __tablename__ = "foo"
            id = sa.Column(sa.Integer, primary_key=True)
            bars = sa.orm.relationship(Bar)

        assert Bar.__mapper__.primary_key[0] is Bar.__table__.c.id
        assert Bar.__mapper__.primary_key[1] is Bar.__table__.c.ex

    @testing.provide_metadata
    def test_with_explicit_autoloaded(self):
        meta = self.metadata
        t1 = Table(
            "t1",
            meta,
            Column("id", String(50), primary_key=True),
            Column("data", String(50)),
        )
        meta.create_all(testing.db)

        class MyObj(Base):
            __table__ = Table("t1", Base.metadata, autoload_with=testing.db)

        sess = fixture_session()
        m = MyObj(id="someid", data="somedata")
        sess.add(m)
        sess.flush()
        eq_(sess.execute(t1.select()).fetchall(), [("someid", "somedata")])

    def test_synonym_for(self):
        class User(Base, ComparableEntity):
            __tablename__ = "users"
            id = Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column("name", String(50))

            @synonym_for("name")
            @property
            def namesyn(self):
                return self.name

        Base.metadata.create_all(testing.db)
        sess = fixture_session()
        u1 = User(name="someuser")
        eq_(u1.name, "someuser")
        eq_(u1.namesyn, "someuser")
        sess.add(u1)
        sess.flush()
        rt = sess.query(User).filter(User.namesyn == "someuser").one()
        eq_(rt, u1)

    def test_duplicate_classes_in_base(self):
        class Test(Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)

        with expect_warnings(
            "This declarative base already contains a class with ",
        ):
            type(Base)(
                "Test",
                (Base,),
                dict(__tablename__="b", id=Column(Integer, primary_key=True)),
            )

    @testing.teardown_events(MapperEvents)
    @testing.teardown_events(InstrumentationEvents)
    def test_instrument_class_before_instrumentation(self):
        # test #3388

        canary = mock.Mock()

        @event.listens_for(Mapper, "instrument_class")
        def instrument_class(mp, cls):
            canary.instrument_class(mp, cls)

        @event.listens_for(object, "class_instrument")
        def class_instrument(cls):
            canary.class_instrument(cls)

        class Test(Base):
            __tablename__ = "test"
            id = Column(Integer, primary_key=True)

        eq_(
            canary.mock_calls,
            [
                mock.call.instrument_class(Test.__mapper__, Test),
                mock.call.class_instrument(Test),
            ],
        )

    def test_classes_can_override_new(self):
        class MyTable(Base):
            __tablename__ = "my_table"
            id = Column(Integer, primary_key=True)

            def __new__(cls, *args, **kwargs):
                return object.__new__(cls)

            def some_method(self):
                pass

            @staticmethod
            def some_static_method(self):
                pass

        mt = MyTable(id=5)
        eq_(mt.id, 5)


class NamedAttrOrderingTest(fixtures.TestBase):
    """test for #8705"""

    @testing.variation(
        "mapping_style",
        [
            "decl_base_fn",
            "decl_base_base",
            "classical_mapping",
        ],
    )
    def test_ordering_of_attrs_cols_named_or_unnamed(self, mapping_style):
        seen_names = {"noname"}

        is_declarative = (
            mapping_style.decl_base_fn or mapping_style.decl_base_base
        )

        def make_name():
            name = "noname"

            while name in seen_names:
                uppercase = random.randint(1, 3) == 1
                name = "".join(
                    random.choice("abcdefghijklmnopqrstuvxyz")
                    for i in range(random.randint(4, 10))
                )
                if uppercase:
                    name = random.choice("ABCDEFGHIJKLMNOP") + name
            seen_names.add(name)
            return name

        def make_column(assign_col_name):
            use_key = random.randint(1, 3) == 1
            use_name = random.randint(1, 3) == 1

            args = []
            kw = {}
            name = col_name = make_name()

            if use_name:
                use_different_name = random.randint(1, 3) != 3
                if use_different_name:
                    col_name = make_name()

                args.append(col_name)
            elif assign_col_name:
                args.append(col_name)

            if use_key:
                kw["key"] = name
                expected_c_name = name
            else:
                expected_c_name = col_name

            args.append(Integer)

            use_mapped_column = is_declarative and random.randint(1, 2) == 1

            if use_mapped_column:
                col = mapped_column(*args, **kw)
            else:
                col = Column(*args, **kw)

            use_explicit_property = (
                not use_mapped_column and random.randint(1, 6) == 1
            )
            if use_explicit_property:
                col_prop = column_property(col)
            else:
                col_prop = col

            return name, expected_c_name, col, col_prop

        assign_col_name = mapping_style.classical_mapping

        names = [
            make_column(assign_col_name) for i in range(random.randint(10, 15))
        ]
        len_names = len(names)

        pk_col = names[random.randint(0, len_names - 1)][2]
        if isinstance(pk_col, MappedColumn):
            pk_col.column.primary_key = True
        else:
            pk_col.primary_key = True

        names_only = [name for name, _, _, _ in names]
        col_names_only = [col_name for _, col_name, _, _ in names]
        cols_only = [col for _, _, col, _ in names]

        if is_declarative:
            if mapping_style.decl_base_fn:
                Base = declarative_base()
            elif mapping_style.decl_base_base:

                class Base(DeclarativeBase):
                    pass

            else:
                assert False

            clsdict = {
                "__tablename__": "new_table",
            }
            clsdict.update({name: colprop for name, _, _, colprop in names})

            new_cls = type("NewCls", (Base,), clsdict)

        elif mapping_style.classical_mapping:

            class new_cls:
                pass

            reg = registry()
            t = Table("new_table", reg.metadata, *cols_only)

            reg.map_imperatively(
                new_cls,
                t,
                properties={
                    key: colprop
                    for key, col_name, col, colprop in names
                    if col_name != key
                },
            )
        else:
            mapping_style.fail()

        eq_(new_cls.__table__.c.keys(), col_names_only)
        eq_(new_cls.__mapper__.attrs.keys(), names_only)
        eq_(list(new_cls._sa_class_manager.keys()), names_only)
        eq_([k for k in new_cls.__dict__ if not k.startswith("_")], names_only)

        stmt = select(new_cls)
        eq_(stmt.selected_columns.keys(), col_names_only)

    @testing.variation(
        "mapping_style",
        [
            "decl_base_fn",
            "decl_base_base",
            "decl_base_no_meta",
            "map_declaratively",
            "decorator",
            "mapped_as_dataclass",
        ],
    )
    def test_no_imperative_with_declarative_table(self, mapping_style):
        if mapping_style.decl_base_fn:
            Base = declarative_base()

            class DecModel(Base):
                __tablename__ = "foo"
                id: Mapped[int] = mapped_column(primary_key=True)
                data: Mapped[str]

        elif mapping_style.decl_base_base:

            class Base(DeclarativeBase):
                pass

            class DecModel(Base):
                __tablename__ = "foo"
                id: Mapped[int] = mapped_column(primary_key=True)
                data: Mapped[str]

        elif mapping_style.decl_base_no_meta:

            class Base(DeclarativeBaseNoMeta):
                pass

            class DecModel(Base):
                __tablename__ = "foo"
                id: Mapped[int] = mapped_column(primary_key=True)
                data: Mapped[str]

        elif mapping_style.decorator:
            r = registry()

            @r.mapped
            class DecModel:
                __tablename__ = "foo"
                id: Mapped[int] = mapped_column(primary_key=True)
                data: Mapped[str]

        elif mapping_style.map_declaratively:

            class DecModel:
                __tablename__ = "foo"
                id: Mapped[int] = mapped_column(primary_key=True)
                data: Mapped[str]

            registry().map_declaratively(DecModel)
        elif mapping_style.decorator:
            r = registry()

            @r.mapped
            class DecModel:
                __tablename__ = "foo"
                id: Mapped[int] = mapped_column(primary_key=True)
                data: Mapped[str]

        elif mapping_style.mapped_as_dataclass:
            r = registry()

            @r.mapped_as_dataclass
            class DecModel:
                __tablename__ = "foo"
                id: Mapped[int] = mapped_column(primary_key=True)
                data: Mapped[str]

        else:
            assert False

        class ImpModel:
            id: int
            data: str

        with expect_raises_message(
            exc.ArgumentError,
            "FROM expression, such as a Table or alias.. object expected "
            "for argument 'local_table'; got",
        ):
            registry().map_imperatively(ImpModel, DecModel)
