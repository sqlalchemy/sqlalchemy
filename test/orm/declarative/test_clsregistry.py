from contextlib import nullcontext

from sqlalchemy import BLANK_SCHEMA
from sqlalchemy import Column
from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.orm import class_mapper
from sqlalchemy.orm import clsregistry
from sqlalchemy.orm import configure_mappers
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import registry
from sqlalchemy.orm import relationship
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import assertions
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import mock
from sqlalchemy.testing.assertions import expect_warnings
from sqlalchemy.testing.schema import Table
from sqlalchemy.testing.util import gc_collect


class MockClass:
    def __init__(self, base, name):
        self._sa_class_manager = mock.Mock(registry=base)
        tokens = name.split(".")
        self.__module__ = ".".join(tokens[0:-1])
        self.name = self.__name__ = tokens[-1]
        self.metadata = MetaData()


class MockProp:
    parent = "some_parent"


class ClsRegistryTest(fixtures.TestBase):
    __requires__ = ("predictable_gc",)

    def test_same_module_same_name(self):
        base = registry()
        f1 = MockClass(base, "foo.bar.Foo")
        f2 = MockClass(base, "foo.bar.Foo")
        clsregistry._add_class("Foo", f1, base._class_registry)
        gc_collect()

        with expect_warnings(
            "This declarative base already contains a class with the "
            "same class name and module name as foo.bar.Foo, and "
            "will be replaced in the string-lookup table."
        ):
            clsregistry._add_class(
                "Foo",
                f2,
                base._class_registry,
            )

    def test_resolve(self):
        base = registry()
        f1 = MockClass(base, "foo.bar.Foo")
        f2 = MockClass(base, "foo.alt.Foo")
        clsregistry._add_class("Foo", f1, base._class_registry)
        clsregistry._add_class("Foo", f2, base._class_registry)
        name_resolver, resolver = clsregistry._resolver(f1, MockProp())

        gc_collect()

        is_(resolver("foo.bar.Foo")(), f1)
        is_(resolver("foo.alt.Foo")(), f2)

        is_(name_resolver("foo.bar.Foo")(), f1)
        is_(name_resolver("foo.alt.Foo")(), f2)

    def test_fragment_resolve(self):
        base = registry()
        f1 = MockClass(base, "foo.bar.Foo")
        f2 = MockClass(base, "foo.alt.Foo")
        f3 = MockClass(base, "bat.alt.Hoho")
        clsregistry._add_class("Foo", f1, base._class_registry)
        clsregistry._add_class("Foo", f2, base._class_registry)
        clsregistry._add_class("HoHo", f3, base._class_registry)
        name_resolver, resolver = clsregistry._resolver(f1, MockProp())

        gc_collect()

        is_(resolver("bar.Foo")(), f1)
        is_(resolver("alt.Foo")(), f2)

        is_(name_resolver("bar.Foo")(), f1)
        is_(name_resolver("alt.Foo")(), f2)

    def test_fragment_ambiguous(self):
        base = registry()
        f1 = MockClass(base, "foo.bar.Foo")
        f2 = MockClass(base, "foo.alt.Foo")
        f3 = MockClass(base, "bat.alt.Foo")
        clsregistry._add_class("Foo", f1, base._class_registry)
        clsregistry._add_class("Foo", f2, base._class_registry)
        clsregistry._add_class("Foo", f3, base._class_registry)
        name_resolver, resolver = clsregistry._resolver(f1, MockProp())

        gc_collect()

        assert_raises_message(
            exc.InvalidRequestError,
            'Multiple classes found for path "alt.Foo" in the registry '
            "of this declarative base. Please use a fully "
            "module-qualified path.",
            resolver("alt.Foo"),
        )

        assert_raises_message(
            exc.InvalidRequestError,
            'Multiple classes found for path "alt.Foo" in the registry '
            "of this declarative base. Please use a fully "
            "module-qualified path.",
            name_resolver("alt.Foo"),
        )

    @testing.combinations(
        ("NonExistentFoo",),
        ("nonexistent.Foo",),
        ("existent.nonexistent.Foo",),
        ("existent.NonExistentFoo",),
        ("nonexistent.NonExistentFoo",),
        ("existent.existent.NonExistentFoo",),
        argnames="name",
    )
    def test_name_resolution_failures(self, name, registry):
        Base = registry.generate_base()

        f1 = MockClass(registry, "existent.Foo")
        f2 = MockClass(registry, "existent.existent.Foo")
        clsregistry._add_class("Foo", f1, registry._class_registry)
        clsregistry._add_class("Foo", f2, registry._class_registry)

        class MyClass(Base):
            __tablename__ = "my_table"
            id = Column(Integer, primary_key=True)
            foo = relationship(name)

        with expect_raises_message(
            exc.InvalidRequestError,
            r"When initializing mapper .*MyClass.*, expression '%s' "
            r"failed to locate a name" % (name,),
        ):
            registry.configure()

    @testing.variation("has_default_schema", [True, False])
    def test_name_resolution_failure_error_message(self, has_default_schema):
        """test #13291"""
        if has_default_schema:
            metadata = MetaData(schema="fooschema")
        else:
            metadata = MetaData()

        reg = registry(metadata=metadata)
        Base = reg.generate_base()

        class MyClass(Base):
            __tablename__ = "my_table"
            id = Column(Integer, primary_key=True)

        MyClass.foo = relationship(
            "Foo",
            secondary="nonexistent_table",
            backref="my_classes",
        )

        with expect_raises_message(
            exc.InvalidRequestError,
            r"When initializing mapper .*MyClass.*, expression "
            r"'nonexistent_table' failed to locate a name "
            r"\('nonexistent_table'\)",
        ):
            reg.configure()

        reg.dispose()

    def test_no_fns_in_name_resolve(self):
        base = registry()
        f1 = MockClass(base, "foo.bar.Foo")
        f2 = MockClass(base, "foo.alt.Foo")
        clsregistry._add_class("Foo", f1, base._class_registry)
        clsregistry._add_class("Foo", f2, base._class_registry)
        name_resolver, resolver = clsregistry._resolver(f1, MockProp())

        gc_collect()

        import sqlalchemy

        is_(
            resolver("__import__('sqlalchemy.util').util.EMPTY_SET")(),
            sqlalchemy.util.EMPTY_SET,
        )

        assert_raises_message(
            exc.InvalidRequestError,
            r"When initializing mapper some_parent, expression "
            r"\"__import__\('sqlalchemy.util'\).util.EMPTY_SET\" "
            "failed to locate a name",
            name_resolver("__import__('sqlalchemy.util').util.EMPTY_SET"),
        )

    def test_resolve_dupe_by_name(self):
        base = registry()
        f1 = MockClass(base, "foo.bar.Foo")
        f2 = MockClass(base, "foo.alt.Foo")
        clsregistry._add_class("Foo", f1, base._class_registry)
        clsregistry._add_class("Foo", f2, base._class_registry)

        gc_collect()

        name_resolver, resolver = clsregistry._resolver(f1, MockProp())
        resolver = resolver("Foo")
        assert_raises_message(
            exc.InvalidRequestError,
            'Multiple classes found for path "Foo" in the '
            "registry of this declarative base. Please use a "
            "fully module-qualified path.",
            resolver,
        )

        resolver = name_resolver("Foo")
        assert_raises_message(
            exc.InvalidRequestError,
            'Multiple classes found for path "Foo" in the '
            "registry of this declarative base. Please use a "
            "fully module-qualified path.",
            resolver,
        )

    def test_dupe_classes_back_to_one(self):
        base = registry()
        f1 = MockClass(base, "foo.bar.Foo")
        f2 = MockClass(base, "foo.alt.Foo")
        clsregistry._add_class("Foo", f1, base._class_registry)
        clsregistry._add_class("Foo", f2, base._class_registry)

        del f2
        gc_collect()

        # registry restores itself to just the one class
        name_resolver, resolver = clsregistry._resolver(f1, MockProp())
        f_resolver = resolver("Foo")
        is_(f_resolver(), f1)

        f_resolver = name_resolver("Foo")
        is_(f_resolver(), f1)

    def test_dupe_classes_cleanout(self):
        # force this to maintain isolation between tests
        clsregistry._registries.clear()

        base = registry()

        for i in range(3):
            f1 = MockClass(base, "foo.bar.Foo")
            f2 = MockClass(base, "foo.alt.Foo")
            clsregistry._add_class("Foo", f1, base._class_registry)
            clsregistry._add_class("Foo", f2, base._class_registry)

            eq_(len(clsregistry._registries), 11)

            del f1
            del f2
            gc_collect()

            eq_(len(clsregistry._registries), 0)

    def test_dupe_classes_name_race(self):
        """test the race condition that the class was garbage "
        "collected while being resolved from a dupe class."""
        base = registry()
        f1 = MockClass(base, "foo.bar.Foo")
        f2 = MockClass(base, "foo.alt.Foo")
        clsregistry._add_class("Foo", f1, base._class_registry)
        clsregistry._add_class("Foo", f2, base._class_registry)

        dupe_reg = base._class_registry["Foo"]
        dupe_reg.contents = [lambda: None]
        name_resolver, resolver = clsregistry._resolver(f1, MockProp())
        f_resolver = resolver("Foo")
        assert_raises_message(
            exc.InvalidRequestError,
            r"When initializing mapper some_parent, expression "
            r"'Foo' failed to locate a name \('Foo'\).",
            f_resolver,
        )

        f_resolver = name_resolver("Foo")
        assert_raises_message(
            exc.InvalidRequestError,
            r"When initializing mapper some_parent, expression "
            r"'Foo' failed to locate a name \('Foo'\).",
            f_resolver,
        )

    def test_module_reg_cleanout_race(self):
        """test the race condition that a class was gc'ed as we tried
        to look it up by module name."""

        base = registry()
        f1 = MockClass(base, "foo.bar.Foo")
        clsregistry._add_class("Foo", f1, base._class_registry)
        reg = base._class_registry["_sa_module_registry"]

        mod_entry = reg["foo"]["bar"]
        name_resolver, resolver = clsregistry._resolver(f1, MockProp())
        f_resolver = resolver("foo")
        del mod_entry.contents["Foo"]
        assert_raises_message(
            NameError,
            "Module 'bar' has no mapped classes registered "
            "under the name 'Foo'",
            lambda: f_resolver().bar.Foo,
        )

        f_resolver = name_resolver("foo")
        assert_raises_message(
            NameError,
            "Module 'bar' has no mapped classes registered "
            "under the name 'Foo'",
            lambda: f_resolver().bar.Foo,
        )

    def test_module_reg_no_class(self):
        base = registry()
        f1 = MockClass(base, "foo.bar.Foo")
        clsregistry._add_class("Foo", f1, base._class_registry)
        reg = base._class_registry["_sa_module_registry"]
        mod_entry = reg["foo"]["bar"]  # noqa
        name_resolver, resolver = clsregistry._resolver(f1, MockProp())
        f_resolver = resolver("foo")
        assert_raises_message(
            NameError,
            "Module 'bar' has no mapped classes registered "
            "under the name 'Bat'",
            lambda: f_resolver().bar.Bat,
        )

        f_resolver = name_resolver("foo")
        assert_raises_message(
            NameError,
            "Module 'bar' has no mapped classes registered "
            "under the name 'Bat'",
            lambda: f_resolver().bar.Bat,
        )

    def test_module_reg_cleanout_two_sub(self):
        base = registry()
        f1 = MockClass(base, "foo.bar.Foo")
        clsregistry._add_class("Foo", f1, base._class_registry)
        reg = base._class_registry["_sa_module_registry"]

        f2 = MockClass(base, "foo.alt.Bar")
        clsregistry._add_class("Bar", f2, base._class_registry)
        assert reg["foo"]["bar"]
        del f1
        gc_collect()
        assert "bar" not in reg["foo"]
        assert "alt" in reg["foo"]

        del f2
        gc_collect()
        assert "foo" not in reg.contents

    def test_module_reg_cleanout_sub_to_base(self):
        base = registry()
        f3 = MockClass(base, "bat.bar.Hoho")
        clsregistry._add_class("Hoho", f3, base._class_registry)
        reg = base._class_registry["_sa_module_registry"]

        assert reg["bat"]["bar"]
        del f3
        gc_collect()
        assert "bat" not in reg

    def test_module_reg_cleanout_cls_to_base(self):
        base = registry()
        f4 = MockClass(base, "single.Blat")
        clsregistry._add_class("Blat", f4, base._class_registry)
        reg = base._class_registry["_sa_module_registry"]
        assert reg["single"]
        del f4
        gc_collect()
        assert "single" not in reg

    @testing.variation(
        "resolve_type", ["secondary_only", "primaryjoin_secondaryjoin"]
    )
    @testing.variation("owner_schema", ["inherits", "blank", "different"])
    @testing.variation(
        "secondary_schema",
        ["inherits", "blank", "inherits_qualified"],
    )
    def test_string_dependency_resolution_default_schema(
        self, resolve_type, owner_schema, secondary_schema
    ):
        """test #13291"""
        metadata = MetaData(schema="fooschema")
        Base = declarative_base(metadata=metadata)

        if owner_schema.inherits:
            owner_schema_kw: dict = {}
        elif owner_schema.blank:
            owner_schema_kw = {"schema": BLANK_SCHEMA}
        elif owner_schema.different:
            owner_schema_kw = {"schema": "otherschema"}
        else:
            owner_schema.fail()

        if secondary_schema.inherits or secondary_schema.inherits_qualified:
            sec_kw: dict = {}
        elif secondary_schema.blank:
            sec_kw = {"schema": BLANK_SCHEMA}
        else:
            secondary_schema.fail()

        if secondary_schema.inherits_qualified:
            secondary_ref = "fooschema.user_to_prop"
        else:
            secondary_ref = "user_to_prop"

        class User(Base):
            __tablename__ = "users"
            __table_args__ = owner_schema_kw
            id = Column(Integer, primary_key=True)
            name = Column(String(50))

        class Prop(Base):
            __tablename__ = "props"
            __table_args__ = owner_schema_kw
            id = Column(Integer, primary_key=True)
            name = Column(String(50))

        user_to_prop = Table(
            "user_to_prop",
            Base.metadata,
            Column(
                "user_id",
                Integer,
                ForeignKey(User.__table__.c.id),
            ),
            Column(
                "prop_id",
                Integer,
                ForeignKey(Prop.__table__.c.id),
            ),
            **sec_kw,
        )

        if resolve_type.secondary_only:
            User.props = relationship(
                "Prop",
                secondary=secondary_ref,
                backref="users",
            )
        elif resolve_type.primaryjoin_secondaryjoin:
            User.props = relationship(
                "Prop",
                secondary=user_to_prop,
                primaryjoin=("User.id==user_to_prop.c.user_id"),
                secondaryjoin=("user_to_prop.c.prop_id==Prop.id"),
                backref="users",
            )
        else:
            resolve_type.fail()

        expects_warning = (
            secondary_schema.blank and not secondary_schema.inherits_qualified
        )

        if expects_warning:
            ctx = assertions.expect_deprecated(
                r"The string 'user_to_prop' was resolved"
            )
        else:
            ctx = nullcontext()

        with ctx:
            configure_mappers()

        assert (
            class_mapper(User).get_property("props").secondary is user_to_prop
        )

    @testing.variation(
        "resolve_type", ["secondary_only", "primaryjoin_secondaryjoin"]
    )
    @testing.variation(
        "mapping_style", ["declarative_base", "registry_mapped"]
    )
    def test_string_dependency_resolution_cls_metadata(
        self, resolve_type, mapping_style
    ):
        """test #8068"""
        alt_metadata = MetaData()

        a_to_b = Table(
            "a_to_b",
            alt_metadata,
            Column("a_id", Integer, ForeignKey("a.id")),
            Column("b_id", Integer, ForeignKey("b.id")),
        )

        if mapping_style.declarative_base:
            Base = declarative_base()

            class AltMetadataMixin(Base):
                __abstract__ = True
                metadata = alt_metadata

            if resolve_type.secondary_only:

                class A(AltMetadataMixin):
                    __tablename__ = "a"
                    id = Column(Integer, primary_key=True)
                    bs = relationship("B", secondary="a_to_b", backref="as_")

            elif resolve_type.primaryjoin_secondaryjoin:

                class A(AltMetadataMixin):
                    __tablename__ = "a"
                    id = Column(Integer, primary_key=True)
                    bs = relationship(
                        "B",
                        secondary=a_to_b,
                        primaryjoin="A.id==a_to_b.c.a_id",
                        secondaryjoin="a_to_b.c.b_id==B.id",
                        backref="as_",
                    )

            else:
                resolve_type.fail()

            class B(AltMetadataMixin):
                __tablename__ = "b"
                id = Column(Integer, primary_key=True)
                a_id = Column(Integer, ForeignKey("a.id"))

        elif mapping_style.registry_mapped:
            reg = registry()

            class AltMetadataMixin:
                metadata = alt_metadata

            if resolve_type.secondary_only:

                @reg.mapped
                class A(AltMetadataMixin):
                    __tablename__ = "a"
                    id = Column(Integer, primary_key=True)
                    bs = relationship("B", secondary="a_to_b", backref="as_")

            elif resolve_type.primaryjoin_secondaryjoin:

                @reg.mapped
                class A(AltMetadataMixin):
                    __tablename__ = "a"
                    id = Column(Integer, primary_key=True)
                    bs = relationship(
                        "B",
                        secondary=a_to_b,
                        primaryjoin="A.id==a_to_b.c.a_id",
                        secondaryjoin="a_to_b.c.b_id==B.id",
                        backref="as_",
                    )

            else:
                resolve_type.fail()

            @reg.mapped
            class B(AltMetadataMixin):
                __tablename__ = "b"
                id = Column(Integer, primary_key=True)
                a_id = Column(Integer, ForeignKey("a.id"))

        else:
            mapping_style.fail()

        configure_mappers()

        assert class_mapper(A).get_property("bs").secondary is a_to_b

    @testing.variation(
        "mapping_style", ["declarative_base", "registry_mapped"]
    )
    @testing.variation("relationship_style", ["backref", "back_populates"])
    def test_backref_named_metadata(self, mapping_style, relationship_style):
        """test that a backref or back_populates which overwrites
        cls.metadata with a relationship collection does not break
        _metadata_for_cls() when used in combination with string-based
        relationship references on the same class.

        Reproduces the pattern used by OpenStack Nova where a child model
        uses backref='metadata' on a relationship to a parent class,
        thereby shadowing the declarative 'metadata' attribute with an
        InstrumentedList. When the parent class also has a string-based
        secondary table reference, the class registry resolver calls
        _metadata_for_cls() after the backref has already overwritten
        cls.metadata.

        See https://bugs.launchpad.net/nova/+bug/2154165,
        https://github.com/sqlalchemy/sqlalchemy/discussions/8619
        """

        use_backref = bool(relationship_style.backref)

        if mapping_style.declarative_base:
            Base = declarative_base()

            class InstanceMetadata(Base):
                __tablename__ = "instance_metadata"
                id = Column(Integer, primary_key=True)
                instance_id = Column(Integer, ForeignKey("instance.id"))
                if use_backref:
                    instance = relationship("Instance", backref="metadata")
                else:
                    instance = relationship(
                        "Instance",
                        back_populates="metadata",
                    )

            class Tag(Base):
                __tablename__ = "tag"
                id = Column(Integer, primary_key=True)

            instance_tag = Table(
                "instance_tag",
                Base.metadata,
                Column(
                    "instance_id",
                    Integer,
                    ForeignKey("instance.id"),
                ),
                Column(
                    "tag_id",
                    Integer,
                    ForeignKey("tag.id"),
                ),
            )

            ctx = (
                expect_warnings(
                    r"Attribute name 'metadata' should be left reserved"
                )
                if not use_backref
                else nullcontext()
            )
            with ctx:

                class Instance(Base):
                    __tablename__ = "instance"
                    id = Column(Integer, primary_key=True)
                    tags = relationship("Tag", secondary="instance_tag")
                    if not use_backref:
                        metadata = relationship(
                            "InstanceMetadata",
                            back_populates="instance",
                        )

        elif mapping_style.registry_mapped:
            reg = registry()

            @reg.mapped
            class InstanceMetadata:
                __tablename__ = "instance_metadata"
                id = Column(Integer, primary_key=True)
                instance_id = Column(Integer, ForeignKey("instance.id"))
                if use_backref:
                    instance = relationship("Instance", backref="metadata")
                else:
                    instance = relationship(
                        "Instance",
                        back_populates="metadata",
                    )

            @reg.mapped
            class Tag:
                __tablename__ = "tag"
                id = Column(Integer, primary_key=True)

            instance_tag = Table(
                "instance_tag",
                reg.metadata,
                Column(
                    "instance_id",
                    Integer,
                    ForeignKey("instance.id"),
                ),
                Column(
                    "tag_id",
                    Integer,
                    ForeignKey("tag.id"),
                ),
            )

            ctx = (
                expect_warnings(
                    r"Attribute name 'metadata' should be left reserved"
                )
                if not use_backref
                else nullcontext()
            )
            with ctx:

                @reg.mapped
                class Instance:
                    __tablename__ = "instance"
                    id = Column(Integer, primary_key=True)
                    tags = relationship("Tag", secondary="instance_tag")
                    if not use_backref:
                        metadata = relationship(
                            "InstanceMetadata",
                            back_populates="instance",
                        )

        else:
            mapping_style.fail()

        configure_mappers()

        eq_(
            class_mapper(Instance).get_property("metadata").mapper.class_,
            InstanceMetadata,
        )
        is_(
            class_mapper(Instance).get_property("tags").secondary,
            instance_tag,
        )
