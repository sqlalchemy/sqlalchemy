import dataclasses
import datetime
from decimal import Decimal
from typing import ClassVar
from typing import Dict
from typing import Generic
from typing import List
from typing import Optional
from typing import Set
from typing import Type
from typing import TypeVar
from typing import Union
import uuid

from sqlalchemy import BIGINT
from sqlalchemy import BigInteger
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import exc as sa_exc
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Identity
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import Numeric
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import types
from sqlalchemy import VARCHAR
from sqlalchemy.exc import ArgumentError
from sqlalchemy.orm import as_declarative
from sqlalchemy.orm import composite
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import declared_attr
from sqlalchemy.orm import deferred
from sqlalchemy.orm import DynamicMapped
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.orm import undefer
from sqlalchemy.orm import WriteOnlyMapped
from sqlalchemy.orm.collections import attribute_keyed_dict
from sqlalchemy.orm.collections import KeyFuncDict
from sqlalchemy.schema import CreateTable
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_false
from sqlalchemy.testing import is_not
from sqlalchemy.testing import is_true
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.util import compat
from sqlalchemy.util.typing import Annotated


class DeclarativeBaseTest(fixtures.TestBase):
    def test_class_getitem_as_declarative(self):
        T = TypeVar("T", bound="CommonBase")  # noqa

        class CommonBase(Generic[T]):
            @classmethod
            def boring(cls: Type[T]) -> Type[T]:
                return cls

            @classmethod
            def more_boring(cls: Type[T]) -> int:
                return 27

        @as_declarative()
        class Base(CommonBase[T]):
            foo = 1

        class Tab(Base["Tab"]):
            __tablename__ = "foo"

            # old mypy plugin use
            a: int = Column(Integer, primary_key=True)

        eq_(Tab.foo, 1)
        is_(Tab.__table__, inspect(Tab).local_table)
        eq_(Tab.boring(), Tab)
        eq_(Tab.more_boring(), 27)

        with expect_raises(AttributeError):
            Tab.non_existent


class MappedColumnTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "default"

    @testing.combinations(
        "default", "insert_default", argnames="use_paramname"
    )
    @testing.combinations(True, False, argnames="use_none")
    def test_col_defaults(self, use_paramname, use_none, decl_base):
        class Foo(decl_base):
            __tablename__ = "foo"

            id: Mapped[int] = mapped_column(primary_key=True)

            data: Mapped[int] = mapped_column(
                **{use_paramname: None if use_none else 5}
            )

        if use_none:
            assert not Foo.__table__.c.data.default
        else:
            eq_(Foo.__table__.c.data.default.arg, 5)

    def test_legacy_declarative_base(self):
        typ = VARCHAR(50)
        Base = declarative_base(type_annotation_map={str: typ})

        class MyClass(Base):
            __tablename__ = "mytable"

            id: Mapped[int] = mapped_column(primary_key=True)
            data: Mapped[str]
            x: Mapped[int]

        is_(MyClass.__table__.c.data.type, typ)
        is_true(MyClass.__table__.c.id.primary_key)

    @testing.combinations(
        (BIGINT(),),
        (BIGINT,),
        (Integer().with_variant(BIGINT, "default")),
        (Integer().with_variant(BIGINT(), "default")),
        (BIGINT().with_variant(String(), "some_other_dialect")),
    )
    def test_type_map_varieties(self, typ):

        Base = declarative_base(type_annotation_map={int: typ})

        class MyClass(Base):
            __tablename__ = "mytable"

            id: Mapped[int] = mapped_column(primary_key=True)
            x: Mapped[int]
            y: Mapped[int] = mapped_column()
            z: Mapped[int] = mapped_column(typ)

        self.assert_compile(
            CreateTable(MyClass.__table__),
            "CREATE TABLE mytable (id BIGINT NOT NULL, "
            "x BIGINT NOT NULL, y BIGINT NOT NULL, z BIGINT NOT NULL, "
            "PRIMARY KEY (id))",
        )

    def test_required_no_arg(self, decl_base):
        with expect_raises_message(
            sa_exc.ArgumentError,
            r"Python typing annotation is required for attribute "
            r'"A.data" when primary '
            r'argument\(s\) for "MappedColumn" construct are None or '
            r"not present",
        ):

            class A(decl_base):
                __tablename__ = "a"

                id: Mapped[int] = mapped_column(primary_key=True)
                data = mapped_column()

    @testing.combinations("key", "name", "both", argnames="case")
    @testing.combinations(True, False, argnames="deferred")
    @testing.combinations(True, False, argnames="use_add_property")
    def test_separate_name(self, decl_base, case, deferred, use_add_property):
        if case == "key":
            args = {"key": "data_"}
        elif case == "name":
            args = {"name": "data_"}
        else:
            args = {"name": "data_", "key": "data_"}

        if deferred:
            args["deferred"] = True

        class A(decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True)

            if not use_add_property:
                data: Mapped[str] = mapped_column(**args)

        if use_add_property:
            args["type_"] = String()
            A.data = mapped_column(**args)

        assert not hasattr(A, "data_")
        is_(A.data.property.expression, A.__table__.c.data_)
        eq_(A.__table__.c.data_.key, "data_")

    def test_construct_rhs(self, decl_base):
        class User(decl_base):
            __tablename__ = "users"

            id = mapped_column("id", Integer, primary_key=True)
            name = mapped_column(String(50))

        self.assert_compile(
            select(User), "SELECT users.id, users.name FROM users"
        )
        eq_(User.__mapper__.primary_key, (User.__table__.c.id,))

    def test_construct_lhs(self, decl_base):
        class User(decl_base):
            __tablename__ = "users"

            id: Mapped[int] = mapped_column(primary_key=True)
            name: Mapped[str] = mapped_column()
            data: Mapped[Optional[str]] = mapped_column()

        self.assert_compile(
            select(User), "SELECT users.id, users.name, users.data FROM users"
        )
        eq_(User.__mapper__.primary_key, (User.__table__.c.id,))
        is_false(User.__table__.c.id.nullable)
        is_false(User.__table__.c.name.nullable)
        is_true(User.__table__.c.data.nullable)

    def test_construct_lhs_omit_mapped_column(self, decl_base):
        class User(decl_base):
            __tablename__ = "users"

            id: Mapped[int] = mapped_column(primary_key=True)
            name: Mapped[str]
            data: Mapped[Optional[str]]
            x: Mapped[int]
            y: Mapped[int]
            created_at: Mapped[datetime.datetime]

        self.assert_compile(
            select(User),
            "SELECT users.id, users.name, users.data, users.x, "
            "users.y, users.created_at FROM users",
        )
        eq_(User.__mapper__.primary_key, (User.__table__.c.id,))
        is_false(User.__table__.c.id.nullable)
        is_false(User.__table__.c.name.nullable)
        is_true(User.__table__.c.data.nullable)
        assert isinstance(User.__table__.c.created_at.type, DateTime)

    def test_i_have_a_classvar_on_my_class(self, decl_base):
        class MyClass(decl_base):
            __tablename__ = "mytable"

            id: Mapped[int] = mapped_column(primary_key=True)
            data: Mapped[str] = mapped_column(default="some default")

            status: ClassVar[int]

        m1 = MyClass(id=1, data=5)
        assert "status" not in inspect(m1).mapper.attrs

    def test_i_have_plain_or_column_attrs_on_my_class_w_values(
        self, decl_base
    ):
        class MyClass(decl_base):
            __tablename__ = "mytable"

            id: Mapped[int] = mapped_column(primary_key=True)
            data: Mapped[str] = mapped_column(default="some default")

            old_column: str = Column(String)

            # we assume this is intentional
            status: int = 5

        # it's mapped too
        assert "old_column" in inspect(MyClass).attrs

    def test_i_have_plain_attrs_on_my_class_disallowed(self, decl_base):
        with expect_raises_message(
            sa_exc.ArgumentError,
            r'Type annotation for "MyClass.status" should use the syntax '
            r'"Mapped\[int\]".  To leave the attribute unmapped, use '
            r"ClassVar\[int\], assign a value to the attribute, or "
            r"set __allow_unmapped__ = True on the class.",
        ):

            class MyClass(decl_base):
                __tablename__ = "mytable"

                id: Mapped[int] = mapped_column(primary_key=True)
                data: Mapped[str] = mapped_column(default="some default")

                # we assume this is not intentional.  because I made the
                # same mistake myself :)
                status: int

    def test_i_have_plain_attrs_on_my_class_allowed(self, decl_base):
        class MyClass(decl_base):
            __tablename__ = "mytable"
            __allow_unmapped__ = True

            id: Mapped[int] = mapped_column(primary_key=True)
            data: Mapped[str] = mapped_column(default="some default")

            status: int

    def test_allow_unmapped_on_mixin(self, decl_base):
        class AllowsUnmapped:
            __allow_unmapped__ = True

        class MyClass(AllowsUnmapped, decl_base):
            __tablename__ = "mytable"

            id: Mapped[int] = mapped_column(primary_key=True)
            data: Mapped[str] = mapped_column(default="some default")

            status: int

    def test_allow_unmapped_on_base(self):
        class Base(DeclarativeBase):
            __allow_unmapped__ = True

        class MyClass(Base):
            __tablename__ = "mytable"

            id: Mapped[int] = mapped_column(primary_key=True)
            data: Mapped[str] = mapped_column(default="some default")

            status: int

    def test_column_default(self, decl_base):
        class MyClass(decl_base):
            __tablename__ = "mytable"

            id: Mapped[int] = mapped_column(primary_key=True)
            data: Mapped[str] = mapped_column(default="some default")

        mc = MyClass()
        assert "data" not in mc.__dict__

        eq_(MyClass.__table__.c.data.default.arg, "some default")

    def test_anno_w_fixed_table(self, decl_base):
        users = Table(
            "users",
            decl_base.metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50), nullable=False),
            Column("data", String(50)),
            Column("x", Integer),
            Column("y", Integer),
            Column("created_at", DateTime),
        )

        class User(decl_base):
            __table__ = users

            id: Mapped[int]
            name: Mapped[str]
            data: Mapped[Optional[str]]
            x: Mapped[int]
            y: Mapped[int]
            created_at: Mapped[datetime.datetime]

        self.assert_compile(
            select(User),
            "SELECT users.id, users.name, users.data, users.x, "
            "users.y, users.created_at FROM users",
        )
        eq_(User.__mapper__.primary_key, (User.__table__.c.id,))
        is_false(User.__table__.c.id.nullable)
        is_false(User.__table__.c.name.nullable)
        is_true(User.__table__.c.data.nullable)
        assert isinstance(User.__table__.c.created_at.type, DateTime)

    def test_construct_lhs_type_missing(self, decl_base):
        class MyClass:
            pass

        with expect_raises_message(
            sa_exc.ArgumentError,
            "Could not locate SQLAlchemy Core type for Python type: .*MyClass",
        ):

            class User(decl_base):
                __tablename__ = "users"

                id: Mapped[int] = mapped_column(primary_key=True)
                data: Mapped[MyClass] = mapped_column()

    def test_construct_rhs_type_override_lhs(self, decl_base):
        class Element(decl_base):
            __tablename__ = "element"

            id: Mapped[int] = mapped_column(BIGINT, primary_key=True)

        class User(decl_base):
            __tablename__ = "users"

            id: Mapped[int] = mapped_column(BIGINT, primary_key=True)
            other_id: Mapped[int] = mapped_column(ForeignKey("element.id"))
            data: Mapped[int] = mapped_column()

        # exact class test
        is_(User.__table__.c.id.type.__class__, BIGINT)
        is_(User.__table__.c.other_id.type.__class__, BIGINT)
        is_(User.__table__.c.data.type.__class__, Integer)

    @testing.combinations(True, False, argnames="include_rhs_type")
    def test_construct_nullability_overrides(
        self, decl_base, include_rhs_type
    ):

        if include_rhs_type:
            args = (String,)
        else:
            args = ()

        class User(decl_base):
            __tablename__ = "users"

            id: Mapped[int] = mapped_column(primary_key=True)

            lnnl_rndf: Mapped[str] = mapped_column(*args)
            lnnl_rnnl: Mapped[str] = mapped_column(*args, nullable=False)
            lnnl_rnl: Mapped[str] = mapped_column(*args, nullable=True)
            lnl_rndf: Mapped[Optional[str]] = mapped_column(*args)
            lnl_rnnl: Mapped[Optional[str]] = mapped_column(
                *args, nullable=False
            )
            lnl_rnl: Mapped[Optional[str]] = mapped_column(
                *args, nullable=True
            )

        is_false(User.__table__.c.lnnl_rndf.nullable)
        is_false(User.__table__.c.lnnl_rnnl.nullable)
        is_true(User.__table__.c.lnnl_rnl.nullable)

        is_true(User.__table__.c.lnl_rndf.nullable)
        is_false(User.__table__.c.lnl_rnnl.nullable)
        is_true(User.__table__.c.lnl_rnl.nullable)

        # test #8410
        is_false(User.__table__.c.lnnl_rndf._copy().nullable)
        is_false(User.__table__.c.lnnl_rnnl._copy().nullable)
        is_true(User.__table__.c.lnnl_rnl._copy().nullable)
        is_true(User.__table__.c.lnl_rndf._copy().nullable)
        is_false(User.__table__.c.lnl_rnnl._copy().nullable)
        is_true(User.__table__.c.lnl_rnl._copy().nullable)

    def test_fwd_refs(self, decl_base: Type[DeclarativeBase]):
        class MyClass(decl_base):
            __tablename__ = "my_table"

            id: Mapped["int"] = mapped_column(primary_key=True)
            data_one: Mapped["str"]

    def test_pep593_types_as_typemap_keys(
        self, decl_base: Type[DeclarativeBase]
    ):
        """neat!!!"""

        str50 = Annotated[str, 50]
        str30 = Annotated[str, 30]
        opt_str50 = Optional[str50]
        opt_str30 = Optional[str30]

        decl_base.registry.update_type_annotation_map(
            {str50: String(50), str30: String(30)}
        )

        class MyClass(decl_base):
            __tablename__ = "my_table"

            id: Mapped[str50] = mapped_column(primary_key=True)
            data_one: Mapped[str30]
            data_two: Mapped[opt_str30]
            data_three: Mapped[str50]
            data_four: Mapped[opt_str50]
            data_five: Mapped[str]
            data_six: Mapped[Optional[str]]

        eq_(MyClass.__table__.c.data_one.type.length, 30)
        is_false(MyClass.__table__.c.data_one.nullable)
        eq_(MyClass.__table__.c.data_two.type.length, 30)
        is_true(MyClass.__table__.c.data_two.nullable)
        eq_(MyClass.__table__.c.data_three.type.length, 50)

    def test_extract_base_type_from_pep593(
        self, decl_base: Type[DeclarativeBase]
    ):
        """base type is extracted from an Annotated structure if not otherwise
        in the type lookup dictionary"""

        class MyClass(decl_base):
            __tablename__ = "my_table"

            id: Mapped[Annotated[Annotated[int, "q"], "t"]] = mapped_column(
                primary_key=True
            )

        is_(MyClass.__table__.c.id.type._type_affinity, Integer)

    def test_extract_sqla_from_pep593_not_yet(
        self, decl_base: Type[DeclarativeBase]
    ):
        """https://twitter.com/zzzeek/status/1536693554621341697"""

        class SomeRelated(decl_base):
            __tablename__: ClassVar[Optional[str]] = "some_related"
            id: Mapped["int"] = mapped_column(primary_key=True)

        with expect_raises_message(
            NotImplementedError,
            r"Use of the \<class 'sqlalchemy.orm."
            r"relationships.Relationship'\> construct inside of an Annotated "
            r"object is not yet supported.",
        ):

            class MyClass(decl_base):
                __tablename__ = "my_table"

                id: Mapped["int"] = mapped_column(primary_key=True)
                data_one: Mapped[Annotated["SomeRelated", relationship()]]

    def test_extract_sqla_from_pep593_plain(
        self, decl_base: Type[DeclarativeBase]
    ):
        """extraction of mapped_column() from the Annotated type

        https://twitter.com/zzzeek/status/1536693554621341697"""

        intpk = Annotated[int, mapped_column(primary_key=True)]

        strnone = Annotated[str, mapped_column()]  # str -> NOT NULL
        str30nullable = Annotated[
            str, mapped_column(String(30), nullable=True)  # nullable -> NULL
        ]
        opt_strnone = Optional[strnone]  # Optional[str] -> NULL
        opt_str30 = Optional[str30nullable]  # nullable -> NULL

        class MyClass(decl_base):
            __tablename__ = "my_table"

            id: Mapped[intpk]

            data_one: Mapped[strnone]
            data_two: Mapped[str30nullable]
            data_three: Mapped[opt_strnone]
            data_four: Mapped[opt_str30]

        class MyOtherClass(decl_base):
            __tablename__ = "my_other_table"

            id: Mapped[intpk]

            data_one: Mapped[strnone]
            data_two: Mapped[str30nullable]
            data_three: Mapped[opt_strnone]
            data_four: Mapped[opt_str30]

        for cls in MyClass, MyOtherClass:
            table = cls.__table__
            assert table is not None

            is_(table.c.id.primary_key, True)
            is_(table.c.id.table, table)

            eq_(table.c.data_one.type.length, None)
            eq_(table.c.data_two.type.length, 30)
            eq_(table.c.data_three.type.length, None)

            is_false(table.c.data_one.nullable)
            is_true(table.c.data_two.nullable)
            is_true(table.c.data_three.nullable)
            is_true(table.c.data_four.nullable)

    def test_extract_sqla_from_pep593_mixin(
        self, decl_base: Type[DeclarativeBase]
    ):
        """extraction of mapped_column() from the Annotated type

        https://twitter.com/zzzeek/status/1536693554621341697"""

        intpk = Annotated[int, mapped_column(primary_key=True)]

        strnone = Annotated[str, mapped_column()]  # str -> NOT NULL
        str30nullable = Annotated[
            str, mapped_column(String(30), nullable=True)  # nullable -> NULL
        ]
        opt_strnone = Optional[strnone]  # Optional[str] -> NULL
        opt_str30 = Optional[str30nullable]  # nullable -> NULL

        class HasPk:
            id: Mapped[intpk]

            data_one: Mapped[strnone]
            data_two: Mapped[str30nullable]

        class MyClass(HasPk, decl_base):
            __tablename__ = "my_table"

            data_three: Mapped[opt_strnone]
            data_four: Mapped[opt_str30]

        table = MyClass.__table__
        assert table is not None

        is_(table.c.id.primary_key, True)
        is_(table.c.id.table, table)

        eq_(table.c.data_one.type.length, None)
        eq_(table.c.data_two.type.length, 30)
        eq_(table.c.data_three.type.length, None)

        is_false(table.c.data_one.nullable)
        is_true(table.c.data_two.nullable)
        is_true(table.c.data_three.nullable)
        is_true(table.c.data_four.nullable)

    def test_extract_fk_col_from_pep593(
        self, decl_base: Type[DeclarativeBase]
    ):
        intpk = Annotated[int, mapped_column(primary_key=True)]
        element_ref = Annotated[int, mapped_column(ForeignKey("element.id"))]

        class Element(decl_base):
            __tablename__ = "element"

            id: Mapped[intpk]

        class RefElementOne(decl_base):
            __tablename__ = "refone"

            id: Mapped[intpk]
            other_id: Mapped[element_ref]

        class RefElementTwo(decl_base):
            __tablename__ = "reftwo"

            id: Mapped[intpk]
            some_id: Mapped[element_ref]

        assert Element.__table__ is not None
        assert RefElementOne.__table__ is not None
        assert RefElementTwo.__table__ is not None

        is_true(
            RefElementOne.__table__.c.other_id.references(
                Element.__table__.c.id
            )
        )
        is_true(
            RefElementTwo.__table__.c.some_id.references(
                Element.__table__.c.id
            )
        )

    @testing.combinations(
        ("default", lambda ctx: 10),
        ("default", func.foo()),
        ("onupdate", lambda ctx: 10),
        ("onupdate", func.foo()),
        ("server_onupdate", func.foo()),
        ("server_default", func.foo()),
        ("server_default", Identity()),
        ("nullable", True),
        ("nullable", False),
        ("type", BigInteger()),
        ("index", True),
        ("unique", True),
        argnames="paramname, value",
    )
    @testing.combinations(True, False, argnames="optional")
    @testing.combinations(True, False, argnames="include_existing_col")
    def test_combine_args_from_pep593(
        self,
        decl_base: Type[DeclarativeBase],
        paramname,
        value,
        include_existing_col,
        optional,
    ):
        intpk = Annotated[int, mapped_column(primary_key=True)]

        args = []
        params = {}
        if paramname == "type":
            args.append(value)
        else:
            params[paramname] = value

        element_ref = Annotated[int, mapped_column(*args, **params)]
        if optional:
            element_ref = Optional[element_ref]

        class Element(decl_base):
            __tablename__ = "element"

            id: Mapped[intpk]

            if include_existing_col:
                data: Mapped[element_ref] = mapped_column()
            else:
                data: Mapped[element_ref]

        data_col = Element.__table__.c.data
        if paramname in (
            "default",
            "onupdate",
            "server_default",
            "server_onupdate",
        ):
            default = getattr(data_col, paramname)
            if default.is_server_default and default.has_argument:
                is_(default.arg, value)
            is_(default.column, data_col)
        elif paramname == "type":
            assert type(data_col.type) is type(value)
        else:
            is_(getattr(data_col, paramname), value)

            # test _copy() for #8410
            is_(getattr(data_col._copy(), paramname), value)

        sd = data_col.server_default
        if sd is not None and isinstance(sd, Identity):
            if paramname == "nullable" and value:
                is_(data_col.nullable, True)
            else:
                is_(data_col.nullable, False)
        elif paramname != "nullable":
            is_(data_col.nullable, optional)
        else:
            is_(data_col.nullable, value)

    @testing.combinations(True, False, argnames="specify_identity")
    @testing.combinations(True, False, None, argnames="specify_nullable")
    @testing.combinations(True, False, argnames="optional")
    @testing.combinations(True, False, argnames="include_existing_col")
    def test_combine_args_from_pep593_identity_nullable(
        self,
        decl_base: Type[DeclarativeBase],
        specify_identity,
        specify_nullable,
        optional,
        include_existing_col,
    ):
        intpk = Annotated[int, mapped_column(primary_key=True)]

        if specify_identity:
            args = [Identity()]
        else:
            args = []

        if specify_nullable is not None:
            params = {"nullable": specify_nullable}
        else:
            params = {}

        element_ref = Annotated[int, mapped_column(*args, **params)]
        if optional:
            element_ref = Optional[element_ref]

        class Element(decl_base):
            __tablename__ = "element"

            id: Mapped[intpk]

            if include_existing_col:
                data: Mapped[element_ref] = mapped_column()
            else:
                data: Mapped[element_ref]

        # test identity + _copy() for #8410
        for col in (
            Element.__table__.c.data,
            Element.__table__.c.data._copy(),
        ):
            if specify_nullable is True:
                is_(col.nullable, True)
            elif specify_identity:
                is_(col.nullable, False)
            elif specify_nullable is False:
                is_(col.nullable, False)
            elif not optional:
                is_(col.nullable, False)
            else:
                is_(col.nullable, True)

    @testing.combinations(
        ("default", lambda ctx: 10, lambda ctx: 15),
        ("default", func.foo(), func.bar()),
        ("onupdate", lambda ctx: 10, lambda ctx: 15),
        ("onupdate", func.foo(), func.bar()),
        ("server_onupdate", func.foo(), func.bar()),
        ("server_default", func.foo(), func.bar()),
        ("nullable", True, False),
        ("nullable", False, True),
        ("type", BigInteger(), Numeric()),
        argnames="paramname, value, override_value",
    )
    def test_dont_combine_args_from_pep593(
        self,
        decl_base: Type[DeclarativeBase],
        paramname,
        value,
        override_value,
    ):
        intpk = Annotated[int, mapped_column(primary_key=True)]

        args = []
        params = {}
        override_args = []
        override_params = {}
        if paramname == "type":
            args.append(value)
            override_args.append(override_value)
        else:
            params[paramname] = value
            if paramname == "default":
                override_params["insert_default"] = override_value
            else:
                override_params[paramname] = override_value

        element_ref = Annotated[int, mapped_column(*args, **params)]

        class Element(decl_base):
            __tablename__ = "element"

            id: Mapped[intpk]

            data: Mapped[element_ref] = mapped_column(
                *override_args, **override_params
            )

        if paramname in (
            "default",
            "onupdate",
            "server_default",
            "server_onupdate",
        ):
            default = getattr(Element.__table__.c.data, paramname)
            is_(default.arg, override_value)
            is_(default.column, Element.__table__.c.data)
        elif paramname == "type":
            assert type(Element.__table__.c.data.type) is type(override_value)
        else:
            is_(getattr(Element.__table__.c.data, paramname), override_value)

    def test_unions(self):
        our_type = Numeric(10, 2)

        class Base(DeclarativeBase):
            type_annotation_map = {Union[float, Decimal]: our_type}

        class User(Base):
            __tablename__ = "users"
            __table__: Table

            id: Mapped[int] = mapped_column(primary_key=True)

            data: Mapped[Union[float, Decimal]] = mapped_column()
            reverse_data: Mapped[Union[Decimal, float]] = mapped_column()

            optional_data: Mapped[
                Optional[Union[float, Decimal]]
            ] = mapped_column()

            # use Optional directly
            reverse_optional_data: Mapped[
                Optional[Union[Decimal, float]]
            ] = mapped_column()

            # use Union with None, same as Optional but presents differently
            # (Optional object with __origin__ Union vs. Union)
            reverse_u_optional_data: Mapped[
                Union[Decimal, float, None]
            ] = mapped_column()

            float_data: Mapped[float] = mapped_column()
            decimal_data: Mapped[Decimal] = mapped_column()

            if compat.py310:
                pep604_data: Mapped[float | Decimal] = mapped_column()
                pep604_reverse: Mapped[Decimal | float] = mapped_column()
                pep604_optional: Mapped[
                    Decimal | float | None
                ] = mapped_column()
                pep604_data_fwd: Mapped["float | Decimal"] = mapped_column()
                pep604_reverse_fwd: Mapped["Decimal | float"] = mapped_column()
                pep604_optional_fwd: Mapped[
                    "Decimal | float | None"
                ] = mapped_column()

        is_(User.__table__.c.data.type, our_type)
        is_false(User.__table__.c.data.nullable)
        is_(User.__table__.c.reverse_data.type, our_type)
        is_(User.__table__.c.optional_data.type, our_type)
        is_true(User.__table__.c.optional_data.nullable)

        is_(User.__table__.c.reverse_optional_data.type, our_type)
        is_(User.__table__.c.reverse_u_optional_data.type, our_type)
        is_true(User.__table__.c.reverse_optional_data.nullable)
        is_true(User.__table__.c.reverse_u_optional_data.nullable)

        is_(User.__table__.c.float_data.type, our_type)
        is_(User.__table__.c.decimal_data.type, our_type)

        if compat.py310:
            for suffix in ("", "_fwd"):
                data_col = User.__table__.c[f"pep604_data{suffix}"]
                reverse_col = User.__table__.c[f"pep604_reverse{suffix}"]
                optional_col = User.__table__.c[f"pep604_optional{suffix}"]
                is_(data_col.type, our_type)
                is_false(data_col.nullable)
                is_(reverse_col.type, our_type)
                is_false(reverse_col.nullable)
                is_(optional_col.type, our_type)
                is_true(optional_col.nullable)

    def test_missing_mapped_lhs(self, decl_base):
        with expect_raises_message(
            ArgumentError,
            r'Type annotation for "User.name" should use the '
            r'syntax "Mapped\[str\]" or "MappedColumn\[str\]"',
        ):

            class User(decl_base):
                __tablename__ = "users"

                id: Mapped[int] = mapped_column(primary_key=True)
                name: str = mapped_column()  # type: ignore

    def test_construct_lhs_separate_name(self, decl_base):
        class User(decl_base):
            __tablename__ = "users"

            id: Mapped[int] = mapped_column(primary_key=True)
            name: Mapped[str] = mapped_column()
            data: Mapped[Optional[str]] = mapped_column("the_data")

        self.assert_compile(
            select(User.data), "SELECT users.the_data FROM users"
        )
        is_true(User.__table__.c.the_data.nullable)

    def test_construct_works_in_expr(self, decl_base):
        class User(decl_base):
            __tablename__ = "users"

            id: Mapped[int] = mapped_column(primary_key=True)

        class Address(decl_base):
            __tablename__ = "addresses"

            id: Mapped[int] = mapped_column(primary_key=True)
            user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))

            user = relationship(User, primaryjoin=user_id == User.id)

        self.assert_compile(
            select(Address.user_id, User.id).join(Address.user),
            "SELECT addresses.user_id, users.id FROM addresses "
            "JOIN users ON addresses.user_id = users.id",
        )

    def test_construct_works_as_polymorphic_on(self, decl_base):
        class User(decl_base):
            __tablename__ = "users"

            id: Mapped[int] = mapped_column(primary_key=True)
            type: Mapped[str] = mapped_column()

            __mapper_args__ = {"polymorphic_on": type}

        decl_base.registry.configure()
        is_(User.__table__.c.type, User.__mapper__.polymorphic_on)

    def test_construct_works_as_version_id_col(self, decl_base):
        class User(decl_base):
            __tablename__ = "users"

            id: Mapped[int] = mapped_column(primary_key=True)
            version_id: Mapped[int] = mapped_column()

            __mapper_args__ = {"version_id_col": version_id}

        decl_base.registry.configure()
        is_(User.__table__.c.version_id, User.__mapper__.version_id_col)

    def test_construct_works_in_deferred(self, decl_base):
        class User(decl_base):
            __tablename__ = "users"

            id: Mapped[int] = mapped_column(primary_key=True)
            data: Mapped[str] = deferred(mapped_column())

        self.assert_compile(select(User), "SELECT users.id FROM users")
        self.assert_compile(
            select(User).options(undefer(User.data)),
            "SELECT users.data, users.id FROM users",
        )

    def test_deferred_kw(self, decl_base):
        class User(decl_base):
            __tablename__ = "users"

            id: Mapped[int] = mapped_column(primary_key=True)
            data: Mapped[str] = mapped_column(deferred=True)

        self.assert_compile(select(User), "SELECT users.id FROM users")
        self.assert_compile(
            select(User).options(undefer(User.data)),
            "SELECT users.data, users.id FROM users",
        )

    @testing.combinations(
        (str, types.String),
        (Decimal, types.Numeric),
        (float, types.Float),
        (datetime.datetime, types.DateTime),
        (uuid.UUID, types.Uuid),
        argnames="pytype,sqltype",
    )
    def test_datatype_lookups(self, decl_base, pytype, sqltype):
        class MyClass(decl_base):
            __tablename__ = "mytable"
            id: Mapped[int] = mapped_column(primary_key=True)

            data: Mapped[pytype]

        assert isinstance(MyClass.__table__.c.data.type, sqltype)


class MixinTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "default"

    def test_mapped_column_omit_fn(self, decl_base):
        class MixinOne:
            name: Mapped[str]
            x: Mapped[int]
            y: Mapped[int] = mapped_column()

        class A(MixinOne, decl_base):
            __tablename__ = "a"
            id: Mapped[int] = mapped_column(primary_key=True)

        eq_(A.__table__.c.keys(), ["id", "name", "x", "y"])

        self.assert_compile(select(A), "SELECT a.id, a.name, a.x, a.y FROM a")

    def test_mapped_column_omit_fn_fixed_table(self, decl_base):
        class MixinOne:
            name: Mapped[str]
            x: Mapped[int]
            y: Mapped[int]

        a = Table(
            "a",
            decl_base.metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50), nullable=False),
            Column("data", String(50)),
            Column("x", Integer),
            Column("y", Integer),
        )

        class A(MixinOne, decl_base):
            __table__ = a
            id: Mapped[int]

        self.assert_compile(
            select(A), "SELECT a.id, a.name, a.data, a.x, a.y FROM a"
        )

    def test_mc_duplication_plain(self, decl_base):
        class MixinOne:
            name: Mapped[str] = mapped_column()

        class A(MixinOne, decl_base):
            __tablename__ = "a"
            id: Mapped[int] = mapped_column(primary_key=True)

        class B(MixinOne, decl_base):
            __tablename__ = "b"
            id: Mapped[int] = mapped_column(primary_key=True)

        is_not(A.__table__.c.name, B.__table__.c.name)

    def test_mc_duplication_declared_attr(self, decl_base):
        class MixinOne:
            @declared_attr
            def name(cls) -> Mapped[str]:
                return mapped_column()

        class A(MixinOne, decl_base):
            __tablename__ = "a"
            id: Mapped[int] = mapped_column(primary_key=True)

        class B(MixinOne, decl_base):
            __tablename__ = "b"
            id: Mapped[int] = mapped_column(primary_key=True)

        is_not(A.__table__.c.name, B.__table__.c.name)

    def test_relationship_requires_declared_attr(self, decl_base):
        class Related(decl_base):
            __tablename__ = "related"

            id: Mapped[int] = mapped_column(primary_key=True)

        class HasRelated:
            related_id: Mapped[int] = mapped_column(ForeignKey(Related.id))

            related: Mapped[Related] = relationship()

        with expect_raises_message(
            sa_exc.InvalidRequestError,
            r"Mapper properties \(i.e. deferred,column_property\(\), "
            r"relationship\(\), etc.\) must be declared",
        ):

            class A(HasRelated, decl_base):
                __tablename__ = "a"
                id: Mapped[int] = mapped_column(primary_key=True)

    def test_relationship_duplication_declared_attr(self, decl_base):
        class Related(decl_base):
            __tablename__ = "related"

            id: Mapped[int] = mapped_column(primary_key=True)

        class HasRelated:
            related_id: Mapped[int] = mapped_column(ForeignKey(Related.id))

            @declared_attr
            def related(cls) -> Mapped[Related]:
                return relationship()

        class A(HasRelated, decl_base):
            __tablename__ = "a"
            id: Mapped[int] = mapped_column(primary_key=True)

        class B(HasRelated, decl_base):
            __tablename__ = "b"
            id: Mapped[int] = mapped_column(primary_key=True)

        self.assert_compile(
            select(A).join(A.related),
            "SELECT a.id, a.related_id FROM a "
            "JOIN related ON related.id = a.related_id",
        )
        self.assert_compile(
            select(B).join(B.related),
            "SELECT b.id, b.related_id FROM b "
            "JOIN related ON related.id = b.related_id",
        )


class RelationshipLHSTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "default"

    @testing.fixture
    def decl_base(self):
        class Base(DeclarativeBase):
            pass

        yield Base
        Base.registry.dispose()

    def test_no_typing_in_rhs(self, decl_base):
        class A(decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True)
            bs = relationship("List['B']")

        class B(decl_base):
            __tablename__ = "b"
            id: Mapped[int] = mapped_column(Integer, primary_key=True)
            a_id: Mapped[int] = mapped_column(ForeignKey("a.id"))

        with expect_raises_message(
            sa_exc.InvalidRequestError,
            r"When initializing mapper Mapper\[A\(a\)\], expression "
            r'"relationship\(\"List\[\'B\'\]\"\)\" seems to be using a '
            r"generic class as the argument to relationship\(\); please "
            r"state the generic argument using an annotation, e.g. "
            r'"bs: Mapped\[List\[\'B\'\]\] = relationship\(\)"',
        ):

            decl_base.registry.configure()

    def test_required_no_arg(self, decl_base):
        with expect_raises_message(
            sa_exc.ArgumentError,
            r"Python typing annotation is required for attribute "
            r'"A.bs" when primary '
            r'argument\(s\) for "Relationship" construct are None or '
            r"not present",
        ):

            class A(decl_base):
                __tablename__ = "a"

                id: Mapped[int] = mapped_column(primary_key=True)
                bs = relationship()

    def test_rudimentary_dataclasses_support(self, registry):
        @registry.mapped
        @dataclasses.dataclass
        class A:
            __tablename__ = "a"
            __sa_dataclass_metadata_key__ = "sa"

            id: Mapped[int] = mapped_column(primary_key=True)
            bs: List["B"] = dataclasses.field(  # noqa: F821
                default_factory=list, metadata={"sa": relationship()}
            )

        @registry.mapped
        @dataclasses.dataclass
        class B:
            __tablename__ = "b"
            id: Mapped[int] = mapped_column(primary_key=True)
            a_id = mapped_column(ForeignKey("a.id"))

        self.assert_compile(
            select(A).join(A.bs), "SELECT a.id FROM a JOIN b ON a.id = b.a_id"
        )

    def test_basic_bidirectional(self, decl_base):
        class A(decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True)
            data: Mapped[str] = mapped_column()
            bs: Mapped[List["B"]] = relationship(  # noqa: F821
                back_populates="a"
            )

        class B(decl_base):
            __tablename__ = "b"
            id: Mapped[int] = mapped_column(Integer, primary_key=True)
            a_id: Mapped[int] = mapped_column(ForeignKey("a.id"))

            a: Mapped["A"] = relationship(
                back_populates="bs", primaryjoin=a_id == A.id
            )

        a1 = A(data="data")
        b1 = B()
        a1.bs.append(b1)
        is_(a1, b1.a)

    def test_wrong_annotation_type_one(self, decl_base):

        with expect_raises_message(
            sa_exc.ArgumentError,
            r"Type annotation for \"A.data\" should use the "
            r"syntax \"Mapped\['B'\]\" or \"Relationship\['B'\]\"",
        ):

            class A(decl_base):
                __tablename__ = "a"

                id: Mapped[int] = mapped_column(primary_key=True)
                data: "B" = relationship()  # type: ignore  # noqa

    def test_wrong_annotation_type_two(self, decl_base):

        with expect_raises_message(
            sa_exc.ArgumentError,
            r"Type annotation for \"A.data\" should use the "
            r"syntax \"Mapped\[B\]\" or \"Relationship\[B\]\"",
        ):

            class B(decl_base):
                __tablename__ = "b"

                id: Mapped[int] = mapped_column(primary_key=True)

            class A(decl_base):
                __tablename__ = "a"

                id: Mapped[int] = mapped_column(primary_key=True)
                data: B = relationship()  # type: ignore  # noqa

    def test_wrong_annotation_type_three(self, decl_base):

        with expect_raises_message(
            sa_exc.ArgumentError,
            r"Type annotation for \"A.data\" should use the "
            r"syntax \"Mapped\['List\[B\]'\]\" or "
            r"\"Relationship\['List\[B\]'\]\"",
        ):

            class B(decl_base):
                __tablename__ = "b"

                id: Mapped[int] = mapped_column(primary_key=True)

            class A(decl_base):
                __tablename__ = "a"

                id: Mapped[int] = mapped_column(primary_key=True)
                data: "List[B]" = relationship()  # type: ignore  # noqa

    def test_collection_class_uselist(self, decl_base):
        class A(decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True)
            data: Mapped[str] = mapped_column()
            bs_list: Mapped[List["B"]] = relationship(  # noqa: F821
                viewonly=True
            )
            bs_set: Mapped[Set["B"]] = relationship(  # noqa: F821
                viewonly=True
            )
            bs_list_warg: Mapped[List["B"]] = relationship(  # noqa: F821
                "B", viewonly=True
            )
            bs_set_warg: Mapped[Set["B"]] = relationship(  # noqa: F821
                "B", viewonly=True
            )

            # note this is string annotation
            b_one_to_one: Mapped["B"] = relationship(  # noqa: F821
                viewonly=True
            )

            b_one_to_one_warg: Mapped["B"] = relationship(  # noqa: F821
                "B", viewonly=True
            )

        class B(decl_base):
            __tablename__ = "b"
            id: Mapped[int] = mapped_column(Integer, primary_key=True)
            a_id: Mapped[int] = mapped_column(ForeignKey("a.id"))

            a: Mapped["A"] = relationship(viewonly=True)
            a_warg: Mapped["A"] = relationship("A", viewonly=True)

        is_(A.__mapper__.attrs["bs_list"].collection_class, list)
        is_(A.__mapper__.attrs["bs_set"].collection_class, set)
        is_(A.__mapper__.attrs["bs_list_warg"].collection_class, list)
        is_(A.__mapper__.attrs["bs_set_warg"].collection_class, set)
        is_true(A.__mapper__.attrs["bs_list"].uselist)
        is_true(A.__mapper__.attrs["bs_set"].uselist)
        is_true(A.__mapper__.attrs["bs_list_warg"].uselist)
        is_true(A.__mapper__.attrs["bs_set_warg"].uselist)

        is_false(A.__mapper__.attrs["b_one_to_one"].uselist)
        is_false(A.__mapper__.attrs["b_one_to_one_warg"].uselist)

        is_false(B.__mapper__.attrs["a"].uselist)
        is_false(B.__mapper__.attrs["a_warg"].uselist)

    def test_one_to_one_example(self, decl_base: Type[DeclarativeBase]):
        """test example in the relationship docs will derive uselist=False
        correctly"""

        class Parent(decl_base):
            __tablename__ = "parent"

            id: Mapped[int] = mapped_column(primary_key=True)
            child: Mapped["Child"] = relationship(  # noqa: F821
                back_populates="parent"
            )

        class Child(decl_base):
            __tablename__ = "child"

            id: Mapped[int] = mapped_column(primary_key=True)
            parent_id: Mapped[int] = mapped_column(ForeignKey("parent.id"))
            parent: Mapped["Parent"] = relationship(back_populates="child")

        c1 = Child()
        p1 = Parent(child=c1)
        is_(p1.child, c1)
        is_(c1.parent, p1)

    def test_collection_class_dict_no_collection(self, decl_base):
        class A(decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True)
            data: Mapped[str] = mapped_column()
            bs: Mapped[Dict[str, "B"]] = relationship()  # noqa: F821

        class B(decl_base):
            __tablename__ = "b"
            id: Mapped[int] = mapped_column(Integer, primary_key=True)
            a_id: Mapped[int] = mapped_column(ForeignKey("a.id"))
            name: Mapped[str] = mapped_column()

        # this is the old collections message.  it's not great, but at the
        # moment I like that this is what's raised
        with expect_raises_message(
            sa_exc.ArgumentError,
            "Type InstrumentedDict must elect an appender",
        ):
            decl_base.registry.configure()

    def test_collection_class_dict_attr_mapped_collection(self, decl_base):
        class A(decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True)
            data: Mapped[str] = mapped_column()

            bs: Mapped[KeyFuncDict[str, "B"]] = relationship(  # noqa: F821
                collection_class=attribute_keyed_dict("name")
            )

        class B(decl_base):
            __tablename__ = "b"
            id: Mapped[int] = mapped_column(Integer, primary_key=True)
            a_id: Mapped[int] = mapped_column(ForeignKey("a.id"))
            name: Mapped[str] = mapped_column()

        decl_base.registry.configure()

        a1 = A()
        b1 = B(name="foo")

        # collection appender on MappedCollection
        a1.bs.set(b1)

        is_(a1.bs["foo"], b1)


class CompositeTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "default"

    @testing.fixture
    def dataclass_point_fixture(self, decl_base):
        @dataclasses.dataclass
        class Point:
            x: int
            y: int

        class Edge(decl_base):
            __tablename__ = "edge"
            id: Mapped[int] = mapped_column(primary_key=True)
            graph_id: Mapped[int] = mapped_column(ForeignKey("graph.id"))

            start: Mapped[Point] = composite(
                Point, mapped_column("x1"), mapped_column("y1")
            )

            end: Mapped[Point] = composite(
                Point, mapped_column("x2"), mapped_column("y2")
            )

        class Graph(decl_base):
            __tablename__ = "graph"
            id: Mapped[int] = mapped_column(primary_key=True)

            edges: Mapped[List[Edge]] = relationship()

        decl_base.metadata.create_all(testing.db)
        return Point, Graph, Edge

    def test_composite_setup(self, dataclass_point_fixture):
        Point, Graph, Edge = dataclass_point_fixture

        with fixture_session() as sess:
            sess.add(
                Graph(
                    edges=[
                        Edge(start=Point(1, 2), end=Point(3, 4)),
                        Edge(start=Point(7, 8), end=Point(5, 6)),
                    ]
                )
            )
            sess.commit()

        self.assert_compile(
            select(Edge),
            "SELECT edge.id, edge.graph_id, edge.x1, edge.y1, "
            "edge.x2, edge.y2 FROM edge",
        )

        with fixture_session() as sess:
            g1 = sess.scalar(select(Graph))

            # round trip!
            eq_(g1.edges[0].end, Point(3, 4))

    def test_named_setup(self, decl_base: Type[DeclarativeBase]):
        @dataclasses.dataclass
        class Address:
            street: str
            state: str
            zip_: str

        class User(decl_base):
            __tablename__ = "user"

            id: Mapped[int] = mapped_column(primary_key=True)
            name: Mapped[str] = mapped_column()

            address: Mapped[Address] = composite(
                Address, mapped_column(), mapped_column(), mapped_column("zip")
            )

        decl_base.metadata.create_all(testing.db)

        with fixture_session() as sess:
            sess.add(
                User(
                    name="user 1",
                    address=Address("123 anywhere street", "NY", "12345"),
                )
            )
            sess.commit()

        with fixture_session() as sess:
            u1 = sess.scalar(select(User))

            # round trip!
            eq_(u1.address, Address("123 anywhere street", "NY", "12345"))

    def test_no_fwd_ref_annotated_setup(self, decl_base):
        @dataclasses.dataclass
        class Address:
            street: str
            state: str
            zip_: str

        with expect_raises_message(
            ArgumentError,
            r"Can't use forward ref ForwardRef\('Address'\) "
            r"for composite class argument",
        ):

            class User(decl_base):
                __tablename__ = "user"

                id: Mapped[int] = mapped_column(primary_key=True)
                name: Mapped[str] = mapped_column()

                address: Mapped["Address"] = composite(
                    mapped_column(), mapped_column(), mapped_column("zip")
                )

    def test_fwd_ref_plus_no_mapped(self, decl_base):
        @dataclasses.dataclass
        class Address:
            street: str
            state: str
            zip_: str

        with expect_raises_message(
            ArgumentError,
            r"Type annotation for \"User.address\" should use the syntax "
            r"\"Mapped\['Address'\]\"",
        ):

            class User(decl_base):
                __tablename__ = "user"

                id: Mapped[int] = mapped_column(primary_key=True)
                name: Mapped[str] = mapped_column()

                address: "Address" = composite(  # type: ignore
                    mapped_column(), mapped_column(), mapped_column("zip")
                )

    def test_extract_from_pep593(self, decl_base):
        @dataclasses.dataclass
        class Address:
            street: str
            state: str
            zip_: str

        class User(decl_base):
            __tablename__ = "user"

            id: Mapped[int] = mapped_column(primary_key=True)
            name: Mapped[str] = mapped_column()

            address: Mapped[Annotated[Address, "foo"]] = composite(
                mapped_column(), mapped_column(), mapped_column("zip")
            )

        self.assert_compile(
            select(User),
            'SELECT "user".id, "user".name, "user".street, '
            '"user".state, "user".zip FROM "user"',
            dialect="default",
        )

    def test_cls_not_composite_compliant(self, decl_base):
        class Address:
            def __init__(self, street: int, state: str, zip_: str):
                pass

            street: str
            state: str
            zip_: str

        with expect_raises_message(
            ArgumentError,
            r"Composite class column arguments must be "
            r"named unless a dataclass is used",
        ):

            class User(decl_base):
                __tablename__ = "user"

                id: Mapped[int] = mapped_column(primary_key=True)
                name: Mapped[str] = mapped_column()

                address: Mapped[Address] = composite(
                    mapped_column(), mapped_column(), mapped_column("zip")
                )

    def test_fwd_ref_ok_explicit_cls(self, decl_base):
        @dataclasses.dataclass
        class Address:
            street: str
            state: str
            zip_: str

        class User(decl_base):
            __tablename__ = "user"

            id: Mapped[int] = mapped_column(primary_key=True)
            name: Mapped[str] = mapped_column()

            address: Mapped["Address"] = composite(
                Address, mapped_column(), mapped_column(), mapped_column("zip")
            )

        self.assert_compile(
            select(User),
            'SELECT "user".id, "user".name, "user".street, '
            '"user".state, "user".zip FROM "user"',
        )

    def test_name_cols_by_str(self, decl_base):
        @dataclasses.dataclass
        class Address:
            street: str
            state: str
            zip_: str

        class User(decl_base):
            __tablename__ = "user"

            id: Mapped[int] = mapped_column(primary_key=True)
            name: Mapped[str]
            street: Mapped[str]
            state: Mapped[str]

            # TODO: this needs to be improved, we should be able to say:
            # zip_: Mapped[str] = mapped_column("zip")
            # and it should assign to "zip_" for the attribute. not working

            zip_: Mapped[str] = mapped_column(name="zip", key="zip_")

            address: Mapped["Address"] = composite(
                Address, "street", "state", "zip_"
            )

        eq_(
            User.__mapper__.attrs["address"].props,
            [
                User.__mapper__.attrs["street"],
                User.__mapper__.attrs["state"],
                User.__mapper__.attrs["zip_"],
            ],
        )
        self.assert_compile(
            select(User),
            'SELECT "user".id, "user".name, "user".street, '
            '"user".state, "user".zip FROM "user"',
        )

    def test_cls_annotated_setup(self, decl_base):
        @dataclasses.dataclass
        class Address:
            street: str
            state: str
            zip_: str

        class User(decl_base):
            __tablename__ = "user"

            id: Mapped[int] = mapped_column(primary_key=True)
            name: Mapped[str] = mapped_column()

            address: Mapped[Address] = composite(
                mapped_column(), mapped_column(), mapped_column("zip")
            )

        decl_base.metadata.create_all(testing.db)

        with fixture_session() as sess:
            sess.add(
                User(
                    name="user 1",
                    address=Address("123 anywhere street", "NY", "12345"),
                )
            )
            sess.commit()

        with fixture_session() as sess:
            u1 = sess.scalar(select(User))

            # round trip!
            eq_(u1.address, Address("123 anywhere street", "NY", "12345"))

    def test_cls_annotated_no_mapped_cols_setup(self, decl_base):
        @dataclasses.dataclass
        class Address:
            street: str
            state: str
            zip_: str

        class User(decl_base):
            __tablename__ = "user"

            id: Mapped[int] = mapped_column(primary_key=True)
            name: Mapped[str] = mapped_column()

            address: Mapped[Address] = composite()

        decl_base.metadata.create_all(testing.db)

        with fixture_session() as sess:
            sess.add(
                User(
                    name="user 1",
                    address=Address("123 anywhere street", "NY", "12345"),
                )
            )
            sess.commit()

        with fixture_session() as sess:
            u1 = sess.scalar(select(User))

            # round trip!
            eq_(u1.address, Address("123 anywhere street", "NY", "12345"))

    def test_one_col_setup(self, decl_base):
        @dataclasses.dataclass
        class Address:
            street: str

        class User(decl_base):
            __tablename__ = "user"

            id: Mapped[int] = mapped_column(primary_key=True)
            name: Mapped[str] = mapped_column()

            address: Mapped[Address] = composite(Address, mapped_column())

        decl_base.metadata.create_all(testing.db)

        with fixture_session() as sess:
            sess.add(
                User(
                    name="user 1",
                    address=Address("123 anywhere street"),
                )
            )
            sess.commit()

        with fixture_session() as sess:
            u1 = sess.scalar(select(User))

            # round trip!
            eq_(u1.address, Address("123 anywhere street"))


class AllYourFavoriteHitsTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    """try a bunch of common mappings using the new style"""

    __dialect__ = "default"

    def test_employee_joined_inh(self, decl_base: Type[DeclarativeBase]):

        str50 = Annotated[str, 50]
        str30 = Annotated[str, 30]
        opt_str50 = Optional[str50]

        decl_base.registry.update_type_annotation_map(
            {str50: String(50), str30: String(30)}
        )

        class Company(decl_base):
            __tablename__ = "company"

            company_id: Mapped[int] = mapped_column(Integer, primary_key=True)

            name: Mapped[str50]

            employees: Mapped[Set["Person"]] = relationship()  # noqa: F821

        class Person(decl_base):
            __tablename__ = "person"
            person_id: Mapped[int] = mapped_column(primary_key=True)
            company_id: Mapped[int] = mapped_column(
                ForeignKey("company.company_id")
            )
            name: Mapped[str50]
            type: Mapped[str30] = mapped_column()

            __mapper_args__ = {"polymorphic_on": type}

        class Engineer(Person):
            __tablename__ = "engineer"

            person_id: Mapped[int] = mapped_column(
                ForeignKey("person.person_id"), primary_key=True
            )
            __mapper_args__ = {"polymorphic_identity": "engineer"}

            status: Mapped[str] = mapped_column(String(30))
            engineer_name: Mapped[opt_str50]
            primary_language: Mapped[opt_str50]

        class Manager(Person):
            __tablename__ = "manager"

            person_id: Mapped[int] = mapped_column(
                ForeignKey("person.person_id"), primary_key=True
            )
            status: Mapped[str] = mapped_column(String(30))
            manager_name: Mapped[str50]
            __mapper_args__ = {"polymorphic_identity": "manager"}

        is_(Person.__mapper__.polymorphic_on, Person.__table__.c.type)

        # the SELECT statements here confirm the columns present and their
        # ordering
        self.assert_compile(
            select(Person),
            "SELECT person.person_id, person.company_id, person.name, "
            "person.type FROM person",
        )

        self.assert_compile(
            select(Manager),
            "SELECT manager.person_id, person.person_id AS person_id_1, "
            "person.company_id, person.name, person.type, manager.status, "
            "manager.manager_name FROM person "
            "JOIN manager ON person.person_id = manager.person_id",
        )

        self.assert_compile(
            select(Company).join(Company.employees.of_type(Engineer)),
            "SELECT company.company_id, company.name FROM company JOIN "
            "(person JOIN engineer ON person.person_id = engineer.person_id) "
            "ON company.company_id = person.company_id",
        )


class WriteOnlyRelationshipTest(fixtures.TestBase):
    def _assertions(self, A, B, lazy):
        is_(A.bs.property.mapper, B.__mapper__)

        is_true(A.bs.property.uselist)
        eq_(A.bs.property.lazy, lazy)

    def test_dynamic(self, decl_base):
        class B(decl_base):
            __tablename__ = "b"
            id: Mapped[int] = mapped_column(primary_key=True)
            a_id: Mapped[int] = mapped_column(
                ForeignKey("a.id", ondelete="cascade")
            )

        class A(decl_base):
            __tablename__ = "a"
            id: Mapped[int] = mapped_column(primary_key=True)
            bs: DynamicMapped[B] = relationship()

        self._assertions(A, B, "dynamic")

    def test_write_only(self, decl_base):
        class B(decl_base):
            __tablename__ = "b"
            id: Mapped[int] = mapped_column(primary_key=True)
            a_id: Mapped[int] = mapped_column(
                ForeignKey("a.id", ondelete="cascade")
            )

        class A(decl_base):
            __tablename__ = "a"
            id: Mapped[int] = mapped_column(primary_key=True)
            bs: WriteOnlyMapped[B] = relationship()

        self._assertions(A, B, "write_only")
