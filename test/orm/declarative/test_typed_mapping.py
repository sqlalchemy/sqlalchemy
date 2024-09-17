import collections.abc
import dataclasses
import datetime
from decimal import Decimal
import enum
import inspect as _py_inspect
import typing
from typing import Any
from typing import cast
from typing import ClassVar
from typing import Dict
from typing import Generic
from typing import List
from typing import NewType
from typing import Optional
from typing import Set
from typing import Type
from typing import TYPE_CHECKING
from typing import TypedDict
from typing import TypeVar
from typing import Union
import uuid

import typing_extensions
from typing_extensions import get_args as get_args
from typing_extensions import Literal as Literal
from typing_extensions import TypeAlias as TypeAlias

from sqlalchemy import BIGINT
from sqlalchemy import BigInteger
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import exc
from sqlalchemy import exc as sa_exc
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Identity
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import JSON
from sqlalchemy import Numeric
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import types
from sqlalchemy import VARCHAR
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.associationproxy import AssociationProxy
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import as_declarative
from sqlalchemy.orm import composite
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import declared_attr
from sqlalchemy.orm import deferred
from sqlalchemy.orm import DynamicMapped
from sqlalchemy.orm import foreign
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import MappedAsDataclass
from sqlalchemy.orm import Relationship
from sqlalchemy.orm import relationship
from sqlalchemy.orm import remote
from sqlalchemy.orm import Session
from sqlalchemy.orm import undefer
from sqlalchemy.orm import WriteOnlyMapped
from sqlalchemy.orm.attributes import CollectionAttributeImpl
from sqlalchemy.orm.collections import attribute_keyed_dict
from sqlalchemy.orm.collections import KeyFuncDict
from sqlalchemy.orm.dynamic import DynamicAttributeImpl
from sqlalchemy.orm.properties import MappedColumn
from sqlalchemy.orm.writeonly import WriteOnlyAttributeImpl
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql.base import _NoArg
from sqlalchemy.sql.sqltypes import Enum
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_deprecated
from sqlalchemy.testing import expect_raises
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_false
from sqlalchemy.testing import is_not
from sqlalchemy.testing import is_true
from sqlalchemy.testing import Variation
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.util import compat
from sqlalchemy.util.typing import Annotated


class _SomeDict1(TypedDict):
    type: Literal["1"]


class _SomeDict2(TypedDict):
    type: Literal["2"]


_UnionTypeAlias: TypeAlias = Union[_SomeDict1, _SomeDict2]

_StrTypeAlias: TypeAlias = str

_StrPep695: TypeAlias = str
_UnionPep695: TypeAlias = Union[_SomeDict1, _SomeDict2]

_Literal695: TypeAlias = Literal["to-do", "in-progress", "done"]
_Recursive695_0: TypeAlias = _Literal695
_Recursive695_1: TypeAlias = _Recursive695_0
_Recursive695_2: TypeAlias = _Recursive695_1

_TypingLiteral = typing.Literal["a", "b"]
_TypingExtensionsLiteral = typing_extensions.Literal["a", "b"]

if compat.py312:
    exec(
        """
type _UnionPep695 = _SomeDict1 | _SomeDict2
type _StrPep695 = str

type strtypalias_keyword = Annotated[str, mapped_column(info={"hi": "there"})]

strtypalias_tat: typing.TypeAliasType = Annotated[
    str, mapped_column(info={"hi": "there"})]

strtypalias_plain = Annotated[str, mapped_column(info={"hi": "there"})]

type _Literal695 = Literal["to-do", "in-progress", "done"]
type _Recursive695_0 = _Literal695
type _Recursive695_1 = _Recursive695_0
type _Recursive695_2 = _Recursive695_1
""",
        globals(),
    )


def expect_annotation_syntax_error(name):
    return expect_raises_message(
        sa_exc.ArgumentError,
        f'Type annotation for "{name}" '
        "can't be correctly interpreted for "
        "Annotated Declarative Table form.  ",
    )


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


_annotated_names_tested = set()


def annotated_name_test_cases(*cases, **kw):
    _annotated_names_tested.update([case[0] for case in cases])

    return testing.combinations_list(cases, **kw)


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

    def test_type_inline_declaration(self, decl_base):
        """test #10899"""

        class User(decl_base):
            __tablename__ = "user"

            class Role(enum.Enum):
                admin = "admin"
                user = "user"

            id: Mapped[int] = mapped_column(primary_key=True)
            role: Mapped[Role]

        is_true(isinstance(User.__table__.c.role.type, Enum))
        eq_(User.__table__.c.role.type.length, 5)
        is_(User.__table__.c.role.type.enum_class, User.Role)
        eq_(User.__table__.c.role.type.name, "role")  # and not 'enum'

    def test_type_uses_inner_when_present(self, decl_base):
        """test #10899, that we use inner name when appropriate"""

        class Role(enum.Enum):
            foo = "foo"
            bar = "bar"

        class User(decl_base):
            __tablename__ = "user"

            class Role(enum.Enum):
                admin = "admin"
                user = "user"

            id: Mapped[int] = mapped_column(primary_key=True)
            role: Mapped[Role]

        is_true(isinstance(User.__table__.c.role.type, Enum))
        eq_(User.__table__.c.role.type.length, 5)
        is_(User.__table__.c.role.type.enum_class, User.Role)
        eq_(User.__table__.c.role.type.name, "role")  # and not 'enum'

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

    @testing.variation("style", ["none", "lambda_", "string", "direct"])
    def test_foreign_annotation_propagates_correctly(self, decl_base, style):
        """test #10597"""

        class Parent(decl_base):
            __tablename__ = "parent"
            id: Mapped[int] = mapped_column(primary_key=True)

        class Child(decl_base):
            __tablename__ = "child"

            name: Mapped[str] = mapped_column(primary_key=True)

            if style.none:
                parent_id: Mapped[int] = mapped_column(ForeignKey("parent.id"))
            else:
                parent_id: Mapped[int] = mapped_column()

            if style.lambda_:
                parent: Mapped[Parent] = relationship(
                    primaryjoin=lambda: remote(Parent.id)
                    == foreign(Child.parent_id),
                )
            elif style.string:
                parent: Mapped[Parent] = relationship(
                    primaryjoin="remote(Parent.id) == "
                    "foreign(Child.parent_id)",
                )
            elif style.direct:
                parent: Mapped[Parent] = relationship(
                    primaryjoin=remote(Parent.id) == foreign(parent_id),
                )
            elif style.none:
                parent: Mapped[Parent] = relationship()

        assert Child.__mapper__.attrs.parent.strategy.use_get

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

    @testing.variation("case", ["key", "name", "both"])
    @testing.variation("deferred", [True, False])
    @testing.variation("use_add_property", [True, False])
    def test_separate_name(self, decl_base, case, deferred, use_add_property):
        if case.key:
            args = {"key": "data_"}
        elif case.name:
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
        with expect_annotation_syntax_error("MyClass.status"):

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

    @testing.variation("annotation", ["none", "any", "datatype"])
    @testing.variation("explicit_name", [True, False])
    @testing.variation("attribute", ["column", "deferred"])
    def test_allow_unmapped_cols(self, annotation, explicit_name, attribute):
        class Base(DeclarativeBase):
            __allow_unmapped__ = True

        if attribute.column:
            if explicit_name:
                attr = Column("data_one", Integer)
            else:
                attr = Column(Integer)
        elif attribute.deferred:
            if explicit_name:
                attr = deferred(Column("data_one", Integer))
            else:
                attr = deferred(Column(Integer))
        else:
            attribute.fail()

        class MyClass(Base):
            __tablename__ = "mytable"

            id: Mapped[int] = mapped_column(primary_key=True)

            if annotation.none:
                data = attr
            elif annotation.any:
                data: Any = attr
            elif annotation.datatype:
                data: int = attr
            else:
                annotation.fail()

        if explicit_name:
            eq_(MyClass.__table__.c.keys(), ["id", "data_one"])
        else:
            eq_(MyClass.__table__.c.keys(), ["id", "data"])

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
        # anno only: global MyClass

        class MyClass:
            pass

        with expect_raises_message(
            sa_exc.ArgumentError,
            "Could not locate SQLAlchemy Core type for Python type "
            ".*MyClass.* inside the 'data' attribute Mapped annotation",
        ):

            class User(decl_base):
                __tablename__ = "users"

                id: Mapped[int] = mapped_column(primary_key=True)
                data: Mapped[MyClass] = mapped_column()

    def test_construct_lhs_sqlalchemy_type(self, decl_base):
        with expect_raises_message(
            sa_exc.ArgumentError,
            "The type provided inside the 'data' attribute Mapped "
            "annotation is the SQLAlchemy type .*BigInteger.*. Expected "
            "a Python type instead",
        ):

            class User(decl_base):
                __tablename__ = "users"

                id: Mapped[int] = mapped_column(primary_key=True)
                data: Mapped[BigInteger] = mapped_column()

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
    @testing.combinations(True, False, argnames="use_mixin")
    def test_construct_nullability_overrides(
        self, decl_base, include_rhs_type, use_mixin
    ):
        if include_rhs_type:
            args = (String,)
        else:
            args = ()

        # anno only: global anno_str, anno_str_optional, anno_str_mc
        # anno only: global anno_str_optional_mc, anno_str_mc_nullable
        # anno only: global anno_str_optional_mc_notnull
        # anno only: global newtype_str

        anno_str = Annotated[str, 50]
        anno_str_optional = Annotated[Optional[str], 30]

        newtype_str = NewType("MyType", str)

        anno_str_mc = Annotated[str, mapped_column()]
        anno_str_optional_mc = Annotated[Optional[str], mapped_column()]
        anno_str_mc_nullable = Annotated[str, mapped_column(nullable=True)]
        anno_str_optional_mc_notnull = Annotated[
            Optional[str], mapped_column(nullable=False)
        ]

        decl_base.registry.update_type_annotation_map(
            {
                anno_str: String(50),
                anno_str_optional: String(30),
                newtype_str: String(40),
            }
        )

        if TYPE_CHECKING:

            class user_base:
                pass

        else:
            if use_mixin:
                user_base = object
            else:
                user_base = decl_base

        class UserPossibleMixin(user_base):
            if not use_mixin:
                __tablename__ = "users"

                id: Mapped[int] = mapped_column(primary_key=True)  # noqa: A001

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

            # test #9177 cases
            anno_1a: Mapped[anno_str] = mapped_column(*args)
            anno_1b: Mapped[anno_str] = mapped_column(*args, nullable=True)
            anno_1c: Mapped[anno_str] = mapped_column(*args, deferred=True)
            anno_1d: Mapped[anno_str] = mapped_column(
                *args, deferred=True, deferred_group="mygroup"
            )

            anno_2a: Mapped[anno_str_optional] = mapped_column(*args)
            anno_2b: Mapped[anno_str_optional] = mapped_column(
                *args, nullable=False
            )

            anno_3a: Mapped[anno_str_mc] = mapped_column(*args)
            anno_3b: Mapped[anno_str_mc] = mapped_column(*args, nullable=True)
            anno_3c: Mapped[Optional[anno_str_mc]] = mapped_column(*args)

            anno_4a: Mapped[anno_str_optional_mc] = mapped_column(*args)
            anno_4b: Mapped[anno_str_optional_mc] = mapped_column(
                *args, nullable=False
            )

            anno_5a: Mapped[anno_str_mc_nullable] = mapped_column(*args)
            anno_5b: Mapped[anno_str_mc_nullable] = mapped_column(
                *args, nullable=False
            )

            anno_6a: Mapped[anno_str_optional_mc_notnull] = mapped_column(
                *args
            )
            anno_6b: Mapped[anno_str_optional_mc_notnull] = mapped_column(
                *args, nullable=True
            )

            newtype_1a: Mapped[newtype_str] = mapped_column(*args)
            newtype_1b: Mapped[newtype_str] = mapped_column(
                *args, nullable=True
            )

        if use_mixin:

            class User(UserPossibleMixin, decl_base):
                __tablename__ = "users"

                id: Mapped[int] = mapped_column(primary_key=True)

        else:
            User = UserPossibleMixin

        eq_(User.anno_1b.property.deferred, False)
        eq_(User.anno_1c.property.deferred, True)
        eq_(User.anno_1d.property.group, "mygroup")

        is_false(User.__table__.c.lnnl_rndf.nullable)
        is_false(User.__table__.c.lnnl_rnnl.nullable)
        is_true(User.__table__.c.lnnl_rnl.nullable)

        is_true(User.__table__.c.lnl_rndf.nullable)
        is_false(User.__table__.c.lnl_rnnl.nullable)
        is_true(User.__table__.c.lnl_rnl.nullable)

        is_false(User.__table__.c.anno_1a.nullable)
        is_true(User.__table__.c.anno_1b.nullable)
        is_true(User.__table__.c.anno_2a.nullable)
        is_false(User.__table__.c.anno_2b.nullable)
        is_false(User.__table__.c.anno_3a.nullable)
        is_true(User.__table__.c.anno_3b.nullable)
        is_true(User.__table__.c.anno_3c.nullable)
        is_true(User.__table__.c.anno_4a.nullable)
        is_false(User.__table__.c.anno_4b.nullable)
        is_true(User.__table__.c.anno_5a.nullable)
        is_false(User.__table__.c.anno_5b.nullable)
        is_false(User.__table__.c.anno_6a.nullable)
        is_true(User.__table__.c.anno_6b.nullable)

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
        # anno only: global str50, str30, opt_str50, opt_str30

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

    def test_plain_typealias_as_typemap_keys(
        self, decl_base: Type[DeclarativeBase]
    ):
        decl_base.registry.update_type_annotation_map(
            {_UnionTypeAlias: JSON, _StrTypeAlias: String(30)}
        )

        class Test(decl_base):
            __tablename__ = "test"
            id: Mapped[int] = mapped_column(primary_key=True)
            data: Mapped[_StrTypeAlias]
            structure: Mapped[_UnionTypeAlias]

        eq_(Test.__table__.c.data.type.length, 30)
        is_(Test.__table__.c.structure.type._type_affinity, JSON)

    @testing.requires.python312
    def test_pep695_typealias_as_typemap_keys(
        self, decl_base: Type[DeclarativeBase]
    ):
        """test #10807"""

        decl_base.registry.update_type_annotation_map(
            {_UnionPep695: JSON, _StrPep695: String(30)}
        )

        class Test(decl_base):
            __tablename__ = "test"
            id: Mapped[int] = mapped_column(primary_key=True)
            data: Mapped[_StrPep695]
            structure: Mapped[_UnionPep695]

        eq_(Test.__table__.c.data.type._type_affinity, String)
        eq_(Test.__table__.c.data.type.length, 30)
        is_(Test.__table__.c.structure.type._type_affinity, JSON)

    @testing.variation("alias_type", ["none", "typekeyword", "typealiastype"])
    @testing.requires.python312
    def test_extract_pep593_from_pep695(
        self, decl_base: Type[DeclarativeBase], alias_type
    ):
        """test #11130"""

        class MyClass(decl_base):
            __tablename__ = "my_table"

            id: Mapped[int] = mapped_column(primary_key=True)

            if alias_type.typekeyword:
                data_one: Mapped[strtypalias_keyword]  # noqa: F821
            elif alias_type.typealiastype:
                data_one: Mapped[strtypalias_tat]  # noqa: F821
            elif alias_type.none:
                data_one: Mapped[strtypalias_plain]  # noqa: F821
            else:
                alias_type.fail()

        table = MyClass.__table__
        assert table is not None

        eq_(MyClass.data_one.expression.info, {"hi": "there"})

    @testing.requires.python312
    def test_pep695_literal_defaults_to_enum(self, decl_base):
        """test #11305."""

        class Foo(decl_base):
            __tablename__ = "footable"

            id: Mapped[int] = mapped_column(primary_key=True)
            status: Mapped[_Literal695]
            r2: Mapped[_Recursive695_2]

        for col in (Foo.__table__.c.status, Foo.__table__.c.r2):
            is_true(isinstance(col.type, Enum))
            eq_(col.type.enums, ["to-do", "in-progress", "done"])
            is_(col.type.native_enum, False)

    def test_typing_literal_identity(self, decl_base):
        """See issue #11820"""

        class Foo(decl_base):
            __tablename__ = "footable"

            id: Mapped[int] = mapped_column(primary_key=True)
            t: Mapped[_TypingLiteral]
            te: Mapped[_TypingExtensionsLiteral]

        for col in (Foo.__table__.c.t, Foo.__table__.c.te):
            is_true(isinstance(col.type, Enum))
            eq_(col.type.enums, ["a", "b"])
            is_(col.type.native_enum, False)

    @testing.requires.python310
    def test_we_got_all_attrs_test_annotated(self):
        argnames = _py_inspect.getfullargspec(mapped_column)
        assert _annotated_names_tested.issuperset(argnames.kwonlyargs), (
            f"annotated attributes were not tested: "
            f"{set(argnames.kwonlyargs).difference(_annotated_names_tested)}"
        )

    @annotated_name_test_cases(
        ("sort_order", 100, lambda sort_order: sort_order == 100),
        ("nullable", False, lambda column: column.nullable is False),
        (
            "active_history",
            True,
            lambda column_property: column_property.active_history is True,
        ),
        (
            "deferred",
            True,
            lambda column_property: column_property.deferred is True,
        ),
        (
            "deferred",
            _NoArg.NO_ARG,
            lambda column_property: column_property is None,
        ),
        (
            "deferred_group",
            "mygroup",
            lambda column_property: column_property.deferred is True
            and column_property.group == "mygroup",
        ),
        (
            "deferred_raiseload",
            True,
            lambda column_property: column_property.deferred is True
            and column_property.raiseload is True,
        ),
        (
            "server_default",
            "25",
            lambda column: column.server_default.arg == "25",
        ),
        (
            "server_onupdate",
            "25",
            lambda column: column.server_onupdate.arg == "25",
        ),
        (
            "default",
            25,
            lambda column: column.default.arg == 25,
        ),
        (
            "insert_default",
            25,
            lambda column: column.default.arg == 25,
        ),
        (
            "onupdate",
            25,
            lambda column: column.onupdate.arg == 25,
        ),
        ("doc", "some doc", lambda column: column.doc == "some doc"),
        (
            "comment",
            "some comment",
            lambda column: column.comment == "some comment",
        ),
        ("index", True, lambda column: column.index is True),
        ("index", _NoArg.NO_ARG, lambda column: column.index is None),
        ("index", False, lambda column: column.index is False),
        ("unique", True, lambda column: column.unique is True),
        ("unique", False, lambda column: column.unique is False),
        ("autoincrement", True, lambda column: column.autoincrement is True),
        ("system", True, lambda column: column.system is True),
        ("primary_key", True, lambda column: column.primary_key is True),
        ("type_", BIGINT, lambda column: isinstance(column.type, BIGINT)),
        ("info", {"foo": "bar"}, lambda column: column.info == {"foo": "bar"}),
        (
            "use_existing_column",
            True,
            lambda mc: mc._use_existing_column is True,
        ),
        (
            "quote",
            True,
            exc.SADeprecationWarning(
                "Can't use the 'key' or 'name' arguments in Annotated "
            ),
        ),
        (
            "key",
            "mykey",
            exc.SADeprecationWarning(
                "Can't use the 'key' or 'name' arguments in Annotated "
            ),
        ),
        (
            "name",
            "mykey",
            exc.SADeprecationWarning(
                "Can't use the 'key' or 'name' arguments in Annotated "
            ),
        ),
        (
            "kw_only",
            True,
            exc.SADeprecationWarning(
                "Argument 'kw_only' is a dataclass argument "
            ),
            testing.requires.python310,
        ),
        (
            "compare",
            True,
            exc.SADeprecationWarning(
                "Argument 'compare' is a dataclass argument "
            ),
            testing.requires.python310,
        ),
        (
            "default_factory",
            lambda: 25,
            exc.SADeprecationWarning(
                "Argument 'default_factory' is a dataclass argument "
            ),
        ),
        (
            "repr",
            True,
            exc.SADeprecationWarning(
                "Argument 'repr' is a dataclass argument "
            ),
        ),
        (
            "init",
            True,
            exc.SADeprecationWarning(
                "Argument 'init' is a dataclass argument"
            ),
        ),
        argnames="argname, argument, assertion",
    )
    @testing.variation("use_annotated", [True, False, "control"])
    def test_names_encountered_for_annotated(
        self, argname, argument, assertion, use_annotated, decl_base
    ):
        # anno only: global myint

        if argument is not _NoArg.NO_ARG:
            kw = {argname: argument}

            if argname == "quote":
                kw["name"] = "somename"
        else:
            kw = {}

        is_warning = isinstance(assertion, exc.SADeprecationWarning)
        is_dataclass = argname in (
            "kw_only",
            "init",
            "repr",
            "compare",
            "default_factory",
        )

        if is_dataclass:

            class Base(MappedAsDataclass, decl_base):
                __abstract__ = True

        else:
            Base = decl_base

        if use_annotated.control:
            # test in reverse; that kw set on the main mapped_column() takes
            # effect when the Annotated is there also and does not have the
            # kw
            amc = mapped_column()
            myint = Annotated[int, amc]

            mc = mapped_column(**kw)

            class User(Base):
                __tablename__ = "user"
                id: Mapped[int] = mapped_column(primary_key=True)
                myname: Mapped[myint] = mc

        elif use_annotated:
            amc = mapped_column(**kw)
            myint = Annotated[int, amc]

            mc = mapped_column()

            if is_warning:
                with expect_deprecated(assertion.args[0]):

                    class User(Base):
                        __tablename__ = "user"
                        id: Mapped[int] = mapped_column(primary_key=True)
                        myname: Mapped[myint] = mc

            else:

                class User(Base):
                    __tablename__ = "user"
                    id: Mapped[int] = mapped_column(primary_key=True)
                    myname: Mapped[myint] = mc

        else:
            mc = cast(MappedColumn, mapped_column(**kw))

        mapper_prop = mc.mapper_property_to_assign
        column_to_assign, sort_order = mc.columns_to_assign[0]

        if not is_warning:
            assert_result = testing.resolve_lambda(
                assertion,
                sort_order=sort_order,
                column_property=mapper_prop,
                column=column_to_assign,
                mc=mc,
            )
            assert assert_result
        elif is_dataclass and (not use_annotated or use_annotated.control):
            eq_(
                getattr(mc._attribute_options, f"dataclasses_{argname}"),
                argument,
            )

    @testing.combinations(("index",), ("unique",), argnames="paramname")
    @testing.combinations((True,), (False,), (None,), argnames="orig")
    @testing.combinations((True,), (False,), (None,), argnames="merging")
    def test_index_unique_combinations(
        self, paramname, orig, merging, decl_base
    ):
        """test #11091"""

        # anno only: global myint

        amc = mapped_column(**{paramname: merging})
        myint = Annotated[int, amc]

        mc = mapped_column(**{paramname: orig})

        class User(decl_base):
            __tablename__ = "user"
            id: Mapped[int] = mapped_column(primary_key=True)
            myname: Mapped[myint] = mc

        result = getattr(User.__table__.c.myname, paramname)
        if orig is None:
            is_(result, merging)
        else:
            is_(result, orig)

    def test_pep484_newtypes_as_typemap_keys(
        self, decl_base: Type[DeclarativeBase]
    ):
        # anno only: global str50, str30, str3050

        str50 = NewType("str50", str)
        str30 = NewType("str30", str)
        str3050 = NewType("str30", str50)

        decl_base.registry.update_type_annotation_map(
            {str50: String(50), str30: String(30), str3050: String(150)}
        )

        class MyClass(decl_base):
            __tablename__ = "my_table"

            id: Mapped[str50] = mapped_column(primary_key=True)
            data_one: Mapped[str30]
            data_two: Mapped[str50]
            data_three: Mapped[Optional[str30]]
            data_four: Mapped[str3050]

        eq_(MyClass.__table__.c.data_one.type.length, 30)
        is_false(MyClass.__table__.c.data_one.nullable)

        eq_(MyClass.__table__.c.data_two.type.length, 50)
        is_false(MyClass.__table__.c.data_two.nullable)

        eq_(MyClass.__table__.c.data_three.type.length, 30)
        is_true(MyClass.__table__.c.data_three.nullable)

        eq_(MyClass.__table__.c.data_four.type.length, 150)
        is_false(MyClass.__table__.c.data_four.nullable)

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

        global SomeRelated

        class SomeRelated(decl_base):
            __tablename__: ClassVar[Optional[str]] = "some_related"
            id: Mapped["int"] = mapped_column(primary_key=True)

        with expect_raises_message(
            NotImplementedError,
            r"Use of the 'Relationship' construct inside of an Annotated "
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
        # anno only: global intpk, strnone, str30nullable
        # anno only: global opt_strnone, opt_str30

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

        # anno only: global intpk, strnone, str30nullable
        # anno only: global opt_strnone, opt_str30
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

    @testing.variation("to_assert", ["ddl", "fkcount", "references"])
    @testing.variation("assign_blank", [True, False])
    def test_extract_fk_col_from_pep593(
        self, decl_base: Type[DeclarativeBase], to_assert, assign_blank
    ):
        # anno only: global intpk, element_ref
        intpk = Annotated[int, mapped_column(primary_key=True)]
        element_ref = Annotated[int, mapped_column(ForeignKey("element.id"))]

        class Element(decl_base):
            __tablename__ = "element"

            id: Mapped[intpk]

        class RefElementOne(decl_base):
            __tablename__ = "refone"

            id: Mapped[intpk]

            if assign_blank:
                other_id: Mapped[element_ref] = mapped_column()
            else:
                other_id: Mapped[element_ref]

        class RefElementTwo(decl_base):
            __tablename__ = "reftwo"

            id: Mapped[intpk]
            if assign_blank:
                some_id: Mapped[element_ref] = mapped_column()
            else:
                some_id: Mapped[element_ref]

        assert Element.__table__ is not None
        assert RefElementOne.__table__ is not None
        assert RefElementTwo.__table__ is not None

        if to_assert.fkcount:
            # test #9766
            eq_(len(RefElementOne.__table__.c.other_id.foreign_keys), 1)
            eq_(len(RefElementTwo.__table__.c.some_id.foreign_keys), 1)
        elif to_assert.references:
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

        elif to_assert.ddl:
            self.assert_compile(
                CreateTable(RefElementOne.__table__),
                "CREATE TABLE refone "
                "(id INTEGER NOT NULL, other_id INTEGER NOT NULL, "
                "PRIMARY KEY (id), "
                "FOREIGN KEY(other_id) REFERENCES element (id))",
            )
            self.assert_compile(
                CreateTable(RefElementTwo.__table__),
                "CREATE TABLE reftwo "
                "(id INTEGER NOT NULL, some_id INTEGER NOT NULL, "
                "PRIMARY KEY (id), "
                "FOREIGN KEY(some_id) REFERENCES element (id))",
            )
        else:
            to_assert.fail()

    @testing.combinations(
        (collections.abc.Sequence, (str,), testing.requires.python310),
        (collections.abc.MutableSequence, (str,), testing.requires.python310),
        (collections.abc.Mapping, (str, str), testing.requires.python310),
        (
            collections.abc.MutableMapping,
            (str, str),
            testing.requires.python310,
        ),
        (typing.Mapping, (str, str), testing.requires.python310),
        (typing.MutableMapping, (str, str), testing.requires.python310),
        (typing.Sequence, (str,)),
        (typing.MutableSequence, (str,)),
        (list, (str,), testing.requires.python310),
        (
            List,
            (str,),
        ),
        (dict, (str, str), testing.requires.python310),
        (
            Dict,
            (str, str),
        ),
        (list, None, testing.requires.python310),
        (
            List,
            None,
        ),
        (dict, None, testing.requires.python310),
        (
            Dict,
            None,
        ),
        id_="sa",
        argnames="container_typ,args",
    )
    @testing.variation("style", ["pep593", "alias", "direct"])
    def test_extract_composed(self, container_typ, args, style):
        """test #9099 (pep593)

        test #11814

        test #11831, regression from #11814
        """

        global TestType

        if style.pep593:
            if args is None:
                TestType = Annotated[container_typ, 0]
            else:
                TestType = Annotated[container_typ[args], 0]
        elif style.alias:
            if args is None:
                TestType = container_typ
            else:
                TestType = container_typ[args]
        elif style.direct:
            TestType = container_typ

        class Base(DeclarativeBase):
            if style.direct:
                if args == (str, str):
                    type_annotation_map = {TestType[str, str]: JSON()}
                elif args is None:
                    type_annotation_map = {TestType: JSON()}
                else:
                    type_annotation_map = {TestType[str]: JSON()}
            else:
                type_annotation_map = {TestType: JSON()}

        class MyClass(Base):
            __tablename__ = "my_table"

            id: Mapped[int] = mapped_column(primary_key=True)

            if style.direct:
                if args == (str, str):
                    data: Mapped[TestType[str, str]] = mapped_column()
                elif args is None:
                    data: Mapped[TestType] = mapped_column()
                else:
                    data: Mapped[TestType[str]] = mapped_column()
            else:
                data: Mapped[TestType] = mapped_column()

        is_(MyClass.__table__.c.data.type._type_affinity, JSON)

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
        # anno only: global intpk, element_ref
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
        # anno only: global intpk, element_ref
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
        # anno only: global intpk, element_ref
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

            optional_data: Mapped[Optional[Union[float, Decimal]]] = (
                mapped_column()
            )

            # use Optional directly
            reverse_optional_data: Mapped[Optional[Union[Decimal, float]]] = (
                mapped_column()
            )

            # use Union with None, same as Optional but presents differently
            # (Optional object with __origin__ Union vs. Union)
            reverse_u_optional_data: Mapped[Union[Decimal, float, None]] = (
                mapped_column()
            )

            float_data: Mapped[float] = mapped_column()
            decimal_data: Mapped[Decimal] = mapped_column()

            if compat.py310:
                pep604_data: Mapped[float | Decimal] = mapped_column()
                pep604_reverse: Mapped[Decimal | float] = mapped_column()
                pep604_optional: Mapped[Decimal | float | None] = (
                    mapped_column()
                )
                pep604_data_fwd: Mapped["float | Decimal"] = mapped_column()
                pep604_reverse_fwd: Mapped["Decimal | float"] = mapped_column()
                pep604_optional_fwd: Mapped["Decimal | float | None"] = (
                    mapped_column()
                )

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

    @testing.combinations(
        ("not_optional",),
        ("optional",),
        ("optional_fwd_ref",),
        ("union_none",),
        ("pep604", testing.requires.python310),
        ("pep604_fwd_ref", testing.requires.python310),
        argnames="optional_on_json",
    )
    @testing.combinations(
        "include_mc_type", "derive_from_anno", argnames="include_mc_type"
    )
    def test_optional_styles_nested_brackets(
        self, optional_on_json, include_mc_type
    ):
        class Base(DeclarativeBase):
            if testing.requires.python310.enabled:
                type_annotation_map = {
                    Dict[str, str]: JSON,
                    dict[str, str]: JSON,
                }
            else:
                type_annotation_map = {
                    Dict[str, str]: JSON,
                }

        if include_mc_type == "include_mc_type":
            mc = mapped_column(JSON)
        else:
            mc = mapped_column()

        class A(Base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True)
            data: Mapped[str] = mapped_column()

            if optional_on_json == "not_optional":
                json: Mapped[Dict[str, str]] = mapped_column()  # type: ignore
            elif optional_on_json == "optional":
                json: Mapped[Optional[Dict[str, str]]] = mc
            elif optional_on_json == "optional_fwd_ref":
                json: Mapped["Optional[Dict[str, str]]"] = mc
            elif optional_on_json == "union_none":
                json: Mapped[Union[Dict[str, str], None]] = mc
            elif optional_on_json == "pep604":
                json: Mapped[dict[str, str] | None] = mc
            elif optional_on_json == "pep604_fwd_ref":
                json: Mapped["dict[str, str] | None"] = mc

        is_(A.__table__.c.json.type._type_affinity, JSON)
        if optional_on_json == "not_optional":
            is_false(A.__table__.c.json.nullable)
        else:
            is_true(A.__table__.c.json.nullable)

    @testing.variation("optional", [True, False])
    @testing.variation("provide_type", [True, False])
    @testing.variation("add_to_type_map", [True, False])
    def test_recursive_type(
        self, decl_base, optional, provide_type, add_to_type_map
    ):
        """test #9553"""

        global T

        T = Dict[str, Optional["T"]]

        if not provide_type and not add_to_type_map:
            with expect_raises_message(
                sa_exc.ArgumentError,
                r"Could not locate SQLAlchemy.*" r".*ForwardRef\('T'\).*",
            ):

                class TypeTest(decl_base):
                    __tablename__ = "my_table"

                    id: Mapped[int] = mapped_column(primary_key=True)
                    if optional:
                        type_test: Mapped[Optional[T]] = mapped_column()
                    else:
                        type_test: Mapped[T] = mapped_column()

            return

        else:
            if add_to_type_map:
                decl_base.registry.update_type_annotation_map({T: JSON()})

            class TypeTest(decl_base):
                __tablename__ = "my_table"

                id: Mapped[int] = mapped_column(primary_key=True)

                if add_to_type_map:
                    if optional:
                        type_test: Mapped[Optional[T]] = mapped_column()
                    else:
                        type_test: Mapped[T] = mapped_column()
                else:
                    if optional:
                        type_test: Mapped[Optional[T]] = mapped_column(JSON())
                    else:
                        type_test: Mapped[T] = mapped_column(JSON())

        if optional:
            is_(TypeTest.__table__.c.type_test.nullable, True)
        else:
            is_(TypeTest.__table__.c.type_test.nullable, False)

        self.assert_compile(
            select(TypeTest),
            "SELECT my_table.id, my_table.type_test FROM my_table",
        )

    def test_missing_mapped_lhs(self, decl_base):
        with expect_annotation_syntax_error("User.name"):

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
            "SELECT users.id, users.data FROM users",
        )

    def test_deferred_kw(self, decl_base):
        class User(decl_base):
            __tablename__ = "users"

            id: Mapped[int] = mapped_column(primary_key=True)
            data: Mapped[str] = mapped_column(deferred=True)

        self.assert_compile(select(User), "SELECT users.id FROM users")
        self.assert_compile(
            select(User).options(undefer(User.data)),
            "SELECT users.id, users.data FROM users",
        )

    @testing.combinations(
        (str, types.String),
        (Decimal, types.Numeric),
        (float, types.Float),
        (datetime.datetime, types.DateTime),
        (uuid.UUID, types.Uuid),
        argnames="pytype_arg,sqltype",
    )
    def test_datatype_lookups(self, decl_base, pytype_arg, sqltype):
        # anno only: global pytype
        pytype = pytype_arg

        class MyClass(decl_base):
            __tablename__ = "mytable"
            id: Mapped[int] = mapped_column(primary_key=True)

            data: Mapped[pytype]

        assert isinstance(MyClass.__table__.c.data.type, sqltype)

    def test_dont_ignore_unresolvable(self, decl_base):
        """test #8888"""

        with expect_raises_message(
            sa_exc.ArgumentError,
            r"Could not resolve all types within mapped annotation: "
            r"\".*Mapped\[.*fake.*\]\".  Ensure all types are written "
            r"correctly and are imported within the module in use.",
        ):

            class A(decl_base):
                __tablename__ = "a"

                id: Mapped[int] = mapped_column(primary_key=True)
                data: Mapped["fake"]  # noqa

    def test_type_dont_mis_resolve_on_superclass(self):
        """test for #8859.

        For subclasses of a type that's in the map, don't resolve this
        by default, even though we do a search through __mro__.

        """
        # anno only: global int_sub

        class int_sub(int):
            pass

        Base = declarative_base(
            type_annotation_map={
                int: Integer,
            }
        )

        with expect_raises_message(
            sa_exc.ArgumentError, "Could not locate SQLAlchemy Core type"
        ):

            class MyClass(Base):
                __tablename__ = "mytable"

                id: Mapped[int] = mapped_column(primary_key=True)
                data: Mapped[int_sub]

    @testing.variation(
        "dict_key", ["typing", ("plain", testing.requires.python310)]
    )
    def test_type_dont_mis_resolve_on_non_generic(self, dict_key):
        """test for #8859.

        For a specific generic type with arguments, don't do any MRO
        lookup.

        """

        Base = declarative_base(
            type_annotation_map={
                dict: String,
            }
        )

        with expect_raises_message(
            sa_exc.ArgumentError, "Could not locate SQLAlchemy Core type"
        ):

            class MyClass(Base):
                __tablename__ = "mytable"

                id: Mapped[int] = mapped_column(primary_key=True)

                if dict_key.plain:
                    data: Mapped[dict[str, str]]
                elif dict_key.typing:
                    data: Mapped[Dict[str, str]]

    def test_type_secondary_resolution(self):
        class MyString(String):
            def _resolve_for_python_type(
                self, python_type, matched_type, matched_on_flattened
            ):
                return String(length=42)

        Base = declarative_base(type_annotation_map={str: MyString})

        class MyClass(Base):
            __tablename__ = "mytable"

            id: Mapped[int] = mapped_column(primary_key=True)
            data: Mapped[str]

        is_true(isinstance(MyClass.__table__.c.data.type, String))
        eq_(MyClass.__table__.c.data.type.length, 42)


class EnumOrLiteralTypeMapTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "default"

    @testing.variation("use_explicit_name", [True, False])
    @testing.variation("use_individual_values", [True, False])
    @testing.variation("include_generic", [True, False])
    @testing.variation("set_native_enum", ["none", True, False])
    def test_enum_explicit(
        self,
        include_generic,
        set_native_enum: Variation,
        use_explicit_name,
        use_individual_values,
    ):
        # anno only: global FooEnum

        class FooEnum(enum.Enum):
            foo = enum.auto()
            bar = enum.auto()

        kw = {"length": 500}

        if use_explicit_name:
            kw["name"] = "my_foo_enum"

        if set_native_enum.none:
            expected_native_enum = True
        elif set_native_enum.set_native_enum:
            kw["native_enum"] = True
            expected_native_enum = True
        elif set_native_enum.not_set_native_enum:
            kw["native_enum"] = False
            expected_native_enum = False
        else:
            set_native_enum.fail()

        if use_individual_values:
            tam = {FooEnum: Enum("foo", "bar", **kw)}
        else:
            tam = {FooEnum: Enum(FooEnum, **kw)}

        if include_generic:
            tam[enum.Enum] = Enum(enum.Enum)
        Base = declarative_base(type_annotation_map=tam)

        class MyClass(Base):
            __tablename__ = "mytable"

            id: Mapped[int] = mapped_column(primary_key=True)
            data: Mapped[FooEnum]

        if use_explicit_name:
            eq_(MyClass.__table__.c.data.type.name, "my_foo_enum")
        elif use_individual_values:
            is_(MyClass.__table__.c.data.type.enum_class, None)
            eq_(MyClass.__table__.c.data.type.name, None)
        else:
            is_(MyClass.__table__.c.data.type.enum_class, FooEnum)
            eq_(MyClass.__table__.c.data.type.name, "fooenum")

        is_true(isinstance(MyClass.__table__.c.data.type, Enum))
        eq_(MyClass.__table__.c.data.type.length, 500)

        is_(MyClass.__table__.c.data.type.native_enum, expected_native_enum)

    @testing.variation("set_native_enum", ["none", True, False])
    def test_enum_generic(self, set_native_enum: Variation):
        """test for #8859"""
        # anno only: global FooEnum

        class FooEnum(enum.Enum):
            foo = enum.auto()
            bar = enum.auto()

        kw = {"length": 42}

        if set_native_enum.none:
            expected_native_enum = True
        elif set_native_enum.set_native_enum:
            kw["native_enum"] = True
            expected_native_enum = True
        elif set_native_enum.not_set_native_enum:
            kw["native_enum"] = False
            expected_native_enum = False
        else:
            set_native_enum.fail()

        Base = declarative_base(
            type_annotation_map={enum.Enum: Enum(enum.Enum, **kw)}
        )

        class MyClass(Base):
            __tablename__ = "mytable"

            id: Mapped[int] = mapped_column(primary_key=True)
            data: Mapped[FooEnum]

        is_true(isinstance(MyClass.__table__.c.data.type, Enum))
        eq_(MyClass.__table__.c.data.type.length, 42)
        is_(MyClass.__table__.c.data.type.enum_class, FooEnum)
        is_(MyClass.__table__.c.data.type.native_enum, expected_native_enum)

    def test_enum_default(self, decl_base):
        """test #8859.

        We now have Enum in the default SQL lookup map, in conjunction with
        a mechanism that will adapt it for a given enum type.

        This relies on a search through __mro__ for the given type,
        which in other tests we ensure does not actually function if
        we aren't dealing with Enum (or some other type that allows for
        __mro__ lookup)

        """
        # anno only: global FooEnum

        class FooEnum(enum.Enum):
            foo = "foo"
            bar_value = "bar"

        class MyClass(decl_base):
            __tablename__ = "mytable"

            id: Mapped[int] = mapped_column(primary_key=True)
            data: Mapped[FooEnum]

        is_true(isinstance(MyClass.__table__.c.data.type, Enum))
        eq_(MyClass.__table__.c.data.type.length, 9)
        is_(MyClass.__table__.c.data.type.enum_class, FooEnum)
        eq_(MyClass.__table__.c.data.type.name, "fooenum")  # and not 'enum'

    @testing.variation(
        "sqltype",
        [
            "custom",
            "base_enum_name_none",
            "base_enum_default_name",
            "specific_unnamed_enum",
            "specific_named_enum",
            "string",
        ],
    )
    @testing.variation("indicate_type_explicitly", [True, False])
    def test_pep586_literal(
        self, decl_base, sqltype: Variation, indicate_type_explicitly
    ):
        """test #9187."""

        # anno only: global Status

        Status = Literal["to-do", "in-progress", "done"]

        if sqltype.custom:

            class LiteralSqlType(types.TypeDecorator):
                impl = types.String
                cache_ok = True

                def __init__(self, literal_type: Any) -> None:
                    super().__init__()
                    self._possible_values = get_args(literal_type)

            our_type = mapped_col_type = LiteralSqlType(Status)
        elif sqltype.specific_unnamed_enum:
            our_type = mapped_col_type = Enum(
                "to-do", "in-progress", "done", native_enum=False
            )
        elif sqltype.specific_named_enum:
            our_type = mapped_col_type = Enum(
                "to-do", "in-progress", "done", name="specific_name"
            )
        elif sqltype.base_enum_name_none:
            our_type = Enum(enum.Enum, native_enum=False, name=None)
            mapped_col_type = Enum(
                "to-do", "in-progress", "done", native_enum=False
            )
        elif sqltype.base_enum_default_name:
            our_type = Enum(enum.Enum, native_enum=False)
            mapped_col_type = Enum(
                "to-do", "in-progress", "done", native_enum=False
            )
        elif sqltype.string:
            our_type = mapped_col_type = String(50)
        else:
            sqltype.fail()

        decl_base.registry.update_type_annotation_map({Status: our_type})

        class Foo(decl_base):
            __tablename__ = "footable"

            id: Mapped[int] = mapped_column(primary_key=True)

            if indicate_type_explicitly:
                status: Mapped[Status] = mapped_column(mapped_col_type)
            else:
                status: Mapped[Status]

        is_true(isinstance(Foo.__table__.c.status.type, type(our_type)))

        if sqltype.custom:
            eq_(
                Foo.__table__.c.status.type._possible_values,
                ("to-do", "in-progress", "done"),
            )
        elif (
            sqltype.specific_unnamed_enum
            or sqltype.base_enum_name_none
            or sqltype.base_enum_default_name
        ):
            eq_(
                Foo.__table__.c.status.type.enums,
                ["to-do", "in-progress", "done"],
            )
            is_(Foo.__table__.c.status.type.native_enum, False)
        elif sqltype.specific_named_enum:
            is_(Foo.__table__.c.status.type.native_enum, True)

        if (
            sqltype.specific_unnamed_enum
            or sqltype.base_enum_name_none
            or sqltype.base_enum_default_name
        ):
            eq_(Foo.__table__.c.status.type.name, None)
        elif sqltype.specific_named_enum:
            eq_(Foo.__table__.c.status.type.name, "specific_name")

    @testing.variation("indicate_type_explicitly", [True, False])
    def test_pep586_literal_defaults_to_enum(
        self, decl_base, indicate_type_explicitly
    ):
        """test #9187."""

        # anno only: global Status

        Status = Literal["to-do", "in-progress", "done"]

        if indicate_type_explicitly:
            expected_native_enum = True
        else:
            expected_native_enum = False

        class Foo(decl_base):
            __tablename__ = "footable"

            id: Mapped[int] = mapped_column(primary_key=True)

            if indicate_type_explicitly:
                status: Mapped[Status] = mapped_column(
                    Enum("to-do", "in-progress", "done")
                )
            else:
                status: Mapped[Status]

        is_true(isinstance(Foo.__table__.c.status.type, Enum))

        eq_(
            Foo.__table__.c.status.type.enums,
            ["to-do", "in-progress", "done"],
        )
        is_(Foo.__table__.c.status.type.native_enum, expected_native_enum)

    @testing.variation("override_in_type_map", [True, False])
    @testing.variation("indicate_type_explicitly", [True, False])
    def test_pep586_literal_checks_the_arguments(
        self, decl_base, indicate_type_explicitly, override_in_type_map
    ):
        """test #9187."""

        # anno only: global NotReallyStrings

        NotReallyStrings = Literal["str1", 17, False]

        if override_in_type_map:
            decl_base.registry.update_type_annotation_map(
                {NotReallyStrings: JSON}
            )

        if not override_in_type_map and not indicate_type_explicitly:
            with expect_raises_message(
                ArgumentError,
                "Can't create string-based Enum datatype from non-string "
                "values: 17, False.  Please provide an explicit Enum "
                "datatype for this Python type",
            ):

                class Foo(decl_base):
                    __tablename__ = "footable"

                    id: Mapped[int] = mapped_column(primary_key=True)
                    status: Mapped[NotReallyStrings]

        else:
            # if we override the type in the type_map or mapped_column,
            # then we can again use a Literal with non-strings
            class Foo(decl_base):
                __tablename__ = "footable"

                id: Mapped[int] = mapped_column(primary_key=True)

                if indicate_type_explicitly:
                    status: Mapped[NotReallyStrings] = mapped_column(JSON)
                else:
                    status: Mapped[NotReallyStrings]

            is_true(isinstance(Foo.__table__.c.status.type, JSON))


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

    @testing.variation("use_directive", [True, False])
    @testing.variation("use_annotation", [True, False])
    def test_supplemental_declared_attr(
        self, decl_base, use_directive, use_annotation
    ):
        """test #9957"""

        class User(decl_base):
            __tablename__ = "user"
            id: Mapped[int] = mapped_column(primary_key=True)
            branch_id: Mapped[int] = mapped_column(ForeignKey("thing.id"))

        class Mixin:
            id: Mapped[int] = mapped_column(primary_key=True)

            @declared_attr
            def users(self) -> Mapped[List[User]]:
                return relationship(User)

            if use_directive:
                if use_annotation:

                    @declared_attr.directive
                    def user_ids(self) -> AssociationProxy[List[int]]:
                        return association_proxy("users", "id")

                else:

                    @declared_attr.directive
                    def user_ids(self):
                        return association_proxy("users", "id")

            else:
                if use_annotation:

                    @declared_attr
                    def user_ids(self) -> AssociationProxy[List[int]]:
                        return association_proxy("users", "id")

                else:

                    @declared_attr
                    def user_ids(self):
                        return association_proxy("users", "id")

        class Thing(Mixin, decl_base):
            __tablename__ = "thing"

        t1 = Thing()
        t1.users.extend([User(id=1), User(id=2)])
        eq_(t1.user_ids, [1, 2])


class RelationshipLHSTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "default"

    @testing.fixture
    def decl_base(self):
        class Base(DeclarativeBase):
            pass

        yield Base
        Base.registry.dispose()

    @testing.combinations(
        (Relationship, CollectionAttributeImpl),
        (Mapped, CollectionAttributeImpl),
        (WriteOnlyMapped, WriteOnlyAttributeImpl),
        (DynamicMapped, DynamicAttributeImpl),
        argnames="mapped_cls,implcls",
    )
    def test_use_relationship(self, decl_base, mapped_cls, implcls):
        """test #10611"""

        # anno only: global B

        class B(decl_base):
            __tablename__ = "b"
            id: Mapped[int] = mapped_column(Integer, primary_key=True)
            a_id: Mapped[int] = mapped_column(ForeignKey("a.id"))

        class A(decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True)

            # for future annotations support, need to write these
            # directly in source code
            if mapped_cls is Relationship:
                bs: Relationship[List[B]] = relationship()
            elif mapped_cls is Mapped:
                bs: Mapped[List[B]] = relationship()
            elif mapped_cls is WriteOnlyMapped:
                bs: WriteOnlyMapped[List[B]] = relationship()
            elif mapped_cls is DynamicMapped:
                bs: DynamicMapped[List[B]] = relationship()

        decl_base.registry.configure()
        assert isinstance(A.bs.impl, implcls)

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

    def test_legacy_dataclasses_not_currently_using_annotations(
        self, registry
    ):
        """test if relationship() inspects annotations when using
        the legacy dataclass style.

        As of #8692, we are not looking at any annotations that don't use
        ``Mapped[]``.   dataclass users should use MappedAsDataclass and
        new conventions.

        """

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

        with expect_raises_message(
            ArgumentError,
            "relationship 'bs' expects a class or a mapper argument",
        ):
            registry.configure()

    @testing.variation(
        "datatype",
        [
            "typing_sequence",
            ("collections_sequence", testing.requires.python310),
            "typing_mutable_sequence",
            ("collections_mutable_sequence", testing.requires.python310),
        ],
    )
    @testing.variation("include_explicit", [True, False])
    def test_relationship_abstract_cls_error(
        self, decl_base, datatype, include_explicit
    ):
        """test #9100"""

        class B(decl_base):
            __tablename__ = "b"
            id: Mapped[int] = mapped_column(primary_key=True)
            a_id: Mapped[int] = mapped_column(ForeignKey("a.id"))
            data: Mapped[str]

        if include_explicit:

            class A(decl_base):
                __tablename__ = "a"

                id: Mapped[int] = mapped_column(primary_key=True)

                # note this can be done more succinctly by assigning to
                # an interim type, however making it explicit here
                # allows us to further test de-stringifying of these
                # collection types
                if datatype.typing_sequence:
                    bs: Mapped[typing.Sequence[B]] = relationship(
                        collection_class=list
                    )
                elif datatype.collections_sequence:
                    bs: Mapped[collections.abc.Sequence[B]] = relationship(
                        collection_class=list
                    )
                elif datatype.typing_mutable_sequence:
                    bs: Mapped[typing.MutableSequence[B]] = relationship(
                        collection_class=list
                    )
                elif datatype.collections_mutable_sequence:
                    bs: Mapped[collections.abc.MutableSequence[B]] = (
                        relationship(collection_class=list)
                    )
                else:
                    datatype.fail()

            decl_base.registry.configure()
            self.assert_compile(
                select(A).join(A.bs),
                "SELECT a.id FROM a JOIN b ON a.id = b.a_id",
            )
        else:
            with expect_raises_message(
                sa_exc.ArgumentError,
                r"Collection annotation type "
                r".*Sequence.* cannot be "
                r"instantiated; please provide an explicit "
                r"'collection_class' parameter \(e.g. list, set, etc.\) to "
                r"the relationship\(\) function to accompany this annotation",
            ):

                class A(decl_base):
                    __tablename__ = "a"

                    id: Mapped[int] = mapped_column(primary_key=True)

                    if datatype.typing_sequence:
                        bs: Mapped[typing.Sequence[B]] = relationship()
                    elif datatype.collections_sequence:
                        bs: Mapped[collections.abc.Sequence[B]] = (
                            relationship()
                        )
                    elif datatype.typing_mutable_sequence:
                        bs: Mapped[typing.MutableSequence[B]] = relationship()
                    elif datatype.collections_mutable_sequence:
                        bs: Mapped[collections.abc.MutableSequence[B]] = (
                            relationship()
                        )
                    else:
                        datatype.fail()

                decl_base.registry.configure()

    @testing.variation(
        "collection_type",
        [
            ("list", testing.requires.python310),
            "List",
            ("set", testing.requires.python310),
            "Set",
        ],
    )
    def test_14_style_anno_accepted_w_allow_unmapped(self, collection_type):
        """test for #8692 and #10385"""

        class Base(DeclarativeBase):
            __allow_unmapped__ = True

        class A(Base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True)
            data: str = Column(String)

            if collection_type.list:
                bs: list["B"] = relationship(  # noqa: F821
                    "B",
                    back_populates="a",
                )
            elif collection_type.List:
                bs: List["B"] = relationship(  # noqa: F821
                    "B",
                    back_populates="a",
                )
            elif collection_type.set:
                bs: set["B"] = relationship(  # noqa: F821
                    "B",
                    back_populates="a",
                )
            elif collection_type.Set:
                bs: Set["B"] = relationship(  # noqa: F821
                    "B",
                    back_populates="a",
                )
            else:
                collection_type.fail()

        class B(Base):
            __tablename__ = "b"
            id: Mapped[int] = mapped_column(primary_key=True)
            a_id: Mapped[int] = mapped_column(ForeignKey("a.id"))
            data: Mapped[str]
            a: A = relationship("A", back_populates="bs")

        Base.registry.configure()

        self.assert_compile(
            select(A).join(A.bs),
            "SELECT a.id, a.data FROM a JOIN b ON a.id = b.a_id",
        )
        self.assert_compile(
            select(B).join(B.a),
            "SELECT b.id, b.a_id, b.data FROM b JOIN a ON a.id = b.a_id",
        )

    @testing.combinations(
        ("not_optional",),
        ("optional",),
        ("optional_fwd_ref",),
        ("union_none",),
        ("pep604", testing.requires.python310),
        argnames="optional_on_m2o",
    )
    def test_basic_bidirectional(self, decl_base, optional_on_m2o):
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

            if optional_on_m2o == "optional":
                a: Mapped[Optional["A"]] = relationship(
                    back_populates="bs", primaryjoin=a_id == A.id
                )
            elif optional_on_m2o == "optional_fwd_ref":
                a: Mapped["Optional[A]"] = relationship(
                    back_populates="bs", primaryjoin=a_id == A.id
                )
            elif optional_on_m2o == "union_none":
                a: Mapped["Union[A, None]"] = relationship(
                    back_populates="bs", primaryjoin=a_id == A.id
                )
            elif optional_on_m2o == "pep604":
                a: Mapped[A | None] = relationship(
                    back_populates="bs", primaryjoin=a_id == A.id
                )
            else:
                a: Mapped["A"] = relationship(
                    back_populates="bs", primaryjoin=a_id == A.id
                )

        a1 = A(data="data")
        b1 = B()
        a1.bs.append(b1)
        is_(a1, b1.a)

    def test_wrong_annotation_type_one(self, decl_base):
        with expect_annotation_syntax_error("A.data"):

            class A(decl_base):
                __tablename__ = "a"

                id: Mapped[int] = mapped_column(primary_key=True)
                data: "B" = relationship()  # type: ignore  # noqa

    def test_wrong_annotation_type_two(self, decl_base):
        with expect_annotation_syntax_error("A.data"):

            class B(decl_base):
                __tablename__ = "b"

                id: Mapped[int] = mapped_column(primary_key=True)

            class A(decl_base):
                __tablename__ = "a"

                id: Mapped[int] = mapped_column(primary_key=True)
                data: B = relationship()  # type: ignore  # noqa

    def test_wrong_annotation_type_three(self, decl_base):
        with expect_annotation_syntax_error("A.data"):

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

    def test_one_to_one_example_quoted(self, decl_base: Type[DeclarativeBase]):
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

    def test_one_to_one_example_non_quoted(
        self, decl_base: Type[DeclarativeBase]
    ):
        """test example in the relationship docs will derive uselist=False
        correctly"""

        class Child(decl_base):
            __tablename__ = "child"

            id: Mapped[int] = mapped_column(primary_key=True)
            parent_id: Mapped[int] = mapped_column(ForeignKey("parent.id"))
            parent: Mapped["Parent"] = relationship(back_populates="child")

        class Parent(decl_base):
            __tablename__ = "parent"

            id: Mapped[int] = mapped_column(primary_key=True)
            child: Mapped[Child] = relationship(  # noqa: F821
                back_populates="parent"
            )

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

    @testing.combinations(
        "include_relationship",
        "no_relationship",
        argnames="include_relationship",
    )
    @testing.combinations(
        "direct_name", "indirect_name", argnames="indirect_name"
    )
    def test_indirect_name_collection(
        self, decl_base, include_relationship, indirect_name
    ):
        """test #8759"""
        # anno only: global B_

        class B(decl_base):
            __tablename__ = "b"
            id: Mapped[int] = mapped_column(Integer, primary_key=True)
            a_id: Mapped[int] = mapped_column(ForeignKey("a.id"))

        B_ = B

        class A(decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True)
            data: Mapped[str] = mapped_column()

            if indirect_name == "indirect_name":
                if include_relationship == "include_relationship":
                    bs: Mapped[List[B_]] = relationship("B")
                else:
                    bs: Mapped[List[B_]] = relationship()
            else:
                if include_relationship == "include_relationship":
                    bs: Mapped[List[B]] = relationship("B")
                else:
                    bs: Mapped[List[B]] = relationship()

        self.assert_compile(
            select(A).join(A.bs),
            "SELECT a.id, a.data FROM a JOIN b ON a.id = b.a_id",
        )

    @testing.combinations(
        "include_relationship",
        "no_relationship",
        argnames="include_relationship",
    )
    @testing.combinations(
        "direct_name", "indirect_name", argnames="indirect_name"
    )
    def test_indirect_name_scalar(
        self, decl_base, include_relationship, indirect_name
    ):
        """test #8759"""
        # anno only: global A_

        class A(decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True)
            data: Mapped[str] = mapped_column()

        A_ = A

        class B(decl_base):
            __tablename__ = "b"
            id: Mapped[int] = mapped_column(Integer, primary_key=True)
            a_id: Mapped[int] = mapped_column(ForeignKey("a.id"))

            if indirect_name == "indirect_name":
                if include_relationship == "include_relationship":
                    a: Mapped[A_] = relationship("A")
                else:
                    a: Mapped[A_] = relationship()
            else:
                if include_relationship == "include_relationship":
                    a: Mapped[A] = relationship("A")
                else:
                    a: Mapped[A] = relationship()

        self.assert_compile(
            select(B).join(B.a),
            "SELECT b.id, b.a_id FROM b JOIN a ON a.id = b.a_id",
        )


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

    def test_annotated_setup(self, decl_base):
        global Address

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
                mapped_column(), mapped_column(), mapped_column("zip")
            )

        self.assert_compile(
            select(User),
            'SELECT "user".id, "user".name, "user".street, '
            '"user".state, "user".zip FROM "user"',
            dialect="default",
        )

    def test_fwd_ref_plus_no_mapped(self, decl_base):
        @dataclasses.dataclass
        class Address:
            street: str
            state: str
            zip_: str

        with expect_annotation_syntax_error("User.address"):

            class User(decl_base):
                __tablename__ = "user"

                id: Mapped[int] = mapped_column(primary_key=True)
                name: Mapped[str] = mapped_column()

                address: "Address" = composite(  # type: ignore
                    mapped_column(), mapped_column(), mapped_column("zip")
                )

    def test_extract_from_pep593(self, decl_base):
        # anno only: global Address

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
        # anno only: global Address

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
        # anno only: global Address

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
        # anno only: global Address

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
        # anno only: global Address

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
        # anno only: global str50, str30, opt_str50
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

    @testing.variation("anno_type", ["plain", "typemap", "annotated"])
    @testing.variation("inh_type", ["single", "joined"])
    def test_mixin_interp_on_inh(self, decl_base, inh_type, anno_type):
        # anno only: global anno_col

        if anno_type.typemap:
            anno_col = Annotated[str, 30]

            decl_base.registry.update_type_annotation_map({anno_col: String})

            class Mixin:
                foo: Mapped[anno_col]

        elif anno_type.annotated:
            anno_col = Annotated[str, mapped_column(String)]

            class Mixin:
                foo: Mapped[anno_col]

        else:

            class Mixin:
                foo: Mapped[str]

        class Employee(Mixin, decl_base):
            __tablename__ = "employee"

            id: Mapped[int] = mapped_column(primary_key=True)
            name: Mapped[str]
            type: Mapped[str]

            __mapper_args__ = {
                "polymorphic_on": "type",
                "polymorphic_identity": "employee",
            }

        class Manager(Employee):
            if inh_type.joined:
                __tablename__ = "manager"

                id: Mapped[int] = mapped_column(  # noqa: A001
                    ForeignKey("employee.id"), primary_key=True
                )

            manager_data: Mapped[str] = mapped_column(nullable=True)

            __mapper_args__ = {
                "polymorphic_identity": "manager",
            }

        if inh_type.single:
            self.assert_compile(
                select(Manager),
                "SELECT employee.id, employee.name, employee.type, "
                "employee.foo, employee.manager_data FROM employee "
                "WHERE employee.type IN (__[POSTCOMPILE_type_1])",
            )
        elif inh_type.joined:
            self.assert_compile(
                select(Manager),
                "SELECT manager.id, employee.id AS id_1, employee.name, "
                "employee.type, employee.foo, manager.manager_data "
                "FROM employee JOIN manager ON employee.id = manager.id",
            )
        else:
            inh_type.fail()


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


class GenericMappingQueryTest(AssertsCompiledSQL, fixtures.TestBase):
    """test the Generic support added as part of #8665"""

    __dialect__ = "default"

    @testing.fixture
    def mapping(self):
        # anno only: global T_Value
        T_Value = TypeVar("T_Value")

        class SomeBaseClass(DeclarativeBase):
            pass

        class GenericSetting(
            MappedAsDataclass, SomeBaseClass, Generic[T_Value]
        ):
            """Represents key value pairs for settings or values"""

            __tablename__ = "xx"

            id: Mapped[int] = mapped_column(
                Integer, primary_key=True, init=False
            )

            key: Mapped[str] = mapped_column(String, init=True)

            value: Mapped[T_Value] = mapped_column(
                MutableDict.as_mutable(JSON),
                init=True,
                default_factory=lambda: {},
            )

        return GenericSetting

    def test_inspect(self, mapping):
        GenericSetting = mapping

        typ = GenericSetting[Dict[str, Any]]
        is_(inspect(typ), GenericSetting.__mapper__)

    def test_select(self, mapping):
        GenericSetting = mapping

        typ = GenericSetting[Dict[str, Any]]
        self.assert_compile(
            select(typ).where(typ.key == "x"),
            "SELECT xx.id, xx.key, xx.value FROM xx WHERE xx.key = :key_1",
        )


class BackendTests(fixtures.TestBase):
    __backend__ = True

    @testing.variation("native_enum", [True, False])
    @testing.variation("include_column", [True, False])
    @testing.variation("python_type", ["enum", "literal"])
    def test_schema_type_actually_works(
        self,
        connection,
        decl_base,
        include_column,
        native_enum,
        python_type: Variation,
    ):
        """test that schema type bindings are set up correctly"""

        # anno only: global Status

        if python_type.enum:

            class Status(enum.Enum):
                PENDING = "pending"
                RECEIVED = "received"
                COMPLETED = "completed"

            enum_argument = [Status]
            test_value = Status.RECEIVED
        elif python_type.literal:
            Status = Literal[  # type: ignore
                "pending", "received", "completed"
            ]
            enum_argument = ["pending", "received", "completed"]
            test_value = "received"
        else:
            python_type.fail()

        if not include_column and not native_enum:
            decl_base.registry.update_type_annotation_map(
                {
                    enum.Enum: Enum(enum.Enum, native_enum=False),
                    Literal: Enum(enum.Enum, native_enum=False),
                }
            )

        class SomeClass(decl_base):
            __tablename__ = "some_table"

            id: Mapped[int] = mapped_column(primary_key=True)

            if include_column:
                status: Mapped[Status] = mapped_column(
                    Enum(
                        *enum_argument,
                        native_enum=bool(native_enum),
                        name="status",
                    )
                )
            else:
                status: Mapped[Status]

        decl_base.metadata.create_all(connection)

        with Session(connection) as sess:
            sess.add(SomeClass(id=1, status=test_value))
            sess.commit()

            eq_(
                sess.scalars(
                    select(SomeClass.status).where(SomeClass.id == 1)
                ).first(),
                test_value,
            )
