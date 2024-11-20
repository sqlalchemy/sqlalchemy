import contextlib
import dataclasses
from dataclasses import InitVar
import functools
import inspect as pyinspect
from itertools import product
from typing import Any
from typing import ClassVar
from typing import Dict
from typing import Generic
from typing import List
from typing import Optional
from typing import Set
from typing import Type
from typing import TypeVar
from unittest import mock

from typing_extensions import Annotated

from sqlalchemy import BigInteger
from sqlalchemy import Column
from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import JSON
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import column_property
from sqlalchemy.orm import composite
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import declared_attr
from sqlalchemy.orm import deferred
from sqlalchemy.orm import interfaces
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import MappedAsDataclass
from sqlalchemy.orm import MappedColumn
from sqlalchemy.orm import query_expression
from sqlalchemy.orm import registry
from sqlalchemy.orm import registry as _RegistryType
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm import synonym
from sqlalchemy.sql.base import _NoArg
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import eq_regex
from sqlalchemy.testing import expect_deprecated
from sqlalchemy.testing import expect_raises
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_false
from sqlalchemy.testing import is_true
from sqlalchemy.testing import ne_
from sqlalchemy.testing import Variation
from sqlalchemy.util import compat


def _dataclass_mixin_warning(clsname, attrnames):
    return testing.expect_deprecated(
        rf"When transforming .* to a dataclass, attribute\(s\) "
        rf"{attrnames} originates from superclass .*{clsname}"
    )


class DCTransformsTest(AssertsCompiledSQL, fixtures.TestBase):
    @testing.fixture(params=["(MAD, DB)", "(DB, MAD)"])
    def dc_decl_base(self, request, metadata):
        _md = metadata

        if request.param == "(MAD, DB)":

            class Base(MappedAsDataclass, DeclarativeBase):
                _mad_before = True
                metadata = _md
                type_annotation_map = {
                    str: String().with_variant(String(50), "mysql", "mariadb")
                }

        else:
            # test #8665 by reversing the order of the classes
            class Base(DeclarativeBase, MappedAsDataclass):
                _mad_before = False
                metadata = _md
                type_annotation_map = {
                    str: String().with_variant(String(50), "mysql", "mariadb")
                }

        yield Base
        Base.registry.dispose()

    def test_basic_constructor_repr_base_cls(
        self, dc_decl_base: Type[MappedAsDataclass]
    ):
        class A(dc_decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True, init=False)
            data: Mapped[str]

            x: Mapped[Optional[int]] = mapped_column(default=None)

            bs: Mapped[List["B"]] = relationship(  # noqa: F821
                default_factory=list
            )

        class B(dc_decl_base):
            __tablename__ = "b"

            id: Mapped[int] = mapped_column(primary_key=True, init=False)
            data: Mapped[str]
            a_id: Mapped[Optional[int]] = mapped_column(
                ForeignKey("a.id"), init=False
            )
            x: Mapped[Optional[int]] = mapped_column(default=None)

        A.__qualname__ = "some_module.A"
        B.__qualname__ = "some_module.B"

        eq_(
            pyinspect.getfullargspec(A.__init__),
            pyinspect.FullArgSpec(
                args=["self", "data", "x", "bs"],
                varargs=None,
                varkw=None,
                defaults=(None, mock.ANY),
                kwonlyargs=[],
                kwonlydefaults=None,
                annotations={},
            ),
        )
        eq_(
            pyinspect.getfullargspec(B.__init__),
            pyinspect.FullArgSpec(
                args=["self", "data", "x"],
                varargs=None,
                varkw=None,
                defaults=(None,),
                kwonlyargs=[],
                kwonlydefaults=None,
                annotations={},
            ),
        )

        a2 = A("10", x=5, bs=[B("data1"), B("data2", x=12)])
        eq_(
            repr(a2),
            "some_module.A(id=None, data='10', x=5, "
            "bs=[some_module.B(id=None, data='data1', a_id=None, x=None), "
            "some_module.B(id=None, data='data2', a_id=None, x=12)])",
        )

        a3 = A("data")
        eq_(repr(a3), "some_module.A(id=None, data='data', x=None, bs=[])")

    def test_generic_class(self):
        """further test for #8665"""

        T_Value = TypeVar("T_Value")

        class SomeBaseClass(DeclarativeBase):
            pass

        class GenericSetting(
            MappedAsDataclass, SomeBaseClass, Generic[T_Value]
        ):
            __tablename__ = "xx"

            id: Mapped[int] = mapped_column(
                Integer, primary_key=True, init=False
            )

            key: Mapped[str] = mapped_column(String, init=True)

            value: Mapped[T_Value] = mapped_column(
                JSON, init=True, default_factory=lambda: {}
            )

        new_instance: GenericSetting[Dict[str, Any]] = (  # noqa: F841
            GenericSetting(key="x", value={"foo": "bar"})
        )

    def test_no_anno_doesnt_go_into_dc(
        self, dc_decl_base: Type[MappedAsDataclass]
    ):
        class User(dc_decl_base):
            __tablename__: ClassVar[Optional[str]] = "user"

            id: Mapped[int] = mapped_column(primary_key=True, init=False)
            username: Mapped[str]
            password: Mapped[str]
            addresses: Mapped[List["Address"]] = relationship(  # noqa: F821
                default_factory=list
            )

        class Address(dc_decl_base):
            __tablename__: ClassVar[Optional[str]] = "address"

            id: Mapped[int] = mapped_column(primary_key=True, init=False)

            # should not be in the dataclass constructor
            user_id = mapped_column(ForeignKey(User.id))

            email_address: Mapped[str]

        a1 = Address("email@address")
        eq_(a1.email_address, "email@address")

    def test_warn_on_non_dc_mixin(self):
        class _BaseMixin:
            create_user: Mapped[int] = mapped_column()
            update_user: Mapped[Optional[int]] = mapped_column(
                default=None, init=False
            )

        class Base(DeclarativeBase, MappedAsDataclass, _BaseMixin):
            pass

        class SubMixin:
            foo: Mapped[str]
            bar: Mapped[str] = mapped_column()

        with (
            _dataclass_mixin_warning(
                "_BaseMixin", "'create_user', 'update_user'"
            ),
            _dataclass_mixin_warning("SubMixin", "'foo', 'bar'"),
        ):

            class User(SubMixin, Base):
                __tablename__ = "sys_user"

                id: Mapped[int] = mapped_column(primary_key=True, init=False)
                username: Mapped[str] = mapped_column(String)
                password: Mapped[str] = mapped_column(String)

    def test_basic_constructor_repr_cls_decorator(
        self, registry: _RegistryType
    ):
        @registry.mapped_as_dataclass()
        class A:
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True, init=False)
            data: Mapped[str]

            x: Mapped[Optional[int]] = mapped_column(default=None)

            bs: Mapped[List["B"]] = relationship(  # noqa: F821
                default_factory=list
            )

        @registry.mapped_as_dataclass()
        class B:
            __tablename__ = "b"

            id: Mapped[int] = mapped_column(primary_key=True, init=False)
            a_id = mapped_column(ForeignKey("a.id"), init=False)
            data: Mapped[str]
            x: Mapped[Optional[int]] = mapped_column(default=None)

        A.__qualname__ = "some_module.A"
        B.__qualname__ = "some_module.B"

        eq_(
            pyinspect.getfullargspec(A.__init__),
            pyinspect.FullArgSpec(
                args=["self", "data", "x", "bs"],
                varargs=None,
                varkw=None,
                defaults=(None, mock.ANY),
                kwonlyargs=[],
                kwonlydefaults=None,
                annotations={},
            ),
        )
        eq_(
            pyinspect.getfullargspec(B.__init__),
            pyinspect.FullArgSpec(
                args=["self", "data", "x"],
                varargs=None,
                varkw=None,
                defaults=(None,),
                kwonlyargs=[],
                kwonlydefaults=None,
                annotations={},
            ),
        )

        a2 = A("10", x=5, bs=[B("data1"), B("data2", x=12)])

        # note a_id isn't included because it wasn't annotated
        eq_(
            repr(a2),
            "some_module.A(id=None, data='10', x=5, "
            "bs=[some_module.B(id=None, data='data1', x=None), "
            "some_module.B(id=None, data='data2', x=12)])",
        )

        a3 = A("data")
        eq_(repr(a3), "some_module.A(id=None, data='data', x=None, bs=[])")

    @testing.variation("dc_type", ["decorator", "superclass"])
    def test_dataclass_fn(self, dc_type: Variation):
        annotations = {}

        def dc_callable(kls, **kw) -> Type[Any]:
            annotations[kls] = kls.__annotations__
            return dataclasses.dataclass(kls, **kw)  # type: ignore

        if dc_type.decorator:
            reg = registry()

            @reg.mapped_as_dataclass(dataclass_callable=dc_callable)
            class MappedClass:
                __tablename__ = "mapped_class"

                id: Mapped[int] = mapped_column(primary_key=True)
                name: Mapped[str]

            eq_(annotations, {MappedClass: {"id": int, "name": str}})

        elif dc_type.superclass:

            class Base(DeclarativeBase):
                pass

            class Mixin(MappedAsDataclass, dataclass_callable=dc_callable):
                id: Mapped[int] = mapped_column(primary_key=True)

            class MappedClass(Mixin, Base):
                __tablename__ = "mapped_class"
                name: Mapped[str]

            eq_(
                annotations,
                {Mixin: {"id": int}, MappedClass: {"id": int, "name": str}},
            )
        else:
            dc_type.fail()

    def test_default_fn(self, dc_decl_base: Type[MappedAsDataclass]):
        class A(dc_decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True, init=False)
            data: Mapped[str] = mapped_column(default="d1")
            data2: Mapped[str] = mapped_column(default_factory=lambda: "d2")

        a1 = A()
        eq_(a1.data, "d1")
        eq_(a1.data2, "d2")

    def test_default_factory_vs_collection_class(
        self, dc_decl_base: Type[MappedAsDataclass]
    ):
        # this is currently the error raised by dataclasses.  We can instead
        # do this validation ourselves, but overall I don't know that we
        # can hit every validation and rule that's in dataclasses
        with expect_raises_message(
            ValueError, "cannot specify both default and default_factory"
        ):

            class A(dc_decl_base):
                __tablename__ = "a"

                id: Mapped[int] = mapped_column(primary_key=True, init=False)
                data: Mapped[str] = mapped_column(
                    default="d1", default_factory=lambda: "d2"
                )

    def test_combine_args_from_pep593(self, decl_base: Type[DeclarativeBase]):
        """test that we can set up column-level defaults separate from
        dataclass defaults

        """
        intpk = Annotated[int, mapped_column(primary_key=True)]
        str30 = Annotated[
            str, mapped_column(String(30), insert_default=func.foo())
        ]
        s_str30 = Annotated[
            str,
            mapped_column(String(30), server_default="some server default"),
        ]
        user_fk = Annotated[int, mapped_column(ForeignKey("user_account.id"))]

        class User(MappedAsDataclass, decl_base):
            __tablename__ = "user_account"

            # we need this case for dataclasses that can't derive things
            # from Annotated yet at the typing level
            id: Mapped[intpk] = mapped_column(init=False)
            name_none: Mapped[Optional[str30]] = mapped_column(default=None)
            name: Mapped[str30] = mapped_column(default="hi")
            name2: Mapped[s_str30] = mapped_column(default="there")
            addresses: Mapped[List["Address"]] = relationship(  # noqa: F821
                back_populates="user", default_factory=list
            )

        class Address(MappedAsDataclass, decl_base):
            __tablename__ = "address"

            id: Mapped[intpk] = mapped_column(init=False)
            email_address: Mapped[str]
            user_id: Mapped[user_fk] = mapped_column(init=False)
            user: Mapped[Optional["User"]] = relationship(
                back_populates="addresses", default=None
            )

        is_true(User.__table__.c.id.primary_key)
        is_true(User.__table__.c.name_none.default.arg.compare(func.foo()))
        is_true(User.__table__.c.name.default.arg.compare(func.foo()))
        eq_(User.__table__.c.name2.server_default.arg, "some server default")

        is_true(Address.__table__.c.user_id.references(User.__table__.c.id))
        u1 = User()
        eq_(u1.name_none, None)
        eq_(u1.name, "hi")
        eq_(u1.name2, "there")

    def test_inheritance(self, dc_decl_base: Type[MappedAsDataclass]):
        class Person(dc_decl_base):
            __tablename__ = "person"
            person_id: Mapped[int] = mapped_column(
                primary_key=True, init=False
            )
            name: Mapped[str]
            type: Mapped[str] = mapped_column(init=False)

            __mapper_args__ = {"polymorphic_on": type}

        class Engineer(Person):
            __tablename__ = "engineer"

            person_id: Mapped[int] = mapped_column(
                ForeignKey("person.person_id"), primary_key=True, init=False
            )

            status: Mapped[str] = mapped_column(String(30))
            engineer_name: Mapped[str]
            primary_language: Mapped[str]
            __mapper_args__ = {"polymorphic_identity": "engineer"}

        e1 = Engineer("nm", "st", "en", "pl")
        eq_(e1.name, "nm")
        eq_(e1.status, "st")
        eq_(e1.engineer_name, "en")
        eq_(e1.primary_language, "pl")

    def test_non_mapped_fields_wo_mapped_or_dc(
        self, dc_decl_base: Type[MappedAsDataclass]
    ):
        class A(dc_decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True, init=False)
            data: str
            ctrl_one: str = dataclasses.field()
            some_field: int = dataclasses.field(default=5)

        a1 = A("data", "ctrl_one", 5)
        eq_(
            dataclasses.asdict(a1),
            {
                "ctrl_one": "ctrl_one",
                "data": "data",
                "id": None,
                "some_field": 5,
            },
        )

    def test_non_mapped_fields_wo_mapped_or_dc_w_inherits(
        self, dc_decl_base: Type[MappedAsDataclass]
    ):
        class A(dc_decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True, init=False)
            data: str
            ctrl_one: str = dataclasses.field()
            some_field: int = dataclasses.field(default=5)

        class B(A):
            b_data: Mapped[str] = mapped_column(default="bd")

        # ensure we didnt break dataclasses contract of removing Field
        # issue #8880
        eq_(A.__dict__["some_field"], 5)
        assert "ctrl_one" not in A.__dict__

        b1 = B(data="data", ctrl_one="ctrl_one", some_field=5, b_data="x")
        eq_(
            dataclasses.asdict(b1),
            {
                "ctrl_one": "ctrl_one",
                "data": "data",
                "id": None,
                "some_field": 5,
                "b_data": "x",
            },
        )

    def test_init_var(self, dc_decl_base: Type[MappedAsDataclass]):
        class User(dc_decl_base):
            __tablename__ = "user_account"

            id: Mapped[int] = mapped_column(init=False, primary_key=True)
            name: Mapped[str]

            password: InitVar[str]
            repeat_password: InitVar[str]

            password_hash: Mapped[str] = mapped_column(
                init=False, nullable=False
            )

            def __post_init__(self, password: str, repeat_password: str):
                if password != repeat_password:
                    raise ValueError("passwords do not match")

                self.password_hash = f"some hash... {password}"

        u1 = User(name="u1", password="p1", repeat_password="p1")
        eq_(u1.password_hash, "some hash... p1")
        self.assert_compile(
            select(User),
            "SELECT user_account.id, user_account.name, "
            "user_account.password_hash FROM user_account",
        )

    def test_integrated_dc(self, dc_decl_base: Type[MappedAsDataclass]):
        """We will be telling users "this is a dataclass that is also
        mapped". Therefore, they will want *any* kind of attribute to do what
        it would normally do in a dataclass, including normal types without any
        field and explicit use of dataclasses.field(). additionally, we'd like
        ``Mapped`` to mean "persist this attribute". So the absence of
        ``Mapped`` should also mean something too.

        """

        class A(dc_decl_base):
            __tablename__ = "a"

            ctrl_one: str = dataclasses.field()

            id: Mapped[int] = mapped_column(primary_key=True, init=False)
            data: Mapped[str]
            some_field: int = dataclasses.field(default=5)

            some_none_field: Optional[str] = dataclasses.field(default=None)

            some_other_int_field: int = 10

        # some field is part of the constructor
        a1 = A("ctrlone", "datafield")
        eq_(
            dataclasses.asdict(a1),
            {
                "ctrl_one": "ctrlone",
                "data": "datafield",
                "id": None,
                "some_field": 5,
                "some_none_field": None,
                "some_other_int_field": 10,
            },
        )

        a2 = A(
            "ctrlone",
            "datafield",
            some_field=7,
            some_other_int_field=12,
            some_none_field="x",
        )
        eq_(
            dataclasses.asdict(a2),
            {
                "ctrl_one": "ctrlone",
                "data": "datafield",
                "id": None,
                "some_field": 7,
                "some_none_field": "x",
                "some_other_int_field": 12,
            },
        )

        # only Mapped[] is mapped
        self.assert_compile(select(A), "SELECT a.id, a.data FROM a")
        eq_(
            pyinspect.getfullargspec(A.__init__),
            pyinspect.FullArgSpec(
                args=[
                    "self",
                    "ctrl_one",
                    "data",
                    "some_field",
                    "some_none_field",
                    "some_other_int_field",
                ],
                varargs=None,
                varkw=None,
                defaults=(5, None, 10),
                kwonlyargs=[],
                kwonlydefaults=None,
                annotations={},
            ),
        )

    def test_dc_on_top_of_non_dc(self, decl_base: Type[DeclarativeBase]):
        class Person(decl_base):
            __tablename__ = "person"
            person_id: Mapped[int] = mapped_column(primary_key=True)
            name: Mapped[str]
            type: Mapped[str] = mapped_column()

            __mapper_args__ = {"polymorphic_on": type}

        class Engineer(MappedAsDataclass, Person):
            __tablename__ = "engineer"

            person_id: Mapped[int] = mapped_column(
                ForeignKey("person.person_id"), primary_key=True, init=False
            )

            status: Mapped[str] = mapped_column(String(30))
            engineer_name: Mapped[str]
            primary_language: Mapped[str]
            __mapper_args__ = {"polymorphic_identity": "engineer"}

        e1 = Engineer("st", "en", "pl")
        eq_(e1.status, "st")
        eq_(e1.engineer_name, "en")
        eq_(e1.primary_language, "pl")

        eq_(
            pyinspect.getfullargspec(Person.__init__),
            # the boring **kw __init__
            pyinspect.FullArgSpec(
                args=["self"],
                varargs=None,
                varkw="kwargs",
                defaults=None,
                kwonlyargs=[],
                kwonlydefaults=None,
                annotations={},
            ),
        )

        eq_(
            pyinspect.getfullargspec(Engineer.__init__),
            # the exciting dataclasses __init__
            pyinspect.FullArgSpec(
                args=["self", "status", "engineer_name", "primary_language"],
                varargs=None,
                varkw=None,
                defaults=None,
                kwonlyargs=[],
                kwonlydefaults=None,
                annotations={},
            ),
        )

    def test_compare(self, dc_decl_base: Type[MappedAsDataclass]):
        class A(dc_decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True, compare=False)
            data: Mapped[str]

        a1 = A(id=0, data="foo")
        a2 = A(id=1, data="foo")
        eq_(a1, a2)

    @testing.requires.python310
    def test_kw_only_attribute(self, dc_decl_base: Type[MappedAsDataclass]):
        class A(dc_decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True)
            data: Mapped[str] = mapped_column(kw_only=True)

        fas = pyinspect.getfullargspec(A.__init__)
        eq_(fas.args, ["self", "id"])
        eq_(fas.kwonlyargs, ["data"])

    @testing.combinations(True, False, argnames="unsafe_hash")
    def test_hash_attribute(
        self, dc_decl_base: Type[MappedAsDataclass], unsafe_hash
    ):
        class A(dc_decl_base, unsafe_hash=unsafe_hash):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True, hash=False)
            data: Mapped[str] = mapped_column(hash=True)

        a = A(id=1, data="x")
        if not unsafe_hash or not dc_decl_base._mad_before:
            with expect_raises(TypeError):
                a_hash1 = hash(a)
        else:
            a_hash1 = hash(a)
            a.id = 41
            eq_(hash(a), a_hash1)
            a.data = "y"
            ne_(hash(a), a_hash1)

    @testing.requires.python310
    def test_kw_only_dataclass_constant(
        self, dc_decl_base: Type[MappedAsDataclass]
    ):
        class Mixin(MappedAsDataclass):
            a: Mapped[int] = mapped_column(primary_key=True)
            b: Mapped[int] = mapped_column(default=1)

        class Child(Mixin, dc_decl_base):
            __tablename__ = "child"

            _: dataclasses.KW_ONLY
            c: Mapped[int]

        c1 = Child(1, c=5)
        eq_(c1, Child(a=1, b=1, c=5))

    def test_mapped_column_overrides(self, dc_decl_base):
        """test #8688"""

        class TriggeringMixin(MappedAsDataclass):
            mixin_value: Mapped[int] = mapped_column(BigInteger)

        class NonTriggeringMixin(MappedAsDataclass):
            mixin_value: Mapped[int]

        class Foo(dc_decl_base, TriggeringMixin):
            __tablename__ = "foo"
            id: Mapped[int] = mapped_column(primary_key=True, init=False)
            foo_value: Mapped[float] = mapped_column(default=78)

        class Bar(dc_decl_base, NonTriggeringMixin):
            __tablename__ = "bar"
            id: Mapped[int] = mapped_column(primary_key=True, init=False)
            bar_value: Mapped[float] = mapped_column(default=78)

        f1 = Foo(mixin_value=5)
        eq_(f1.foo_value, 78)

        b1 = Bar(mixin_value=5)
        eq_(b1.bar_value, 78)

    def test_mixing_MappedAsDataclass_with_decorator_raises(self, registry):
        """test #9211"""

        class Mixin(MappedAsDataclass):
            id: Mapped[int] = mapped_column(primary_key=True, init=False)

        with expect_raises_message(
            exc.InvalidRequestError,
            "Class .*Foo.* is already a dataclass; ensure that "
            "base classes / decorator styles of establishing dataclasses "
            "are not being mixed. ",
        ):

            @registry.mapped_as_dataclass
            class Foo(Mixin):
                bar_value: Mapped[float] = mapped_column(default=78)

    def test_MappedAsDataclass_table_provided(self, registry):
        """test #11973"""

        with expect_raises_message(
            exc.InvalidRequestError,
            "Class .*Foo.* already defines a '__table__'. "
            "ORM Annotated Dataclasses do not support a pre-existing "
            "'__table__' element",
        ):

            @registry.mapped_as_dataclass
            class Foo:
                __table__ = Table("foo", registry.metadata)
                foo: Mapped[float]

    def test_dataclass_exception_wrapped(self, dc_decl_base):
        with expect_raises_message(
            exc.InvalidRequestError,
            r"Python dataclasses error encountered when creating dataclass "
            r"for \'Foo\': .*Please refer to Python dataclasses.*",
        ) as ec:

            class Foo(dc_decl_base):
                id: Mapped[int] = mapped_column(primary_key=True, init=False)
                foo_value: Mapped[float] = mapped_column(default=78)
                foo_no_value: Mapped[float] = mapped_column()
                __tablename__ = "foo"

        is_true(isinstance(ec.error.__cause__, TypeError))

    def test_dataclass_default(self, dc_decl_base):
        """test for #9879"""

        def c10():
            return 10

        def c20():
            return 20

        class A(dc_decl_base):
            __tablename__ = "a"
            id: Mapped[int] = mapped_column(primary_key=True)
            def_init: Mapped[int] = mapped_column(default=42)
            call_init: Mapped[int] = mapped_column(default_factory=c10)
            def_no_init: Mapped[int] = mapped_column(default=13, init=False)
            call_no_init: Mapped[int] = mapped_column(
                default_factory=c20, init=False
            )

        a = A(id=100)
        eq_(a.def_init, 42)
        eq_(a.call_init, 10)
        eq_(a.def_no_init, 13)
        eq_(a.call_no_init, 20)

        fields = {f.name: f for f in dataclasses.fields(A)}
        eq_(fields["def_init"].default, 42)
        eq_(fields["call_init"].default_factory, c10)
        eq_(fields["def_no_init"].default, dataclasses.MISSING)
        ne_(fields["def_no_init"].default_factory, dataclasses.MISSING)
        eq_(fields["call_no_init"].default_factory, c20)

    def test_dataclass_default_callable(self, dc_decl_base):
        """test for #9936"""

        def cd():
            return 42

        with expect_deprecated(
            "Callable object passed to the ``default`` parameter for "
            "attribute 'value' in a ORM-mapped Dataclasses context is "
            "ambiguous, and this use will raise an error in a future "
            "release.  If this callable is intended to produce Core level ",
            "Callable object passed to the ``default`` parameter for "
            "attribute 'no_init' in a ORM-mapped Dataclasses context is "
            "ambiguous, and this use will raise an error in a future "
            "release.  If this callable is intended to produce Core level ",
        ):

            class A(dc_decl_base):
                __tablename__ = "a"
                id: Mapped[int] = mapped_column(primary_key=True)
                value: Mapped[int] = mapped_column(default=cd)
                no_init: Mapped[int] = mapped_column(default=cd, init=False)

        a = A(id=100)
        is_false("no_init" in a.__dict__)
        eq_(a.value, cd)
        eq_(a.no_init, None)

        fields = {f.name: f for f in dataclasses.fields(A)}
        eq_(fields["value"].default, cd)
        eq_(fields["no_init"].default, cd)


class RelationshipDefaultFactoryTest(fixtures.TestBase):
    def test_list(self, dc_decl_base: Type[MappedAsDataclass]):
        class A(dc_decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True, init=False)

            bs: Mapped[List["B"]] = relationship(  # noqa: F821
                default_factory=lambda: [B(data="hi")]
            )

        class B(dc_decl_base):
            __tablename__ = "b"

            id: Mapped[int] = mapped_column(primary_key=True, init=False)
            a_id = mapped_column(ForeignKey("a.id"), init=False)
            data: Mapped[str]

        a1 = A()
        eq_(a1.bs[0].data, "hi")

    def test_set(self, dc_decl_base: Type[MappedAsDataclass]):
        class A(dc_decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True, init=False)

            bs: Mapped[Set["B"]] = relationship(  # noqa: F821
                default_factory=lambda: {B(data="hi")}
            )

        class B(dc_decl_base, unsafe_hash=True):
            __tablename__ = "b"

            id: Mapped[int] = mapped_column(primary_key=True, init=False)
            a_id = mapped_column(ForeignKey("a.id"), init=False)
            data: Mapped[str]

        a1 = A()
        eq_(a1.bs.pop().data, "hi")

    def test_oh_no_mismatch(self, dc_decl_base: Type[MappedAsDataclass]):
        class A(dc_decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True, init=False)

            bs: Mapped[Set["B"]] = relationship(  # noqa: F821
                default_factory=lambda: [B(data="hi")]
            )

        class B(dc_decl_base, unsafe_hash=True):
            __tablename__ = "b"

            id: Mapped[int] = mapped_column(primary_key=True, init=False)
            a_id = mapped_column(ForeignKey("a.id"), init=False)
            data: Mapped[str]

        # old school collection mismatch error FTW
        with expect_raises_message(
            TypeError, "Incompatible collection type: list is not set-like"
        ):
            A()

    def test_one_to_one_example(self, dc_decl_base: Type[MappedAsDataclass]):
        """test example in the relationship docs will derive uselist=False
        correctly"""

        class Parent(dc_decl_base):
            __tablename__ = "parent"

            id: Mapped[int] = mapped_column(init=False, primary_key=True)
            child: Mapped["Child"] = relationship(  # noqa: F821
                back_populates="parent", default=None
            )

        class Child(dc_decl_base):
            __tablename__ = "child"

            id: Mapped[int] = mapped_column(init=False, primary_key=True)
            parent_id: Mapped[int] = mapped_column(
                ForeignKey("parent.id"), init=False
            )
            parent: Mapped["Parent"] = relationship(
                back_populates="child", default=None
            )

        c1 = Child()
        p1 = Parent(child=c1)
        is_(p1.child, c1)
        is_(c1.parent, p1)

        p2 = Parent()
        is_(p2.child, None)

    def test_replace_operation_works_w_history_etc(
        self, registry: _RegistryType
    ):
        @registry.mapped_as_dataclass
        class A:
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True, init=False)
            data: Mapped[str]

            x: Mapped[Optional[int]] = mapped_column(default=None)

            bs: Mapped[List["B"]] = relationship(  # noqa: F821
                default_factory=list
            )

        @registry.mapped_as_dataclass
        class B:
            __tablename__ = "b"

            id: Mapped[int] = mapped_column(primary_key=True, init=False)
            a_id = mapped_column(ForeignKey("a.id"), init=False)
            data: Mapped[str]
            x: Mapped[Optional[int]] = mapped_column(default=None)

        registry.metadata.create_all(testing.db)

        with Session(testing.db) as sess:
            a1 = A("data", 10, [B("b1"), B("b2", x=5), B("b3")])
            sess.add(a1)
            sess.commit()

            a2 = dataclasses.replace(a1, x=12, bs=[B("b4")])

            assert a1 in sess
            assert not sess.is_modified(a1, include_collections=True)
            assert a2 not in sess
            eq_(inspect(a2).attrs.x.history, ([12], (), ()))
            sess.add(a2)
            sess.commit()

            eq_(sess.scalars(select(A.x).order_by(A.id)).all(), [10, 12])
            eq_(
                sess.scalars(select(B.data).order_by(B.id)).all(),
                ["b1", "b2", "b3", "b4"],
            )

    def test_post_init(self, registry: _RegistryType):
        @registry.mapped_as_dataclass
        class A:
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True, init=False)
            data: Mapped[str] = mapped_column(init=False)

            def __post_init__(self):
                self.data = "some data"

        a1 = A()
        eq_(a1.data, "some data")

    def test_no_field_args_w_new_style(self, registry: _RegistryType):
        with expect_raises_message(
            exc.InvalidRequestError,
            "SQLAlchemy mapped dataclasses can't consume mapping information",
        ):

            @registry.mapped_as_dataclass()
            class A:
                __tablename__ = "a"
                __sa_dataclass_metadata_key__ = "sa"

                account_id: int = dataclasses.field(
                    init=False,
                    metadata={"sa": Column(Integer, primary_key=True)},
                )

    def test_no_field_args_w_new_style_two(self, registry: _RegistryType):
        @dataclasses.dataclass
        class Base:
            pass

        with expect_raises_message(
            exc.InvalidRequestError,
            "SQLAlchemy mapped dataclasses can't consume mapping information",
        ):

            @registry.mapped_as_dataclass()
            class A(Base):
                __tablename__ = "a"
                __sa_dataclass_metadata_key__ = "sa"

                account_id: int = dataclasses.field(
                    init=False,
                    metadata={"sa": Column(Integer, primary_key=True)},
                )


class DataclassesForNonMappedClassesTest(fixtures.TestBase):
    """test for cases added in #9179"""

    def test_base_is_dc(self):
        class Parent(MappedAsDataclass, DeclarativeBase):
            a: int

        class Child(Parent):
            __tablename__ = "child"
            b: Mapped[int] = mapped_column(primary_key=True)

        eq_regex(repr(Child(5, 6)), r".*\.Child\(a=5, b=6\)")

    def test_base_is_dc_plus_options(self):
        class Parent(MappedAsDataclass, DeclarativeBase, unsafe_hash=True):
            a: int

        class Child(Parent, repr=False):
            __tablename__ = "child"
            b: Mapped[int] = mapped_column(primary_key=True)

        c1 = Child(5, 6)
        eq_(hash(c1), hash(Child(5, 6)))

        # still reprs, because base has a repr, but b not included
        eq_regex(repr(c1), r".*\.Child\(a=5\)")

    def test_base_is_dc_init_var(self):
        class Parent(MappedAsDataclass, DeclarativeBase):
            a: InitVar[int]

        class Child(Parent):
            __tablename__ = "child"
            b: Mapped[int] = mapped_column(primary_key=True)

        c1 = Child(a=5, b=6)
        eq_regex(repr(c1), r".*\.Child\(b=6\)")

    def test_base_is_dc_field(self):
        class Parent(MappedAsDataclass, DeclarativeBase):
            a: int = dataclasses.field(default=10)

        class Child(Parent):
            __tablename__ = "child"
            b: Mapped[int] = mapped_column(primary_key=True, default=7)

        c1 = Child(a=5, b=6)
        eq_regex(repr(c1), r".*\.Child\(a=5, b=6\)")

        c1 = Child(b=6)
        eq_regex(repr(c1), r".*\.Child\(a=10, b=6\)")

        c1 = Child()
        eq_regex(repr(c1), r".*\.Child\(a=10, b=7\)")

    def test_abstract_and_base_is_dc(self):
        class Parent(MappedAsDataclass, DeclarativeBase):
            a: int

        class Mixin(Parent):
            __abstract__ = True
            b: int

        class Child(Mixin):
            __tablename__ = "child"
            c: Mapped[int] = mapped_column(primary_key=True)

        eq_regex(repr(Child(5, 6, 7)), r".*\.Child\(a=5, b=6, c=7\)")

    def test_abstract_and_base_is_dc_plus_options(self):
        class Parent(MappedAsDataclass, DeclarativeBase):
            a: int

        class Mixin(Parent, unsafe_hash=True):
            __abstract__ = True
            b: int

        class Child(Mixin, repr=False):
            __tablename__ = "child"
            c: Mapped[int] = mapped_column(primary_key=True)

        eq_(hash(Child(5, 6, 7)), hash(Child(5, 6, 7)))

        eq_regex(repr(Child(5, 6, 7)), r".*\.Child\(a=5, b=6\)")

    def test_abstract_and_base_is_dc_init_var(self):
        class Parent(MappedAsDataclass, DeclarativeBase):
            a: InitVar[int]

        class Mixin(Parent):
            __abstract__ = True
            b: InitVar[int]

        class Child(Mixin):
            __tablename__ = "child"
            c: Mapped[int] = mapped_column(primary_key=True)

        c1 = Child(a=5, b=6, c=7)
        eq_regex(repr(c1), r".*\.Child\(c=7\)")

    def test_abstract_and_base_is_dc_field(self):
        class Parent(MappedAsDataclass, DeclarativeBase):
            a: int = dataclasses.field(default=10)

        class Mixin(Parent):
            __abstract__ = True
            b: int = dataclasses.field(default=7)

        class Child(Mixin):
            __tablename__ = "child"
            c: Mapped[int] = mapped_column(primary_key=True, default=9)

        c1 = Child(b=6, c=7)
        eq_regex(repr(c1), r".*\.Child\(a=10, b=6, c=7\)")

        c1 = Child()
        eq_regex(repr(c1), r".*\.Child\(a=10, b=7, c=9\)")

    def test_abstract_is_dc(self):
        collected_annotations = {}

        def check_args(cls, **kw):
            collected_annotations[cls] = cls.__annotations__
            return dataclasses.dataclass(cls, **kw)

        class Parent(DeclarativeBase):
            a: int

        class Mixin(MappedAsDataclass, Parent, dataclass_callable=check_args):
            __abstract__ = True
            b: int

        class Child(Mixin):
            __tablename__ = "child"
            c: Mapped[int] = mapped_column(primary_key=True)

        eq_(collected_annotations, {Mixin: {"b": int}, Child: {"c": int}})
        eq_regex(repr(Child(6, 7)), r".*\.Child\(b=6, c=7\)")

    @testing.variation("check_annotations", [True, False])
    def test_abstract_is_dc_w_mapped(self, check_annotations):
        if check_annotations:
            collected_annotations = {}

            def check_args(cls, **kw):
                collected_annotations[cls] = cls.__annotations__
                return dataclasses.dataclass(cls, **kw)

            class_kw = {"dataclass_callable": check_args}
        else:
            class_kw = {}

        class Parent(DeclarativeBase):
            a: int

        class Mixin(MappedAsDataclass, Parent, **class_kw):
            __abstract__ = True
            b: Mapped[int] = mapped_column()

        class Child(Mixin):
            __tablename__ = "child"
            c: Mapped[int] = mapped_column(primary_key=True)

        if check_annotations:
            # note: current dataclasses process adds Field() object to Child
            # based on attributes which include those from Mixin.  This means
            # the annotations of Child are also augmented while we do
            # dataclasses collection.
            eq_(
                collected_annotations,
                {Mixin: {"b": int}, Child: {"b": int, "c": int}},
            )
        eq_regex(repr(Child(6, 7)), r".*\.Child\(b=6, c=7\)")

    def test_mixin_and_base_is_dc(self):
        class Parent(MappedAsDataclass, DeclarativeBase):
            a: int

        @dataclasses.dataclass
        class Mixin:
            b: int

        class Child(Mixin, Parent):
            __tablename__ = "child"
            c: Mapped[int] = mapped_column(primary_key=True)

        eq_regex(repr(Child(5, 6, 7)), r".*\.Child\(a=5, b=6, c=7\)")

    def test_mixin_and_base_is_dc_init_var(self):
        class Parent(MappedAsDataclass, DeclarativeBase):
            a: InitVar[int]

        @dataclasses.dataclass
        class Mixin:
            b: InitVar[int]

        class Child(Mixin, Parent):
            __tablename__ = "child"
            c: Mapped[int] = mapped_column(primary_key=True)

        eq_regex(repr(Child(a=5, b=6, c=7)), r".*\.Child\(c=7\)")

    @testing.variation(
        "dataclass_scope",
        ["on_base", "on_mixin", "on_base_class", "on_sub_class"],
    )
    @testing.variation(
        "test_alternative_callable",
        [True, False],
    )
    def test_mixin_w_inheritance(
        self, dataclass_scope, test_alternative_callable
    ):
        """test #9226"""

        expected_annotations = {}

        if test_alternative_callable:
            collected_annotations = {}

            def check_args(cls, **kw):
                collected_annotations[cls] = getattr(
                    cls, "__annotations__", {}
                )
                return dataclasses.dataclass(cls, **kw)

            klass_kw = {"dataclass_callable": check_args}
        else:
            klass_kw = {}

        if dataclass_scope.on_base:

            class Base(MappedAsDataclass, DeclarativeBase, **klass_kw):
                pass

            expected_annotations[Base] = {}
        else:

            class Base(DeclarativeBase):
                pass

        if dataclass_scope.on_mixin:

            class Mixin(MappedAsDataclass, **klass_kw):
                @declared_attr.directive
                @classmethod
                def __tablename__(cls) -> str:
                    return cls.__name__.lower()

                @declared_attr.directive
                @classmethod
                def __mapper_args__(cls) -> Dict[str, Any]:
                    return {
                        "polymorphic_identity": cls.__name__,
                        "polymorphic_on": "polymorphic_type",
                    }

                @declared_attr
                @classmethod
                def polymorphic_type(cls) -> Mapped[str]:
                    return mapped_column(
                        String,
                        insert_default=cls.__name__,
                        init=False,
                    )

            expected_annotations[Mixin] = {}

            non_dc_mixin = contextlib.nullcontext

        else:

            class Mixin:
                @declared_attr.directive
                @classmethod
                def __tablename__(cls) -> str:
                    return cls.__name__.lower()

                @declared_attr.directive
                @classmethod
                def __mapper_args__(cls) -> Dict[str, Any]:
                    return {
                        "polymorphic_identity": cls.__name__,
                        "polymorphic_on": "polymorphic_type",
                    }

                if dataclass_scope.on_base or dataclass_scope.on_base_class:

                    @declared_attr
                    @classmethod
                    def polymorphic_type(cls) -> Mapped[str]:
                        return mapped_column(
                            String,
                            insert_default=cls.__name__,
                            init=False,
                        )

                else:

                    @declared_attr
                    @classmethod
                    def polymorphic_type(cls) -> Mapped[str]:
                        return mapped_column(
                            String,
                            insert_default=cls.__name__,
                        )

            non_dc_mixin = functools.partial(
                _dataclass_mixin_warning, "Mixin", "'polymorphic_type'"
            )

        if dataclass_scope.on_base_class:
            with non_dc_mixin():

                class Book(Mixin, MappedAsDataclass, Base, **klass_kw):
                    id: Mapped[int] = mapped_column(
                        Integer,
                        primary_key=True,
                        init=False,
                    )

        else:
            if dataclass_scope.on_base:
                local_non_dc_mixin = non_dc_mixin
            else:
                local_non_dc_mixin = contextlib.nullcontext

            with local_non_dc_mixin():

                class Book(Mixin, Base):
                    if not dataclass_scope.on_sub_class:
                        id: Mapped[int] = mapped_column(  # noqa: A001
                            Integer, primary_key=True, init=False
                        )
                    else:
                        id: Mapped[int] = mapped_column(  # noqa: A001
                            Integer,
                            primary_key=True,
                        )

        if MappedAsDataclass in Book.__mro__:
            expected_annotations[Book] = {"id": int, "polymorphic_type": str}

        if dataclass_scope.on_sub_class:
            with non_dc_mixin():

                class Novel(MappedAsDataclass, Book, **klass_kw):
                    id: Mapped[int] = mapped_column(  # noqa: A001
                        ForeignKey("book.id"),
                        primary_key=True,
                        init=False,
                    )
                    description: Mapped[Optional[str]]

        else:
            with non_dc_mixin():

                class Novel(Book):
                    id: Mapped[int] = mapped_column(
                        ForeignKey("book.id"),
                        primary_key=True,
                        init=False,
                    )
                    description: Mapped[Optional[str]]

        expected_annotations[Novel] = {"id": int, "description": Optional[str]}

        if test_alternative_callable:
            eq_(collected_annotations, expected_annotations)

        n1 = Novel("the description")
        eq_(n1.description, "the description")


class DataclassArgsTest(fixtures.TestBase):
    dc_arg_names = ("init", "repr", "eq", "order", "unsafe_hash")
    if compat.py310:
        dc_arg_names += ("match_args", "kw_only")

    @testing.fixture(params=product(dc_arg_names, (True, False)))
    def dc_argument_fixture(self, request: Any, registry: _RegistryType):
        name, use_defaults = request.param

        args = {n: n == name for n in self.dc_arg_names}
        if args["order"]:
            args["eq"] = True
        if use_defaults:
            default = {
                "init": True,
                "repr": True,
                "eq": True,
                "order": False,
                "unsafe_hash": False,
            }
            if compat.py310:
                default |= {"match_args": True, "kw_only": False}
            to_apply = {k: v for k, v in args.items() if v}
            effective = {**default, **to_apply}
            return to_apply, effective
        else:
            return args, args

    @testing.fixture(params=["mapped_column", "synonym", "deferred"])
    def mapped_expr_constructor(self, request):
        name = request.param

        if name == "mapped_column":
            yield mapped_column(default=7, init=True)
        elif name == "synonym":
            yield synonym("some_int", default=7, init=True)
        elif name == "deferred":
            yield deferred(Column(Integer), default=7, init=True)

    def test_attrs_rejected_if_not_a_dc(
        self, mapped_expr_constructor, decl_base: Type[DeclarativeBase]
    ):
        if isinstance(mapped_expr_constructor, MappedColumn):
            unwanted_args = "'init'"
        else:
            unwanted_args = "'default', 'init'"
        with expect_raises_message(
            exc.ArgumentError,
            r"Attribute 'x' on class .*A.* includes dataclasses "
            r"argument\(s\): "
            rf"{unwanted_args} but class does not specify SQLAlchemy native "
            "dataclass configuration",
        ):

            class A(decl_base):
                __tablename__ = "a"

                id: Mapped[int] = mapped_column(primary_key=True)

                x: Mapped[int] = mapped_expr_constructor

    def _assert_cls(self, cls, dc_arguments):
        if dc_arguments["init"]:

            def create(data, x):
                if dc_arguments.get("kw_only"):
                    return cls(data=data, x=x)
                else:
                    return cls(data, x)

        else:

            def create(data, x):
                a1 = cls()
                a1.data = data
                a1.x = x
                return a1

        for n in self.dc_arg_names:
            if dc_arguments[n]:
                getattr(self, f"_assert_{n}")(cls, create, dc_arguments)
            else:
                getattr(self, f"_assert_not_{n}")(cls, create, dc_arguments)

            if dc_arguments["init"]:
                a1 = cls(data="some data")
                eq_(a1.x, 7)

        a1 = create("some data", 15)
        some_int = a1.some_int
        eq_(
            dataclasses.asdict(a1),
            {"data": "some data", "id": None, "some_int": some_int, "x": 15},
        )
        eq_(dataclasses.astuple(a1), (None, "some data", some_int, 15))

    def _assert_unsafe_hash(self, cls, create, dc_arguments):
        a1 = create("d1", 5)
        hash(a1)

    def _assert_not_unsafe_hash(self, cls, create, dc_arguments):
        a1 = create("d1", 5)

        if dc_arguments["eq"]:
            with expect_raises(TypeError):
                hash(a1)
        else:
            hash(a1)

    def _assert_eq(self, cls, create, dc_arguments):
        a1 = create("d1", 5)
        a2 = create("d2", 10)
        a3 = create("d1", 5)

        eq_(a1, a3)
        ne_(a1, a2)

    def _assert_not_eq(self, cls, create, dc_arguments):
        a1 = create("d1", 5)
        a2 = create("d2", 10)
        a3 = create("d1", 5)

        eq_(a1, a1)
        ne_(a1, a3)
        ne_(a1, a2)

    def _assert_order(self, cls, create, dc_arguments):
        is_false(create("g", 10) < create("b", 7))

        is_true(create("g", 10) > create("b", 7))

        is_false(create("g", 10) <= create("b", 7))

        is_true(create("g", 10) >= create("b", 7))

        eq_(
            list(sorted([create("g", 10), create("g", 5), create("b", 7)])),
            [
                create("b", 7),
                create("g", 5),
                create("g", 10),
            ],
        )

    def _assert_not_order(self, cls, create, dc_arguments):
        with expect_raises(TypeError):
            create("g", 10) < create("b", 7)

        with expect_raises(TypeError):
            create("g", 10) > create("b", 7)

        with expect_raises(TypeError):
            create("g", 10) <= create("b", 7)

        with expect_raises(TypeError):
            create("g", 10) >= create("b", 7)

    def _assert_repr(self, cls, create, dc_arguments):
        assert "__repr__" in cls.__dict__
        a1 = create("some data", 12)
        eq_regex(repr(a1), r".*A\(id=None, data='some data', x=12\)")

    def _assert_not_repr(self, cls, create, dc_arguments):
        assert "__repr__" not in cls.__dict__

        # if a superclass has __repr__, then we still get repr.
        # so can't test this
        # a1 = create("some data", 12)
        # eq_regex(repr(a1), r"<.*A object at 0x.*>")

    def _assert_init(self, cls, create, dc_arguments):
        if not dc_arguments.get("kw_only", False):
            a1 = cls("some data", 5)

            eq_(a1.data, "some data")
            eq_(a1.x, 5)

        a2 = cls(data="some data", x=5)
        eq_(a2.data, "some data")
        eq_(a2.x, 5)

        a3 = cls(data="some data")
        eq_(a3.data, "some data")
        eq_(a3.x, 7)

    def _assert_not_init(self, cls, create, dc_arguments):
        with expect_raises(TypeError):
            cls("Some data", 5)

        # we run real "dataclasses" on the class.  so with init=False, it
        # doesn't touch what was there, and the SQLA default constructor
        # gets put on.
        a1 = cls(data="some data")
        eq_(a1.data, "some data")
        eq_(a1.x, None)

        a1 = cls()
        eq_(a1.data, None)

        # no constructor, it sets None for x...ok
        eq_(a1.x, None)

    def _assert_match_args(self, cls, create, dc_arguments):
        if not dc_arguments["kw_only"]:
            is_true(len(cls.__match_args__) > 0)

    def _assert_not_match_args(self, cls, create, dc_arguments):
        is_false(hasattr(cls, "__match_args__"))

    def _assert_kw_only(self, cls, create, dc_arguments):
        if dc_arguments["init"]:
            fas = pyinspect.getfullargspec(cls.__init__)
            eq_(fas.args, ["self"])
            eq_(
                len(fas.kwonlyargs),
                len(pyinspect.signature(cls.__init__).parameters) - 1,
            )

    def _assert_not_kw_only(self, cls, create, dc_arguments):
        if dc_arguments["init"]:
            fas = pyinspect.getfullargspec(cls.__init__)
            eq_(
                len(fas.args),
                len(pyinspect.signature(cls.__init__).parameters),
            )
            eq_(fas.kwonlyargs, [])

    def test_dc_arguments_decorator(
        self,
        dc_argument_fixture,
        mapped_expr_constructor,
        registry: _RegistryType,
    ):
        @registry.mapped_as_dataclass(**dc_argument_fixture[0])
        class A:
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True, init=False)
            data: Mapped[str]

            some_int: Mapped[int] = mapped_column(init=False, repr=False)

            x: Mapped[Optional[int]] = mapped_expr_constructor

        self._assert_cls(A, dc_argument_fixture[1])

    def test_dc_arguments_base(
        self,
        dc_argument_fixture,
        mapped_expr_constructor,
        registry: _RegistryType,
    ):
        reg = registry

        class Base(
            MappedAsDataclass, DeclarativeBase, **dc_argument_fixture[0]
        ):
            registry = reg

        class A(Base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True, init=False)
            data: Mapped[str]

            some_int: Mapped[int] = mapped_column(init=False, repr=False)

            x: Mapped[Optional[int]] = mapped_expr_constructor

        self._assert_cls(A, dc_argument_fixture[1])

    def test_dc_arguments_perclass(
        self,
        dc_argument_fixture,
        mapped_expr_constructor,
        decl_base: Type[DeclarativeBase],
    ):
        class A(MappedAsDataclass, decl_base, **dc_argument_fixture[0]):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True, init=False)
            data: Mapped[str]

            some_int: Mapped[int] = mapped_column(init=False, repr=False)

            x: Mapped[Optional[int]] = mapped_expr_constructor

        self._assert_cls(A, dc_argument_fixture[1])

    def test_dc_arguments_override_base(self, registry: _RegistryType):
        reg = registry

        class Base(MappedAsDataclass, DeclarativeBase, init=False, order=True):
            registry = reg

        class A(Base, init=True, repr=False):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True, init=False)
            data: Mapped[str]

            some_int: Mapped[int] = mapped_column(init=False, repr=False)

            x: Mapped[Optional[int]] = mapped_column(default=7)

        effective = {
            "init": True,
            "repr": False,
            "eq": True,
            "order": True,
            "unsafe_hash": False,
        }
        if compat.py310:
            effective |= {"match_args": True, "kw_only": False}
        self._assert_cls(A, effective)

    def test_dc_base_unsupported_argument(self, registry: _RegistryType):
        reg = registry
        with expect_raises(TypeError):

            class Base(MappedAsDataclass, DeclarativeBase, slots=True):
                registry = reg

        class Base2(MappedAsDataclass, DeclarativeBase, order=True):
            registry = reg

        with expect_raises(TypeError):

            class A(Base2, slots=False):
                __tablename__ = "a"

                id: Mapped[int] = mapped_column(primary_key=True, init=False)

    def test_dc_decorator_unsupported_argument(self, registry: _RegistryType):
        reg = registry
        with expect_raises(TypeError):

            @registry.mapped_as_dataclass(slots=True)
            class Base(DeclarativeBase):
                registry = reg

        class Base2(MappedAsDataclass, DeclarativeBase, order=True):
            registry = reg

        with expect_raises(TypeError):

            @registry.mapped_as_dataclass(slots=True)
            class A(Base2):
                __tablename__ = "a"

                id: Mapped[int] = mapped_column(primary_key=True, init=False)

    def test_dc_raise_for_slots(
        self,
        registry: _RegistryType,
        decl_base: Type[DeclarativeBase],
    ):
        reg = registry
        with expect_raises_message(
            exc.ArgumentError,
            r"Dataclass argument\(s\) 'slots', 'unknown' are not accepted",
        ):

            class A(MappedAsDataclass, decl_base):
                __tablename__ = "a"
                _sa_apply_dc_transforms = {"slots": True, "unknown": 5}

                id: Mapped[int] = mapped_column(primary_key=True, init=False)

        with expect_raises_message(
            exc.ArgumentError,
            r"Dataclass argument\(s\) 'slots' are not accepted",
        ):

            class Base(MappedAsDataclass, DeclarativeBase, order=True):
                registry = reg
                _sa_apply_dc_transforms = {"slots": True}

        with expect_raises_message(
            exc.ArgumentError,
            r"Dataclass argument\(s\) 'slots', 'unknown' are not accepted",
        ):

            @reg.mapped
            class C:
                __tablename__ = "a"
                _sa_apply_dc_transforms = {"slots": True, "unknown": 5}

                id: Mapped[int] = mapped_column(primary_key=True, init=False)

    @testing.variation("use_arguments", [True, False])
    @testing.combinations(
        mapped_column,
        lambda **kw: synonym("some_int", **kw),
        lambda **kw: deferred(Column(Integer), **kw),
        lambda **kw: composite("foo", **kw),
        lambda **kw: relationship("Foo", **kw),
        lambda **kw: association_proxy("foo", "bar", **kw),
        argnames="construct",
    )
    def test_attribute_options(self, use_arguments, construct):
        if use_arguments:
            kw = {
                "init": False,
                "repr": False,
                "default": False,
                "default_factory": list,
                "compare": True,
                "kw_only": False,
                "hash": False,
            }
            exp = interfaces._AttributeOptions(
                False, False, False, list, True, False, False
            )
        else:
            kw = {}
            exp = interfaces._DEFAULT_ATTRIBUTE_OPTIONS

        prop = construct(**kw)
        eq_(prop._attribute_options, exp)

    @testing.variation("use_arguments", [True, False])
    @testing.combinations(
        lambda **kw: column_property(Column(Integer), **kw),
        lambda **kw: query_expression(**kw),
        argnames="construct",
    )
    def test_ro_attribute_options(self, use_arguments, construct):
        if use_arguments:
            kw = {
                "repr": False,
                "compare": True,
            }
            exp = interfaces._AttributeOptions(
                False,
                False,
                _NoArg.NO_ARG,
                _NoArg.NO_ARG,
                True,
                _NoArg.NO_ARG,
                _NoArg.NO_ARG,
            )
        else:
            kw = {}
            exp = interfaces._DEFAULT_READONLY_ATTRIBUTE_OPTIONS

        prop = construct(**kw)
        eq_(prop._attribute_options, exp)


class MixinColumnTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    """tests for #8718"""

    __dialect__ = "default"

    @testing.fixture
    def model(self):
        def go(use_mixin, use_inherits, mad_setup, dataclass_kw):
            if use_mixin:
                if mad_setup == "dc, mad":

                    class BaseEntity(
                        DeclarativeBase, MappedAsDataclass, **dataclass_kw
                    ):
                        pass

                elif mad_setup == "mad, dc":

                    class BaseEntity(
                        MappedAsDataclass, DeclarativeBase, **dataclass_kw
                    ):
                        pass

                elif mad_setup == "subclass":

                    class BaseEntity(DeclarativeBase):
                        pass

                class IdMixin(MappedAsDataclass):
                    id: Mapped[int] = mapped_column(
                        primary_key=True, init=False
                    )

                if mad_setup == "subclass":

                    class A(
                        IdMixin, MappedAsDataclass, BaseEntity, **dataclass_kw
                    ):
                        __mapper_args__ = {
                            "polymorphic_on": "type",
                            "polymorphic_identity": "a",
                        }

                        __tablename__ = "a"
                        type: Mapped[str] = mapped_column(String, init=False)
                        data: Mapped[str] = mapped_column(String, init=False)

                else:

                    class A(IdMixin, BaseEntity):
                        __mapper_args__ = {
                            "polymorphic_on": "type",
                            "polymorphic_identity": "a",
                        }

                        __tablename__ = "a"
                        type: Mapped[str] = mapped_column(String, init=False)
                        data: Mapped[str] = mapped_column(String, init=False)

            else:
                if mad_setup == "dc, mad":

                    class BaseEntity(
                        DeclarativeBase, MappedAsDataclass, **dataclass_kw
                    ):
                        id: Mapped[int] = mapped_column(
                            primary_key=True, init=False
                        )

                elif mad_setup == "mad, dc":

                    class BaseEntity(
                        MappedAsDataclass, DeclarativeBase, **dataclass_kw
                    ):
                        id: Mapped[int] = mapped_column(
                            primary_key=True, init=False
                        )

                elif mad_setup == "subclass":

                    class BaseEntity(MappedAsDataclass, DeclarativeBase):
                        id: Mapped[int] = mapped_column(
                            primary_key=True, init=False
                        )

                if mad_setup == "subclass":

                    class A(BaseEntity, **dataclass_kw):
                        __mapper_args__ = {
                            "polymorphic_on": "type",
                            "polymorphic_identity": "a",
                        }

                        __tablename__ = "a"
                        type: Mapped[str] = mapped_column(String, init=False)
                        data: Mapped[str] = mapped_column(String, init=False)

                else:

                    class A(BaseEntity):
                        __mapper_args__ = {
                            "polymorphic_on": "type",
                            "polymorphic_identity": "a",
                        }

                        __tablename__ = "a"
                        type: Mapped[str] = mapped_column(String, init=False)
                        data: Mapped[str] = mapped_column(String, init=False)

            if use_inherits:

                class B(A):
                    __mapper_args__ = {
                        "polymorphic_identity": "b",
                    }
                    b_data: Mapped[str] = mapped_column(String, init=False)

                return B
            else:
                return A

        yield go

    @testing.combinations("inherits", "plain", argnames="use_inherits")
    @testing.combinations("mixin", "base", argnames="use_mixin")
    @testing.combinations(
        "mad, dc", "dc, mad", "subclass", argnames="mad_setup"
    )
    def test_mapping(self, model, use_inherits, use_mixin, mad_setup):
        target_cls = model(
            use_inherits=use_inherits == "inherits",
            use_mixin=use_mixin == "mixin",
            mad_setup=mad_setup,
            dataclass_kw={},
        )

        obj = target_cls()
        assert "id" not in obj.__dict__


class CompositeTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "default"

    def test_composite_setup(self, dc_decl_base: Type[MappedAsDataclass]):
        @dataclasses.dataclass
        class Point:
            x: int
            y: int

        class Edge(dc_decl_base):
            __tablename__ = "edge"
            id: Mapped[int] = mapped_column(primary_key=True, init=False)
            graph_id: Mapped[int] = mapped_column(
                ForeignKey("graph.id"), init=False
            )

            start: Mapped[Point] = composite(
                Point, mapped_column("x1"), mapped_column("y1"), default=None
            )

            end: Mapped[Point] = composite(
                Point, mapped_column("x2"), mapped_column("y2"), default=None
            )

        class Graph(dc_decl_base):
            __tablename__ = "graph"
            id: Mapped[int] = mapped_column(primary_key=True, init=False)

            edges: Mapped[List[Edge]] = relationship()

        Point.__qualname__ = "mymodel.Point"
        Edge.__qualname__ = "mymodel.Edge"
        Graph.__qualname__ = "mymodel.Graph"
        g = Graph(
            edges=[
                Edge(start=Point(1, 2), end=Point(3, 4)),
                Edge(start=Point(7, 8), end=Point(5, 6)),
            ]
        )
        eq_(
            repr(g),
            "mymodel.Graph(id=None, edges=[mymodel.Edge(id=None, "
            "graph_id=None, start=mymodel.Point(x=1, y=2), "
            "end=mymodel.Point(x=3, y=4)), "
            "mymodel.Edge(id=None, graph_id=None, "
            "start=mymodel.Point(x=7, y=8), end=mymodel.Point(x=5, y=6))])",
        )

    def test_named_setup(self, dc_decl_base: Type[MappedAsDataclass]):
        @dataclasses.dataclass
        class Address:
            street: str
            state: str
            zip_: str

        class User(dc_decl_base):
            __tablename__ = "user"

            id: Mapped[int] = mapped_column(
                primary_key=True, init=False, repr=False
            )
            name: Mapped[str] = mapped_column()

            address: Mapped[Address] = composite(
                Address,
                mapped_column(),
                mapped_column(),
                mapped_column("zip"),
                default=None,
            )

        Address.__qualname__ = "mymodule.Address"
        User.__qualname__ = "mymodule.User"
        u = User(
            name="user 1",
            address=Address("123 anywhere street", "NY", "12345"),
        )
        u2 = User("u2")
        eq_(
            repr(u),
            "mymodule.User(name='user 1', "
            "address=mymodule.Address(street='123 anywhere street', "
            "state='NY', zip_='12345'))",
        )
        eq_(repr(u2), "mymodule.User(name='u2', address=None)")


class ReadOnlyAttrTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    """tests related to #9628"""

    __dialect__ = "default"

    @testing.combinations(
        (query_expression,), (column_property,), argnames="construct"
    )
    def test_default_behavior(
        self, dc_decl_base: Type[MappedAsDataclass], construct
    ):
        class MyClass(dc_decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True, init=False)
            data: Mapped[str] = mapped_column()

            const: Mapped[str] = construct(data + "asdf")

        m1 = MyClass(data="foo")
        eq_(m1, MyClass(data="foo"))
        ne_(m1, MyClass(data="bar"))

        eq_regex(
            repr(m1),
            r".*MyClass\(id=None, data='foo', const=None\)",
        )

    @testing.combinations(
        (query_expression,), (column_property,), argnames="construct"
    )
    def test_no_repr_behavior(
        self, dc_decl_base: Type[MappedAsDataclass], construct
    ):
        class MyClass(dc_decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True, init=False)
            data: Mapped[str] = mapped_column()

            const: Mapped[str] = construct(data + "asdf", repr=False)

        m1 = MyClass(data="foo")

        eq_regex(
            repr(m1),
            r".*MyClass\(id=None, data='foo'\)",
        )

    @testing.combinations(
        (query_expression,), (column_property,), argnames="construct"
    )
    def test_enable_compare(
        self, dc_decl_base: Type[MappedAsDataclass], construct
    ):
        class MyClass(dc_decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True, init=False)
            data: Mapped[str] = mapped_column()

            const: Mapped[str] = construct(data + "asdf", compare=True)

        m1 = MyClass(data="foo")
        eq_(m1, MyClass(data="foo"))
        ne_(m1, MyClass(data="bar"))

        m2 = MyClass(data="foo")
        m2.const = "some const"
        ne_(m2, MyClass(data="foo"))
        m3 = MyClass(data="foo")
        m3.const = "some const"
        eq_(m2, m3)
