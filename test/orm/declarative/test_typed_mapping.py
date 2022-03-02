import dataclasses
import datetime
from decimal import Decimal
from typing import Dict
from typing import Generic
from typing import List
from typing import Optional
from typing import Set
from typing import Type
from typing import TypeVar
from typing import Union

from sqlalchemy import BIGINT
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import exc as sa_exc
from sqlalchemy import ForeignKey
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import Numeric
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import VARCHAR
from sqlalchemy.exc import ArgumentError
from sqlalchemy.orm import as_declarative
from sqlalchemy.orm import composite
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import declared_attr
from sqlalchemy.orm import deferred
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.orm import undefer
from sqlalchemy.orm.collections import attribute_mapped_collection
from sqlalchemy.orm.collections import MappedCollection
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_false
from sqlalchemy.testing import is_not
from sqlalchemy.testing import is_true
from sqlalchemy.testing.fixtures import fixture_session
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
            a = Column(Integer, primary_key=True)

        eq_(Tab.foo, 1)
        is_(Tab.__table__, inspect(Tab).local_table)
        eq_(Tab.boring(), Tab)
        eq_(Tab.more_boring(), 27)

        with expect_raises(AttributeError):
            Tab.non_existent


class MappedColumnTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "default"

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

    def test_fwd_refs(self, decl_base: Type[DeclarativeBase]):
        class MyClass(decl_base):
            __tablename__ = "my_table"

            id: Mapped["int"] = mapped_column(primary_key=True)
            data_one: Mapped["str"]

    def test_annotated_types_as_keys(self, decl_base: Type[DeclarativeBase]):
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

        # ordering of cols is TODO
        eq_(A.__table__.c.keys(), ["id", "y", "name", "x"])

        self.assert_compile(select(A), "SELECT a.id, a.y, a.name, a.x FROM a")

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
            bs: Mapped[List["B"]] = relationship(  # noqa F821
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
            bs_list: Mapped[List["B"]] = relationship(  # noqa F821
                viewonly=True
            )
            bs_set: Mapped[Set["B"]] = relationship(viewonly=True)  # noqa F821
            bs_list_warg: Mapped[List["B"]] = relationship(  # noqa F821
                "B", viewonly=True
            )
            bs_set_warg: Mapped[Set["B"]] = relationship(  # noqa F821
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

        is_false(B.__mapper__.attrs["a"].uselist)
        is_false(B.__mapper__.attrs["a_warg"].uselist)

    def test_collection_class_dict_no_collection(self, decl_base):
        class A(decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True)
            data: Mapped[str] = mapped_column()
            bs: Mapped[Dict[str, "B"]] = relationship()  # noqa F821

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

            bs: Mapped[MappedCollection[str, "B"]] = relationship(  # noqa F821
                collection_class=attribute_mapped_collection("name")
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
            r"\"Mapped\['Address'\]\" or \"MappedColumn\['Address'\]\"",
        ):

            class User(decl_base):
                __tablename__ = "user"

                id: Mapped[int] = mapped_column(primary_key=True)
                name: Mapped[str] = mapped_column()

                address: "Address" = composite(  # type: ignore
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

            employees: Mapped[Set["Person"]] = relationship()  # noqa F821

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
