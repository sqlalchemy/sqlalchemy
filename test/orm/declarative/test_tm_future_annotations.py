from __future__ import annotations

from decimal import Decimal
from typing import List
from typing import Optional
from typing import Set
from typing import TYPE_CHECKING
from typing import TypeVar
from typing import Union
import uuid

from sqlalchemy import Column
from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import Numeric
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import Uuid
from sqlalchemy.orm import attribute_keyed_dict
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import DynamicMapped
from sqlalchemy.orm import KeyFuncDict
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.orm import WriteOnlyMapped
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_false
from sqlalchemy.testing import is_true
from sqlalchemy.util import compat
from .test_typed_mapping import expect_annotation_syntax_error
from .test_typed_mapping import MappedColumnTest as _MappedColumnTest
from .test_typed_mapping import RelationshipLHSTest as _RelationshipLHSTest
from .test_typed_mapping import (
    WriteOnlyRelationshipTest as _WriteOnlyRelationshipTest,
)

"""runs the annotation-sensitive tests from test_typed_mappings while
having ``from __future__ import annotations`` in effect.

"""


_R = TypeVar("_R")

M = Mapped


class M3:
    pass


class MappedColumnTest(_MappedColumnTest):
    def test_indirect_mapped_name_module_level(self, decl_base):
        """test #8759


        Note that M by definition has to be at the module level to be
        valid, and not locally declared here, this is in accordance with
        mypy::


            def make_class() -> None:
                ll = list

                x: ll[int] = [1, 2, 3]

        Will return::

            $ mypy test3.py
            test3.py:4: error: Variable "ll" is not valid as a type  [valid-type]
            test3.py:4: note: See https://mypy.readthedocs.io/en/stable/common_issues.html#variables-vs-type-aliases
            Found 1 error in 1 file (checked 1 source file)

        Whereas the correct form is::

            ll = list

            def make_class() -> None:

                x: ll[int] = [1, 2, 3]


        """  # noqa: E501

        class Foo(decl_base):
            __tablename__ = "foo"

            id: M[int] = mapped_column(primary_key=True)

            data: M[int] = mapped_column()

            data2: M[int]

        self.assert_compile(
            select(Foo), "SELECT foo.id, foo.data, foo.data2 FROM foo"
        )

    def test_indirect_mapped_name_local_level(self, decl_base):
        """test #8759.

        this should raise an error.

        """

        M2 = Mapped

        with expect_raises_message(
            exc.ArgumentError,
            r"Could not interpret annotation M2\[int\].  Check that it "
            "uses names that are correctly imported at the module level.",
        ):

            class Foo(decl_base):
                __tablename__ = "foo"

                id: M2[int] = mapped_column(primary_key=True)

                data2: M2[int]

    def test_indirect_mapped_name_itswrong(self, decl_base):
        """test #8759.

        this should raise an error.

        """

        with expect_annotation_syntax_error("Foo.id"):

            class Foo(decl_base):
                __tablename__ = "foo"

                id: M3[int] = mapped_column(primary_key=True)

                data2: M3[int]

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

    def test_typ_not_in_cls_namespace(self, decl_base):
        """test #8742.

        This tests that when types are resolved, they use the ``__module__``
        of they class they are used within, not the mapped class.

        """

        class Mixin:
            id: Mapped[int] = mapped_column(primary_key=True)
            data: Mapped[uuid.UUID]

        class MyClass(Mixin, decl_base):
            # basically no type will be resolvable here
            __module__ = "some.module"
            __tablename__ = "mytable"

        is_(MyClass.id.expression.type._type_affinity, Integer)
        is_(MyClass.data.expression.type._type_affinity, Uuid)


class MappedOneArg(KeyFuncDict[str, _R]):
    pass


class RelationshipLHSTest(_RelationshipLHSTest):
    def test_bidirectional_literal_annotations(self, decl_base):
        """test the 'string cleanup' function in orm/util.py, where
        we receive a string annotation like::

            "Mapped[List[B]]"

        Which then fails to evaluate because we don't have "B" yet.
        The annotation is converted on the fly to::

            'Mapped[List["B"]]'

        so that when we evaluated it, we get ``Mapped[List["B"]]`` and
        can extract "B" as a string.

        """

        class A(decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True)
            data: Mapped[str] = mapped_column()
            bs: Mapped[List[B]] = relationship(back_populates="a")

        class B(decl_base):
            __tablename__ = "b"
            id: Mapped[int] = mapped_column(Integer, primary_key=True)
            a_id: Mapped[int] = mapped_column(ForeignKey("a.id"))

            a: Mapped[A] = relationship(
                back_populates="bs", primaryjoin=a_id == A.id
            )

        a1 = A(data="data")
        b1 = B()
        a1.bs.append(b1)
        is_(a1, b1.a)

    def test_collection_class_uselist_implicit_fwd(self, decl_base):
        class A(decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True)
            data: Mapped[str] = mapped_column()
            bs_list: Mapped[List[B]] = relationship(  # noqa: F821
                viewonly=True
            )
            bs_set: Mapped[Set[B]] = relationship(viewonly=True)  # noqa: F821
            bs_list_warg: Mapped[List[B]] = relationship(  # noqa: F821
                "B", viewonly=True
            )
            bs_set_warg: Mapped[Set[B]] = relationship(  # noqa: F821
                "B", viewonly=True
            )

            b_one_to_one: Mapped[B] = relationship(viewonly=True)  # noqa: F821

            b_one_to_one_warg: Mapped[B] = relationship(  # noqa: F821
                "B", viewonly=True
            )

        class B(decl_base):
            __tablename__ = "b"
            id: Mapped[int] = mapped_column(Integer, primary_key=True)
            a_id: Mapped[int] = mapped_column(ForeignKey("a.id"))

            a: Mapped[A] = relationship(viewonly=True)
            a_warg: Mapped[A] = relationship("A", viewonly=True)

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

    def test_collection_class_dict_attr_mapped_collection_literal_annotations(
        self, decl_base
    ):
        class A(decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True)
            data: Mapped[str] = mapped_column()

            bs: Mapped[KeyFuncDict[str, B]] = relationship(  # noqa: F821
                collection_class=attribute_keyed_dict("name")
            )

        class B(decl_base):
            __tablename__ = "b"
            id: Mapped[int] = mapped_column(Integer, primary_key=True)
            a_id: Mapped[int] = mapped_column(ForeignKey("a.id"))
            name: Mapped[str] = mapped_column()

        self._assert_dict(A, B)

    def test_collection_cls_attr_mapped_collection_dbl_literal_annotations(
        self, decl_base
    ):
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

        self._assert_dict(A, B)

    def test_collection_cls_not_locatable(self, decl_base):
        class MyCollection(KeyFuncDict):
            pass

        with expect_raises_message(
            exc.ArgumentError,
            r"Could not interpret annotation Mapped\[MyCollection\['B'\]\].",
        ):

            class A(decl_base):
                __tablename__ = "a"

                id: Mapped[int] = mapped_column(primary_key=True)
                data: Mapped[str] = mapped_column()

                bs: Mapped[MyCollection["B"]] = relationship(  # noqa: F821
                    collection_class=attribute_keyed_dict("name")
                )

    def test_collection_cls_one_arg(self, decl_base):
        class A(decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True)
            data: Mapped[str] = mapped_column()

            bs: Mapped[MappedOneArg["B"]] = relationship(  # noqa: F821
                collection_class=attribute_keyed_dict("name")
            )

        class B(decl_base):
            __tablename__ = "b"
            id: Mapped[int] = mapped_column(Integer, primary_key=True)
            a_id: Mapped[int] = mapped_column(ForeignKey("a.id"))
            name: Mapped[str] = mapped_column()

        self._assert_dict(A, B)

    def _assert_dict(self, A, B):
        A.registry.configure()

        a1 = A()
        b1 = B(name="foo")

        # collection appender on MappedCollection
        a1.bs.set(b1)

        is_(a1.bs["foo"], b1)

    def test_14_style_anno_accepted_w_allow_unmapped(self):
        """test for #8692"""

        class Base(DeclarativeBase):
            __allow_unmapped__ = True

        class A(Base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True)
            data: str = Column(String)
            bs: List[B] = relationship("B", back_populates="a")

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
                a: Mapped[Union[A, None]] = relationship(
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

        class B(decl_base):
            __tablename__ = "b"
            id: Mapped[int] = mapped_column(Integer, primary_key=True)
            a_id: Mapped[int] = mapped_column(ForeignKey("a.id"))

        global B_
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

        class A(decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True)
            data: Mapped[str] = mapped_column()

        global A_
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

    def test_indirect_name_relationship_arg_override(self, decl_base):
        """test #8759

        in this test we assume a case where the type for the Mapped annnotation
        a. has to be a different name than the actual class name and
        b. cannot be imported outside of TYPE CHECKING.  user will then put
        the real name inside of relationship().  we have to succeed even though
        we can't resolve the annotation.

        """

        class B(decl_base):
            __tablename__ = "b"
            id: Mapped[int] = mapped_column(Integer, primary_key=True)
            a_id: Mapped[int] = mapped_column(ForeignKey("a.id"))

        if TYPE_CHECKING:
            BNonExistent = B

        class A(decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True)
            data: Mapped[str] = mapped_column()

            bs: Mapped[List[BNonExistent]] = relationship("B")

        self.assert_compile(
            select(A).join(A.bs),
            "SELECT a.id, a.data FROM a JOIN b ON a.id = b.a_id",
        )


class WriteOnlyRelationshipTest(_WriteOnlyRelationshipTest):
    def test_dynamic(self, decl_base):
        class A(decl_base):
            __tablename__ = "a"
            id: Mapped[int] = mapped_column(primary_key=True)
            bs: DynamicMapped[B] = relationship()

        class B(decl_base):
            __tablename__ = "b"
            id: Mapped[int] = mapped_column(primary_key=True)
            a_id: Mapped[int] = mapped_column(
                ForeignKey("a.id", ondelete="cascade")
            )

        self._assertions(A, B, "dynamic")

    def test_write_only(self, decl_base):
        class A(decl_base):
            __tablename__ = "a"
            id: Mapped[int] = mapped_column(primary_key=True)
            bs: WriteOnlyMapped[B] = relationship()  # noqa: F821

        class B(decl_base):
            __tablename__ = "b"
            id: Mapped[int] = mapped_column(primary_key=True)
            a_id: Mapped[int] = mapped_column(
                ForeignKey("a.id", ondelete="cascade")
            )

        self._assertions(A, B, "write_only")
