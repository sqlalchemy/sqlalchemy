from __future__ import annotations

from decimal import Decimal
from typing import List
from typing import Optional
from typing import Set
from typing import TypeVar
from typing import Union

from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import Numeric
from sqlalchemy import Table
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
from .test_typed_mapping import MappedColumnTest as _MappedColumnTest
from .test_typed_mapping import RelationshipLHSTest as _RelationshipLHSTest
from .test_typed_mapping import (
    WriteOnlyRelationshipTest as _WriteOnlyRelationshipTest,
)

"""runs the annotation-sensitive tests from test_typed_mappings while
having ``from __future__ import annotations`` in effect.

"""


_R = TypeVar("_R")


class MappedColumnTest(_MappedColumnTest):
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
