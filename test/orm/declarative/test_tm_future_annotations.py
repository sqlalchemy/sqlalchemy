from __future__ import annotations

from typing import List
from typing import Set
from typing import TypeVar

from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy.orm import attribute_mapped_collection
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import MappedCollection
from sqlalchemy.orm import relationship
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_false
from sqlalchemy.testing import is_true
from .test_typed_mapping import MappedColumnTest  # noqa
from .test_typed_mapping import RelationshipLHSTest as _RelationshipLHSTest

"""runs the annotation-sensitive tests from test_typed_mappings while
having ``from __future__ import annotations`` in effect.

"""


_R = TypeVar("_R")


class MappedOneArg(MappedCollection[str, _R]):
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

            bs: Mapped[MappedCollection[str, B]] = relationship(  # noqa: F821
                collection_class=attribute_mapped_collection("name")
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

            bs: Mapped[
                MappedCollection[str, "B"]
            ] = relationship(  # noqa: F821
                collection_class=attribute_mapped_collection("name")
            )

        class B(decl_base):
            __tablename__ = "b"
            id: Mapped[int] = mapped_column(Integer, primary_key=True)
            a_id: Mapped[int] = mapped_column(ForeignKey("a.id"))
            name: Mapped[str] = mapped_column()

        self._assert_dict(A, B)

    def test_collection_cls_not_locatable(self, decl_base):
        class MyCollection(MappedCollection):
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
                    collection_class=attribute_mapped_collection("name")
                )

    def test_collection_cls_one_arg(self, decl_base):
        class A(decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(primary_key=True)
            data: Mapped[str] = mapped_column()

            bs: Mapped[MappedOneArg["B"]] = relationship(  # noqa: F821
                collection_class=attribute_mapped_collection("name")
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
