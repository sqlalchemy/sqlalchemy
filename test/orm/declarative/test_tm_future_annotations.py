from __future__ import annotations

from typing import List

from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.testing import is_
from .test_typed_mapping import MappedColumnTest  # noqa
from .test_typed_mapping import RelationshipLHSTest as _RelationshipLHSTest

"""runs the annotation-sensitive tests from test_typed_mappings while
having ``from __future__ import annotations`` in effect.

"""


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
