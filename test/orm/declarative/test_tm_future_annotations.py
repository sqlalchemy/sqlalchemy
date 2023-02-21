"""This file includes annotation-sensitive tests while having
``from __future__ import annotations`` in effect.

Only tests that don't have an equivalent in ``test_typed_mappings`` are
specified here. All test from ``test_typed_mappings`` are copied over to
the ``test_tm_future_annotations_sync`` by the ``sync_test_file`` script.
"""

from __future__ import annotations

from typing import List
from typing import TYPE_CHECKING
from typing import TypeVar
import uuid

from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import Uuid
import sqlalchemy.orm
from sqlalchemy.orm import attribute_keyed_dict
from sqlalchemy.orm import KeyFuncDict
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import is_
from .test_typed_mapping import expect_annotation_syntax_error
from .test_typed_mapping import MappedColumnTest as _MappedColumnTest
from .test_typed_mapping import RelationshipLHSTest as _RelationshipLHSTest


_R = TypeVar("_R")

M = Mapped


class M3:
    pass


class MappedColumnTest(_MappedColumnTest):
    def test_fully_qualified_mapped_name(self, decl_base):
        """test #8853, regression caused by #8759 ;)


        See same test in test_abs_import_only

        """

        class Foo(decl_base):
            __tablename__ = "foo"

            id: sqlalchemy.orm.Mapped[int] = mapped_column(primary_key=True)

            data: sqlalchemy.orm.Mapped[int] = mapped_column()

            data2: sqlalchemy.orm.Mapped[int]

        self.assert_compile(
            select(Foo), "SELECT foo.id, foo.data, foo.data2 FROM foo"
        )

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

    def test_dont_ignore_unresolvable(self, decl_base):
        """test #8888"""

        with expect_raises_message(
            exc.ArgumentError,
            r"Could not resolve all types within mapped annotation: "
            r"\"Mapped\[fake\]\".  Ensure all types are written correctly and "
            r"are imported within the module in use.",
        ):

            class A(decl_base):
                __tablename__ = "a"

                id: Mapped[int] = mapped_column(primary_key=True)
                data: Mapped[fake]  # noqa


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
