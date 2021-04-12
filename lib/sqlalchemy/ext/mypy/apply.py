# ext/mypy/apply.py
# Copyright (C) 2021 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from typing import Optional
from typing import Union

from mypy import nodes
from mypy.nodes import ARG_NAMED_OPT
from mypy.nodes import Argument
from mypy.nodes import AssignmentStmt
from mypy.nodes import CallExpr
from mypy.nodes import ClassDef
from mypy.nodes import MDEF
from mypy.nodes import NameExpr
from mypy.nodes import StrExpr
from mypy.nodes import SymbolTableNode
from mypy.nodes import TempNode
from mypy.nodes import TypeInfo
from mypy.nodes import Var
from mypy.plugin import SemanticAnalyzerPluginInterface
from mypy.plugins.common import add_method_to_class
from mypy.types import AnyType
from mypy.types import get_proper_type
from mypy.types import Instance
from mypy.types import NoneTyp
from mypy.types import ProperType
from mypy.types import TypeOfAny
from mypy.types import UnboundType
from mypy.types import UnionType

from . import util


def _apply_mypy_mapped_attr(
    cls: ClassDef,
    api: SemanticAnalyzerPluginInterface,
    item: Union[NameExpr, StrExpr],
    cls_metadata: util.DeclClassApplied,
) -> None:
    if isinstance(item, NameExpr):
        name = item.name
    elif isinstance(item, StrExpr):
        name = item.value
    else:
        return

    for stmt in cls.defs.body:
        if (
            isinstance(stmt, AssignmentStmt)
            and isinstance(stmt.lvalues[0], NameExpr)
            and stmt.lvalues[0].name == name
        ):
            break
    else:
        util.fail(api, "Can't find mapped attribute {}".format(name), cls)
        return

    if stmt.type is None:
        util.fail(
            api,
            "Statement linked from _mypy_mapped_attrs has no "
            "typing information",
            stmt,
        )
        return

    left_hand_explicit_type = get_proper_type(stmt.type)
    assert isinstance(
        left_hand_explicit_type, (Instance, UnionType, UnboundType)
    )

    cls_metadata.mapped_attr_names.append((name, left_hand_explicit_type))

    _apply_type_to_mapped_statement(
        api, stmt, stmt.lvalues[0], left_hand_explicit_type, None
    )


def _re_apply_declarative_assignments(
    cls: ClassDef,
    api: SemanticAnalyzerPluginInterface,
    cls_metadata: util.DeclClassApplied,
) -> None:
    """For multiple class passes, re-apply our left-hand side types as mypy
    seems to reset them in place.

    """
    mapped_attr_lookup = {
        name: typ for name, typ in cls_metadata.mapped_attr_names
    }

    for stmt in cls.defs.body:
        # for a re-apply, all of our statements are AssignmentStmt;
        # @declared_attr calls will have been converted and this
        # currently seems to be preserved by mypy (but who knows if this
        # will change).
        if (
            isinstance(stmt, AssignmentStmt)
            and isinstance(stmt.lvalues[0], NameExpr)
            and stmt.lvalues[0].name in mapped_attr_lookup
            and isinstance(stmt.lvalues[0].node, Var)
        ):
            typ = mapped_attr_lookup[stmt.lvalues[0].name]
            left_node = stmt.lvalues[0].node

            left_node.type = api.named_type("__sa_Mapped", [typ])


def _apply_type_to_mapped_statement(
    api: SemanticAnalyzerPluginInterface,
    stmt: AssignmentStmt,
    lvalue: NameExpr,
    left_hand_explicit_type: Optional[ProperType],
    python_type_for_type: Optional[ProperType],
) -> None:
    """Apply the Mapped[<type>] annotation and right hand object to a
    declarative assignment statement.

    This converts a Python declarative class statement such as::

        class User(Base):
            # ...

            attrname = Column(Integer)

    To one that describes the final Python behavior to Mypy::

        class User(Base):
            # ...

            attrname : Mapped[Optional[int]] = <meaningless temp node>

    """
    left_node = lvalue.node
    assert isinstance(left_node, Var)

    if left_hand_explicit_type is not None:
        left_node.type = api.named_type(
            "__sa_Mapped", [left_hand_explicit_type]
        )
    else:
        lvalue.is_inferred_def = False
        left_node.type = api.named_type(
            "__sa_Mapped",
            [] if python_type_for_type is None else [python_type_for_type],
        )

    # so to have it skip the right side totally, we can do this:
    # stmt.rvalue = TempNode(AnyType(TypeOfAny.special_form))

    # however, if we instead manufacture a new node that uses the old
    # one, then we can still get type checking for the call itself,
    # e.g. the Column, relationship() call, etc.

    # rewrite the node as:
    # <attr> : Mapped[<typ>] =
    # _sa_Mapped._empty_constructor(<original CallExpr from rvalue>)
    # the original right-hand side is maintained so it gets type checked
    # internally
    column_descriptor = nodes.NameExpr("__sa_Mapped")
    column_descriptor.fullname = "sqlalchemy.orm.attributes.Mapped"
    mm = nodes.MemberExpr(column_descriptor, "_empty_constructor")
    orig_call_expr = stmt.rvalue
    stmt.rvalue = CallExpr(mm, [orig_call_expr], [nodes.ARG_POS], ["arg1"])


def _add_additional_orm_attributes(
    cls: ClassDef,
    api: SemanticAnalyzerPluginInterface,
    cls_metadata: util.DeclClassApplied,
) -> None:
    """Apply __init__, __table__ and other attributes to the mapped class."""

    info = util._info_for_cls(cls, api)
    if "__init__" not in info.names and cls_metadata.is_mapped:
        mapped_attr_names = {n: t for n, t in cls_metadata.mapped_attr_names}

        for mapped_base in cls_metadata.mapped_mro:
            base_cls_metadata = util.DeclClassApplied.deserialize(
                mapped_base.type.metadata["_sa_decl_class_applied"], api
            )
            for n, t in base_cls_metadata.mapped_attr_names:
                mapped_attr_names.setdefault(n, t)

        arguments = []
        for name, typ in mapped_attr_names.items():
            if typ is None:
                typ = AnyType(TypeOfAny.special_form)
            arguments.append(
                Argument(
                    variable=Var(name, typ),
                    type_annotation=typ,
                    initializer=TempNode(typ),
                    kind=ARG_NAMED_OPT,
                )
            )
        add_method_to_class(api, cls, "__init__", arguments, NoneTyp())

    if "__table__" not in info.names and cls_metadata.has_table:
        _apply_placeholder_attr_to_class(
            api, cls, "sqlalchemy.sql.schema.Table", "__table__"
        )
    if cls_metadata.is_mapped:
        _apply_placeholder_attr_to_class(
            api, cls, "sqlalchemy.orm.mapper.Mapper", "__mapper__"
        )


def _apply_placeholder_attr_to_class(
    api: SemanticAnalyzerPluginInterface,
    cls: ClassDef,
    qualified_name: str,
    attrname: str,
) -> None:
    sym = api.lookup_fully_qualified_or_none(qualified_name)
    if sym:
        assert isinstance(sym.node, TypeInfo)
        type_: ProperType = Instance(sym.node, [])
    else:
        type_ = AnyType(TypeOfAny.special_form)
    var = Var(attrname)
    var.info = cls.info
    var.type = type_
    cls.info.names[attrname] = SymbolTableNode(MDEF, var)
