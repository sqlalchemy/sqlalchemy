# ext/mypy/infer.py
# Copyright (C) 2021 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from typing import Optional
from typing import Union

from mypy import nodes
from mypy import types
from mypy.messages import format_type
from mypy.nodes import AssignmentStmt
from mypy.nodes import CallExpr
from mypy.nodes import NameExpr
from mypy.nodes import StrExpr
from mypy.nodes import TypeInfo
from mypy.nodes import Var
from mypy.plugin import SemanticAnalyzerPluginInterface
from mypy.subtypes import is_subtype
from mypy.types import AnyType
from mypy.types import Instance
from mypy.types import NoneType
from mypy.types import TypeOfAny
from mypy.types import UnionType

from . import names
from . import util


def _infer_type_from_relationship(
    api: SemanticAnalyzerPluginInterface,
    stmt: AssignmentStmt,
    node: Var,
    left_hand_explicit_type: Optional[types.Type],
) -> Union[Instance, UnionType, None]:
    """Infer the type of mapping from a relationship.

    E.g.::

        @reg.mapped
        class MyClass:
            # ...

            addresses = relationship(Address, uselist=True)

            order: Mapped["Order"] = relationship("Order")

    Will resolve in mypy as::

        @reg.mapped
        class MyClass:
            # ...

            addresses: Mapped[List[Address]]

            order: Mapped["Order"]

    """

    assert isinstance(stmt.rvalue, CallExpr)
    target_cls_arg = stmt.rvalue.args[0]
    python_type_for_type = None

    if isinstance(target_cls_arg, NameExpr) and isinstance(
        target_cls_arg.node, TypeInfo
    ):
        # type
        related_object_type = target_cls_arg.node
        python_type_for_type = Instance(related_object_type, [])

    # other cases not covered - an error message directs the user
    # to set an explicit type annotation
    #
    # node.type == str, it's a string
    # if isinstance(target_cls_arg, NameExpr) and isinstance(
    #     target_cls_arg.node, Var
    # )
    # points to a type
    # isinstance(target_cls_arg, NameExpr) and isinstance(
    #     target_cls_arg.node, TypeAlias
    # )
    # string expression
    # isinstance(target_cls_arg, StrExpr)

    uselist_arg = util._get_callexpr_kwarg(stmt.rvalue, "uselist")
    collection_cls_arg = util._get_callexpr_kwarg(
        stmt.rvalue, "collection_class"
    )
    type_is_a_collection = False

    # this can be used to determine Optional for a many-to-one
    # in the same way nullable=False could be used, if we start supporting
    # that.
    # innerjoin_arg = _get_callexpr_kwarg(stmt.rvalue, "innerjoin")

    if (
        uselist_arg is not None
        and uselist_arg.fullname == "builtins.True"
        and collection_cls_arg is None
    ):
        type_is_a_collection = True
        if python_type_for_type is not None:
            python_type_for_type = Instance(
                api.lookup_fully_qualified("builtins.list").node,
                [python_type_for_type],
            )
    elif (
        uselist_arg is None or uselist_arg.fullname == "builtins.True"
    ) and collection_cls_arg is not None:
        type_is_a_collection = True
        if isinstance(collection_cls_arg, CallExpr):
            collection_cls_arg = collection_cls_arg.callee

        if isinstance(collection_cls_arg, NameExpr) and isinstance(
            collection_cls_arg.node, TypeInfo
        ):
            if python_type_for_type is not None:
                # this can still be overridden by the left hand side
                # within _infer_Type_from_left_and_inferred_right
                python_type_for_type = Instance(
                    collection_cls_arg.node, [python_type_for_type]
                )
        else:
            util.fail(
                api,
                "Expected Python collection type for "
                "collection_class parameter",
                stmt.rvalue,
            )
            python_type_for_type = None
    elif uselist_arg is not None and uselist_arg.fullname == "builtins.False":
        if collection_cls_arg is not None:
            util.fail(
                api,
                "Sending uselist=False and collection_class at the same time "
                "does not make sense",
                stmt.rvalue,
            )
        if python_type_for_type is not None:
            python_type_for_type = UnionType(
                [python_type_for_type, NoneType()]
            )

    else:
        if left_hand_explicit_type is None:
            msg = (
                "Can't infer scalar or collection for ORM mapped expression "
                "assigned to attribute '{}' if both 'uselist' and "
                "'collection_class' arguments are absent from the "
                "relationship(); please specify a "
                "type annotation on the left hand side."
            )
            util.fail(api, msg.format(node.name), node)

    if python_type_for_type is None:
        return _infer_type_from_left_hand_type_only(
            api, node, left_hand_explicit_type
        )
    elif left_hand_explicit_type is not None:
        return _infer_type_from_left_and_inferred_right(
            api,
            node,
            left_hand_explicit_type,
            python_type_for_type,
            type_is_a_collection=type_is_a_collection,
        )
    else:
        return python_type_for_type


def _infer_type_from_decl_composite_property(
    api: SemanticAnalyzerPluginInterface,
    stmt: AssignmentStmt,
    node: Var,
    left_hand_explicit_type: Optional[types.Type],
) -> Union[Instance, UnionType, None]:
    """Infer the type of mapping from a CompositeProperty."""

    assert isinstance(stmt.rvalue, CallExpr)
    target_cls_arg = stmt.rvalue.args[0]
    python_type_for_type = None

    if isinstance(target_cls_arg, NameExpr) and isinstance(
        target_cls_arg.node, TypeInfo
    ):
        related_object_type = target_cls_arg.node
        python_type_for_type = Instance(related_object_type, [])
    else:
        python_type_for_type = None

    if python_type_for_type is None:
        return _infer_type_from_left_hand_type_only(
            api, node, left_hand_explicit_type
        )
    elif left_hand_explicit_type is not None:
        return _infer_type_from_left_and_inferred_right(
            api, node, left_hand_explicit_type, python_type_for_type
        )
    else:
        return python_type_for_type


def _infer_type_from_decl_column_property(
    api: SemanticAnalyzerPluginInterface,
    stmt: AssignmentStmt,
    node: Var,
    left_hand_explicit_type: Optional[types.Type],
) -> Union[Instance, UnionType, None]:
    """Infer the type of mapping from a ColumnProperty.

    This includes mappings against ``column_property()`` as well as the
    ``deferred()`` function.

    """
    assert isinstance(stmt.rvalue, CallExpr)
    first_prop_arg = stmt.rvalue.args[0]

    if isinstance(first_prop_arg, CallExpr):
        type_id = names._type_id_for_callee(first_prop_arg.callee)
    else:
        type_id = None

    # look for column_property() / deferred() etc with Column as first
    # argument
    if type_id is names.COLUMN:
        return _infer_type_from_decl_column(
            api, stmt, node, left_hand_explicit_type, first_prop_arg
        )
    else:
        return _infer_type_from_left_hand_type_only(
            api, node, left_hand_explicit_type
        )


def _infer_type_from_decl_column(
    api: SemanticAnalyzerPluginInterface,
    stmt: AssignmentStmt,
    node: Var,
    left_hand_explicit_type: Optional[types.Type],
    right_hand_expression: CallExpr,
) -> Union[Instance, UnionType, None]:
    """Infer the type of mapping from a Column.

    E.g.::

        @reg.mapped
        class MyClass:
            # ...

            a = Column(Integer)

            b = Column("b", String)

            c: Mapped[int] = Column(Integer)

            d: bool = Column(Boolean)

    Will resolve in MyPy as::

        @reg.mapped
        class MyClass:
            # ...

            a : Mapped[int]

            b : Mapped[str]

            c: Mapped[int]

            d: Mapped[bool]

    """
    assert isinstance(node, Var)

    callee = None

    for column_arg in right_hand_expression.args[0:2]:
        if isinstance(column_arg, nodes.CallExpr):
            # x = Column(String(50))
            callee = column_arg.callee
            type_args = column_arg.args
            break
        elif isinstance(column_arg, (nodes.NameExpr, nodes.MemberExpr)):
            if isinstance(column_arg.node, TypeInfo):
                # x = Column(String)
                callee = column_arg
                type_args = ()
                break
            else:
                # x = Column(some_name, String), go to next argument
                continue
        elif isinstance(column_arg, (StrExpr,)):
            # x = Column("name", String), go to next argument
            continue
        else:
            assert False

    if callee is None:
        return None

    if isinstance(callee.node, TypeInfo) and names._mro_has_id(
        callee.node.mro, names.TYPEENGINE
    ):
        python_type_for_type = _extract_python_type_from_typeengine(
            api, callee.node, type_args
        )

        if left_hand_explicit_type is not None:

            return _infer_type_from_left_and_inferred_right(
                api, node, left_hand_explicit_type, python_type_for_type
            )

        else:
            python_type_for_type = UnionType(
                [python_type_for_type, NoneType()]
            )
        return python_type_for_type
    else:
        # it's not TypeEngine, it's typically implicitly typed
        # like ForeignKey.  we can't infer from the right side.
        return _infer_type_from_left_hand_type_only(
            api, node, left_hand_explicit_type
        )


def _infer_type_from_left_and_inferred_right(
    api: SemanticAnalyzerPluginInterface,
    node: Var,
    left_hand_explicit_type: Optional[types.Type],
    python_type_for_type: Union[Instance, UnionType],
    type_is_a_collection: bool = False,
) -> Optional[Union[Instance, UnionType]]:
    """Validate type when a left hand annotation is present and we also
    could infer the right hand side::

        attrname: SomeType = Column(SomeDBType)

    """

    orig_left_hand_type = left_hand_explicit_type
    orig_python_type_for_type = python_type_for_type

    if type_is_a_collection and left_hand_explicit_type.args:
        left_hand_explicit_type = left_hand_explicit_type.args[0]
        python_type_for_type = python_type_for_type.args[0]

    if not is_subtype(left_hand_explicit_type, python_type_for_type):
        descriptor = api.lookup("__sa_Mapped", node)

        effective_type = Instance(descriptor.node, [orig_python_type_for_type])

        msg = (
            "Left hand assignment '{}: {}' not compatible "
            "with ORM mapped expression of type {}"
        )
        util.fail(
            api,
            msg.format(
                node.name,
                format_type(orig_left_hand_type),
                format_type(effective_type),
            ),
            node,
        )

    return orig_left_hand_type


def _infer_type_from_left_hand_type_only(
    api: SemanticAnalyzerPluginInterface,
    node: Var,
    left_hand_explicit_type: Optional[types.Type],
) -> Optional[Union[Instance, UnionType]]:
    """Determine the type based on explicit annotation only.

    if no annotation were present, note that we need one there to know
    the type.

    """
    if left_hand_explicit_type is None:
        msg = (
            "Can't infer type from ORM mapped expression "
            "assigned to attribute '{}'; please specify a "
            "Python type or "
            "Mapped[<python type>] on the left hand side."
        )
        util.fail(api, msg.format(node.name), node)

        descriptor = api.lookup("__sa_Mapped", node)
        return Instance(descriptor.node, [AnyType(TypeOfAny.special_form)])

    else:
        # use type from the left hand side
        return left_hand_explicit_type


def _extract_python_type_from_typeengine(
    api: SemanticAnalyzerPluginInterface, node: TypeInfo, type_args
) -> Instance:
    if node.fullname == "sqlalchemy.sql.sqltypes.Enum" and type_args:
        first_arg = type_args[0]
        if isinstance(first_arg, NameExpr) and isinstance(
            first_arg.node, TypeInfo
        ):
            for base_ in first_arg.node.mro:
                if base_.fullname == "enum.Enum":
                    return Instance(first_arg.node, [])
            # TODO: support other pep-435 types here
        else:
            n = api.lookup_fully_qualified("builtins.str")
            return Instance(n.node, [])

    for mr in node.mro:
        if mr.bases:
            for base_ in mr.bases:
                if base_.type.fullname == "sqlalchemy.sql.type_api.TypeEngine":
                    return base_.args[-1]
    assert False, "could not extract Python type from node: %s" % node
