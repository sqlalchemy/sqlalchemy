# ext/mypy/decl_class.py
# Copyright (C) 2021 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import Type
from typing import Union

from mypy import nodes
from mypy import types
from mypy.messages import format_type
from mypy.nodes import ARG_NAMED_OPT
from mypy.nodes import Argument
from mypy.nodes import AssignmentStmt
from mypy.nodes import CallExpr
from mypy.nodes import ClassDef
from mypy.nodes import Decorator
from mypy.nodes import JsonDict
from mypy.nodes import ListExpr
from mypy.nodes import MDEF
from mypy.nodes import NameExpr
from mypy.nodes import PlaceholderNode
from mypy.nodes import RefExpr
from mypy.nodes import StrExpr
from mypy.nodes import SymbolTableNode
from mypy.nodes import TempNode
from mypy.nodes import TypeInfo
from mypy.nodes import Var
from mypy.plugin import SemanticAnalyzerPluginInterface
from mypy.plugins.common import add_method_to_class
from mypy.plugins.common import deserialize_and_fixup_type
from mypy.subtypes import is_subtype
from mypy.types import AnyType
from mypy.types import Instance
from mypy.types import NoneTyp
from mypy.types import NoneType
from mypy.types import TypeOfAny
from mypy.types import UnboundType
from mypy.types import UnionType

from . import names
from . import util


class DeclClassApplied:
    def __init__(
        self,
        is_mapped: bool,
        has_table: bool,
        mapped_attr_names: Sequence[Tuple[str, Type]],
        mapped_mro: Sequence[Type],
    ):
        self.is_mapped = is_mapped
        self.has_table = has_table
        self.mapped_attr_names = mapped_attr_names
        self.mapped_mro = mapped_mro

    def serialize(self) -> JsonDict:
        return {
            "is_mapped": self.is_mapped,
            "has_table": self.has_table,
            "mapped_attr_names": [
                (name, type_.serialize())
                for name, type_ in self.mapped_attr_names
            ],
            "mapped_mro": [type_.serialize() for type_ in self.mapped_mro],
        }

    @classmethod
    def deserialize(
        cls, data: JsonDict, api: SemanticAnalyzerPluginInterface
    ) -> "DeclClassApplied":

        return DeclClassApplied(
            is_mapped=data["is_mapped"],
            has_table=data["has_table"],
            mapped_attr_names=[
                (name, deserialize_and_fixup_type(type_, api))
                for name, type_ in data["mapped_attr_names"]
            ],
            mapped_mro=[
                deserialize_and_fixup_type(type_, api)
                for type_ in data["mapped_mro"]
            ],
        )


def _scan_declarative_assignments_and_apply_types(
    cls: ClassDef, api: SemanticAnalyzerPluginInterface, is_mixin_scan=False
) -> Optional[DeclClassApplied]:

    if cls.fullname.startswith("builtins"):
        return None
    elif "_sa_decl_class_applied" in cls.info.metadata:
        cls_metadata = DeclClassApplied.deserialize(
            cls.info.metadata["_sa_decl_class_applied"], api
        )

        # ensure that a class that's mapped is always picked up by
        # its mapped() decorator or declarative metaclass before
        # it would be detected as an unmapped mixin class
        if not is_mixin_scan:
            assert cls_metadata.is_mapped

            # mypy can call us more than once.  it then will have reset the
            # left hand side of everything, but not the right that we removed,
            # removing our ability to re-scan.   but we have the types
            # here, so lets re-apply them.

            _re_apply_declarative_assignments(cls, api, cls_metadata)

        return cls_metadata

    cls_metadata = DeclClassApplied(not is_mixin_scan, False, [], [])

    for stmt in util._flatten_typechecking(cls.defs.body):
        if isinstance(stmt, AssignmentStmt):
            _scan_declarative_assignment_stmt(cls, api, stmt, cls_metadata)
        elif isinstance(stmt, Decorator):
            _scan_declarative_decorator_stmt(cls, api, stmt, cls_metadata)
    _scan_for_mapped_bases(cls, api, cls_metadata)
    _add_additional_orm_attributes(cls, api, cls_metadata)

    cls.info.metadata["_sa_decl_class_applied"] = cls_metadata.serialize()

    return cls_metadata


def _scan_declarative_decorator_stmt(
    cls: ClassDef,
    api: SemanticAnalyzerPluginInterface,
    stmt: Decorator,
    cls_metadata: DeclClassApplied,
):
    """Extract mapping information from a @declared_attr in a declarative
    class.

    E.g.::

        @reg.mapped
        class MyClass:
            # ...

            @declared_attr
            def updated_at(cls) -> Column[DateTime]:
                return Column(DateTime)

    Will resolve in mypy as::

        @reg.mapped
        class MyClass:
            # ...

            updated_at: Mapped[Optional[datetime.datetime]]

    """
    for dec in stmt.decorators:
        if names._type_id_for_named_node(dec) is names.DECLARED_ATTR:
            break
    else:
        return

    dec_index = cls.defs.body.index(stmt)

    left_hand_explicit_type = None

    if stmt.func.type is not None:
        func_type = stmt.func.type.ret_type
        if isinstance(func_type, UnboundType):
            type_id = names._type_id_for_unbound_type(func_type, cls, api)
        else:
            # this does not seem to occur unless the type argument is
            # incorrect
            return

        if (
            type_id
            in {
                names.MAPPED,
                names.RELATIONSHIP,
                names.COMPOSITE_PROPERTY,
                names.MAPPER_PROPERTY,
                names.SYNONYM_PROPERTY,
                names.COLUMN_PROPERTY,
            }
            and func_type.args
        ):
            left_hand_explicit_type = func_type.args[0]
        elif type_id is names.COLUMN and func_type.args:
            typeengine_arg = func_type.args[0]
            if isinstance(typeengine_arg, UnboundType):
                sym = api.lookup(typeengine_arg.name, typeengine_arg)
                if sym is not None and names._mro_has_id(
                    sym.node.mro, names.TYPEENGINE
                ):

                    left_hand_explicit_type = UnionType(
                        [
                            _extract_python_type_from_typeengine(sym.node),
                            NoneType(),
                        ]
                    )
                else:
                    util.fail(
                        api,
                        "Column type should be a TypeEngine "
                        "subclass not '{}'".format(sym.node.fullname),
                        func_type,
                    )

    if left_hand_explicit_type is None:
        # no type on the decorated function.  our option here is to
        # dig into the function body and get the return type, but they
        # should just have an annotation.
        msg = (
            "Can't infer type from @declared_attr on function '{}';  "
            "please specify a return type from this function that is "
            "one of: Mapped[<python type>], relationship[<target class>], "
            "Column[<TypeEngine>], MapperProperty[<python type>]"
        )
        util.fail(api, msg.format(stmt.var.name), stmt)

        left_hand_explicit_type = AnyType(TypeOfAny.special_form)

    descriptor = api.modules["sqlalchemy.orm.attributes"].names["Mapped"]

    left_node = NameExpr(stmt.var.name)
    left_node.node = stmt.var

    # totally feeling around in the dark here as I don't totally understand
    # the significance of UnboundType.  It seems to be something that is
    # not going to do what's expected when it is applied as the type of
    # an AssignmentStatement.  So do a feeling-around-in-the-dark version
    # of converting it to the regular Instance/TypeInfo/UnionType structures
    # we see everywhere else.
    if isinstance(left_hand_explicit_type, UnboundType):
        left_hand_explicit_type = util._unbound_to_instance(
            api, left_hand_explicit_type
        )

    left_node.node.type = Instance(descriptor.node, [left_hand_explicit_type])

    # this will ignore the rvalue entirely
    # rvalue = TempNode(AnyType(TypeOfAny.special_form))

    # rewrite the node as:
    # <attr> : Mapped[<typ>] =
    # _sa_Mapped._empty_constructor(lambda: <function body>)
    # the function body is maintained so it gets type checked internally
    api.add_symbol_table_node("_sa_Mapped", descriptor)
    column_descriptor = nodes.NameExpr("_sa_Mapped")
    column_descriptor.fullname = "sqlalchemy.orm.Mapped"
    mm = nodes.MemberExpr(column_descriptor, "_empty_constructor")

    arg = nodes.LambdaExpr(stmt.func.arguments, stmt.func.body)
    rvalue = CallExpr(
        mm,
        [arg],
        [nodes.ARG_POS],
        ["arg1"],
    )

    new_stmt = AssignmentStmt([left_node], rvalue)
    new_stmt.type = left_node.node.type

    cls_metadata.mapped_attr_names.append(
        (left_node.name, left_hand_explicit_type)
    )
    cls.defs.body[dec_index] = new_stmt


def _scan_declarative_assignment_stmt(
    cls: ClassDef,
    api: SemanticAnalyzerPluginInterface,
    stmt: AssignmentStmt,
    cls_metadata: DeclClassApplied,
):
    """Extract mapping information from an assignment statement in a
    declarative class.

    """
    lvalue = stmt.lvalues[0]
    if not isinstance(lvalue, NameExpr):
        return

    sym = cls.info.names.get(lvalue.name)

    # this establishes that semantic analysis has taken place, which
    # means the nodes are populated and we are called from an appropriate
    # hook.
    assert sym is not None
    node = sym.node

    if isinstance(node, PlaceholderNode):
        return

    assert node is lvalue.node
    assert isinstance(node, Var)

    if node.name == "__abstract__":
        if stmt.rvalue.fullname == "builtins.True":
            cls_metadata.is_mapped = False
        return
    elif node.name == "__tablename__":
        cls_metadata.has_table = True
    elif node.name.startswith("__"):
        return
    elif node.name == "_mypy_mapped_attrs":
        if not isinstance(stmt.rvalue, ListExpr):
            util.fail(api, "_mypy_mapped_attrs is expected to be a list", stmt)
        else:
            for item in stmt.rvalue.items:
                if isinstance(item, (NameExpr, StrExpr)):
                    _apply_mypy_mapped_attr(cls, api, item, cls_metadata)

    left_hand_mapped_type: Type = None

    if node.is_inferred or node.type is None:
        if isinstance(stmt.type, UnboundType):
            # look for an explicit Mapped[] type annotation on the left
            # side with nothing on the right

            # print(stmt.type)
            # Mapped?[Optional?[A?]]

            left_hand_explicit_type = stmt.type

            if stmt.type.name == "Mapped":
                mapped_sym = api.lookup("Mapped", cls)
                if (
                    mapped_sym is not None
                    and names._type_id_for_named_node(mapped_sym.node)
                    is names.MAPPED
                ):
                    left_hand_explicit_type = stmt.type.args[0]
                    left_hand_mapped_type = stmt.type

            # TODO: do we need to convert from unbound for this case?
            # left_hand_explicit_type = util._unbound_to_instance(
            #     api, left_hand_explicit_type
            # )

        else:
            left_hand_explicit_type = None
    else:
        if (
            isinstance(node.type, Instance)
            and names._type_id_for_named_node(node.type.type) is names.MAPPED
        ):
            # print(node.type)
            # sqlalchemy.orm.attributes.Mapped[<python type>]
            left_hand_explicit_type = node.type.args[0]
            left_hand_mapped_type = node.type
        else:
            # print(node.type)
            # <python type>
            left_hand_explicit_type = node.type
            left_hand_mapped_type = None

    if isinstance(stmt.rvalue, TempNode) and left_hand_mapped_type is not None:
        # annotation without assignment and Mapped is present
        # as type annotation
        # equivalent to using _infer_type_from_left_hand_type_only.

        python_type_for_type = left_hand_explicit_type
    elif isinstance(stmt.rvalue, CallExpr) and isinstance(
        stmt.rvalue.callee, RefExpr
    ):

        type_id = names._type_id_for_callee(stmt.rvalue.callee)

        if type_id is None:
            return
        elif type_id is names.COLUMN:
            python_type_for_type = _infer_type_from_decl_column(
                api, stmt, node, left_hand_explicit_type, stmt.rvalue
            )
        elif type_id is names.RELATIONSHIP:
            python_type_for_type = _infer_type_from_relationship(
                api, stmt, node, left_hand_explicit_type
            )
        elif type_id is names.COLUMN_PROPERTY:
            python_type_for_type = _infer_type_from_decl_column_property(
                api, stmt, node, left_hand_explicit_type
            )
        elif type_id is names.SYNONYM_PROPERTY:
            python_type_for_type = _infer_type_from_left_hand_type_only(
                api, node, left_hand_explicit_type
            )
        elif type_id is names.COMPOSITE_PROPERTY:
            python_type_for_type = _infer_type_from_decl_composite_property(
                api, stmt, node, left_hand_explicit_type
            )
        else:
            return

    else:
        return

    cls_metadata.mapped_attr_names.append((node.name, python_type_for_type))

    assert python_type_for_type is not None

    _apply_type_to_mapped_statement(
        api,
        stmt,
        lvalue,
        left_hand_explicit_type,
        python_type_for_type,
    )


def _apply_mypy_mapped_attr(
    cls: ClassDef,
    api: SemanticAnalyzerPluginInterface,
    item: Union[NameExpr, StrExpr],
    cls_metadata: DeclClassApplied,
):
    if isinstance(item, NameExpr):
        name = item.name
    elif isinstance(item, StrExpr):
        name = item.value
    else:
        return

    for stmt in cls.defs.body:
        if isinstance(stmt, AssignmentStmt) and stmt.lvalues[0].name == name:
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

    left_hand_explicit_type = stmt.type

    cls_metadata.mapped_attr_names.append((name, left_hand_explicit_type))

    _apply_type_to_mapped_statement(
        api, stmt, stmt.lvalues[0], left_hand_explicit_type, None
    )


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

    # this can be used to determine Optional for a many-to-one
    # in the same way nullable=False could be used, if we start supporting
    # that.
    # innerjoin_arg = _get_callexpr_kwarg(stmt.rvalue, "innerjoin")

    if (
        uselist_arg is not None
        and uselist_arg.fullname == "builtins.True"
        and collection_cls_arg is None
    ):
        if python_type_for_type is not None:
            python_type_for_type = Instance(
                api.lookup_fully_qualified("builtins.list").node,
                [python_type_for_type],
            )
    elif (
        uselist_arg is None or uselist_arg.fullname == "builtins.True"
    ) and collection_cls_arg is not None:
        if isinstance(collection_cls_arg.node, TypeInfo):
            if python_type_for_type is not None:
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
            api, node, left_hand_explicit_type, python_type_for_type
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

    print(stmt.lvalues[0].name)

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
            break
        elif isinstance(column_arg, (nodes.NameExpr, nodes.MemberExpr)):
            if isinstance(column_arg.node, TypeInfo):
                # x = Column(String)
                callee = column_arg
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

    if names._mro_has_id(callee.node.mro, names.TYPEENGINE):
        python_type_for_type = _extract_python_type_from_typeengine(
            callee.node
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
) -> Optional[Union[Instance, UnionType]]:
    """Validate type when a left hand annotation is present and we also
    could infer the right hand side::

        attrname: SomeType = Column(SomeDBType)

    """
    if not is_subtype(left_hand_explicit_type, python_type_for_type):
        descriptor = api.modules["sqlalchemy.orm.attributes"].names["Mapped"]

        effective_type = Instance(descriptor.node, [python_type_for_type])

        msg = (
            "Left hand assignment '{}: {}' not compatible "
            "with ORM mapped expression of type {}"
        )
        util.fail(
            api,
            msg.format(
                node.name,
                format_type(left_hand_explicit_type),
                format_type(effective_type),
            ),
            node,
        )

    return left_hand_explicit_type


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

        descriptor = api.modules["sqlalchemy.orm.attributes"].names["Mapped"]

        return Instance(descriptor.node, [AnyType(TypeOfAny.special_form)])

    else:
        # use type from the left hand side
        return left_hand_explicit_type


def _re_apply_declarative_assignments(
    cls: ClassDef,
    api: SemanticAnalyzerPluginInterface,
    cls_metadata: DeclClassApplied,
):
    """For multiple class passes, re-apply our left-hand side types as mypy
    seems to reset them in place.

    """
    mapped_attr_lookup = {
        name: typ for name, typ in cls_metadata.mapped_attr_names
    }

    descriptor = api.modules["sqlalchemy.orm.attributes"].names["Mapped"]

    for stmt in cls.defs.body:
        # for a re-apply, all of our statements are AssignmentStmt;
        # @declared_attr calls will have been converted and this
        # currently seems to be preserved by mypy (but who knows if this
        # will change).
        if (
            isinstance(stmt, AssignmentStmt)
            and stmt.lvalues[0].name in mapped_attr_lookup
        ):
            typ = mapped_attr_lookup[stmt.lvalues[0].name]
            left_node = stmt.lvalues[0].node

            inst = Instance(descriptor.node, [typ])
            left_node.type = inst


def _apply_type_to_mapped_statement(
    api: SemanticAnalyzerPluginInterface,
    stmt: AssignmentStmt,
    lvalue: NameExpr,
    left_hand_explicit_type: Optional[Union[Instance, UnionType]],
    python_type_for_type: Union[Instance, UnionType],
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
    descriptor = api.modules["sqlalchemy.orm.attributes"].names["Mapped"]

    left_node = lvalue.node

    inst = Instance(descriptor.node, [python_type_for_type])

    if left_hand_explicit_type is not None:
        left_node.type = Instance(descriptor.node, [left_hand_explicit_type])
    else:
        lvalue.is_inferred_def = False
        left_node.type = inst

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
    api.add_symbol_table_node("_sa_Mapped", descriptor)
    column_descriptor = nodes.NameExpr("_sa_Mapped")
    column_descriptor.fullname = "sqlalchemy.orm.Mapped"
    mm = nodes.MemberExpr(column_descriptor, "_empty_constructor")
    orig_call_expr = stmt.rvalue
    stmt.rvalue = CallExpr(
        mm,
        [orig_call_expr],
        [nodes.ARG_POS],
        ["arg1"],
    )


def _scan_for_mapped_bases(
    cls: ClassDef,
    api: SemanticAnalyzerPluginInterface,
    cls_metadata: DeclClassApplied,
) -> None:
    """Given a class, iterate through its superclass hierarchy to find
    all other classes that are considered as ORM-significant.

    Locates non-mapped mixins and scans them for mapped attributes to be
    applied to subclasses.

    """

    baseclasses = list(cls.info.bases)
    while baseclasses:
        base: Instance = baseclasses.pop(0)

        # scan each base for mapped attributes.  if they are not already
        # scanned, that means they are unmapped mixins
        base_decl_class_applied = (
            _scan_declarative_assignments_and_apply_types(
                base.type.defn, api, is_mixin_scan=True
            )
        )
        if base_decl_class_applied is not None:
            cls_metadata.mapped_mro.append(base)
        baseclasses.extend(base.type.bases)


def _add_additional_orm_attributes(
    cls: ClassDef,
    api: SemanticAnalyzerPluginInterface,
    cls_metadata: DeclClassApplied,
) -> None:
    """Apply __init__, __table__ and other attributes to the mapped class."""
    if "__init__" not in cls.info.names and cls_metadata.is_mapped:
        mapped_attr_names = {n: t for n, t in cls_metadata.mapped_attr_names}

        for mapped_base in cls_metadata.mapped_mro:
            base_cls_metadata = DeclClassApplied.deserialize(
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

    if "__table__" not in cls.info.names and cls_metadata.has_table:
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
):
    sym = api.lookup_fully_qualified_or_none(qualified_name)
    if sym:
        assert isinstance(sym.node, TypeInfo)
        type_ = Instance(sym.node, [])
    else:
        type_ = AnyType(TypeOfAny.special_form)
    var = Var(attrname)
    var.info = cls.info
    var.type = type_
    cls.info.names[attrname] = SymbolTableNode(MDEF, var)


def _extract_python_type_from_typeengine(node: TypeInfo) -> Instance:
    for mr in node.mro:
        if (
            mr.bases
            and mr.bases[-1].type.fullname
            == "sqlalchemy.sql.type_api.TypeEngine"
        ):
            return mr.bases[-1].args[-1]
    else:
        assert False, "could not extract Python type from node: %s" % node
