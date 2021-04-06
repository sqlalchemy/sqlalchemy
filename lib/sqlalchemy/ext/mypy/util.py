from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import Type

from mypy.nodes import CallExpr
from mypy.nodes import CLASSDEF_NO_INFO
from mypy.nodes import Context
from mypy.nodes import IfStmt
from mypy.nodes import JsonDict
from mypy.nodes import NameExpr
from mypy.nodes import SymbolTableNode
from mypy.nodes import TypeInfo
from mypy.plugin import SemanticAnalyzerPluginInterface
from mypy.plugins.common import deserialize_and_fixup_type
from mypy.types import Instance
from mypy.types import NoneType
from mypy.types import UnboundType
from mypy.types import UnionType


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


def fail(api: SemanticAnalyzerPluginInterface, msg: str, ctx: Context):
    msg = "[SQLAlchemy Mypy plugin] %s" % msg
    return api.fail(msg, ctx)


def add_global(
    ctx: SemanticAnalyzerPluginInterface,
    module: str,
    symbol_name: str,
    asname: str,
):
    module_globals = ctx.api.modules[ctx.api.cur_mod_id].names

    if asname not in module_globals:
        lookup_sym: SymbolTableNode = ctx.api.modules[module].names[
            symbol_name
        ]

        module_globals[asname] = lookup_sym


def _get_callexpr_kwarg(callexpr: CallExpr, name: str) -> Optional[NameExpr]:
    try:
        arg_idx = callexpr.arg_names.index(name)
    except ValueError:
        return None

    return callexpr.args[arg_idx]


def _flatten_typechecking(stmts):
    for stmt in stmts:
        if isinstance(stmt, IfStmt) and stmt.expr[0].name == "TYPE_CHECKING":
            for substmt in stmt.body[0].body:
                yield substmt
        else:
            yield stmt


def _unbound_to_instance(
    api: SemanticAnalyzerPluginInterface, typ: UnboundType
) -> Type:
    """Take the UnboundType that we seem to get as the ret_type from a FuncDef
    and convert it into an Instance/TypeInfo kind of structure that seems
    to work as the left-hand type of an AssignmentStatement.

    """

    if not isinstance(typ, UnboundType):
        return typ

    # TODO: figure out a more robust way to check this.  The node is some
    # kind of _SpecialForm, there's a typing.Optional that's _SpecialForm,
    # but I cant figure out how to get them to match up
    if typ.name == "Optional":
        # convert from "Optional?" to the more familiar
        # UnionType[..., NoneType()]
        return _unbound_to_instance(
            api,
            UnionType(
                [_unbound_to_instance(api, typ_arg) for typ_arg in typ.args]
                + [NoneType()]
            ),
        )

    node = api.lookup(typ.name, typ)

    if node is not None and isinstance(node, SymbolTableNode):
        bound_type = node.node

        return Instance(
            bound_type,
            [
                _unbound_to_instance(api, arg)
                if isinstance(arg, UnboundType)
                else arg
                for arg in typ.args
            ],
        )
    else:
        return typ


def _info_for_cls(cls, api):
    if cls.info is CLASSDEF_NO_INFO:
        sym = api.lookup(cls.name, cls)
        if sym.node and isinstance(sym.node, TypeInfo):
            info = sym.node
    else:
        info = cls.info

    return info
