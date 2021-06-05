from typing import Any
from typing import cast
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Optional
from typing import overload
from typing import Tuple
from typing import Type as TypingType
from typing import TypeVar
from typing import Union

from mypy.nodes import CallExpr
from mypy.nodes import ClassDef
from mypy.nodes import CLASSDEF_NO_INFO
from mypy.nodes import Context
from mypy.nodes import IfStmt
from mypy.nodes import JsonDict
from mypy.nodes import NameExpr
from mypy.nodes import Statement
from mypy.nodes import SymbolTableNode
from mypy.nodes import TypeInfo
from mypy.plugin import ClassDefContext
from mypy.plugin import DynamicClassDefContext
from mypy.plugin import SemanticAnalyzerPluginInterface
from mypy.plugins.common import deserialize_and_fixup_type
from mypy.types import Instance
from mypy.types import NoneType
from mypy.types import ProperType
from mypy.types import Type
from mypy.types import UnboundType
from mypy.types import UnionType


_TArgType = TypeVar("_TArgType", bound=Union[CallExpr, NameExpr])


class DeclClassApplied:
    def __init__(
        self,
        is_mapped: bool,
        has_table: bool,
        mapped_attr_names: Iterable[Tuple[str, ProperType]],
        mapped_mro: Iterable[Instance],
    ):
        self.is_mapped = is_mapped
        self.has_table = has_table
        self.mapped_attr_names = list(mapped_attr_names)
        self.mapped_mro = list(mapped_mro)

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
            mapped_attr_names=cast(
                List[Tuple[str, ProperType]],
                [
                    (name, deserialize_and_fixup_type(type_, api))
                    for name, type_ in data["mapped_attr_names"]
                ],
            ),
            mapped_mro=cast(
                List[Instance],
                [
                    deserialize_and_fixup_type(type_, api)
                    for type_ in data["mapped_mro"]
                ],
            ),
        )


def fail(api: SemanticAnalyzerPluginInterface, msg: str, ctx: Context) -> None:
    msg = "[SQLAlchemy Mypy plugin] %s" % msg
    return api.fail(msg, ctx)


def add_global(
    ctx: Union[ClassDefContext, DynamicClassDefContext],
    module: str,
    symbol_name: str,
    asname: str,
) -> None:
    module_globals = ctx.api.modules[ctx.api.cur_mod_id].names

    if asname not in module_globals:
        lookup_sym: SymbolTableNode = ctx.api.modules[module].names[
            symbol_name
        ]

        module_globals[asname] = lookup_sym


@overload
def _get_callexpr_kwarg(
    callexpr: CallExpr, name: str, *, expr_types: None = ...
) -> Optional[Union[CallExpr, NameExpr]]:
    ...


@overload
def _get_callexpr_kwarg(
    callexpr: CallExpr,
    name: str,
    *,
    expr_types: Tuple[TypingType[_TArgType], ...]
) -> Optional[_TArgType]:
    ...


def _get_callexpr_kwarg(
    callexpr: CallExpr,
    name: str,
    *,
    expr_types: Optional[Tuple[TypingType[Any], ...]] = None
) -> Optional[Any]:
    try:
        arg_idx = callexpr.arg_names.index(name)
    except ValueError:
        return None

    kwarg = callexpr.args[arg_idx]
    if isinstance(
        kwarg, expr_types if expr_types is not None else (NameExpr, CallExpr)
    ):
        return kwarg

    return None


def _flatten_typechecking(stmts: Iterable[Statement]) -> Iterator[Statement]:
    for stmt in stmts:
        if (
            isinstance(stmt, IfStmt)
            and isinstance(stmt.expr[0], NameExpr)
            and stmt.expr[0].fullname == "typing.TYPE_CHECKING"
        ):
            for substmt in stmt.body[0].body:
                yield substmt
        else:
            yield stmt


def _unbound_to_instance(
    api: SemanticAnalyzerPluginInterface, typ: Type
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

    node = api.lookup_qualified(typ.name, typ)

    if (
        node is not None
        and isinstance(node, SymbolTableNode)
        and isinstance(node.node, TypeInfo)
    ):
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


def _info_for_cls(
    cls: ClassDef, api: SemanticAnalyzerPluginInterface
) -> TypeInfo:
    if cls.info is CLASSDEF_NO_INFO:
        sym = api.lookup_qualified(cls.name, cls)
        assert sym and isinstance(sym.node, TypeInfo)
        return sym.node

    return cls.info
