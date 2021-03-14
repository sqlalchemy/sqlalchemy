# ext/mypy/names.py
# Copyright (C) 2021 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from typing import List

from mypy.nodes import ClassDef
from mypy.nodes import Expression
from mypy.nodes import FuncDef
from mypy.nodes import RefExpr
from mypy.nodes import SymbolNode
from mypy.nodes import TypeAlias
from mypy.nodes import TypeInfo
from mypy.nodes import Union
from mypy.plugin import SemanticAnalyzerPluginInterface
from mypy.types import UnboundType

from ... import util

COLUMN = util.symbol("COLUMN")
RELATIONSHIP = util.symbol("RELATIONSHIP")
REGISTRY = util.symbol("REGISTRY")
COLUMN_PROPERTY = util.symbol("COLUMN_PROPERTY")
TYPEENGINE = util.symbol("TYPEENGNE")
MAPPED = util.symbol("MAPPED")
DECLARATIVE_BASE = util.symbol("DECLARATIVE_BASE")
DECLARATIVE_META = util.symbol("DECLARATIVE_META")
MAPPED_DECORATOR = util.symbol("MAPPED_DECORATOR")
COLUMN_PROPERTY = util.symbol("COLUMN_PROPERTY")
SYNONYM_PROPERTY = util.symbol("SYNONYM_PROPERTY")
COMPOSITE_PROPERTY = util.symbol("COMPOSITE_PROPERTY")
DECLARED_ATTR = util.symbol("DECLARED_ATTR")
MAPPER_PROPERTY = util.symbol("MAPPER_PROPERTY")


_lookup = {
    "Column": (
        COLUMN,
        {
            "sqlalchemy.sql.schema.Column",
            "sqlalchemy.sql.Column",
        },
    ),
    "RelationshipProperty": (
        RELATIONSHIP,
        {
            "sqlalchemy.orm.relationships.RelationshipProperty",
            "sqlalchemy.orm.RelationshipProperty",
        },
    ),
    "registry": (
        REGISTRY,
        {
            "sqlalchemy.orm.decl_api.registry",
            "sqlalchemy.orm.registry",
        },
    ),
    "ColumnProperty": (
        COLUMN_PROPERTY,
        {
            "sqlalchemy.orm.properties.ColumnProperty",
            "sqlalchemy.orm.ColumnProperty",
        },
    ),
    "SynonymProperty": (
        SYNONYM_PROPERTY,
        {
            "sqlalchemy.orm.descriptor_props.SynonymProperty",
            "sqlalchemy.orm.SynonymProperty",
        },
    ),
    "CompositeProperty": (
        COMPOSITE_PROPERTY,
        {
            "sqlalchemy.orm.descriptor_props.CompositeProperty",
            "sqlalchemy.orm.CompositeProperty",
        },
    ),
    "MapperProperty": (
        MAPPER_PROPERTY,
        {
            "sqlalchemy.orm.interfaces.MapperProperty",
            "sqlalchemy.orm.MapperProperty",
        },
    ),
    "TypeEngine": (TYPEENGINE, {"sqlalchemy.sql.type_api.TypeEngine"}),
    "Mapped": (MAPPED, {"sqlalchemy.orm.attributes.Mapped"}),
    "declarative_base": (
        DECLARATIVE_BASE,
        {
            "sqlalchemy.ext.declarative.declarative_base",
            "sqlalchemy.orm.declarative_base",
            "sqlalchemy.orm.decl_api.declarative_base",
        },
    ),
    "DeclarativeMeta": (
        DECLARATIVE_META,
        {
            "sqlalchemy.ext.declarative.DeclarativeMeta",
            "sqlalchemy.orm.DeclarativeMeta",
            "sqlalchemy.orm.decl_api.DeclarativeMeta",
        },
    ),
    "mapped": (
        MAPPED_DECORATOR,
        {
            "sqlalchemy.orm.decl_api.registry.mapped",
            "sqlalchemy.orm.registry.mapped",
        },
    ),
    "declared_attr": (
        DECLARED_ATTR,
        {
            "sqlalchemy.orm.decl_api.declared_attr",
            "sqlalchemy.orm.declared_attr",
        },
    ),
}


def _mro_has_id(mro: List[TypeInfo], type_id: int):
    for mr in mro:
        check_type_id, fullnames = _lookup.get(mr.name, (None, None))
        if check_type_id == type_id:
            break
    else:
        return False

    return mr.fullname in fullnames


def _type_id_for_unbound_type(
    type_: UnboundType, cls: ClassDef, api: SemanticAnalyzerPluginInterface
) -> int:
    type_id = None

    sym = api.lookup(type_.name, type_)
    if sym is not None:
        if isinstance(sym.node, TypeAlias):
            type_id = _type_id_for_named_node(sym.node.target.type)
        elif isinstance(sym.node, TypeInfo):
            type_id = _type_id_for_named_node(sym.node)

    return type_id


def _type_id_for_callee(callee: Expression) -> int:
    if isinstance(callee.node, FuncDef):
        return _type_id_for_funcdef(callee.node)
    elif isinstance(callee.node, TypeAlias):
        type_id = _type_id_for_fullname(callee.node.target.type.fullname)
    elif isinstance(callee.node, TypeInfo):
        type_id = _type_id_for_named_node(callee)
    else:
        type_id = None
    return type_id


def _type_id_for_funcdef(node: FuncDef) -> int:
    if hasattr(node.type.ret_type, "type"):
        type_id = _type_id_for_fullname(node.type.ret_type.type.fullname)
    else:
        type_id = None
    return type_id


def _type_id_for_named_node(node: Union[RefExpr, SymbolNode]) -> int:
    type_id, fullnames = _lookup.get(node.name, (None, None))

    if type_id is None:
        return None

    elif node.fullname in fullnames:
        return type_id
    else:
        return None


def _type_id_for_fullname(fullname: str) -> int:
    tokens = fullname.split(".")
    immediate = tokens[-1]

    type_id, fullnames = _lookup.get(immediate, (None, None))

    if type_id is None:
        return None

    elif fullname in fullnames:
        return type_id
    else:
        return None
