# ext/mypy/plugin.py
# Copyright (C) 2021 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""
Mypy plugin for SQLAlchemy ORM.

"""
from typing import List
from typing import Tuple
from typing import Type

from mypy import nodes
from mypy.mro import calculate_mro
from mypy.mro import MroError
from mypy.nodes import Block
from mypy.nodes import ClassDef
from mypy.nodes import GDEF
from mypy.nodes import MypyFile
from mypy.nodes import NameExpr
from mypy.nodes import SymbolTable
from mypy.nodes import SymbolTableNode
from mypy.nodes import TypeInfo
from mypy.plugin import AttributeContext
from mypy.plugin import Callable
from mypy.plugin import ClassDefContext
from mypy.plugin import DynamicClassDefContext
from mypy.plugin import Optional
from mypy.plugin import Plugin
from mypy.types import Instance

from . import decl_class
from . import names
from . import util


class CustomPlugin(Plugin):
    def get_dynamic_class_hook(
        self, fullname: str
    ) -> Optional[Callable[[DynamicClassDefContext], None]]:
        if names._type_id_for_fullname(fullname) is names.DECLARATIVE_BASE:
            return _dynamic_class_hook
        return None

    def get_base_class_hook(
        self, fullname: str
    ) -> Optional[Callable[[ClassDefContext], None]]:

        # kind of a strange relationship between get_metaclass_hook()
        # and get_base_class_hook().  the former doesn't fire off for
        # subclasses.   but then you can just check it here from the "base"
        # and get the same effect.
        sym = self.lookup_fully_qualified(fullname)
        if (
            sym
            and isinstance(sym.node, TypeInfo)
            and sym.node.metaclass_type
            and names._type_id_for_named_node(sym.node.metaclass_type.type)
            is names.DECLARATIVE_META
        ):
            return _base_cls_hook
        return None

    def get_class_decorator_hook(
        self, fullname: str
    ) -> Optional[Callable[[ClassDefContext], None]]:

        sym = self.lookup_fully_qualified(fullname)

        if (
            sym is not None
            and names._type_id_for_named_node(sym.node)
            is names.MAPPED_DECORATOR
        ):
            return _cls_decorator_hook
        return None

    def get_customize_class_mro_hook(
        self, fullname: str
    ) -> Optional[Callable[[ClassDefContext], None]]:
        return _fill_in_decorators

    def get_attribute_hook(
        self, fullname: str
    ) -> Optional[Callable[[AttributeContext], Type]]:
        if fullname.startswith(
            "sqlalchemy.orm.attributes.QueryableAttribute."
        ):
            return _queryable_getattr_hook
        return None

    def get_additional_deps(
        self, file: MypyFile
    ) -> List[Tuple[int, str, int]]:
        return [
            (10, "sqlalchemy.orm.attributes", -1),
            (10, "sqlalchemy.orm.decl_api", -1),
        ]


def plugin(version: str):
    return CustomPlugin


def _queryable_getattr_hook(ctx: AttributeContext) -> Type:
    # how do I....tell it it has no attribute of a certain name?
    # can't find any Type that seems to match that
    return ctx.default_attr_type


def _fill_in_decorators(ctx: ClassDefContext) -> None:
    for decorator in ctx.cls.decorators:
        # set the ".fullname" attribute of a class decorator
        # that is a MemberExpr.   This causes the logic in
        # semanal.py->apply_class_plugin_hooks to invoke the
        # get_class_decorator_hook for our "registry.map_class()" method.
        # this seems like a bug in mypy that these decorators are otherwise
        # skipped.
        if (
            isinstance(decorator, nodes.MemberExpr)
            and decorator.name == "mapped"
        ):

            sym = ctx.api.lookup(
                decorator.expr.name, decorator, suppress_errors=True
            )
            if sym:
                if sym.node.type and hasattr(sym.node.type, "type"):
                    decorator.fullname = (
                        f"{sym.node.type.type.fullname}.{decorator.name}"
                    )
                else:
                    # if the registry is in the same file as where the
                    # decorator is used, it might not have semantic
                    # symbols applied and we can't get a fully qualified
                    # name or an inferred type, so we are actually going to
                    # flag an error in this case that they need to annotate
                    # it.  The "registry" is declared just
                    # once (or few times), so they have to just not use
                    # type inference for its assignment in this one case.
                    util.fail(
                        ctx.api,
                        "Class decorator called mapped(), but we can't "
                        "tell if it's from an ORM registry.  Please "
                        "annotate the registry assignment, e.g. "
                        "my_registry: registry = registry()",
                        sym.node,
                    )


def _cls_metadata_hook(ctx: ClassDefContext) -> None:
    decl_class._scan_declarative_assignments_and_apply_types(ctx.cls, ctx.api)


def _base_cls_hook(ctx: ClassDefContext) -> None:
    decl_class._scan_declarative_assignments_and_apply_types(ctx.cls, ctx.api)


def _cls_decorator_hook(ctx: ClassDefContext) -> None:
    assert isinstance(ctx.reason, nodes.MemberExpr)
    expr = ctx.reason.expr
    assert names._type_id_for_named_node(expr.node.type.type) is names.REGISTRY

    decl_class._scan_declarative_assignments_and_apply_types(ctx.cls, ctx.api)


def _dynamic_class_hook(ctx: DynamicClassDefContext) -> None:
    """Generate a declarative Base class when the declarative_base() function
    is encountered."""

    cls = ClassDef(ctx.name, Block([]))
    cls.fullname = ctx.api.qualified_name(ctx.name)

    declarative_meta_sym: SymbolTableNode = ctx.api.modules[
        "sqlalchemy.orm.decl_api"
    ].names["DeclarativeMeta"]
    declarative_meta_typeinfo: TypeInfo = declarative_meta_sym.node

    declarative_meta_name: NameExpr = NameExpr("DeclarativeMeta")
    declarative_meta_name.kind = GDEF
    declarative_meta_name.fullname = "sqlalchemy.orm.decl_api.DeclarativeMeta"
    declarative_meta_name.node = declarative_meta_typeinfo

    cls.metaclass = declarative_meta_name

    declarative_meta_instance = Instance(declarative_meta_typeinfo, [])

    info = TypeInfo(SymbolTable(), cls, ctx.api.cur_mod_id)
    info.declared_metaclass = info.metaclass_type = declarative_meta_instance
    cls.info = info

    cls_arg = util._get_callexpr_kwarg(ctx.call, "cls")
    if cls_arg is not None:
        decl_class._scan_declarative_assignments_and_apply_types(
            cls_arg.node.defn, ctx.api, is_mixin_scan=True
        )
        info.bases = [Instance(cls_arg.node, [])]
    else:
        obj = ctx.api.builtin_type("builtins.object")

        info.bases = [obj]

    try:
        calculate_mro(info)
    except MroError:
        util.fail(
            ctx.api, "Not able to calculate MRO for declarative base", ctx.call
        )
        info.bases = [obj]
        info.fallback_to_any = True

    ctx.api.add_symbol_table_node(ctx.name, SymbolTableNode(GDEF, info))
