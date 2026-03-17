# sql/_annotated_cols.py
# Copyright (C) 2005-2026 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
from __future__ import annotations

from typing import Any
from typing import Generic
from typing import Literal
from typing import NoReturn
from typing import overload
from typing import Protocol
from typing import TYPE_CHECKING

from . import sqltypes
from ._typing import _T
from ._typing import _Ts
from .base import _NoArg
from .base import ReadOnlyColumnCollection
from .. import util
from ..exc import ArgumentError
from ..exc import InvalidRequestError
from ..util import typing as sa_typing
from ..util.langhelpers import dunders_re
from ..util.typing import Never
from ..util.typing import Self
from ..util.typing import TypeVar
from ..util.typing import Unpack

if TYPE_CHECKING:
    from .elements import ColumnClause  # noqa (for zimports)
    from .elements import KeyedColumnElement  # noqa (for zimports)
    from .schema import Column
    from .type_api import TypeEngine
    from ..util.typing import _AnnotationScanType


class Named(Generic[_T]):
    """A named descriptor that is interpreted by SQLAlchemy in various ways.

    .. seealso::

        :class:`_schema.TypedColumns` Define table columns using this
        descriptor.

    .. versionadded:: 2.1.0b2
    """

    __slots__ = ()

    key: str
    if TYPE_CHECKING:

        # NOTE: this overload prevents users from using the a TypedColumns
        # class like if it were an orm mapped class
        @overload
        def __get__(self, instance: None, owner: Any) -> Never: ...

        @overload
        def __get__(
            self, instance: TypedColumns, owner: Any
        ) -> Column[_T]: ...
        @overload
        def __get__(self, instance: Any, owner: Any) -> Self: ...

        def __get__(self, instance: object | None, owner: Any) -> Any: ...


# NOTE: TypedColumns subclasses are ignored by the ORM mapping process
class TypedColumns(ReadOnlyColumnCollection[str, "Column[Any]"]):
    """Class that generally represent the typed columns of a :class:`.Table`,
    but can be used with most :class:`_sql.FromClause` subclasses with the
    :meth:`_sql.FromClause.with_cols` method.

    This is a "typing only" class that is never instantiated at runtime: the
    type checker will think that this class is exposed as the ``table.c``
    attribute, but in reality a normal :class:`_schema.ColumnCollection` is
    used at runtime.

    Subclasses should just list the columns as class attributes, without
    specifying method or other non column members.

    To resolve the columns, a simplified version of the ORM logic is used,
    in particular, columns can be declared by:

    * directly instantiating them, to declare constraint, custom SQL types and
      additional column options;
    * using only a :class:`.Named` or :class:`_schema.Column` type annotation,
      where nullability and SQL type will be inferred by the python type
      provided.
      Type inference is available for a common subset of python types.
    * a mix of both, where the instance can be used to declare
      constraints and other column options while the annotation will be used
      to set the SQL type and nullability if not provided by the instance.

    In all cases the name is inferred from the attribute name, unless
    explicitly provided.

    .. note::

        The generated table will create a copy of any column instance assigned
        as attributes of this class, so columns should be accessed only via
        the ``table.c`` collection, not using this class directly.

    Example of the inference behavior::

        from sqlalchemy import Column, Integer, Named, String, TypedColumns


        class tbl_cols(TypedColumns):
            # the name will be set to ``id``, type is inferred as Column[int]
            id = Column(Integer, primary_key=True)

            # not null String column is generated
            name: Named[str]

            # nullable Double column is generated
            weight: Named[float | None]

            # nullable Integer column, with sql name 'user_age'
            age: Named[int | None] = Column("user_age")

            # not null column with type String(42)
            middle_name: Named[str] = Column(String(42))

    Mixins and subclasses are also supported::

        class with_id(TypedColumns):
            id = Column(Integer, primary_key=True)


        class named_cols(TypedColumns):
            name: Named[str]
            description: Named[str | None]


        class product_cols(named_cols, with_id):
            ean: Named[str] = Column(unique=True)


        product = Table("product", metadata, product_cols)


        class office_cols(named_cols, with_id):
            address: Named[str]


        office = Table("office", metadata, office_cols)

    The positional types returned when selecting the table can
    be optionally declared by specifying a :attr:`.HasRowPos.__row_pos__`
    annotation::

        from sqlalchemy import select


        class some_cols(TypedColumns):
            id = Column(Integer, primary_key=True)
            name: Named[str]
            weight: Named[float | None]

            __row_pos__: tuple[int, str, float | None]


        some_table = Table("st", metadata, some_cols)

        # both will be typed as Select[int, str, float | None]
        stmt1 = some_table.select()
        stmt2 = select(some_table)

    .. seealso::

        :class:`.Table` for usage details on how to use this class to
        create a table instance.

        :meth:`_sql.FromClause.with_cols` to apply a :class:`.TypedColumns`
        to a from clause.

    .. versionadded:: 2.1.0b2
    """  # noqa

    __slots__ = ()

    if not TYPE_CHECKING:

        def __new__(cls, *args: Any, **kwargs: Any) -> NoReturn:
            raise InvalidRequestError(
                "Cannot instantiate a TypedColumns object."
            )

    def __init_subclass__(cls) -> None:
        methods = {
            name
            for name, value in cls.__dict__.items()
            if not dunders_re.match(name) and callable(value)
        }
        if methods:
            raise InvalidRequestError(
                "TypedColumns subclasses may not define methods. "
                f"Found {sorted(methods)}"
            )


_KeyColCC_co = TypeVar(
    "_KeyColCC_co",
    bound=ReadOnlyColumnCollection[str, "KeyedColumnElement[Any]"],
    covariant=True,
    default=ReadOnlyColumnCollection[str, "KeyedColumnElement[Any]"],
)
_ColClauseCC_co = TypeVar(
    "_ColClauseCC_co",
    bound=ReadOnlyColumnCollection[str, "ColumnClause[Any]"],
    covariant=True,
    default=ReadOnlyColumnCollection[str, "ColumnClause[Any]"],
)
_ColCC_co = TypeVar(
    "_ColCC_co",
    bound=ReadOnlyColumnCollection[str, "Column[Any]"],
    covariant=True,
    default=ReadOnlyColumnCollection[str, "Column[Any]"],
)

_TC = TypeVar("_TC", bound=TypedColumns)
_TC_co = TypeVar("_TC_co", bound=TypedColumns, covariant=True)


class HasRowPos(Protocol[Unpack[_Ts]]):
    """Protocol for a :class:`_schema.TypedColumns` used to indicate the
    positional types will be returned when selecting the table.

    .. versionadded:: 2.1.0b2
    """

    __row_pos__: tuple[Unpack[_Ts]]
    """A tuple that represents the types that will be returned when
    selecting from the table.
    """


@util.preload_module("sqlalchemy.sql.schema")
def _extract_columns_from_class(
    table_columns_cls: type[TypedColumns],
) -> list[Column[Any]]:
    columns: dict[str, Column[Any]] = {}

    Column = util.preloaded.sql_schema.Column
    NULL_UNSPECIFIED = util.preloaded.sql_schema.NULL_UNSPECIFIED

    for base in table_columns_cls.__mro__[::-1]:
        if base in TypedColumns.__mro__:
            continue

        # _ClassScanAbstractConfig._cls_attr_resolver
        cls_annotations = util.get_annotations(base)
        cls_vars = vars(base)
        items = [
            (n, cls_vars.get(n), cls_annotations.get(n))
            for n in util.merge_lists_w_ordering(
                list(cls_vars), list(cls_annotations)
            )
            if not dunders_re.match(n)
        ]
        # --
        for name, obj, annotation in items:
            if obj is None:
                assert annotation is not None
                # no attribute, just annotation
                extracted_type = _collect_annotation(
                    table_columns_cls, name, base.__module__, annotation
                )
                if extracted_type is _NoArg.NO_ARG:
                    raise ArgumentError(
                        "No type information could be extracted from "
                        f"annotation {annotation} for attribute "
                        f"'{base.__name__}.{name}'"
                    )
                sqltype = _get_sqltype(extracted_type)
                if sqltype is None:
                    raise ArgumentError(
                        f"Could not find a SQL type for type {extracted_type} "
                        f"obtained from annotation {annotation} in "
                        f"attribute '{base.__name__}.{name}'"
                    )
                columns[name] = Column(
                    name,
                    sqltype,
                    nullable=sa_typing.includes_none(extracted_type),
                )
            elif isinstance(obj, Column):
                # has attribute attribute
                # _DeclarativeMapperConfig._produce_column_copies
                # as with orm this case is not supported
                for fk in obj.foreign_keys:
                    if (
                        fk._table_column is not None
                        and fk._table_column.table is None
                    ):
                        raise InvalidRequestError(
                            f"Column '{base.__name__}.{name}' with foreign "
                            "key to non-table-bound columns is not supported "
                            "when using a TypedColumns. If possible use the "
                            "qualified string name the column"
                        )

                col = obj._copy()
                # MapptedColumn.declarative_scan
                if col.key == col.name and col.key != name:
                    col.key = name
                if col.name is None:
                    col.name = name

                sqltype = col.type
                anno_sqltype = None
                nullable: Literal[_NoArg.NO_ARG] | bool = _NoArg.NO_ARG
                if annotation is not None:
                    # there is an annotation, extract the type
                    extracted_type = _collect_annotation(
                        table_columns_cls, name, base.__module__, annotation
                    )
                    if extracted_type is not _NoArg.NO_ARG:
                        anno_sqltype = _get_sqltype(extracted_type)
                        nullable = sa_typing.includes_none(extracted_type)

                if sqltype._isnull:
                    if anno_sqltype is None and not col.foreign_keys:
                        raise ArgumentError(
                            "Python typing annotation is required for "
                            f"attribute '{base.__name__}.{name}' when "
                            "primary argument(s) for Column construct are "
                            "None or not present"
                        )
                    elif anno_sqltype is not None:
                        col._set_type(anno_sqltype)

                if (
                    nullable is not _NoArg.NO_ARG
                    and col._user_defined_nullable is NULL_UNSPECIFIED
                    and not col.primary_key
                ):
                    col.nullable = nullable
                columns[name] = col
            else:
                raise ArgumentError(
                    f"Unexpected value for attribute '{base.__name__}.{name}'"
                    f". Expected a Column, not: {type(obj)}"
                )

    # Return columns as a list
    return list(columns.values())


@util.preload_module("sqlalchemy.sql.schema")
def _collect_annotation(
    cls: type[Any], name: str, module: str, raw_annotation: _AnnotationScanType
) -> _AnnotationScanType | Literal[_NoArg.NO_ARG]:
    Column = util.preloaded.sql_schema.Column

    _locals = {"Column": Column, "Named": Named}
    # _ClassScanAbstractConfig._collect_annotation & _extract_mapped_subtype
    try:
        annotation = sa_typing.de_stringify_annotation(
            cls, raw_annotation, module, _locals
        )
    except Exception as e:
        raise ArgumentError(
            f"Could not interpret annotation {raw_annotation} for "
            f"attribute '{cls.__name__}.{name}'"
        ) from e

    if (
        not sa_typing.is_generic(annotation)
        and isinstance(annotation, type)
        and issubclass(annotation, (Column, Named))
    ):
        # no generic information, ignore
        return _NoArg.NO_ARG
    elif not sa_typing.is_origin_of_cls(annotation, (Column, Named)):
        raise ArgumentError(
            f"Annotation {raw_annotation} for attribute "
            f"'{cls.__name__}.{name}' is not of type Named/Column[...]"
        )
    else:
        assert len(annotation.__args__) == 1  # Column[int, int] raises
        return annotation.__args__[0]  # type: ignore[no-any-return]


def _get_sqltype(annotation: _AnnotationScanType) -> TypeEngine[Any] | None:
    our_type = sa_typing.de_optionalize_union_types(annotation)
    # simplified version of registry._resolve_type given no customizable
    # type map
    sql_type = sqltypes._type_map_get(our_type)  # type: ignore[arg-type]
    if sql_type is not None and not sql_type._isnull:
        return sqltypes.to_instance(sql_type)
    else:
        return None
