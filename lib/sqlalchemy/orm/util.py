# orm/util.py
# Copyright (C) 2005-2022 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

import re
import types
import typing
from typing import Any
from typing import Generic
from typing import Optional
from typing import overload
from typing import Tuple
from typing import Type
from typing import TypeVar
from typing import Union
import weakref

from . import attributes  # noqa
from .base import _class_to_mapper  # noqa
from .base import _IdentityKeyType
from .base import _never_set  # noqa
from .base import _none_set  # noqa
from .base import attribute_str  # noqa
from .base import class_mapper  # noqa
from .base import InspectionAttr  # noqa
from .base import instance_str  # noqa
from .base import object_mapper  # noqa
from .base import object_state  # noqa
from .base import state_attribute_str  # noqa
from .base import state_class_str  # noqa
from .base import state_str  # noqa
from .interfaces import CriteriaOption
from .interfaces import MapperProperty  # noqa
from .interfaces import ORMColumnsClauseRole
from .interfaces import ORMEntityColumnsClauseRole
from .interfaces import ORMFromClauseRole
from .interfaces import PropComparator  # noqa
from .path_registry import PathRegistry  # noqa
from .. import event
from .. import exc as sa_exc
from .. import inspection
from .. import sql
from .. import util
from ..engine.result import result_tuple
from ..sql import base as sql_base
from ..sql import coercions
from ..sql import expression
from ..sql import lambdas
from ..sql import roles
from ..sql import util as sql_util
from ..sql import visitors
from ..sql.annotation import SupportsCloneAnnotations
from ..sql.base import ColumnCollection
from ..sql.selectable import FromClause
from ..util.langhelpers import MemoizedSlots
from ..util.typing import de_stringify_annotation
from ..util.typing import is_origin_of

if typing.TYPE_CHECKING:
    from .mapper import Mapper
    from ..engine import Row
    from ..sql.selectable import Alias

_T = TypeVar("_T", bound=Any)

all_cascades = frozenset(
    (
        "delete",
        "delete-orphan",
        "all",
        "merge",
        "expunge",
        "save-update",
        "refresh-expire",
        "none",
    )
)


class CascadeOptions(frozenset):
    """Keeps track of the options sent to
    :paramref:`.relationship.cascade`"""

    _add_w_all_cascades = all_cascades.difference(
        ["all", "none", "delete-orphan"]
    )
    _allowed_cascades = all_cascades

    _viewonly_cascades = ["expunge", "all", "none", "refresh-expire"]

    __slots__ = (
        "save_update",
        "delete",
        "refresh_expire",
        "merge",
        "expunge",
        "delete_orphan",
    )

    def __new__(cls, value_list):
        if isinstance(value_list, str) or value_list is None:
            return cls.from_string(value_list)
        values = set(value_list)
        if values.difference(cls._allowed_cascades):
            raise sa_exc.ArgumentError(
                "Invalid cascade option(s): %s"
                % ", ".join(
                    [
                        repr(x)
                        for x in sorted(
                            values.difference(cls._allowed_cascades)
                        )
                    ]
                )
            )

        if "all" in values:
            values.update(cls._add_w_all_cascades)
        if "none" in values:
            values.clear()
        values.discard("all")

        self = frozenset.__new__(CascadeOptions, values)
        self.save_update = "save-update" in values
        self.delete = "delete" in values
        self.refresh_expire = "refresh-expire" in values
        self.merge = "merge" in values
        self.expunge = "expunge" in values
        self.delete_orphan = "delete-orphan" in values

        if self.delete_orphan and not self.delete:
            util.warn(
                "The 'delete-orphan' cascade " "option requires 'delete'."
            )
        return self

    def __repr__(self):
        return "CascadeOptions(%r)" % (",".join([x for x in sorted(self)]))

    @classmethod
    def from_string(cls, arg):
        values = [c for c in re.split(r"\s*,\s*", arg or "") if c]
        return cls(values)


def _validator_events(desc, key, validator, include_removes, include_backrefs):
    """Runs a validation method on an attribute value to be set or
    appended.
    """

    if not include_backrefs:

        def detect_is_backref(state, initiator):
            impl = state.manager[key].impl
            return initiator.impl is not impl

    if include_removes:

        def append(state, value, initiator):
            if initiator.op is not attributes.OP_BULK_REPLACE and (
                include_backrefs or not detect_is_backref(state, initiator)
            ):
                return validator(state.obj(), key, value, False)
            else:
                return value

        def bulk_set(state, values, initiator):
            if include_backrefs or not detect_is_backref(state, initiator):
                obj = state.obj()
                values[:] = [
                    validator(obj, key, value, False) for value in values
                ]

        def set_(state, value, oldvalue, initiator):
            if include_backrefs or not detect_is_backref(state, initiator):
                return validator(state.obj(), key, value, False)
            else:
                return value

        def remove(state, value, initiator):
            if include_backrefs or not detect_is_backref(state, initiator):
                validator(state.obj(), key, value, True)

    else:

        def append(state, value, initiator):
            if initiator.op is not attributes.OP_BULK_REPLACE and (
                include_backrefs or not detect_is_backref(state, initiator)
            ):
                return validator(state.obj(), key, value)
            else:
                return value

        def bulk_set(state, values, initiator):
            if include_backrefs or not detect_is_backref(state, initiator):
                obj = state.obj()
                values[:] = [validator(obj, key, value) for value in values]

        def set_(state, value, oldvalue, initiator):
            if include_backrefs or not detect_is_backref(state, initiator):
                return validator(state.obj(), key, value)
            else:
                return value

    event.listen(desc, "append", append, raw=True, retval=True)
    event.listen(desc, "bulk_replace", bulk_set, raw=True)
    event.listen(desc, "set", set_, raw=True, retval=True)
    if include_removes:
        event.listen(desc, "remove", remove, raw=True, retval=True)


def polymorphic_union(
    table_map, typecolname, aliasname="p_union", cast_nulls=True
):
    """Create a ``UNION`` statement used by a polymorphic mapper.

    See  :ref:`concrete_inheritance` for an example of how
    this is used.

    :param table_map: mapping of polymorphic identities to
     :class:`_schema.Table` objects.
    :param typecolname: string name of a "discriminator" column, which will be
     derived from the query, producing the polymorphic identity for
     each row.  If ``None``, no polymorphic discriminator is generated.
    :param aliasname: name of the :func:`~sqlalchemy.sql.expression.alias()`
     construct generated.
    :param cast_nulls: if True, non-existent columns, which are represented
     as labeled NULLs, will be passed into CAST.   This is a legacy behavior
     that is problematic on some backends such as Oracle - in which case it
     can be set to False.

    """

    colnames = util.OrderedSet()
    colnamemaps = {}
    types = {}
    for key in table_map:
        table = table_map[key]

        table = coercions.expect(
            roles.StrictFromClauseRole, table, allow_select=True
        )
        table_map[key] = table

        m = {}
        for c in table.c:
            if c.key == typecolname:
                raise sa_exc.InvalidRequestError(
                    "Polymorphic union can't use '%s' as the discriminator "
                    "column due to mapped column %r; please apply the "
                    "'typecolname' "
                    "argument; this is available on "
                    "ConcreteBase as '_concrete_discriminator_name'"
                    % (typecolname, c)
                )
            colnames.add(c.key)
            m[c.key] = c
            types[c.key] = c.type
        colnamemaps[table] = m

    def col(name, table):
        try:
            return colnamemaps[table][name]
        except KeyError:
            if cast_nulls:
                return sql.cast(sql.null(), types[name]).label(name)
            else:
                return sql.type_coerce(sql.null(), types[name]).label(name)

    result = []
    for type_, table in table_map.items():
        if typecolname is not None:
            result.append(
                sql.select(
                    *(
                        [col(name, table) for name in colnames]
                        + [
                            sql.literal_column(
                                sql_util._quote_ddl_expr(type_)
                            ).label(typecolname)
                        ]
                    )
                ).select_from(table)
            )
        else:
            result.append(
                sql.select(
                    *[col(name, table) for name in colnames]
                ).select_from(table)
            )
    return sql.union_all(*result).alias(aliasname)


@overload
def identity_key(
    class_: type, ident: Tuple[Any, ...], *, identity_token: Optional[str]
) -> _IdentityKeyType:
    ...


@overload
def identity_key(*, instance: Any) -> _IdentityKeyType:
    ...


@overload
def identity_key(
    class_: type, *, row: "Row", identity_token: Optional[str]
) -> _IdentityKeyType:
    ...


def identity_key(
    class_=None, ident=None, *, instance=None, row=None, identity_token=None
) -> _IdentityKeyType:
    r"""Generate "identity key" tuples, as are used as keys in the
    :attr:`.Session.identity_map` dictionary.

    This function has several call styles:

    * ``identity_key(class, ident, identity_token=token)``

      This form receives a mapped class and a primary key scalar or
      tuple as an argument.

      E.g.::

        >>> identity_key(MyClass, (1, 2))
        (<class '__main__.MyClass'>, (1, 2), None)

      :param class: mapped class (must be a positional argument)
      :param ident: primary key, may be a scalar or tuple argument.
      :param identity_token: optional identity token

        .. versionadded:: 1.2 added identity_token


    * ``identity_key(instance=instance)``

      This form will produce the identity key for a given instance.  The
      instance need not be persistent, only that its primary key attributes
      are populated (else the key will contain ``None`` for those missing
      values).

      E.g.::

        >>> instance = MyClass(1, 2)
        >>> identity_key(instance=instance)
        (<class '__main__.MyClass'>, (1, 2), None)

      In this form, the given instance is ultimately run though
      :meth:`_orm.Mapper.identity_key_from_instance`, which will have the
      effect of performing a database check for the corresponding row
      if the object is expired.

      :param instance: object instance (must be given as a keyword arg)

    * ``identity_key(class, row=row, identity_token=token)``

      This form is similar to the class/tuple form, except is passed a
      database result row as a :class:`.Row` object.

      E.g.::

        >>> row = engine.execute(\
            text("select * from table where a=1 and b=2")\
            ).first()
        >>> identity_key(MyClass, row=row)
        (<class '__main__.MyClass'>, (1, 2), None)

      :param class: mapped class (must be a positional argument)
      :param row: :class:`.Row` row returned by a :class:`_engine.CursorResult`
       (must be given as a keyword arg)
      :param identity_token: optional identity token

        .. versionadded:: 1.2 added identity_token

    """
    if class_ is not None:
        mapper = class_mapper(class_)
        if row is None:
            if ident is None:
                raise sa_exc.ArgumentError("ident or row is required")
            return mapper.identity_key_from_primary_key(
                util.to_list(ident), identity_token=identity_token
            )
        else:
            return mapper.identity_key_from_row(
                row, identity_token=identity_token
            )
    elif instance is not None:
        mapper = object_mapper(instance)
        return mapper.identity_key_from_instance(instance)
    else:
        raise sa_exc.ArgumentError("class or instance is required")


class ORMAdapter(sql_util.ColumnAdapter):
    """ColumnAdapter subclass which excludes adaptation of entities from
    non-matching mappers.

    """

    is_aliased_class = False
    aliased_insp = None

    def __init__(
        self,
        entity,
        equivalents=None,
        adapt_required=False,
        allow_label_resolve=True,
        anonymize_labels=False,
    ):
        info = inspection.inspect(entity)

        self.mapper = info.mapper
        selectable = info.selectable
        if info.is_aliased_class:
            self.is_aliased_class = True
            self.aliased_insp = info

        sql_util.ColumnAdapter.__init__(
            self,
            selectable,
            equivalents,
            adapt_required=adapt_required,
            allow_label_resolve=allow_label_resolve,
            anonymize_labels=anonymize_labels,
            include_fn=self._include_fn,
        )

    def _include_fn(self, elem):
        entity = elem._annotations.get("parentmapper", None)
        return not entity or entity.isa(self.mapper)


class AliasedClass(inspection.Inspectable["AliasedInsp"], Generic[_T]):
    r"""Represents an "aliased" form of a mapped class for usage with Query.

    The ORM equivalent of a :func:`~sqlalchemy.sql.expression.alias`
    construct, this object mimics the mapped class using a
    ``__getattr__`` scheme and maintains a reference to a
    real :class:`~sqlalchemy.sql.expression.Alias` object.

    A primary purpose of :class:`.AliasedClass` is to serve as an alternate
    within a SQL statement generated by the ORM, such that an existing
    mapped entity can be used in multiple contexts.   A simple example::

        # find all pairs of users with the same name
        user_alias = aliased(User)
        session.query(User, user_alias).\
                        join((user_alias, User.id > user_alias.id)).\
                        filter(User.name == user_alias.name)

    :class:`.AliasedClass` is also capable of mapping an existing mapped
    class to an entirely new selectable, provided this selectable is column-
    compatible with the existing mapped selectable, and it can also be
    configured in a mapping as the target of a :func:`_orm.relationship`.
    See the links below for examples.

    The :class:`.AliasedClass` object is constructed typically using the
    :func:`_orm.aliased` function.   It also is produced with additional
    configuration when using the :func:`_orm.with_polymorphic` function.

    The resulting object is an instance of :class:`.AliasedClass`.
    This object implements an attribute scheme which produces the
    same attribute and method interface as the original mapped
    class, allowing :class:`.AliasedClass` to be compatible
    with any attribute technique which works on the original class,
    including hybrid attributes (see :ref:`hybrids_toplevel`).

    The :class:`.AliasedClass` can be inspected for its underlying
    :class:`_orm.Mapper`, aliased selectable, and other information
    using :func:`_sa.inspect`::

        from sqlalchemy import inspect
        my_alias = aliased(MyClass)
        insp = inspect(my_alias)

    The resulting inspection object is an instance of :class:`.AliasedInsp`.


    .. seealso::

        :func:`.aliased`

        :func:`.with_polymorphic`

        :ref:`relationship_aliased_class`

        :ref:`relationship_to_window_function`


    """

    def __init__(
        self,
        mapped_class_or_ac: Union[Type[_T], "Mapper[_T]", "AliasedClass[_T]"],
        alias=None,
        name=None,
        flat=False,
        adapt_on_names=False,
        #  TODO: None for default here?
        with_polymorphic_mappers=(),
        with_polymorphic_discriminator=None,
        base_alias=None,
        use_mapper_path=False,
        represents_outer_join=False,
    ):
        insp = inspection.inspect(mapped_class_or_ac)
        mapper = insp.mapper

        nest_adapters = False

        if alias is None:
            if insp.is_aliased_class and insp.selectable._is_subquery:
                alias = insp.selectable.alias()
            else:
                alias = (
                    mapper._with_polymorphic_selectable._anonymous_fromclause(
                        name=name,
                        flat=flat,
                    )
                )
        elif insp.is_aliased_class:
            nest_adapters = True

        self._aliased_insp = AliasedInsp(
            self,
            insp,
            alias,
            name,
            with_polymorphic_mappers
            if with_polymorphic_mappers
            else mapper.with_polymorphic_mappers,
            with_polymorphic_discriminator
            if with_polymorphic_discriminator is not None
            else mapper.polymorphic_on,
            base_alias,
            use_mapper_path,
            adapt_on_names,
            represents_outer_join,
            nest_adapters,
        )

        self.__name__ = f"aliased({mapper.class_.__name__})"

    @classmethod
    def _reconstitute_from_aliased_insp(cls, aliased_insp):
        obj = cls.__new__(cls)
        obj.__name__ = f"aliased({aliased_insp.mapper.class_.__name__})"
        obj._aliased_insp = aliased_insp

        if aliased_insp._is_with_polymorphic:
            for sub_aliased_insp in aliased_insp._with_polymorphic_entities:
                if sub_aliased_insp is not aliased_insp:
                    ent = AliasedClass._reconstitute_from_aliased_insp(
                        sub_aliased_insp
                    )
                    setattr(obj, sub_aliased_insp.class_.__name__, ent)

        return obj

    def __getattr__(self, key):
        try:
            _aliased_insp = self.__dict__["_aliased_insp"]
        except KeyError:
            raise AttributeError()
        else:
            target = _aliased_insp._target
            # maintain all getattr mechanics
            attr = getattr(target, key)

        # attribute is a method, that will be invoked against a
        # "self"; so just return a new method with the same function and
        # new self
        if hasattr(attr, "__call__") and hasattr(attr, "__self__"):
            return types.MethodType(attr.__func__, self)

        # attribute is a descriptor, that will be invoked against a
        # "self"; so invoke the descriptor against this self
        if hasattr(attr, "__get__"):
            attr = attr.__get__(None, self)

        # attributes within the QueryableAttribute system will want this
        # to be invoked so the object can be adapted
        if hasattr(attr, "adapt_to_entity"):
            attr = attr.adapt_to_entity(_aliased_insp)
            setattr(self, key, attr)

        return attr

    def _get_from_serialized(self, key, mapped_class, aliased_insp):
        # this method is only used in terms of the
        # sqlalchemy.ext.serializer extension
        attr = getattr(mapped_class, key)
        if hasattr(attr, "__call__") and hasattr(attr, "__self__"):
            return types.MethodType(attr.__func__, self)

        # attribute is a descriptor, that will be invoked against a
        # "self"; so invoke the descriptor against this self
        if hasattr(attr, "__get__"):
            attr = attr.__get__(None, self)

        # attributes within the QueryableAttribute system will want this
        # to be invoked so the object can be adapted
        if hasattr(attr, "adapt_to_entity"):
            aliased_insp._weak_entity = weakref.ref(self)
            attr = attr.adapt_to_entity(aliased_insp)
            setattr(self, key, attr)

        return attr

    def __repr__(self):
        return "<AliasedClass at 0x%x; %s>" % (
            id(self),
            self._aliased_insp._target.__name__,
        )

    def __str__(self):
        return str(self._aliased_insp)


class AliasedInsp(
    ORMEntityColumnsClauseRole,
    ORMFromClauseRole,
    sql_base.HasCacheKey,
    roles.HasFromClauseElement,
    InspectionAttr,
    MemoizedSlots,
):
    """Provide an inspection interface for an
    :class:`.AliasedClass` object.

    The :class:`.AliasedInsp` object is returned
    given an :class:`.AliasedClass` using the
    :func:`_sa.inspect` function::

        from sqlalchemy import inspect
        from sqlalchemy.orm import aliased

        my_alias = aliased(MyMappedClass)
        insp = inspect(my_alias)

    Attributes on :class:`.AliasedInsp`
    include:

    * ``entity`` - the :class:`.AliasedClass` represented.
    * ``mapper`` - the :class:`_orm.Mapper` mapping the underlying class.
    * ``selectable`` - the :class:`_expression.Alias`
      construct which ultimately
      represents an aliased :class:`_schema.Table` or
      :class:`_expression.Select`
      construct.
    * ``name`` - the name of the alias.  Also is used as the attribute
      name when returned in a result tuple from :class:`_query.Query`.
    * ``with_polymorphic_mappers`` - collection of :class:`_orm.Mapper`
      objects
      indicating all those mappers expressed in the select construct
      for the :class:`.AliasedClass`.
    * ``polymorphic_on`` - an alternate column or SQL expression which
      will be used as the "discriminator" for a polymorphic load.

    .. seealso::

        :ref:`inspection_toplevel`

    """

    __slots__ = (
        "__weakref__",
        "_weak_entity",
        "mapper",
        "selectable",
        "name",
        "_adapt_on_names",
        "with_polymorphic_mappers",
        "polymorphic_on",
        "_use_mapper_path",
        "_base_alias",
        "represents_outer_join",
        "persist_selectable",
        "local_table",
        "_is_with_polymorphic",
        "_with_polymorphic_entities",
        "_adapter",
        "_target",
        "__clause_element__",
        "_memoized_values",
        "_all_column_expressions",
        "_nest_adapters",
    )

    def __init__(
        self,
        entity,
        inspected,
        selectable,
        name,
        with_polymorphic_mappers,
        polymorphic_on,
        _base_alias,
        _use_mapper_path,
        adapt_on_names,
        represents_outer_join,
        nest_adapters,
    ):

        mapped_class_or_ac = inspected.entity
        mapper = inspected.mapper

        self._weak_entity = weakref.ref(entity)
        self.mapper = mapper
        self.selectable = (
            self.persist_selectable
        ) = self.local_table = selectable
        self.name = name
        self.polymorphic_on = polymorphic_on
        self._base_alias = weakref.ref(_base_alias or self)
        self._use_mapper_path = _use_mapper_path
        self.represents_outer_join = represents_outer_join
        self._nest_adapters = nest_adapters

        if with_polymorphic_mappers:
            self._is_with_polymorphic = True
            self.with_polymorphic_mappers = with_polymorphic_mappers
            self._with_polymorphic_entities = []
            for poly in self.with_polymorphic_mappers:
                if poly is not mapper:
                    ent = AliasedClass(
                        poly.class_,
                        selectable,
                        base_alias=self,
                        adapt_on_names=adapt_on_names,
                        use_mapper_path=_use_mapper_path,
                    )

                    setattr(self.entity, poly.class_.__name__, ent)
                    self._with_polymorphic_entities.append(ent._aliased_insp)

        else:
            self._is_with_polymorphic = False
            self.with_polymorphic_mappers = [mapper]

        self._adapter = sql_util.ColumnAdapter(
            selectable,
            equivalents=mapper._equivalent_columns,
            adapt_on_names=adapt_on_names,
            anonymize_labels=True,
            # make sure the adapter doesn't try to grab other tables that
            # are not even the thing we are mapping, such as embedded
            # selectables in subqueries or CTEs.  See issue #6060
            adapt_from_selectables=[
                m.selectable for m in self.with_polymorphic_mappers
            ],
        )

        if nest_adapters:
            self._adapter = inspected._adapter.wrap(self._adapter)

        self._adapt_on_names = adapt_on_names
        self._target = mapped_class_or_ac
        # self._target = mapper.class_  # mapped_class_or_ac

    @classmethod
    def _alias_factory(
        cls,
        element: Union[
            Type[_T], "Mapper[_T]", "FromClause", "AliasedClass[_T]"
        ],
        alias=None,
        name=None,
        flat=False,
        adapt_on_names=False,
    ) -> Union["AliasedClass[_T]", "Alias"]:

        if isinstance(element, FromClause):
            if adapt_on_names:
                raise sa_exc.ArgumentError(
                    "adapt_on_names only applies to ORM elements"
                )
            if name:
                return element.alias(name=name, flat=flat)
            else:
                return coercions.expect(
                    roles.AnonymizedFromClauseRole, element, flat=flat
                )
        else:
            return AliasedClass(
                element,
                alias=alias,
                flat=flat,
                name=name,
                adapt_on_names=adapt_on_names,
            )

    @classmethod
    def _with_polymorphic_factory(
        cls,
        base,
        classes,
        selectable=False,
        flat=False,
        polymorphic_on=None,
        aliased=False,
        innerjoin=False,
        _use_mapper_path=False,
    ):

        primary_mapper = _class_to_mapper(base)

        if selectable not in (None, False) and flat:
            raise sa_exc.ArgumentError(
                "the 'flat' and 'selectable' arguments cannot be passed "
                "simultaneously to with_polymorphic()"
            )

        mappers, selectable = primary_mapper._with_polymorphic_args(
            classes, selectable, innerjoin=innerjoin
        )
        if aliased or flat:
            selectable = selectable._anonymous_fromclause(flat=flat)
        return AliasedClass(
            base,
            selectable,
            with_polymorphic_mappers=mappers,
            with_polymorphic_discriminator=polymorphic_on,
            use_mapper_path=_use_mapper_path,
            represents_outer_join=not innerjoin,
        )

    @property
    def entity(self):
        # to eliminate reference cycles, the AliasedClass is held weakly.
        # this produces some situations where the AliasedClass gets lost,
        # particularly when one is created internally and only the AliasedInsp
        # is passed around.
        # to work around this case, we just generate a new one when we need
        # it, as it is a simple class with very little initial state on it.
        ent = self._weak_entity()
        if ent is None:
            ent = AliasedClass._reconstitute_from_aliased_insp(self)
            self._weak_entity = weakref.ref(ent)
        return ent

    is_aliased_class = True
    "always returns True"

    def _memoized_method___clause_element__(self):
        return self.selectable._annotate(
            {
                "parentmapper": self.mapper,
                "parententity": self,
                "entity_namespace": self,
            }
        )._set_propagate_attrs(
            {"compile_state_plugin": "orm", "plugin_subject": self}
        )

    @property
    def entity_namespace(self):
        return self.entity

    _cache_key_traversal = [
        ("name", visitors.ExtendedInternalTraversal.dp_string),
        ("_adapt_on_names", visitors.ExtendedInternalTraversal.dp_boolean),
        ("selectable", visitors.ExtendedInternalTraversal.dp_clauseelement),
    ]

    @property
    def class_(self):
        """Return the mapped class ultimately represented by this
        :class:`.AliasedInsp`."""
        return self.mapper.class_

    @property
    def _path_registry(self) -> PathRegistry:
        if self._use_mapper_path:
            return self.mapper._path_registry
        else:
            return PathRegistry.per_mapper(self)

    def __getstate__(self):
        return {
            "entity": self.entity,
            "mapper": self.mapper,
            "alias": self.selectable,
            "name": self.name,
            "adapt_on_names": self._adapt_on_names,
            "with_polymorphic_mappers": self.with_polymorphic_mappers,
            "with_polymorphic_discriminator": self.polymorphic_on,
            "base_alias": self._base_alias(),
            "use_mapper_path": self._use_mapper_path,
            "represents_outer_join": self.represents_outer_join,
            "nest_adapters": self._nest_adapters,
        }

    def __setstate__(self, state):
        self.__init__(
            state["entity"],
            state["mapper"],
            state["alias"],
            state["name"],
            state["with_polymorphic_mappers"],
            state["with_polymorphic_discriminator"],
            state["base_alias"],
            state["use_mapper_path"],
            state["adapt_on_names"],
            state["represents_outer_join"],
            state["nest_adapters"],
        )

    def _merge_with(self, other):
        # assert self._is_with_polymorphic
        # assert other._is_with_polymorphic

        primary_mapper = other.mapper

        assert self.mapper is primary_mapper

        our_classes = util.to_set(
            mp.class_ for mp in self.with_polymorphic_mappers
        )
        new_classes = set([mp.class_ for mp in other.with_polymorphic_mappers])
        if our_classes == new_classes:
            return other
        else:
            classes = our_classes.union(new_classes)

        mappers, selectable = primary_mapper._with_polymorphic_args(
            classes, None, innerjoin=not other.represents_outer_join
        )
        selectable = selectable._anonymous_fromclause(flat=True)

        return AliasedClass(
            primary_mapper,
            selectable,
            with_polymorphic_mappers=mappers,
            with_polymorphic_discriminator=other.polymorphic_on,
            use_mapper_path=other._use_mapper_path,
            represents_outer_join=other.represents_outer_join,
        )

    def _adapt_element(self, elem, key=None):
        d = {
            "parententity": self,
            "parentmapper": self.mapper,
        }
        if key:
            d["proxy_key"] = key
        return (
            self._adapter.traverse(elem)
            ._annotate(d)
            ._set_propagate_attrs(
                {"compile_state_plugin": "orm", "plugin_subject": self}
            )
        )

    def _entity_for_mapper(self, mapper):
        self_poly = self.with_polymorphic_mappers
        if mapper in self_poly:
            if mapper is self.mapper:
                return self
            else:
                return getattr(
                    self.entity, mapper.class_.__name__
                )._aliased_insp
        elif mapper.isa(self.mapper):
            return self
        else:
            assert False, "mapper %s doesn't correspond to %s" % (mapper, self)

    def _memoized_attr__get_clause(self):
        onclause, replacemap = self.mapper._get_clause
        return (
            self._adapter.traverse(onclause),
            {
                self._adapter.traverse(col): param
                for col, param in replacemap.items()
            },
        )

    def _memoized_attr__memoized_values(self):
        return {}

    def _memoized_attr__all_column_expressions(self):
        if self._is_with_polymorphic:
            cols_plus_keys = self.mapper._columns_plus_keys(
                [ent.mapper for ent in self._with_polymorphic_entities]
            )
        else:
            cols_plus_keys = self.mapper._columns_plus_keys()

        cols_plus_keys = [
            (key, self._adapt_element(col)) for key, col in cols_plus_keys
        ]

        return ColumnCollection(cols_plus_keys)

    def _memo(self, key, callable_, *args, **kw):
        if key in self._memoized_values:
            return self._memoized_values[key]
        else:
            self._memoized_values[key] = value = callable_(*args, **kw)
            return value

    def __repr__(self):
        if self.with_polymorphic_mappers:
            with_poly = "(%s)" % ", ".join(
                mp.class_.__name__ for mp in self.with_polymorphic_mappers
            )
        else:
            with_poly = ""
        return "<AliasedInsp at 0x%x; %s%s>" % (
            id(self),
            self.class_.__name__,
            with_poly,
        )

    def __str__(self):
        if self._is_with_polymorphic:
            return "with_polymorphic(%s, [%s])" % (
                self._target.__name__,
                ", ".join(
                    mp.class_.__name__
                    for mp in self.with_polymorphic_mappers
                    if mp is not self.mapper
                ),
            )
        else:
            return "aliased(%s)" % (self._target.__name__,)


class _WrapUserEntity:
    """A wrapper used within the loader_criteria lambda caller so that
    we can bypass declared_attr descriptors on unmapped mixins, which
    normally emit a warning for such use.

    might also be useful for other per-lambda instrumentations should
    the need arise.

    """

    def __init__(self, subject):
        self.subject = subject

    @util.preload_module("sqlalchemy.orm.decl_api")
    def __getattribute__(self, name):
        decl_api = util.preloaded.orm.decl_api

        subject = object.__getattribute__(self, "subject")
        if name in subject.__dict__ and isinstance(
            subject.__dict__[name], decl_api.declared_attr
        ):
            return subject.__dict__[name].fget(subject)
        else:
            return getattr(subject, name)


class LoaderCriteriaOption(CriteriaOption):
    """Add additional WHERE criteria to the load for all occurrences of
    a particular entity.

    :class:`_orm.LoaderCriteriaOption` is invoked using the
    :func:`_orm.with_loader_criteria` function; see that function for
    details.

    .. versionadded:: 1.4

    """

    __slots__ = (
        "root_entity",
        "entity",
        "deferred_where_criteria",
        "where_criteria",
        "include_aliases",
        "propagate_to_loaders",
    )

    _traverse_internals = [
        ("root_entity", visitors.ExtendedInternalTraversal.dp_plain_obj),
        ("entity", visitors.ExtendedInternalTraversal.dp_has_cache_key),
        ("where_criteria", visitors.InternalTraversal.dp_clauseelement),
        ("include_aliases", visitors.InternalTraversal.dp_boolean),
        ("propagate_to_loaders", visitors.InternalTraversal.dp_boolean),
    ]

    def __init__(
        self,
        entity_or_base,
        where_criteria,
        loader_only=False,
        include_aliases=False,
        propagate_to_loaders=True,
        track_closure_variables=True,
    ):
        entity = inspection.inspect(entity_or_base, False)
        if entity is None:
            self.root_entity = entity_or_base
            self.entity = None
        else:
            self.root_entity = None
            self.entity = entity

        if callable(where_criteria):
            self.deferred_where_criteria = True
            self.where_criteria = lambdas.DeferredLambdaElement(
                where_criteria,
                roles.WhereHavingRole,
                lambda_args=(
                    _WrapUserEntity(
                        self.root_entity
                        if self.root_entity is not None
                        else self.entity.entity,
                    ),
                ),
                opts=lambdas.LambdaOptions(
                    track_closure_variables=track_closure_variables
                ),
            )
        else:
            self.deferred_where_criteria = False
            self.where_criteria = coercions.expect(
                roles.WhereHavingRole, where_criteria
            )

        self.include_aliases = include_aliases
        self.propagate_to_loaders = propagate_to_loaders

    def _all_mappers(self):
        if self.entity:
            for ent in self.entity.mapper.self_and_descendants:
                yield ent
        else:
            stack = list(self.root_entity.__subclasses__())
            while stack:
                subclass = stack.pop(0)
                ent = inspection.inspect(subclass, raiseerr=False)
                if ent:
                    for mp in ent.mapper.self_and_descendants:
                        yield mp
                else:
                    stack.extend(subclass.__subclasses__())

    def _should_include(self, compile_state):
        if (
            compile_state.select_statement._annotations.get(
                "for_loader_criteria", None
            )
            is self
        ):
            return False
        return True

    def _resolve_where_criteria(self, ext_info):
        if self.deferred_where_criteria:
            crit = self.where_criteria._resolve_with_args(ext_info.entity)
        else:
            crit = self.where_criteria
        return sql_util._deep_annotate(
            crit, {"for_loader_criteria": self}, detect_subquery_cols=True
        )

    def process_compile_state_replaced_entities(
        self, compile_state, mapper_entities
    ):
        return self.process_compile_state(compile_state)

    def process_compile_state(self, compile_state):
        """Apply a modification to a given :class:`.CompileState`."""

        # if options to limit the criteria to immediate query only,
        # use compile_state.attributes instead

        if not compile_state.compile_options._for_refresh_state:
            self.get_global_criteria(compile_state.global_attributes)

    def get_global_criteria(self, attributes):
        for mp in self._all_mappers():
            load_criteria = attributes.setdefault(
                ("additional_entity_criteria", mp), []
            )

            load_criteria.append(self)


inspection._inspects(AliasedClass)(lambda target: target._aliased_insp)
inspection._inspects(AliasedInsp)(lambda target: target)


@inspection._self_inspects
class Bundle(
    ORMColumnsClauseRole,
    SupportsCloneAnnotations,
    sql_base.MemoizedHasCacheKey,
    InspectionAttr,
):
    """A grouping of SQL expressions that are returned by a :class:`.Query`
    under one namespace.

    The :class:`.Bundle` essentially allows nesting of the tuple-based
    results returned by a column-oriented :class:`_query.Query` object.
    It also
    is extensible via simple subclassing, where the primary capability
    to override is that of how the set of expressions should be returned,
    allowing post-processing as well as custom return types, without
    involving ORM identity-mapped classes.

    .. versionadded:: 0.9.0

    .. seealso::

        :ref:`bundles`


    """

    single_entity = False
    """If True, queries for a single Bundle will be returned as a single
    entity, rather than an element within a keyed tuple."""

    is_clause_element = False

    is_mapper = False

    is_aliased_class = False

    is_bundle = True

    _propagate_attrs = util.immutabledict()

    def __init__(self, name, *exprs, **kw):
        r"""Construct a new :class:`.Bundle`.

        e.g.::

            bn = Bundle("mybundle", MyClass.x, MyClass.y)

            for row in session.query(bn).filter(
                    bn.c.x == 5).filter(bn.c.y == 4):
                print(row.mybundle.x, row.mybundle.y)

        :param name: name of the bundle.
        :param \*exprs: columns or SQL expressions comprising the bundle.
        :param single_entity=False: if True, rows for this :class:`.Bundle`
         can be returned as a "single entity" outside of any enclosing tuple
         in the same manner as a mapped entity.

        """
        self.name = self._label = name
        self.exprs = exprs = [
            coercions.expect(
                roles.ColumnsClauseRole, expr, apply_propagate_attrs=self
            )
            for expr in exprs
        ]

        self.c = self.columns = ColumnCollection(
            (getattr(col, "key", col._label), col)
            for col in [e._annotations.get("bundle", e) for e in exprs]
        )
        self.single_entity = kw.pop("single_entity", self.single_entity)

    def _gen_cache_key(self, anon_map, bindparams):
        return (self.__class__, self.name, self.single_entity) + tuple(
            [expr._gen_cache_key(anon_map, bindparams) for expr in self.exprs]
        )

    @property
    def mapper(self):
        return self.exprs[0]._annotations.get("parentmapper", None)

    @property
    def entity(self):
        return self.exprs[0]._annotations.get("parententity", None)

    @property
    def entity_namespace(self):
        return self.c

    columns = None
    """A namespace of SQL expressions referred to by this :class:`.Bundle`.

        e.g.::

            bn = Bundle("mybundle", MyClass.x, MyClass.y)

            q = sess.query(bn).filter(bn.c.x == 5)

        Nesting of bundles is also supported::

            b1 = Bundle("b1",
                    Bundle('b2', MyClass.a, MyClass.b),
                    Bundle('b3', MyClass.x, MyClass.y)
                )

            q = sess.query(b1).filter(
                b1.c.b2.c.a == 5).filter(b1.c.b3.c.y == 9)

    .. seealso::

        :attr:`.Bundle.c`

    """

    c = None
    """An alias for :attr:`.Bundle.columns`."""

    def _clone(self):
        cloned = self.__class__.__new__(self.__class__)
        cloned.__dict__.update(self.__dict__)
        return cloned

    def __clause_element__(self):
        # ensure existing entity_namespace remains
        annotations = {"bundle": self, "entity_namespace": self}
        annotations.update(self._annotations)

        plugin_subject = self.exprs[0]._propagate_attrs.get(
            "plugin_subject", self.entity
        )
        return (
            expression.ClauseList(
                _literal_as_text_role=roles.ColumnsClauseRole,
                group=False,
                *[e._annotations.get("bundle", e) for e in self.exprs],
            )
            ._annotate(annotations)
            ._set_propagate_attrs(
                # the Bundle *must* use the orm plugin no matter what.  the
                # subject can be None but it's much better if it's not.
                {
                    "compile_state_plugin": "orm",
                    "plugin_subject": plugin_subject,
                }
            )
        )

    @property
    def clauses(self):
        return self.__clause_element__().clauses

    def label(self, name):
        """Provide a copy of this :class:`.Bundle` passing a new label."""

        cloned = self._clone()
        cloned.name = name
        return cloned

    def create_row_processor(self, query, procs, labels):
        """Produce the "row processing" function for this :class:`.Bundle`.

        May be overridden by subclasses.

        .. seealso::

            :ref:`bundles` - includes an example of subclassing.

        """
        keyed_tuple = result_tuple(labels, [() for l in labels])

        def proc(row):
            return keyed_tuple([proc(row) for proc in procs])

        return proc


def _orm_annotate(element, exclude=None):
    """Deep copy the given ClauseElement, annotating each element with the
    "_orm_adapt" flag.

    Elements within the exclude collection will be cloned but not annotated.

    """
    return sql_util._deep_annotate(element, {"_orm_adapt": True}, exclude)


def _orm_deannotate(element):
    """Remove annotations that link a column to a particular mapping.

    Note this doesn't affect "remote" and "foreign" annotations
    passed by the :func:`_orm.foreign` and :func:`_orm.remote`
    annotators.

    """

    return sql_util._deep_deannotate(
        element, values=("_orm_adapt", "parententity")
    )


def _orm_full_deannotate(element):
    return sql_util._deep_deannotate(element)


class _ORMJoin(expression.Join):
    """Extend Join to support ORM constructs as input."""

    __visit_name__ = expression.Join.__visit_name__

    inherit_cache = True

    def __init__(
        self,
        left,
        right,
        onclause=None,
        isouter=False,
        full=False,
        _left_memo=None,
        _right_memo=None,
        _extra_criteria=(),
    ):
        left_info = inspection.inspect(left)

        right_info = inspection.inspect(right)
        adapt_to = right_info.selectable

        # used by joined eager loader
        self._left_memo = _left_memo
        self._right_memo = _right_memo

        # legacy, for string attr name ON clause.  if that's removed
        # then the "_joined_from_info" concept can go
        left_orm_info = getattr(left, "_joined_from_info", left_info)
        self._joined_from_info = right_info
        if isinstance(onclause, str):
            onclause = getattr(left_orm_info.entity, onclause)
        # ####

        if isinstance(onclause, attributes.QueryableAttribute):
            on_selectable = onclause.comparator._source_selectable()
            prop = onclause.property
            _extra_criteria += onclause._extra_criteria
        elif isinstance(onclause, MapperProperty):
            # used internally by joined eager loader...possibly not ideal
            prop = onclause
            on_selectable = prop.parent.selectable
        else:
            prop = None

        if prop:
            left_selectable = left_info.selectable

            if sql_util.clause_is_present(on_selectable, left_selectable):
                adapt_from = on_selectable
            else:
                adapt_from = left_selectable

            (
                pj,
                sj,
                source,
                dest,
                secondary,
                target_adapter,
            ) = prop._create_joins(
                source_selectable=adapt_from,
                dest_selectable=adapt_to,
                source_polymorphic=True,
                of_type_entity=right_info,
                alias_secondary=True,
                extra_criteria=_extra_criteria,
            )

            if sj is not None:
                if isouter:
                    # note this is an inner join from secondary->right
                    right = sql.join(secondary, right, sj)
                    onclause = pj
                else:
                    left = sql.join(left, secondary, pj, isouter)
                    onclause = sj
            else:
                onclause = pj

            self._target_adapter = target_adapter

        augment_onclause = onclause is None and _extra_criteria
        expression.Join.__init__(self, left, right, onclause, isouter, full)

        if augment_onclause:
            self.onclause &= sql.and_(*_extra_criteria)

        if (
            not prop
            and getattr(right_info, "mapper", None)
            and right_info.mapper.single
        ):
            # if single inheritance target and we are using a manual
            # or implicit ON clause, augment it the same way we'd augment the
            # WHERE.
            single_crit = right_info.mapper._single_table_criterion
            if single_crit is not None:
                if right_info.is_aliased_class:
                    single_crit = right_info._adapter.traverse(single_crit)
                self.onclause = self.onclause & single_crit

    def _splice_into_center(self, other):
        """Splice a join into the center.

        Given join(a, b) and join(b, c), return join(a, b).join(c)

        """
        leftmost = other
        while isinstance(leftmost, sql.Join):
            leftmost = leftmost.left

        assert self.right is leftmost

        left = _ORMJoin(
            self.left,
            other.left,
            self.onclause,
            isouter=self.isouter,
            _left_memo=self._left_memo,
            _right_memo=other._left_memo,
        )

        return _ORMJoin(
            left,
            other.right,
            other.onclause,
            isouter=other.isouter,
            _right_memo=other._right_memo,
        )

    def join(
        self,
        right,
        onclause=None,
        isouter=False,
        full=False,
        join_to_left=None,
    ):
        return _ORMJoin(self, right, onclause, full=full, isouter=isouter)

    def outerjoin(self, right, onclause=None, full=False, join_to_left=None):
        return _ORMJoin(self, right, onclause, isouter=True, full=full)


def with_parent(instance, prop, from_entity=None):
    """Create filtering criterion that relates this query's primary entity
    to the given related instance, using established
    :func:`_orm.relationship()`
    configuration.

    E.g.::

        stmt = select(Address).where(with_parent(some_user, User.addresses))


    The SQL rendered is the same as that rendered when a lazy loader
    would fire off from the given parent on that attribute, meaning
    that the appropriate state is taken from the parent object in
    Python without the need to render joins to the parent table
    in the rendered statement.

    The given property may also make use of :meth:`_orm.PropComparator.of_type`
    to indicate the left side of the criteria::


        a1 = aliased(Address)
        a2 = aliased(Address)
        stmt = select(a1, a2).where(
            with_parent(u1, User.addresses.of_type(a2))
        )

    The above use is equivalent to using the
    :func:`_orm.with_parent.from_entity` argument::

        a1 = aliased(Address)
        a2 = aliased(Address)
        stmt = select(a1, a2).where(
            with_parent(u1, User.addresses, from_entity=a2)
        )

    :param instance:
      An instance which has some :func:`_orm.relationship`.

    :param property:
      Class-bound attribute, which indicates
      what relationship from the instance should be used to reconcile the
      parent/child relationship.

    :param from_entity:
      Entity in which to consider as the left side.  This defaults to the
      "zero" entity of the :class:`_query.Query` itself.

      .. versionadded:: 1.2

    """
    if isinstance(prop, str):
        raise sa_exc.ArgumentError(
            "with_parent() accepts class-bound mapped attributes, not strings"
        )
    elif isinstance(prop, attributes.QueryableAttribute):
        if prop._of_type:
            from_entity = prop._of_type
        prop = prop.property

    return prop._with_parent(instance, from_entity=from_entity)


def has_identity(object_):
    """Return True if the given object has a database
    identity.

    This typically corresponds to the object being
    in either the persistent or detached state.

    .. seealso::

        :func:`.was_deleted`

    """
    state = attributes.instance_state(object_)
    return state.has_identity


def was_deleted(object_):
    """Return True if the given object was deleted
    within a session flush.

    This is regardless of whether or not the object is
    persistent or detached.

    .. seealso::

        :attr:`.InstanceState.was_deleted`

    """

    state = attributes.instance_state(object_)
    return state.was_deleted


def _entity_corresponds_to(given, entity):
    """determine if 'given' corresponds to 'entity', in terms
    of an entity passed to Query that would match the same entity
    being referred to elsewhere in the query.

    """
    if entity.is_aliased_class:
        if given.is_aliased_class:
            if entity._base_alias() is given._base_alias():
                return True
        return False
    elif given.is_aliased_class:
        if given._use_mapper_path:
            return entity in given.with_polymorphic_mappers
        else:
            return entity is given

    return entity.common_parent(given)


def _entity_corresponds_to_use_path_impl(given, entity):
    """determine if 'given' corresponds to 'entity', in terms
    of a path of loader options where a mapped attribute is taken to
    be a member of a parent entity.

    e.g.::

        someoption(A).someoption(A.b)  # -> fn(A, A) -> True
        someoption(A).someoption(C.d)  # -> fn(A, C) -> False

        a1 = aliased(A)
        someoption(a1).someoption(A.b) # -> fn(a1, A) -> False
        someoption(a1).someoption(a1.b) # -> fn(a1, a1) -> True

        wp = with_polymorphic(A, [A1, A2])
        someoption(wp).someoption(A1.foo)  # -> fn(wp, A1) -> False
        someoption(wp).someoption(wp.A1.foo)  # -> fn(wp, wp.A1) -> True


    """
    if given.is_aliased_class:
        return (
            entity.is_aliased_class
            and not entity._use_mapper_path
            and (given is entity or entity in given._with_polymorphic_entities)
        )
    elif not entity.is_aliased_class:
        return given.isa(entity.mapper)
    else:
        return (
            entity._use_mapper_path
            and given in entity.with_polymorphic_mappers
        )


def _entity_isa(given, mapper):
    """determine if 'given' "is a" mapper, in terms of the given
    would load rows of type 'mapper'.

    """
    if given.is_aliased_class:
        return mapper in given.with_polymorphic_mappers or given.mapper.isa(
            mapper
        )
    elif given.with_polymorphic_mappers:
        return mapper in given.with_polymorphic_mappers
    else:
        return given.isa(mapper)


def randomize_unitofwork():
    """Use random-ordering sets within the unit of work in order
    to detect unit of work sorting issues.

    This is a utility function that can be used to help reproduce
    inconsistent unit of work sorting issues.   For example,
    if two kinds of objects A and B are being inserted, and
    B has a foreign key reference to A - the A must be inserted first.
    However, if there is no relationship between A and B, the unit of work
    won't know to perform this sorting, and an operation may or may not
    fail, depending on how the ordering works out.   Since Python sets
    and dictionaries have non-deterministic ordering, such an issue may
    occur on some runs and not on others, and in practice it tends to
    have a great dependence on the state of the interpreter.  This leads
    to so-called "heisenbugs" where changing entirely irrelevant aspects
    of the test program still cause the failure behavior to change.

    By calling ``randomize_unitofwork()`` when a script first runs, the
    ordering of a key series of sets within the unit of work implementation
    are randomized, so that the script can be minimized down to the
    fundamental mapping and operation that's failing, while still reproducing
    the issue on at least some runs.

    This utility is also available when running the test suite via the
    ``--reversetop`` flag.

    """
    from sqlalchemy.orm import unitofwork, session, mapper, dependency
    from sqlalchemy.util import topological
    from sqlalchemy.testing.util import RandomSet

    topological.set = (
        unitofwork.set
    ) = session.set = mapper.set = dependency.set = RandomSet


def _getitem(iterable_query, item):
    """calculate __getitem__ in terms of an iterable query object
    that also has a slice() method.

    """

    def _no_negative_indexes():
        raise IndexError(
            "negative indexes are not accepted by SQL "
            "index / slice operators"
        )

    if isinstance(item, slice):
        start, stop, step = util.decode_slice(item)

        if (
            isinstance(stop, int)
            and isinstance(start, int)
            and stop - start <= 0
        ):
            return []

        elif (isinstance(start, int) and start < 0) or (
            isinstance(stop, int) and stop < 0
        ):
            _no_negative_indexes()
            return list(iterable_query)[item]

        res = iterable_query.slice(start, stop)
        if step is not None:
            return list(res)[None : None : item.step]
        else:
            return list(res)
    else:
        if item == -1:
            _no_negative_indexes()
            return list(iterable_query)[-1]
        else:
            return list(iterable_query[item : item + 1])[0]


def _is_mapped_annotation(raw_annotation: Union[type, str], cls: type):
    annotated = de_stringify_annotation(cls, raw_annotation)
    return is_origin_of(annotated, "Mapped", module="sqlalchemy.orm")


def _extract_mapped_subtype(
    raw_annotation: Union[type, str],
    cls: type,
    key: str,
    attr_cls: type,
    required: bool,
    is_dataclass_field: bool,
) -> Optional[Union[type, str]]:

    if raw_annotation is None:

        if required:
            raise sa_exc.ArgumentError(
                f"Python typing annotation is required for attribute "
                f'"{cls.__name__}.{key}" when primary argument(s) for '
                f'"{attr_cls.__name__}" construct are None or not present'
            )
        return None

    annotated = de_stringify_annotation(cls, raw_annotation)

    if is_dataclass_field:
        return annotated
    else:
        if (
            not hasattr(annotated, "__origin__")
            or not issubclass(annotated.__origin__, attr_cls)
            and not issubclass(attr_cls, annotated.__origin__)
        ):
            our_annotated_str = (
                annotated.__name__
                if not isinstance(annotated, str)
                else repr(annotated)
            )
            raise sa_exc.ArgumentError(
                f'Type annotation for "{cls.__name__}.{key}" should use the '
                f'syntax "Mapped[{our_annotated_str}]" or '
                f'"{attr_cls.__name__}[{our_annotated_str}]".'
            )

        if len(annotated.__args__) != 1:
            raise sa_exc.ArgumentError(
                "Expected sub-type for Mapped[] annotation"
            )

        return annotated.__args__[0]
