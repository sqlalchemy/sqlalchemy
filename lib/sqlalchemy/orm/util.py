# orm/util.py
# Copyright (C) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


from .. import sql, util, event, exc as sa_exc, inspection
from ..sql import expression, util as sql_util, operators
from .interfaces import PropComparator, MapperProperty
from itertools import chain
from . import attributes, exc
import re

mapperlib = util.importlater("sqlalchemy.orm", "mapperlib")

all_cascades = frozenset(("delete", "delete-orphan", "all", "merge",
                          "expunge", "save-update", "refresh-expire",
                          "none"))

_INSTRUMENTOR = ('mapper', 'instrumentor')

_none_set = frozenset([None])

class CascadeOptions(frozenset):
    """Keeps track of the options sent to relationship().cascade"""

    _add_w_all_cascades = all_cascades.difference([
                            'all', 'none', 'delete-orphan'])
    _allowed_cascades = all_cascades

    def __new__(cls, arg):
        values = set([
                    c for c 
                    in re.split('\s*,\s*', arg or "")
                    if c
                ])

        if values.difference(cls._allowed_cascades):
            raise sa_exc.ArgumentError(
                    "Invalid cascade option(s): %s" % 
                    ", ".join([repr(x) for x in 
                        sorted(
                            values.difference(cls._allowed_cascades)
                    )])
            )

        if "all" in values:
            values.update(cls._add_w_all_cascades)
        if "none" in values:
            values.clear()
        values.discard('all')

        self = frozenset.__new__(CascadeOptions, values)
        self.save_update = 'save-update' in values
        self.delete = 'delete' in values
        self.refresh_expire = 'refresh-expire' in values
        self.merge = 'merge' in values
        self.expunge = 'expunge' in values
        self.delete_orphan = "delete-orphan" in values

        if self.delete_orphan and not self.delete:
            util.warn("The 'delete-orphan' cascade "
                        "option requires 'delete'.")
        return self

    def __repr__(self):
        return "CascadeOptions(%r)" % (
            ",".join([x for x in sorted(self)])
        )

def _validator_events(desc, key, validator, include_removes):
    """Runs a validation method on an attribute value to be set or appended."""

    if include_removes:
        def append(state, value, initiator):
            return validator(state.obj(), key, value, False)

        def set_(state, value, oldvalue, initiator):
            return validator(state.obj(), key, value, False)

        def remove(state, value, initiator):
            validator(state.obj(), key, value, True)
    else:
        def append(state, value, initiator):
            return validator(state.obj(), key, value)

        def set_(state, value, oldvalue, initiator):
            return validator(state.obj(), key, value)

    event.listen(desc, 'append', append, raw=True, retval=True)
    event.listen(desc, 'set', set_, raw=True, retval=True)
    if include_removes:
        event.listen(desc, "remove", remove, raw=True, retval=True)

def polymorphic_union(table_map, typecolname, aliasname='p_union', cast_nulls=True):
    """Create a ``UNION`` statement used by a polymorphic mapper.

    See  :ref:`concrete_inheritance` for an example of how
    this is used.

    :param table_map: mapping of polymorphic identities to 
     :class:`.Table` objects.
    :param typecolname: string name of a "discriminator" column, which will be 
     derived from the query, producing the polymorphic identity for each row.  If
     ``None``, no polymorphic discriminator is generated.
    :param aliasname: name of the :func:`~sqlalchemy.sql.expression.alias()` 
     construct generated.
    :param cast_nulls: if True, non-existent columns, which are represented as labeled
     NULLs, will be passed into CAST.   This is a legacy behavior that is problematic
     on some backends such as Oracle - in which case it can be set to False.

    """

    colnames = util.OrderedSet()
    colnamemaps = {}
    types = {}
    for key in table_map.keys():
        table = table_map[key]

        # mysql doesnt like selecting from a select; 
        # make it an alias of the select
        if isinstance(table, sql.Select):
            table = table.alias()
            table_map[key] = table

        m = {}
        for c in table.c:
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
    for type, table in table_map.iteritems():
        if typecolname is not None:
            result.append(
                    sql.select([col(name, table) for name in colnames] +
                    [sql.literal_column(sql_util._quote_ddl_expr(type)).
                            label(typecolname)],
                             from_obj=[table]))
        else:
            result.append(sql.select([col(name, table) for name in colnames],
                                     from_obj=[table]))
    return sql.union_all(*result).alias(aliasname)

def identity_key(*args, **kwargs):
    """Get an identity key.

    Valid call signatures:

    * ``identity_key(class, ident)``

      class
          mapped class (must be a positional argument)

      ident
          primary key, if the key is composite this is a tuple


    * ``identity_key(instance=instance)``

      instance
          object instance (must be given as a keyword arg)

    * ``identity_key(class, row=row)``

      class
          mapped class (must be a positional argument)

      row
          result proxy row (must be given as a keyword arg)

    """
    if args:
        if len(args) == 1:
            class_ = args[0]
            try:
                row = kwargs.pop("row")
            except KeyError:
                ident = kwargs.pop("ident")
        elif len(args) == 2:
            class_, ident = args
        elif len(args) == 3:
            class_, ident = args
        else:
            raise sa_exc.ArgumentError("expected up to three "
                "positional arguments, got %s" % len(args))
        if kwargs:
            raise sa_exc.ArgumentError("unknown keyword arguments: %s"
                % ", ".join(kwargs.keys()))
        mapper = class_mapper(class_)
        if "ident" in locals():
            return mapper.identity_key_from_primary_key(util.to_list(ident))
        return mapper.identity_key_from_row(row)
    instance = kwargs.pop("instance")
    if kwargs:
        raise sa_exc.ArgumentError("unknown keyword arguments: %s"
            % ", ".join(kwargs.keys()))
    mapper = object_mapper(instance)
    return mapper.identity_key_from_instance(instance)

class ORMAdapter(sql_util.ColumnAdapter):
    """Extends ColumnAdapter to accept ORM entities.

    The selectable is extracted from the given entity,
    and the AliasedClass if any is referenced.

    """
    def __init__(self, entity, equivalents=None, 
                            chain_to=None, adapt_required=False):
        self.mapper, selectable, is_aliased_class = _entity_info(entity)
        if is_aliased_class:
            self.aliased_class = entity
        else:
            self.aliased_class = None
        sql_util.ColumnAdapter.__init__(self, selectable, 
                                        equivalents, chain_to,
                                        adapt_required=adapt_required)

    def replace(self, elem):
        entity = elem._annotations.get('parentmapper', None)
        if not entity or entity.isa(self.mapper):
            return sql_util.ColumnAdapter.replace(self, elem)
        else:
            return None

class PathRegistry(object):
    """Represent query load paths and registry functions.

    Basically represents structures like:

    (<User mapper>, "orders", <Order mapper>, "items", <Item mapper>)

    These structures are generated by things like
    query options (joinedload(), subqueryload(), etc.) and are
    used to compose keys stored in the query._attributes dictionary
    for various options.

    They are then re-composed at query compile/result row time as
    the query is formed and as rows are fetched, where they again
    serve to compose keys to look up options in the context.attributes
    dictionary, which is copied from query._attributes.

    The path structure has a limited amount of caching, where each
    "root" ultimately pulls from a fixed registry associated with
    the first mapper, that also contains elements for each of its 
    property keys.  However paths longer than two elements, which 
    are the exception rather than the rule, are generated on an 
    as-needed basis.

    """

    def __eq__(self, other):
        return other is not None and \
            self.path == other.path

    def set(self, reg, key, value):
        reg._attributes[(key, self.reduced_path)] = value

    def setdefault(self, reg, key, value):
        reg._attributes.setdefault((key, self.reduced_path), value)

    def get(self, reg, key, value=None):
        key = (key, self.reduced_path)
        if key in reg._attributes:
            return reg._attributes[key]
        else:
            return value

    @property
    def length(self):
        return len(self.path)

    def contains_mapper(self, mapper):
        return mapper.base_mapper in self.reduced_path

    def contains(self, reg, key):
        return (key, self.reduced_path) in reg._attributes

    def serialize(self):
        path = self.path
        return zip(
            [m.class_ for m in [path[i] for i in range(0, len(path), 2)]], 
            [path[i] for i in range(1, len(path), 2)] + [None]
        )

    @classmethod
    def deserialize(cls, path):
        if path is None:
            return None

        p = tuple(chain(*[(class_mapper(mcls), key) for mcls, key in path]))
        if p and p[-1] is None:
            p = p[0:-1]
        return cls.coerce(p)

    @classmethod
    def per_mapper(cls, mapper):
        return EntityRegistry(
                cls.root, mapper
            )

    @classmethod
    def coerce(cls, raw):
        return util.reduce(lambda prev, next:prev[next], raw, cls.root)

    @classmethod
    def token(cls, token):
        return KeyRegistry(cls.root, token)

    def __add__(self, other):
        return util.reduce(
                    lambda prev, next:prev[next],
                    other.path, self)

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.path, )

class RootRegistry(PathRegistry):
    """Root registry, defers to mappers so that
    paths are maintained per-root-mapper.

    """
    path = ()
    reduced_path = ()

    def __getitem__(self, mapper):
        return mapper._sa_path_registry
PathRegistry.root = RootRegistry()

class KeyRegistry(PathRegistry):
    def __init__(self, parent, key):
        self.key = key
        self.parent = parent
        self.path = parent.path + (key,)
        self.reduced_path = parent.reduced_path + (key,)

    def __getitem__(self, entity):
        return EntityRegistry(
            self, entity
        )

class EntityRegistry(PathRegistry, dict):
    is_aliased_class = False

    def __init__(self, parent, entity):
        self.key = reduced_key = entity
        self.parent = parent
        if hasattr(entity, 'base_mapper'):
            reduced_key = entity.base_mapper
        else:
            self.is_aliased_class = True

        self.path = parent.path + (entity,)
        self.reduced_path = parent.reduced_path + (reduced_key,)

    def __nonzero__(self):
        return True

    def __missing__(self, key):
        self[key] = item = KeyRegistry(self, key)
        return item


class AliasedClass(object):
    """Represents an "aliased" form of a mapped class for usage with Query.

    The ORM equivalent of a :func:`sqlalchemy.sql.expression.alias`
    construct, this object mimics the mapped class using a
    __getattr__ scheme and maintains a reference to a
    real :class:`~sqlalchemy.sql.expression.Alias` object.

    Usage is via the :class:`~sqlalchemy.orm.aliased()` synonym::

        # find all pairs of users with the same name
        user_alias = aliased(User)
        session.query(User, user_alias).\\
                        join((user_alias, User.id > user_alias.id)).\\
                        filter(User.name==user_alias.name)

    The resulting object is an instance of :class:`.AliasedClass`, however
    it implements a ``__getattribute__()`` scheme which will proxy attribute
    access to that of the ORM class being aliased.  All classmethods
    on the mapped entity should also be available here, including 
    hybrids created with the :ref:`hybrids_toplevel` extension,
    which will receive the :class:`.AliasedClass` as the "class" argument
    when classmethods are called.

    :param cls: ORM mapped entity which will be "wrapped" around an alias.
    :param alias: a selectable, such as an :func:`.alias` or :func:`.select`
     construct, which will be rendered in place of the mapped table of the
     ORM entity.  If left as ``None``, an ordinary :class:`.Alias` of the 
     ORM entity's mapped table will be generated.
    :param name: A name which will be applied both to the :class:`.Alias`
     if one is generated, as well as the name present in the "named tuple"
     returned by the :class:`.Query` object when results are returned.
    :param adapt_on_names: if True, more liberal "matching" will be used when
     mapping the mapped columns of the ORM entity to those of the given selectable - 
     a name-based match will be performed if the given selectable doesn't 
     otherwise have a column that corresponds to one on the entity.  The 
     use case for this is when associating an entity with some derived
     selectable such as one that uses aggregate functions::

        class UnitPrice(Base):
            __tablename__ = 'unit_price'
            ...
            unit_id = Column(Integer)
            price = Column(Numeric)

        aggregated_unit_price = Session.query(
                                    func.sum(UnitPrice.price).label('price')
                                ).group_by(UnitPrice.unit_id).subquery()

        aggregated_unit_price = aliased(UnitPrice, alias=aggregated_unit_price, adapt_on_names=True)

     Above, functions on ``aggregated_unit_price`` which
     refer to ``.price`` will return the
     ``fund.sum(UnitPrice.price).label('price')`` column,
     as it is matched on the name "price".  Ordinarily, the "price" function wouldn't
     have any "column correspondence" to the actual ``UnitPrice.price`` column
     as it is not a proxy of the original.

     .. versionadded:: 0.7.3

    """
    def __init__(self, cls, alias=None, 
                            name=None, 
                            adapt_on_names=False,
                            with_polymorphic_mappers=(),
                            with_polymorphic_discriminator=None):
        self.__mapper = _class_to_mapper(cls)
        self.__target = self.__mapper.class_
        self.__adapt_on_names = adapt_on_names
        if alias is None:
            alias = self.__mapper._with_polymorphic_selectable.alias(
                                    name=name)
        self.__adapter = sql_util.ClauseAdapter(alias,
                            equivalents=self.__mapper._equivalent_columns,
                            adapt_on_names=self.__adapt_on_names)
        self.__alias = alias
        self.__with_polymorphic_mappers = with_polymorphic_mappers
        self.__with_polymorphic_discriminator = \
                        with_polymorphic_discriminator
        for poly in with_polymorphic_mappers:
            setattr(self, poly.class_.__name__, 
                    AliasedClass(poly.class_, alias))

        # used to assign a name to the RowTuple object
        # returned by Query.
        self._sa_label_name = name
        self.__name__ = 'AliasedClass_' + str(self.__target)

    @util.memoized_property
    def _sa_path_registry(self):
        return PathRegistry.per_mapper(self)

    def __getstate__(self):
        return {
            'mapper':self.__mapper, 
            'alias':self.__alias, 
            'name':self._sa_label_name,
            'adapt_on_names':self.__adapt_on_names,
            'with_polymorphic_mappers':
                self.__with_polymorphic_mappers,
            'with_polymorphic_discriminator':
                self.__with_polymorphic_discriminator
        }

    def __setstate__(self, state):
        self.__mapper = state['mapper']
        self.__target = self.__mapper.class_
        self.__adapt_on_names = state['adapt_on_names']
        alias = state['alias']
        self.__adapter = sql_util.ClauseAdapter(alias,
                            equivalents=self.__mapper._equivalent_columns,
                            adapt_on_names=self.__adapt_on_names)
        self.__alias = alias
        self.__with_polymorphic_mappers = \
                        state.get('with_polymorphic_mappers')
        self.__with_polymorphic_discriminator = \
                        state.get('with_polymorphic_discriminator')
        name = state['name']
        self._sa_label_name = name
        self.__name__ = 'AliasedClass_' + str(self.__target)

    def __adapt_element(self, elem):
        return self.__adapter.traverse(elem).\
                    _annotate({
                        'parententity': self, 
                        'parentmapper':self.__mapper}
                    )

    def __adapt_prop(self, existing, key):
        comparator = existing.comparator.adapted(self.__adapt_element)

        queryattr = attributes.QueryableAttribute(self, key,
            impl=existing.impl, parententity=self, comparator=comparator)
        setattr(self, key, queryattr)
        return queryattr

    def __getattr__(self, key):
        for base in self.__target.__mro__:
            try:
                attr = object.__getattribute__(base, key)
            except AttributeError:
                continue
            else:
                break
        else:
            raise AttributeError(key)

        if isinstance(attr, attributes.QueryableAttribute):
            return self.__adapt_prop(attr, key)
        elif hasattr(attr, 'func_code'):
            is_method = getattr(self.__target, key, None)
            if is_method and is_method.im_self is not None:
                return util.types.MethodType(attr.im_func, self, self)
            else:
                return None
        elif hasattr(attr, '__get__'):
            ret = attr.__get__(None, self)
            if isinstance(ret, PropComparator):
                return ret.adapted(self.__adapt_element)
            return ret
        else:
            return attr

    def __repr__(self):
        return '<AliasedClass at 0x%x; %s>' % (
            id(self), self.__target.__name__)

def aliased(element, alias=None, name=None, adapt_on_names=False):
    if isinstance(element, expression.FromClause):
        if adapt_on_names:
            raise sa_exc.ArgumentError(
                "adapt_on_names only applies to ORM elements"
            )
        return element.alias(name)
    else:
        return AliasedClass(element, alias=alias, 
                    name=name, adapt_on_names=adapt_on_names)

def with_polymorphic(base, classes, selectable=False, 
                        polymorphic_on=None, aliased=False,
                        innerjoin=False):
    """Produce an :class:`.AliasedClass` construct which specifies
    columns for descendant mappers of the given base.

    .. versionadded:: 0.8
        :func:`.orm.with_polymorphic` is in addition to the existing
        :class:`.Query` method :meth:`.Query.with_polymorphic`,
        which has the same purpose but is not as flexible in its usage.

    Using this method will ensure that each descendant mapper's
    tables are included in the FROM clause, and will allow filter()
    criterion to be used against those tables.  The resulting
    instances will also have those columns already loaded so that
    no "post fetch" of those columns will be required.

    See the examples at :ref:`with_polymorphic`.

    :param base: Base class to be aliased.

    :param cls_or_mappers: a single class or mapper, or list of
        class/mappers, which inherit from the base class.
        Alternatively, it may also be the string ``'*'``, in which case
        all descending mapped classes will be added to the FROM clause.

    :param aliased: when True, the selectable will be wrapped in an
        alias, that is ``(SELECT * FROM <fromclauses>) AS anon_1``.
        This can be important when using the with_polymorphic()
        to create the target of a JOIN on a backend that does not
        support parenthesized joins, such as SQLite and older
        versions of MySQL.

    :param selectable: a table or select() statement that will
        be used in place of the generated FROM clause. This argument is
        required if any of the desired classes use concrete table
        inheritance, since SQLAlchemy currently cannot generate UNIONs
        among tables automatically. If used, the ``selectable`` argument
        must represent the full set of tables and columns mapped by every
        mapped class. Otherwise, the unaccounted mapped columns will
        result in their table being appended directly to the FROM clause
        which will usually lead to incorrect results.

    :param polymorphic_on: a column to be used as the "discriminator"
        column for the given selectable. If not given, the polymorphic_on
        attribute of the base classes' mapper will be used, if any. This 
        is useful for mappings that don't have polymorphic loading 
        behavior by default.

    :param innerjoin: if True, an INNER JOIN will be used.  This should
       only be specified if querying for one specific subtype only
    """
    primary_mapper = _class_to_mapper(base)
    mappers, selectable = primary_mapper.\
                    _with_polymorphic_args(classes, selectable, innerjoin=innerjoin)
    if aliased:
        selectable = selectable.alias()
    return AliasedClass(base, 
                selectable, 
                with_polymorphic_mappers=mappers, 
                with_polymorphic_discriminator=polymorphic_on)


def _orm_annotate(element, exclude=None):
    """Deep copy the given ClauseElement, annotating each element with the
    "_orm_adapt" flag.

    Elements within the exclude collection will be cloned but not annotated.

    """
    return sql_util._deep_annotate(element, {'_orm_adapt':True}, exclude)

def _orm_deannotate(element):
    """Remove annotations that link a column to a particular mapping.

    Note this doesn't affect "remote" and "foreign" annotations
    passed by the :func:`.orm.foreign` and :func:`.orm.remote`
    annotators.

    """

    return sql_util._deep_deannotate(element, 
                values=("_orm_adapt", "parententity")
            )

def _orm_full_deannotate(element):
    return sql_util._deep_deannotate(element)

class _ORMJoin(expression.Join):
    """Extend Join to support ORM constructs as input."""

    __visit_name__ = expression.Join.__visit_name__

    def __init__(self, left, right, onclause=None, 
                            isouter=False, join_to_left=True):
        adapt_from = None

        if hasattr(left, '_orm_mappers'):
            left_mapper = left._orm_mappers[1]
            if join_to_left:
                adapt_from = left.right
        else:
            left_mapper, left, left_is_aliased = _entity_info(left)
            if join_to_left and (left_is_aliased or not left_mapper):
                adapt_from = left

        right_mapper, right, right_is_aliased = _entity_info(right)
        if right_is_aliased:
            adapt_to = right
        else:
            adapt_to = None

        if left_mapper or right_mapper:
            self._orm_mappers = (left_mapper, right_mapper)

            if isinstance(onclause, basestring):
                prop = left_mapper.get_property(onclause)
            elif isinstance(onclause, attributes.QueryableAttribute):
                if adapt_from is None:
                    adapt_from = onclause.__clause_element__()
                prop = onclause.property
            elif isinstance(onclause, MapperProperty):
                prop = onclause
            else:
                prop = None

            if prop:
                pj, sj, source, dest, \
                secondary, target_adapter = prop._create_joins(
                                source_selectable=adapt_from,
                                dest_selectable=adapt_to,
                                source_polymorphic=True,
                                dest_polymorphic=True,
                                of_type=right_mapper)

                if sj is not None:
                    left = sql.join(left, secondary, pj, isouter)
                    onclause = sj
                else:
                    onclause = pj
                self._target_adapter = target_adapter

        expression.Join.__init__(self, left, right, onclause, isouter)

    def join(self, right, onclause=None, isouter=False, join_to_left=True):
        return _ORMJoin(self, right, onclause, isouter, join_to_left)

    def outerjoin(self, right, onclause=None, join_to_left=True):
        return _ORMJoin(self, right, onclause, True, join_to_left)

def join(left, right, onclause=None, isouter=False, join_to_left=True):
    """Produce an inner join between left and right clauses.

    :func:`.orm.join` is an extension to the core join interface
    provided by :func:`.sql.expression.join()`, where the
    left and right selectables may be not only core selectable
    objects such as :class:`.Table`, but also mapped classes or
    :class:`.AliasedClass` instances.   The "on" clause can
    be a SQL expression, or an attribute or string name
    referencing a configured :func:`.relationship`.

    ``join_to_left`` indicates to attempt aliasing the ON clause,
    in whatever form it is passed, to the selectable
    passed as the left side.  If False, the onclause
    is used as is.

    :func:`.orm.join` is not commonly needed in modern usage,
    as its functionality is encapsulated within that of the
    :meth:`.Query.join` method, which features a
    significant amount of automation beyond :func:`.orm.join`
    by itself.  Explicit usage of :func:`.orm.join` 
    with :class:`.Query` involves usage of the 
    :meth:`.Query.select_from` method, as in::

        from sqlalchemy.orm import join
        session.query(User).\\
            select_from(join(User, Address, User.addresses)).\\
            filter(Address.email_address=='foo@bar.com')

    In modern SQLAlchemy the above join can be written more 
    succinctly as::

        session.query(User).\\
                join(User.addresses).\\
                filter(Address.email_address=='foo@bar.com')

    See :meth:`.Query.join` for information on modern usage
    of ORM level joins.

    """
    return _ORMJoin(left, right, onclause, isouter, join_to_left)

def outerjoin(left, right, onclause=None, join_to_left=True):
    """Produce a left outer join between left and right clauses.

    This is the "outer join" version of the :func:`.orm.join` function,
    featuring the same behavior except that an OUTER JOIN is generated.
    See that function's documentation for other usage details.

    """
    return _ORMJoin(left, right, onclause, True, join_to_left)

def with_parent(instance, prop):
    """Create filtering criterion that relates this query's primary entity
    to the given related instance, using established :func:`.relationship()`
    configuration.

    The SQL rendered is the same as that rendered when a lazy loader
    would fire off from the given parent on that attribute, meaning
    that the appropriate state is taken from the parent object in 
    Python without the need to render joins to the parent table
    in the rendered statement.

    .. versionchanged:: 0.6.4
        This method accepts parent instances in all 
        persistence states, including transient, persistent, and detached.
        Only the requisite primary key/foreign key attributes need to
        be populated.  Previous versions didn't work with transient
        instances.

    :param instance:
      An instance which has some :func:`.relationship`.

    :param property:
      String property name, or class-bound attribute, which indicates
      what relationship from the instance should be used to reconcile the 
      parent/child relationship. 

    """
    if isinstance(prop, basestring):
        mapper = object_mapper(instance)
        prop = getattr(mapper.class_, prop).property
    elif isinstance(prop, attributes.QueryableAttribute):
        prop = prop.property

    return prop.compare(operators.eq, 
                        instance, 
                        value_is_parent=True)


extended_entity_info = util.namedtuple("extended_entity_info", [
    "entity",
    "mapper",
    "selectable",
    "is_aliased_class",
    "with_polymorphic_mappers",
    "with_polymorphic_discriminator"
])
def _extended_entity_info(entity, compile=True):
    if isinstance(entity, AliasedClass):
        return extended_entity_info(
            entity,
            entity._AliasedClass__mapper, \
                    entity._AliasedClass__alias, \
                    True, \
                    entity._AliasedClass__with_polymorphic_mappers, \
                    entity._AliasedClass__with_polymorphic_discriminator
        )

    if isinstance(entity, mapperlib.Mapper):
        mapper = entity

    elif isinstance(entity, type):
        class_manager = attributes.manager_of_class(entity)

        if class_manager is None:
            return extended_entity_info(entity, None, entity, False, [], None)

        mapper = class_manager.mapper
    else:
        return extended_entity_info(entity, None, entity, False, [], None)

    if compile and mapperlib.module._new_mappers:
        mapperlib.configure_mappers()
    return extended_entity_info(
        entity, 
        mapper, \
            mapper._with_polymorphic_selectable, \
            False, \
            mapper._with_polymorphic_mappers, \
            mapper.polymorphic_on
        )

def _entity_info(entity, compile=True):
    """Return mapping information given a class, mapper, or AliasedClass.

    Returns 3-tuple of: mapper, mapped selectable, boolean indicating if this
    is an aliased() construct.

    If the given entity is not a mapper, mapped class, or aliased construct,
    returns None, the entity, False.  This is typically used to allow
    unmapped selectables through.

    """
    return _extended_entity_info(entity, compile)[1:4]

def _entity_descriptor(entity, key):
    """Return a class attribute given an entity and string name.

    May return :class:`.InstrumentedAttribute` or user-defined
    attribute.

    """
    if isinstance(entity, expression.FromClause):
        description = entity
        entity = entity.c
    elif not isinstance(entity, (AliasedClass, type)):
        description = entity = entity.class_
    else:
        description = entity

    try:
        return getattr(entity, key)
    except AttributeError:
        raise sa_exc.InvalidRequestError(
                    "Entity '%s' has no property '%s'" % 
                    (description, key)
                )

def _orm_columns(entity):
    mapper, selectable, is_aliased_class = _entity_info(entity)
    if isinstance(selectable, expression.Selectable):
        return [c for c in selectable.c]
    else:
        return [selectable]

def _orm_selectable(entity):
    mapper, selectable, is_aliased_class = _entity_info(entity)
    return selectable

def _attr_as_key(attr):
    if hasattr(attr, 'key'):
        return attr.key
    else:
        return expression._column_as_key(attr)

def _is_aliased_class(entity):
    return isinstance(entity, AliasedClass)

_state_mapper = util.dottedgetter('manager.mapper')

def object_mapper(instance):
    """Given an object, return the primary Mapper associated with the object
    instance.

    Raises UnmappedInstanceError if no mapping is configured.

    This function is available via the inspection system as::

        inspect(instance).mapper

    """
    return object_state(instance).mapper

@inspection._inspects(object)
def object_state(instance):
    """Given an object, return the primary Mapper associated with the object
    instance.

    Raises UnmappedInstanceError if no mapping is configured.

    This function is available via the inspection system as::

        inspect(instance)

    """
    try:
        return attributes.instance_state(instance)
        # TODO: whats the py-2/3 syntax to catch two
        # different kinds of exceptions at once ?
    except exc.UnmappedClassError:
        raise exc.UnmappedInstanceError(instance)
    except exc.NO_STATE:
        raise exc.UnmappedInstanceError(instance)


@inspection._inspects(type)
def class_mapper(class_, compile=True):
    """Given a class, return the primary :class:`.Mapper` associated 
    with the key.

    Raises :class:`.UnmappedClassError` if no mapping is configured
    on the given class, or :class:`.ArgumentError` if a non-class
    object is passed.

    This function is available via the inspection system as::

        inspect(some_mapped_class)

    """

    try:
        class_manager = attributes.manager_of_class(class_)
        mapper = class_manager.mapper

    except exc.NO_STATE:
        if not isinstance(class_, type): 
            raise sa_exc.ArgumentError("Class object expected, got '%r'." % class_) 
        raise exc.UnmappedClassError(class_)

    if compile and mapperlib.module._new_mappers:
        mapperlib.configure_mappers()
    return mapper

def _class_to_mapper(class_or_mapper, compile=True):
    if _is_aliased_class(class_or_mapper):
        return class_or_mapper._AliasedClass__mapper

    elif isinstance(class_or_mapper, type):
        try:
            class_manager = attributes.manager_of_class(class_or_mapper)
            mapper = class_manager.mapper
        except exc.NO_STATE:
            raise exc.UnmappedClassError(class_or_mapper)
    elif isinstance(class_or_mapper, mapperlib.Mapper):
        mapper = class_or_mapper
    else:
        raise exc.UnmappedClassError(class_or_mapper)

    if compile and mapperlib.module._new_mappers:
        mapperlib.configure_mappers()
    return mapper

def has_identity(object):
    state = attributes.instance_state(object)
    return state.has_identity

def _is_mapped_class(cls):
    """Return True if the given object is a mapped class, 
    :class:`.Mapper`, or :class:`.AliasedClass`."""

    if isinstance(cls, (AliasedClass, mapperlib.Mapper)):
        return True
    if isinstance(cls, expression.ClauseElement):
        return False
    if isinstance(cls, type):
        manager = attributes.manager_of_class(cls)
        return manager and _INSTRUMENTOR in manager.info
    return False

def _mapper_or_none(cls):
    """Return the :class:`.Mapper` for the given class or None if the 
    class is not mapped."""

    manager = attributes.manager_of_class(cls)
    if manager is not None and _INSTRUMENTOR in manager.info:
        return manager.info[_INSTRUMENTOR]
    else:
        return None

def instance_str(instance):
    """Return a string describing an instance."""

    return state_str(attributes.instance_state(instance))

def state_str(state):
    """Return a string describing an instance via its InstanceState."""

    if state is None:
        return "None"
    else:
        return '<%s at 0x%x>' % (state.class_.__name__, id(state.obj()))

def state_class_str(state):
    """Return a string describing an instance's class via its InstanceState."""

    if state is None:
        return "None"
    else:
        return '<%s>' % (state.class_.__name__, )

def attribute_str(instance, attribute):
    return instance_str(instance) + "." + attribute

def state_attribute_str(state, attribute):
    return state_str(state) + "." + attribute

def identity_equal(a, b):
    if a is b:
        return True
    if a is None or b is None:
        return False
    try:
        state_a = attributes.instance_state(a)
        state_b = attributes.instance_state(b)
    except exc.NO_STATE:
        return False
    if state_a.key is None or state_b.key is None:
        return False
    return state_a.key == state_b.key

